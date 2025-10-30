mod iter;
mod node;
mod table;

use crate::node::Node;
use crate::table::Table;
use crossbeam::epoch::{Atomic, Guard, Owned, Shared, pin, unprotected};
use node::Bin;
use parking_lot::Mutex;
use std::cmp::min;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::{mem, thread};

/// The largest possible table capacity. This value must be
/// exactly 1 << 30 to stay within Java array allocation and indexing
/// bounds for power of two table sizes, and is further required
/// because the top two bits of 32bit hash fields are used for
/// control purposes.
const MAXIMUM_CAPACITY: usize = 1 << 30;

/// Default initial table capacity. Must be a power of 2
/// (i.e., at least 1) and at most `MAXIMUM_CAPACITY`.
const DEFAULT_CAPACITY: usize = 16;

/// The load factor for this table. Overrides of this value in
/// constructors affect only the initial table capacity. The
/// actual floating point value isn't normally used. It is
/// simpler to use expressions such as (n >> 2) for
/// the associated resizing threshold.
const LOAD_FACTOR: f64 = 0.75;

/// Minimum number of bins one thread transfers at a time. Ranges are
/// subdivided to allow multiple resizer threads. This value
/// serves as a lower bound to avoid resizers encountering
/// excessive memory contention. The value should be at least
/// `DEFAULT_CAPACITY`.
// Minimal amount of bins one thread can transfer
// during resizing of a table.
// It ensures that when the hash table grows,
// the work is divided into chunks that are large enough
// to be efficient but small enough to enable parallelism.
const MIN_TRANSFER_STEP: usize = 16;

/// The maximum number of threads that can help resize.
/// Must fit in `32 - RESIZE_STAMP_BITS` bits.
const MAX_RESIZERS: usize = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

/// The number of bits used for generation stamp in `size_control`.
/// Must be at least 6 for 32bit arrays.
const RESIZE_STAMP_BITS: usize = 16;

/// The bit shift for recording size stamp in `size_control`.
const RESIZE_STAMP_SHIFT: usize = 32 - RESIZE_STAMP_BITS;

pub struct Map<K, V, S = RandomState> {
    /// The array of bins. Lazily initialized upon first insertion.
    /// Size is always a power of two. Accessed directly by iterators.
    table: Atomic<Table<K, V>>,
    next_table: Atomic<Table<K, V>>,
    /// index of bin to transfer next during resizing
    transfer_index: AtomicIsize,
    /// map length
    len: AtomicUsize,
    /// Table initialization and resizing control. When negative, the
    /// table is being initialized or resized: -1 for initialization,
    /// else -(1 + the number of active resizing threads). Otherwise,
    /// when table is null, holds the initial table size to use upon
    /// creation, or 0 for default. After initialization, holds the
    /// length value upon which to resize the table.
    // Resize threshold here is 0.75 * len.
    // During normal operation `size_control` contains only the resize threshold.
    // During resize `size_control` contains resize stamp and thread count:
    // [RESIZE_STAMP (16 bits)][NUMBER_OF_HELPING_THREADS]
    // ┌─────────────────┐
    // │        0        │ ────┐
    // │ (Uninitialized) │     │
    // └─────────────────┘     │
    // │              │ Constructor with
    // │ Initialize   │ initial capacity
    // ▼              │        │
    // ┌─────────────────┐     │
    // │       -1        │ ◄───┘
    // │ (Initializing)  │
    // └─────────────────┘
    // │
    // │ Completion
    // ▼
    // ┌─────────────────┐
    // │     Positive    │ ◄───┐
    // │   (Normal op.)  │     │
    // └─────────────────┘     │
    // │              │        │
    // │ Trigger      │      Resize
    // │ resize       │     complete
    // ▼              │        │
    // ┌─────────────────┐     │
    // │  (rs<<16) + 2   │ ────┘
    // │ (Resize start)  │
    // └─────────────────┘
    // │
    // │ More threads
    // ▼ join
    // ┌─────────────────┐
    // │  (rs<<16) + n   │
    // │(Resize progress)│
    // └─────────────────┘
    size_control: AtomicIsize,
    hasher_builder: S,
}

impl<K, V> Map<K, V>
where
    K: Hash + Eq + Clone + Sync + Send,
    V: Sync + Send,
{
    pub fn new() -> Self {
        Self {
            table: Atomic::null(),
            next_table: Atomic::null(),
            transfer_index: AtomicIsize::new(0),
            len: AtomicUsize::new(0),
            size_control: AtomicIsize::new(0),
            hasher_builder: RandomState::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        assert_ne!(cap, 0);
        // `map` is immutable because no methods take `&mut self`
        let map = Self::new();
        // size = 1.0 + 16 / 0.75
        // size = 1.0 + 21.333
        // size = 22.333
        // size = 22 (as usize)
        // without 1.0 added: 21 × 0.75 = 15.75 ≈ 15 elements (not requested 16)
        let size = (1.0 + (cap as f64) / LOAD_FACTOR) as usize;
        // This is `tableSizeFor` in Java implementation.
        // See `Hacker's Delight, sec 3.2` for the algorithm.
        let cap = min(MAXIMUM_CAPACITY, size.next_power_of_two());
        map.size_control.store(cap as isize, Ordering::SeqCst);
        map
    }
}

impl<K, V, S> Map<K, V, S>
where
    K: Hash + Eq + Clone + Sync + Send,
    V: Sync + Send,
    S: BuildHasher,
{
    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>> {
        let hash = self.hash(key);
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }
        // SAFETY: We loaded the table while epoch was pinned. Table won't be
        // deallocated until the next epoch.
        let table_ref = unsafe { table.deref() };
        if table_ref.bins.len() == 0 {
            return None;
        }
        let bin = table_ref.get_by_hash(hash, guard);
        if bin.is_null() {
            return None;
        }
        let node = unsafe { bin.deref() }.find(hash, key, guard);
        if node.is_null() {
            return None;
        }
        let node = unsafe { node.deref() }.as_node().unwrap();
        let value = node.value.load(Ordering::SeqCst, guard);
        assert!(!value.is_null());
        Some(value)
    }

    pub fn contains_key<'g>(&self, key: &K) -> bool{
        let guard = &pin();
        self.get(key, guard).is_some()
    }

    pub fn insert(&self, key: K, value: V) -> Option<()> {
        self.put(key, value, false)
    }

    fn put(&self, key: K, value: V, no_replace: bool) -> Option<()> {
        let hash = self.hash(&key);
        let mut new_node = Owned::new(Bin::Node(Node {
            hash,
            key,
            value: Atomic::new(value),
            next: Atomic::null(),
            mu: Mutex::new(()),
        }));
        let guard = &pin();
        let mut table = self.table.load(Ordering::SeqCst, guard);
        loop {
            // SAFETY: Table is a valid pointer.
            // 1) If table is the one we read before the loop, then we read it while
            //    holding the guard, so it won't be dropped until after we drop that guard, because
            //    the drop logic only queues a drop for the next epoch after removing the table;
            // 2) If table is read by `init_table`, then either we did a load, and the argument is
            //    as point 1, or we allocated a table, in which case the earliest it can be
            //    deallocated is in the next epoch. We are holding up the epoch by holding the guard,
            //    so dereference is safe;
            // 3) If table is set by a `Moved` node below, it will either keep using table,
            //    or use the next table raw pointer from inside `Moved`.
            let table_ref = unsafe { table.deref() };
            if table.is_null() || table_ref.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }
            let bin_i = table_ref.bin_index(hash);
            let mut bin = table_ref.get_by_index(bin_i, guard);
            if bin.is_null() {
                // bin is empty so we put a new node at the front
                match table_ref.compare_and_swap(bin_i, bin, new_node, guard) {
                    Ok(_old_null_ptr) => {
                        // If bin doesn't have next nodes its length (depth) is 0.
                        // That's why we pass `Some(0)`.
                        self.add_len(1, Some(0), guard);
                        return None;
                    }
                    Err(err) => {
                        assert!(!err.current.is_null());
                        new_node = err.new;
                        bin = err.current;
                    }
                }
            }
            // bin is not empty
            let bin_ref = unsafe { bin.deref() };
            let (hash, key) = if let Bin::Node(Node { hash, ref key, .. }) = *new_node {
                (hash, key)
            } else {
                unreachable!();
            };
            match bin_ref {
                Bin::Node(head) if no_replace && head.hash == hash && &head.key == key => {
                    // if replacement is disallowed and first bin matches
                    return Some(());
                }
                Bin::Node(head) => {
                    // bin is not empty, so we must take the lock and link into it
                    let lock = head.mu.lock();
                    // check if head is still head
                    let current_head = table_ref.get_by_index(bin_i, guard);
                    if current_head != bin {
                        continue;
                    }
                    // It's still the head so we can "own" the bin.
                    // There can still be readers in the bin.
                    let mut bin_len = 1;
                    let mut current_node = bin;
                    let old_value = loop {
                        // SAFETY: We read the bin while pinning the epoch. A bin will never be dropped
                        // until the next epoch, after it's removed. Since it wasn't removed,
                        // and the epoch was pinned, that cannot happen until we drop our guard.
                        let node = unsafe { current_node.deref() }.as_node().unwrap();
                        if node.hash == hash && &node.key == key {
                            // the key already exists in the map
                            if no_replace {
                                // not allowed to update value
                            } else if let Bin::Node(Node { value, .. }) = *new_node.into_box() {
                                // SAFETY: we own value and have never shared it
                                // insert new value into node
                                let garbage = node.value.swap(
                                    unsafe { value.into_owned() },
                                    Ordering::SeqCst,
                                    guard,
                                );
                                // SAFETY: need to guarantee that garbage is no longer reachable.
                                // No thread that executes after this line can ever get a reference to garbage.
                                // Possible scenarios:
                                // - Another thread had read the value before the swap (has a reference to it).
                                //   Either its guard was taken before ours, in which case that thread
                                //   must be pinned to an epoch <= epoch of our guard, since garbage is placed
                                //   in our epoch and won't be freed until next epoch, at which point that thread
                                //   must have dropped its guard and with it any reference to the value.
                                // - Another thread about to get a reference to this value. They execute after
                                //   the swap, and therefore doesn't get a reference to the garbage, so freeing
                                //   garbage is now OK.
                                unsafe { guard.defer_destroy(garbage) }
                            } else {
                                unreachable!()
                            }
                            break Some(());
                        }
                        let next_node = node.next.load(Ordering::SeqCst, guard);
                        if next_node.is_null() {
                            // we're at the end of the bin, so we put node here
                            node.next.store(new_node, Ordering::SeqCst);
                            break None;
                        }
                        current_node = next_node;
                        bin_len += 1;
                    };
                    // drop the lock because `add_len` might need it
                    // inside `transfer` method during a resize
                    drop(lock);
                    if old_value.is_none() {
                        // increment length if we put a new node
                        self.add_len(1, Some(bin_len), guard);
                    }
                    return old_value;
                }
                Bin::Moved(next_table) => {
                    table = self.help_transfer(table, *next_table, guard);
                    continue;
                }
            }
        }
    }

    fn init_table<'g>(&self, guard: &'g Guard) -> Shared<'g, Table<K, V>> {
        loop {
            let table = self.table.load(Ordering::SeqCst, guard);
            // SAFETY: We loaded the table while epoch was pinned. Table won't be
            // deallocated until the next epoch.
            if !table.is_null() && !unsafe { table.deref() }.bins.is_empty() {
                break table;
            }
            let mut size_control = self.size_control.load(Ordering::SeqCst);
            if size_control < 0 {
                // someone else is initializing the table
                thread::yield_now();
                continue;
            }
            if let Ok(_) = self.size_control.compare_exchange(
                size_control,
                -1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                let mut table = self.table.load(Ordering::SeqCst, guard);
                // SAFETY: We loaded the table while epoch was pinned. Table won't be
                // deallocated until the next epoch.
                if table.is_null() || unsafe { table.deref() }.bins.is_empty() {
                    let cap = if size_control > 0 {
                        size_control as usize
                    } else {
                        DEFAULT_CAPACITY
                    };
                    let new_table = Owned::new(Table::new(cap));
                    table = new_table.into_shared(guard);
                    self.table.store(table, Ordering::SeqCst);
                    size_control = (cap - (cap >> 2)) as isize; // cap - cap/4 = 0.75 * cap
                }
                // Unsets `-1`. It's similar to releasing the lock.
                self.size_control.store(size_control, Ordering::SeqCst);
                break table;
            }
        }
    }

    fn add_len(&self, n: isize, resize_hint: Option<usize>, guard: &Guard) {
        // If `resize_hint` is `None`, caller does not consider a resize.
        // If it's `Some(n)`, the caller traversed `n` nodes in the bin.
        if resize_hint.is_none() {
            return;
        }
        // Treeify the linked list. Not implemented yet.
        let _traversed_nodes = resize_hint.unwrap();
        let mut len = if n > 0 {
            let n = n as usize;
            self.len.fetch_add(n, Ordering::SeqCst) + n
        } else if n < 0 {
            let n = n.abs() as usize;
            self.len.fetch_sub(n, Ordering::SeqCst) - n
        } else {
            self.len.load(Ordering::SeqCst)
        };
        loop {
            let size_control = self.size_control.load(Ordering::SeqCst);
            if size_control > len as isize {
                // we're not at the next resize point
                break;
            };
            let table = self.table.load(Ordering::SeqCst, guard);
            if table.is_null() {
                // table has been initialized by another thread
                break;
            }
            // SAFETY: Table is only dropped on the next epoch change after it's swapped
            // to null. We read it as not null, so it must not be dropped until the next
            // epoch, since we hold a guard. We know that the current epoch will persist,
            // and that our reference therefore will remain valid.
            let table_ref = unsafe { table.deref() };
            let n_bins = table_ref.bins.len();
            if n_bins >= MAXIMUM_CAPACITY {
                // can't resize anymore
                break;
            }
            let resize_stamp = Self::resize_stamp(n_bins) << RESIZE_STAMP_SHIFT;
            if size_control < 0 {
                // Ongoing resize.
                // Check if we're allowed to help resize.
                // The low 16 bits being 1 indicate that resize is finished
                // (size_control == resize_stamp + 1).
                if size_control == resize_stamp + MAX_RESIZERS as isize
                    || size_control == resize_stamp + 1
                {
                    break;
                }
                let next_table = self.next_table.load(Ordering::SeqCst, guard);
                if next_table.is_null() {
                    break;
                }
                if self.transfer_index.load(Ordering::SeqCst) <= 0 {
                    break;
                }
                // try to join
                if let Ok(_) = self.size_control.compare_exchange(
                    size_control,
                    size_control + 1, // add our thread to resizers count
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    self.transfer(table, next_table, guard);
                };
            } else if let Ok(_) = self.size_control.compare_exchange(
                size_control,
                // first thread adds `2` to resizers count for some reason
                resize_stamp + 2,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // resize is needed but hasn't been started yet
                self.transfer(table, Shared::null(), guard);
            }
            // another resize may be needed
            len = self.len.load(Ordering::SeqCst);
        }
    }

    fn help_transfer<'g>(
        &self,
        table: Shared<'g, Table<K, V>>,
        next_table: *const Table<K, V>,
        guard: &Guard,
    ) -> Shared<'g, Table<K, V>> {
        let next_table = Shared::from(next_table);
        if table.is_null() || next_table.is_null() {
            return table;
        }
        // SAFETY: Table is only dropped on the next epoch change after it's swapped
        // to null. We read it as not null, so it must not be dropped until the next
        // epoch, since we hold a guard. We know that the current epoch will persist,
        // and that our reference therefore will remain valid.
        let table_ref = unsafe { table.deref() };
        let cap = table_ref.bins.len();
        let resize_stamp = Self::resize_stamp(cap) << RESIZE_STAMP_SHIFT;
        // table == self.table.load(Ordering::SeqCst, guard)
        // table: parameter passed to the method (expected to be the current table).
        // self.table: current main table reference.
        // Checks: table hasn't been replaced by a newer resize operation.
        // next_table: local variable storing the new table from the `Moved` bin.
        // next_table: field in Map that points to the new table during resizing.
        // Checks: resize operation we're trying to help with is
        // still ongoing and hasn't been completed.
        while table == self.table.load(Ordering::SeqCst, guard)
            && next_table == self.next_table.load(Ordering::SeqCst, guard)
        {
            let size_control = self.size_control.load(Ordering::SeqCst);
            if size_control >= 0
                || size_control == resize_stamp + MAX_RESIZERS as isize
                // `size_control == resize_stamp` + 1 is the completion signal.
                // The low 16 bits being 1 indicate that resize is finished.
                || size_control == resize_stamp + 1
                || self.transfer_index.load(Ordering::SeqCst) <= 0
            {
                break;
            }
            if let Ok(_) = self.size_control.compare_exchange(
                size_control,
                size_control + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                self.transfer(table, next_table, guard);
                break;
            }
        }
        next_table
    }

    fn transfer<'g>(
        &self,
        table: Shared<'g, Table<K, V>>,
        mut next_table: Shared<'g, Table<K, V>>,
        guard: &'g Guard,
    ) {
        // SAFETY: It was read while guard was held. The code that drops it
        // only drops it after:
        //  1. It's no longer reachable;
        //  2. Any outstanding references are no longer active. This reference is
        //     still active (marked by the guard), so the target of this reference
        //     won't be dropped while the guard remains active.
        let table_ref = unsafe { table.deref() };
        let cap = table_ref.bins.len() as isize;
        let step = MIN_TRANSFER_STEP as isize;
        if next_table.is_null() {
            // We are initializing a resize.
            // `<< 1` multiplies capacity by two.
            let new_cap = cap << 1;
            let new_table = Owned::new(Table::new(new_cap as usize));
            let garbage = self.next_table.swap(new_table, Ordering::SeqCst, guard);
            assert!(garbage.is_null());
            // initializing thread sets transfer index to old table's capacity
            self.transfer_index.store(cap, Ordering::SeqCst);
            next_table = self.next_table.load(Ordering::Relaxed, guard);
        }
        let next_table_ref = unsafe { next_table.deref() };
        let next_cap = next_table_ref.bins.len() as isize;
        // specifies if we should advance to the next bin to process it
        let mut advance = true;
        let mut finishing = false;
        // current bin's index which is being processed
        let mut bin_i = 0;
        // Last bin's index to process (lower index bound).
        // if transfer_index = 64, step = 16, then bound = 48.
        let mut bound = 0;
        // Transfer index is the next bin to transfer.
        // Step is amount of bins to transfer at one go.
        // Bins are transferred from the end.
        // This loop tries to claim a region of bins to transfer.
        loop {
            // 1: transfer_index = 64
            while advance {
                // we move backward in the bin slice
                // 1: bin_i = -1
                // 2: bin_i = 63
                bin_i -= 1;
                // 1: -1 >= 0 || false
                // 1: false || false
                // 2: 63 >= 48
                // 2: we break and transfer bin_i = 63
                if bin_i >= bound || finishing {
                    advance = false;
                    break;
                }
                // `transfer_index` starts at table's capacity
                // 1: next_bin_i = 64
                let next_bin_i = self.transfer_index.load(Ordering::SeqCst);
                // check for `0` here because after `transfer_index` was set to `0`
                // all bins had been processed
                if next_bin_i <= 0 {
                    // this assignment is necessary because our bin_i can still be
                    // some positive number while other threads finished the transfer
                    bin_i = -1;
                    advance = false;
                    break;
                }
                // 1: next_bound = 64 - 16 = 48
                let next_bound = if next_bin_i > step {
                    next_bin_i - step
                } else {
                    0
                };
                // 1: transfer_index = 48
                if let Ok(_) = self.transfer_index.compare_exchange(
                    next_bin_i,
                    next_bound,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    bound = next_bound;
                    // 1: bin_i = 64
                    // TODO: `i = nextIndex - 1` in Java code.
                    // check during tests if it's a bug
                    bin_i = next_bin_i - 1;
                    advance = false;
                    break;
                }
            }
            // 1: bin_i = 64
            // 1) `bin_i < 0`: no more work
            // 2) `bin_i >= cap`: defensive programming check against corrupted state
            // 3) `bin_i + cap >= next_cap`: not clear why this is necessary
            if bin_i < 0 || bin_i >= cap || bin_i + cap >= next_cap {
                // the resize has finished
                if finishing {
                    // this branch is only taken for one thread partaking in the resize
                    self.next_table.store(Shared::null(), Ordering::SeqCst);
                    let garbage = self.table.swap(next_table, Ordering::SeqCst, guard);
                    // SAFETY: need to guarantee that garbage is no longer reachable.
                    // No thread that executes after this line can ever get a reference to garbage.
                    // Possible scenarios:
                    // - Another thread had read the value before the swap (has a reference to it).
                    //   Either its guard was taken before ours, in which case that thread
                    //   must be pinned to an epoch <= epoch of our guard, since garbage is placed
                    //   in our epoch and won't be freed until next epoch, at which point that thread
                    //   must have dropped its guard and with it any reference to the value.
                    // - Another thread about to get a reference to this value. They execute after
                    //   the swap, and therefore doesn't get a reference to the garbage, so freeing
                    //   garbage is now OK.
                    unsafe { guard.defer_destroy(garbage) }
                    // Store new resize threshold.
                    // cap = 64
                    // cap << 1 = 64 × 2 = 128 (new table size)
                    // cap >> 1 = 64 ÷ 2 = 32 (half of old size)
                    // control_size = 128 - 32 = 96 (load factor = 0.75)
                    self.size_control
                        .store((cap << 1) - (cap >> 1), Ordering::SeqCst);
                    return;
                }
                let size_control = self.size_control.load(Ordering::SeqCst);
                if let Ok(_) = self.size_control.compare_exchange(
                    size_control,
                    size_control - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    // size_control = (resize_stamp << RESIZE_STAMP_SHIFT) + number_of_workers + 2
                    // Determines if we are the last thread participating in resize. If we are not
                    // the last thread we return, else we check the bins again and then do the
                    // "cleanup" in `if finishing` clause above.
                    if (size_control - 2) != Self::resize_stamp(cap as usize) << RESIZE_STAMP_SHIFT
                    {
                        return;
                    }
                    finishing = true;
                    // check if we can assist with any subsequent resize
                    advance = true;
                    // By setting `bin_i = cap`, the thread ensures it will process every bucket
                    // one more time to ensure no bucket was missed due to race conditions.
                    bin_i = cap;
                }
                continue;
            }
            let bin = table_ref.get_by_index(bin_i as usize, guard);
            if bin.is_null() {
                advance = table_ref
                    .compare_and_swap(
                        bin_i as usize,
                        Shared::null(),
                        Owned::new(Bin::Moved(next_table.as_raw())),
                        guard,
                    )
                    .is_ok();
                continue;
            }
            // SAFETY: `bin` is a valid pointer.
            // There are two cases when bin is invalidated:
            //  1) If the table was resized, bin is a `Bin::Moved` and the resize has completed.
            //     In this case the table and its heads will be dropped in the next epoch;
            //  2) If the table is being resized, bin may have been swapped with a `Bin::Moved`.
            //     The old bin will then be dropped in the following epoch.
            // In both cases we held the guard when we got the reference to bin. If any such swap
            // happened it happened after we read. Since we did the read while pinning the epoch,
            // the drop must happen in the next epoch.
            match unsafe { bin.deref() } {
                Bin::Node(head) => {
                    // bin is not empty, need to link through it, so we must take the lock
                    let _lock = head.mu.lock();
                    // check if head hasn't changed in case it was modified by other thread
                    // before we took the lock
                    let current_head = table_ref.get_by_index(bin_i as usize, guard);
                    if current_head != bin {
                        continue;
                    }
                    // It's still the head so we can "own" the bin.
                    // There can still be readers in the bin.
                    // Part of bins are moved to a different index during resize,
                    // and part of bins remain at the same index.
                    // Optimization with `sequence_node` only helps with the last sequence
                    // which doesn't require reallocation!
                    let mut sequence_bit = head.hash & cap as u64;
                    // first node in a sequence of nodes which are moved
                    // to new indices or a sequence of nodes which stay
                    // at the same indices
                    let mut sequence_node = bin;
                    // current node during bin traversal
                    let mut current_node = bin;
                    loop {
                        // SAFETY: `node` is a valid pointer. It will only be dropped in the next epoch
                        // after it is replaced with `Bin::Moved`. We read the bin and got to `node`, so it
                        // wasn't swapped with `Bin::Moved` and we have the epoch pinned so the epoch cannot
                        // have arrived yet, therefore, it will be dropped in a future epoch.
                        let node = unsafe { current_node.deref() }.as_node().unwrap();
                        // Old table size: 16 (binary: 10000)
                        // New table size: 32 (binary: 100000)
                        //
                        // Node hash: 25 (binary: 11001)
                        // Node index: 9 (binary: 1001)
                        // let index_change_bit = 25 & 16; // = 16 (binary: 11001 & 10000 = 10000)
                        //
                        // Since index_change_bit != 0, this node moves from index 9 to index 9 + 16 = 25
                        //
                        // Node hash: 9 (binary: 01001)
                        // let index_change_bit = 9 & 16; // = 0 (binary: 01001 & 10000 = 00000)
                        //
                        // Since index_change_bit == 0, this node stays at index 9
                        let index_change_bit = node.hash & cap as u64;
                        if index_change_bit != sequence_bit {
                            sequence_bit = index_change_bit;
                            sequence_node = current_node;
                        }
                        let next_node = node.next.load(Ordering::SeqCst, guard);
                        if next_node.is_null() {
                            break;
                        }
                        current_node = next_node;
                    }
                    // split bin in two
                    let mut unchanged_index_bin = Shared::null();
                    let mut changed_index_bin = Shared::null();
                    if sequence_bit == 0 {
                        unchanged_index_bin = sequence_node;
                    } else {
                        changed_index_bin = sequence_node;
                    }
                    current_node = bin;
                    // no need to proceed past last sequence node,
                    // because next nodes after it don't change bins
                    while current_node != sequence_node {
                        // SAFETY: `node` is a valid pointer. It will only be dropped in the next epoch
                        // after it is replaced with `Bin::Moved`. We read the bin and got to `node`, so it
                        // wasn't swapped with `Bin::Moved` and we have the epoch pinned so the epoch cannot
                        // have arrived yet, therefore, it will be dropped in a future epoch.
                        let node = unsafe { current_node.deref() }.as_node().unwrap();
                        let link = if node.hash & cap as u64 == 0 {
                            // move to the unchanged index bin
                            &mut unchanged_index_bin
                        } else {
                            // move to the changed index bin
                            &mut changed_index_bin
                        };
                        *link = Owned::new(Bin::Node(Node {
                            hash: node.hash,
                            key: node.key.clone(),
                            value: node.value.clone(),
                            // Nodes are appended to the linked list in reverse order,
                            // because order of nodes in the bucket doesn't matter.
                            // Appending to the front of a linked list is O(1).
                            // Appending to the end would require:
                            // - maintaining a tail pointer
                            // - additional checks
                            // - more complex code
                            next: Atomic::from(*link),
                            mu: Mutex::new(()),
                        }))
                        .into_shared(guard);
                        current_node = node.next.load(Ordering::SeqCst, guard);
                    }
                    next_table_ref.store_bin(bin_i as usize, unchanged_index_bin);
                    next_table_ref.store_bin((bin_i + cap) as usize, changed_index_bin);
                    // "link" to the new table
                    table_ref
                        .store_bin(bin_i as usize, Owned::new(Bin::Moved(next_table.as_raw())));
                    // Everything up to last sequence node is garbage.
                    // All those nodes have been reallocated.
                    while current_node != sequence_node {
                        // SAFETY:
                        // 1. The only way to get to node is through `table.bin[i]`.
                        // Since `table.bin[i]` now contains `Bin::Moved`, node is not accessible.
                        // 2. Any existing reference to node must have been taken before
                        // `table.store.bin`. At that time we had the epoch pinned, so any threads
                        // that have such a reference must be before or at our epoch. Since node
                        // is not destroyed until the next epoch, those old references are since
                        // they are tied to those old threads' pins of the old epoch.
                        let node = unsafe { current_node.deref() }.as_node().unwrap();
                        let next_node = node.next.load(Ordering::SeqCst, guard);
                        unsafe { guard.defer_destroy(current_node) };
                        current_node = next_node;
                    }
                    advance = true;
                }
                Bin::Moved(_) => {
                    // already processed
                    advance = true;
                    continue;
                }
            }
        }
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hasher_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns the stamp bits for resizing a table of size `n`.
    /// Must be negative when shifted left by `RESIZE_STAMP_SHIFT`.
    // When resize is in progress `size_control` should be negative.
    // That's why we set MSB in `RESIZE_STAMP_BITS` part of `size_control` as 1.
    //                ∨ (set as 1)
    // Size Control: [RESIZE_STAMP][NUMBER_OF_HELPING_THREADS]
    // Leading zeros take less space to represent a big number.
    // 128 is 24 leading zeros (binary: 11000).
    // Only 5 bits needed instead of potentially 30+!
    fn resize_stamp(n: usize) -> isize {
        // n = 32 = 0b0010_0000
        // 58 | (1 << (16 - 1))
        // 0b0011_1010 | 1 << 15
        // 0b0011_1010 | 0b1000_0000_0000_0000
        // 0b1000_0000_0011_1010
        n.leading_zeros() as isize | (1 << (RESIZE_STAMP_BITS - 1))
    }
}

impl<K, V, S> Drop for Map<K, V, S> {
    fn drop(&mut self) {
        // SAFETY: we have access to `&mut self`, so it's not concurrently accessed by anyone else
        let guard = unsafe { unprotected() };
        assert!(self.next_table.load(Ordering::SeqCst, guard).is_null());
        let table = self.table.swap(Shared::null(), Ordering::SeqCst, guard);
        if table.is_null() {
            // table was never allocated
            return;
        }
        // SAFETY: same as above and we own the table
        let mut table = unsafe { table.into_owned() };
        let bins = Vec::from(mem::replace(&mut table.bins, vec![].into_boxed_slice()));
        for bin in bins {
            if bin.load(Ordering::SeqCst, guard).is_null() {
                // bin was never used
                continue;
            }
            let bin = unsafe { bin.into_owned() };
            match *bin {
                Bin::Node(_) => {
                    let mut bin = bin;
                    loop {
                        // TODO: not sure why value can't be moved out of `Owned`
                        let node = if let Bin::Node(node) = *bin.into_box() {
                            node
                        } else {
                            unreachable!();
                        };
                        // SAFETY: We are dropping the map so no one else is accessing it.
                        // We replace the bin with a null, so there's no future way to access it.
                        // We own all the nodes in the list.
                        let _ = unsafe { node.value.into_owned() };
                        if node.next.load(Ordering::SeqCst, guard).is_null() {
                            break;
                        }
                        bin = unsafe { node.next.into_owned() };
                    }
                }
                Bin::Moved(_) => {}
            };
        }
    }
}
