mod node;
mod table;

use crate::node::Node;
use crate::table::Table;
use crossbeam::epoch::{Atomic, Guard, Owned, Shared, pin};
use node::Bin;
use parking_lot::Mutex;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::thread;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

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
/// actual floating point value isn't normally used -- it is
/// simpler to use expressions such as (n >>> 2) for
/// the associated resizing threshold.
const LOAD_FACTOR: f64 = 0.75;

/// Minimum number of re-binnings per transfer step. Ranges are
/// subdivided to allow multiple resizer threads. This value
/// serves as a lower bound to avoid re-sizers encountering
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
    /// The next table index (plus one) to split while resizing.
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
    // First bit set as `1` indicates that resize is finished.
    size_control: AtomicIsize,
    hasher_builder: S,
}

impl<K, V, S> Map<K, V, S>
where
    K: Hash + Eq,
    S: BuildHasher,
{
    pub fn get<'g>(&self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>> {
        let hash = self.hash(key);
        let table = self.table.load(Ordering::SeqCst, guard);
        if table.is_null() {
            return None;
        }
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
        let v = unsafe { node.deref() }.value.load(Ordering::SeqCst, guard);
        assert!(!v.is_null());
        Some(v)
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
        let old_value = loop {
            let table_ref = unsafe { table.deref() };
            if table.is_null() || table_ref.bins.len() == 0 {
                table = self.init_table(guard);
                continue;
            }
            let bin_i = table_ref.bin_index(hash);
            let mut bin = table_ref.get_by_index(bin_i, guard);
            if bin.is_null() {
                // bin is empty so stick new node at the front
                match table_ref.compare_and_swap(bin_i, bin, new_node, guard) {
                    Ok(_old_null_ptr) => {
                        self.add_len(1, Some(0), guard);
                        return None;
                    }
                    Err(changed) => {
                        assert!(!changed.current.is_null());
                        new_node = changed.new;
                        bin = changed.current;
                    }
                }
            }
            let bin_ref = unsafe { bin.deref() };
            // TODO: clean it up
            let (hash, key) = if let Bin::Node(Node {hash, ref key, ..}) = *new_node {
                (hash, key)
            } else {
                unreachable!();
            };
            // bin is not empty
            match bin_ref {
                Bin::Node(head) if no_replace && head.hash == hash && head.key == key => {
                    // if replacement is disallowed and first bin matches
                    return Some(());
                }
                Bin::Node(head) => {
                    let head = Shared::from(head as *const _);
                    // bin is not empty, need to link to it, so we must take the lock
                    let _guard = head.mu.lock();
                    // need to check that it's still the head
                    let current_head = table_ref.get_by_index(bin_i, guard);
                    if current_head.as_raw() != bin.as_raw() {
                        continue;
                    }
                    // It's still the head so we can "own" the bin.
                    // There can still be readers in the bin.
                    let mut bin_len = 1;
                    // current node
                    let mut node = head;
                    let old_value = loop {
                        if node.hash == hash && node.key == key {
                            // the key already exists in the map
                            if !no_replace {
                                // the key is not absent so don't update
                            } else if let Bin::Node(Node { value, ..}) = *new_node.into_box() {
                                let garbage =
                                    node.swap(value, Ordering::SeqCst, guard);
                            } else {
                                unreachable!()
                            }
                            break Some(());
                        }
                        // this ordering can probably be relaxed due to mutex
                        let next = node.next.load(Ordering::SeqCst, guard);
                        if next.is_null() {
                            // we're at the end of the bin, stick node here
                            node.next.store(new_node, Ordering::SeqCst);
                            break None;
                        }
                        node = unsafe { next.deref() };
                        bin_len += 1;
                    };
                    // TODO: treeify threshold
                    if old_value.is_none() {
                        // increment length
                        // TODO: what do we pass as second argument?
                        self.add_len(1, Some(0), guard);
                    }
                    return old_value;
                }
                Bin::Moved(next_table) => {
                    // FIXME: Moved(next_table) vs self.next_table
                    table = self.help_transfer(table, *next_table, guard);
                    unimplemented!()
                }
            }
        };
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hasher_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn init_table<'g>(&self, guard: &'g Guard) -> Shared<'g, Table<K, V>> {
        loop {
            let table = self.table.load(Ordering::SeqCst, guard);
            if !table.is_null() && !unsafe { table.deref() }.bins.is_empty() {
                break table;
            }
            let mut size_control = self.size_control.load(Ordering::SeqCst);
            if size_control < 0 {
                // some else is initializing the table
                thread::yield_now();
                continue;
            }
            // TODO: why `-1` without shifting?
            if let Ok(_) = self.size_control.compare_exchange(
                size_control,
                -1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                let mut table = self.table.load(Ordering::SeqCst, guard);
                if table.is_null() || unsafe { table.deref() }.bins.is_empty() {
                    let len = if size_control > 0 {
                        size_control as usize
                    } else {
                        DEFAULT_CAPACITY
                    };
                    let new_table = Owned::new(Table::new(len));
                    table = new_table.into_shared(guard);
                    self.table.store(table, Ordering::SeqCst);
                    let len = len as isize;
                    size_control = len - (len >> 2); // len - len/4 = 0.75 * len
                }
                // Unset `-1`. It's similar to releasing the lock.
                self.size_control.store(size_control, Ordering::SeqCst);
                break table;
            }
        }
    }

    fn add_len(&self, n: isize, resize_hint: Option<usize>, guard: &Guard) {
        // If `resize_hint` is `None`, caller does not consider a resize.
        // If it's `Some(num)`, the caller traversed `num` nodes in a bin.
        if resize_hint.is_none() {
            return;
        }
        let traversed_nodes = resize_hint.unwrap();
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
                break;
                // we're not at the next resize point
            };
            let mut table = self.table.load(Ordering::SeqCst, guard);
            if table.is_null() {
                // table has been initialized by another thread
                break;
            }
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
                // `size_control == resize_stamp` + 1 is the completion signal.
                // The low 16 bits being 1 indicate that resize is finished.
                if size_control == resize_stamp + MAX_RESIZERS as isize || size_control == resize_stamp + 1 {
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
                    size_control + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    self.tranfer_table(table, next_table)
                };
            } else if let Ok(_) = self.size_control.compare_exchange(
                size_control,
                resize_stamp + 2,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // a resize is needed but has not yet started
                self.tranfer_table(table, Shared::null())
            }
            // another resize may be needed
            len = self.len.load(Ordering::SeqCst);
        }
    }

    /// Returns the stamp bits for resizing a table of size `n`.
    /// Must be negative when shifted left by `RESIZE_STAMP_SHIFT`.
    // When resize is in progress `size_control` should be negative.
    // That's why we set MSB in `RESIZE_STAMP_BITS` part of `size_control` as 1.
    //                ∨ (set as 1)
    // Size Control: [RESIZE_STAMP][NUMBER_OF_HELPING_THREADS]
    // Leading zeros take less space to represent a big number.
    // 128 -> 24 leading zeros (binary: 11000).
    // Only 5 bits needed instead of potentially 30+!
    fn resize_stamp(n: usize) -> isize {
        // n = 32 = 0010 0000
        // 58 | (1 << (16 - 1))
        // 0011 1010 | 1 << 15
        // 0011 1010 | 1000 0000 0000 0000
        // 1000 0000 0011 1010
        n.leading_zeros() as isize | (1 << (RESIZE_STAMP_BITS - 1))
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
        let table_ref = unsafe { table.deref() };
        let len = table_ref.bins.len();
        let resize_stamp = Self::resize_stamp(len) << RESIZE_STAMP_SHIFT;
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

    fn transfer(
        &self,
        table: Shared<Table<K, V>>,
        mut next_table: Shared<Table<K, V>>,
        guard: &Guard,
    ) {
        let table_ref = unsafe { table.deref() };
        let len = table_ref.bins.len() as isize;
        // TODO: use `n_cpus` to help determine step
        let step = MIN_TRANSFER_STEP as isize;
        if next_table.is_null() {
            // We are initializing a resize.
            // `<< 1` multiplies len by two.
            let next_len = len << 1;
            let new_table = Owned::new(Table::new(next_len as usize));
            // TODO: take care of garbage
            let garbage = self.next_table.swap(new_table, Ordering::SeqCst, guard);
            assert!(garbage.is_null());
            // initializing thread sets transfer index to old table's length
            self.transfer_index.store(len, Ordering::SeqCst);
            next_table = self.next_table.load(Ordering::Relaxed, guard);
        }
        let next_table_ref = unsafe { next_table.deref() };
        let next_len = next_table_ref.bins.len() as isize;
        // specifies if we should advance to the next bin to process it
        let mut advance_index = true;
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
            while advance_index {
                // we move backward in the bin slice
                // 1: bin_i = -1
                // 2: bin_i = 63
                bin_i -= 1;
                // 1: -1 >= 0 || false
                // 2: 63 >= 48
                // 2: we break and transfer bin_i = 63
                if bin_i >= bound || finishing {
                    advance_index = false;
                    break;
                }
                // `transfer_index` starts equaling table length
                // 1: next_bin_i = 64
                let next_bin_i = self.transfer_index.load(Ordering::SeqCst);
                // TODO: why is `next_i = 0` true here?
                if next_bin_i <= 0 {
                    bin_i -= 1;
                    advance_index = false;
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
                    // Check during tests if it's a bug.
                    bin_i = next_bin_i;
                    advance_index = false;
                    break;
                }
            }
            // 1: bin_i = 64
            if bin_i < 0 || bin_i >= len || bin_i + len >= next_len {
                // the resize has finished
                if finishing {
                    // this branch is only taken for one thread partaking in the resize
                    self.next_table.store(Shared::null(), Ordering::SeqCst);
                    // TODO: deal with garbage
                    let garbage = self.table.swap(next_table, Ordering::SeqCst, guard);
                    // len = 64
                    // len << 1 = 64 × 2 = 128 (new table size)
                    // len >> 1 = 64 ÷ 2 = 32 (half of old size)
                    // control_size = 128 - 32 = 96 (load factor = 0.75)
                    self.size_control
                        .store((len << 1) - (len >> 1), Ordering::SeqCst);
                    return;
                }
                let size_control = self.size_control.load(Ordering::SeqCst);
                if let Ok(_) = self.size_control.compare_exchange(
                    size_control,
                    size_control - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    if (size_control - 2) != Self::resize_stamp(len as usize) << RESIZE_STAMP_SHIFT
                    {
                        return;
                    }
                    // resizing
                    finishing = true;
                    // check if we can assist with any subsequent resize
                    advance_index = true;
                    bin_i = len;
                }
                continue;
            }
            let bin = table_ref.get_by_index(bin_i as usize, guard);
            if bin.is_null() {
                advance_index = table_ref
                    .compare_and_swap(
                        bin_i as usize,
                        Shared::null(),
                        Owned::new(Bin::Moved(next_table.as_raw())),
                        guard,
                    )
                    .is_ok();
                continue;
            }
            let bin_ref = unsafe { bin.deref() };
            match bin_ref {
                Bin::Node(head) => {
                    let head = Shared::from(head as *const _);
                    // bin is not empty, need to link to it, so we must take the lock
                    let _guard = head.mu.lock();
                    // need to check if it's still the head
                    let current_head = table_ref.get_by_index(bin_i as usize, guard);
                    if current_head.as_raw() != bin.as_raw() {
                        continue;
                    }
                    // It's still the head so we can "own" the bin.
                    // There can still be readers in the bin.
                    // Every second bin is moved during resize!
                    // Optimization only helps with the trailing sequence!
                    let mut sequence_bit = head.hash & len as u64;
                    // first node in a sequence of nodes which are moved
                    // to new indices or a sequence of nodes which stay
                    // at the same indices
                    let mut sequence_node = head;
                    // current node during bin traversal
                    let mut node = head;
                    while !node.next.is_null() {
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
                        let index_change_bit = node.hash & len as u64;
                        if index_change_bit != sequence_bit {
                            sequence_bit = index_change_bit;
                            sequence_node = node;
                        }
                        node = node.next.load(Ordering::SeqCst, guard);
                    }
                    // split bin in two
                    let mut unchanged_index_bin = Shared::null();
                    let mut changed_index_bin = Shared::null();
                    if sequence_bit == 0 {
                        unchanged_index_bin = sequence_node;
                    } else {
                        changed_index_bin = sequence_node;
                    }
                    node = head;
                    // no need to proceed past trailing sequence node,
                    // because next nodes after it don't change bins
                    while node != sequence_node {
                        let link = if node.hash & len as u64 == 0 {
                            // to the unchanged index bin
                            &mut unchanged_index_bin
                        } else {
                            // to the changed index bin
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
                            //  maintaining a tail pointer
                            //  additional checks
                            //  more complex code
                            next: Atomic::from(*link),
                            mu: Mutex::new(()), // TODO: not sure if it's correct
                        }))
                        .into_shared(guard);
                        node = node.next.load(Ordering::SeqCst, guard);
                    }
                    next_table_ref.store_bin(bin_i as usize, unchanged_index_bin);
                    next_table_ref.store_bin((bin_i + len) as usize, changed_index_bin);
                    // "link" to the value moved to the new table
                    table_ref.store_bin(bin_i as usize, Owned::new(Bin::Moved(next_table.as_raw())));
                    advance_index = true;
                }
                Bin::Moved(_) => {
                    // already processed
                    advance_index = true;
                    continue;
                }
            }
        }
    }
}
