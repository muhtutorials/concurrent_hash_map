use crate::node::Bin;
use crossbeam::epoch::{Atomic, CompareExchangeError, Guard, Pointer, Shared, unprotected};
use std::sync::atomic::Ordering;

pub(crate) struct Table<K, V> {
    pub(crate) bins: Box<[Atomic<Bin<K, V>>]>,
}

impl<K, V> Table<K, V> {
    pub(crate) fn new(cap: usize) -> Self {
        let bins = vec![Atomic::null(); cap];
        Self {
            bins: bins.into_boxed_slice(),
        }
    }

    pub(crate) fn bin_index(&self, hash: u64) -> usize {
        // hash = 0b1010_1001
        // len = 4 = 0b0000_0100
        // mask = 4 - 1 = 0b0000_0011
        // index = hash & mask
        // index = 0b1010_1001 & 0b0000_0011 = 0b0000_0001 = 1
        let mask = self.bins.len() as u64 - 1;
        (hash & mask) as usize
    }

    pub(crate) fn get_by_index<'g>(&self, bin_i: usize, guard: &'g Guard) -> Shared<'g, Bin<K, V>> {
        self.bins[bin_i].load(Ordering::Acquire, guard)
    }

    pub(crate) fn get_by_hash<'g>(&self, hash: u64, guard: &'g Guard) -> Shared<'g, Bin<K, V>> {
        let bin_i = self.bin_index(hash);
        self.get_by_index(bin_i, guard)
    }

    pub(crate) fn compare_and_swap<'g, P>(
        &self,
        bin_i: usize,
        current: Shared<Bin<K, V>>,
        new: P,
        guard: &'g Guard,
    ) -> Result<Shared<'g, Bin<K, V>>, CompareExchangeError<'g, Bin<K, V>, P>>
    where
        P: Pointer<Bin<K, V>>,
    {
        self.bins[bin_i].compare_exchange(
            current,
            new,
            Ordering::AcqRel,
            Ordering::AcqRel,
            guard,
        )
    }

    pub(crate) fn store_bin<P>(&self, bin_i: usize, new: P)
    where
        P: Pointer<Bin<K, V>>,
    {
        self.bins[bin_i].store(new, Ordering::Release);
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // We need to drop any forwarding nodes, since they are heap allocated.
        // SAFETY: no one else is accessing this table anymore, so we own its contents.
        let guard = unsafe { unprotected() };
        for bin in &mut self.bins {
            let bin = bin.swap(Shared::null(), Ordering::SeqCst, guard);
            if bin.is_null() {
                continue;
            }
            // SAFETY: we have access to `&mut self`, so no one else
            // will drop this value under us
            let bin = unsafe { bin.into_owned() };
            if let Bin::Moved(_) = *bin {
            } else {
                unreachable!("dropped table with non empty bin")
            }
        }
    }
}
