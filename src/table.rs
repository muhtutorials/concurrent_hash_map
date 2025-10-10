use crate::node::Bin;
use crossbeam::epoch::{Atomic, CompareExchangeError, Guard, Owned, Pointer, Shared};
use std::sync::atomic::Ordering;

pub(crate) struct Table<K, V> {
    pub(crate) bins: Box<[Atomic<Bin<K, V>>]>,
}

impl<K, V> Table<K, V> {
    pub(crate) fn new(len: usize) -> Self {
        let bins = vec![Atomic::null(); len];
        Self {
            bins: bins.into_boxed_slice(),
        }
    }

    pub(crate) fn bin_index(&self, hash: u64) -> usize {
        // hash = 0b10101001
        // len = 4 = 0b100
        // mask = 4 - 1 = 0b11
        // hash & mask = 0b...01 & 0b11 = 0b01
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
            // ordering?
            Ordering::SeqCst,
            Ordering::SeqCst,
            guard,
        )
    }

    pub(crate) fn store_bin(&self, bin_i: usize, new: Owned<Bin<K, V>>) {
        self.bins[bin_i].store(new, Ordering::Release);
    }
}
