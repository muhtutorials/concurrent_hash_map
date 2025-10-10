use crate::table::Table;
use crossbeam::epoch::{Atomic, Guard, Shared, pin};
use parking_lot::Mutex;
use std::hash::Hash;
use std::sync::atomic::Ordering;

// if a bin is empty we add a nod atomically,
// otherwise we take the lock and add another bin.
pub(crate) enum Bin<K, V> {
    Node(Node<K, V>),
    Moved(*const Table<K, V>),
}

impl<K: Hash + Eq, V> Bin<K, V> {
    pub(crate) fn find<'g>(&self, hash: u64, key: &K, guard: &Guard) -> Shared<'g, Node<K, V>> {
        match self {
            Bin::Node(node) => node.find(hash, key, guard),
            Bin::Moved(next_table) => {
                let mut table = Shared::from(*next_table);
                let table_ref = unsafe { table.deref() };
                loop {
                    if table.is_null() || table_ref.bins.is_empty() {
                        return Shared::null();
                    }
                    let guard = &pin();
                    let bin = table_ref.get_by_hash(hash, guard);
                    if bin.is_null() {
                        return Shared::null();
                    }
                    let bin_ref = unsafe { bin.deref() };
                    match bin_ref {
                        Bin::Node(node) => {
                            return node.find(hash, key, guard);
                        }
                        Bin::Moved(next_table) => {
                            table = Shared::from(*next_table);
                            continue;
                        }
                    }
                }
            }
        }
    }
}

pub(crate) struct Node<K, V> {
    pub(crate) key: K,
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<Node<K, V>>,
    pub(crate) mu: Mutex<()>,
    // if bin is not empty we take the lock and add a node
    pub(crate) hash: u64,
}

impl<K: Eq, V> Node<K, V> {
    pub(crate) fn find<'g>(&self, hash: u64, key: &K, guard: &Guard) -> Shared<'g, Node<K, V>> {
        if self.hash == hash && self.key == *key {
            return Shared::from(self as *const _);
        }
        let next = self.next.load(Ordering::SeqCst, guard);
        if next.is_null() {
            return Shared::null();
        }
        unsafe { next.deref() }.find(hash, key, guard)
    }
}
