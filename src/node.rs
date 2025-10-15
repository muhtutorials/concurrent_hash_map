use crate::table::Table;
use crossbeam::epoch::{Atomic, Guard, Shared, pin};
use parking_lot::Mutex;
use std::hash::Hash;
use std::sync::atomic::Ordering;

// if bin is empty we add a node atomically,
// otherwise we take the lock and add another bin.
pub(crate) enum Bin<K, V> {
    Node(Node<K, V>),
    Moved(*const Table<K, V>),
}

impl<K: Hash + Eq, V> Bin<K, V> {
    pub(crate) fn find<'g>(&self, hash: u64, key: &K, guard: &Guard) -> Shared<'g, Bin<K, V>> {
        match *self {
            Bin::Node(_) => {
                let mut bin = self;
                loop {
                    let node = bin.as_node().unwrap();
                    if node.hash == hash && &node.key == key {
                        break Shared::from(self as *const _);
                    }
                    let next = node.next.load(Ordering::SeqCst, guard);
                    if next.is_null() {
                        break Shared::null();
                    }
                    // SAFETY: `next` will only be dropped, if we are dropped.
                    // We won't be dropped until epoch passes, which is protected by guard.
                    bin = unsafe { next.deref() };
                }

            }
            Bin::Moved(next_table) => {
                let mut table = unsafe { &*next_table };
                // SAFETY: We have a reference to the old table, otherwise we wouldn't have
                // a reference to `self`. We got that under the given guard. Since we haven't
                // dropped that guard, this table hasn't been garbage collected
                // so hasn't the next table.
                loop {
                    if table.bins.is_empty() {
                        return Shared::null();
                    }
                    let guard = &pin();
                    let bin = table.get_by_hash(hash, guard);
                    if bin.is_null() {
                        return Shared::null();
                    }
                    // SAFETY: the table is protected by the guard and so is the bin
                    let bin = unsafe { bin.deref() };
                    match *bin {
                        Bin::Node(_) => break bin.find(hash, key, guard),
                        Bin::Moved(next_table) => {
                            // SAFETY: same as above
                            table = unsafe { &*next_table };
                            continue;
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn as_node(&self) -> Option<&Node<K, V>> {
        if let Bin::Node(node) = self {
            Some(node)
        } else {
            None
        }
    }
}

pub(crate) struct Node<K, V> {
    pub(crate) hash: u64,
    pub(crate) key: K,
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<Bin<K, V>>,
    // if bin is not empty we take the lock and add a node
    pub(crate) mu: Mutex<()>,
}
