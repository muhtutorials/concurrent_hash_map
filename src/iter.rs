use crate::node::{Bin, Node};
use crate::table::Table;
use crossbeam::epoch::{Guard, Shared};
use std::hash::Hash;
use std::sync::atomic::Ordering;

struct Iter<'g, K, V> {
    /// current table; updated if resized
    table: Option<&'g Table<K, V>>,
    /// the last node iterated over
    prev: Option<&'g Node<K, V>>,
    /// Current bin index. Could be in any table during resize.
    index: usize,
    /// bin index in initial table
    base_index: usize,
    /// initial table capacity
    base_cap: usize,
    stack: Option<Box<TableStack<'g, K, V>>>,
    // for stack reuse to avoid new allocations
    spare: Option<Box<TableStack<'g, K, V>>>,
    guard: &'g Guard,
}

impl<'g, K, V> Iter<'g, K, V> {
    fn new(table: Shared<'g, Table<K, V>>, guard: &'g Guard) -> Self {
        let (table, len) = if table.is_null() {
            (None, 0)
        } else {
            // SAFETY: map guarantees that a table read under a guard
            // is never dropped before guard is dropped
            let table_ref = unsafe { table.deref() };
            (Some(table_ref), table_ref.bins.len())
        };
        Self {
            table,
            prev: None,
            index: 0,
            base_index: 0,
            base_cap: len,
            stack: None,
            spare: None,
            guard,
        }
    }

    fn push_state(&mut self, table: &'g Table<K, V>, len: usize, index: usize) {
        let mut spare = self.spare.take();
        if let Some(ref mut stack) = spare {
            self.spare = stack.next.take();
        }
        let new_stack = TableStack {
            table,
            len,
            index,
            next: self.stack.take(),
        };
        self.stack = if let Some(mut stack) = spare {
            *stack = new_stack;
            Some(stack)
        } else {
            Some(Box::new(new_stack))
        };
    }

    fn recover_state(&mut self, mut len: usize) {
        while let Some(ref mut stack) = self.stack {
            if self.index + stack.len < len {
                // if we haven't checked the high part of this bucket,
                // then don't pop the stack, and instead move on to that bin.
                self.index += stack.len;
                break;
            }
            // popping stack
            let mut stack = self.stack.take().expect("while let Some");
            len = stack.len;
            self.table = Some(stack.table);
            self.stack = stack.next.take();
            self.index = stack.index;
            // save stack frame for reuse
            stack.next = self.spare.take();
            self.spare = Some(stack);
        }
        if self.stack.is_none() {
            // move to next "part" of the top-level bin
            // in the largest table
            self.index += self.base_cap;
            if self.index >= len {
                // we've gone past the last part of this top-level bin,
                // so move to the next top-level bin
                self.base_index += 1;
                self.index = self.base_index;
            }
        }
    }
}

impl<'g, K: Hash + Eq, V> Iterator for Iter<'g, K, V> {
    type Item = &'g Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut node = None;
        // get next node in a bin
        if let Some(prev) = self.prev {
            let next = prev.next.load(Ordering::SeqCst, self.guard);
            if !next.is_null() {
                let next = unsafe { next.deref() }
                    .as_node()
                    .expect("only nodes follow node");
                node = Some(next)
            }
        }
        loop {
            if node.is_some() {
                self.prev = node;
                return node;
            }
            if self.table.is_none() || self.table.unwrap().bins.len() <= self.index {
                self.prev = None;
                return None;
            }
            let table = self.table.expect("`is_none` in if statement above");
            let len = table.bins.len();
            let bin = table.get_by_index(self.index, self.guard);
            if !bin.is_null() {
                match unsafe { bin.deref() } {
                    Bin::Node(n) => {
                        node = Some(n);
                    }
                    Bin::Moved(next_table) => {
                        self.table = Some(unsafe { &**next_table });
                        self.prev = None;
                        self.push_state(table, len, self.index);
                        continue;
                    }
                }
            }
            if self.stack.is_some() {
                self.recover_state(len);
            } else {
                self.index = self.index + self.base_cap;
                // visit upper slots if present
                if self.index >= len {
                    self.base_index += 1;
                    self.index = self.base_index;
                }
            }
        }
    }
}

// Records the table, its length, and current traversal index for a
// traverser that must process a region of a forwarded table before
// proceeding with current table.
struct TableStack<'g, K, V> {
    table: &'g Table<K, V>,
    len: usize,
    index: usize,
    next: Option<Box<TableStack<'g, K, V>>>,
}

#[cfg(test)]
mod tests {
    use crate::iter::Iter;
    use crate::table::Table;
    use crossbeam::epoch::{Owned, Shared, pin};

    #[test]
    fn iter_new() {
        let guard = &pin();
        let iter = Iter::<usize, usize>::new(Shared::null(), guard);
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn iter_empty() {
        let table = Owned::new(Table::<usize, usize>::new(16));
        let guard = &pin();
        let table = table.into_shared(guard);
        let iter = Iter::new(table, guard);
        assert_eq!(iter.count(), 0);
        // SAFETY: nothing holds on to references into the table anymore
        let _ = unsafe { table.into_owned() };
    }
}
