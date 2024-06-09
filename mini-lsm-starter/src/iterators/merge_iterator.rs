#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

/// `(index, iterator)`. We use `index` to ensure an iterator with smaller index contains the latest data.
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        // reverse the order to make it a min heap
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    /// A min heap. (`BinaryHeap` is a max heap. We implement `PartialOrd` to reverse the order.)
    /// Invariant: all iters are valid.
    iters: BinaryHeap<HeapWrapper<I>>,
    /// `None` if the iterator is invalid.
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    /// Why using `Box<I>` here? Because `I` maybe very large and we will move it around in the heap.
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .enumerate()
            .filter(|(_i, it)| it.is_valid())
            .map(|(i, it)| HeapWrapper(i, it))
            .collect();
        debug_assert!(iters.iter().all(|it| it.1.is_valid()));
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        debug_assert!(self.is_valid());
        let current = self.current.as_ref().unwrap();
        let key = current.1.key();
        debug_assert!(!key.is_empty());
        key
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());
        let current = self.current.as_ref().unwrap();
        current.1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|it| it.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());
        let current = self.current.as_mut().unwrap();

        // skip all keys equal to current key
        while let Some(mut head) = self.iters.peek_mut() {
            debug_assert!(head.1.key() >= current.1.key());
            if head.1.key() == current.1.key() {
                // try advance head
                if let e @ Err(_) = head.1.next() {
                    // Error handling: Now `head` is in bad state.
                    // If we don't pop here, droping PeekMut will call `partical_cmp()` which accesses its `key()`.
                    // That is undefined behavior.
                    PeekMut::pop(head);
                    return e;
                }
                if !head.1.is_valid() {
                    PeekMut::pop(head);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if current.1.is_valid() {
            if let Some(mut head) = self.iters.peek_mut() {
                if *current < *head {
                    std::mem::swap(&mut *head, current);
                }
            }
        } else {
            self.current = self.iters.pop();
        }
        Ok(())
    }
}
