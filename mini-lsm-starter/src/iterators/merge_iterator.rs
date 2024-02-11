#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

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
    /// A min heap.
    /// Invariant: all iters are valid.
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .enumerate()
            .filter(|(_i, it)| it.is_valid())
            .map(|(i, it)| HeapWrapper(i, it))
            .collect();
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let current = self.current.as_ref().unwrap();
        current.1.key()
    }

    fn value(&self) -> &[u8] {
        let current = self.current.as_ref().unwrap();
        current.1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|it| it.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();

        // skip all keys equal to current key
        while let Some(mut head) = self.iters.peek_mut() {
            if head.1.key() == current.1.key() {
                if let e @ Err(_) = head.1.next() {
                    // Now `head` is in bad state.
                    // If we don't pop here. Drop PeekMut will access its `key()`, which
                    // is kind of undefined behavior.
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
