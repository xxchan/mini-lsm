#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

/// This iterator skips tombstones, and skips old versions of the same key (handled by [`MergeIterator`]).
/// We only handles tomebstones here so that other iterators can be simpler.
pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let mut it = Self { inner: iter };
        if it.inner.is_valid() && it.value().is_empty() {
            it.go_to_next_nontombstone()?;
        }
        Ok(it)
    }

    fn go_to_next_nontombstone(&mut self) -> Result<()> {
        debug_assert!(self.inner.is_valid());
        loop {
            self.inner.next()?;
            if !self.inner.is_valid() {
                return Ok(());
            }
            if !self.inner.value().is_empty() {
                return Ok(());
            }
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.go_to_next_nontombstone()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid to provide extra safety (avoid calling any methods on an invalid iterator to prevent undefined behavior).
///
/// The behavior of `FusedIterator` when the inner iterator becomes invalid is as follows:
/// - `is_valid()` returns false.
/// - `next()` does nothing and returns an error.
/// - `key()` and `value()` will panic.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        (!self.has_errored) && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            panic!("The iterator has errored");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored {
            panic!("The iterator has errored");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("The iterator has errored");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
