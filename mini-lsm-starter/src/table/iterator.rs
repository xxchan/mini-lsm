#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (iter, blk_idx) = Self::seek_to_first_inner(table.clone())?;
        let iter = Self {
            table,
            blk_iter: iter,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (iter, blk_idx) = Self::seek_to_first_inner(self.table.clone())?;
        self.blk_iter = iter;
        self.blk_idx = blk_idx;
        Ok(())
    }

    fn seek_to_first_inner(table: Arc<SsTable>) -> Result<(BlockIterator, usize)> {
        let block = table.read_block(0)?;
        let iter = BlockIterator::create_and_seek_to_first(block);
        Ok((iter, 0))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (iter, blk_idx) = Self::seek_to_key_inner(table.clone(), key)?;
        let iter = Self {
            table,
            blk_iter: iter,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (iter, blk_idx) = Self::seek_to_key_inner(self.table.clone(), key)?;
        self.blk_iter = iter;
        self.blk_idx = blk_idx;
        Ok(())
    }

    fn seek_to_key_inner(table: Arc<SsTable>, key: KeySlice) -> Result<(BlockIterator, usize)> {
        // search the first block s.t. key < block.first_key
        // invariant: [0, lo): block.first_key <= key, [hi, n): block.first_key > key
        let mut lo = 0;
        let mut hi = table.block_meta.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if key >= table.block_meta[mid].first_key.as_key_slice() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == table.block_meta.len() {
            // not found
            // TODO: optimize this invalid iter
            let iter = BlockIterator::create_and_seek_to_key(
                table.read_block(table.block_meta.len() - 1)?,
                key,
            );
            Ok((iter, table.block_meta.len() - 1))
        } else if lo > 0 && key <= table.block_meta[lo - 1].last_key.as_key_slice() {
            // in prev block
            let block = table.read_block(lo - 1)?;
            let iter = BlockIterator::create_and_seek_to_key(block, key);
            return Ok((iter, lo - 1));
        } else {
            // in current block
            let block = table.read_block(lo)?;
            let iter = BlockIterator::create_and_seek_to_first(block);
            return Ok((iter, lo));
        }
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        debug_assert!(self.is_valid());
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            // move to next block
            if self.blk_idx == self.table.block_meta.len() - 1 {
                return Ok(());
            }
            self.blk_idx += 1;
            let block = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
