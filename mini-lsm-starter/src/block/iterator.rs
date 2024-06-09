use std::sync::Arc;

use bytes::{Buf, Bytes};

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZE_OF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    /// the value range from the block `(value_start, value_end)`
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block (Why do we need this?)
    first_key: KeyVec,
}

impl BlockIterator {
    /// It is still invalid after `new`. Must seek before using it.
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            // placeholder data below
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut it = Self::new(block);
        it.seek_to_first();
        it
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut it = Self::new(block);
        it.seek_to_key(key);
        it
    }

    #[track_caller]
    fn debug_assert_iterator_valid(&self) {
        debug_assert!(
            !self.key.is_empty(),
            "key is empty, {}",
            self.print_current_entry()
        );
        debug_assert!(self.idx < self.block.num_elements());
        debug_assert!(self.value_range.0 < self.value_range.1);
        debug_assert!(self.value_range.1 <= self.block.size());
    }

    pub fn print_iter(&mut self) {
        let current_idx = self.idx;
        while self.is_valid() {
            tracing::info!("{}", self.print_current_entry());
            self.next();
        }
        self.seek_to_idx(current_idx);
    }

    #[must_use]
    pub fn print_current_entry(&self) -> String {
        format!(
            "idx: {}, key: {:?}, value: {:?}",
            self.idx,
            self.key().for_testing_debug(),
            Bytes::copy_from_slice(self.value())
        )
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.debug_assert_iterator_valid();
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.debug_assert_iterator_valid();
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.debug_assert_iterator_valid();
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    #[tracing::instrument(level = "trace", skip(self),fields(idx=self.idx))]
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.num_elements() {
            // Invalid now
            self.key = KeyVec::new();
            self.idx = self.block.num_elements();
            return;
        }
        self.debug_assert_iterator_valid();
        self.idx += 1;
        let offset = self.block.offsets[self.idx] as usize;
        self.seek_to_byte_offset(offset)
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted ASC when being added by
    /// callers.
    #[tracing::instrument(level = "trace", skip_all, fields(key = ?key.for_testing_debug()))]
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_idx(mid);
            self.debug_assert_iterator_valid();
            if self.key.as_key_slice() < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.seek_to_idx(low);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn seek_to_idx(&mut self, idx: usize) {
        // debug_assert!(idx >= self.idx, "should not seek back, idx: {}, self.idx: {}", idx, self.idx);
        if idx >= self.block.num_elements() {
            self.key = KeyVec::new();
            self.idx = self.block.num_elements();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.idx = idx;
        self.seek_to_byte_offset(offset);
    }

    /// Note: This only updates `self.key` and `self.value_range`, but not `self.index`.
    fn seek_to_byte_offset(&mut self, offset: usize) {
        tracing::trace!(offset, "seeking to byte offset");

        let mut entry = &self.block.data[offset..];

        let key_len = entry.get_u16_le() as usize;
        let key = entry[..key_len].to_vec();
        self.key = KeyVec::from_vec(key);

        entry.advance(key_len);
        let value_len = entry.get_u16_le() as usize;
        let value_begin = offset + SIZE_OF_U16 + key_len + SIZE_OF_U16;
        self.value_range = (value_begin, value_begin + value_len);
    }
}
