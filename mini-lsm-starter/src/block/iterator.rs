#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

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
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut it = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        let key_len = u16::from_ne_bytes(it.block.data[0..2].try_into().unwrap()) as usize;
        let key = it.block.data[2..2 + key_len].to_vec();
        it.key = KeyVec::from_vec(key);
        it.first_key = it.key.clone();
        it
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self::new(block)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut it = Self::new(block);
        it.seek_to_key(key);
        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");

        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");

        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.key = self.first_key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.idx = self.block.offsets.len();
            return;
        }
        self.idx += 1;
        let key_offset = self.block.offsets[self.idx] as usize;
        let key_len = u16::from_ne_bytes(
            self.block.data[key_offset..key_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        println!(
            "after next, idx: {}, key_offset: {}, key_len: {}",
            self.idx, key_offset, key_len
        );
        let key = self.block.data[key_offset + 2..key_offset + 2 + key_len].to_vec();
        self.key = KeyVec::from_vec(key);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted ASC when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_idx(mid);
            if self.key.as_key_slice() < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
    }

    fn seek_to_idx(&mut self, offset: usize) {
        let key_len =
            u16::from_ne_bytes(self.block.data[offset..offset + 2].try_into().unwrap()) as usize;
        let key = self.block.data[offset + 2..offset + 2 + key_len].to_vec();
        self.key = KeyVec::from_vec(key);
        let value_offset = offset + 2 + key_len;
        let value_len = u16::from_ne_bytes(
            self.block.data[value_offset..value_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        self.value_range = (value_offset + 2, value_offset + 2 + value_len);
    }
}
