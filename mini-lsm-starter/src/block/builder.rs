use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::with_capacity(block_size),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len: u16 = key.len() as u16;
        let value_len: u16 = value.len() as u16;
        let entry_len = 2 + key_len + 2 + value_len;

        if !self.is_empty() && self.data.len() + entry_len as usize + 2 + 4 > self.block_size {
            return false;
        }

        if self.is_empty() {
            self.first_key = key.clone().to_key_vec();
        }

        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&key_len.to_ne_bytes());
        self.data.extend_from_slice(key.into_inner());
        self.data.extend_from_slice(&value_len.to_ne_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
