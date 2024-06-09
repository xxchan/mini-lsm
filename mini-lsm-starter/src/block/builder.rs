use bytes::BufMut;

use crate::{
    block::SIZE_OF_U16,
    key::{KeySlice, KeyVec},
};

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

    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZE_OF_U16 + SIZE_OF_U16
    }

    fn current_num_elements(&self) -> usize {
        self.offsets.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        debug_assert!(
            self.current_num_elements() <= u16::MAX as usize,
            "cannot add more elements"
        );

        let key_len = key.len();
        let value_len = value.len();
        debug_assert!(
            key_len <= u16::MAX as usize && key_len > 0 && value_len <= u16::MAX as usize,
            "abnormal key or value length, key_len: {}, value_len: {}",
            key_len,
            value_len
        );

        let entry_len: usize = SIZE_OF_U16 + key_len + SIZE_OF_U16 + value_len;

        if self.is_empty() {
            // For the first entry, we do not check block size?
            self.first_key = key.to_key_vec();
        } else {
            // Otherwise, we need to check if the block is full.
            if self.current_size() + entry_len > self.block_size {
                return false;
            }
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16_le(key_len as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16_le(value_len as u16);
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    fn is_empty(&self) -> bool {
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
