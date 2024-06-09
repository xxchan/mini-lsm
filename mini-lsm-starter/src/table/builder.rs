use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    /// Q: what if this is too big? Can we put the whole SST in memory?
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            // build the current block
            self.finish_block();
            // add to the new block
            let success = self.builder.add(key, value);
            assert!(success);

            self.first_key.set_from_slice(key);
            self.last_key.set_from_slice(key);
        } else {
            if self.first_key.is_empty() {
                self.first_key.set_from_slice(key);
            }
            // We will clone the key everytime. Is this reasonable?
            self.last_key.set_from_slice(key);
        }
    }

    /// Get the estimated size of the SSTable. so that the caller can know when can it start a new SST to write data.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.finish_block();
        }
        let Self {
            builder,
            first_key,
            last_key,
            mut data,
            meta,
            block_size: _,
        } = self;
        debug_assert!(builder.is_empty());
        debug_assert!(first_key.is_empty());
        debug_assert!(last_key.is_empty());
        debug_assert!(!data.is_empty());
        debug_assert!(!meta.is_empty());

        let block_meta_offset = data.len();
        let first_key = meta[0].first_key.clone();
        let last_key = meta[meta.len() - 1].last_key.clone();
        debug_assert!(!first_key.is_empty());
        debug_assert!(!last_key.is_empty());
        BlockMeta::encode_block_meta(&meta, &mut data);
        let file = FileObject::create(path.as_ref(), data)?;

        let sst = SsTable {
            file,
            block_meta: meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            // TODO
            bloom: None,
            // TODO
            max_ts: 0,
        };
        Ok(sst)
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let first_key = std::mem::replace(&mut self.first_key, KeyVec::new());
        let last_key = std::mem::replace(&mut self.last_key, KeyVec::new());
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: first_key.into_key_bytes(),
            last_key: last_key.into_key_bytes(),
        });
        self.data.extend_from_slice(&builder.build().encode());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
