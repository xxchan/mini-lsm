#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf: Vec<u8> = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 8);
        buf.extend(self.data.clone());
        for offset in &self.offsets {
            buf.extend(offset.to_ne_bytes().iter());
        }
        let num_of_elements = self.offsets.len();
        buf.extend(num_of_elements.to_ne_bytes().iter());
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements =
            u64::from_ne_bytes(data[data.len() - 8..data.len()].try_into().unwrap());

        let offset_bytes = (num_of_elements * 2) as usize;
        let offset_end = data.len() - 8;
        let offsets: Vec<u16> = {
            let offsets = &data[offset_end - offset_bytes..offset_end];
            // note: Transmute is correct iff we use native endianness. This isn't a good practice. We should iterate 2 bytes at a time.
            let (head, body, tail) = unsafe { offsets.align_to::<u16>() };

            // This example simply does not handle the case where the input data
            // is misaligned such that there are bytes that cannot be correctly
            // reinterpreted as u16.
            assert!(head.is_empty());
            assert!(tail.is_empty());
            body.to_vec()
        };
        let data = data[0..offset_end - offset_bytes].to_vec();
        Self { data, offsets }
    }
}
