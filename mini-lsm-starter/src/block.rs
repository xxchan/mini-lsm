mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::key::KeyVec;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
///
/// Format:
///
/// ```text
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
///
/// |                           Entry #1                            | ... |
/// -----------------------------------------------------------------------
/// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
/// -----------------------------------------------------------------------
/// ```
///
/// Blocks are usually of 4-KB size (the size may vary depending on the storage medium), which is equivalent to the page size in the operating system and the page size on an SSD.
///
/// - Key length and value length are both 2 bytes, which means their maximum lengths are 65535.
/// - `offset` is 2 bytes (max pointing to 64KB).
/// - `num_of_elements` is 2 bytes, which means the maximum number of elements in a block is 65535.

pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

const SIZE_OF_U16: usize = std::mem::size_of::<u16>();

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf: Vec<u8> =
            Vec::with_capacity(self.data.len() + self.offsets.len() * SIZE_OF_U16 + SIZE_OF_U16);
        buf.extend(self.data.clone());
        for offset in &self.offsets {
            buf.put_u16_le(*offset);
        }
        assert!(
            self.offsets.len() < u16::MAX as usize,
            "Too many elements in the block"
        );
        let num_of_elements: u16 = self.offsets.len() as u16;
        buf.extend(num_of_elements.to_le_bytes());
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let offset_end = data.len() - SIZE_OF_U16;

        let num_of_elements = (&data[offset_end..]).get_u16_le() as usize;

        let offset_bytes = num_of_elements * SIZE_OF_U16;
        let data_end = offset_end - offset_bytes;
        let offsets: Vec<u16> = {
            let offsets = &data[data_end..offset_end];
            offsets
                .chunks(SIZE_OF_U16)
                .map(|mut chunk| chunk.get_u16_le())
                .collect()

            // // note: Transmute is correct iff we use native endianness. This isn't a good practice. We should iterate 2 bytes at a time.
            // let (head, body, tail) = unsafe { offsets.align_to::<u16>() };

            // // This example simply does not handle the case where the input data
            // // is misaligned such that there are bytes that cannot be correctly
            // // reinterpreted as u16.
            // assert!(head.is_empty());
            // assert!(tail.is_empty());
            // body.to_vec()
        };
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }

    pub fn get_first_key(&self) -> KeyVec {
        let mut entry = self.data.as_slice();
        let key_len = entry.get_u16_le() as usize;
        KeyVec::from_vec(entry[..key_len].to_vec())
    }

    pub fn num_elements(&self) -> usize {
        self.offsets.len()
    }

    pub fn size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZE_OF_U16
    }
}
