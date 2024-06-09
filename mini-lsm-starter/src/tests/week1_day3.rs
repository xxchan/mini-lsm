use std::sync::Arc;

use bytes::Bytes;

use crate::{
    block::{Block, BlockBuilder, BlockIterator},
    key::{KeySlice, KeyVec}, tests::harness::init_logging,
};

#[test]
fn test_block_build_single_key() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"233"), b"233333"));
    builder.build();
}

#[test]
fn test_block_build_full() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11"));
    assert!(!builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22"));
    builder.build();
}

#[test]
fn test_block_build_large_1() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(
        KeySlice::for_testing_from_slice_no_ts(b"11"),
        &b"1".repeat(100)
    ));
    builder.build();
}

#[test]
fn test_block_build_large_2() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"1"));
    assert!(!builder.add(
        KeySlice::for_testing_from_slice_no_ts(b"11"),
        &b"1".repeat(100)
    ));
}

/// key_000, key_005, key_010, ...
fn key_of(idx: usize) -> KeyVec {
    KeyVec::for_testing_from_vec_no_ts(format!("key_{:03}", idx * 5).into_bytes())
}

/// value_000, value_001, ...
fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_block() -> Block {
    let mut builder = BlockBuilder::new(10000);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(key.as_key_slice(), &value[..]));
    }
    builder.build()
}

#[test]
fn test_block_build_all() {
    generate_block();
}

#[test]
fn test_block_encode() {
    let block = generate_block();
    block.encode();
}

#[test]
fn test_block_decode() {
    let block = generate_block();
    let encoded = block.encode();
    let decoded_block = Block::decode(&encoded);
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_block_iterator() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_first(block);
    for _ in 0..5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key.for_testing_key_ref(),
                key_of(i).for_testing_key_ref(),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(key_of(i).for_testing_key_ref()),
                as_bytes(key.for_testing_key_ref())
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next();
        }
        iter.seek_to_first();
    }
}

#[test]
fn test_block_seek_key() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_key(block, key_of(0).as_key_slice());

    for offset in 1..=5 {
        for i in 0..num_of_keys() {
            // in each iteration, we seek to {i * 5 + offset}

            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key.for_testing_key_ref(),
                key_of(i).for_testing_key_ref(),
                "[offset: {offset}, i: {i}] expected key: {:?}, actual key: {:?}",
                as_bytes(key_of(i).for_testing_key_ref()),
                as_bytes(key.for_testing_key_ref())
            );
            assert_eq!(
                value,
                value_of(i),
                "[offset: {offset}, i: {i}] expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            tracing::info!("[offset: {offset}, i: {i}], {}", iter.print_current_entry());
            let target_key_str = format!("key_{:03}", i * 5 + offset).into_bytes();
            let target_key = KeySlice::for_testing_from_slice_no_ts(&target_key_str);
            tracing::info!("seeking to key: {:?}", target_key.for_testing_debug());
            iter.seek_to_key(target_key);
        }
        iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(b"k"));
    }
}

#[test]
fn test_block_seek_nonexistent() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_first(block);

    iter.seek_to_key(KeySlice::for_testing_from_slice_no_ts(b"key_999"));
    assert_eq!(iter.is_valid(), false);
}

#[test]
#[should_panic]
fn test_malicious_block() {
    Block::decode(&[]);
}

