pub mod concat_iterator;
pub mod merge_iterator;
pub mod two_merge_iterator;

/// This iterator interface is not aware of the business logic of LSM:
/// - It might or might not be aware of tombstones.
/// - It might or might not be aware duplicated keys (multiple versions).
///
/// [LsmIterator](crate::lsm_iterator::LsmIterator) handles these logics, other iterators can ignore the logic.
pub trait StorageIterator {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    /// Get the current value. Tombsto  nes might be returned or skipped according to the implementation.
    fn value(&self) -> &[u8];

    /// Get the current key. Keys should never be empty. Implementations might use empty keys to indicate invalid state.
    fn key(&self) -> Self::KeyType<'_>;

    /// Check if the current iterator is valid. It means current item is present.
    /// Should check before calling `key()` and `value()`.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    ///
    /// Invariant: `next()` should be called if `is_valid()` returns true.
    ///
    /// If `next` returns an error (i.e., due to disk failure, network failure, checksum error, etc.),
    /// the iterator is in an undefined state, and any methods should not be called.
    /// Note: `FusedIterator` prevents undefined behavior.
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }
}
