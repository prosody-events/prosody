//! Reproduces librdkafka's default `consistent_random` partitioner for the
//! reader's key→partition lookup, and the [`PartitionCount`] newtype that makes
//! an invalid partition count unrepresentable past decode.

use std::num::NonZeroU32;

use crate::Partition;
use crate::error::{ClassifyError, ErrorCategory};

/// A Kafka topic's partition count, guaranteed to be in `[1, i32::MAX]`.
///
/// Minted only via [`TryFrom<i32>`] — the publication-row decode boundary —
/// so zero, negative, and oversized counts cannot exist past decode. The `i32`
/// source domain caps the value at `i32::MAX` inherently; no runtime upper
/// bound is checked because a larger value is unrepresentable in the input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionCount(NonZeroU32);

impl TryFrom<i32> for PartitionCount {
    type Error = PartitionCountError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let count = u32::try_from(value).map_err(|_| PartitionCountError::NonPositive(value))?;
        NonZeroU32::new(count)
            .map(Self)
            .ok_or(PartitionCountError::NonPositive(value))
    }
}

impl From<PartitionCount> for i32 {
    fn from(count: PartitionCount) -> Self {
        // Invariant: inner value is in [1, i32::MAX], so the cast is lossless.
        count.0.get() as i32
    }
}

/// Computes the partition librdkafka's `consistent_random` partitioner assigns
/// to a non-empty `key`: `crc32(key) % count`.
///
/// librdkafka randomizes empty/NULL keys, so an empty key has no reproducible
/// partition and is rejected with [`EmptyKeyError`]. The key bytes are hashed
/// directly with no intermediate allocation.
///
/// # Errors
///
/// Returns [`EmptyKeyError`] when `key` is empty.
pub fn partition_for_key(key: &[u8], count: PartitionCount) -> Result<Partition, EmptyKeyError> {
    if key.is_empty() {
        return Err(EmptyKeyError);
    }
    // 0 <= remainder < count <= i32::MAX, so the cast is lossless and non-negative.
    Ok((crc32fast::hash(key) % count.0.get()) as Partition)
}

/// The key was empty; librdkafka randomizes empty/NULL keys, so no
/// deterministic partition exists.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("cannot compute a partition for an empty key")]
pub struct EmptyKeyError;

impl ClassifyError for EmptyKeyError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

/// A partition count decoded from a publication row was not in `[1, i32::MAX]`.
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum PartitionCountError {
    /// The decoded count was zero or negative.
    #[error("partition count must be positive, got {0}")]
    NonPositive(i32),
}

impl ClassifyError for PartitionCountError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

#[cfg(test)]
mod tests;
