//! Standalone read-only access to published keyed state.
//!
//! A sibling of [`producer`](crate::producer) and
//! [`consumer`](crate::consumer), constructed directly from the stores and
//! loader it needs. The reader proper lands in a later change; this shell
//! currently carries only the key→partition routing primitive shared with the
//! publication store.

mod partitioner;

pub use partitioner::{EmptyKeyError, PartitionCount, PartitionCountError, partition_for_key};
