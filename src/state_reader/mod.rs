//! Standalone read-only access to published keyed state.
//!
//! A sibling of [`producer`](crate::producer) and
//! [`consumer`](crate::consumer): a [`StateReader`] observes a collection's
//! **committed** state from another consumer group, without owning the
//! partition or running the write machinery. Every value it returns is
//! [`Cell::project_committed`](crate::state::cell::Cell::project_committed) —
//! never an in-flight provisional value, never owner-side repair — read from at
//! most one publication source per logical operation (probe-and-pin).
//!
//! Construct one from a [`SharedDeps`] bundle with [`StateReader::new`]: build
//! the bundle once (its stores, loader, and byte-budgeted cache are shared) and
//! mint a reader per collection.

mod cache;
mod deps;
mod error;
mod loader;
mod partitioner;
mod reader;
mod session;
mod source;
mod stores;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use deps::DEFAULT_READER_CACHE_SIZE_BYTES;
pub use deps::SharedDeps;
pub use error::StateReaderError;
pub use loader::{ReaderLoader, ReaderLoaderError};
// Deliberately public: `partition_for_key` is the librdkafka-compatible routing
// primitive the reader ecosystem (and the cross-language clients) must compute
// the same as the producer's pinned partitioner — cross-checked against live
// Kafka in `tests/partitioner.rs`. Its error `EmptyKeyError` and
// `PartitionCount` with its `TryFrom` error `PartitionCountError` are therefore
// public too (they sit in that public signature and the public
// `PartitionCount::try_from`).
pub use partitioner::{EmptyKeyError, PartitionCount, PartitionCountError, partition_for_key};
pub use reader::StateReader;
pub use session::ReadSession;
