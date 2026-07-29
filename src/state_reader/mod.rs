//! Standalone read-only access to published keyed state.
//!
//! A sibling of [`producer`](crate::producer) and
//! [`consumer`](crate::consumer): a [`StateReader`] observes a collection's
//! **committed** state from another consumer group, without owning the
//! partition or running the write machinery. Every value it returns comes from
//! [`Cell::project_committed`](crate::state::cell::Cell::project_committed):
//! never an in-flight provisional value, never owner-side repair. Each logical
//! operation reads from at most one publication source (probe-and-pin).
//!
//! Construct one from a [`SharedDeps`] bundle with [`StateReader::new`]. Build
//! the bundle once so its stores, loader, and byte-budgeted cache are shared,
//! then build a reader per collection.

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
pub(crate) use source::PUBLICATION_READ_LIMIT;
// `partition_for_key` is public on purpose. It is the librdkafka-compatible
// routing primitive. The reader ecosystem and the cross-language clients must
// route a key to the same partition the producer's partitioner would.
// `tests/partitioner.rs` cross-checks it against live Kafka. Its error
// `EmptyKeyError`, the `PartitionCount` argument, and
// `PartitionCount::try_from`'s error `PartitionCountError` are public for the
// same reason: they appear in that public signature and in the public
// `PartitionCount::try_from`.
pub use partitioner::{EmptyKeyError, PartitionCount, PartitionCountError, partition_for_key};
pub use reader::StateReader;
pub use session::ReadSession;
