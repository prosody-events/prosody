//! Standalone access to published committed state.
//!
//! A [`StateReader`] reads another consumer group's collection without
//! partition ownership. [`CommittedCellSource`] supplies value and presence
//! projections through collection evidence. Committed clears restrict results
//! to their frozen survivors. Reads do not change durable state.
//!
//! Each operation captures the publication sources and selects at most one
//! source. Point, batch, and range reads can select that source.
//! Later reads in the operation retain the selection, including after an absent
//! result or an error. [`ReadSession`] owns this selection contract.
//!
//! Construct a [`StateReaderClient`] from one [`StateReaderDependencies`]
//! bundle. The client shares its stores, loader, heartbeat registry, and
//! bounded cache across collection readers. The cache stores values, presence,
//! and absence under one projection interface.

mod backend;
mod cache;
mod client;
mod deps;
mod error;
mod partitioner;
mod publication_cache;
mod reader;
mod session;
mod source;

#[cfg(test)]
pub(crate) mod tests;

pub(crate) use backend::ConsumerReaderBackend;
pub use backend::{
    CassandraReaderBackend, CellSource, CommittedCellSource, MemoryReaderBackend, ReaderBackend,
};
pub use client::{CassandraStateReaderClient, StateReaderClient};
pub use deps::StateReaderDependencies;
pub use error::StateReaderError;
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
