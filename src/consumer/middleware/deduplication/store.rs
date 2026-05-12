//! Deduplication store traits.
//!
//! Abstracts over storage backends for checking and recording processed message
//! identifiers.

use crate::{Partition, Topic};
use std::error::Error;
use std::future::Future;
use uuid::Uuid;

/// Storage backend for deduplication identifiers.
///
/// Each implementation stores UUIDs representing processed messages and
/// provides existence checks. Reads/writes are best-effort — callers handle
/// failures gracefully.
pub trait DeduplicationStore: Clone + Send + Sync + 'static {
    /// Error type for store operations.
    type Error: Error + Send + Sync + 'static;

    /// Checks whether a deduplication identifier has already been recorded.
    fn exists(&self, id: Uuid) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Records a deduplication identifier.
    fn insert(&self, id: Uuid) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Factory for creating per-partition [`DeduplicationStore`] instances.
pub trait DeduplicationStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    type Store: DeduplicationStore;

    /// Creates a store instance. The `topic`, `partition`, and
    /// `consumer_group` parameters are available for scoping but may be
    /// unused by implementations that use a global table.
    fn create_store(&self, topic: Topic, partition: Partition, consumer_group: &str)
    -> Self::Store;
}
