//! Deduplication store traits and cache wrapper.
//!
//! Abstracts over storage backends for checking and recording processed message
//! identifiers.

use crate::{Partition, Topic};
use quick_cache::sync::Cache;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
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

/// Write-through cache wrapping an inner [`DeduplicationStore`].
///
/// A single `Arc<Cache>` is shared across all partitions via
/// [`CachedDeduplicationStoreProvider`], so a cache hit on one partition
/// avoids a store round-trip on another.
#[derive(Clone, Debug)]
pub struct CachedDeduplicationStore<S> {
    pub(super) cache: Arc<Cache<Uuid, ()>>,
    pub(super) inner: S,
}

impl<S> CachedDeduplicationStore<S> {
    /// Creates a new cached store wrapping `inner` and sharing `cache`.
    #[must_use]
    pub fn new(cache: Arc<Cache<Uuid, ()>>, inner: S) -> Self {
        Self { cache, inner }
    }
}

impl<S: DeduplicationStore> DeduplicationStore for CachedDeduplicationStore<S> {
    type Error = S::Error;

    async fn exists(&self, id: Uuid) -> Result<bool, S::Error> {
        if self.cache.get(&id).is_some() {
            return Ok(true);
        }
        let hit = self.inner.exists(id).await?;
        if hit {
            self.cache.insert(id, ());
        }
        Ok(hit)
    }

    async fn insert(&self, id: Uuid) -> Result<(), S::Error> {
        self.inner.insert(id).await?;
        self.cache.insert(id, ());
        Ok(())
    }
}

/// Provider that wraps an inner [`DeduplicationStoreProvider`], sharing one
/// `Arc<Cache>` across all produced stores.
#[derive(Clone, Debug)]
pub struct CachedDeduplicationStoreProvider<P> {
    cache: Arc<Cache<Uuid, ()>>,
    inner: P,
}

impl<P> CachedDeduplicationStoreProvider<P> {
    /// Creates a new provider wrapping `inner` with the given `cache`.
    #[must_use]
    pub fn new(inner: P, cache: Arc<Cache<Uuid, ()>>) -> Self {
        Self { cache, inner }
    }
}

impl<P: DeduplicationStoreProvider> DeduplicationStoreProvider
    for CachedDeduplicationStoreProvider<P>
{
    type Store = CachedDeduplicationStore<P::Store>;

    fn create_store(
        &self,
        topic: Topic,
        partition: Partition,
        consumer_group: &str,
    ) -> Self::Store {
        CachedDeduplicationStore::new(
            self.cache.clone(),
            self.inner.create_store(topic, partition, consumer_group),
        )
    }
}
