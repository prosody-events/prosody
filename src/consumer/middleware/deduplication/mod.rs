//! Cassandra-backed deduplication middleware.
//!
//! Replaces the previous local-only LRU deduplication cache with a two-tier
//! approach: a global shared write-through cache backed by persistent
//! Cassandra storage. This ensures duplicates are detected even after restarts
//! or rebalances.
//!
//! The cache is shared across all partitions so it survives partition
//! reassignments without cold-start penalties.
//!
//! The middleware sits just inside the retry layer on the pipeline consumer.
//! It is optional — setting `cache_capacity = 0` disables it via the
//! [`Option<M>`](crate::consumer::middleware::optional) pattern.

pub mod cassandra;
pub mod config;
pub mod memory;
pub mod queries;
pub mod store;
#[cfg(test)]
pub mod tests;

use std::hash::Hasher;
use std::sync::Arc;

use quick_cache::sync::Cache;
use thiserror::Error;

type DeduplicationCache = Cache<Uuid, ()>;
use tracing::{debug, info_span, warn};
use uuid::Uuid;
use validator::Validate;
use xxhash_rust::xxh3::Xxh3Default;

use crate::consumer::DemandType;
use crate::consumer::Keyed;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::timers::Trigger;
use crate::{EventIdentity, Partition, Topic};

pub use self::cassandra::{CassandraDeduplicationStore, CassandraDeduplicationStoreProvider};
pub use self::config::{
    DeduplicationConfiguration, DeduplicationConfigurationBuilder,
    DeduplicationConfigurationBuilderError,
};
pub use self::memory::{MemoryDeduplicationStore, MemoryDeduplicationStoreProvider};
pub use self::store::{DeduplicationStore, DeduplicationStoreProvider};

/// Shared state for the deduplication middleware.
#[derive(Clone, Debug)]
struct DeduplicationShared<P> {
    config: DeduplicationConfiguration,
    group_id: Arc<str>,
    store_provider: P,
    cache: Arc<DeduplicationCache>,
}

/// Deduplication middleware.
///
/// Wraps the inner middleware stack and checks incoming messages against a
/// two-tier cache (local + persistent store). Duplicates are filtered out
/// before reaching the handler.
#[derive(Clone, Debug)]
pub struct DeduplicationMiddleware<P: DeduplicationStoreProvider> {
    shared: Arc<DeduplicationShared<P>>,
}

impl<P: DeduplicationStoreProvider> DeduplicationMiddleware<P> {
    /// Creates a new middleware, or `None` if `cache_capacity == 0`.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if the configuration is invalid.
    pub fn new(
        config: DeduplicationConfiguration,
        group_id: &str,
        store_provider: P,
    ) -> Result<Option<Self>, validator::ValidationErrors> {
        config.validate()?;

        if config.cache_capacity == 0 {
            return Ok(None);
        }

        let cache = Arc::new(Cache::new(config.cache_capacity));
        Ok(Some(Self {
            shared: Arc::new(DeduplicationShared {
                config,
                group_id: Arc::from(group_id),
                store_provider,
                cache,
            }),
        }))
    }
}

impl<P: DeduplicationStoreProvider> HandlerMiddleware for DeduplicationMiddleware<P> {
    type Provider<T: FallibleHandlerProvider> = DeduplicationProvider<T, P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        DeduplicationProvider {
            inner: provider,
            shared: self.shared.clone(),
        }
    }
}

/// Provider that creates per-partition deduplication handlers.
#[derive(Clone, Debug)]
pub struct DeduplicationProvider<T, P: DeduplicationStoreProvider> {
    inner: T,
    shared: Arc<DeduplicationShared<P>>,
}

impl<T, P> FallibleHandlerProvider for DeduplicationProvider<T, P>
where
    T: FallibleHandlerProvider,
    P: DeduplicationStoreProvider,
{
    type Handler = DeduplicationHandler<T::Handler, P::Store>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let inner = self.inner.handler_for_partition(topic, partition);
        let cache = self.shared.cache.clone();
        let store =
            self.shared
                .store_provider
                .create_store(topic, partition, &self.shared.group_id);

        DeduplicationHandler {
            inner,
            cache,
            store,
            version: self.shared.config.version.clone(),
            group_id: self.shared.group_id.clone(),
            topic,
            partition,
        }
    }
}

/// Handler that checks messages against the shared dedup cache.
pub struct DeduplicationHandler<T, S: DeduplicationStore> {
    inner: T,
    cache: Arc<DeduplicationCache>,
    store: S,
    version: String,
    group_id: Arc<str>,
    topic: Topic,
    partition: Partition,
}

impl<T, S> DeduplicationHandler<T, S>
where
    T: FallibleHandler,
    S: DeduplicationStore,
{
    /// Computes the dedup UUID for a message, incorporating the `event_id`
    /// (from payload) or falling back to writing the offset directly.
    fn dedup_uuid_for_message(&self, message: &ConsumerMessage) -> Uuid {
        let mut hasher = Xxh3Default::new();
        hasher.write_u32(self.version.len() as u32);
        hasher.write(self.version.as_bytes());
        hasher.write_u32(self.group_id.len() as u32);
        hasher.write(self.group_id.as_bytes());
        hasher.write_u32(self.topic.len() as u32);
        hasher.write(self.topic.as_bytes());
        hasher.write_i32(self.partition);
        hasher.write_u32(message.key().len() as u32);
        hasher.write(message.key().as_bytes());

        if let Some(id) = message.payload().event_id() {
            hasher.write_u8(1);
            hasher.write_u32(id.len() as u32);
            hasher.write(id.as_bytes());
        } else {
            hasher.write_u8(0);
            hasher.write_i64(message.offset());
        }

        let hash = hasher.digest128();
        uuid::Builder::from_custom_bytes(hash.to_le_bytes()).into_uuid()
    }
}

impl<T, S> FallibleHandler for DeduplicationHandler<T, S>
where
    T: FallibleHandler,
    S: DeduplicationStore,
{
    type Error = DeduplicationError<T::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let dedup_uuid = self.dedup_uuid_for_message(&message);

        // 1. Check local cache
        if self.cache.get(&dedup_uuid).is_some() {
            info_span!(
                parent: message.span(),
                "message.filtered",
                reason = "deduplicated"
            )
            .in_scope(|| {
                debug!("message deduplicated via local cache");
            });
            return Ok(());
        }

        // 2. Check persistent store
        match self.store.exists(dedup_uuid).await {
            Ok(true) => {
                self.cache.insert(dedup_uuid, ());
                info_span!(
                    parent: message.span(),
                    "message.filtered",
                    reason = "deduplicated"
                )
                .in_scope(|| {
                    debug!("message deduplicated via persistent store");
                });
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                warn!("deduplication store read failed: {error:#}; treating as cache miss");
            }
        }

        // 3. Process message
        self.inner
            .on_message(context, message, demand_type)
            .await
            .map_err(DeduplicationError::Inner)?;

        // 4. Record in local cache and persistent store
        self.cache.insert(dedup_uuid, ());
        if let Err(error) = self.store.insert(dedup_uuid).await {
            warn!("deduplication store write failed: {error:#}; continuing");
        }

        Ok(())
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        self.inner
            .on_timer(context, trigger, demand_type)
            .await
            .map_err(DeduplicationError::Inner)
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

/// Error type for the deduplication middleware.
///
/// Transparently wraps the inner handler's error, delegating error
/// classification.
#[derive(Debug, Error)]
pub enum DeduplicationError<E> {
    /// Error from the inner handler.
    #[error(transparent)]
    Inner(E),
}

impl<E: ClassifyError> ClassifyError for DeduplicationError<E> {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(e) => e.classify_error(),
        }
    }
}
