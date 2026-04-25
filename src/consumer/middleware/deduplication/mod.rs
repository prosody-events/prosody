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
//!
//! # Apply-hook contract
//!
//! Per the [`FallibleHandler`] invariant, exactly one of `after_commit` or
//! `after_abort` must fire on the inner handler for every dispatch in which
//! the inner's `on_message` / `on_timer` actually ran. This middleware honors
//! that by encoding "did the inner run?" in its `Output` discriminant: a `Some`
//! variant means the inner ran (and thus its apply hook is forwarded), while a
//! `None` variant means a dedup hit short-circuited the dispatch before the
//! inner was ever invoked (so neither apply hook is forwarded to the inner).
//! This is distinct from forwarding the work outcome — when the inner did run,
//! both success and inner errors are forwarded faithfully to the same apply
//! hook the framework invoked here.
//!
//! Concretely, the inner is invoked at most once per call: a dedup hit (local
//! cache or persistent store) short-circuits before the inner runs, while a
//! dedup miss invokes the inner exactly once. `on_timer` always invokes the
//! inner exactly once (timers are not deduplicated). Combined with the
//! suppress-on-`None` / forward-on-`Some`-or-`Inner` apply-hook routing above,
//! the per-invocation invariant is trivially upheld for the inner handler.

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
use tracing::{debug, info_span, warn};
use uuid::Uuid;
use validator::Validate;
use xxhash_rust::xxh3::Xxh3Default;

type DeduplicationCache = Cache<Uuid, ()>;

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
    /// Marker for whether the inner handler's `on_message` / `on_timer`
    /// actually ran on this dispatch.
    ///
    /// - `Some(inner_output)` — the inner ran to completion and produced
    ///   `inner_output`. The framework's subsequent `after_commit` /
    ///   `after_abort` call on this handler must be forwarded to the inner so
    ///   the `FallibleHandler` invariant (exactly one apply hook per run) is
    ///   preserved for the inner.
    /// - `None` — a dedup hit short-circuited the dispatch and the inner's
    ///   `on_message` / `on_timer` was never invoked. Per the invariant,
    ///   neither of the inner's apply hooks must fire for this dispatch, so
    ///   both `after_commit` and `after_abort` on this handler suppress the
    ///   forward to the inner.
    ///
    /// Note: this is purely a "did the inner run?" marker, not a "should the
    /// inner's work be applied?" gate — when the inner did run, its outcome
    /// (success or `Err`) flows through to whichever apply hook the framework
    /// chooses.
    type Output = Option<T::Output>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
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
            return Ok(None);
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
                return Ok(None);
            }
            Ok(false) => {}
            Err(error) => {
                warn!("deduplication store read failed: {error:#}; treating as cache miss");
            }
        }

        // 3. Process message
        let result = self
            .inner
            .on_message(context, message, demand_type)
            .await
            .map_err(DeduplicationError::Inner);

        // 4. Record in local cache and persistent store on success or permanent error.
        // Permanent errors are any message-level failures that won't succeed on retry
        // (e.g., corruption, serialization/invalid data, non-retryable business
        // rejections). Transient and terminal errors are left un-deduplicated:
        // transient so the retry layer can reattempt, terminal because
        // processing is being aborted.
        let should_dedup = match &result {
            Ok(_) => true,
            Err(e) => matches!(e.classify_error(), ErrorCategory::Permanent),
        };

        if should_dedup {
            self.cache.insert(dedup_uuid, ());
            if let Err(error) = self.store.insert(dedup_uuid).await {
                warn!("deduplication store write failed: {error:#}; continuing");
            }
        }

        result.map(Some)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        self.inner
            .on_timer(context, trigger, demand_type)
            .await
            .map(Some)
            .map_err(DeduplicationError::Inner)
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            // Inner ran and produced a success: forward the final commit so
            // the inner sees exactly one apply hook for its run.
            Ok(Some(output)) => self.inner.after_commit(context, Ok(output)).await,
            // Dedup hit — the inner's `on_message` / `on_timer` did not run on
            // this dispatch, so per the `FallibleHandler` invariant we must
            // not invoke either of the inner's apply hooks.
            Ok(None) => {}
            // Inner ran and returned an error that the framework is treating
            // as final (terminal / DLQ-bound): forward the final commit so
            // the inner sees exactly one apply hook for its run.
            Err(DeduplicationError::Inner(error)) => {
                self.inner.after_commit(context, Err(error)).await;
            }
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        match result {
            // Inner ran and produced a success, but the framework is aborting
            // this dispatch (a retry of the same logical event is coming):
            // forward the abort so the inner sees exactly one apply hook for
            // its run.
            Ok(Some(output)) => self.inner.after_abort(context, Ok(output)).await,
            // Dedup hit — the inner's `on_message` / `on_timer` did not run on
            // this dispatch, so per the `FallibleHandler` invariant we must
            // not invoke either of the inner's apply hooks.
            Ok(None) => {}
            // Inner ran and returned an error that the framework is treating
            // as retryable: forward the abort so the inner sees exactly one
            // apply hook for its run.
            Err(DeduplicationError::Inner(error)) => {
                self.inner.after_abort(context, Err(error)).await;
            }
        }
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
