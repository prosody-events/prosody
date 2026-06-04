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
//! # Apply hooks
//!
//! `Output` encodes whether the inner ran: `Some` means the inner ran and
//! its apply hook is forwarded; `None` means a dedup hit prevented the inner
//! from running and both hooks are suppressed.

pub mod cassandra;
pub mod config;
pub mod memory;
pub mod queries;
pub mod store;
#[cfg(test)]
pub mod tests;

use std::error::Error as StdError;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

use thiserror::Error;
use tracing::{debug, info_span};
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
    DEFAULT_IDEMPOTENCE_VERSION, DeduplicationConfiguration, DeduplicationConfigurationBuilder,
    DeduplicationConfigurationBuilderError, IDEMPOTENCE_VERSION_ENV,
};
pub use self::memory::{MemoryDeduplicationStore, MemoryDeduplicationStoreProvider};
pub use self::store::{DeduplicationStore, DeduplicationStoreProvider};

/// Shared state for the deduplication middleware.
#[derive(Clone, Debug)]
struct DeduplicationShared<S> {
    config: DeduplicationConfiguration,
    group_id: Arc<str>,
    store_provider: S,
}

/// Deduplication middleware.
///
/// Wraps the inner middleware stack and checks incoming messages against a
/// persistent store. Duplicates are filtered out before reaching the handler.
///
/// The `P` parameter is the handler payload type, fixed by the chain it is
/// composed into. `S` is the store provider; any caching is the provider's
/// responsibility.
#[derive(Clone, Debug)]
pub struct DeduplicationMiddleware<S: DeduplicationStoreProvider, P> {
    shared: Arc<DeduplicationShared<S>>,
    _payload: PhantomData<fn() -> P>,
}

impl<S: DeduplicationStoreProvider, P> DeduplicationMiddleware<S, P> {
    /// Creates a new middleware, or `None` if `cache_capacity == 0`.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if the configuration is invalid.
    pub fn new(
        config: DeduplicationConfiguration,
        group_id: &str,
        store_provider: S,
    ) -> Result<Option<Self>, validator::ValidationErrors> {
        config.validate()?;

        if config.cache_capacity == 0 {
            return Ok(None);
        }

        Ok(Some(Self {
            shared: Arc::new(DeduplicationShared {
                config,
                group_id: Arc::from(group_id),
                store_provider,
            }),
            _payload: PhantomData,
        }))
    }
}

impl<S: DeduplicationStoreProvider, P: Send + Sync + 'static + EventIdentity> HandlerMiddleware<P>
    for DeduplicationMiddleware<S, P>
{
    type Provider<T>
        = DeduplicationProvider<T, S>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        DeduplicationProvider {
            inner: provider,
            shared: self.shared.clone(),
        }
    }
}

/// Provider that creates per-partition deduplication handlers.
#[derive(Clone, Debug)]
pub struct DeduplicationProvider<T, S: DeduplicationStoreProvider> {
    inner: T,
    shared: Arc<DeduplicationShared<S>>,
}

impl<T, S> FallibleHandlerProvider for DeduplicationProvider<T, S>
where
    T: FallibleHandlerProvider,
    <T::Handler as FallibleHandler>::Payload: EventIdentity,
    S: DeduplicationStoreProvider,
{
    type Handler = DeduplicationHandler<T::Handler, S::Store>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let inner = self.inner.handler_for_partition(topic, partition);
        let store =
            self.shared
                .store_provider
                .create_store(topic, partition, &self.shared.group_id);

        DeduplicationHandler {
            inner,
            store,
            version: self.shared.config.version.clone(),
            group_id: self.shared.group_id.clone(),
            topic,
            partition,
        }
    }
}

/// Handler that checks messages against the shared dedup store (with cache).
pub struct DeduplicationHandler<T, S: DeduplicationStore> {
    inner: T,
    store: S,
    version: String,
    group_id: Arc<str>,
    topic: Topic,
    partition: Partition,
}

/// Computes the dedup UUID for a message.
///
/// Length-prefixes each field before hashing so that adjacent fields cannot
/// be confused — the prefix is load-bearing for canonical equality. Both
/// the deduplication middleware and any future state-middleware WAL writer
/// must call this function with the same arguments to produce the same UUID.
#[must_use]
pub fn dedup_uuid(
    version: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
    key: &[u8],
    event_id: Option<&[u8]>,
    offset: i64,
) -> Uuid {
    let mut hasher = Xxh3Default::new();
    hasher.write_u32(version.len() as u32);
    hasher.write(version.as_bytes());
    hasher.write_u32(group_id.len() as u32);
    hasher.write(group_id.as_bytes());
    hasher.write_u32(topic.len() as u32);
    hasher.write(topic.as_bytes());
    hasher.write_i32(partition);
    hasher.write_u32(key.len() as u32);
    hasher.write(key);

    if let Some(id) = event_id {
        hasher.write_u8(1);
        hasher.write_u32(id.len() as u32);
        hasher.write(id);
    } else {
        hasher.write_u8(0);
        hasher.write_i64(offset);
    }

    let hash = hasher.digest128();
    uuid::Builder::from_custom_bytes(hash.to_le_bytes()).into_uuid()
}

/// Computes the dedup UUID for a message, assembling the per-message hash
/// arguments (key, `event_id`, offset) from `message` in one place.
///
/// This is the single source of truth for the message → dedup-id mapping.
/// The deduplication middleware writes a row under this id; any reader that
/// must look the row up — notably the keyed-state recovery oracle — calls
/// this same function with the same `(version, group_id, topic, partition)`
/// so the two derivations cannot drift apart on the `event_id`/offset branch
/// selection inside [`dedup_uuid`].
#[must_use]
pub fn dedup_uuid_for_message<P>(
    version: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
    message: &ConsumerMessage<P>,
) -> Uuid
where
    P: EventIdentity,
{
    dedup_uuid(
        version,
        group_id,
        topic,
        partition,
        message.key().as_bytes(),
        message.payload().event_id().map(str::as_bytes),
        message.offset(),
    )
}

impl<T, S> DeduplicationHandler<T, S>
where
    T: FallibleHandler,
    T::Payload: EventIdentity,
    S: DeduplicationStore,
{
    fn dedup_uuid_for_message(&self, message: &ConsumerMessage<T::Payload>) -> Uuid {
        dedup_uuid_for_message(
            &self.version,
            &self.group_id,
            &self.topic,
            self.partition,
            message,
        )
    }
}

impl<T, S> FallibleHandler for DeduplicationHandler<T, S>
where
    T: FallibleHandler,
    T::Payload: EventIdentity,
    S: DeduplicationStore,
{
    type Error = DeduplicationError<T::Error>;
    /// `Some` — inner ran; forward apply hook. `None` — dedup hit; suppress
    /// both hooks.
    type Output = Option<T::Output>;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let id = self.dedup_uuid_for_message(&message);

        if self
            .store
            .exists(id)
            .await
            .map_err(|e| DeduplicationError::Store(Box::new(e)))?
        {
            info_span!(
                parent: message.span(),
                "message.filtered",
                reason = "deduplicated"
            )
            .in_scope(|| {
                debug!("message deduplicated");
            });
            return Ok(None);
        }

        // Process message.
        let result = self
            .inner
            .on_message(context, message, demand_type)
            .await
            .map_err(DeduplicationError::Inner);

        // Record on success or permanent error.
        let should_dedup = match &result {
            Ok(_) => true,
            Err(e) => matches!(e.classify_error(), ErrorCategory::Permanent),
        };

        if should_dedup {
            self.store
                .insert(id)
                .await
                .map_err(|e| DeduplicationError::Store(Box::new(e)))?;
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
        C: EventContext<Payload = T::Payload>,
    {
        self.inner
            .on_timer(context, trigger, demand_type)
            .await
            .map(Some)
            .map_err(DeduplicationError::Inner)
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        // The `Err(Store(_))` arm covers two cases:
        //   1. Inner did not run (store read failed before dispatch).
        //   2. Inner ran, then the post-inner store write failed.
        // Both deliberately suppress the inner's apply hook. `Store(_)` is
        // classified as Transient (see `ClassifyError` impl below), so the
        // outer retry layer will redrive the whole stack and the inner sees
        // a fresh invocation. Apply hooks are best-effort by design — see
        // `FallibleHandler::after_commit` docs.
        match result {
            Ok(Some(output)) => self.inner.after_commit(context, Ok(output)).await,
            Ok(None) | Err(DeduplicationError::Store(_)) => {}
            Err(DeduplicationError::Inner(error)) => {
                self.inner.after_commit(context, Err(error)).await;
            }
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        // See `after_commit`: `Err(Store(_))` covers both pre-inner read
        // failure and post-inner write failure. Both suppress the inner hook;
        // retry redrives.
        match result {
            Ok(Some(output)) => self.inner.after_abort(context, Ok(output)).await,
            Ok(None) | Err(DeduplicationError::Store(_)) => {}
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
    /// A store read or write failed.
    ///
    /// Classified as transient so the retry layer prevents the Kafka offset
    /// from committing until the store is healthy.
    #[error("deduplication store error")]
    Store(#[source] Box<dyn StdError + Send + Sync + 'static>),
}

impl<E: ClassifyError> ClassifyError for DeduplicationError<E> {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(e) => e.classify_error(),
            Self::Store(_) => ErrorCategory::Transient,
        }
    }
}
