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
//! The middleware lives in the common block (built by
//! `build_common_middleware`), just outside `cancellation`. It is a
//! duplicate **filter** plus a marker **register**: a dedup hit
//! short-circuits before the inner runs; otherwise, on a handler `Ok` or a
//! permanent error, it registers the message's dedup id in the event's
//! keyed-state session. The actual marker write happens later, at the
//! `settle` durability boundary, strictly after the WAL seal — so the
//! commit marker can never precede the durable state it certifies.
//! Deduplication is mandatory; there is no disabled variant.
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
use crate::state::session::LifecycleAccessExt;
use crate::state::session::sealed::StateLifecycle;
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
    /// Creates a new middleware.
    ///
    /// Deduplication is mandatory: it is the commit oracle for keyed state, so
    /// there is no disabled variant.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if the configuration is invalid.
    pub fn new(
        config: DeduplicationConfiguration,
        group_id: &str,
        store_provider: S,
    ) -> Result<Self, validator::ValidationErrors> {
        config.validate()?;

        Ok(Self {
            shared: Arc::new(DeduplicationShared {
                config,
                group_id: Arc::from(group_id),
                store_provider,
            }),
            _payload: PhantomData,
        })
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
/// the deduplication middleware and any other source of the dedup UUID
/// (today, the commit oracle) must call this function with the same arguments
/// to produce the same UUID.
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

/// The fixed coordinates a consumer deduplicates a partition's messages
/// under: the hash version, consumer group, topic, and partition. These four
/// never vary across a partition's message stream — only the per-message key,
/// event id, and offset do. Bundled so the deduplication writer and the
/// keyed-state recovery reader derive a message's id from the same identity,
/// and passed by value (it borrows; the fields are cheap to reference).
#[derive(Clone, Copy)]
pub struct DedupIdentity<'a> {
    /// Deduplication hash version.
    pub version: &'a str,
    /// Consumer group.
    pub group_id: &'a str,
    /// Kafka topic.
    pub topic: &'a str,
    /// Kafka partition.
    pub partition: Partition,
}

/// Computes the dedup UUID for a message, assembling the per-message hash
/// arguments (key, `event_id`, offset) from `message` and the fixed
/// coordinates from `identity` in one place.
///
/// This is the single source of truth for the message → dedup-id mapping.
/// The deduplication middleware writes a row under this id; any reader that
/// must look the row up — notably the keyed-state recovery oracle — calls
/// this same function with the same [`DedupIdentity`] so the two derivations
/// cannot drift apart on the `event_id`/offset branch selection inside
/// [`dedup_uuid`].
#[must_use]
pub fn dedup_uuid_for_message<P>(identity: DedupIdentity<'_>, message: &ConsumerMessage<P>) -> Uuid
where
    P: EventIdentity,
{
    dedup_uuid(
        identity.version,
        identity.group_id,
        identity.topic,
        identity.partition,
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
            DedupIdentity {
                version: &self.version,
                group_id: &self.group_id,
                topic: &self.topic,
                partition: self.partition,
            },
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
            .on_message(context.clone(), message, demand_type)
            .await
            .map_err(DeduplicationError::Inner);

        // Register the marker on success or permanent error — both are final
        // from the consumer's POV, so the message must dedup on redelivery.
        // The marker is written (flushed) later by the `settle` boundary,
        // strictly after the WAL seal; registering it here, not writing it,
        // is what makes "marker before seal" unwritable. A defer-swallow or a
        // retry attempt-boundary resets the session, discarding the marker, so
        // a deferred reload is correctly not deduped.
        let register = match &result {
            Ok(_) => true,
            Err(e) => matches!(e.classify_error(), ErrorCategory::Permanent),
        };
        if register && let Ok(lifecycle) = context.lifecycle() {
            lifecycle.register_marker(id);
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
        // `Store(_)` now arises only from the `exists` read before dispatch —
        // the inner never ran — so suppressing the inner's apply hook is
        // correct. `Store(_)` is classified Transient (see `ClassifyError`
        // below), so the outer retry layer redrives the whole stack and the
        // inner sees a fresh invocation. (The marker is no longer written
        // here; the `settle` boundary flushes it after the seal.) Apply hooks
        // are best-effort by design — see `FallibleHandler::after_commit`.
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
        // See `after_commit`: `Err(Store(_))` is the pre-inner read failure;
        // the inner never ran, so suppress its hook and let retry redrive.
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
