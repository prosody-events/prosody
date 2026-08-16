//! Defer middleware for transient failure handling.
//!
//! Defers transiently-failed messages to timer-based retry instead of blocking
//! the partition, maintaining throughput during downstream outages.
//!
//! # Invariants
//!
//! 1. **Ordering**: Messages for a key are processed in offset order.
//!
//! 2. **Completion**: All messages are processed. Deferred keys always have an
//!    active timer ensuring eventual processing.
//!
//! 3. **Deferral**: When enabled, all transient errors are deferred. Once
//!    deferred, transient errors always re-defer (config/decider only gate
//!    initial deferral).
//!
//! # Apply hooks
//!
//! The inner is invoked at most once per dispatch; retries arrive as new
//! `on_timer` dispatches, each with their own apply-hook pairing.
//! [`MessageDeferOutput`] encodes the routing:
//!
//! * `Inner` — inner ran; forward the framework's chosen hook.
//! * `Deferred` — inner ran and returned a transient error that we captured for
//!   retry. Both hooks route to `after_abort(Err(..))`: a retry is coming even
//!   though the dispatch's offset itself commits.
//! * `NoInner` — inner did not run (queue-append, orphan-timer, loader failure,
//!   key-mismatch); suppress both hooks.

use super::store::{MessageDeferStore, MessageDeferStoreProvider};
use crate::JsonCodec;
use crate::consumer::ConsumerConfiguration;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::{DeferralDecider, FailureTracker};
use crate::consumer::middleware::defer::error::{DeferError, DeferInitError};
use crate::consumer::middleware::{
    FallibleHandler, FallibleHandlerProvider, HandlerMiddleware, Settlement, SettlementHandler,
};
use crate::loader::{KafkaLoader, MessageLoader};
use crate::telemetry::Telemetry;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::{ConsumerGroup, Partition, Topic};
use std::sync::Arc;

mod dispatch;
mod operations;

/// Property-based tests for defer handler invariants.
#[cfg(test)]
pub mod tests;

/// Output of [`MessageDeferHandler`] dispatches; drives apply-hook routing.
///
/// See the module-level apply-hooks section for how `after_commit` /
/// `after_abort` dispatch on these variants.
#[derive(Debug)]
pub enum MessageDeferOutput<O, E> {
    /// Inner ran and produced an output.
    Inner(O),
    /// Inner did not run (queue-append, orphan-timer, loader failure,
    /// key-mismatch) — suppress both apply hooks.
    NoInner,
    /// Inner ran and returned a transient error captured for retry. Both
    /// apply hooks fire `after_abort(Err(E))`: the retry will re-dispatch
    /// the same logical message.
    Deferred(E),
}

/// Middleware that defers transiently-failed messages for timer-based retry.
///
/// This middleware handles message deferral independently of timer deferral.
/// Both can be composed via `.layer()`.
///
/// # Type Parameters
///
/// * `S` - Message defer store provider
/// * `L` - Message loader (default: [`KafkaLoader`])
/// * `D` - Deferral decider (default: [`FailureTracker`])
#[derive(Clone)]
pub struct MessageDeferMiddleware<S, L = KafkaLoader<JsonCodec>, D = FailureTracker>
where
    S: MessageDeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    loader: L,
    provider: S,
    decider: D,
    consumer_group: ConsumerGroup,
    dedup_version: Arc<str>,
    telemetry: Telemetry,
}

impl<S, L> MessageDeferMiddleware<S, L, FailureTracker>
where
    S: MessageDeferStoreProvider,
    L: MessageLoader,
{
    /// Creates middleware with a caller-supplied loader and a
    /// [`FailureTracker`] decider.
    ///
    /// Callers pick the loader: [`KafkaLoader`] for production (see
    /// [`KafkaLoader::for_consumer`]) or [`MemoryLoader`] for mock mode,
    /// where connecting to real Kafka is not permitted.
    ///
    /// `dedup_version` is the deduplication hash version: the reload path
    /// derives each reloaded message's dedup id (the session's identity
    /// override) with it, so it must match the version the partition loop
    /// derives message `EventRef`s with.
    ///
    /// [`KafkaLoader::for_consumer`]: crate::loader::KafkaLoader::for_consumer
    /// [`MemoryLoader`]: crate::loader::MemoryLoader
    ///
    /// # Errors
    ///
    /// Returns an error if config validation fails.
    pub fn new(
        config: DeferConfiguration,
        consumer_config: &ConsumerConfiguration,
        provider: S,
        decider: FailureTracker,
        loader: L,
        dedup_version: &str,
        telemetry: &Telemetry,
    ) -> Result<Self, DeferInitError> {
        use validator::Validate;

        config.validate()?;

        Ok(Self {
            config,
            loader,
            provider,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
            dedup_version: Arc::from(dedup_version),
            telemetry: telemetry.clone(),
        })
    }
}

/// Creates [`MessageDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct MessageDeferProvider<T, S, L = KafkaLoader<JsonCodec>, D = FailureTracker>
where
    S: MessageDeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    inner_provider: T,
    config: DeferConfiguration,
    loader: L,
    store_provider: S,
    decider: D,
    consumer_group: ConsumerGroup,
    dedup_version: Arc<str>,
    telemetry: Telemetry,
}

/// Per-partition handler wrapping an inner handler with defer logic.
#[derive(Clone)]
pub struct MessageDeferHandler<T, M, L = KafkaLoader<JsonCodec>, D = FailureTracker>
where
    M: MessageDeferStore,
    L: MessageLoader,
    D: DeferralDecider,
{
    pub(crate) handler: T,
    pub(crate) loader: L,
    pub(crate) store: M,
    pub(crate) decider: D,
    pub(crate) config: DeferConfiguration,
    pub(crate) topic: Topic,
    pub(crate) partition: Partition,
    pub(crate) sender: TelemetryPartitionSender,
    pub(crate) source: Arc<str>,
    /// Deduplication hash version; the reload path derives the reloaded
    /// message's dedup id with it (must match the partition loop's).
    pub(crate) dedup_version: Arc<str>,
}

impl<S, L, D> HandlerMiddleware<L::Payload> for MessageDeferMiddleware<S, L, D>
where
    S: MessageDeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Provider<T>
        = MessageDeferProvider<T, S, L, D>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = L::Payload>;

    fn with_provider<T>(&self, inner_provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = L::Payload>,
    {
        MessageDeferProvider {
            inner_provider,
            config: self.config.clone(),
            loader: self.loader.clone(),
            store_provider: self.provider.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
            dedup_version: self.dedup_version.clone(),
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T, S, L, D> FallibleHandlerProvider for MessageDeferProvider<T, S, L, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler<Payload = L::Payload>,
    S: MessageDeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Handler = MessageDeferHandler<T::Handler, S::Store, L, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let store = self.store_provider.create_store(
            topic,
            partition,
            &self.consumer_group,
            self.config.store_cache_size,
        );

        let inner_handler = self.inner_provider.handler_for_partition(topic, partition);

        let sender = self.telemetry.partition_sender(topic, partition);

        MessageDeferHandler {
            handler: inner_handler,
            loader: self.loader.clone(),
            store,
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
            sender,
            source: self.consumer_group.clone(),
            dedup_version: self.dedup_version.clone(),
        }
    }
}

impl<T, M, L, D> SettlementHandler for MessageDeferHandler<T, M, L, D>
where
    T: SettlementHandler<Payload = L::Payload>,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        match result {
            // Inner ran: its result is the dispatch's outcome.
            Ok(MessageDeferOutput::Inner(output)) => T::settlement(Ok(output)),
            // Inner ran and its error surfaced.
            Err(DeferError::Handler(error)) => T::settlement(Err(error)),
            // `Deferred`/`NoInner` — parked for retry / queued behind /
            // handled at the load layer: the outcome lives in the defer
            // queue, so nothing here may stage or record — the reload must
            // re-run unfiltered. The error rows are the defer layer's own
            // rescue failing (store/timer/loader/backoff computation) — a
            // layer failure, never the event's outcome.
            Ok(MessageDeferOutput::Deferred(_) | MessageDeferOutput::NoInner)
            | Err(
                DeferError::Store(_)
                | DeferError::Timer(_)
                | DeferError::Loader(_)
                | DeferError::CompactTime(_),
            ) => Settlement::Bypassed,
        }
    }
}
