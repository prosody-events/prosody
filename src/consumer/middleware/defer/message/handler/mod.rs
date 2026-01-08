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

use super::loader::{KafkaLoader, MessageLoader};
use super::store::cassandra::{CassandraMessageDeferStore, MessageQueries};
use super::store::memory::MemoryMessageDeferStore;
use super::store::{CachedDeferStore, MessageDeferStore, MessageRetryCompletionResult};
use crate::cassandra::CassandraStore;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::{DeferralDecider, FailureTracker};
use crate::consumer::middleware::defer::error::{
    CassandraDeferStoreError, DeferError, DeferInitError, DeferResult,
};
use crate::consumer::middleware::defer::segment::CassandraSegmentStore;
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{ConsumerConfiguration, DemandType, Keyed};
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Property-based tests for defer handler invariants.
#[cfg(test)]
pub mod tests;

/// Cassandra resources for creating message defer stores.
#[derive(Clone, Debug)]
pub struct CassandraResources {
    /// Shared Cassandra store.
    pub store: CassandraStore,
    /// Prepared message queries.
    pub queries: Arc<MessageQueries>,
    /// Cassandra segment store for segment persistence.
    pub segment_store: CassandraSegmentStore,
}

/// Message store variant - either memory or Cassandra.
#[derive(Clone, Debug)]
pub enum MessageStoreKind {
    /// In-memory store for testing.
    Memory,
    /// Cassandra-backed store for production.
    Cassandra(CassandraResources),
}

/// Enum wrapper for different message defer store implementations.
#[derive(Clone)]
pub enum MessageStoreWrapper {
    /// In-memory store for testing.
    Memory(CachedDeferStore<MemoryMessageDeferStore>),
    /// Cassandra-backed store for production.
    Cassandra(CachedDeferStore<CassandraMessageDeferStore>),
}

/// Unified error type for message store operations.
#[derive(Debug, Error)]
pub enum MessageStoreError {
    /// Cassandra store error.
    #[error(transparent)]
    Cassandra(#[from] CassandraDeferStoreError),
    /// Memory store error (never occurs).
    #[error(transparent)]
    Memory(#[from] Infallible),
}

impl ClassifyError for MessageStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Cassandra(e) => e.classify_error(),
            Self::Memory(e) => match *e {},
        }
    }
}

impl MessageDeferStore for MessageStoreWrapper {
    type Error = MessageStoreError;

    async fn defer_first_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .defer_first_message(key, offset)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .defer_first_message(key, offset)
                .await
                .map_err(Into::into),
        }
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        match self {
            Self::Memory(store) => store
                .get_next_deferred_message(key)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .get_next_deferred_message(key)
                .await
                .map_err(Into::into),
        }
    }

    async fn append_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .append_deferred_message(key, offset)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .append_deferred_message(key, offset)
                .await
                .map_err(Into::into),
        }
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .remove_deferred_message(key, offset)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .remove_deferred_message(key, offset)
                .await
                .map_err(Into::into),
        }
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store
                .set_retry_count(key, retry_count)
                .await
                .map_err(Into::into),
            Self::Cassandra(store) => store
                .set_retry_count(key, retry_count)
                .await
                .map_err(Into::into),
        }
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        match self {
            Self::Memory(store) => store.delete_key(key).await.map_err(Into::into),
            Self::Cassandra(store) => store.delete_key(key).await.map_err(Into::into),
        }
    }
}

/// Middleware that defers transiently-failed messages for timer-based retry.
///
/// This middleware handles message deferral independently of timer deferral.
/// Both can be composed via `.layer()`.
///
/// # Type Parameters
///
/// * `L` - Message loader (default: [`KafkaLoader`])
/// * `D` - Deferral decider (default: [`FailureTracker`])
#[derive(Clone)]
pub struct MessageDeferMiddleware<L = KafkaLoader, D = FailureTracker>
where
    L: MessageLoader,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    loader: L,
    store_kind: MessageStoreKind,
    decider: D,
    consumer_group: ConsumerGroup,
}

impl<D> MessageDeferMiddleware<KafkaLoader, D>
where
    D: DeferralDecider,
{
    /// Creates middleware with default [`KafkaLoader`].
    ///
    /// # Errors
    ///
    /// Returns error if config validation or loader construction fails.
    pub fn new(
        config: DeferConfiguration,
        consumer_config: &ConsumerConfiguration,
        scheduler_config: &SchedulerConfiguration,
        store_kind: MessageStoreKind,
        decider: D,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self, DeferInitError> {
        use super::loader::LoaderConfiguration;
        use validator::Validate;

        config.validate()?;

        // Create a derived group ID for the loader with .defer-loader suffix
        let loader_group_id = format!("{}.defer-loader", consumer_config.group_id);

        // Build KafkaLoader with shared consumer configuration
        let loader_config = LoaderConfiguration {
            bootstrap_servers: consumer_config.bootstrap_servers.clone(),
            group_id: loader_group_id,
            max_permits: scheduler_config.max_concurrency,
            cache_size: config.cache_size,
            poll_interval: consumer_config.poll_interval,
            seek_timeout: config.seek_timeout,
            discard_threshold: config.discard_threshold,
        };
        let loader = KafkaLoader::new(loader_config, heartbeats)?;

        Ok(Self {
            config,
            loader,
            store_kind,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
        })
    }
}

impl<L, D> MessageDeferMiddleware<L, D>
where
    L: MessageLoader,
    D: DeferralDecider,
{
    /// Creates middleware with custom loader and decider.
    ///
    /// # Errors
    ///
    /// Returns error if config validation fails.
    pub fn with_custom(
        config: DeferConfiguration,
        loader: L,
        store_kind: MessageStoreKind,
        decider: D,
        consumer_config: &ConsumerConfiguration,
    ) -> Result<Self, DeferInitError> {
        use validator::Validate;

        config.validate()?;

        Ok(Self {
            config,
            loader,
            store_kind,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
        })
    }
}

/// Creates [`MessageDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct MessageDeferProvider<T, L = KafkaLoader, D = FailureTracker>
where
    L: MessageLoader,
    D: DeferralDecider,
{
    inner_provider: T,
    config: DeferConfiguration,
    loader: L,
    store_kind: MessageStoreKind,
    decider: D,
    consumer_group: ConsumerGroup,
}

/// Per-partition handler wrapping an inner handler with defer logic.
#[derive(Clone)]
pub struct MessageDeferHandler<T, M, L = KafkaLoader, D = FailureTracker>
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
}

impl<L, D> HandlerMiddleware for MessageDeferMiddleware<L, D>
where
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Provider<T: FallibleHandlerProvider> = MessageDeferProvider<T, L, D>;

    fn with_provider<T>(&self, inner_provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        MessageDeferProvider {
            inner_provider,
            config: self.config.clone(),
            loader: self.loader.clone(),
            store_kind: self.store_kind.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
        }
    }
}

impl<T, L, D> FallibleHandlerProvider for MessageDeferProvider<T, L, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Handler = MessageDeferHandler<T::Handler, MessageStoreWrapper, L, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        // Create the appropriate store based on store_kind
        let message_store = match &self.store_kind {
            MessageStoreKind::Memory => {
                let store = MemoryMessageDeferStore::new();
                MessageStoreWrapper::Memory(CachedDeferStore::new(store, self.config.cache_size))
            }
            MessageStoreKind::Cassandra(resources) => {
                let store = CassandraMessageDeferStore::new(
                    resources.store.clone(),
                    resources.queries.clone(),
                    resources.segment_store.clone(),
                    topic,
                    partition,
                    self.consumer_group.clone(),
                );
                MessageStoreWrapper::Cassandra(CachedDeferStore::new(store, self.config.cache_size))
            }
        };

        // Inner handler
        let inner_handler = self.inner_provider.handler_for_partition(topic, partition);

        // Message defer wraps inner handler
        MessageDeferHandler {
            handler: inner_handler,
            loader: self.loader.clone(),
            store: message_store,
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
        }
    }
}

impl<T, M, L, D> MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    /// Returns `now + backoff(retry_count)`; used for scheduling timers.
    fn next_retry_time(
        &self,
        retry_count: u32,
    ) -> DeferResult<CompactDateTime, M::Error, T::Error, L::Error> {
        let delay = calculate_backoff(&self.config, retry_count);
        let now = CompactDateTime::now()?;
        Ok(now.add_duration(delay)?)
    }

    /// Schedules a `DeferredMessage` timer with backoff based on retry count.
    async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        let fire_time = self.next_retry_time(retry_count)?;

        context
            .clear_and_schedule(fire_time, TimerType::DeferredMessage)
            .await
            .map_err(|e| DeferError::Timer(Box::new(e)))?;

        debug!(
            fire_time = %fire_time,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Scheduled defer retry timer"
        );

        Ok(())
    }

    /// Schedules timer for next message or clears if queue empty.
    async fn schedule_next_or_clear<C>(
        &self,
        context: &C,
        result: MessageRetryCompletionResult,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        match result {
            MessageRetryCompletionResult::MoreMessages { .. } => {
                // More messages in queue - schedule timer (retry_count reset to 0)
                self.schedule_retry_timer(context, 0).await
            }
            MessageRetryCompletionResult::Completed => {
                // No more messages - clear the timer
                context
                    .clear_scheduled(TimerType::DeferredMessage)
                    .await
                    .map_err(|e| DeferError::Timer(Box::new(e)))
            }
        }
    }

    /// Removes message from queue and schedules timer for next (or clears).
    /// Used after success, permanent failure, or skipping corrupted messages.
    async fn complete_and_advance<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        let result = self
            .store
            .complete_retry_success(message_key, offset)
            .await
            .map_err(DeferError::Store)?;

        self.schedule_next_or_clear(context, result).await
    }

    /// Appends message to an already-deferred key's queue (maintains ordering).
    async fn append_to_deferred_queue(
        &self,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), M::Error, T::Error, L::Error> {
        self.store
            .defer_additional_message(message_key, offset)
            .await
            .map_err(DeferError::Store)?;

        debug!(
            key = ?message_key,
            offset = offset,
            topic = %self.topic,
            partition = self.partition,
            "Queued message behind already-deferred key"
        );

        Ok(())
    }

    /// Handles retry failures by error category:
    /// - **Transient**: Always re-defer (maintains completion invariant)
    /// - **Permanent**: Remove and advance (unblocks queue)
    /// - **Terminal**: Propagate without state change (shutdown handling)
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: T::Error,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        match error.classify_error() {
            ErrorCategory::Transient => {
                // Always re-defer: message is committed to queue, dropping would
                // violate ordering for messages queued behind it.
                let new_retry_count = self
                    .store
                    .increment_retry_count(message_key, retry_count)
                    .await
                    .map_err(DeferError::Store)?;

                self.schedule_retry_timer(context, new_retry_count).await?;

                info!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = new_retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Re-deferred message after transient failure"
                );

                Ok(())
            }
            ErrorCategory::Permanent => {
                warn!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Permanent handler error during retry - removing from queue: {error:#}"
                );

                self.complete_and_advance(context, message_key, offset)
                    .await?;

                Err(DeferError::Handler(error))
            }
            ErrorCategory::Terminal => Err(DeferError::Handler(error)),
        }
    }

    /// Loads message from Kafka. Returns `None` if load failed and was handled
    /// (timer rescheduled). Returns `Err` only for terminal errors.
    async fn load_deferred_message<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
    ) -> DeferResult<Option<ConsumerMessage>, M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        let message = match self
            .loader
            .load_message(self.topic, self.partition, offset)
            .await
        {
            Ok(msg) => msg,
            Err(error) => {
                return self
                    .handle_load_failure(context, message_key, offset, retry_count, error)
                    .await;
            }
        };

        if message.key() != message_key {
            warn!(
                expected_key = ?message_key,
                actual_key = ?message.key(),
                offset = offset,
                topic = %self.topic,
                partition = self.partition,
                "Key mismatch at offset - skipping corrupted entry"
            );

            self.complete_and_advance(context, message_key, offset)
                .await?;

            return Ok(None);
        }

        Ok(Some(message))
    }

    /// Handles loader errors: permanent skips, transient retries, terminal
    /// propagates.
    async fn handle_load_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: L::Error,
    ) -> DeferResult<Option<ConsumerMessage>, M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        match error.classify_error() {
            ErrorCategory::Permanent => {
                warn!(
                    key = ?message_key,
                    offset = offset,
                    topic = %self.topic,
                    partition = self.partition,
                    "Permanent loader error - skipping message: {error:#}"
                );
                self.complete_and_advance(context, message_key, offset)
                    .await?;
            }
            ErrorCategory::Transient => {
                let new_retry_count = self
                    .store
                    .increment_retry_count(message_key, retry_count)
                    .await
                    .map_err(DeferError::Store)?;

                self.schedule_retry_timer(context, new_retry_count).await?;

                warn!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = new_retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Transient loader error - scheduling retry: {error:#}"
                );
            }
            ErrorCategory::Terminal => {
                return Err(DeferError::Loader(error));
            }
        }
        Ok(None)
    }

    /// Defers a message for the first time. Schedules timer before storing
    /// to ensure timer coverage on partial failure.
    async fn defer_message<C>(
        &self,
        context: C,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        // Timer first, then store: ensures timer coverage on partial failure.
        self.schedule_retry_timer(&context, 0).await?;

        self.store
            .defer_first_message(message_key, offset)
            .await
            .map_err(DeferError::Store)?;

        info!(
            key = ?message_key,
            offset = offset,
            topic = %self.topic,
            partition = self.partition,
            "Deferred message for timer-based retry"
        );

        Ok(())
    }
}

impl<T, M, L, D> FallibleHandler for MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Error = DeferError<M::Error, T::Error, L::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // Already deferred: queue behind existing messages (ordering invariant).
        if self
            .store
            .is_deferred(message.key())
            .await
            .map_err(DeferError::Store)?
            .is_some()
        {
            let offset = message.offset();
            return self.append_to_deferred_queue(message.key(), offset).await;
        }

        // Not deferred: try handler, defer on transient failure if enabled.
        let message_key = message.key().clone();
        let offset = message.offset();

        let Err(error) = self
            .handler
            .on_message(context.clone(), message, demand_type)
            .await
        else {
            return Ok(());
        };

        if !matches!(error.classify_error(), ErrorCategory::Transient) {
            return Err(DeferError::Handler(error));
        }

        // Only gate initial deferral; once deferred, always re-defer transient.
        if !self.config.enabled {
            debug!(
                key = ?message_key,
                offset = offset,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: middleware disabled"
            );
            return Err(DeferError::Handler(error));
        }

        if !self.decider.should_defer() {
            debug!(
                key = ?message_key,
                offset = offset,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: decider threshold not met"
            );
            return Err(DeferError::Handler(error));
        }

        self.defer_message(context, &message_key, offset).await
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
        if trigger.timer_type != TimerType::DeferredMessage {
            return self
                .handler
                .on_timer(context, trigger, demand_type)
                .await
                .map_err(DeferError::Handler);
        }

        let message_key = &trigger.key;

        debug!(
            key = ?message_key,
            scheduled_time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            "Defer retry timer fired"
        );

        let Some((offset, retry_count)) = self
            .store
            .get_next_deferred_message(message_key)
            .await
            .map_err(DeferError::Store)?
        else {
            // Queue empty (race or already processed): clear orphaned timer.
            debug!(
                key = ?message_key,
                topic = %self.topic,
                partition = self.partition,
                "Clearing orphaned defer timer: queue empty"
            );

            // Clean up any orphaned store state for this key
            self.store
                .delete_key(message_key)
                .await
                .map_err(DeferError::Store)?;

            return Ok(());
        };

        let Some(message) = self
            .load_deferred_message(&context, message_key, offset, retry_count)
            .await?
        else {
            return Ok(()); // Load failure handled internally.
        };

        debug!(
            key = ?message_key,
            offset = offset,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Loaded deferred message - attempting retry"
        );

        // Retry handler; on failure, handle_retry_failure maintains invariants.
        match self
            .handler
            .on_message(context.clone(), message, DemandType::Failure)
            .await
        {
            Ok(()) => {
                self.complete_and_advance(&context, message_key, offset)
                    .await?;

                info!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Deferred message retry succeeded"
                );
                Ok(())
            }
            Err(error) => {
                self.handle_retry_failure(&context, message_key, offset, retry_count, error)
                    .await
            }
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
