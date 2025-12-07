//! Defer middleware for handling transient failures.
//!
//! When a message fails with a transient error, this middleware defers it
//! for later retry via timers rather than blocking the partition. This
//! maintains partition throughput during downstream outages while preserving
//! per-key ordering guarantees.

use super::config::DeferConfiguration;
use super::decider::DeferralDecider;
use super::error::{DeferError, DeferInitError, DeferResult};
use super::failure_tracker::FailureTracker;
use super::loader::{KafkaLoader, MessageLoader};
use super::store::{
    CachedDeferStore, DeferStore, DeferStoreProvider, LazyStore, RetryCompletionResult,
    StoreFactory,
};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{ConsumerConfiguration, DemandType, Keyed};
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use rand::Rng;
use std::cmp::min;
use std::sync::Arc;
use tracing::{debug, warn};

/// Test support module for property-based testing of defer handler.
#[cfg(test)]
pub mod tests;

/// Middleware that defers transiently failed messages for later retry.
///
/// Wraps an inner handler and intercepts transient failures. Failed messages
/// are stored with their metadata and a timer is scheduled. When the timer
/// fires, the message is reloaded from Kafka and retried.
#[derive(Clone)]
pub struct DeferMiddleware<P, L = KafkaLoader, D = FailureTracker>
where
    P: DeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    loader: L,
    provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
}

impl<P> DeferMiddleware<P, KafkaLoader, FailureTracker>
where
    P: DeferStoreProvider,
{
    /// Creates the middleware with default [`KafkaLoader`] and
    /// [`FailureTracker`].
    ///
    /// # Errors
    ///
    /// Returns an error if configuration validation or `KafkaLoader`
    /// construction fails.
    pub fn new(
        config: DeferConfiguration,
        consumer_config: &ConsumerConfiguration,
        scheduler_config: &SchedulerConfiguration,
        provider: P,
        telemetry: &Telemetry,
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

        let decider = FailureTracker::new(
            config.failure_window,
            config.failure_threshold,
            telemetry,
            heartbeats,
        );

        Ok(Self {
            config,
            loader,
            provider,
            decider,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
        })
    }
}

/// Provider that creates defer handlers for each partition.
#[derive(Clone)]
pub struct DeferProvider<T, P, L = KafkaLoader, D = FailureTracker>
where
    P: DeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    provider: T,
    config: DeferConfiguration,
    loader: L,
    store_provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
}

/// Per-partition handler that wraps an inner handler with defer logic.
#[derive(Clone)]
pub struct DeferHandler<T, S, L = KafkaLoader, D = FailureTracker>
where
    S: DeferStore,
    L: MessageLoader,
    D: DeferralDecider,
{
    pub(crate) handler: T,
    pub(crate) loader: L,
    pub(crate) store: S,
    pub(crate) decider: D,
    pub(crate) config: DeferConfiguration,
    pub(crate) topic: Topic,
    pub(crate) partition: Partition,
}

impl<P, L, D> HandlerMiddleware for DeferMiddleware<P, L, D>
where
    P: DeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Provider<T: FallibleHandlerProvider> = DeferProvider<T, P, L, D>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        DeferProvider {
            provider,
            config: self.config.clone(),
            loader: self.loader.clone(),
            store_provider: self.provider.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
        }
    }
}

/// Factory for creating cached defer stores.
#[derive(Clone)]
pub struct DeferStoreFactory<P: DeferStoreProvider> {
    provider: P,
    topic: Topic,
    partition: Partition,
    consumer_group: ConsumerGroup,
    cache_size: usize,
}

impl<P: DeferStoreProvider> StoreFactory for DeferStoreFactory<P> {
    type Store = CachedDeferStore<P::Store>;

    async fn create(self) -> Result<Self::Store, <Self::Store as DeferStore>::Error> {
        let store = self
            .provider
            .create_store(self.topic, self.partition, &self.consumer_group)
            .await?;
        Ok(CachedDeferStore::new(store, self.cache_size))
    }
}

/// Lazy cached store for [`DeferHandler`].
pub type DeferLazyStore<P> = LazyStore<DeferStoreFactory<P>>;

impl<T, P, L, D> FallibleHandlerProvider for DeferProvider<T, P, L, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    P: DeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Handler = DeferHandler<T::Handler, DeferLazyStore<P>, L, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let factory = DeferStoreFactory {
            provider: self.store_provider.clone(),
            topic,
            partition,
            consumer_group: self.consumer_group.clone(),
            cache_size: self.config.cache_size,
        };

        DeferHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            loader: self.loader.clone(),
            store: LazyStore::new(factory),
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
        }
    }
}

impl<T, S, L, D> DeferHandler<T, S, L, D>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    // ==================== Backoff & Timing ====================

    /// Calculates jittered exponential backoff: `random(1, min(base *
    /// 2^retry_count, max_delay))`.
    ///
    /// Returns 0 when `retry_count` is 0 (no delay for initial attempt).
    fn calculate_backoff(&self, retry_count: u32) -> CompactDuration {
        // No delay for the initial attempt
        if retry_count == 0 {
            return CompactDuration::MIN;
        }

        let base_seconds = self.config.base.as_secs();
        let max_delay_seconds = self.config.max_delay.as_secs();

        // Calculate exponential backoff: base * 2^(retry_count - 1)
        // Subtract 1 so first retry (count=1) uses base delay
        // Using checked operations to avoid overflow
        let multiplier = 2_u64.checked_pow(retry_count - 1).unwrap_or(u64::MAX);
        let delay_seconds = base_seconds.saturating_mul(multiplier);

        // Cap at max_delay, with minimum of 1 second.
        // Minimum 1 second ensures a meaningful delay when jitter would
        // otherwise produce 0.
        let capped_seconds = min(delay_seconds, max_delay_seconds).max(1);

        // Apply full jitter: random(1, capped_seconds)
        // This prevents thundering herd when many keys retry simultaneously.
        let jittered_seconds = rand::rng().random_range(1..=capped_seconds);

        // Convert to u32 for CompactDuration, saturating at MAX
        let compact_seconds = u32::try_from(jittered_seconds).unwrap_or(u32::MAX);

        CompactDuration::new(compact_seconds)
    }

    /// Returns `now + backoff(retry_count)`.
    fn next_retry_time(
        &self,
        retry_count: u32,
    ) -> DeferResult<CompactDateTime, S::Error, T::Error, L::Error> {
        let delay = self.calculate_backoff(retry_count);
        let now = CompactDateTime::now()?;
        Ok(now.add_duration(delay)?)
    }

    // ==================== Timer Operations ====================

    /// Schedules a `DeferRetry` timer using backoff for the given retry count.
    async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        let fire_time = self.next_retry_time(retry_count)?;

        context
            .clear_and_schedule(fire_time, TimerType::DeferRetry)
            .await
            .map_err(|e| DeferError::Timer(Box::new(e)))?;

        Ok(())
    }

    /// After completing a message, schedules the next retry or clears the
    /// timer.
    async fn schedule_next_or_clear<C>(
        &self,
        context: &C,
        result: RetryCompletionResult,
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        match result {
            RetryCompletionResult::MoreMessages { .. } => {
                // More messages in queue - schedule timer (retry_count reset to 0)
                self.schedule_retry_timer(context, 0).await
            }
            RetryCompletionResult::Completed => {
                // No more messages - clear the timer
                context
                    .clear_scheduled(TimerType::DeferRetry)
                    .await
                    .map_err(|e| DeferError::Timer(Box::new(e)))
            }
        }
    }

    /// Removes a message from the deferred queue and reschedules for remaining.
    ///
    /// Used after successfully processing a message, giving up on a permanent
    /// failure, or skipping a corrupted message. Maintains the invariant that
    /// all remaining messages will be processed.
    async fn complete_and_advance<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
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

    // ==================== Queue Management ====================

    /// Appends a new message to an already-deferred key's queue.
    ///
    /// Per-key ordering requires queuing subsequent messages when a key is
    /// already deferred, rather than processing them out of order.
    async fn append_to_deferred_queue(
        &self,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), S::Error, T::Error, L::Error> {
        self.store
            .defer_additional_message(message_key, offset)
            .await
            .map_err(DeferError::Store)?;

        debug!(
            "Appended offset {} to deferred queue for key {:?}",
            offset, message_key
        );

        Ok(())
    }

    // ==================== Error Handling ====================

    /// Either re-defers on transient error or propagates the error after
    /// cleanup.
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: T::Error,
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        // Transient errors may be re-deferred; permanent errors are cleaned up and
        // propagated
        let is_transient = matches!(error.classify_error(), ErrorCategory::Transient);
        let should_redefer = is_transient && self.decider.should_defer();

        if should_redefer {
            // Increment retry count and schedule next retry
            let new_retry_count = self
                .store
                .increment_retry_count(message_key, retry_count)
                .await
                .map_err(DeferError::Store)?;

            self.schedule_retry_timer(context, new_retry_count).await?;

            debug!(
                "Re-deferred message for key {:?} (retry_count now {})",
                message_key, new_retry_count
            );

            return Ok(());
        }

        // Not re-deferring: clean up this offset and reschedule for remaining
        self.complete_and_advance(context, message_key, offset)
            .await?;

        // Propagate the original handler error
        Err(DeferError::Handler(error))
    }

    /// Loads a deferred message from Kafka.
    ///
    /// Returns `Ok(Some(message))` on success, or `Ok(None)` if the load failed
    /// and was handled internally (timer rescheduled). Only returns `Err` for
    /// terminal errors (shutdown).
    async fn load_deferred_message<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
    ) -> DeferResult<Option<ConsumerMessage>, S::Error, T::Error, L::Error>
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
                "Key mismatch: expected {:?}, got {:?} - skipping offset {}",
                message_key,
                message.key(),
                offset
            );
            self.complete_and_advance(context, message_key, offset)
                .await?;
            return Ok(None);
        }

        Ok(Some(message))
    }

    /// Handles loader errors while maintaining timer coverage invariant.
    ///
    /// - Permanent: complete this offset, schedule timer for next message
    /// - Transient: schedule retry timer for current message
    /// - Terminal: propagate for shutdown
    async fn handle_load_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: L::Error,
    ) -> DeferResult<Option<ConsumerMessage>, S::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        match error.classify_error() {
            ErrorCategory::Permanent => {
                warn!(
                    "Permanent loader error for key {:?} at offset {}: {} - skipping",
                    message_key, offset, error
                );
                self.complete_and_advance(context, message_key, offset)
                    .await?;
            }
            ErrorCategory::Transient => {
                warn!(
                    "Transient loader error for key {:?} at offset {}: {} - scheduling retry",
                    message_key, offset, error
                );
                self.schedule_retry_timer(context, retry_count).await?;
            }
            ErrorCategory::Terminal => {
                return Err(DeferError::Loader(error));
            }
        }
        Ok(None)
    }

    // ==================== Initial Deferral ====================

    /// Defers a message for the first time (key not already deferred).
    async fn defer_message<C>(
        &self,
        context: C,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        // CRITICAL ORDERING (Invariant 1: Timer Coverage):
        // Schedule timer FIRST, then write to store. If timer scheduling fails,
        // error propagates and nothing is stored. If store write fails after timer
        // scheduled, RetryMiddleware retries the entire operation.
        //
        // We use clear_and_schedule to handle retries idempotently.
        self.schedule_retry_timer(&context, 0).await?;

        self.store
            .defer_first_message(message_key, offset)
            .await
            .map_err(DeferError::Store)?;

        debug!("Deferred message for key {:?}", message_key);

        Ok(())
    }
}

impl<T, S, L, D> FallibleHandler for DeferHandler<T, S, L, D>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Error = DeferError<S::Error, T::Error, L::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        // If key is already deferred, append this message to the queue.
        // Borrow from message to avoid cloning in this path.
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

        // Key is not deferred - clone key before handler consumes message
        let message_key = message.key().clone();
        let offset = message.offset();

        // Try to process the message with the inner handler
        let Err(error) = self
            .handler
            .on_message(context.clone(), message, demand_type)
            .await
        else {
            return Ok(());
        };

        // Only defer transient (recoverable) errors
        if !matches!(error.classify_error(), ErrorCategory::Transient) {
            return Err(DeferError::Handler(error));
        }

        // Check if deferral is enabled based on failure rate
        if !self.decider.should_defer() {
            debug!("Deferral disabled due to high failure rate");
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
        // Not our timer - pass to inner handler
        if trigger.timer_type != TimerType::DeferRetry {
            return self
                .handler
                .on_timer(context, trigger, demand_type)
                .await
                .map_err(DeferError::Handler);
        }

        let message_key = &trigger.key;

        // Step 1: Get the next deferred message metadata from store.
        // The offset must remain in scope for error handling paths.
        let Some((offset, retry_count)) = self
            .store
            .get_next_deferred_message(message_key)
            .await
            .map_err(DeferError::Store)?
        else {
            debug!(
                "No deferred message found for key {:?}, clearing timer",
                message_key
            );
            context
                .clear_scheduled(TimerType::DeferRetry)
                .await
                .map_err(|e| DeferError::Timer(Box::new(e)))?;
            return Ok(());
        };

        // Step 2: Load and validate the message from Kafka.
        let Some(message) = self
            .load_deferred_message(&context, message_key, offset, retry_count)
            .await?
        else {
            // Load failure handled: timer rescheduled for next or current message.
            return Ok(());
        };

        // Step 3: Retry the handler
        match self
            .handler
            .on_message(context.clone(), message, DemandType::Failure)
            .await
        {
            Ok(()) => {
                self.complete_and_advance(&context, message_key, offset)
                    .await?;
                debug!(
                    "Deferred message succeeded for key {:?} after {} retries",
                    message_key, retry_count
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
