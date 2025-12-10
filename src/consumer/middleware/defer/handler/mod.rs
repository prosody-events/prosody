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
use tracing::{debug, info, warn};

/// Property-based tests for defer handler invariants.
#[cfg(test)]
pub mod tests;

/// Middleware that defers transiently-failed messages for timer-based retry.
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
    /// Creates middleware with default [`KafkaLoader`] and [`FailureTracker`].
    ///
    /// # Errors
    ///
    /// Returns error if config validation or loader construction fails.
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

/// Creates [`DeferHandler`]s for each partition.
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

/// Per-partition handler wrapping an inner handler with defer logic.
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

/// Factory for cached defer stores.
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

/// Lazily-initialized cached store for [`DeferHandler`].
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
    /// Jittered exponential backoff: `random(1, min(base * 2^retry, max))`.
    /// Returns 0 for `retry_count == 0`.
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

    /// Returns `now + backoff(retry_count)`; used for scheduling timers.
    fn next_retry_time(
        &self,
        retry_count: u32,
    ) -> DeferResult<CompactDateTime, S::Error, T::Error, L::Error> {
        let delay = self.calculate_backoff(retry_count);
        let now = CompactDateTime::now()?;
        Ok(now.add_duration(delay)?)
    }

    /// Schedules a `DeferRetry` timer with backoff based on retry count.
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

    /// Removes message from queue and schedules timer for next (or clears).
    /// Used after success, permanent failure, or skipping corrupted messages.
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

    /// Appends message to an already-deferred key's queue (maintains ordering).
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
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
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
    ) -> DeferResult<Option<ConsumerMessage>, S::Error, T::Error, L::Error>
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
                warn!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Transient loader error - scheduling retry: {error:#}"
                );
                self.schedule_retry_timer(context, retry_count).await?;
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
    ) -> DeferResult<(), S::Error, T::Error, L::Error>
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
        if trigger.timer_type != TimerType::DeferRetry {
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
            context
                .clear_scheduled(TimerType::DeferRetry)
                .await
                .map_err(|e| DeferError::Timer(Box::new(e)))?;
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
