//! Defer middleware handler implementation.
//!
//! Implements the defer middleware that intercepts transient failures and
//! defers them for later retry using the timer system. This allows the system
//! to handle temporary failures (e.g., downstream service outages) without
//! blocking partition processing.

use super::DeferState;
use super::config::DeferConfiguration;
use super::error::{DeferError, DeferInitError};
use super::failure_tracker::FailureTracker;
use super::loader::{KafkaLoader, MessageLoader};
use super::store::{CachedDeferStore, DeferStore, RetryCompletionResult};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{ConsumerConfiguration, DemandType, EventHandler, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};
use crate::{Key, Partition, Topic};
use std::sync::Arc;
use tracing::{debug, error};

/// Middleware that defers transiently failed messages for later retry.
///
/// Intercepts transient failures from the inner handler and schedules them
/// for retry using the timer system, allowing partition processing to
/// continue without blocking.
///
/// # Type Parameters
///
/// * `S` - Store implementation for deferred state
/// * `L` - Loader implementation for retrieving messages by offset
#[derive(Clone)]
pub struct DeferMiddleware<S, L = KafkaLoader>
where
    S: DeferStore,
    L: MessageLoader,
{
    config: DeferConfiguration,
    loader: L,
    store: S,
    failure_tracker: FailureTracker,
    consumer_group: Arc<str>,
}

impl<S> DeferMiddleware<S, KafkaLoader>
where
    S: DeferStore,
{
    /// Creates a new defer middleware with the given configuration and store.
    ///
    /// Uses [`KafkaLoader`] to load messages from Kafka for retry.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for defer behavior
    /// * `consumer_config` - Consumer configuration for shared Kafka settings
    /// * `scheduler_config` - Scheduler configuration for max concurrency
    /// * `store` - Storage backend for deferred state
    /// * `telemetry` - Telemetry system for failure tracking
    /// * `heartbeats` - Registry for monitoring background actors
    ///
    /// # Returns
    ///
    /// A `Result` containing the new middleware if configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration validation fails or `KafkaLoader`
    /// construction fails.
    pub fn new(
        config: DeferConfiguration,
        consumer_config: &ConsumerConfiguration,
        scheduler_config: &SchedulerConfiguration,
        store: S,
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

        let failure_tracker = FailureTracker::new(
            config.failure_window,
            config.failure_threshold,
            telemetry,
            heartbeats,
        );

        Ok(Self {
            config,
            loader,
            store,
            failure_tracker,
            consumer_group: Arc::from(consumer_config.group_id.as_str()),
        })
    }
}

impl<S, L> DeferMiddleware<S, L>
where
    S: DeferStore,
    L: MessageLoader,
{
    /// Creates a new defer middleware with a custom loader.
    ///
    /// Use this constructor for testing with [`MemoryLoader`] or other
    /// custom loader implementations.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for defer behavior
    /// * `consumer_group` - Consumer group ID for state isolation
    /// * `loader` - Message loader implementation
    /// * `store` - Storage backend for deferred state
    /// * `telemetry` - Telemetry system for failure tracking
    /// * `heartbeats` - Registry for monitoring background actors
    ///
    /// # Returns
    ///
    /// A `Result` containing the new middleware if configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration validation fails.
    pub fn with_loader<G>(
        config: DeferConfiguration,
        consumer_group: G,
        loader: L,
        store: S,
        telemetry: &Telemetry,
        heartbeats: &HeartbeatRegistry,
    ) -> Result<Self, DeferInitError>
    where
        G: Into<Arc<str>>,
    {
        use validator::Validate;

        config.validate()?;

        let failure_tracker = FailureTracker::new(
            config.failure_window,
            config.failure_threshold,
            telemetry,
            heartbeats,
        );

        Ok(Self {
            config,
            loader,
            store,
            failure_tracker,
            consumer_group: consumer_group.into(),
        })
    }
}

/// Provider that creates defer handlers for each partition.
#[derive(Clone)]
pub struct DeferProvider<T, S, L = KafkaLoader>
where
    S: DeferStore,
    L: MessageLoader,
{
    provider: T,
    config: DeferConfiguration,
    loader: L,
    store: S,
    failure_tracker: FailureTracker,
    consumer_group: Arc<str>,
}

/// Handler that implements defer logic for a specific partition.
///
/// # Type Parameters
///
/// * `T` - Inner handler type
/// * `S` - Store implementation (`DeferStore`, typically wrapped in
///   `CachedDeferStore`)
/// * `L` - Loader implementation for retrieving messages by offset
#[derive(Clone)]
pub struct DeferHandler<T, S, L = KafkaLoader>
where
    S: DeferStore,
    L: MessageLoader,
{
    handler: T,
    loader: L,
    store: S,
    failure_tracker: FailureTracker,
    config: DeferConfiguration,
    topic: Topic,
    partition: Partition,
    consumer_group: Arc<str>,
}

impl<S, L> HandlerMiddleware for DeferMiddleware<S, L>
where
    S: DeferStore,
    L: MessageLoader + 'static,
{
    type Provider<T: FallibleHandlerProvider> = DeferProvider<T, S, L>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        DeferProvider {
            provider,
            config: self.config.clone(),
            loader: self.loader.clone(),
            store: self.store.clone(),
            failure_tracker: self.failure_tracker.clone(),
            consumer_group: self.consumer_group.clone(),
        }
    }
}

impl<T, S, L> FallibleHandlerProvider for DeferProvider<T, S, L>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
{
    type Handler = DeferHandler<T::Handler, CachedDeferStore<S>, L>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        // Wrap store in cache for this partition handler.
        // Each partition gets its own cache with the same lifecycle as the handler.
        let cached_store = CachedDeferStore::new(self.store.clone(), self.config.cache_size);

        DeferHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            loader: self.loader.clone(),
            store: cached_store,
            failure_tracker: self.failure_tracker.clone(),
            config: self.config.clone(),
            topic,
            partition,
            consumer_group: self.consumer_group.clone(),
        }
    }
}

// Note: DeferMiddleware only works with FallibleHandler.
// HandlerProvider implementation removed as it requires non-fallible handlers
// which don't support error classification needed for defer logic.

impl<T, S, L> DeferHandler<T, S, L>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
{
    /// Checks the defer state for a key.
    ///
    /// Returns `Some(DeferState)` if key has deferred messages, `None`
    /// otherwise.
    async fn check_defer_state(
        &self,
        _key: &Key,
        key_id: &uuid::Uuid,
    ) -> Result<Option<DeferState>, DeferError<S::Error, T::Error, L::Error>> {
        // Query store (cached internally by CachedDeferStore)
        let retry_count = self
            .store
            .is_deferred(key_id)
            .await
            .map_err(DeferError::Store)?;

        Ok(retry_count.map(|retry_count| DeferState::Deferred { retry_count }))
    }

    /// Handles a new message arriving for an already-deferred key.
    ///
    /// Appends the message to the deferred queue without processing it.
    async fn handle_deferred_key_message(
        &self,
        key: &Key,
        key_id: &uuid::Uuid,
        offset: crate::Offset,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>> {
        use crate::timers::datetime::CompactDateTime;

        // Calculate expected retry time using current backoff
        let delay = self.calculate_backoff(retry_count);
        let now = CompactDateTime::now()?;
        let expected_retry_time = now.add_duration(delay)?;

        // Append to deferred queue (doesn't modify retry_count)
        self.store
            .defer_additional_message(key_id, offset, expected_retry_time)
            .await
            .map_err(DeferError::Store)?;

        debug!(
            "Appended offset {} to deferred queue for key {:?} (retry_count={})",
            offset, key, retry_count
        );

        Ok(())
    }

    /// Calculates the backoff delay for a given retry count.
    ///
    /// Uses exponential backoff: base * `2^retry_count`, capped at `max_delay`.
    fn calculate_backoff(&self, retry_count: u32) -> CompactDuration {
        use rand::Rng;
        use std::cmp::min;

        let base_seconds = self.config.base.as_secs();
        let max_delay_seconds = self.config.max_delay.as_secs();

        // Calculate exponential backoff: base * 2^retry_count
        // Using checked operations to avoid overflow
        let multiplier = 2_u64.checked_pow(retry_count).unwrap_or(u64::MAX);
        let delay_seconds = base_seconds.saturating_mul(multiplier);

        // Cap at max_delay
        let capped_seconds = min(delay_seconds, max_delay_seconds);

        // Apply full jitter: random(0, capped_seconds)
        // This prevents thundering herd when many keys retry simultaneously
        let mut rng = rand::rng();
        let jittered_seconds = if capped_seconds > 0 {
            rng.random_range(0..=capped_seconds)
        } else {
            0
        };

        // Convert to u32 for CompactDuration, saturating at MAX
        let compact_seconds = u32::try_from(jittered_seconds).unwrap_or(u32::MAX);

        CompactDuration::new(compact_seconds)
    }

    /// Schedule a retry timer with the given retry count.
    async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        use crate::timers::datetime::CompactDateTime;

        let delay = self.calculate_backoff(retry_count);
        let now = CompactDateTime::now()?;
        let fire_time = now.add_duration(delay)?;

        context
            .clear_and_schedule(fire_time, TimerType::DeferRetry)
            .await
            .map_err(|e| DeferError::Timer(Box::new(e)))?;

        Ok(())
    }

    /// Handle successful retry of a deferred message.
    async fn handle_retry_success<C>(
        &self,
        context: &C,
        key: &Key,
        key_id: &uuid::Uuid,
        offset: crate::Offset,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        // Complete successful retry and prepare for next message or cleanup
        let result = self
            .store
            .complete_retry_success(key_id, offset)
            .await
            .map_err(DeferError::Store)?;

        match result {
            RetryCompletionResult::MoreMessages { next_offset } => {
                // More messages in queue - retry_count has been reset to 0
                // (cache updated by CachedDeferStore)

                // Schedule timer for the next deferred message
                self.schedule_retry_timer(context, 0).await?;

                debug!(
                    "Scheduled next retry for key {:?} at offset {} (more messages in queue)",
                    key, next_offset
                );
            }
            RetryCompletionResult::Completed => {
                // No more messages - key has been deleted from storage
                // (cache updated by CachedDeferStore)
            }
        }

        debug!(
            "Deferred message succeeded for key {:?} after {} retries",
            key, retry_count
        );

        Ok(())
    }

    /// Handle failed retry of a deferred message.
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        key: &Key,
        key_id: &uuid::Uuid,
        offset: crate::Offset,
        retry_count: u32,
        error: T::Error,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        use super::error::ConfigurationError;
        use crate::consumer::middleware::ClassifyError;

        // Check error classification - only defer transient (recoverable) errors
        if let ErrorCategory::Transient = error.classify_error() {
            // Check if we should defer again
            if !self.failure_tracker.should_defer() {
                debug!("Deferral disabled due to high failure rate");
                // Clean up this offset and propagate error
                self.store
                    .remove_deferred_message(key_id, offset)
                    .await
                    .map_err(DeferError::Store)?;
                return Err(DeferError::Configuration(ConfigurationError::Invalid(
                    format!("Deferral disabled: {error}"),
                )));
            }

            // Increment retry count (cache updated by CachedDeferStore)
            let new_retry_count = self
                .store
                .increment_retry_count(key_id, retry_count)
                .await
                .map_err(DeferError::Store)?;

            // Schedule next retry with new backoff
            self.schedule_retry_timer(context, new_retry_count).await?;

            debug!(
                "Re-deferred message for key {:?} (retry_count now {})",
                key, new_retry_count
            );

            Ok(())
        } else {
            // Transient or terminal error - clean up and propagate
            // (cache invalidated by CachedDeferStore)
            self.store
                .remove_deferred_message(key_id, offset)
                .await
                .map_err(DeferError::Store)?;
            Err(DeferError::Handler(error))
        }
    }

    /// Defers a message for later retry.
    ///
    /// Stores the offset metadata and schedules a timer for retry.
    /// For first failure, sets `retry_count` to 0 in the store.
    /// For subsequent failures on the same key, increments the existing
    /// `retry_count`.
    async fn defer_message<C>(
        &self,
        context: C,
        key: &Key,
        message: &ConsumerMessage,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        use crate::consumer::middleware::defer::generate_key_id;
        use crate::timers::datetime::CompactDateTime;

        // Generate UUID for this key
        let key_id = generate_key_id(
            self.consumer_group.as_ref(),
            &self.topic,
            self.partition,
            key,
        );

        // Query store to check if key already has deferred messages
        // (cached internally by CachedDeferStore)
        let retry_count = self
            .store
            .is_deferred(&key_id)
            .await
            .map_err(DeferError::Store)?
            .unwrap_or(0); // If not deferred, this is the first failure (retry_count = 0)

        // Calculate backoff delay
        let delay = self.calculate_backoff(retry_count);

        // Calculate expected retry time for TTL
        let now = CompactDateTime::now()?;
        let expected_retry_time = now.add_duration(delay)?;

        // CRITICAL ORDERING (Invariant 1: Timer Coverage):
        // 1. Schedule timer FIRST
        // 2. Write to store (cache updated automatically by CachedDeferStore)
        //
        // If timer scheduling fails, error propagates and nothing is stored.
        // If store write fails after timer scheduled, error propagates to
        // RetryMiddleware which retries the entire operation (including timer
        // scheduling).
        //
        // This ensures every deferred message has timer coverage.

        // 1. Schedule timer FIRST (for first failure only)
        if retry_count == 0 {
            context
                .schedule(expected_retry_time, TimerType::DeferRetry)
                .await
                .map_err(|e| DeferError::Timer(Box::new(e)))?;
        }

        // 2. Write to storage (cache updated by CachedDeferStore)
        if retry_count == 0 {
            // First failure - initialize with retry_count=0
            self.store
                .defer_first_message(&key_id, message.offset(), expected_retry_time)
                .await
                .map_err(DeferError::Store)?;
        } else {
            // Additional message for already-deferred key
            self.store
                .defer_additional_message(&key_id, message.offset(), expected_retry_time)
                .await
                .map_err(DeferError::Store)?;
        }

        debug!(
            "Deferred message for key {:?} (retry_count={}, delay={} seconds)",
            key,
            retry_count,
            delay.seconds()
        );

        Ok(())
    }
}

impl<T, S, L> FallibleHandler for DeferHandler<T, S, L>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
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
        use super::error::ConfigurationError;
        use crate::consumer::Keyed;
        use crate::consumer::middleware::defer::generate_key_id;

        let key = message.key().clone();
        let key_id = generate_key_id(
            self.consumer_group.as_ref(),
            &self.topic,
            self.partition,
            &key,
        );

        // Check if this key is already deferred
        let defer_state = self.check_defer_state(&key, &key_id).await?;

        // If key is already deferred, append this message to the queue
        if let Some(DeferState::Deferred { retry_count }) = defer_state {
            return self
                .handle_deferred_key_message(&key, &key_id, message.offset(), retry_count)
                .await;
        }

        // Key is not deferred - try to process the message with the inner handler
        match self
            .handler
            .on_message(context.clone(), message.clone(), demand_type)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                // Check error classification - only defer transient (recoverable) errors
                if let ErrorCategory::Transient = error.classify_error() {
                    // Check if deferral is enabled based on failure rate
                    if !self.failure_tracker.should_defer() {
                        debug!("Deferral disabled due to high failure rate");
                        return Err(DeferError::Configuration(ConfigurationError::Invalid(
                            format!("Deferral disabled: {error}"),
                        )));
                    }

                    // Handle deferral
                    self.defer_message(context, &key, &message).await?;
                    Ok(())
                } else {
                    // Transient or terminal errors - don't defer, propagate immediately
                    Err(DeferError::Handler(error))
                }
            }
        }
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
        use crate::consumer::Keyed;
        use crate::consumer::middleware::defer::generate_key_id;

        // Check if this is a defer retry timer
        if trigger.timer_type != TimerType::DeferRetry {
            // Not our timer, pass to inner handler
            return self
                .handler
                .on_timer(context, trigger, demand_type)
                .await
                .map_err(DeferError::Handler);
        }

        // This is a defer retry timer - load and retry the deferred message
        let key = &trigger.key;

        // Generate UUID for this key
        let key_id = generate_key_id(
            self.consumer_group.as_ref(),
            &self.topic,
            self.partition,
            key,
        );

        // Get the next deferred message for this key
        let (offset, retry_count) = match self.store.get_next_deferred_message(&key_id).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                // No deferred message found - possibly already succeeded or expired
                debug!("No deferred message found for key {:?}", key);
                return Ok(());
            }
            Err(e) => {
                return Err(DeferError::Store(e));
            }
        };

        // Load the actual message
        let message = self
            .loader
            .load_message(self.topic, self.partition, offset)
            .await
            .map_err(DeferError::Loader)?;

        // Verify the key matches (sanity check)
        if message.key() != key {
            debug!("Key mismatch: expected {:?}, got {:?}", key, message.key());
            // Clean up this offset from the deferred queue
            // (cache invalidated by CachedDeferStore)
            self.store
                .remove_deferred_message(&key_id, offset)
                .await
                .map_err(DeferError::Store)?;
            return Ok(());
        }

        // Retry the handler
        match self
            .handler
            .on_message(context.clone(), message.clone(), demand_type)
            .await
        {
            Ok(()) => {
                self.handle_retry_success(&context, key, &key_id, offset, retry_count)
                    .await
            }
            Err(error) => {
                self.handle_retry_failure(&context, key, &key_id, offset, retry_count, error)
                    .await
            }
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

impl<T, S, L> EventHandler for DeferHandler<T, S, L>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
{
    async fn on_message<C>(&self, context: C, message: UncommittedMessage, demand_type: DemandType)
    where
        C: EventContext,
    {
        let (consumer_message, uncommitted_offset) = message.into_inner();

        match <Self as FallibleHandler>::on_message(
            self,
            context.clone(),
            consumer_message,
            demand_type,
        )
        .await
        {
            Ok(()) => {
                uncommitted_offset.commit();
            }
            Err(error) => {
                error!("defer handler failed: {error:#}");
                uncommitted_offset.abort();
            }
        }
    }

    async fn on_timer<C, U>(&self, context: C, timer: U, demand_type: DemandType)
    where
        C: EventContext,
        U: UncommittedTimer,
    {
        let (trigger, uncommitted_timer) = timer.into_inner();

        match <Self as FallibleHandler>::on_timer(self, context.clone(), trigger, demand_type).await
        {
            Ok(()) => {
                uncommitted_timer.commit().await;
            }
            Err(error) => {
                error!("defer timer handler failed: {error:#}");
                uncommitted_timer.abort().await;
            }
        }
    }

    async fn shutdown(self) {
        <Self as FallibleHandler>::shutdown(self).await;
    }
}
