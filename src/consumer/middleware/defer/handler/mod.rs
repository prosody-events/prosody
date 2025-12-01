//! Defer middleware handler implementation.
//!
//! Implements the defer middleware that intercepts transient failures and
//! defers them for later retry using the timer system. This allows the system
//! to handle temporary failures (e.g., downstream service outages) without
//! blocking partition processing.

use super::DeferState;
use super::config::DeferConfiguration;
use super::decider::DeferralDecider;
use super::error::{DeferError, DeferInitError};
use super::failure_tracker::FailureTracker;
use super::loader::{KafkaLoader, MessageLoader};
use super::store::{CachedDeferStore, DeferStore, DeferStoreProvider, RetryCompletionResult};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::scheduler::SchedulerConfiguration;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{ConsumerConfiguration, DemandType, EventHandler, Uncommitted};
use crate::heartbeat::HeartbeatRegistry;
use crate::telemetry::Telemetry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::{debug, error};

/// Test support module for property-based testing of defer handler.
#[cfg(test)]
pub mod tests;

/// Middleware that defers transiently failed messages for later retry.
///
/// Intercepts transient failures from the inner handler and schedules them
/// for retry using the timer system, allowing partition processing to
/// continue without blocking.
///
/// # Type Parameters
///
/// * `P` - Store provider implementation for creating partition-specific stores
/// * `L` - Loader implementation for retrieving messages by offset
/// * `D` - Deferral decision implementation
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
    /// Creates a new defer middleware with the given configuration and store
    /// provider.
    ///
    /// Uses [`KafkaLoader`] to load messages from Kafka for retry and
    /// [`FailureTracker`] for deferral decisions.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for defer behavior
    /// * `consumer_config` - Consumer configuration for shared Kafka settings
    /// * `scheduler_config` - Scheduler configuration for max concurrency
    /// * `provider` - Store provider for creating partition-specific stores
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

/// Lazy-initialized store wrapper for deferred message storage.
///
/// This wrapper allows the store to be initialized asynchronously on first use,
/// propagating initialization errors to callers instead of panicking during
/// handler construction.
///
/// # Thread Safety
///
/// Uses `tokio::sync::OnceCell` for safe concurrent initialization. The first
/// caller to access the store triggers initialization; subsequent callers wait
/// for or reuse the result.
#[derive(Clone)]
pub struct LazyDeferStore<P>
where
    P: DeferStoreProvider,
{
    /// Lazy cell for the cached store.
    cell: Arc<OnceCell<CachedDeferStore<P::Store>>>,
    /// Store provider for initialization.
    provider: P,
    /// Topic for this store.
    topic: Topic,
    /// Partition for this store.
    partition: Partition,
    /// Consumer group for segment identification.
    consumer_group: ConsumerGroup,
    /// Cache size for the cached store wrapper.
    cache_size: usize,
}

impl<P> LazyDeferStore<P>
where
    P: DeferStoreProvider,
{
    /// Creates a new lazy store wrapper.
    ///
    /// The underlying store is not initialized until first use.
    fn new(
        provider: P,
        topic: Topic,
        partition: Partition,
        consumer_group: ConsumerGroup,
        cache_size: usize,
    ) -> Self {
        Self {
            cell: Arc::new(OnceCell::new()),
            provider,
            topic,
            partition,
            consumer_group,
            cache_size,
        }
    }

    /// Gets or initializes the underlying store.
    ///
    /// # Errors
    ///
    /// Returns the provider's error if store creation fails.
    async fn get_store(&self) -> Result<&CachedDeferStore<P::Store>, P::Error> {
        self.cell
            .get_or_try_init(|| async {
                let store = self
                    .provider
                    .create_store(self.topic, self.partition, &self.consumer_group)
                    .await?;
                Ok(CachedDeferStore::new(store, self.cache_size))
            })
            .await
    }
}

impl<P> DeferStore for LazyDeferStore<P>
where
    P: DeferStoreProvider,
{
    type Error = P::Error;

    async fn defer_first_message(
        &self,
        key: &Key,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.get_store()
            .await?
            .defer_first_message(key, offset, expected_retry_time)
            .await
    }

    async fn defer_additional_message(
        &self,
        key: &Key,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.get_store()
            .await?
            .defer_additional_message(key, offset, expected_retry_time)
            .await
    }

    async fn complete_retry_success(
        &self,
        key: &Key,
        offset: Offset,
    ) -> Result<RetryCompletionResult, Self::Error> {
        self.get_store()
            .await?
            .complete_retry_success(key, offset)
            .await
    }

    async fn increment_retry_count(
        &self,
        key: &Key,
        current_retry_count: u32,
    ) -> Result<u32, Self::Error> {
        self.get_store()
            .await?
            .increment_retry_count(key, current_retry_count)
            .await
    }

    async fn get_next_deferred_message(
        &self,
        key: &Key,
    ) -> Result<Option<(Offset, u32)>, Self::Error> {
        self.get_store().await?.get_next_deferred_message(key).await
    }

    async fn is_deferred(&self, key: &Key) -> Result<Option<u32>, Self::Error> {
        self.get_store().await?.is_deferred(key).await
    }

    async fn append_deferred_message(
        &self,
        key: &Key,
        offset: Offset,
        expected_retry_time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.get_store()
            .await?
            .append_deferred_message(key, offset, expected_retry_time)
            .await
    }

    async fn remove_deferred_message(&self, key: &Key, offset: Offset) -> Result<(), Self::Error> {
        self.get_store()
            .await?
            .remove_deferred_message(key, offset)
            .await
    }

    async fn set_retry_count(&self, key: &Key, retry_count: u32) -> Result<(), Self::Error> {
        self.get_store()
            .await?
            .set_retry_count(key, retry_count)
            .await
    }

    async fn delete_key(&self, key: &Key) -> Result<(), Self::Error> {
        self.get_store().await?.delete_key(key).await
    }
}

/// Handler that implements defer logic for a specific partition.
///
/// # Type Parameters
///
/// * `T` - Inner handler type
/// * `S` - Store implementation (`DeferStore`, typically wrapped in
///   `CachedDeferStore`)
/// * `L` - Loader implementation for retrieving messages by offset
/// * `D` - Deferral decision implementation
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

impl<T, P, L, D> FallibleHandlerProvider for DeferProvider<T, P, L, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    P: DeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    type Handler = DeferHandler<T::Handler, LazyDeferStore<P>, L, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        // Create lazy store wrapper that initializes on first use.
        // This allows handler_for_partition to be synchronous while deferring
        // the async store initialization to the first handler operation.
        // Initialization errors propagate to callers of on_message/on_timer.
        let lazy_store = LazyDeferStore::new(
            self.store_provider.clone(),
            topic,
            partition,
            self.consumer_group.clone(),
            self.config.cache_size,
        );

        DeferHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            loader: self.loader.clone(),
            store: lazy_store,
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
        }
    }
}

// Note: DeferMiddleware only works with FallibleHandler.
// HandlerProvider implementation removed as it requires non-fallible handlers
// which don't support error classification needed for defer logic.

impl<T, S, L, D> DeferHandler<T, S, L, D>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    /// Checks the defer state for a key.
    ///
    /// Returns `Some(DeferState)` if key has deferred messages, `None`
    /// otherwise.
    async fn check_defer_state(
        &self,
        key: &Key,
    ) -> Result<Option<DeferState>, DeferError<S::Error, T::Error, L::Error>> {
        // Query store (cached internally by CachedDeferStore)
        let retry_count = self
            .store
            .is_deferred(key)
            .await
            .map_err(DeferError::Store)?;

        Ok(retry_count.map(|retry_count| DeferState::Deferred { retry_count }))
    }

    /// Handles a new message arriving for an already-deferred key.
    ///
    /// Appends the message to the deferred queue without processing it.
    async fn handle_deferred_key_message(
        &self,
        message_key: &Key,
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
            .defer_additional_message(message_key, offset, expected_retry_time)
            .await
            .map_err(DeferError::Store)?;

        debug!(
            "Appended offset {} to deferred queue for key {:?} (retry_count={})",
            offset, message_key, retry_count
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

        // Apply full jitter: random(1, capped_seconds)
        // This prevents thundering herd when many keys retry simultaneously.
        // Minimum 1 second prevents ConflictsWithCurrentTimer error when jitter
        // would otherwise produce 0, causing the new timer to be scheduled at
        // the same time as the currently-processing timer.
        let mut rng = rand::rng();
        let jittered_seconds = if capped_seconds > 1 {
            rng.random_range(1..=capped_seconds)
        } else {
            1
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
        message_key: &Key,
        offset: crate::Offset,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        // Complete successful retry and prepare for next message or cleanup
        let result = self
            .store
            .complete_retry_success(message_key, offset)
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
                    message_key, next_offset
                );
            }
            RetryCompletionResult::Completed => {
                // No more messages - key has been deleted from storage
                // (cache updated by CachedDeferStore)
                // Clear the timer since we won't be retrying
                context
                    .clear_scheduled(TimerType::DeferRetry)
                    .await
                    .map_err(|e| DeferError::Timer(Box::new(e)))?;
            }
        }

        debug!(
            "Deferred message succeeded for key {:?} after {} retries",
            message_key, retry_count
        );

        Ok(())
    }

    /// Handle failed retry of a deferred message.
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
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
            if !self.decider.should_defer() {
                debug!("Deferral disabled due to high failure rate");
                // Clean up this offset and propagate error
                self.store
                    .remove_deferred_message(message_key, offset)
                    .await
                    .map_err(DeferError::Store)?;
                return Err(DeferError::Configuration(ConfigurationError::Invalid(
                    format!("Deferral disabled: {error}"),
                )));
            }

            // Increment retry count (cache updated by CachedDeferStore)
            let new_retry_count = self
                .store
                .increment_retry_count(message_key, retry_count)
                .await
                .map_err(DeferError::Store)?;

            // Schedule next retry with new backoff
            self.schedule_retry_timer(context, new_retry_count).await?;

            debug!(
                "Re-deferred message for key {:?} (retry_count now {})",
                message_key, new_retry_count
            );

            Ok(())
        } else {
            // Permanent or terminal error - clean up current offset but check for more
            // messages (cache updated by CachedDeferStore)
            let result = self
                .store
                .complete_retry_success(message_key, offset)
                .await
                .map_err(DeferError::Store)?;

            // Handle timer management before propagating the error
            match result {
                RetryCompletionResult::MoreMessages { next_offset } => {
                    // Schedule timer for the next deferred message (retry_count reset to 0)
                    self.schedule_retry_timer(context, 0).await?;

                    debug!(
                        "Permanent error for key {:?} offset {}, scheduled timer for next offset \
                         {}",
                        message_key, offset, next_offset
                    );
                }
                RetryCompletionResult::Completed => {
                    // No more messages - clear the timer
                    context
                        .clear_scheduled(TimerType::DeferRetry)
                        .await
                        .map_err(|e| DeferError::Timer(Box::new(e)))?;
                }
            }

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
        message_key: &Key,
        message: &ConsumerMessage,
    ) -> Result<(), DeferError<S::Error, T::Error, L::Error>>
    where
        C: EventContext,
    {
        // Query store to check if key already has deferred messages
        // (cached internally by CachedDeferStore)
        let retry_count = self
            .store
            .is_deferred(message_key)
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
                .defer_first_message(message_key, message.offset(), expected_retry_time)
                .await
                .map_err(DeferError::Store)?;
        } else {
            // Additional message for already-deferred key
            self.store
                .defer_additional_message(message_key, message.offset(), expected_retry_time)
                .await
                .map_err(DeferError::Store)?;
        }

        debug!(
            "Deferred message for key {:?} (retry_count={}, delay={} seconds)",
            message_key,
            retry_count,
            delay.seconds()
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
        use super::error::ConfigurationError;
        use crate::consumer::Keyed;

        let message_key = message.key().clone();

        // Check if this key is already deferred
        let defer_state = self.check_defer_state(&message_key).await?;

        // If key is already deferred, append this message to the queue
        if let Some(DeferState::Deferred { retry_count }) = defer_state {
            return self
                .handle_deferred_key_message(&message_key, message.offset(), retry_count)
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
                    if !self.decider.should_defer() {
                        debug!("Deferral disabled due to high failure rate");
                        return Err(DeferError::Configuration(ConfigurationError::Invalid(
                            format!("Deferral disabled: {error}"),
                        )));
                    }

                    // Handle deferral
                    self.defer_message(context, &message_key, &message).await?;
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
        let message_key = &trigger.key;

        // Get the next deferred message for this key
        let (offset, retry_count) = match self.store.get_next_deferred_message(message_key).await {
            Ok(Some(result)) => {
                tracing::info!(
                    "on_timer: got deferred message for key {:?}: offset={}, retry_count={}",
                    message_key,
                    result.0,
                    result.1
                );
                result
            }
            Ok(None) => {
                // No deferred message found - possibly already succeeded or expired
                tracing::info!(
                    "on_timer: No deferred message found for key {:?}",
                    message_key
                );
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

        tracing::info!(
            "on_timer: loaded message for offset {}: key={:?}",
            offset,
            message.key()
        );

        // Verify the key matches (sanity check)
        if message.key() != message_key {
            tracing::info!(
                "on_timer: Key mismatch! expected {:?}, got {:?} - removing offset {}",
                message_key,
                message.key(),
                offset
            );
            // Clean up this offset from the deferred queue
            // (cache invalidated by CachedDeferStore)
            self.store
                .remove_deferred_message(message_key, offset)
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
                self.handle_retry_success(&context, message_key, offset, retry_count)
                    .await
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

impl<T, S, L, D> EventHandler for DeferHandler<T, S, L, D>
where
    T: FallibleHandler,
    S: DeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
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
