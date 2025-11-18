//! Defer middleware handler implementation.
//!
//! Implements the defer middleware that intercepts permanent failures and
//! defers them for later retry using the timer system. This allows the system
//! to handle persistent failures (e.g., downstream service outages) without
//! blocking partition processing.

use super::DeferState;
use super::config::DeferConfiguration;
use super::error::DeferError;
use super::failure_tracker::FailureTracker;
use super::loader::KafkaLoader;
use super::store::DeferStore;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, UncommittedMessage};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, EventHandler, Uncommitted};
use crate::timers::duration::CompactDuration;
use crate::timers::{TimerType, Trigger, UncommittedTimer};
use crate::{Key, Partition, Topic};
use quick_cache::sync::Cache;
use std::sync::Arc;
use tracing::{debug, error};

/// Middleware that defers permanently failed messages for later retry.
///
/// Intercepts permanent failures from the inner handler and schedules them
/// for retry using the timer system, allowing partition processing to
/// continue without blocking.
#[derive(Clone)]
pub struct DeferMiddleware<S>
where
    S: DeferStore,
{
    config: DeferConfiguration,
    loader: Arc<KafkaLoader>,
    store: S,
    failure_tracker: FailureTracker,
    consumer_group: Arc<str>,
}

impl<S> DeferMiddleware<S>
where
    S: DeferStore,
{
    /// Creates a new defer middleware with the given configuration and store.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for defer behavior
    /// * `loader` - Kafka loader for reloading messages
    /// * `store` - Storage backend for deferred state
    /// * `consumer_group` - Consumer group name for UUID generation
    ///
    /// # Returns
    ///
    /// A `Result` containing the new middleware if configuration is valid.
    ///
    /// # Errors
    ///
    /// Returns `ValidationErrors` if the configuration is invalid.
    pub fn new(
        config: DeferConfiguration,
        loader: KafkaLoader,
        store: S,
        consumer_group: &str,
    ) -> Result<Self, validator::ValidationErrors> {
        use validator::Validate;
        config.validate()?;

        let failure_tracker = FailureTracker::new(config.failure_window, config.failure_threshold);

        Ok(Self {
            config,
            loader: Arc::new(loader),
            store,
            failure_tracker,
            consumer_group: Arc::from(consumer_group),
        })
    }
}

/// Provider that creates defer handlers for each partition.
#[derive(Clone)]
pub struct DeferProvider<T, S>
where
    S: DeferStore,
{
    provider: T,
    config: DeferConfiguration,
    loader: Arc<KafkaLoader>,
    store: S,
    failure_tracker: FailureTracker,
    consumer_group: Arc<str>,
}

/// Handler that implements defer logic for a specific partition.
///
/// # Type Parameters
///
/// * `T` - Inner handler type
/// * `S` - Store implementation (`DeferStore`)
#[derive(Clone)]
pub struct DeferHandler<T, S>
where
    S: DeferStore,
{
    handler: T,
    loader: Arc<KafkaLoader>,
    store: S,
    failure_tracker: FailureTracker,
    cache: Arc<Cache<Key, DeferState>>,
    config: DeferConfiguration,
    topic: Topic,
    partition: Partition,
    consumer_group: Arc<str>,
}

impl<S> HandlerMiddleware for DeferMiddleware<S>
where
    S: DeferStore,
{
    type Provider<T: FallibleHandlerProvider> = DeferProvider<T, S>;

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

impl<T, S> FallibleHandlerProvider for DeferProvider<T, S>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    S: DeferStore,
{
    type Handler = DeferHandler<T::Handler, S>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        DeferHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            loader: self.loader.clone(),
            store: self.store.clone(),
            failure_tracker: self.failure_tracker.clone(),
            cache: Arc::new(Cache::new(self.config.cache_size)),
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

impl<T, S> DeferHandler<T, S>
where
    T: FallibleHandler,
    S: DeferStore,
{
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
    ) -> Result<(), DeferError<S::Error, T::Error>>
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
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        self.failure_tracker.record_success();

        // Success! Remove this offset from deferred queue
        self.store
            .remove_deferred_message(key_id, offset)
            .await
            .map_err(DeferError::Store)?;

        // Check if there are more deferred messages
        let has_more = self
            .store
            .get_next_deferred_message(key_id)
            .await
            .map_err(DeferError::Store)?
            .is_some();

        if has_more {
            // Reset retry_count to 0 for the next message
            self.store
                .set_retry_count(key_id, 0)
                .await
                .map_err(DeferError::Store)?;
            self.cache
                .insert(key.clone(), DeferState::Deferred { retry_count: 0 });

            // Schedule timer for the next deferred message
            self.schedule_retry_timer(context, 0).await?;

            debug!(
                "Scheduled next retry for key {:?} (more messages in queue)",
                key
            );
        } else {
            // No more messages, clean up entirely
            self.store
                .delete_key(key_id)
                .await
                .map_err(DeferError::Store)?;
            self.cache.remove(key);
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
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        use super::error::ConfigurationError;
        use crate::consumer::middleware::ClassifyError;

        self.failure_tracker.record_failure();

        // Check error classification
        if let ErrorCategory::Permanent = error.classify_error() {
            // Check if we should defer again
            if !self.failure_tracker.should_defer() {
                debug!("Deferral disabled due to high failure rate");
                // Clean up this offset and propagate error
                self.store
                    .remove_deferred_message(key_id, offset)
                    .await
                    .map_err(DeferError::Store)?;
                self.cache.remove(key);
                return Err(DeferError::Configuration(ConfigurationError::Invalid(
                    format!("Deferral disabled: {error}"),
                )));
            }

            // Increment retry count
            let new_retry_count = retry_count + 1;
            self.store
                .set_retry_count(key_id, new_retry_count)
                .await
                .map_err(DeferError::Store)?;

            // Update cache
            self.cache.insert(
                key.clone(),
                DeferState::Deferred {
                    retry_count: new_retry_count,
                },
            );

            // Schedule next retry with new backoff
            self.schedule_retry_timer(context, new_retry_count).await?;

            debug!(
                "Re-deferred message for key {:?} (retry_count now {})",
                key, new_retry_count
            );

            Ok(())
        } else {
            // Transient or terminal error - clean up and propagate
            self.store
                .remove_deferred_message(key_id, offset)
                .await
                .map_err(DeferError::Store)?;
            self.cache.remove(key);
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
    ) -> Result<(), DeferError<S::Error, T::Error>>
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

        // Check cache for existing defer state
        let current_state = if let Some(state) = self.cache.get(key) {
            state
        } else {
            // Cache miss - query store to see if key has deferred messages
            let store_state = self
                .store
                .get_next_deferred_message(&key_id)
                .await
                .map_err(DeferError::Store)?;

            match store_state {
                Some((_offset, retry_count)) => {
                    let state = DeferState::Deferred { retry_count };
                    self.cache.insert(key.clone(), state.clone());
                    state
                }
                None => DeferState::NotDeferred,
            }
        };

        // Determine retry_count for this deferred message
        // For first failure: retry_count = 0
        // For subsequent messages on already-deferred key: keep existing retry_count
        let retry_count = match current_state {
            DeferState::NotDeferred => 0,
            DeferState::Deferred { retry_count } => retry_count, // Don't increment here!
        };

        // Calculate backoff delay
        let delay = self.calculate_backoff(retry_count);

        // Calculate expected retry time for TTL
        let now = CompactDateTime::now()?;
        let expected_retry_time = now.add_duration(delay)?;

        // CRITICAL ORDERING (Invariant 1: Timer Coverage):
        // 1. Schedule timer FIRST
        // 2. Write to Cassandra
        // 3. Update cache
        //
        // If timer scheduling fails, error propagates and nothing is stored.
        // If Cassandra write fails after timer scheduled, error propagates to
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

        // 2. Write to Cassandra
        // For first failure (retry_count == 0), pass Some(0) to set the static
        // retry_count column
        // For subsequent messages, pass None (just append offset, keep retry_count)
        let retry_count_update = (retry_count == 0).then_some(0);

        self.store
            .append_deferred_message(
                &key_id,
                message.offset(),
                expected_retry_time,
                retry_count_update,
            )
            .await
            .map_err(DeferError::Store)?;

        // 3. Update cache
        self.cache
            .insert(key.clone(), DeferState::Deferred { retry_count });

        debug!(
            "Deferred message for key {:?} (retry_count={}, delay={} seconds)",
            key,
            retry_count,
            delay.seconds()
        );

        Ok(())
    }
}

impl<T, S> FallibleHandler for DeferHandler<T, S>
where
    T: FallibleHandler,
    S: DeferStore,
{
    type Error = DeferError<S::Error, T::Error>;

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
        use crate::timers::datetime::CompactDateTime;

        let key = message.key().clone();

        // Check if this key is already deferred
        let key_id = generate_key_id(
            self.consumer_group.as_ref(),
            &self.topic,
            self.partition,
            &key,
        );

        // Check cache first, query store on miss
        let defer_state = match self.cache.get(&key) {
            Some(state) => Some(state),
            None => {
                // Cache miss - query store
                self.store
                    .get_next_deferred_message(&key_id)
                    .await
                    .map_err(DeferError::Store)?
                    .map(|(_offset, retry_count)| {
                        let state = DeferState::Deferred { retry_count };
                        self.cache.insert(key.clone(), state.clone());
                        state
                    })
            }
        };

        // If key is already deferred, append this message to the queue and return
        if let Some(DeferState::Deferred { retry_count }) = defer_state {
            // Calculate expected retry time for TTL
            let delay = self.calculate_backoff(retry_count);
            let now = CompactDateTime::now()?;
            let expected_retry_time = now.add_duration(delay)?;

            // Append to deferred queue (don't update retry_count - it's managed by timer
            // retries)
            self.store
                .append_deferred_message(&key_id, message.offset(), expected_retry_time, None)
                .await
                .map_err(DeferError::Store)?;

            debug!(
                "Queued message for already-deferred key {:?} (offset={})",
                key,
                message.offset()
            );

            return Ok(());
        }

        // Key is not deferred - try to process the message with the inner handler
        match self
            .handler
            .on_message(context.clone(), message.clone(), demand_type)
            .await
        {
            Ok(()) => {
                self.failure_tracker.record_success();

                // If this key was previously deferred, clean up
                let cached_state = self.cache.get(&key);
                if let Some(DeferState::Deferred { .. }) = cached_state {
                    // Generate UUID for this key
                    let key_id = generate_key_id(
                        self.consumer_group.as_ref(),
                        &self.topic,
                        self.partition,
                        &key,
                    );

                    // Remove the oldest deferred offset for this key
                    // TODO Phase 5.3: After success, if there are more deferred messages,
                    // remove the oldest one and check if queue is empty
                    let deferred_result = self
                        .store
                        .get_next_deferred_message(&key_id)
                        .await
                        .map_err(DeferError::Store)?;

                    if let Some((offset, _retry_count)) = deferred_result {
                        self.store
                            .remove_deferred_message(&key_id, offset)
                            .await
                            .map_err(DeferError::Store)?;
                    }

                    // Remove from cache (will be updated if there are more messages)
                    self.cache.remove(&key);

                    debug!("Cleared defer state for key {:?} after success", key);
                }

                Ok(())
            }
            Err(error) => {
                self.failure_tracker.record_failure();

                // Check error classification - only defer permanent failures
                if let ErrorCategory::Permanent = error.classify_error() {
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
                self.cache.remove(key);
                return Ok(());
            }
            Err(e) => {
                return Err(DeferError::Store(e));
            }
        };

        // Load the actual message from Kafka
        let message = self
            .loader
            .load_message(self.topic, self.partition, offset)
            .await?;

        // Verify the key matches (sanity check)
        if message.key() != key {
            debug!("Key mismatch: expected {:?}, got {:?}", key, message.key());
            // Clean up this offset from the deferred queue
            self.store
                .remove_deferred_message(&key_id, offset)
                .await
                .map_err(DeferError::Store)?;
            self.cache.remove(key);
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

impl<T, S> EventHandler for DeferHandler<T, S>
where
    T: FallibleHandler,
    S: DeferStore,
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
