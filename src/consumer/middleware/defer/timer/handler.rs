//! Timer defer handler for processing deferred timer retries.
//!
//! This module provides the [`TimerDeferHandler`] which wraps an inner handler
//! and intercepts `DeferredTimer` timer events to retry previously-failed
//! application timers.
//!
//! # Handler Flow
//!
//! 1. **Application Timer Fails**: Transient error on `on_timer` for
//!    `Application` type
//! 2. **Timer Deferred**: Store trigger in `deferred_timers`, schedule
//!    `DeferredTimer`
//! 3. **New Timers Queue**: Subsequent timers for same key queue behind failed
//!    one
//! 4. **Retry Fires**: `DeferredTimer` fires, load from store, retry inner
//!    handler
//! 5. **Success/Failure**: On success advance queue, on transient re-defer, on
//!    permanent skip

use super::context::TimerDeferContext;
use super::store::{
    CachedTimerDeferStore, LazyTimerDeferStore, TimerDeferStore, TimerDeferStoreFactory,
    TimerDeferStoreProvider, TimerRetryCompletionResult,
};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::DeferError;
use crate::consumer::middleware::defer::segment::Segment;
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{DemandType, Keyed};
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Partition, Topic};
use tracing::{debug, info, warn};

/// Middleware that defers transiently-failed application timers for timer-based
/// retry.
#[derive(Clone)]
pub struct TimerDeferMiddleware<P, D>
where
    P: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
}

/// Creates [`TimerDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct TimerDeferProvider<T, P, D>
where
    P: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    provider: T,
    config: DeferConfiguration,
    store_provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
}

/// Per-partition handler wrapping an inner handler with timer defer logic.
#[derive(Clone)]
pub struct TimerDeferHandler<T, S, D>
where
    S: TimerDeferStore,
    D: DeferralDecider,
{
    /// Inner handler to call for processing.
    pub(crate) handler: T,
    /// Store for deferred timers.
    pub(crate) store: S,
    /// Decider for deferral decisions.
    pub(crate) decider: D,
    /// Configuration for backoff and deferral behavior.
    pub(crate) config: DeferConfiguration,
    /// Topic this handler is processing.
    pub(crate) topic: Topic,
    /// Partition this handler is processing.
    pub(crate) partition: Partition,
}

impl<P, D> TimerDeferMiddleware<P, D>
where
    P: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    /// Creates a new timer defer middleware.
    ///
    /// # Arguments
    ///
    /// * `config` - Defer configuration (backoff, cache size, etc.)
    /// * `provider` - Store provider for creating partition-specific stores
    /// * `decider` - Decider for gating initial deferral decisions
    /// * `consumer_group` - Consumer group ID for segment isolation
    #[must_use]
    pub fn new(
        config: DeferConfiguration,
        provider: P,
        decider: D,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            config,
            provider,
            decider,
            consumer_group,
        }
    }
}

impl<P, D> HandlerMiddleware for TimerDeferMiddleware<P, D>
where
    P: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    type Provider<T: FallibleHandlerProvider> = TimerDeferProvider<T, P, D>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
    {
        TimerDeferProvider {
            provider,
            config: self.config.clone(),
            store_provider: self.provider.clone(),
            decider: self.decider.clone(),
            consumer_group: self.consumer_group.clone(),
        }
    }
}

/// Factory for cached timer defer stores.
///
/// Holds a [`Segment`] which lives for the partition's lifetime and is used
/// to create the underlying store on first access.
#[derive(Clone)]
pub struct TimerDeferStoreFactoryImpl<P: TimerDeferStoreProvider> {
    /// The timer defer store provider.
    pub provider: P,
    /// The segment this store handles.
    pub segment: Segment,
    /// Maximum cache size.
    pub cache_size: usize,
}

impl<P: TimerDeferStoreProvider> TimerDeferStoreFactory for TimerDeferStoreFactoryImpl<P> {
    type Store = CachedTimerDeferStore<P::Store>;

    async fn create(self) -> Result<Self::Store, <Self::Store as TimerDeferStore>::Error> {
        let store = self.provider.create_store(&self.segment).await?;
        Ok(CachedTimerDeferStore::new(store, self.cache_size))
    }
}

/// Lazily-initialized cached store for [`TimerDeferHandler`].
pub type TimerDeferLazyStore<P> = LazyTimerDeferStore<TimerDeferStoreFactoryImpl<P>>;

impl<T, P, D> FallibleHandlerProvider for TimerDeferProvider<T, P, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler,
    P: TimerDeferStoreProvider,
    D: DeferralDecider,
{
    type Handler = TimerDeferHandler<T::Handler, TimerDeferLazyStore<P>, D>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        let segment = Segment::new(topic, partition, self.consumer_group.clone());

        let factory = TimerDeferStoreFactoryImpl {
            provider: self.store_provider.clone(),
            segment,
            cache_size: self.config.cache_size,
        };

        TimerDeferHandler {
            handler: self.provider.handler_for_partition(topic, partition),
            store: LazyTimerDeferStore::new(factory),
            decider: self.decider.clone(),
            config: self.config.clone(),
            topic,
            partition,
        }
    }
}

impl<T, S, D> TimerDeferHandler<T, S, D>
where
    T: FallibleHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
{
    /// Returns `now + backoff(retry_count)`; used for scheduling retry timers.
    fn next_retry_time(&self, retry_count: u32) -> Result<CompactDateTime, CompactDateTimeError> {
        let delay = calculate_backoff(&self.config, retry_count);
        let now = CompactDateTime::now()?;
        now.add_duration(delay)
    }

    /// Schedules a `DeferredTimer` timer with backoff based on retry count.
    async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        let fire_time = self.next_retry_time(retry_count)?;

        context
            .clear_and_schedule(fire_time, TimerType::DeferredTimer)
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

    /// Schedules timer for next entry or clears if queue empty.
    async fn schedule_next_or_clear<C>(
        &self,
        context: &C,
        result: TimerRetryCompletionResult,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        match result {
            TimerRetryCompletionResult::MoreTimers { .. } => {
                // More timers in queue - schedule retry (retry_count reset to 0)
                self.schedule_retry_timer(context, 0).await
            }
            TimerRetryCompletionResult::Completed => {
                // No more timers - clear the retry timer
                context
                    .clear_scheduled(TimerType::DeferredTimer)
                    .await
                    .map_err(|e| DeferError::Timer(Box::new(e)))
            }
        }
    }

    /// Removes timer from queue and schedules next (or clears).
    /// Used after success, permanent failure, or skipping corrupted entries.
    async fn complete_and_advance<C>(
        &self,
        context: &C,
        trigger: &Trigger,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        let result = self
            .store
            .complete_retry_success(&trigger.key, trigger.time)
            .await
            .map_err(DeferError::Store)?;

        self.schedule_next_or_clear(context, result).await
    }

    /// Appends timer to an already-deferred key's queue (maintains ordering).
    async fn append_to_deferred_queue(
        &self,
        trigger: &Trigger,
    ) -> Result<(), DeferError<S::Error, T::Error>> {
        self.store
            .defer_additional_timer(trigger)
            .await
            .map_err(DeferError::Store)?;

        debug!(
            key = ?trigger.key,
            time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            "Queued timer behind already-deferred key"
        );

        Ok(())
    }

    /// Handles retry failures by error category.
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        trigger: &Trigger,
        retry_count: u32,
        error: T::Error,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        match error.classify_error() {
            ErrorCategory::Transient => {
                // Always re-defer: timer is committed to queue
                let new_retry_count = self
                    .store
                    .increment_retry_count(&trigger.key, retry_count)
                    .await
                    .map_err(DeferError::Store)?;

                self.schedule_retry_timer(context, new_retry_count).await?;

                info!(
                    key = ?trigger.key,
                    time = %trigger.time,
                    retry_count = new_retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Re-deferred timer after transient failure"
                );

                Ok(())
            }
            ErrorCategory::Permanent => {
                warn!(
                    key = ?trigger.key,
                    time = %trigger.time,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Permanent handler error during retry - removing from queue: {error:#}"
                );

                self.complete_and_advance(context, trigger).await?;

                Err(DeferError::Handler(error))
            }
            ErrorCategory::Terminal => Err(DeferError::Handler(error)),
        }
    }

    /// Defers a timer for the first time. Schedules retry timer before storing
    /// to ensure timer coverage on partial failure.
    async fn defer_timer<C>(
        &self,
        context: C,
        trigger: &Trigger,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        // Timer first, then store: ensures timer coverage on partial failure
        self.schedule_retry_timer(&context, 0).await?;

        self.store
            .defer_first_timer(trigger)
            .await
            .map_err(DeferError::Store)?;

        info!(
            key = ?trigger.key,
            time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            "Deferred timer for timer-based retry"
        );

        Ok(())
    }
}

impl<T, S, D> FallibleHandler for TimerDeferHandler<T, S, D>
where
    T: FallibleHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
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
        // Wrap context so inner handlers see unified timer state
        let key = message.key().clone();
        let wrapped_context = TimerDeferContext::new(context, self.store.clone(), key);

        self.handler
            .on_message(wrapped_context, message, demand_type)
            .await
            .map_err(DeferError::Handler)
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
        // Wrap context so inner handlers see unified timer state
        let wrapped_context =
            TimerDeferContext::new(context, self.store.clone(), trigger.key.clone());

        // Handle DeferredTimer retries
        if trigger.timer_type == TimerType::DeferredTimer {
            return self
                .handle_deferred_timer_retry(wrapped_context, trigger)
                .await;
        }

        // Only defer Application timers
        if trigger.timer_type != TimerType::Application {
            return self
                .handler
                .on_timer(wrapped_context, trigger, demand_type)
                .await
                .map_err(DeferError::Handler);
        }

        // Check if key is already deferred
        if self
            .store
            .is_deferred(&trigger.key)
            .await
            .map_err(DeferError::Store)?
            .is_some()
        {
            return self.append_to_deferred_queue(&trigger).await;
        }

        // Try handler, defer on transient failure if enabled
        let Err(error) = self
            .handler
            .on_timer(wrapped_context.clone(), trigger.clone(), demand_type)
            .await
        else {
            return Ok(());
        };

        if !matches!(error.classify_error(), ErrorCategory::Transient) {
            return Err(DeferError::Handler(error));
        }

        // Only gate initial deferral
        if !self.config.enabled {
            debug!(
                key = ?trigger.key,
                time = %trigger.time,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: middleware disabled"
            );
            return Err(DeferError::Handler(error));
        }

        if !self.decider.should_defer() {
            debug!(
                key = ?trigger.key,
                time = %trigger.time,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: decider threshold not met"
            );
            return Err(DeferError::Handler(error));
        }

        self.defer_timer(wrapped_context, &trigger).await
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}

impl<T, S, D> TimerDeferHandler<T, S, D>
where
    T: FallibleHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
{
    /// Handles a `DeferredTimer` retry event.
    async fn handle_deferred_timer_retry<C>(
        &self,
        context: C,
        trigger: Trigger,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext,
    {
        let key = &trigger.key;

        debug!(
            key = ?key,
            scheduled_time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            "Defer retry timer fired"
        );

        // Get next deferred timer
        let Some((stored_trigger, retry_count)) = self
            .store
            .get_next_deferred_timer(key)
            .await
            .map_err(DeferError::Store)?
        else {
            // Queue empty - clear orphaned timer
            debug!(
                key = ?key,
                topic = %self.topic,
                partition = self.partition,
                "Clearing orphaned defer timer: queue empty"
            );

            self.store
                .delete_key(key)
                .await
                .map_err(DeferError::Store)?;

            return Ok(());
        };

        debug!(
            key = ?key,
            original_time = %stored_trigger.time,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Loaded deferred timer - attempting retry"
        );

        // Retry inner handler with stored Application timer
        match self
            .handler
            .on_timer(context.clone(), stored_trigger.clone(), DemandType::Failure)
            .await
        {
            Ok(()) => {
                self.complete_and_advance(&context, &stored_trigger).await?;

                info!(
                    key = ?key,
                    original_time = %stored_trigger.time,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Deferred timer retry succeeded"
                );
                Ok(())
            }
            Err(error) => {
                self.handle_retry_failure(&context, &stored_trigger, retry_count, error)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Handler tests will be added in Phase 3 (integration tests)
    // The handler depends on proper test harness setup
}
