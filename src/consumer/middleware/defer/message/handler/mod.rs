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
//!   though the defer marker itself commits.
//! * `NoInner` — inner did not run (queue-append, orphan-timer, loader failure,
//!   key-mismatch); suppress both hooks.

use super::loader::{KafkaLoader, MessageLoader};
use super::store::{MessageDeferStore, MessageDeferStoreProvider, MessageRetryCompletionResult};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::{DeferralDecider, FailureTracker};
use crate::consumer::middleware::defer::error::{DeferError, DeferInitError, DeferResult};
use crate::consumer::middleware::{
    ClassifyError, ErrorCategory, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{ConsumerConfiguration, DemandType, Keyed};
use crate::telemetry::Telemetry;
use crate::telemetry::event::TimerEventType;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{ConsumerGroup, Key, Offset, Partition, Topic};
use std::sync::Arc;
use tracing::{debug, info, warn};

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
/// * `P` - Message defer store provider
/// * `L` - Message loader (default: [`KafkaLoader`])
/// * `D` - Deferral decider (default: [`FailureTracker`])
#[derive(Clone)]
pub struct MessageDeferMiddleware<P, L = KafkaLoader<crate::codec::JsonCodec>, D = FailureTracker>
where
    P: MessageDeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    config: DeferConfiguration,
    loader: L,
    provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
    telemetry: Telemetry,
}

impl<P, L> MessageDeferMiddleware<P, L, FailureTracker>
where
    P: MessageDeferStoreProvider,
    L: MessageLoader,
{
    /// Creates middleware with a caller-supplied loader and a
    /// [`FailureTracker`] decider.
    ///
    /// Callers pick the loader: [`KafkaLoader`] for production (see
    /// [`KafkaLoader::for_consumer`]) or [`MemoryLoader`] for mock mode,
    /// where connecting to real Kafka is not permitted.
    ///
    /// [`KafkaLoader::for_consumer`]: super::loader::KafkaLoader::for_consumer
    /// [`MemoryLoader`]: super::loader::MemoryLoader
    ///
    /// # Errors
    ///
    /// Returns an error if config validation fails.
    pub fn new(
        config: DeferConfiguration,
        consumer_config: &ConsumerConfiguration,
        provider: P,
        decider: FailureTracker,
        loader: L,
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
            telemetry: telemetry.clone(),
        })
    }
}

/// Creates [`MessageDeferHandler`]s for each partition.
#[derive(Clone)]
pub struct MessageDeferProvider<T, P, L = KafkaLoader<crate::codec::JsonCodec>, D = FailureTracker>
where
    P: MessageDeferStoreProvider,
    L: MessageLoader,
    D: DeferralDecider,
{
    inner_provider: T,
    config: DeferConfiguration,
    loader: L,
    store_provider: P,
    decider: D,
    consumer_group: ConsumerGroup,
    telemetry: Telemetry,
}

/// Per-partition handler wrapping an inner handler with defer logic.
#[derive(Clone)]
pub struct MessageDeferHandler<T, M, L = KafkaLoader<crate::codec::JsonCodec>, D = FailureTracker>
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
}

impl<P, L, D> HandlerMiddleware for MessageDeferMiddleware<P, L, D>
where
    P: MessageDeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Payload = L::Payload;

    type Provider<T> = MessageDeferProvider<T, P, L, D>
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
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T, P, L, D> FallibleHandlerProvider for MessageDeferProvider<T, P, L, D>
where
    T: FallibleHandlerProvider,
    T::Handler: FallibleHandler<Payload = L::Payload>,
    P: MessageDeferStoreProvider,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Handler = MessageDeferHandler<T::Handler, P::Store, L, D>;

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
        }
    }
}

impl<T, M, L, D> MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler<Payload = L::Payload>,
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
    ///
    /// The inner handler does *not* run on this dispatch — the message is
    /// queued behind an existing deferred entry and will be retried later
    /// when the deferred timer fires. Returns
    /// [`MessageDeferOutput::NoInner`] so both inner apply hooks are
    /// suppressed.
    async fn append_to_deferred_queue(
        &self,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error> {
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

        Ok(MessageDeferOutput::NoInner)
    }

    /// Handles retry failures by error category:
    /// - **Transient**: Always re-defer (maintains completion invariant).
    ///   Returns [`MessageDeferOutput::Deferred`] carrying the inner error so
    ///   that `after_abort(Err(e))` is forwarded to the inner — the same
    ///   logical message will be re-dispatched when the rescheduled timer
    ///   fires.
    /// - **Permanent**: Remove and advance (unblocks queue). Surfaces as
    ///   `Err(DeferError::Handler(error))`; the inner sees its chosen apply
    ///   hook with `Err(error)` (final — the message will not be retried).
    /// - **Terminal**: Propagate without state change (shutdown handling).
    ///   Surfaces as `Err(DeferError::Handler(error))`.
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: T::Error,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        let error_category = error.classify_error();
        let exception = format!("{error:?}").into_boxed_str();

        match error_category {
            ErrorCategory::Transient => {
                // Always re-defer: message is committed to queue, dropping would
                // violate ordering for messages queued behind it. The inner
                // ran and returned `error`; we capture it in `Deferred` so
                // the inner sees `after_abort(Err(error))` (its attempt is
                // being rolled back; a retry is coming via the rescheduled
                // timer).
                let new_retry_count = self
                    .store
                    .increment_retry_count(message_key, retry_count)
                    .await
                    .map_err(DeferError::Store)?;

                self.schedule_retry_timer(context, new_retry_count).await?;

                self.sender.message_failed(
                    message_key.clone(),
                    offset,
                    DemandType::Failure,
                    self.source.clone(),
                    error_category,
                    exception,
                );

                info!(
                    key = ?message_key,
                    offset = offset,
                    retry_count = new_retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Re-deferred message after transient failure"
                );

                Ok(MessageDeferOutput::Deferred(error))
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

                self.sender.message_failed(
                    message_key.clone(),
                    offset,
                    DemandType::Failure,
                    self.source.clone(),
                    error_category,
                    exception,
                );

                Err(DeferError::Handler(error))
            }
            ErrorCategory::Terminal => {
                self.sender.message_failed(
                    message_key.clone(),
                    offset,
                    DemandType::Failure,
                    self.source.clone(),
                    error_category,
                    exception,
                );

                Err(DeferError::Handler(error))
            }
        }
    }

    /// Loads message from Kafka. Returns `None` if the load failed and was
    /// handled at the defer layer (timer rescheduled, queue advanced past a
    /// permanently broken offset, or key-mismatch skip) — the inner handler
    /// is *not* invoked for this dispatch and the caller surfaces
    /// [`MessageDeferOutput::NoInner`]. Returns `Err` only for terminal
    /// loader errors.
    async fn load_deferred_message<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
    ) -> DeferResult<Option<ConsumerMessage<T::Payload>>, M::Error, T::Error, L::Error>
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
    /// propagates. The inner handler does not run on any of these paths, so
    /// the caller maps the resulting `Ok(None)` / `Err(Loader)` to
    /// [`MessageDeferOutput::NoInner`] / `Err` and both inner apply hooks
    /// stay suppressed.
    async fn handle_load_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: L::Error,
    ) -> DeferResult<Option<ConsumerMessage<T::Payload>>, M::Error, T::Error, L::Error>
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
    ///
    /// `inner_error` is the transient error returned by the inner handler
    /// for *this* dispatch — it is preserved in the returned
    /// [`MessageDeferOutput::Deferred`] so the inner sees
    /// `after_abort(Err(inner_error))` (its attempt is being rolled back;
    /// the deferred timer will re-dispatch the same logical message).
    async fn defer_message<C>(
        &self,
        context: C,
        message_key: &Key,
        offset: Offset,
        inner_error: T::Error,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
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

        Ok(MessageDeferOutput::Deferred(inner_error))
    }

    /// Retries a deferred message and emits timer + message telemetry.
    ///
    /// On inner success: returns [`MessageDeferOutput::Inner`] (forward
    /// `after_commit(Ok(..))` to the inner). On inner Transient failure:
    /// the queued message is re-deferred and the inner sees
    /// `after_abort(Err(..))` via [`MessageDeferOutput::Deferred`]. On
    /// Permanent / Terminal failure: surfaces as
    /// `Err(DeferError::Handler(_))` and the inner sees the wrapping
    /// framework's chosen apply hook with that `Err`.
    async fn retry_deferred_message<C>(
        &self,
        context: C,
        trigger: &Trigger,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        message: ConsumerMessage<T::Payload>,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
    where
        C: EventContext,
    {
        self.sender.timer_dispatched(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            DemandType::Failure,
            self.source.clone(),
        );

        self.sender.message_dispatched(
            message_key.clone(),
            offset,
            DemandType::Failure,
            self.source.clone(),
        );

        match self
            .handler
            .on_message(context.clone(), message, DemandType::Failure)
            .await
        {
            Ok(output) => {
                self.sender.timer_succeeded(
                    trigger.key.clone(),
                    trigger.time,
                    trigger.timer_type,
                    DemandType::Failure,
                    self.source.clone(),
                );
                self.sender.message_succeeded(
                    message_key.clone(),
                    offset,
                    DemandType::Failure,
                    self.source.clone(),
                );
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
                Ok(MessageDeferOutput::Inner(output))
            }
            Err(error) => {
                let error_category = error.classify_error();
                let exception = format!("{error:?}").into_boxed_str();
                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type: DemandType::Failure,
                        error_category,
                        exception,
                    },
                    trigger.key.clone(),
                    trigger.time,
                    trigger.timer_type,
                    self.source.clone(),
                );
                self.handle_retry_failure(&context, message_key, offset, retry_count, error)
                    .await
            }
        }
    }
}

impl<T, M, L, D> FallibleHandler for MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler<Payload = L::Payload>,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Error = DeferError<M::Error, T::Error, L::Error>;
    /// Encodes the inner's outcome; drives apply-hook routing. See
    /// [`MessageDeferOutput`] and the module-level apply-hooks section.
    type Output = MessageDeferOutput<T::Output, T::Error>;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<T::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Already deferred: queue behind existing messages (ordering
        // invariant). Inner does not run -> NoInner.
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

        let error = match self
            .handler
            .on_message(context.clone(), message, demand_type)
            .await
        {
            Ok(output) => return Ok(MessageDeferOutput::Inner(output)),
            Err(error) => error,
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

        self.defer_message(context, &message_key, offset, error)
            .await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        if trigger.timer_type != TimerType::DeferredMessage {
            return self
                .handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(MessageDeferOutput::Inner)
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
            // Orphan timer for an empty queue — inner did not run.
            debug!(
                key = ?message_key,
                topic = %self.topic,
                partition = self.partition,
                "Clearing orphaned defer timer: queue empty"
            );
            self.store
                .delete_key(message_key)
                .await
                .map_err(DeferError::Store)?;
            return Ok(MessageDeferOutput::NoInner);
        };

        let Some(message) = self
            .load_deferred_message(&context, message_key, offset, retry_count)
            .await?
        else {
            // Loader handled the failure (retry rescheduled, queue advanced
            // past a permanent skip, or key-mismatch skip) — inner did not
            // run for this dispatch.
            return Ok(MessageDeferOutput::NoInner);
        };

        debug!(
            key = ?message_key,
            offset = offset,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Loaded deferred message - attempting retry"
        );

        self.retry_deferred_message(context, &trigger, message_key, offset, retry_count, message)
            .await
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        // Apply-hook routing (see module docs):
        // - Inner(o):    inner ran and succeeded           -> after_commit(Ok)
        // - NoInner:     no inner dispatch happened        -> suppress
        // - Deferred(e): inner ran, transient err deferred -> after_abort(Err(e))
        //   (retry coming)
        // - Handler(e):  inner ran and surfaced an error   -> after_commit(Err)
        // - Store/Loader/...: defer-layer error, inner never ran -> suppress
        match result {
            Ok(MessageDeferOutput::Inner(output)) => {
                self.handler.after_commit(context, Ok(output)).await;
            }
            Ok(MessageDeferOutput::Deferred(error)) => {
                self.handler.after_abort(context, Err(error)).await;
            }
            Err(DeferError::Handler(error)) => {
                self.handler.after_commit(context, Err(error)).await;
            }
            Ok(MessageDeferOutput::NoInner) | Err(_) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        // Symmetric to after_commit. The only twist: Deferred(e) still routes
        // to after_abort(Err(e)) regardless of the outer commit/abort decision.
        match result {
            Ok(MessageDeferOutput::Inner(output)) => {
                self.handler.after_abort(context, Ok(output)).await;
            }
            Ok(MessageDeferOutput::Deferred(error)) | Err(DeferError::Handler(error)) => {
                self.handler.after_abort(context, Err(error)).await;
            }
            Ok(MessageDeferOutput::NoInner) | Err(_) => {}
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
