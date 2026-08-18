use tracing::{Instrument, debug, info, warn};

use super::{MessageDeferHandler, MessageDeferOutput};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::{ConsumerMessage, ConsumerRecord};
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::{DeferError, DeferResult};
use crate::consumer::middleware::defer::message::store::{
    MessageDeferStore, MessageRetryCompletionResult,
};
use crate::consumer::middleware::handler::HandlerMethod;
use crate::consumer::{DemandType, Keyed};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MessageLoader;
use crate::telemetry::event::TimerEventType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger};
use crate::{Key, Offset};

impl<T, M, L, D> MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler<Payload = L::Payload>,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
{
    /// Returns `now + backoff(retry_count)`; used for scheduling timers.
    pub(super) fn next_retry_time(
        &self,
        retry_count: u32,
    ) -> DeferResult<CompactDateTime, M::Error, T::Error, L::Error> {
        let delay = calculate_backoff(&self.config, retry_count);
        let now = CompactDateTime::now()?;
        Ok(now.add_duration(delay)?)
    }

    /// Schedules a `DeferredMessage` timer with backoff based on retry count.
    pub(super) async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn schedule_next_or_clear<C>(
        &self,
        context: &C,
        result: MessageRetryCompletionResult,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn complete_and_advance<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
    ) -> DeferResult<(), M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn append_to_deferred_queue(
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
    pub(super) async fn handle_retry_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: T::Error,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn load_deferred_message<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
    ) -> DeferResult<Option<ConsumerRecord<T::Payload>>, M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn handle_load_failure<C>(
        &self,
        context: &C,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        error: L::Error,
    ) -> DeferResult<Option<ConsumerRecord<T::Payload>>, M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
    /// to ensure the timer still fires on partial failure.
    ///
    /// `inner_error` is the transient error returned by the inner handler
    /// for *this* dispatch — it is preserved in the returned
    /// [`MessageDeferOutput::Deferred`] so the inner sees
    /// `after_abort(Err(inner_error))` (its attempt is being rolled back;
    /// the deferred timer will re-dispatch the same logical message).
    pub(super) async fn defer_message<C>(
        &self,
        context: C,
        message_key: &Key,
        offset: Offset,
        inner_error: T::Error,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Timer first, then store: the timer still fires on partial failure.
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
    pub(super) async fn retry_deferred_message<H, C>(
        &self,
        context: C,
        trigger: &Trigger,
        message_key: &Key,
        offset: Offset,
        retry_count: u32,
        message: ConsumerMessage<H::MessagePayload>,
    ) -> DeferResult<MessageDeferOutput<T::Output, T::Error>, M::Error, T::Error, L::Error>
    where
        C: EventContext<Payload = T::Payload>,
        H: HandlerMethod<T>,
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

        // Instrument with the reload span so the retried handler runs inside
        // it ambiently, mirroring the partition dispatch arms.
        let load_span = message.span();
        match H::call(&self.handler, context.clone(), message, DemandType::Failure)
            .instrument(load_span)
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
