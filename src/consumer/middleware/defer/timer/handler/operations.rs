use tracing::{Instrument, debug, info, warn};

use super::super::store::{TimerDeferStore, TimerRetryCompletionResult};
use super::{TimerDeferHandler, TimerDeferOutput};
use crate::consumer::DemandType;
use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::DeferError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::telemetry::event::TimerEventType;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::{TimerType, Trigger};

impl<T, S, D> TimerDeferHandler<T, S, D>
where
    T: FallibleHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
{
    /// Handles an `Application` timer, deferring on transient failure if
    /// enabled.
    ///
    /// Returns:
    /// - [`TimerDeferOutput::NoInner`] when the key was already deferred and
    ///   this trigger is appended to its queue (inner not invoked).
    /// - [`TimerDeferOutput::Inner`] when the inner handler ran and produced an
    ///   output.
    /// - [`TimerDeferOutput::Deferred`] when the inner ran, returned a
    ///   transient error, and the middleware enqueued a retry. The wrapped
    ///   inner error must be threaded into `after_abort` on the inner.
    /// - `Err(DeferError::Handler(e))` when the inner ran and the error must
    ///   surface (non-transient, or transient but deferral disabled).
    pub(super) async fn handle_application_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Check if key is already deferred - queue behind existing entry
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
        let error = match self
            .handler
            .on_timer(context.clone(), trigger.clone(), demand_type)
            .await
        {
            Ok(output) => return Ok(TimerDeferOutput::Inner(output)),
            Err(error) => error,
        };

        if !matches!(error.classify_error(), ErrorCategory::Transient) {
            return Err(DeferError::Handler(error));
        }

        // Check deferral eligibility
        let enabled = self.config.enabled;
        let should_defer = self.decider.should_defer();

        if enabled && should_defer {
            return self.defer_first_timer(context, &trigger, error).await;
        }

        debug!(
            key = ?trigger.key,
            time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            enabled,
            should_defer,
            "Deferral skipped"
        );
        Err(DeferError::Handler(error))
    }

    /// Handles a `DeferredTimer` retry event.
    pub(super) async fn handle_deferred_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
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
            // Queue empty - clear orphaned timer. Inner did not run, so the
            // inner has no apply work for this dispatch.
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

            return Ok(TimerDeferOutput::NoInner);
        };

        debug!(
            key = ?key,
            original_time = %stored_trigger.time,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Loaded deferred timer - attempting retry"
        );

        // Emit dispatched for the DeferredTimer that actually fired.
        self.sender.timer_dispatched(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            DemandType::Failure,
            self.source.clone(),
        );

        // Instrument with the reload span so the retried handler runs inside
        // it ambiently, mirroring the partition dispatch arms.
        let output = match self
            .handler
            .on_timer(context.clone(), stored_trigger.clone(), DemandType::Failure)
            .instrument(stored_trigger.span())
            .await
        {
            Ok(output) => output,
            Err(error) => {
                return self
                    .handle_retry_failure(&context, &trigger, &stored_trigger, retry_count, error)
                    .await;
            }
        };

        self.sender.timer_succeeded(
            trigger.key.clone(),
            trigger.time,
            trigger.timer_type,
            DemandType::Failure,
            self.source.clone(),
        );

        self.complete_and_advance(&context, &stored_trigger).await?;

        info!(
            key = ?key,
            original_time = %stored_trigger.time,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Deferred timer retry succeeded"
        );

        Ok(TimerDeferOutput::Inner(output))
    }

    /// Defers a timer for the first time after the inner handler returned a
    /// transient error. Schedules retry timer before storing to ensure timer
    /// still fires on partial failure.
    ///
    /// Returns [`TimerDeferOutput::Deferred`] carrying the inner error so the
    /// apply hooks can drive `after_abort(Err(inner_err))` on the inner: the
    /// inner's prior dispatch is being rolled back even though our defer
    /// marker commits.
    pub(super) async fn defer_first_timer<C>(
        &self,
        context: C,
        trigger: &Trigger,
        inner_err: T::Error,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Timer first, then store: the timer still fires on partial failure
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

        Ok(TimerDeferOutput::Deferred(inner_err))
    }

    /// Appends timer to an already-deferred key's queue (maintains ordering).
    /// The inner handler is not invoked for this dispatch — the trigger is a
    /// pure side-effect on the defer queue.
    pub(super) async fn append_to_deferred_queue(
        &self,
        trigger: &Trigger,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>> {
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

        Ok(TimerDeferOutput::NoInner)
    }

    /// Handles retry failures by error category.
    ///
    /// `deferred_trigger` is the `DeferredTimer` that fired (used for
    /// telemetry). `stored_trigger` is the original `Application` timer
    /// retrieved from the store (used for store operations).
    ///
    /// On a transient error this returns [`TimerDeferOutput::Deferred`]
    /// carrying the inner error so the apply hooks route to
    /// `after_abort(Err(inner_err))` on the inner — the inner's retry attempt
    /// is being rolled back and another `DeferredTimer` will re-dispatch it.
    /// Permanent and terminal errors propagate as `Err(DeferError::Handler)`.
    pub(super) async fn handle_retry_failure<C>(
        &self,
        context: &C,
        deferred_trigger: &Trigger,
        stored_trigger: &Trigger,
        retry_count: u32,
        error: T::Error,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let error_category = error.classify_error();
        let exception = format!("{error:?}").into_boxed_str();

        match error_category {
            ErrorCategory::Transient => {
                // Always re-defer: timer is committed to queue
                let new_retry_count = self
                    .store
                    .increment_retry_count(&deferred_trigger.key, retry_count)
                    .await
                    .map_err(DeferError::Store)?;

                self.schedule_retry_timer(context, new_retry_count).await?;

                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type: DemandType::Failure,
                        error_category,
                        exception,
                    },
                    deferred_trigger.key.clone(),
                    deferred_trigger.time,
                    deferred_trigger.timer_type,
                    self.source.clone(),
                );

                info!(
                    key = ?deferred_trigger.key,
                    time = %deferred_trigger.time,
                    retry_count = new_retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Re-deferred timer after transient failure"
                );

                Ok(TimerDeferOutput::Deferred(error))
            }
            ErrorCategory::Permanent => {
                warn!(
                    key = ?deferred_trigger.key,
                    time = %deferred_trigger.time,
                    retry_count = retry_count,
                    topic = %self.topic,
                    partition = self.partition,
                    "Permanent handler error during retry - removing from queue: {error:#}"
                );

                self.complete_and_advance(context, stored_trigger).await?;

                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type: DemandType::Failure,
                        error_category,
                        exception,
                    },
                    deferred_trigger.key.clone(),
                    deferred_trigger.time,
                    deferred_trigger.timer_type,
                    self.source.clone(),
                );

                Err(DeferError::Handler(error))
            }
            ErrorCategory::Terminal => {
                self.sender.emit_timer(
                    TimerEventType::Failed {
                        demand_type: DemandType::Failure,
                        error_category,
                        exception,
                    },
                    deferred_trigger.key.clone(),
                    deferred_trigger.time,
                    deferred_trigger.timer_type,
                    self.source.clone(),
                );

                Err(DeferError::Handler(error))
            }
        }
    }

    /// Removes timer from queue and schedules next (or clears).
    /// Used after success, permanent failure, or skipping corrupted entries.
    pub(super) async fn complete_and_advance<C>(
        &self,
        context: &C,
        trigger: &Trigger,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let result = self
            .store
            .complete_retry_success(&trigger.key, trigger.time)
            .await
            .map_err(DeferError::Store)?;

        self.schedule_next_or_clear(context, result).await
    }

    /// Schedules a `DeferredTimer` timer with backoff based on retry count.
    pub(super) async fn schedule_retry_timer<C>(
        &self,
        context: &C,
        retry_count: u32,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
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
    pub(super) async fn schedule_next_or_clear<C>(
        &self,
        context: &C,
        result: TimerRetryCompletionResult,
    ) -> Result<(), DeferError<S::Error, T::Error>>
    where
        C: EventContext<Payload = T::Payload>,
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

    /// Returns `now + backoff(retry_count)`; used for scheduling retry timers.
    pub(super) fn next_retry_time(
        &self,
        retry_count: u32,
    ) -> Result<CompactDateTime, CompactDateTimeError> {
        let delay = calculate_backoff(&self.config, retry_count);
        let now = CompactDateTime::now()?;
        now.add_duration(delay)
    }
}
