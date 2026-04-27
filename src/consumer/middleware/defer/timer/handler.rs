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
//!
//! # Apply hooks
//!
//! The inner is invoked at most once per dispatch. [`TimerDeferOutput`]
//! encodes the routing: `Inner` forwards the framework's chosen hook,
//! `Deferred` always fires `after_abort` (the original timer's retry is
//! coming even though our marker commits), and `NoInner` suppresses both.

use super::context::TimerDeferContext;
use super::store::{TimerDeferStore, TimerRetryCompletionResult};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::defer::calculate_backoff;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::DeferError;
use crate::consumer::{DemandType, Keyed};
use crate::error::{ClassifyError, ErrorCategory};
use crate::telemetry::event::TimerEventType;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::timers::{TimerType, Trigger};
use crate::{Partition, Topic};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Output of [`TimerDeferHandler`] dispatches; drives apply-hook routing.
///
/// See the module-level apply-hooks section.
#[derive(Debug)]
pub enum TimerDeferOutput<O, E> {
    /// Inner ran and produced an output; forward the surrounding hook.
    Inner(O),
    /// Inner did not run (orphan `DeferredTimer` or queue-append for an
    /// already-deferred key) — suppress both apply hooks.
    NoInner,
    /// Inner ran and returned a transient error captured for retry. Both
    /// hooks fire `after_abort(Err(e))`: the `DeferredTimer` will
    /// re-dispatch the same logical event.
    Deferred(E),
}

/// Per-partition handler wrapping an inner handler with timer defer logic.
///
/// Created by [`TimerDeferProvider`](super::middleware::TimerDeferProvider)
/// as part of a defer handler stack.
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
    /// Telemetry sender for this partition.
    pub(crate) sender: TelemetryPartitionSender,
    /// Consumer group id used as source in telemetry events.
    pub(crate) source: Arc<str>,
}

impl<T, S, D> FallibleHandler for TimerDeferHandler<T, S, D>
where
    T: FallibleHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
{
    type Error = DeferError<S::Error, T::Error>;
    /// Encodes the inner's outcome; drives apply-hook routing. See
    /// [`TimerDeferOutput`].
    type Output = TimerDeferOutput<T::Output, T::Error>;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext,
    {
        // Wrap context so inner handlers see unified timer state
        let wrapped_context =
            TimerDeferContext::new(context, self.store.clone(), message.key().clone());

        self.handler
            .on_message(wrapped_context, message, demand_type)
            .await
            .map(TimerDeferOutput::Inner)
            .map_err(DeferError::Handler)
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
        // Wrap context so inner handlers see unified timer state
        let wrapped_context =
            TimerDeferContext::new(context, self.store.clone(), trigger.key.clone());

        match trigger.timer_type {
            TimerType::DeferredTimer => self.handle_deferred_timer(wrapped_context, trigger).await,
            TimerType::Application => {
                self.handle_application_timer(wrapped_context, trigger, demand_type)
                    .await
            }
            TimerType::DeferredMessage => self
                .handler
                .on_timer(wrapped_context, trigger, demand_type)
                .await
                .map(TimerDeferOutput::Inner)
                .map_err(DeferError::Handler),
        }
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        // Apply-hook routing (see module docs):
        // - Inner(o):     inner ran and succeeded         -> after_commit(Ok)
        // - NoInner:      no inner dispatch happened      -> suppress
        // - Deferred(e):  inner ran, transient err swallowed by defer ->
        //   after_abort(Err(e)) (a retry is coming)
        // - Handler(e):   inner ran and surfaced an error -> after_commit(Err)
        // - Store/Timer/...: defer-layer error before/after inner work; no inner apply
        //   work to forward -> suppress.
        match result {
            Ok(TimerDeferOutput::Inner(output)) => {
                self.handler.after_commit(context, Ok(output)).await;
            }
            Ok(TimerDeferOutput::Deferred(inner_err)) => {
                self.handler.after_abort(context, Err(inner_err)).await;
            }
            Err(DeferError::Handler(error)) => {
                self.handler.after_commit(context, Err(error)).await;
            }
            Ok(TimerDeferOutput::NoInner) | Err(_) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext,
    {
        // Symmetric to after_commit. The only twist: Deferred(e) still
        // routes to after_abort(Err(e)) on the inner — the inner's prior
        // dispatch is being rolled back regardless of whether the outer
        // commit/abort decision committed our defer marker.
        match result {
            Ok(TimerDeferOutput::Inner(output)) => {
                self.handler.after_abort(context, Ok(output)).await;
            }
            Ok(TimerDeferOutput::Deferred(inner_err)) => {
                self.handler.after_abort(context, Err(inner_err)).await;
            }
            Err(DeferError::Handler(error)) => {
                self.handler.after_abort(context, Err(error)).await;
            }
            Ok(TimerDeferOutput::NoInner) | Err(_) => {}
        }
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
    async fn handle_application_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext,
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
    async fn handle_deferred_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
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

        let output = match self
            .handler
            .on_timer(context.clone(), stored_trigger.clone(), DemandType::Failure)
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
    /// coverage on partial failure.
    ///
    /// Returns [`TimerDeferOutput::Deferred`] carrying the inner error so the
    /// apply hooks can drive `after_abort(Err(inner_err))` on the inner: the
    /// inner's prior dispatch is being rolled back even though our defer
    /// marker commits.
    async fn defer_first_timer<C>(
        &self,
        context: C,
        trigger: &Trigger,
        inner_err: T::Error,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
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

        Ok(TimerDeferOutput::Deferred(inner_err))
    }

    /// Appends timer to an already-deferred key's queue (maintains ordering).
    /// The inner handler is not invoked for this dispatch — the trigger is a
    /// pure side-effect on the defer queue.
    async fn append_to_deferred_queue(
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
    async fn handle_retry_failure<C>(
        &self,
        context: &C,
        deferred_trigger: &Trigger,
        stored_trigger: &Trigger,
        retry_count: u32,
        error: T::Error,
    ) -> Result<TimerDeferOutput<T::Output, T::Error>, DeferError<S::Error, T::Error>>
    where
        C: EventContext,
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

    /// Returns `now + backoff(retry_count)`; used for scheduling retry timers.
    fn next_retry_time(&self, retry_count: u32) -> Result<CompactDateTime, CompactDateTimeError> {
        let delay = calculate_backoff(&self.config, retry_count);
        let now = CompactDateTime::now()?;
        now.add_duration(delay)
    }
}
