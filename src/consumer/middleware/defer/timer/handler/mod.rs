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
//! coming even though this dispatch's own commit still advances — `Bypassed`,
//! no message marker records), and `NoInner` suppresses both.

use super::context::TimerDeferContext;
use super::store::TimerDeferStore;
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::defer::config::DeferConfiguration;
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::DeferError;
use crate::consumer::middleware::{FallibleHandler, Settlement, SettlementHandler};
use crate::consumer::{DemandType, Keyed};
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::timers::{TimerType, Trigger};
use crate::{Partition, Topic};
use std::sync::Arc;

mod operations;

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
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<T::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
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

    fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type)
    }

    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
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
            // `DeferredMessage` is owned by the message-defer middleware:
            // forward it unchanged so that owner can act on it.
            // `StateRecovery` is handled by the partition loop before a
            // trigger reaches the middleware stack, so it does not normally
            // arrive here; it is matched alongside only as a defensive
            // pass-through.
            TimerType::DeferredMessage | TimerType::StateRecovery => self
                .handler
                .on_timer(wrapped_context, trigger, demand_type)
                .await
                .map(TimerDeferOutput::Inner)
                .map_err(DeferError::Handler),
        }
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
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
        C: EventContext<Payload = T::Payload>,
    {
        // Symmetric to after_commit. The only twist: Deferred(e) still
        // routes to after_abort(Err(e)) on the inner — the inner's prior
        // dispatch is being rolled back regardless of whether the outer
        // commit/abort decision advanced this dispatch's own commit.
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

impl<T, S, D> SettlementHandler for TimerDeferHandler<T, S, D>
where
    T: SettlementHandler,
    S: TimerDeferStore,
    D: DeferralDecider,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        match result {
            // Inner ran: its result is the dispatch's outcome.
            Ok(TimerDeferOutput::Inner(output)) => T::settlement(Ok(output)),
            // Inner ran and its error surfaced.
            Err(DeferError::Handler(error)) => T::settlement(Err(error)),
            // `Deferred`/`NoInner` — parked for retry / queued behind /
            // orphan cleanup: the outcome lives in the defer queue, so
            // nothing here may stage or record. The error rows are the defer
            // layer's own rescue failing (store/timer/loader/backoff
            // computation) — a layer failure, never the event's outcome.
            Ok(TimerDeferOutput::Deferred(_) | TimerDeferOutput::NoInner)
            | Err(
                DeferError::Store(_)
                | DeferError::Timer(_)
                | DeferError::Loader(_)
                | DeferError::CompactTime(_),
            ) => Settlement::Bypassed,
        }
    }
}
