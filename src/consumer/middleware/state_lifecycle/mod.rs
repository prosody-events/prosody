//! Thin keyed-state lifecycle middleware.
//!
//! Keyed-state machinery lives in the per-partition
//! [`StateManager`](crate::state::manager::StateManager) and the per-event
//! [`StateSession`]; this middleware
//! owns only the one interception-shaped concern — pairing the handler's
//! dispatch result with the session's seal/apply lifecycle:
//!
//! 1. On inner `Ok`, finalize the event's session: `Wal` collections seal
//!    (recorded inside the session), `Direct` collections apply. If anything
//!    sealed, arm the one-shot [`TimerType::StateRecovery`] backstop timer.
//! 2. `after_commit(Ok)` applies the recorded sealed set and clears the
//!    backstop only when every collection resolved; `after_abort(Ok)` rolls it
//!    back symmetrically. Both are best-effort — on partial failure the timer
//!    stays armed and the sweep retries.
//! 3. Finalize/arm errors surface as this middleware's own error and propagate
//!    to the retry middleware above — no retry machinery here.
//!
//! The middleware wraps no context and defines no output enum: the session
//! rides the context the partition loop built, reached through the
//! crate-private `LifecycleAccess` descriptor, and `Output = T::Output`
//! passes through untouched. `StateRecovery` triggers never reach this
//! stack — the partition loop intercepts them and runs the manager's sweep
//! directly.

use crate::consumer::DemandType;
use crate::consumer::event_context::{BoxEventContextError, EventContext, StateAccessError};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, HandlerMiddleware};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::session::sealed::{ApplyOutcome, FinalizeOutcome};
use crate::state::session::{LifecycleAccess, LifecycleView, StateSession};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::{Partition, Topic};
use std::error::Error as StdError;
use thiserror::Error;
use tracing::warn;

/// Stateless lifecycle middleware; see the module docs.
#[derive(Clone, Copy, Debug, Default)]
pub struct StateLifecycleMiddleware;

/// Per-partition provider produced by [`StateLifecycleMiddleware`].
pub struct StateLifecycleProvider<T> {
    inner: T,
}

/// Per-partition handler produced by [`StateLifecycleProvider`].
pub struct StateLifecycleHandler<T> {
    inner: T,
}

impl<T> StateLifecycleHandler<T>
where
    T: FallibleHandler,
{
    /// Finalizes the event's session after an inner `Ok` and arms the
    /// `StateRecovery` backstop when anything sealed.
    async fn finalize_state<C>(&self, context: &C) -> Result<(), StateLifecycleError<T::Error>>
    where
        C: EventContext,
    {
        let lifecycle = match context.state(LifecycleAccess) {
            Ok(view) => view,
            // A context without keyed state has nothing to finalize.
            Err(StateAccessError::Unavailable) => return Ok(()),
            Err(error) => return Err(StateLifecycleError::Lifecycle(error)),
        };
        match lifecycle
            .finalize()
            .await
            .map_err(StateLifecycleError::Lifecycle)?
        {
            FinalizeOutcome::Clean => Ok(()),
            FinalizeOutcome::Sealed => {
                let now = CompactDateTime::now()?;
                let fire = now.add_duration(lifecycle.recovery_fire_delay())?;
                context
                    .schedule(fire, TimerType::StateRecovery)
                    .await
                    .map_err(|e| StateLifecycleError::Timer(Box::new(e)))?;
                Ok(())
            }
        }
    }
}

impl<T> FallibleHandler for StateLifecycleHandler<T>
where
    T: FallibleHandler,
{
    type Error = StateLifecycleError<T::Error>;
    type Output = T::Output;
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
        let output = self
            .inner
            .on_message(context.clone(), message, demand_type)
            .await
            .map_err(StateLifecycleError::Inner)?;
        self.finalize_state(&context).await?;
        Ok(output)
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
        let output = self
            .inner
            .on_timer(context.clone(), trigger, demand_type)
            .await
            .map_err(StateLifecycleError::Inner)?;
        self.finalize_state(&context).await?;
        Ok(output)
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        match result {
            Ok(output) => {
                if let Ok(lifecycle) = context.state(LifecycleAccess) {
                    resolve_sealed_set(&context, &lifecycle, SealResolution::Apply).await;
                }
                self.inner.after_commit(context, Ok(output)).await;
            }
            Err(StateLifecycleError::Inner(error)) => {
                self.inner.after_commit(context, Err(error)).await;
            }
            // The inner returned Ok but finalize/arm failed afterwards:
            // nothing sealed (finalize records only on success), and the
            // best-effort hooks contract permits suppressing the inner
            // hook — the next dispatch recovers via first-touch or the
            // `StateRecovery` sweep.
            Err(_) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        match result {
            Ok(output) => {
                if let Ok(lifecycle) = context.state(LifecycleAccess) {
                    resolve_sealed_set(&context, &lifecycle, SealResolution::Rollback).await;
                }
                self.inner.after_abort(context, Ok(output)).await;
            }
            Err(StateLifecycleError::Inner(error)) => {
                self.inner.after_abort(context, Err(error)).await;
            }
            // See `after_commit`; suppression is symmetric.
            Err(_) => {}
        }
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

impl<T> FallibleHandlerProvider for StateLifecycleProvider<T>
where
    T: FallibleHandlerProvider,
{
    type Handler = StateLifecycleHandler<T::Handler>;

    fn handler_for_partition(&self, topic: Topic, partition: Partition) -> Self::Handler {
        StateLifecycleHandler {
            inner: self.inner.handler_for_partition(topic, partition),
        }
    }
}

impl<P> HandlerMiddleware<P> for StateLifecycleMiddleware
where
    P: Send + Sync + 'static,
{
    type Provider<T>
        = StateLifecycleProvider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>;

    fn with_provider<T>(&self, provider: T) -> Self::Provider<T>
    where
        T: FallibleHandlerProvider,
        T::Handler: FallibleHandler<Payload = P>,
    {
        StateLifecycleProvider { inner: provider }
    }
}

/// Which way the apply hook resolves the recorded sealed set.
#[derive(Clone, Copy)]
enum SealResolution {
    Apply,
    Rollback,
}

/// Best-effort resolution of the session's recorded sealed set inside an
/// apply hook. Clears the `StateRecovery` backstop only when every sealed
/// collection resolved — on any failure the timer stays armed so the sweep
/// retries, otherwise a committed write could be silently lost once the
/// sealed WAL row's TTL expires.
async fn resolve_sealed_set<C, S>(
    context: &C,
    lifecycle: &LifecycleView<S>,
    resolution: SealResolution,
) where
    C: EventContext,
    S: StateSession,
{
    let outcome = match resolution {
        SealResolution::Apply => lifecycle.commit_apply().await,
        SealResolution::Rollback => lifecycle.rollback_aborted().await,
    };
    match outcome {
        ApplyOutcome::NothingSealed => {}
        ApplyOutcome::Resolved => {
            if let Err(error) = context.clear_scheduled(TimerType::StateRecovery).await {
                // Best-effort: the sweep re-clears on its next fire.
                warn!(error = ?error, "failed to clear StateRecovery timer in apply hook");
            }
        }
        ApplyOutcome::Incomplete => {
            warn!("keyed-state seal resolution incomplete; leaving StateRecovery timer armed");
        }
    }
}

/// Errors raised by [`StateLifecycleMiddleware`].
#[derive(Debug, Error)]
pub enum StateLifecycleError<E>
where
    E: ClassifyError + StdError + Send + 'static,
{
    /// The wrapped handler returned an error.
    #[error("wrapped handler failed")]
    Inner(#[source] E),

    /// Sealing or direct-applying the event's session failed.
    #[error("keyed-state lifecycle failed")]
    Lifecycle(#[source] StateAccessError),

    /// Arming the recovery timer failed (type-erased context error).
    #[error("keyed-state recovery timer failed: {0:#}")]
    Timer(BoxEventContextError),

    /// `CompactDateTime` arithmetic failed when computing the recovery
    /// fire time.
    #[error(transparent)]
    DateTime(#[from] CompactDateTimeError),
}

impl<E> ClassifyError for StateLifecycleError<E>
where
    E: ClassifyError + StdError + Send + 'static,
{
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Inner(e) => e.classify_error(),
            Self::Lifecycle(e) => e.classify_error(),
            Self::Timer(e) => e.classify_error(),
            Self::DateTime(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod tests;
