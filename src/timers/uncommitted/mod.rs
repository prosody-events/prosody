//! Uncommitted timer events and transaction-like semantics.
//!
//! Defines the timer event abstraction used for processing fired timers that
//! have been delivered to the application but not yet acknowledged:
//!
//! - [`UncommittedTimer`] - A trait providing timer metadata and transaction
//!   operations while hiding the concrete store implementation.
//! - [`PendingTimer`] - Timer delivered from the queue but not yet firing.
//! - [`FiringTimer`] - Timer currently being processed by a handler.
//!
//! Enforces a type-safe transaction pattern:
//!
//! 1. Delivery: timers arrive as [`PendingTimer`]
//! 2. Activation: the framework transitions a pending timer to [`FiringTimer`]
//! 3. Processing: application handles the timer event
//! 4. Acknowledgment: application calls [`FiringTimer::commit()`] or
//!    [`FiringTimer::abort()`] on [`FiringTimer`]
//! 5. Cleanup: timers are removed from storage or left for retry
//!
//! Timers use at-least-once delivery and survive restarts. Successful commits
//! remove timers permanently; aborts deactivate them in-memory while preserving
//! persistent state for potential reloading.

use crate::consumer::partition::ShutdownPhase;
use crate::consumer::receipted_sealed as sealed;
use crate::consumer::{Keyed, Receipted, Redelivery, Uncommitted};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::TriggerTrace;
use crate::timers::active::TimerState;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::{Fire, TimerManager};
use crate::timers::store::TriggerStore;
use crate::{Key, ProcessScope};
use arc_swap::ArcSwap;
use educe::Educe;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio::time::sleep;
use tracing::{Level, Span, error, warn};

/// Delay between retry attempts when commits fail.
pub(crate) const RETRY_DURATION: Duration = Duration::from_secs(1);

/// Shared released trace (a dispatched no-op span) used to release processing
/// resources without allocating a fresh `Arc` on every
/// [`TriggerProcessGuard`] drop.
static RELEASED_TRACE: LazyLock<Arc<TriggerTrace>> =
    LazyLock::new(|| Arc::new(TriggerTrace::Dispatched(Span::none())));

/// A trait for uncommitted timer operations.
///
/// Provides access to timer metadata and transaction operations,
/// hiding the concrete store implementation from clients.
///
/// Implemented by [`FiringTimer`] which represents a timer actively being
/// processed by a handler.
pub trait UncommittedTimer: Uncommitted + Keyed<Key = Key> + Send {
    /// The commit guard type for this timer.
    type CommitGuard: Receipted + Send;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime;

    /// Timer type classification.
    fn timer_type(&self) -> TimerType;

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span;

    /// Decompose into the raw [`Trigger`] and the commit guard.
    ///
    /// Useful for advanced scenarios that need the `Trigger` and the
    /// commit/abort capability as two independent values (e.g. moving the
    /// `Trigger` into a staged provisional-cell record while retaining the
    /// guard).
    fn into_inner(self) -> (Trigger, Self::CommitGuard);
}

/// Timer delivered from the queue but not yet transitioned to firing state.
///
/// Wraps a [`Trigger`] and an internal transaction state. Call
/// The framework transitions this timer to [`FiringTimer`] before processing.
/// If the timer was cancelled while queued, `fire()` returns `None`.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct PendingTimer<T>
where
    T: TriggerStore,
{
    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// Timer currently being processed by a handler.
///
/// Owns the `commit()` and `abort()` capability for a live timer.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct FiringTimer<T>
where
    T: TriggerStore,
{
    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// Classifies a fired timer before handler dispatch.
pub(crate) enum Fired<T>
where
    T: TriggerStore,
{
    /// The key row identifies a live attempt.
    Live(FiringTimer<T>),
    /// The key row is absent, so the attempt already committed.
    Committed(Key, UncommittedTriggerGuard<T>),
}

impl<T> Fired<T>
where
    T: TriggerStore,
{
    #[cfg(test)]
    pub(crate) fn into_live(self) -> Option<FiringTimer<T>> {
        match self {
            Self::Live(timer) => Some(timer),
            Self::Committed(..) => None,
        }
    }
}

/// A wrapper around `UncommittedTrigger` that implements [`Uncommitted`].
///
/// This wrapper is necessary because `UncommittedTrigger` implements [`Drop`]
/// and [`Uncommitted`] requires consuming `self`.
pub struct UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    /// The wrapped uncommitted trigger.
    inner: Option<UncommittedTrigger<T>>,
}

/// Internal transaction state for an uncommitted timer.
///
/// Tracks whether the timer has been completed and delegates commit/abort
/// operations to the [`TimerManager`].
struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// The attempt identity.
    trigger: Trigger,

    /// Manager coordinating persistent and in-memory state.
    manager: TimerManager<T>,

    /// Current receipt and completion phase.
    phase: Phase,

    /// Global timer semaphore permit; released when this trigger is dropped.
    _permit: OwnedSemaphorePermit,
}

/// Receipt and completion phase for one delivered timer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// The timer has no receipt and can still commit or abort.
    Active,
    /// The timer has a receipt and still has a redelivery source.
    Receipted,
    /// The timer committed or aborted.
    Completed,
}

/// RAII guard that releases timer processing resources (spans) on drop.
///
/// Ensures deterministic cleanup when timer processing completes, rather than
/// waiting for unpredictable garbage collection timing.
pub struct TriggerProcessGuard(Arc<ArcSwap<TriggerTrace>>);

impl<T> PendingTimer<T>
where
    T: TriggerStore,
{
    /// Create a new pending timer from a delivered trigger.
    ///
    /// Holds `permit`, which bounds global timer concurrency, until this
    /// timer is dropped.
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            uncommitted: UncommittedTrigger {
                trigger,
                manager,
                phase: Phase::Active,
                _permit: permit,
            },
        }
    }

    /// Transition this timer to the firing state.
    ///
    /// Consumes the `PendingTimer` and returns a [`FiringTimer`] if the timer
    /// is still active in the scheduler and successfully transitions to
    /// `Firing` state. If the timer was cancelled while queued, returns
    /// `None` and the timer is marked as completed.
    ///
    /// A live `FiringTimer` carries the tag from the store key row.
    /// A committed result has no key row and cannot reach a handler.
    pub(crate) async fn fire(self, shutdown: &watch::Receiver<ShutdownPhase>) -> Option<Fired<T>> {
        let mut uncommitted = self.uncommitted;
        let fire = loop {
            match uncommitted.manager.fire(&uncommitted.trigger).await {
                Ok(Some(fire)) => break fire,
                Ok(None) => {
                    uncommitted.phase = Phase::Completed;
                    return None;
                }
                Err(error) => {
                    if *shutdown.borrow() >= ShutdownPhase::Cancelling {
                        uncommitted.abort().await;
                        return None;
                    }
                    error!(error = %error, "failed to read timer receipt; retrying");
                    sleep(RETRY_DURATION).await;
                }
            }
        };
        Some(match fire {
            Fire::Live(tag) => {
                uncommitted.trigger.tag = tag;
                Fired::Live(FiringTimer { uncommitted })
            }
            Fire::Committed => {
                uncommitted.phase = Phase::Receipted;
                let key = uncommitted.trigger.key.clone();
                Fired::Committed(key, UncommittedTriggerGuard::new(uncommitted))
            }
        })
    }
}

impl<T> FiringTimer<T>
where
    T: TriggerStore,
{
    /// Returns a reference to the underlying [`Trigger`].
    ///
    /// The trigger tag comes from the store key row.
    #[must_use]
    pub fn trigger(&self) -> &Trigger {
        &self.uncommitted.trigger
    }

    /// Replaces the trigger's trace with a `"trigger"` dispatch span.
    ///
    /// Creates the dispatch span from the trigger's scheduling context,
    /// connecting it per `relation`. Called at fire time so that
    /// `trigger.span()` returns the dispatch span when the handler reads it.
    ///
    /// This is the single point where the configured timer span relation is
    /// applied for store-fired timers (deferred-retry reloads mint their own
    /// `timer_defer.load` dispatch span under the same relation), and it
    /// binds the dispatch span directly to the scheduling context — whether
    /// that context was captured in-process at scheduling time or restored
    /// from persistent storage — so memory- and Cassandra-backed timers
    /// produce identical dispatch-span topology.
    ///
    /// The span's level follows the fired timer's type
    /// ([`TimerType::is_application`]); two macro invocations because a
    /// tracing callsite's level is static.
    pub fn set_dispatch_span(&self, relation: SpanRelation) {
        let trigger = &self.uncommitted.trigger;
        let context = trigger.context();
        let span = if trigger.timer_type.is_application() {
            related_span!(
                level: Level::INFO,
                relation,
                context,
                "trigger",
                key = %trigger.key,
                timer.fire_time = %trigger.time.to_rfc3339(),
                timer.type = ?trigger.timer_type,
            )
        } else {
            related_span!(
                level: Level::DEBUG,
                relation,
                context,
                "trigger",
                key = %trigger.key,
                timer.fire_time = %trigger.time.to_rfc3339(),
                timer.type = ?trigger.timer_type,
            )
        };
        trigger.set_span(span);
    }
}

impl<T> Uncommitted for FiringTimer<T>
where
    T: TriggerStore,
{
    /// Commit this timer after successful processing.
    ///
    /// Repeated calls are ignored. Blocks until the underlying storage
    /// removal succeeds, retrying on errors.
    async fn commit(mut self) {
        self.uncommitted.commit().await;
    }

    /// Abort this timer without deleting persistent data.
    ///
    /// The timer is deactivated in-memory; it may be reloaded later.
    async fn abort(mut self) {
        self.uncommitted.abort().await;
    }
}

impl<T> Receipted for FiringTimer<T>
where
    T: TriggerStore,
{
    async fn redelivery(&self) -> Redelivery {
        self.uncommitted.redelivery().await
    }

    async fn receipt(&mut self) {
        self.uncommitted.receipt().await;
    }
}

impl<T> sealed::Sealed for FiringTimer<T> where T: TriggerStore {}

impl<T> Keyed for FiringTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.uncommitted.trigger.key
    }
}

impl<T> Keyed for PendingTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.uncommitted.trigger.key
    }
}

impl<T> UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    /// Create a new guard wrapping an uncommitted trigger.
    fn new(trigger: UncommittedTrigger<T>) -> Self {
        Self {
            inner: Some(trigger),
        }
    }
}

impl<T> Uncommitted for UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    /// Commit this timer after successful processing.
    async fn commit(mut self) {
        if let Some(mut trigger) = self.inner.take() {
            trigger.commit().await;
        }
    }

    /// Abort this timer without deleting persistent data.
    async fn abort(mut self) {
        if let Some(mut trigger) = self.inner.take() {
            trigger.abort().await;
        }
    }
}

impl<T> Receipted for UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    async fn redelivery(&self) -> Redelivery {
        match &self.inner {
            Some(trigger) => trigger.redelivery().await,
            None => Redelivery::Reruns,
        }
    }

    async fn receipt(&mut self) {
        if let Some(trigger) = &mut self.inner {
            trigger.receipt().await;
        }
    }
}

impl<T> sealed::Sealed for UncommittedTriggerGuard<T> where T: TriggerStore {}

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    async fn redelivery(&self) -> Redelivery {
        if self.trigger.timer_type != TimerType::Application {
            // Defer refires reload queued work that the defer middleware owns.
            return Redelivery::Reruns;
        }

        let state = self
            .manager
            .timer_state(
                &self.trigger.key,
                self.trigger.time,
                self.trigger.timer_type,
            )
            .await;
        // A rescheduled refire must run the application handler again.
        if matches!(state, Some(TimerState::FiringRescheduled)) {
            Redelivery::Reruns
        } else {
            Redelivery::Sweeps
        }
    }

    async fn receipt(&mut self) {
        if self.phase != Phase::Active {
            return;
        }

        loop {
            match self.manager.receipt(&self.trigger).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!("failed to record timer receipt: {error:#}; retrying");
                    sleep(RETRY_DURATION).await;
                }
            }
        }

        self.phase = Phase::Receipted;
    }

    /// Permanently remove the timer from storage and deactivate it.
    ///
    /// Retries indefinitely on failures, waiting `RETRY_DURATION` between
    /// attempts. Multiple commits or aborts are ignored.
    async fn commit(&mut self) {
        if self.phase == Phase::Completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        // Retry until the selected timer operation succeeds.
        loop {
            let result = if self.phase == Phase::Receipted {
                self.manager.retire(&self.trigger).await
            } else {
                self.manager.complete(&self.trigger).await
            };
            match result {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!("failed to commit timer: {error:#}; retrying");
                    sleep(RETRY_DURATION).await;
                }
            }
        }

        self.phase = Phase::Completed;
    }

    /// Deactivate the timer in-memory without removing from storage.
    ///
    /// The timer can fire again if reloaded. Multiple aborts or commits
    /// are ignored.
    async fn abort(&mut self) {
        if self.phase == Phase::Completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager.abort(&self.trigger).await;
        self.phase = Phase::Completed;
    }
}

impl<T> UncommittedTimer for FiringTimer<T>
where
    T: TriggerStore,
{
    type CommitGuard = UncommittedTriggerGuard<T>;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime {
        self.uncommitted.trigger.time
    }

    /// Timer type classification.
    fn timer_type(&self) -> TimerType {
        self.uncommitted.trigger.timer_type
    }

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span {
        self.uncommitted.trigger.span()
    }

    /// Decompose into the raw [`Trigger`] and the commit guard.
    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        let trigger = self.uncommitted.trigger.clone();
        (trigger, UncommittedTriggerGuard::new(self.uncommitted))
    }
}

impl<T> Drop for UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Warn if a timer is dropped without being committed or aborted.
    ///
    /// Helps detect resource leaks from unacknowledged timers.
    fn drop(&mut self) {
        if self.phase != Phase::Completed {
            warn!("timer was dropped without committing or aborting");
        }
    }
}

impl Drop for TriggerProcessGuard {
    fn drop(&mut self) {
        self.0.store(Arc::clone(&RELEASED_TRACE));
    }
}

impl<T> ProcessScope for FiringTimer<T>
where
    T: TriggerStore,
{
    type Guard = TriggerProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        TriggerProcessGuard(self.uncommitted.trigger.trace.clone())
    }
}

#[cfg(test)]
mod tests;
