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
//! 2. Activation: call [`PendingTimer::fire()`] to transition to
//!    [`FiringTimer`]
//! 3. Processing: application handles the timer event
//! 4. Acknowledgment: application calls [`FiringTimer::commit()`] or
//!    [`FiringTimer::abort()`] on [`FiringTimer`]
//! 5. Cleanup: timers are removed from storage or left for retry
//!
//! Timers use at-least-once delivery and survive restarts. Successful commits
//! remove timers permanently; aborts deactivate them in-memory while preserving
//! persistent state for potential reloading.

use crate::consumer::{Keyed, Uncommitted};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::TimerManager;
use crate::timers::store::TriggerStore;
use crate::{Key, ProcessScope};
use arc_swap::ArcSwap;
use educe::Educe;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::sleep;
use tracing::{Span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Delay between retry attempts when commits fail.
const RETRY_DURATION: Duration = Duration::from_secs(1);

/// Shared no-op span used to release processing resources without allocating
/// a fresh `Arc` on every [`TriggerProcessGuard`] drop.
static NONE_SPAN: LazyLock<Arc<Span>> = LazyLock::new(|| Arc::new(Span::none()));

/// A trait for uncommitted timer operations.
///
/// Provides access to timer metadata and transaction operations,
/// hiding the concrete store implementation from clients.
///
/// Implemented by [`FiringTimer`] which represents a timer actively being
/// processed by a handler.
pub trait UncommittedTimer: Uncommitted + Keyed<Key = Key> + Send {
    /// The commit guard type for this timer.
    type CommitGuard: Uncommitted + Send;

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
    ///
    /// # Returns
    ///
    /// Tuple `(Trigger, Self::CommitGuard)`.
    fn into_inner(self) -> (Trigger, Self::CommitGuard);
}

/// Timer delivered from the queue but not yet transitioned to firing state.
///
/// Wraps a [`Trigger`] and an internal transaction state. Call
/// [`PendingTimer::fire()`] to transition to [`FiringTimer`] before processing.
/// If the timer was cancelled while queued, `fire()` returns `None`.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct PendingTimer<T>
where
    T: TriggerStore,
{
    /// The underlying timer data: key, execution time, and tracing span.
    trigger: Trigger,

    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// Timer currently being processed by a handler.
///
/// Created by calling [`PendingTimer::fire()`]. Owns the `commit()`/`abort()`
/// capability. Processing must end with either `commit()` or `abort()`.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct FiringTimer<T>
where
    T: TriggerStore,
{
    /// The underlying timer data: key, execution time, and tracing span.
    trigger: Trigger,

    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
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
    /// Logical key of the timer.
    key: Key,

    /// Scheduled execution time.
    time: CompactDateTime,

    /// Timer type classification.
    timer_type: TimerType,

    /// Manager coordinating persistent and in-memory state.
    manager: TimerManager<T>,

    /// Indicates if this timer has already been committed or aborted.
    completed: bool,

    /// Global timer semaphore permit; released when this trigger is dropped.
    _permit: OwnedSemaphorePermit,
}

/// RAII guard that releases timer processing resources (spans) on drop.
///
/// Ensures deterministic cleanup when timer processing completes, rather than
/// waiting for unpredictable garbage collection timing.
pub struct TriggerProcessGuard(Arc<ArcSwap<Span>>);

impl<T> PendingTimer<T>
where
    T: TriggerStore,
{
    /// Create a new pending timer from a delivered trigger.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The timer event with key, time, and tracing context.
    /// * `manager` - The [`TimerManager`] that will handle commit and abort.
    /// * `permit` - Semaphore permit bounding global timer concurrency; held
    ///   until this timer is dropped.
    ///
    /// # Returns
    ///
    /// A new [`PendingTimer`] in the pending state.
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>, permit: OwnedSemaphorePermit) -> Self {
        let key = trigger.key.clone();
        let time = trigger.time;
        let timer_type = trigger.timer_type;
        Self {
            trigger,
            uncommitted: UncommittedTrigger {
                key,
                time,
                timer_type,
                manager,
                completed: false,
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
    /// The `FiringTimer`'s trigger carries the canonical tag from
    /// `ActiveTriggers` at the moment of dispatch. This tag may differ from
    /// the tag on the queue-popped trigger if a `complete()`-from-
    /// `FiringRescheduled` rotation occurred while this entry was in the
    /// delay queue.
    ///
    /// # Returns
    ///
    /// `Some(FiringTimer)` if the timer is still active and can be processed,
    /// `None` if the timer was cancelled while waiting in the queue.
    pub async fn fire(mut self) -> Option<FiringTimer<T>> {
        // Attempt to transition from Scheduled → Firing, reading the canonical
        // tag from ActiveTriggers under the trigger-lock.
        let Some(canonical_tag) = self.uncommitted.fire_with_tag().await else {
            self.uncommitted.completed = true;
            return None;
        };

        // Re-stamp the trigger with the canonical tag so provisional-cell
        // writers can embed the observed-at-dispatch value. `tag` is excluded
        // from `Hash/Eq/Ord` (see `Trigger` doc), so the in-place write
        // preserves the `(key, time, timer_type)` identity used by any
        // downstream map keys.
        self.trigger.tag = canonical_tag;

        Some(FiringTimer {
            trigger: self.trigger,
            uncommitted: self.uncommitted,
        })
    }
}

impl<T> FiringTimer<T>
where
    T: TriggerStore,
{
    /// Returns a reference to the underlying [`Trigger`].
    ///
    /// See [`PendingTimer::fire`] for what the trigger's `tag` field means
    /// here.
    #[must_use]
    pub fn trigger(&self) -> &Trigger {
        &self.trigger
    }

    /// Replaces the trigger's tracing span with a `"trigger"` dispatch span.
    ///
    /// Creates the dispatch span from the trigger's current span context,
    /// connecting it per `relation`. Called at fire time so that
    /// `trigger.span()` returns the dispatch span when the handler reads it.
    pub fn set_dispatch_span(&self, relation: SpanRelation) {
        let context = self.trigger.span().context();
        let span = related_span!(
            relation,
            context,
            "trigger",
            key = %self.trigger.key,
            time = %self.trigger.time,
            timer_type = ?self.trigger.timer_type,
        );
        self.trigger.set_span(span);
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

impl<T> Keyed for FiringTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl<T> Keyed for PendingTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.trigger.key
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

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Attempt to transition the timer from `Scheduled` to `Firing` state,
    /// returning the value observed at the transition; see
    /// [`PendingTimer::fire`] for what "canonical" means here.
    ///
    /// Returns `None` if the transition failed (timer was cancelled or is not
    /// in `Scheduled` state).
    async fn fire_with_tag(&self) -> Option<i32> {
        self.manager
            .fire_with_tag(&self.key, self.time, self.timer_type)
            .await
    }

    /// Permanently remove the timer from storage and deactivate it.
    ///
    /// Retries indefinitely on failures, waiting `RETRY_DURATION` between
    /// attempts. Multiple commits or aborts are ignored.
    async fn commit(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        // Retry loop: ensure TimerManager::complete eventually succeeds.
        loop {
            match self
                .manager
                .complete(&self.key, self.time, self.timer_type)
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!("failed to commit timer: {error:#}; retrying");
                    sleep(RETRY_DURATION).await;
                }
            }
        }

        self.completed = true;
    }

    /// Deactivate the timer in-memory without removing from storage.
    ///
    /// The timer can fire again if reloaded. Multiple aborts or commits
    /// are ignored.
    async fn abort(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager
            .abort(&self.key, self.time, self.timer_type)
            .await;
        self.completed = true;
    }
}

impl<T> UncommittedTimer for FiringTimer<T>
where
    T: TriggerStore,
{
    type CommitGuard = UncommittedTriggerGuard<T>;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    /// Timer type classification.
    fn timer_type(&self) -> TimerType {
        self.trigger.timer_type
    }

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span {
        self.trigger.span.load().as_ref().clone()
    }

    /// Decompose into the raw [`Trigger`] and the commit guard.
    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        (self.trigger, UncommittedTriggerGuard::new(self.uncommitted))
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
        if !self.completed {
            warn!("timer was dropped without committing or aborting");
        }
    }
}

impl Drop for TriggerProcessGuard {
    fn drop(&mut self) {
        self.0.store(Arc::clone(&NONE_SPAN));
    }
}

impl<T> ProcessScope for FiringTimer<T>
where
    T: TriggerStore,
{
    type Guard = TriggerProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        TriggerProcessGuard(self.trigger.span.clone())
    }
}

#[cfg(test)]
mod tests;
