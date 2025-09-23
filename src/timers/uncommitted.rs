//! Uncommitted timer events and transaction-like semantics.
//!
//! Defines the timer event abstraction used for processing fired timers that
//! have been delivered to the application but not yet acknowledged:
//!
//! - [`UncommittedTimer`] - A trait providing timer metadata and transaction
//!   operations while hiding the concrete store implementation.
//! - [`PendingTimer`] - The concrete implementation of [`UncommittedTimer`].
//!
//! Enforces a transaction-like pattern:
//!
//! 1. Delivery: timers arrive as [`UncommittedTimer`]
//! 2. Processing: application handles the timer event
//! 3. Acknowledgment: application calls [`Uncommitted::commit()`] or
//!    [`Uncommitted::abort()`]
//! 4. Cleanup: timers are removed from storage or left for retry
//!
//! Timers use at-least-once delivery and survive restarts. Successful commits
//! remove timers permanently; aborts deactivate them in-memory while preserving
//! persistent state for potential reloading.

use crate::consumer::{Keyed, Uncommitted};
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::TimerManager;
use crate::timers::store::TriggerStore;
use crate::{Key, ProcessScope};
use arc_swap::ArcSwap;
use educe::Educe;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{Span, warn};

/// Delay between retry attempts when commits fail.
const RETRY_DURATION: Duration = Duration::from_secs(1);

/// A trait for uncommitted timer operations.
///
/// Provides access to timer metadata and transaction operations,
/// hiding the concrete store implementation from clients.
pub trait UncommittedTimer: Uncommitted + Keyed<Key = Key> + Send {
    /// The commit guard type for this timer.
    type CommitGuard: Uncommitted + Send;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime;

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span;

    /// Check if this timer is still active in the in-memory scheduler.
    fn is_active(&self) -> impl Future<Output = bool> + Send;

    /// Decompose into the raw [`Trigger`] and the commit guard.
    ///
    /// Useful for advanced scenarios needing direct control over commit
    /// or abort without consuming the wrapper.
    ///
    /// # Returns
    ///
    /// Tuple `(Trigger, Self::CommitGuard)`.
    fn into_inner(self) -> (Trigger, Self::CommitGuard);
}

/// The concrete implementation of an uncommitted timer event.
///
/// Wraps a [`Trigger`] and an internal transaction state. After processing,
/// applications must call [`Uncommitted::commit()`] to remove the timer from
/// storage, or [`Uncommitted::abort()`] to deactivate it in-memory while
/// leaving persistent data.
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

/// A wrapper around [`UncommittedTrigger`] that implements [`Uncommitted`].
///
/// This wrapper is necessary because [`UncommittedTrigger`] implements [`Drop`]
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
pub struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Logical key of the timer.
    key: Key,

    /// Scheduled execution time.
    time: CompactDateTime,

    /// Manager coordinating persistent and in-memory state.
    manager: TimerManager<T>,

    /// Indicates if this timer has already been committed or aborted.
    completed: bool,
}

impl<T> PendingTimer<T>
where
    T: TriggerStore,
{
    /// Create a new uncommitted timer from a fired trigger.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The timer event with key, time, and tracing context.
    /// * `manager` - The [`TimerManager`] that will handle commit and abort.
    ///
    /// # Returns
    ///
    /// A new [`PendingTimer`] in the pending state.
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>) -> Self {
        let key = trigger.key.clone();
        let time = trigger.time;
        Self {
            trigger,
            uncommitted: UncommittedTrigger {
                key,
                time,
                manager,
                completed: false,
            },
        }
    }
}

impl<T> Uncommitted for PendingTimer<T>
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

    /// Check if the timer remains active in the scheduler.
    ///
    /// # Returns
    ///
    /// `true` if still loaded, `false` if completed or no longer scheduled.
    pub async fn is_active(&self) -> bool {
        match &self.inner {
            Some(trigger) => trigger.is_active().await,
            None => false,
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
    /// Check if the timer remains active in the scheduler.
    ///
    /// # Returns
    ///
    /// `true` if still loaded, `false` if completed or no longer scheduled.
    pub async fn is_active(&self) -> bool {
        self.manager.is_active(&self.key, self.time).await
    }

    /// Permanently remove the timer from storage and deactivate it.
    ///
    /// Retries indefinitely on failures, waiting `RETRY_DURATION` between
    /// attempts. Multiple commits or aborts are ignored.
    pub async fn commit(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        // Retry loop: ensure TimerManager::complete eventually succeeds.
        loop {
            match self.manager.complete(&self.key, self.time).await {
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
    pub async fn abort(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager.abort(&self.key, self.time).await;
        self.completed = true;
    }
}

impl<T> UncommittedTimer for PendingTimer<T>
where
    T: TriggerStore,
{
    type CommitGuard = UncommittedTriggerGuard<T>;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span {
        self.trigger.span.load().as_ref().clone()
    }

    /// Check if this timer is still active in the in-memory scheduler.
    async fn is_active(&self) -> bool {
        self.uncommitted.is_active().await
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

/// RAII guard that releases timer processing resources (spans) on drop.
///
/// Ensures deterministic cleanup when timer processing completes, rather than
/// waiting for unpredictable garbage collection timing.
pub struct TriggerProcessGuard(Arc<ArcSwap<Span>>);

impl Drop for TriggerProcessGuard {
    fn drop(&mut self) {
        self.0.store(Arc::new(Span::none()));
    }
}

impl<T> ProcessScope for PendingTimer<T>
where
    T: TriggerStore,
{
    type Guard = TriggerProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        TriggerProcessGuard(self.trigger.span.clone())
    }
}
