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
//! 4. Acknowledgment: application calls [`Uncommitted::commit()`] or
//!    [`Uncommitted::abort()`] on [`FiringTimer`]
//! 5. Cleanup: timers are removed from storage or left for retry
//!
//! Timers use at-least-once delivery and survive restarts. Successful commits
//! remove timers permanently; aborts deactivate them in-memory while preserving
//! persistent state for potential reloading.

use crate::consumer::{Keyed, Uncommitted};
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::TimerManager;
use crate::timers::store::TriggerStore;
use crate::{Key, ProcessScope};
use arc_swap::ArcSwap;
use educe::Educe;
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
    /// Useful for advanced scenarios needing direct control over commit
    /// or abort without consuming the wrapper.
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

    /// Timer type classification.
    timer_type: TimerType,

    /// Manager coordinating persistent and in-memory state.
    manager: TimerManager<T>,

    /// Indicates if this timer has already been committed or aborted.
    completed: bool,
}

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
    ///
    /// # Returns
    ///
    /// A new [`PendingTimer`] in the pending state.
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>) -> Self {
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
    /// # Returns
    ///
    /// `Some(FiringTimer)` if the timer is still active and can be processed,
    /// `None` if the timer was cancelled while waiting in the queue.
    pub async fn fire(mut self) -> Option<FiringTimer<T>> {
        // Attempt to transition from Scheduled → Firing
        if !self.uncommitted.fire().await {
            // Timer was cancelled or not in Scheduled state; mark as completed
            self.uncommitted.completed = true;
            return None;
        }

        // Transfer ownership to FiringTimer
        Some(FiringTimer {
            trigger: self.trigger,
            uncommitted: self.uncommitted,
        })
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
    /// Attempt to transition the timer from `Scheduled` to `Firing` state.
    ///
    /// # Returns
    ///
    /// `true` if the transition succeeded, `false` if the timer is not in
    /// `Scheduled` state (e.g., cancelled or already firing).
    pub async fn fire(&self) -> bool {
        self.manager
            .fire(&self.key, self.time, self.timer_type)
            .await
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
    pub async fn abort(&mut self) {
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
mod tests {
    use super::*;
    use crate::consumer::Uncommitted;
    use crate::heartbeat::HeartbeatRegistry;
    use crate::timers::duration::CompactDuration;
    use crate::timers::manager::TimerManager;
    use crate::timers::store::adapter::TableAdapter;
    use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
    use color_eyre::eyre::{Result, eyre};
    use futures::{StreamExt, TryStreamExt};
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::task;
    use tokio::time::{self, advance};
    use uuid::Uuid;

    /// Helper function to set up a timer manager for testing.
    ///
    /// Returns `(stream, manager, _shutdown_tx)`. The caller holds
    /// `_shutdown_tx` and can send `true` to stop the background slab loader.
    async fn setup_timer_manager() -> Result<(
        impl futures::Stream<Item = PendingTimer<TableAdapter<InMemoryTriggerStore>>>,
        TimerManager<TableAdapter<InMemoryTriggerStore>>,
        watch::Sender<bool>,
    )> {
        let store = memory_store();
        let segment_id = Uuid::new_v4();
        let slab_size = CompactDuration::new(300);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (stream, manager) = TimerManager::new(
            segment_id,
            slab_size,
            "test-manager",
            store,
            HeartbeatRegistry::test(),
            shutdown_rx,
        )
        .await
        .map_err(|e| eyre!("Failed to create timer manager: {}", e))?;

        Ok((stream, manager, shutdown_tx))
    }

    /// Helper function to create a test trigger
    fn create_test_trigger(
        key: &str,
        seconds_offset: u32,
        timer_type: TimerType,
    ) -> Result<Trigger> {
        let time = CompactDateTime::now()?.add_duration(CompactDuration::new(seconds_offset))?;

        Ok(Trigger::new(
            Key::from(key),
            time,
            timer_type,
            tracing::Span::current(),
        ))
    }

    #[tokio::test]
    async fn test_pending_timer_fire_consumes() -> Result<()> {
        time::pause();

        let (mut stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let trigger = create_test_trigger("fire-test", 1, TimerType::Application)?;

        manager.schedule(trigger.clone()).await?;

        // Advance time to trigger emission
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Get the pending timer
        let pending_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;

        // Verify fire() consumes the PendingTimer and returns FiringTimer
        let firing_timer = pending_timer
            .fire()
            .await
            .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

        // Verify the FiringTimer has correct metadata
        assert_eq!(firing_timer.time(), trigger.time);
        assert_eq!(firing_timer.timer_type(), TimerType::Application);
        assert_eq!(firing_timer.key(), &trigger.key);

        // Clean up by committing
        firing_timer.commit().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_firing_timer_commit() -> Result<()> {
        time::pause();

        let (mut stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let trigger = create_test_trigger("commit-test", 1, TimerType::Application)?;

        manager.schedule(trigger.clone()).await?;

        // Advance time to trigger emission
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Get and fire the timer
        let pending_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;
        let firing_timer = pending_timer
            .fire()
            .await
            .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

        // Commit the timer
        firing_timer.commit().await;

        // Verify the timer was removed from storage
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert!(times.is_empty(), "Timer should be removed after commit");

        Ok(())
    }

    #[tokio::test]
    async fn test_firing_timer_abort() -> Result<()> {
        time::pause();

        let (mut stream, manager, _shutdown_tx) = setup_timer_manager().await?;
        let trigger = create_test_trigger("abort-test", 1, TimerType::Application)?;

        manager.schedule(trigger.clone()).await?;

        // Advance time to trigger emission
        advance(Duration::from_secs(2)).await;
        task::yield_now().await;

        // Get and fire the timer
        let pending_timer = stream
            .next()
            .await
            .ok_or_else(|| eyre!("Expected a pending timer"))?;
        let firing_timer = pending_timer
            .fire()
            .await
            .ok_or_else(|| eyre!("Expected fire() to return Some"))?;

        // Abort the timer
        firing_timer.abort().await;

        // Verify the timer is still in storage (abort preserves DB state)
        let times = manager
            .scheduled_times(&trigger.key, TimerType::Application)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(times.len(), 1, "Timer should remain in storage after abort");
        assert!(times.contains(&trigger.time));

        Ok(())
    }
}
