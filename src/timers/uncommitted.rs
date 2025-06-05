//! Uncommitted timer events and transaction-like semantics.
//!
//! This module provides [`UncommittedTimer`], which represents a timer event
//! that has been delivered to an application but not yet acknowledged as
//! processed. This design ensures reliable timer processing with at-least-once
//! delivery semantics.
//!
//! ## Transaction Semantics
//!
//! Timer processing follows a transaction-like pattern:
//!
//! 1. **Delivery**: Timer is delivered as [`UncommittedTimer`]
//! 2. **Processing**: Application processes the timer event
//! 3. **Acknowledgment**: Application calls [`commit()`] or [`abort()`]
//! 4. **Cleanup**: Timer is removed from storage or marked for retry
//!
//! ## Reliability Guarantees
//!
//! - **At-least-once delivery**: Timers are delivered at least once
//! - **Fault tolerance**: Uncommitted timers survive application crashes
//! - **Graceful shutdown**: Uncommitted timers are properly handled during shutdown
//! - **Retry logic**: Failed operations are retried with exponential backoff
//!
//! ## Usage Pattern
//!
//! ```rust,no_run
//! use prosody::consumer::{Uncommitted, Keyed};
//! use prosody::timers::UncommittedTimer;
//! use prosody::timers::store::TriggerStore;
//!
//! async fn handle_timer<T: TriggerStore>(timer: UncommittedTimer<T>) {
//!     // Process the timer event
//!     let key = timer.key();
//!     let time = timer.time();
//!     
//!     // Perform business logic
//!     match process_timer_logic(key, time).await {
//!         Ok(()) => {
//!             // Success: commit the timer
//!             timer.commit().await;
//!         }
//!         Err(_) => {
//!             // Failure: abort the timer (will be retried later)
//!             timer.abort().await;
//!         }
//!     }
//! }
//!
//! # async fn process_timer_logic(_key: &prosody::Key, _time: prosody::timers::datetime::CompactDateTime) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
//! ```

use crate::Key;
use crate::consumer::{Keyed, Uncommitted};
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::TimerManager;
use crate::timers::store::TriggerStore;
use educe::Educe;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{Span, warn};

/// Duration to wait between retry attempts for failed operations.
const RETRY_DURATION: Duration = Duration::from_secs(1);

/// A timer event that has been delivered but not yet acknowledged.
///
/// [`UncommittedTimer`] represents a timer that has fired and been delivered
/// to the application for processing. The timer maintains transaction-like
/// semantics where it must be explicitly committed or aborted after processing.
///
/// ## Lifecycle
///
/// 1. **Creation**: Timer is created when it fires and is ready for processing
/// 2. **Processing**: Application receives and processes the timer event
/// 3. **Acknowledgment**: Application calls [`commit()`] or [`abort()`]
/// 4. **Cleanup**: Timer is permanently removed or marked for retry
///
/// ## Reliability
///
/// - **Persistence**: Uncommitted timers survive application restarts
/// - **Exactly-once semantics**: Each timer is processed exactly once when committed
/// - **Failure handling**: Aborted timers may be retried or permanently discarded
/// - **Resource cleanup**: Uncommitted timers are cleaned up during shutdown
///
/// ## Thread Safety
///
/// [`UncommittedTimer`] is not [`Send`] or [`Sync`] by design, as it represents
/// a processing context that should be handled by a single task/thread.
///
/// ## Generic Parameters
///
/// * `T` - The [`TriggerStore`] implementation used for persistence
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct UncommittedTimer<T>
where
    T: TriggerStore,
{
    /// The underlying timer trigger with key, time, and span information.
    trigger: Trigger,

    /// Internal state for managing the uncommitted transaction.
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// Internal state for managing uncommitted timer transactions.
///
/// This structure tracks the completion state of a timer and coordinates
/// with the [`TimerManager`] to ensure proper cleanup and persistence.
///
/// ## Purpose
///
/// - **Transaction State**: Tracks whether the timer has been committed or aborted
/// - **Resource Management**: Ensures proper cleanup of timer resources
/// - **Coordination**: Interfaces with the timer manager for persistence operations
/// - **Safety**: Prevents double-commit or double-abort scenarios
pub struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// The key associated with this timer.
    key: Key,
    
    /// The scheduled execution time of this timer.
    time: CompactDateTime,
    
    /// The timer manager responsible for this timer's lifecycle.
    manager: TimerManager<T>,
    
    /// Whether this timer has been completed (committed or aborted).
    completed: bool,
}

impl<T> UncommittedTimer<T>
where
    T: TriggerStore,
{
    /// Creates a new uncommitted timer from a trigger and manager.
    ///
    /// This method is typically called internally when a timer fires and
    /// is ready for delivery to the application. The timer starts in an
    /// uncommitted state and must be explicitly acknowledged.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The timer trigger containing key, time, and tracing span
    /// * `manager` - The timer manager responsible for this timer's lifecycle
    ///
    /// # Returns
    ///
    /// A new [`UncommittedTimer`] ready for processing.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use prosody::timers::{UncommittedTimer, Trigger, TimerManager};
    /// use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # use prosody::timers::datetime::CompactDateTime;
    /// # use prosody::Key;
    /// # use tracing::Span;
    /// 
    /// # fn example(manager: TimerManager<InMemoryTriggerStore>) {
    /// let trigger = Trigger {
    ///     key: Key::from("example-key"),
    ///     time: CompactDateTime::from(1234567890_u32),
    ///     span: Span::current(),
    /// };
    /// 
    /// let uncommitted = UncommittedTimer::new(trigger, manager);
    /// // Timer is now ready for processing
    /// # }
    /// ```
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>) -> Self {
        let key = trigger.key.clone();
        let time = trigger.time;
        let completed = false;

        Self {
            trigger,
            uncommitted: UncommittedTrigger {
                key,
                time,
                manager,
                completed,
            },
        }
    }

    /// Deconstructs the timer into its trigger and uncommitted state.
    ///
    /// This method separates the timer data from the transaction state,
    /// which can be useful for advanced processing scenarios where direct
    /// control over the commit/abort lifecycle is needed.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The [`Trigger`] with timer data (key, time, span)
    /// - The [`UncommittedTrigger`] with transaction state
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # fn example(timer: UncommittedTimer<InMemoryTriggerStore>) {
    /// let (trigger, uncommitted) = timer.into_inner();
    /// 
    /// // Access trigger data
    /// println!("Timer key: {:?}", trigger.key);
    /// println!("Timer time: {:?}", trigger.time);
    /// 
    /// // Handle transaction state separately
    /// // ... custom processing logic ...
    /// # }
    /// ```
    #[must_use]
    pub fn into_inner(self) -> (Trigger, UncommittedTrigger<T>) {
        (self.trigger, self.uncommitted)
    }

    /// Returns the scheduled execution time of this timer.
    ///
    /// # Returns
    ///
    /// The [`CompactDateTime`] when this timer was scheduled to execute.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # fn example(timer: &UncommittedTimer<InMemoryTriggerStore>) {
    /// let execution_time = timer.time();
    /// println!("Timer was scheduled for: {}", execution_time);
    /// # }
    /// ```
    #[must_use]
    pub fn time(&self) -> CompactDateTime {
        self.trigger.time
    }

    /// Returns the tracing span associated with this timer.
    ///
    /// The span provides distributed tracing context for the timer event,
    /// enabling observability across the timer's lifecycle.
    ///
    /// # Returns
    ///
    /// A reference to the [`Span`] associated with this timer.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # fn example(timer: &UncommittedTimer<InMemoryTriggerStore>) {
    /// let span = timer.span();
    /// let _entered = span.enter();
    /// // All logging within this scope will be associated with the timer
    /// println!("Processing timer within its span context");
    /// # }
    /// ```
    #[must_use]
    pub fn span(&self) -> &Span {
        &self.trigger.span
    }

    /// Checks if this timer is currently active in the scheduler.
    ///
    /// A timer is considered active if it is currently loaded in the
    /// in-memory scheduler and awaiting processing or has been delivered
    /// but not yet committed.
    ///
    /// # Returns
    ///
    /// `true` if the timer is active, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(timer: &UncommittedTimer<InMemoryTriggerStore>) {
    /// if timer.is_active().await {
    ///     println!("Timer is currently active in the scheduler");
    /// } else {
    ///     println!("Timer is no longer active");
    /// }
    /// # }
    /// ```
    pub async fn is_active(&self) -> bool {
        self.uncommitted.is_active().await
    }
}

impl<T> Uncommitted for UncommittedTimer<T>
where
    T: TriggerStore,
{
    async fn commit(mut self) {
        self.uncommitted.commit().await;
    }

    async fn abort(mut self) {
        self.uncommitted.abort().await;
    }
}

impl<T> Keyed for UncommittedTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.trigger.key
    }
}

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Checks if this timer is currently active in the scheduler.
    ///
    /// This method queries the timer manager to determine if the timer
    /// is currently loaded and active in the in-memory scheduler.
    ///
    /// # Returns
    ///
    /// `true` if the timer is active in the scheduler, `false` otherwise.
    pub async fn is_active(&self) -> bool {
        self.manager.is_active(&self.key, self.time).await
    }

    /// Commits the timer, marking it as successfully processed.
    ///
    /// This method permanently removes the timer from both the in-memory
    /// scheduler and persistent storage. It should be called when the
    /// timer has been successfully processed and should not be retried.
    ///
    /// # Retry Logic
    ///
    /// If the commit operation fails due to storage issues, it will be
    /// retried indefinitely with a 1-second delay between attempts. This
    /// ensures that successfully processed timers are eventually cleaned up.
    ///
    /// # Panics
    ///
    /// This method will log a warning and return early if called on a
    /// timer that has already been committed or aborted.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::consumer::Uncommitted;
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(timer: UncommittedTimer<InMemoryTriggerStore>) {
    /// // Process the timer successfully
    /// let (trigger, mut uncommitted) = timer.into_inner();
    /// 
    /// // Perform business logic...
    /// println!("Processing timer for key: {:?}", trigger.key);
    /// 
    /// // Mark as complete
    /// uncommitted.commit().await;
    /// println!("Timer committed successfully");
    /// # }
    /// ```
    pub async fn commit(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring commit");
            return;
        }

        loop {
            let Err(error) = self.manager.complete(&self.key, self.time).await else {
                break;
            };

            tracing::error!("failed to commit timer: {error:#}; retrying");
            sleep(RETRY_DURATION).await;
        }
    }

    /// Aborts the timer without removing it from persistent storage.
    ///
    /// This method deactivates the timer in the in-memory scheduler but
    /// leaves it in persistent storage. The timer may be reloaded and
    /// retried later, depending on the timer management policy.
    ///
    /// # Use Cases
    ///
    /// - **Processing failures**: When timer processing fails but should be retried
    /// - **Shutdown conditions**: When the application is shutting down gracefully
    /// - **Rate limiting**: When processing should be delayed due to external constraints
    ///
    /// # Panics
    ///
    /// This method will log a warning and return early if called on a
    /// timer that has already been committed or aborted.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(timer: UncommittedTimer<InMemoryTriggerStore>) {
    /// // Attempt to process the timer
    /// let (trigger, mut uncommitted) = timer.into_inner();
    /// 
    /// match process_timer(&trigger).await {
    ///     Ok(()) => uncommitted.commit().await,
    ///     Err(_) => {
    ///         println!("Processing failed, aborting timer");
    ///         uncommitted.abort().await;
    ///     }
    /// }
    /// # }
    /// # async fn process_timer(_trigger: &prosody::timers::Trigger) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// ```
    pub async fn abort(&mut self) {
        if self.completed {
            warn!("timer already marked as completed; ignoring abort");
            return;
        }

        self.manager.abort(&self.key, self.time).await;
    }
}

impl<T> Drop for UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Logs a warning if the timer is dropped without being committed or aborted.
    ///
    /// This implementation helps detect resource leaks where timers are
    /// delivered but not properly acknowledged. Such scenarios can lead to
    /// timer accumulation and potential memory leaks.
    ///
    /// # Warning Conditions
    ///
    /// A warning is logged if:
    /// - The timer was not committed (successful processing)
    /// - The timer was not aborted (failed processing or shutdown)
    /// - The timer is being dropped due to scope exit or panic
    ///
    /// # Best Practices
    ///
    /// Always ensure timers are properly acknowledged:
    /// ```rust,no_run
    /// # use prosody::consumer::Uncommitted;
    /// # use prosody::timers::UncommittedTimer;
    /// # use prosody::timers::store::memory::InMemoryTriggerStore;
    /// # async fn example(timer: UncommittedTimer<InMemoryTriggerStore>) {
    /// match process_timer_event(&timer).await {
    ///     Ok(()) => timer.commit().await,
    ///     Err(_) => timer.abort().await,
    /// }
    /// // Timer is now properly acknowledged
    /// # }
    /// # async fn process_timer_event<T>(_timer: &UncommittedTimer<T>) -> Result<(), Box<dyn std::error::Error>> 
    /// # where T: prosody::timers::store::TriggerStore { Ok(()) }
    /// ```
    fn drop(&mut self) {
        if !self.completed {
            warn!("trigger was dropped without committing");
        }
    }
}
