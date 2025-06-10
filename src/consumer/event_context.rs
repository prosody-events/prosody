//! Provides an execution context for processing Kafka messages and timer
//! events.
//!
//! This module defines:
//! - `EventContext`: Trait for shutdown notification and timer scheduling.
//! - `TimerContext<T>`: Concrete `EventContext` implementation using a
//!   `TimerManager`.

use educe::Educe;
use futures::stream::iter;
use futures::{StreamExt, TryStreamExt};
use std::error::Error;
use std::future::Future;
use tokio::sync::watch;
use tracing::{error, info_span};

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{DELETE_CONCURRENCY, TimerManager, Trigger};

/// Provides shutdown notification and timer operations to message handlers.
///
/// Handlers receive an `EventContext` to:
/// - Await shutdown signals.
/// - Schedule, unschedule, or clear timers for the current message key.
/// - Inspect which timers are scheduled.
/// - Check if shutdown has been requested.
pub trait EventContext: Clone + Send {
    /// The error type returned by timer operations.
    type Error: Error;

    /// Returns a future completing when a shutdown signal is received.
    ///
    /// # Returns
    ///
    /// A future that resolves to `()` once shutdown is triggered.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send;

    /// Schedule a timer for the current key at the given time.
    ///
    /// # Arguments
    ///
    /// * `time` – Execution time for the new timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if scheduling in the store or scheduler
    /// fails.
    fn schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule all existing timers for the key, then schedule a new one.
    ///
    /// This first removes any existing timers for the key in parallel, then
    /// adds the new timer at `time`.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any removal or the new scheduling
    /// fails.
    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule a specific timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – Execution time of the timer to remove.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the unschedule operation fails.
    fn unschedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule all timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule operation fails.
    fn clear_scheduled(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retrieve all execution times of scheduled timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if retrieval from the persistent store
    /// fails.
    fn scheduled(&self) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send;

    /// Returns `true` if a shutdown signal has been issued.
    fn should_shutdown(&self) -> bool;
}

/// Concrete implementation of `EventContext` for message processing.
///
/// Provides the message `key`, a shutdown watcher, and a `TimerManager` for
/// scheduling and managing timers.
///
/// # Type Parameters
///
/// * `T` – The `TriggerStore` backend used by the `TimerManager`.
#[derive(Educe)]
#[educe(Debug, Clone(bound()))]
pub struct TimerContext<T> {
    /// Message key for affinity and timer scoping.
    key: Key,

    #[educe(Debug(ignore))]
    shutdown_rx: watch::Receiver<bool>,

    #[educe(Debug(ignore))]
    timers: TimerManager<T>,
}

impl<T> TimerContext<T>
where
    T: TriggerStore,
{
    /// Constructs a new `TimerContext`.
    ///
    /// # Arguments
    ///
    /// * `key` – The message key for timer operations.
    /// * `shutdown_rx` – A watch receiver that signals shutdown when set to
    ///   `true`.
    /// * `timers` – The `TimerManager` instance for scheduling timers.
    pub(crate) fn new(
        key: Key,
        shutdown_rx: watch::Receiver<bool>,
        timers: TimerManager<T>,
    ) -> Self {
        Self {
            key,
            shutdown_rx,
            timers,
        }
    }
}

impl<T> EventContext for TimerContext<T>
where
    T: TriggerStore,
{
    type Error = TimerManagerError<T::Error>;

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send {
        // Clone receiver to await shutdown without consuming the original.
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    async fn schedule(&self, time: CompactDateTime) -> Result<(), Self::Error> {
        // Track the scheduling operation under a tracing span.
        let span = info_span!("timer", key = %self.key, time = %time);
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = info_span!("timer", key = %self.key, time = %time);

        // Unschedule all existing triggers for this key in parallel.
        iter(self.timers.scheduled_triggers(&self.key).await?)
            .map(|trigger| {
                let span_clone = span.clone();
                async move {
                    // Link new span with the original trigger's span.
                    span_clone.follows_from(&trigger.span);
                    self.timers.unschedule(&trigger.key, trigger.time).await
                }
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await?;

        // Schedule the new trigger.
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    async fn unschedule(&self, time: CompactDateTime) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule(&self.key, time).await
    }

    async fn clear_scheduled(&self) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule_all(&self.key).await
    }

    async fn scheduled(&self) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        Ok(self
            .timers
            .scheduled_times(&self.key)
            .await?
            .into_iter()
            .collect())
    }

    fn should_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}
