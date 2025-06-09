//! Event context types and traits for message processing.
//!
//! This module defines the event context abstraction used for processing Kafka
//! messages and timer events:
//!
//! - `EventContext` – A trait providing shutdown notification capabilities and
//!   timer scheduling operations.
//! - `ConcreteEventContext` – The concrete implementation of `EventContext`.

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

/// A trait for event context operations in message processing.
///
/// Provides shutdown notification capabilities and timer scheduling operations
/// for message handlers.
pub trait EventContext: Clone + Send {
    /// The trigger store type used by this context.
    type Error: Error;

    /// Returns a future that completes when a shutdown signal is received.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send;

    /// Schedule a timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the scheduler or store operation fails.
    fn schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Remove all existing timers for the key, then schedule a new one.
    ///
    /// This first unschedules any existing timers for the key in parallel,
    /// then adds the new timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule or schedule operation
    /// fails.
    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule a specific timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time of the timer to remove.
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

    /// Retrieve all scheduled times for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the retrieval from storage fails.
    fn scheduled(&self) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send;

    /// Return `true` if a shutdown signal has been triggered.
    fn should_shutdown(&self) -> bool;
}

/// The concrete implementation of event context for message processing.
///
/// Provides the current message key, shutdown notification, and a handle to
/// the `TimerManager` for scheduling and managing timers.
///
/// # Type Parameters
///
/// * `T` – The `TriggerStore` implementation used by the timer manager.
#[derive(Educe)]
#[educe(Debug, Clone(bound()))]
pub struct ConcreteEventContext<T> {
    key: Key,

    #[educe(Debug(ignore))]
    shutdown_rx: watch::Receiver<bool>,

    #[educe(Debug(ignore))]
    timers: TimerManager<T>,
}

impl<T> ConcreteEventContext<T>
where
    T: TriggerStore,
{
    /// Create a new `ConcreteEventContext`.
    ///
    /// # Arguments
    ///
    /// * `key` – The message key for partition affinity.
    /// * `shutdown_rx` – A watch receiver that signals shutdown when `true`.
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

impl<T> EventContext for ConcreteEventContext<T>
where
    T: TriggerStore,
{
    type Error = TimerManagerError<T::Error>;

    /// Returns a future that completes when a shutdown signal is received.
    ///
    /// # Panics
    ///
    /// This method logs an error if waiting on the shutdown channel fails,
    /// but does not panic.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send {
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    /// Schedule a timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the scheduler or store operation fails.
    async fn schedule(&self, time: CompactDateTime) -> Result<(), Self::Error> {
        let span = info_span!("timer", key = %self.key, time = %time);
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    /// Remove all existing timers for the key, then schedule a new one.
    ///
    /// This first unschedules any existing timers for the key in parallel,
    /// then adds the new timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time for the timer.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule or schedule operation
    /// fails.
    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = info_span!("timer", key = %self.key, time = %time);

        // Unschedule all existing triggers for this key
        iter(self.timers.scheduled_triggers(&self.key).await?)
            .map(|trigger| {
                let span_clone = span.clone();
                async move {
                    // Link tracing spans
                    span_clone.follows_from(&trigger.span);
                    self.timers.unschedule(&trigger.key, trigger.time).await
                }
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect::<()>()
            .await?;

        // Schedule the new trigger
        self.timers
            .schedule(Trigger {
                key: self.key.clone(),
                time,
                span,
            })
            .await
    }

    /// Unschedule a specific timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time of the timer to remove.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the unschedule operation fails.
    async fn unschedule(&self, time: CompactDateTime) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule(&self.key, time).await
    }

    /// Unschedule all timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if any unschedule operation fails.
    async fn clear_scheduled(&self) -> Result<(), TimerManagerError<T::Error>> {
        self.timers.unschedule_all(&self.key).await
    }

    /// Retrieve all scheduled times for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `TimerManagerError` if the retrieval from storage fails.
    async fn scheduled(&self) -> Result<Vec<CompactDateTime>, TimerManagerError<T::Error>> {
        Ok(self
            .timers
            .scheduled_times(&self.key)
            .await?
            .into_iter()
            .collect())
    }

    /// Return `true` if a shutdown signal has been triggered.
    fn should_shutdown(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}
