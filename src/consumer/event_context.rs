//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, as well as detect shutdown.
//! - `TimerContext<T>`: Concrete `EventContext` implementation backed by a
//!   `TimerManager<T>` using a `TriggerStore` backend.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use async_trait::async_trait;
use dyn_clone::DynClone;
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

/// Provides shutdown notifications and timer operations to message handlers.
///
/// Handlers receive an implementation of `EventContext` that allows them to:
/// - Await a shutdown signal.
/// - Schedule a new timer for the current message key.
/// - Unschedule one or all existing timers for the key.
/// - Clear any scheduled timers and reschedule a fresh one.
/// - Inspect all scheduled timer execution times for the key.
/// - Check synchronously if shutdown has been requested.
pub trait EventContext: Clone + Send {
    /// Error type returned by timer-related operations.
    type Error: Error;

    /// Returns a future that resolves when a shutdown signal is received.
    ///
    /// # Returns
    ///
    /// A future that completes with `()` once shutdown is triggered.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send;

    /// Schedule a new timer at the given execution time for this key.
    ///
    /// # Arguments
    ///
    /// * `time` – The `CompactDateTime` at which the timer should fire.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if scheduling in the persistent store
    /// or in-memory scheduler fails.
    fn schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule all existing timers for this key, then schedule exactly one.
    ///
    /// All prior timers for this key are removed in parallel before a new
    /// timer at `time` is added.
    ///
    /// # Arguments
    ///
    /// * `time` – The time for the new, sole scheduled timer.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule or the final schedule
    /// operation fails.
    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule a single timer for this key at the specified time.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time of the timer to remove.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if the unschedule operation fails.
    fn unschedule(
        &self,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule *all* timers for this key.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule operation fails.
    fn clear_scheduled(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// List all scheduled execution times for timers on this key.
    ///
    /// # Returns
    ///
    /// A `Vec<CompactDateTime>` of all scheduled times.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if retrieving times from the persistent
    /// store fails.
    fn scheduled(&self) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send;

    /// Returns `true` if a shutdown signal has already been issued.
    fn should_shutdown(&self) -> bool;
}

/// Concrete implementation of `EventContext` that uses a `TimerManager<T>`.
///
/// Each `TimerContext` carries:
/// - `key`: The message key to scope timers.
/// - `shutdown_rx`: A watch channel receiver to detect shutdown.
/// - `timers`: A `TimerManager<T>` for persistent and in-memory timer state.
///
/// # Type Parameters
///
/// * `T`: The `TriggerStore` implementation backing the timer manager.
#[derive(Educe)]
#[educe(Debug, Clone(bound()))]
pub struct TimerContext<T> {
    /// Key for which timers are scoped.
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
    /// Create a new `TimerContext` binding a message key to timer operations.
    ///
    /// # Arguments
    ///
    /// * `key` – The message key for affinity and timer scoping.
    /// * `shutdown_rx` – A `watch::Receiver<bool>` that signals shutdown when
    ///   set.
    /// * `timers` – The `TimerManager<T>` instance.
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
        // Clone receiver so awaiting shutdown does not consume original.
        let mut shutdown_rx = self.shutdown_rx.clone();
        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
    }

    async fn schedule(&self, time: CompactDateTime) -> Result<(), Self::Error> {
        // Wrap scheduling in a tracing span for observability.
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

        // Unschedule all existing triggers in parallel, linking spans.
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

        // Schedule exactly one new trigger.
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

/// Boxed error type for object-safe contexts.
pub type BoxedError = Box<dyn Error + Send + Sync>;

/// Object-safe version of `EventContext` with boxed futures and errors.
///
/// Allows using `EventContext` trait objects where return types must be named.
///
/// # Object Safety
///
/// Each method is turned into `async fn` or returns a `bool` for synchronous
/// check.
#[async_trait]
pub trait DynEventContext: DynClone + Send + Sync {
    /// Async wait for shutdown signal.
    async fn on_shutdown(&self);

    /// Schedule a timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time to schedule.
    ///
    /// # Errors
    ///
    /// Returns an error if scheduling fails.
    async fn schedule(&self, time: CompactDateTime) -> Result<(), BoxedError>;

    /// Unschedule all existing timers and schedule a new one.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time.
    async fn clear_and_schedule(&self, time: CompactDateTime) -> Result<(), BoxedError>;

    /// Unschedule a specific timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The time to unschedule.
    async fn unschedule(&self, time: CompactDateTime) -> Result<(), BoxedError>;

    /// Unschedule all timers.
    async fn clear_scheduled(&self) -> Result<(), BoxedError>;

    /// List scheduled execution times.
    async fn scheduled(&self) -> Result<Vec<CompactDateTime>, BoxedError>;

    /// Synchronously check if shutdown has been requested.
    fn should_shutdown(&self) -> bool;
}

dyn_clone::clone_trait_object!(DynEventContext);

#[async_trait]
impl<C> DynEventContext for C
where
    C: EventContext + Send + Sync + 'static,
    C::Error: Error + Send + Sync + 'static,
{
    async fn on_shutdown(&self) {
        EventContext::on_shutdown(self).await;
    }

    async fn schedule(&self, time: CompactDateTime) -> Result<(), BoxedError> {
        EventContext::schedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn clear_and_schedule(&self, time: CompactDateTime) -> Result<(), BoxedError> {
        EventContext::clear_and_schedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn unschedule(&self, time: CompactDateTime) -> Result<(), BoxedError> {
        EventContext::unschedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn clear_scheduled(&self) -> Result<(), BoxedError> {
        EventContext::clear_scheduled(self)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    async fn scheduled(&self) -> Result<Vec<CompactDateTime>, BoxedError> {
        EventContext::scheduled(self)
            .await
            .map_err(|e| Box::new(e) as BoxedError)
    }

    fn should_shutdown(&self) -> bool {
        EventContext::should_shutdown(self)
    }
}
