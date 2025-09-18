//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, as well as detect shutdown.
//! - `TimerContext<T>`: Concrete `EventContext` implementation backed by a
//!   `TimerManager<T>` using a `TriggerStore` backend.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use arc_swap::ArcSwapOption;
use async_stream::try_stream;
use async_trait::async_trait;
use dyn_clone::DynClone;
use educe::Educe;
use futures::stream::{iter, once};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, pin_mut};
use std::error::Error;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::watch;
use tracing::{Span, error};

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
pub trait EventContext: Clone + Send + Sync + 'static {
    /// Error type returned by timer-related operations.
    type Error: Error + Send + Sync + 'static;

    /// Returns `true` if a shutdown signal has already been issued.
    fn should_shutdown(&self) -> bool;

    /// Returns a future that resolves when a shutdown signal is received.
    ///
    /// # Returns
    ///
    /// A future that completes with `()` once shutdown is triggered.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static;

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

    /// Invalidate this context to prevent further usage after message
    /// processing.
    ///
    /// Contexts can be cloned during processing, but must be invalidated after
    /// message completion to prevent race conditions in key-based processing
    /// and data corruption when partition ownership changes. This ensures all
    /// associated resources (such as tracing spans) are properly cleaned up
    /// and the context cannot be used from language bindings where the Rust
    /// compiler cannot enforce lifecycle constraints.
    fn invalidate(self);

    /// List all scheduled execution times for timers on this key.
    ///
    /// # Returns
    ///
    /// A stream of scheduled time results.
    ///
    /// # Errors
    ///
    /// Items will be `Err(Self::Error)` if retrieving times from the persistent
    /// store fails.
    fn scheduled(
        &self,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static;

    /// Return a boxed, type-erased event context
    fn boxed(self) -> BoxEventContext {
        Box::new(self)
    }
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

#[derive(Debug, Clone)]
pub struct TimerContext<T> {
    /// Context state
    inner: Arc<ArcSwapOption<Inner<T>>>,
}

#[derive(Educe)]
#[educe(Debug)]
struct Inner<T> {
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
        let inner = ArcSwapOption::new(Some(
            Inner {
                key,
                shutdown_rx,
                timers,
            }
            .into(),
        ))
        .into();

        Self { inner }
    }
}

impl<T> EventContext for TimerContext<T>
where
    T: TriggerStore,
{
    type Error = TimerManagerError<T::Error>;

    fn should_shutdown(&self) -> bool {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return true;
        };

        *inner.shutdown_rx.borrow()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return ready(()).left_future();
        };

        let mut shutdown_rx = inner.shutdown_rx.clone();

        async move {
            if let Err(error) = shutdown_rx.wait_for(|is_shutdown| *is_shutdown).await {
                error!("shutdown hook failed: {error:#}");
            }
        }
        .right_future()
    }

    async fn schedule(&self, time: CompactDateTime) -> Result<(), Self::Error> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        let trigger = Trigger::new(inner.key.clone(), time, Span::current());

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            result = inner.timers.schedule(trigger) => result,
        }
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        let operation = async {
            // Get scheduled triggers
            let triggers = inner.timers.scheduled_triggers(&inner.key).await?;

            // Unschedule all existing triggers in parallel, linking spans.
            iter(triggers)
                .map(|trigger| {
                    let span_clone = span.clone();
                    async move {
                        // Link new span with the original trigger's span.
                        span_clone.follows_from(trigger.span().as_ref());
                        inner.timers.unschedule(&trigger.key, trigger.time).await
                    }
                })
                .buffer_unordered(DELETE_CONCURRENCY)
                .try_collect::<()>()
                .await?;

            // Schedule exactly one new trigger.
            inner
                .timers
                .schedule(Trigger::new(inner.key.clone(), time, span))
                .await
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            result = operation => result,
        }
    }

    async fn unschedule(&self, time: CompactDateTime) -> Result<(), TimerManagerError<T::Error>> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            result = inner.timers.unschedule(&inner.key, time) => result,
        }
    }

    async fn clear_scheduled(&self) -> Result<(), TimerManagerError<T::Error>> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            result = inner.timers.unschedule_all(&inner.key) => result,
        }
    }

    fn invalidate(self) {
        // Clear the inner state to prevent any further operations on this context.
        // This ensures resource cleanup (spans, channels) and prevents usage after
        // message processing completes, which could cause race conditions or
        // corruption if partition ownership has transferred.
        self.inner.store(None);
    }

    fn scheduled(
        &self,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        let on_shutdown = EventContext::on_shutdown(self);

        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return once(ready(Err(TimerManagerError::InvalidContext))).left_stream();
        };

        let inner = Arc::clone(inner);

        try_stream! {
            let scheduled_timers = inner.timers.scheduled_times(&inner.key);

            pin_mut!(on_shutdown);
            pin_mut!(scheduled_timers);

            while let Some(time) = select! {
                () = &mut on_shutdown => Err(TimerManagerError::Shutdown),
                item = scheduled_timers.try_next() => item,
            }? {
                yield time;
            }
        }
        .right_stream()
    }
}

/// Object-safe boxed event context
pub type BoxEventContext = Box<dyn DynEventContext>;

/// Boxed error type for object-safe contexts.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Object-safe version of `EventContext` with boxed futures and errors.
///
/// Allows using `EventContext` trait objects where return types must be named.
///
/// # Object Safety
///
/// Each method is turned into `async fn` or returns a `bool` for synchronous
/// check.
#[async_trait]
pub trait DynEventContext: DynClone + Send + Sync + 'static {
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
    async fn schedule(&self, time: CompactDateTime) -> Result<(), BoxError>;

    /// Unschedule all existing timers and schedule a new one.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time.
    async fn clear_and_schedule(&self, time: CompactDateTime) -> Result<(), BoxError>;

    /// Unschedule a specific timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The time to unschedule.
    async fn unschedule(&self, time: CompactDateTime) -> Result<(), BoxError>;

    /// Unschedule all timers.
    async fn clear_scheduled(&self) -> Result<(), BoxError>;

    /// List scheduled execution times.
    fn scheduled(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CompactDateTime, BoxError>> + Send + 'static>>;

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

    async fn schedule(&self, time: CompactDateTime) -> Result<(), BoxError> {
        EventContext::schedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxError)
    }

    async fn clear_and_schedule(&self, time: CompactDateTime) -> Result<(), BoxError> {
        EventContext::clear_and_schedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxError)
    }

    async fn unschedule(&self, time: CompactDateTime) -> Result<(), BoxError> {
        EventContext::unschedule(self, time)
            .await
            .map_err(|e| Box::new(e) as BoxError)
    }

    async fn clear_scheduled(&self) -> Result<(), BoxError> {
        EventContext::clear_scheduled(self)
            .await
            .map_err(|e| Box::new(e) as BoxError)
    }

    fn scheduled(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CompactDateTime, BoxError>> + Send + 'static>> {
        Box::pin(EventContext::scheduled(self).map_err(|e| Box::new(e) as BoxError))
    }

    fn should_shutdown(&self) -> bool {
        EventContext::should_shutdown(self)
    }
}
