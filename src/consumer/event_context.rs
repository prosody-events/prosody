//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, as well as detect shutdown.
//! - `TimerContext<T>`: Concrete `EventContext` implementation backed by a
//!   `TimerManager<T>` using a `TriggerStore` backend.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use crate::Key;
use crate::consumer::middleware::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{DELETE_CONCURRENCY, TimerManager, TimerType, Trigger};
use arc_swap::ArcSwapOption;
use async_stream::try_stream;
use async_trait::async_trait;
use dyn_clone::DynClone;
use educe::Educe;
use futures::stream::{iter, once};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, pin_mut};
use serde::de::StdError;
use std::error::Error;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::watch;
use tracing::{Span, error};

/// Marker trait for errors that can be returned from event context operations.
///
/// This trait is automatically implemented for any type that satisfies the
/// bounds.
pub trait EventContextError: StdError + ClassifyError + Send + Sync + 'static {}

impl<T> EventContextError for T where T: StdError + ClassifyError + Send + Sync + 'static {}

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
    type Error: EventContextError;

    /// Returns `true` if a partition shutdown signal has been issued.
    fn should_shutdown(&self) -> bool;

    /// Returns `true` if this message processing has been cancelled.
    fn should_cancel(&self) -> bool;

    /// Returns a future that resolves when a partition shutdown signal is
    /// received.
    ///
    /// # Returns
    ///
    /// A future that completes with `()` once partition shutdown is triggered.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Returns a future that resolves when message processing is cancelled.
    ///
    /// # Returns
    ///
    /// A future that completes with `()` once cancellation is triggered.
    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Schedule a new timer at the given execution time for this key.
    ///
    /// # Arguments
    ///
    /// * `time` – The `CompactDateTime` at which the timer should fire.
    /// * `timer_type` – The `TimerType` of the timer to schedule.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if scheduling in the persistent store
    /// or in-memory scheduler fails.
    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule all existing timers for this key, then schedule exactly one.
    ///
    /// All prior timers for this key are removed in parallel before a new
    /// timer at `time` is added.
    ///
    /// # Arguments
    ///
    /// * `time` – The time for the new, sole scheduled timer.
    /// * `timer_type` – The `TimerType` of the timer to schedule.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule or the final schedule
    /// operation fails.
    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule a single timer for this key at the specified time.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time of the timer to remove.
    /// * `timer_type` – The `TimerType` of the timer to remove.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if the unschedule operation fails.
    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Unschedule *all* timers for this key of the specified type.
    ///
    /// # Arguments
    ///
    /// * `timer_type` – The `TimerType` of timers to clear.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if any unschedule operation fails.
    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

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

    /// List all scheduled execution times for timers on this key of the
    /// specified type.
    ///
    /// # Arguments
    ///
    /// * `timer_type` – The `TimerType` to filter by.
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
        timer_type: TimerType,
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

    /// The timer currently being processed, if any.
    ///
    /// When processing a timer event, this is set to `Some((time, type))` to
    /// prevent scheduling a new timer at the same time and type, which would
    /// cause a collision in the active triggers registry.
    maybe_current_timer: Option<(CompactDateTime, TimerType)>,

    #[educe(Debug(ignore))]
    shutdown_rx: watch::Receiver<bool>,

    #[educe(Debug(ignore))]
    message_cancel_tx: watch::Sender<bool>,

    #[educe(Debug(ignore))]
    message_cancel_rx: watch::Receiver<bool>,

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
    /// * `maybe_current_timer` – If processing a timer event, the `(time,
    ///   type)` of that timer. This prevents scheduling collisions where a new
    ///   timer at the same time/type would be deactivated when the current
    ///   timer commits.
    /// * `shutdown_rx` – A `watch::Receiver<bool>` that signals shutdown when
    ///   set.
    /// * `timers` – The `TimerManager<T>` instance.
    pub(crate) fn new(
        key: Key,
        maybe_current_timer: Option<(CompactDateTime, TimerType)>,
        shutdown_rx: watch::Receiver<bool>,
        timers: TimerManager<T>,
    ) -> Self {
        let (message_cancel_tx, message_cancel_rx) = watch::channel(false);
        let inner = ArcSwapOption::new(Some(
            Inner {
                key,
                maybe_current_timer,
                shutdown_rx,
                message_cancel_tx,
                message_cancel_rx,
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

    fn should_cancel(&self) -> bool {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return true;
        };

        *inner.message_cancel_rx.borrow() | *inner.shutdown_rx.borrow()
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

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return ready(()).left_future();
        };

        let mut shutdown_rx = inner.shutdown_rx.clone();
        let mut message_cancel_rx = inner.message_cancel_rx.clone();

        async move {
            select! {
                result = shutdown_rx.wait_for(|is_shutdown| *is_shutdown) => {
                    if let Err(error) = result {
                        error!("shutdown hook failed: {error:#}");
                    }
                }
                result = message_cancel_rx.wait_for(|is_cancelled| *is_cancelled) => {
                    if let Err(error) = result {
                        error!("message cancellation hook failed: {error:#}");
                    }
                }
            }
        }
        .right_future()
    }

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        // Prevent scheduling at the same time/type as the current timer
        if inner.maybe_current_timer == Some((time, timer_type)) {
            return Err(TimerManagerError::ConflictsWithCurrentTimer);
        }

        let trigger = Trigger::new(inner.key.clone(), time, timer_type, Span::current());

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            () = EventContext::on_cancel(self) => Err(TimerManagerError::Cancelled),
            result = inner.timers.schedule(trigger) => result,
        }
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = Span::current();
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        // Prevent scheduling at the same time/type as the current timer
        if inner.maybe_current_timer == Some((time, timer_type)) {
            return Err(TimerManagerError::ConflictsWithCurrentTimer);
        }

        let operation = async {
            // Get scheduled triggers of this type
            let mut triggers_to_delete = inner
                .timers
                .scheduled_triggers(&inner.key, timer_type)
                .await?;
            triggers_to_delete.retain(|trigger| trigger.time != time);

            // Schedule exactly one new trigger.
            inner
                .timers
                .schedule(Trigger::new(
                    inner.key.clone(),
                    time,
                    timer_type,
                    span.clone(),
                ))
                .await?;

            // Unschedule all existing triggers in parallel, linking spans.
            iter(triggers_to_delete)
                .map(|trigger| {
                    let span_clone = span.clone();
                    async move {
                        // Link new span with the original trigger's span.
                        span_clone.follows_from(trigger.span());
                        inner
                            .timers
                            .unschedule(&trigger.key, trigger.time, trigger.timer_type)
                            .await
                    }
                })
                .buffer_unordered(DELETE_CONCURRENCY)
                .try_collect::<()>()
                .await
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            () = EventContext::on_cancel(self) => Err(TimerManagerError::Cancelled),
            result = operation => result,
        }
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            () = EventContext::on_cancel(self) => Err(TimerManagerError::Cancelled),
            result = inner.timers.unschedule(&inner.key, time, timer_type) => result,
        }
    }

    async fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        select! {
            () = EventContext::on_shutdown(self) => Err(TimerManagerError::Shutdown),
            () = EventContext::on_cancel(self) => Err(TimerManagerError::Cancelled),
            result = inner.timers.unschedule_all(&inner.key, timer_type) => result,
        }
    }

    fn invalidate(self) {
        // Signal cancellation to notify processors waiting on futures from `on_cancel`
        // to abort ongoing operations.
        if let Some(inner) = self.inner.load().as_ref() {
            let _ = inner.message_cancel_tx.send(true);
        }

        // Clear the inner state to prevent any further operations on this context.
        // This ensures resource cleanup (spans, channels) and prevents usage after
        // message processing completes, which could cause race conditions or
        // corruption if partition ownership has transferred.
        self.inner.store(None);
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        let on_shutdown = EventContext::on_shutdown(self);
        let on_cancel = EventContext::on_cancel(self);

        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return once(ready(Err(TimerManagerError::InvalidContext))).left_stream();
        };

        let inner = Arc::clone(inner);

        try_stream! {
            let scheduled_timers = inner.timers.scheduled_times(&inner.key, timer_type);

            pin_mut!(on_shutdown);
            pin_mut!(on_cancel);
            pin_mut!(scheduled_timers);

            while let Some(time) = select! {
                () = &mut on_shutdown => Err(TimerManagerError::Shutdown),
                () = &mut on_cancel => Err(TimerManagerError::Cancelled),
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
pub type BoxEventContextError = Box<dyn EventContextError>;

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
    /// Async wait for partition shutdown signal.
    async fn on_shutdown(&self);

    /// Async wait for message cancellation signal.
    async fn on_cancel(&self);

    /// Schedule a timer for the current key.
    ///
    /// # Arguments
    ///
    /// * `time` – The execution time to schedule.
    ///
    /// # Errors
    ///
    /// Returns an error if scheduling fails.
    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule all existing timers and schedule a new one.
    ///
    /// # Arguments
    ///
    /// * `time` – The new execution time.
    /// * `timer_type` – The timer type.
    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule a specific timer.
    ///
    /// # Arguments
    ///
    /// * `time` – The time to unschedule.
    /// * `timer_type` – The timer type.
    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule all timers of the specified type.
    ///
    /// # Arguments
    ///
    /// * `timer_type` – The timer type.
    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), BoxEventContextError>;

    /// List scheduled execution times for the specified type.
    ///
    /// # Arguments
    ///
    /// * `timer_type` – The timer type.
    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Pin<Box<dyn Stream<Item = Result<CompactDateTime, BoxEventContextError>> + Send + 'static>>;

    /// Synchronously check if partition shutdown has been requested.
    fn should_shutdown(&self) -> bool;

    /// Synchronously check if message cancellation has been requested.
    fn should_cancel(&self) -> bool;
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

    async fn on_cancel(&self) {
        EventContext::on_cancel(self).await;
    }

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::schedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::clear_and_schedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError> {
        EventContext::unschedule(self, time, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), BoxEventContextError> {
        EventContext::clear_scheduled(self, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Pin<Box<dyn Stream<Item = Result<CompactDateTime, BoxEventContextError>> + Send + 'static>>
    {
        Box::pin(
            EventContext::scheduled(self, timer_type)
                .map_err(|e| Box::new(e) as BoxEventContextError),
        )
    }

    fn should_shutdown(&self) -> bool {
        EventContext::should_shutdown(self)
    }

    fn should_cancel(&self) -> bool {
        EventContext::should_cancel(self)
    }
}
