//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, as well as detect shutdown.
//! - `TerminationSignals`: Internal trait for distinguishing shutdown from
//!   message-level cancellation (used by retry middleware).
//! - `TimerContext<T>`: Concrete `EventContext` implementation backed by a
//!   `TimerManager<T>` using a `TriggerStore` backend.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerType, Trigger};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use dyn_clone::DynClone;
use educe::Educe;
use futures::FutureExt;
use serde::de::StdError;
use std::error::Error;
use std::future::{Future, ready};
use std::ops::AsyncFnOnce;
use std::sync::Arc;
use tokio::select;
use tokio::sync::watch;
use tracing::Span;

/// Marker trait for errors that can be returned from event context operations.
///
/// This trait is automatically implemented for any type that satisfies the
/// bounds.
pub trait EventContextError: StdError + ClassifyError + Send + Sync + 'static {}

impl<T> EventContextError for T where T: StdError + ClassifyError + Send + Sync + 'static {}

/// Provides cancellation notifications and timer operations to message
/// handlers.
///
/// Handlers receive an implementation of `EventContext` that allows them to:
/// - Await a cancellation signal (includes partition shutdown).
/// - Schedule a new timer for the current message key.
/// - Unschedule one or all existing timers for the key.
/// - Clear any scheduled timers and reschedule a fresh one.
/// - Inspect all scheduled timer execution times for the key.
/// - Check synchronously if cancellation has been requested.
pub trait EventContext: TerminationSignals + Clone + Send + Sync + 'static {
    /// Error type returned by timer-related operations.
    type Error: EventContextError;

    /// Returns `true` if this message processing has been cancelled.
    ///
    /// Cancellation includes both message-level cancellation and partition
    /// shutdown.
    fn should_cancel(&self) -> bool;

    /// Returns a future that resolves when message processing is cancelled.
    ///
    /// Cancellation includes both message-level cancellation and partition
    /// shutdown.
    ///
    /// # Returns
    ///
    /// A future that completes with `()` once cancellation is triggered.
    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Trigger cancellation for this context.
    ///
    /// Signals that the current operation should be cancelled. Handlers should
    /// check `should_cancel()` or await `on_cancel()` and clean up promptly.
    ///
    /// This is used by middleware (e.g., timeout) to signal cancellation while
    /// continuing to wait for the handler to finish cleanup. Calling multiple
    /// times is idempotent.
    fn cancel(&self);

    /// Resets the message-level cancellation flag.
    ///
    /// Called by the canceller after the inner operation completes, so
    /// subsequent retry attempts start with a clean state. This is the
    /// counterpart to [`cancel`](Self::cancel).
    fn uncancel(&self);

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
    /// A `Vec` of all scheduled times on success.
    ///
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if retrieving times from the persistent
    /// store fails.
    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static;

    /// Return a boxed, type-erased event context
    fn boxed(self) -> BoxEventContext {
        Box::new(self)
    }
}

/// Distinguishes shutdown signals from message-level cancellation.
///
/// This trait is used internally by the retry middleware to determine whether
/// to abort immediately (shutdown) or treat cancellation as a transient error
/// and continue retrying (message cancellation).
///
/// - **Shutdown**: Partition revoked or consumer stopping. Processing must stop
///   immediately to release the partition.
/// - **Message cancellation**: Requested by middleware (e.g., timeout). Should
///   be treated as a transient error; retry logic should continue.
///
/// # Note
///
/// This trait is a supertrait of [`EventContext`] and must be public, but it is
/// considered an implementation detail. External users should not rely on these
/// methods directly.
pub trait TerminationSignals {
    /// Returns `true` if shutdown has been requested.
    ///
    /// Shutdown means the partition is being revoked or the consumer is
    /// stopping. Processing must abort immediately.
    fn is_shutdown(&self) -> bool;

    /// Returns `true` if message-level cancellation has been requested.
    ///
    /// Message cancellation should be treated as a transient error by retry
    /// logic, not as a signal to abort.
    fn is_message_cancelled(&self) -> bool;

    /// Returns a future that resolves when shutdown is requested.
    ///
    /// Use this in `select!` to abort retry sleep on shutdown while ignoring
    /// message-level cancellation.
    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static;

    /// Returns a future that resolves when message-level cancellation is
    /// requested.
    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static;
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
    shutdown_rx: watch::Receiver<ShutdownPhase>,

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
    /// * `shutdown_rx` – A `watch::Receiver<bool>` that signals shutdown when
    ///   set.
    /// * `timers` – The `TimerManager<T>` instance.
    pub(crate) fn new(
        key: Key,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
        timers: TimerManager<T>,
    ) -> Self {
        let (message_cancel_tx, message_cancel_rx) = watch::channel(false);
        let inner = ArcSwapOption::new(Some(
            Inner {
                key,
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

    /// Run a cancellable operation, short-circuiting if already shutdown or
    /// cancelled.
    ///
    /// Takes an async closure that receives `Arc<Inner<T>>` by value. This
    /// ensures no work is done when already cancelled, and the caller writes
    /// natural async code without explicit cloning.
    ///
    /// Uses separate watch channels directly rather than `on_cancel` (which
    /// redundantly includes shutdown checking).
    async fn run_cancellable<F, R>(&self, operation: F) -> Result<R, TimerManagerError<T::Error>>
    where
        F: AsyncFnOnce(Arc<Inner<T>>) -> Result<R, TimerManagerError<T::Error>>,
    {
        let guard = self.inner.load();
        let Some(inner) = guard.as_ref() else {
            return Err(TimerManagerError::InvalidContext);
        };

        // Short-circuit before constructing the future
        if *inner.shutdown_rx.borrow() >= ShutdownPhase::Cancelling {
            return Err(TimerManagerError::Shutdown);
        }
        if *inner.message_cancel_rx.borrow() {
            return Err(TimerManagerError::Cancelled);
        }

        // Clone once here; caller receives owned Arc in async closure
        let mut shutdown_rx = inner.shutdown_rx.clone();
        let mut cancel_rx = inner.message_cancel_rx.clone();
        let inner = Arc::clone(inner);

        select! {
            biased;
            _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Cancelling) => Err(TimerManagerError::Shutdown),
            _ = cancel_rx.wait_for(|v| *v) => Err(TimerManagerError::Cancelled),
            result = operation(inner) => result,
        }
    }
}

impl<T> EventContext for TimerContext<T>
where
    T: TriggerStore,
{
    type Error = TimerManagerError<T::Error>;

    fn should_cancel(&self) -> bool {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return true;
        };

        *inner.message_cancel_rx.borrow()
            || *inner.shutdown_rx.borrow() >= ShutdownPhase::Cancelling
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
                biased;
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Cancelling) => {}
                _ = message_cancel_rx.wait_for(|is_cancelled| *is_cancelled) => {}
            }
        }
        .right_future()
    }

    fn cancel(&self) {
        if let Some(inner) = self.inner.load().as_ref() {
            let _ = inner.message_cancel_tx.send(true);
        }
    }

    fn uncancel(&self) {
        if let Some(inner) = self.inner.load().as_ref() {
            inner.message_cancel_tx.send_replace(false);
        }
    }

    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        self.run_cancellable(async |inner| {
            let trigger = Trigger::new(inner.key.clone(), time, timer_type, Span::current());
            inner.timers.schedule(trigger).await
        })
        .await
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.run_cancellable(async |inner| {
            let trigger = Trigger::new(inner.key.clone(), time, timer_type, Span::current());
            inner.timers.clear_and_schedule(trigger).await
        })
        .await
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.run_cancellable(async |inner| {
            inner.timers.unschedule(&inner.key, time, timer_type).await
        })
        .await
    }

    async fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        self.run_cancellable(async |inner| {
            inner.timers.unschedule_all(&inner.key, timer_type).await
        })
        .await
    }

    fn invalidate(self) {
        // Signal cancellation to notify processors waiting on futures from `on_cancel`
        // to abort ongoing operations.
        self.cancel();

        // Clear the inner state to prevent any further operations on this context.
        // This ensures resource cleanup (spans, channels) and prevents usage after
        // message processing completes, which could cause race conditions or
        // corruption if partition ownership has transferred.
        self.inner.store(None);
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        // Cannot use `run_cancellable` here: the trait requires `+ 'static` on
        // the returned future, but `run_cancellable` borrows `self` via the
        // `AsyncFnOnce` closure. Instead we clone the required handles up front
        // and move them into the returned `async move` block — the same pattern
        // used by `on_shutdown` and `on_message_cancelled`.
        let guard = self.inner.load();
        let Some(inner) = guard.as_ref() else {
            return ready(Err(TimerManagerError::InvalidContext)).left_future();
        };

        if *inner.shutdown_rx.borrow() >= ShutdownPhase::Cancelling {
            return ready(Err(TimerManagerError::Shutdown)).left_future();
        }
        if *inner.message_cancel_rx.borrow() {
            return ready(Err(TimerManagerError::Cancelled)).left_future();
        }

        let mut shutdown_rx = inner.shutdown_rx.clone();
        let mut cancel_rx = inner.message_cancel_rx.clone();
        let inner = Arc::clone(inner);

        async move {
            select! {
                biased;
                _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Cancelling) => Err(TimerManagerError::Shutdown),
                _ = cancel_rx.wait_for(|v| *v) => Err(TimerManagerError::Cancelled),
                result = inner.timers.scheduled_times(&inner.key, timer_type) => result,
            }
        }
        .right_future()
    }
}

impl<T> TerminationSignals for TimerContext<T>
where
    T: TriggerStore,
{
    fn is_shutdown(&self) -> bool {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return true;
        };
        *inner.shutdown_rx.borrow() >= ShutdownPhase::Cancelling
    }

    fn is_message_cancelled(&self) -> bool {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return true;
        };
        *inner.message_cancel_rx.borrow()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return ready(()).left_future();
        };

        let mut shutdown_rx = inner.shutdown_rx.clone();
        async move {
            let _ = shutdown_rx
                .wait_for(|v| *v >= ShutdownPhase::Cancelling)
                .await;
        }
        .right_future()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.load();
        let Some(inner) = inner.as_ref() else {
            return ready(()).left_future();
        };

        let mut cancel_rx = inner.message_cancel_rx.clone();
        async move {
            let _ = cancel_rx.wait_for(|is_cancelled| *is_cancelled).await;
        }
        .right_future()
    }
}

/// Object-safe boxed event context
pub type BoxEventContext = Box<dyn DynEventContext>;

/// Boxed error type for object-safe contexts.
pub type BoxEventContextError = Box<dyn EventContextError>;

impl Error for BoxEventContextError {}

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
    /// Async wait for message cancellation signal (includes partition
    /// shutdown).
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
    async fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, BoxEventContextError>;

    /// Synchronously check if message cancellation has been requested (includes
    /// partition shutdown).
    fn should_cancel(&self) -> bool;
}

dyn_clone::clone_trait_object!(DynEventContext);

#[async_trait]
impl<C> DynEventContext for C
where
    C: EventContext + Send + Sync + 'static,
    C::Error: Error + Send + Sync + 'static,
{
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

    async fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, BoxEventContextError> {
        EventContext::scheduled(self, timer_type)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    fn should_cancel(&self) -> bool {
        EventContext::should_cancel(self)
    }
}
