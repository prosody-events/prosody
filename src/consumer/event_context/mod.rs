//! Execution context for Kafka message and timer event handling.
//!
//! This module defines abstractions for delivering shutdown signals and
//! managing timer scheduling within message handlers. It provides:
//! - `EventContext`: Trait for handler contexts to schedule, unschedule, clear,
//!   and list timers, bind keyed-state descriptors, and detect shutdown.
//! - `TerminationSignals`: Internal trait for distinguishing shutdown from
//!   message-level cancellation (used by retry middleware).
//! - `PartitionEventContext<T, S>`: Concrete `EventContext` implementation
//!   backed by a `TimerManager<T>` and a per-event keyed-state session `S`.
//! - `DynEventContext`: Object-safe wrapper around any `EventContext`.

use crate::Key;
use crate::codec::ErasedStateCodec;
use crate::consumer::kafka_state::message_state;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::RepinProof;
use crate::consumer::partition::ShutdownPhase;
use crate::error::ClassifyError;
use crate::loader::MessageLoader;
use crate::state::descriptor::{Registered, StateDescriptor, value_state};
use crate::state::session::CellSession;
use crate::timers::datetime::CompactDateTime;
use crate::timers::error::TimerManagerError;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerRequest, TimerType, timer_span};
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
use tracing::{Instrument, Span, error, field::Empty, field::display};

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
    /// The message payload type events on this context carry.
    ///
    /// Leaf contexts pin it to the consumer's codec payload; wrapper
    /// contexts forward their inner context's payload. Handler traits
    /// bound their contexts with `C: EventContext<Payload = Self::Payload>`,
    /// which is what lets payload-typed capabilities (e.g. the keyed-state
    /// Kafka-message handles) stay fully typed inside generic handlers.
    type Payload: Send + Sync + 'static;

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
    /// # Errors
    ///
    /// Returns `Err(Self::Error)` if retrieving times from the persistent
    /// store fails.
    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static;

    /// The per-event keyed-state session descriptor binds operate over.
    ///
    /// Leaf contexts carry the session the partition loop minted for the
    /// event; wrapper contexts forward their inner context's session type
    /// (`type State = C::State`). State itself is Kafka-agnostic, so the
    /// payload tie lives here: the session's loader yields `Self::Payload`,
    /// which keeps Kafka-message handles fully typed inside generic handlers
    /// without [`CellSession`] ever naming a payload — its `Loader` associated
    /// type is pinned to the message loader here.
    type State: CellSession<Loader: MessageLoader<Payload = Self::Payload>>;

    /// Binds a registered keyed-state collection, returning its typed handle.
    ///
    /// Takes a [`Registered<DESC>`] capability handle, not a raw descriptor, so
    /// a handler can bind only collections it registered — binding an
    /// unregistered one is a compile error, not a runtime one. (The access-time
    /// [`verify_state_registration`](crate::state::session::CellSession::verify_state_registration)
    /// check is the backstop for names that slip past the type system, e.g.
    /// through the erased FFI seam.)
    ///
    /// Works in message and timer handlers alike. The returned handle owns a
    /// cheap `Arc`-backed clone of the session, so it is `Send + Sync +
    /// 'static`; repeated binds of one collection share the per-event
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] when keyed state is not
    /// wired, [`StateAccessError::Unregistered`] for a collection never
    /// registered with the consumer, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the descriptor's.
    fn state<DESC>(
        &self,
        registered: Registered<DESC>,
    ) -> Result<DESC::Handle<Self::State>, StateAccessError>
    where
        DESC: StateDescriptor;

    /// Rebuilds this context re-pinned to the session's CURRENT attempt epoch —
    /// the crate-internal re-pin primitive wrapper contexts forward (like
    /// [`state`](Self::state)). Gated by [`RepinProof`], so only the
    /// `next_attempt` verb and the settle final-hook stamp (the two mint sites)
    /// produce a live attempt-N+1 (or stamped-final) view; a leaked stale clone
    /// can never re-pin itself.
    ///
    /// The leaf rebuilds into a **fresh** inner cell, so a leaked clone of the
    /// prior context keeps its stale pin (and stays fenced). An invalidated
    /// context (its inner already stored `None`) stays invalidated — re-pin
    /// must never resurrect it. Wrapper contexts forward to their inner,
    /// recursively, so the fence reaches the whole stack.
    #[must_use]
    fn redispatch(&self, proof: RepinProof) -> Self;

    /// Return a boxed, type-erased event context for the FFI seam.
    ///
    /// The payload must map to a codec ([`ErasedStateCodec`]) — the erased
    /// state ops recover it from the payload — which every FFI payload does.
    fn boxed(self) -> BoxEventContext<Self::Payload>
    where
        Self::Payload: ErasedStateCodec,
    {
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

/// Concrete leaf [`EventContext`] constructed once per event by the
/// partition loop.
///
/// Each `PartitionEventContext` carries:
/// - `key`: The message key to scope timers.
/// - `shutdown_rx`: A watch channel receiver to detect shutdown.
/// - `timers`: A `TimerManager<T>` for persistent and in-memory timer state.
/// - `session`: The per-event keyed-state session minted by the partition's
///   state manager; descriptor binds route to it through
///   [`EventContext::state`].
///
/// # Type Parameters
///
/// * `T`: The `TriggerStore` implementation backing the timer manager.
/// * `S`: The per-event [`CellSession`]; its payload pins
///   [`EventContext::Payload`].
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound = ""))]
pub struct PartitionEventContext<T: TriggerStore, S> {
    /// Context state
    inner: Arc<ArcSwapOption<Inner<T, S>>>,
}

#[derive(Educe)]
#[educe(Debug)]
struct Inner<T: TriggerStore, S> {
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

    #[educe(Debug(ignore))]
    session: S,
}

impl<T, S> PartitionEventContext<T, S>
where
    T: TriggerStore,
{
    /// Create a new `PartitionEventContext` binding a message key to timer
    /// operations and the event's keyed-state session.
    ///
    /// `shutdown_rx` short-circuits operations once it reaches
    /// `>= ShutdownPhase::Cancelling`. `message_cancel` is the per-event
    /// cancellation channel created by the partition loop, so the session's
    /// termination watch shares the same receiver.
    pub(crate) fn new(
        key: Key,
        shutdown_rx: watch::Receiver<ShutdownPhase>,
        message_cancel: (watch::Sender<bool>, watch::Receiver<bool>),
        timers: TimerManager<T>,
        session: S,
    ) -> Self {
        let (message_cancel_tx, message_cancel_rx) = message_cancel;
        let inner = ArcSwapOption::new(Some(
            Inner {
                key,
                shutdown_rx,
                message_cancel_tx,
                message_cancel_rx,
                timers,
                session,
            }
            .into(),
        ))
        .into();

        Self { inner }
    }

    /// Run a cancellable operation, short-circuiting if already shutdown or
    /// cancelled.
    ///
    /// Takes an async closure that receives `Arc<Inner<T, S>>` by value. This
    /// ensures no work is done when already cancelled, and the caller writes
    /// natural async code without explicit cloning.
    ///
    /// Uses separate watch channels directly rather than `on_cancel` (which
    /// redundantly includes shutdown checking).
    async fn run_cancellable<F, R>(&self, operation: F) -> Result<R, TimerManagerError<T::Error>>
    where
        F: AsyncFnOnce(Arc<Inner<T, S>>) -> Result<R, TimerManagerError<T::Error>>,
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

    /// Runs a cancellable operation inside `span`, recording the guard's key
    /// on it once the closure runs and any failure as an error event in its
    /// scope — the hand-built equivalent of `#[instrument(err)]`, needed
    /// because timer-op span levels follow the runtime [`TimerType`]
    /// (`timer_span!`).
    ///
    /// The key is recorded through the owned `span` handle, never
    /// `Span::current()`: a level-disabled span (an internal timer op under
    /// an info filter) never becomes current, so recording through "current"
    /// would deface the ambient event span with a duplicate `key`.
    async fn run_spanned<F, R>(
        &self,
        span: Span,
        operation: F,
    ) -> Result<R, TimerManagerError<T::Error>>
    where
        F: AsyncFnOnce(Arc<Inner<T, S>>) -> Result<R, TimerManagerError<T::Error>>,
    {
        let result = self
            .run_cancellable(async |inner| {
                span.record("key", display(&inner.key));
                operation(inner).await
            })
            .instrument(span.clone())
            .await;
        if let Err(error) = &result {
            span.in_scope(|| error!(error = %error));
        }
        result
    }
}

impl<T, S> EventContext for PartitionEventContext<T, S>
where
    T: TriggerStore,
    S: CellSession<Loader: MessageLoader>,
{
    type Error = TimerManagerError<T::Error>;
    type Payload = <S::Loader as MessageLoader>::Payload;
    type State = S;

    fn state<DESC>(&self, registered: Registered<DESC>) -> Result<DESC::Handle<S>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        // Live-guard at bind time: an invalidated context refuses new
        // handles; handles themselves re-guard per operation through the
        // session's termination watch.
        let guard = self.inner.load();
        let Some(inner) = guard.as_ref() else {
            return Err(StateAccessError::Terminated);
        };
        registered.descriptor().bind(&inner.session)
    }

    fn redispatch(&self, proof: RepinProof) -> Self {
        let guard = self.inner.load();
        let Some(inner) = guard.as_ref() else {
            // Invalidated context: nothing to re-pin, and re-pin must never
            // resurrect it — return an equally-invalidated (stateless) clone.
            return self.clone();
        };
        // A FRESH outer `Arc<ArcSwapOption<Inner>>`: leaked clones of the prior
        // context share the OLD Arc (old session, old pin) and stay fenced,
        // while the re-pinned session shares the SAME `SessionInner`
        // (dirty/gate/oracle/epoch) — only its `pinned` epoch differs.
        let repinned = Inner {
            key: inner.key.clone(),
            shutdown_rx: inner.shutdown_rx.clone(),
            message_cancel_tx: inner.message_cancel_tx.clone(),
            message_cancel_rx: inner.message_cancel_rx.clone(),
            timers: inner.timers.clone(),
            session: inner.session.repin(proof),
        };
        Self {
            inner: Arc::new(ArcSwapOption::new(Some(Arc::new(repinned)))),
        }
    }

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

    /// The schedule span is what a fired timer's `"trigger"` dispatch span
    /// relates back to (as `OTel` parent or link, per the configured
    /// `timer_spans`): the request captures it as the trigger's scheduling
    /// context, and its `key`/`timer.fire_time`/`timer.type` attributes make
    /// the relationship self-describing. Span level follows the timer type
    /// ([`TimerType::is_application`]); `key` lives behind `run_spanned`, so
    /// it is recorded once the closure runs.
    async fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), Self::Error> {
        let span = timer_span!(
            timer_type,
            "schedule",
            key = Empty,
            timer.fire_time = %time.to_rfc3339(),
            timer.type = ?timer_type,
        );
        // The request carries the owned span (the trigger's scheduling
        // context), not `Span::current()` — see `run_spanned` for why a
        // level-disabled span must not fall back to the ambient one.
        let request_span = span.clone();
        self.run_spanned(span, async |inner| {
            let request = TimerRequest::new(inner.key.clone(), time, timer_type, request_span);
            inner.timers.schedule(request).await
        })
        .await
    }

    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = timer_span!(
            timer_type,
            "clear_and_schedule",
            key = Empty,
            timer.fire_time = %time.to_rfc3339(),
            timer.type = ?timer_type,
        );
        // The request carries the owned span (the trigger's scheduling
        // context), not `Span::current()` — see `run_spanned` for why a
        // level-disabled span must not fall back to the ambient one.
        let request_span = span.clone();
        self.run_spanned(span, async |inner| {
            let request = TimerRequest::new(inner.key.clone(), time, timer_type, request_span);
            inner.timers.clear_and_schedule(request).await
        })
        .await
    }

    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = timer_span!(
            timer_type,
            "unschedule",
            key = Empty,
            timer.fire_time = %time.to_rfc3339(),
            timer.type = ?timer_type,
        );
        self.run_spanned(span, async |inner| {
            inner.timers.unschedule(&inner.key, time, timer_type).await
        })
        .await
    }

    async fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<(), TimerManagerError<T::Error>> {
        let span = timer_span!(
            timer_type,
            "clear_scheduled",
            key = Empty,
            timer.type = ?timer_type,
        );
        self.run_spanned(span, async |inner| {
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

impl<T, S> TerminationSignals for PartitionEventContext<T, S>
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
pub type BoxEventContext<P> = Box<dyn DynEventContext<Payload = P>>;

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
    /// The message payload type events on this context carry; mirrors
    /// [`EventContext::Payload`] so `Box<dyn DynEventContext<Payload = P>>`
    /// keeps the payload nameable across the type-erased FFI boundary.
    type Payload: Send + Sync + 'static;

    /// Async wait for message cancellation signal (includes partition
    /// shutdown).
    async fn on_cancel(&self);

    /// Schedule a timer for the current key.
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
    async fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule a specific timer.
    async fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<(), BoxEventContextError>;

    /// Unschedule all timers of the specified type.
    async fn clear_scheduled(&self, timer_type: TimerType) -> Result<(), BoxEventContextError>;

    /// List scheduled execution times for the specified type.
    async fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, BoxEventContextError>;

    /// Synchronously check if message cancellation has been requested (includes
    /// partition shutdown).
    fn should_cancel(&self) -> bool;

    // Keyed-state ops — the FFI seam the bindings wrap. They reuse the *same*
    // typed `state(...)` path as the Rust API, recovering the codec from the
    // payload via [`ErasedStateCodec`] (value cells) or `MessageRefCodec`
    // (message refs); the blanket impl's `Self::Payload: ErasedStateCodec`
    // bound is what restricts a boxed context to the FFI payloads. See
    // [`ErasedStateCodec`] for the recovered-codec-matches-registration
    // invariant; each op's `# Errors` notes the per-call failures.

    /// Reads the named value collection's cell, decoded with the recovered
    /// consumer codec.
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name, a codec error, or a store error.
    async fn get_cell(&self, name: &str) -> Result<Option<Self::Payload>, BoxEventContextError>;

    /// Buffers a write of `value` to the named value collection's cell.
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name, a codec error, or a store error.
    async fn set_cell(&self, name: &str, value: Self::Payload) -> Result<(), BoxEventContextError>;

    /// Buffers a clear of the named value collection's cell.
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name, or a store error.
    async fn clear_cell(&self, name: &str) -> Result<(), BoxEventContextError>;

    /// Records `message` as the named Kafka-message collection's durable
    /// reference (topic/partition/offset).
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name, or a store error.
    async fn record_message(
        &self,
        name: &str,
        message: &ConsumerMessage<Self::Payload>,
    ) -> Result<(), BoxEventContextError>;

    /// Resolves the named Kafka-message collection's reference back to the
    /// full message, loading through the consumer's message loader.
    ///
    /// Returns the same [`ConsumerMessage`] the typed
    /// `MessageDescriptor::get` yields — topic, partition, offset, key,
    /// timestamp, and payload — so a binding wraps it with the exact host
    /// `Message` type it already builds in `on_message`.
    ///
    /// # Errors
    ///
    /// Returns a Permanent error for an unregistered or identity-mismatched
    /// name, a loader miss, or a store error.
    async fn get_message(
        &self,
        name: &str,
    ) -> Result<Option<ConsumerMessage<Self::Payload>>, BoxEventContextError>;
}

dyn_clone::clone_trait_object!(<P> DynEventContext<Payload = P>);

#[async_trait]
impl<C> DynEventContext for C
where
    C: EventContext + Send + Sync + 'static,
    C::Error: Error + Send + Sync + 'static,
    // The keyed-state value ops recover the codec from the payload, so the
    // erased seam exists only for payloads that map to one. Every FFI payload
    // does; this is also why `EventContext::boxed` carries the same bound.
    C::Payload: ErasedStateCodec,
{
    type Payload = C::Payload;

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

    async fn get_cell(&self, name: &str) -> Result<Option<Self::Payload>, BoxEventContextError> {
        let registered =
            Registered::new(value_state::<<C::Payload as ErasedStateCodec>::Codec>(name));
        self.state(registered)
            .map_err(|e| Box::new(e) as BoxEventContextError)?
            .get()
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn set_cell(&self, name: &str, value: Self::Payload) -> Result<(), BoxEventContextError> {
        let registered =
            Registered::new(value_state::<<C::Payload as ErasedStateCodec>::Codec>(name));
        self.state(registered)
            .map_err(|e| Box::new(e) as BoxEventContextError)?
            .set(value)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn clear_cell(&self, name: &str) -> Result<(), BoxEventContextError> {
        let registered =
            Registered::new(value_state::<<C::Payload as ErasedStateCodec>::Codec>(name));
        self.state(registered)
            .map_err(|e| Box::new(e) as BoxEventContextError)?
            .clear()
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn record_message(
        &self,
        name: &str,
        message: &ConsumerMessage<Self::Payload>,
    ) -> Result<(), BoxEventContextError> {
        self.state(Registered::new(message_state::<
            <C::State as CellSession>::Loader,
        >(name)))
            .map_err(|e| Box::new(e) as BoxEventContextError)?
            .set(message)
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }

    async fn get_message(
        &self,
        name: &str,
    ) -> Result<Option<ConsumerMessage<Self::Payload>>, BoxEventContextError> {
        self.state(Registered::new(message_state::<
            <C::State as CellSession>::Loader,
        >(name)))
            .map_err(|e| Box::new(e) as BoxEventContextError)?
            .get()
            .await
            .map_err(|e| Box::new(e) as BoxEventContextError)
    }
}

/// The keyed-state capability error, raised by the [`EventContext`] state
/// surface and by descriptor binds. Defined in [`crate::state`] (its
/// `IdentityMismatch` embeds state's `StructuralIdentity`) and re-exported here
/// so the capability's error keeps its `EventContext`-local path.
pub use crate::state::StateAccessError;

#[cfg(test)]
mod tests;
