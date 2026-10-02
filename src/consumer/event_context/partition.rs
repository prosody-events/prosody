//! Event context state owned by the assigned partition.

use super::{
    Arc, ArcSwapOption, AsyncFnOnce, CompactDateTime, Educe, Empty, EventContext, EventSession,
    Future, FutureExt, Instrument, Key, MessageLoader, Registered, RepinProof, ShutdownPhase, Span,
    StateAccessError, StateDescriptor, TerminationSignals, TimerManager, TimerManagerError,
    TimerRequest, TimerType, TriggerStore, display, error, ready, select, timer_span, watch,
};

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
/// * `S`: The per-event [`EventSession`] session; its payload fixes
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

    /// Framework-owned end-of-event teardown of **this** context cell.
    ///
    /// Latches cancellation, then stores `None` into this cell's inner slot:
    /// every clone that shares this inner cell flips stateless (bind refuses
    /// with `Terminated`, timer ops with `InvalidContext`), and the cell's
    /// strong ref to `Inner` drops so its resources (tracing spans, watch
    /// channels, session handle) free once the last clone drops.
    ///
    /// It is **not** event-wide. [`redispatch`](EventContext::redispatch) mints
    /// a fresh inner cell per attempt, so this only tears down the partition's
    /// original cell; attempt-N cells (and the contexts a final apply hook
    /// mints) stay live until their own last clone drops. The cancellation
    /// *signal* is shared event-wide but resettable via
    /// [`uncancel`](EventContext::uncancel), so it is not an enduring fence.
    /// Cross-attempt / post-event fencing for keyed-state ops is owned by the
    /// session (epoch pin, gate, termination), not by this method.
    ///
    /// Crate-internal: the partition loop is the sole caller, once per event
    /// after dispatch returns. Keeping it off the public [`EventContext`] trait
    /// makes the mid-dispatch lost-write misuse (a handler invalidating its own
    /// context, then returning `Ok` so settle commits the offset without
    /// draining the dirty overlay) uncompilable for user code.
    pub(in crate::consumer) fn invalidate(self)
    where
        S: EventSession<Loader: MessageLoader>,
    {
        self.cancel();
        self.inner.store(None);
    }
}

impl<T, S> EventContext for PartitionEventContext<T, S>
where
    T: TriggerStore,
    S: EventSession<Loader: MessageLoader>,
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
