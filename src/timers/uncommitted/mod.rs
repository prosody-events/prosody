//! Timer guards control each attempt and its recovery source.
//!
//! [`PendingTimer`] holds a delivered trigger. Its fire starts a live attempt
//! or returns a committed source for a recovery sweep.
//! [`FiringTimer`] can commit or abort. Its receipt returns [`ReceiptedTimer`],
//! which can retire or keep the source but cannot abort the committed event.

use crate::consumer::partition::ShutdownPhase;
use crate::consumer::receipted_sealed as sealed;
use crate::consumer::{Keyed, Receipted, ReceiptedSource, Uncommitted};
use crate::otel::SpanRelation;
use crate::related_span;
use crate::state::manager::KeySwept;
use crate::timers::TimerType;
use crate::timers::Trigger;
use crate::timers::TriggerTrace;
use crate::timers::datetime::CompactDateTime;
use crate::timers::manager::{Fire, TimerManager};
use crate::timers::store::TriggerStore;
use crate::{Key, ProcessScope};
use arc_swap::ArcSwap;
use educe::Educe;
use std::fmt::Display;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio::time::sleep;
use tracing::{Level, Span, error, warn};

/// Delay between retry attempts when commits fail.
const RETRY_DURATION: Duration = Duration::from_secs(1);

/// Shared released trace (a dispatched no-op span) used to release processing
/// resources without allocating a fresh `Arc` on every
/// [`TriggerProcessGuard`] drop.
static RELEASED_TRACE: LazyLock<Arc<TriggerTrace>> =
    LazyLock::new(|| Arc::new(TriggerTrace::Dispatched(Span::none())));

/// A trait for uncommitted timer operations.
///
/// Provides access to timer metadata and transaction operations,
/// hiding the concrete store implementation from clients.
///
/// Implemented by [`FiringTimer`] which represents a timer actively being
/// processed by a handler.
pub trait UncommittedTimer: Uncommitted + Keyed<Key = Key> + Send {
    /// The commit guard type for this timer.
    type CommitGuard: Receipted + Send;

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
    /// Useful for advanced scenarios that need the `Trigger` and the
    /// commit/abort capability as two independent values (e.g. moving the
    /// `Trigger` into a staged provisional-cell record while retaining the
    /// guard).
    fn into_inner(self) -> (Trigger, Self::CommitGuard);
}

/// Timer delivered from the queue but not yet transitioned to firing state.
///
/// Wraps a [`Trigger`] and an internal transaction state. Call
/// The framework transitions this timer to [`FiringTimer`] before processing.
/// If the timer was cancelled while queued, `fire()` returns `None`.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct PendingTimer<T>
where
    T: TriggerStore,
{
    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// Timer currently being processed by a handler.
///
/// Owns the `commit()` and `abort()` capability for a live timer.
#[derive(Educe)]
#[educe(Debug(bound = ""))]
pub struct FiringTimer<T>
where
    T: TriggerStore,
{
    /// Transaction state and coordination with [`TimerManager`].
    #[educe(Debug(ignore))]
    uncommitted: UncommittedTrigger<T>,
}

/// A live fire whose coordinate holds an earlier attempt.
///
/// `swept` needs the proof that only the key sweep mints, so no handler can
/// run before the sweep.
pub(crate) struct UnsweptTimer<T: TriggerStore>(FiringTimer<T>);

impl<T: TriggerStore> UnsweptTimer<T> {
    pub(crate) fn key(&self) -> &Key {
        self.0.key()
    }

    pub(crate) async fn abort(self) {
        self.0.abort().await;
    }

    pub(crate) fn swept(self, _proof: KeySwept) -> FiringTimer<T> {
        self.0
    }
}

/// Classifies a fired timer before handler dispatch.
pub(crate) enum Fired<T>
where
    T: TriggerStore,
{
    /// The key row identifies a live attempt.
    Live(FiringTimer<T>),
    /// The key row tag differs from the item tag. An earlier attempt can
    /// hold provisional cells, so the key sweep runs first.
    Unswept(UnsweptTimer<T>),
    /// The key row is absent, so the attempt already committed.
    Committed(ReceiptedTimer<T>),
}

impl<T> Fired<T>
where
    T: TriggerStore,
{
    #[cfg(test)]
    pub(crate) fn into_live(self) -> Option<FiringTimer<T>> {
        match self {
            Self::Live(timer) => Some(timer),
            Self::Unswept(..) | Self::Committed(..) => None,
        }
    }
}

/// A timer that can commit or abort its attempt.
pub struct UncommittedTriggerGuard<T: TriggerStore> {
    inner: UncommittedTrigger<T>,
}

/// A committed timer whose slab row permits another recovery sweep.
pub struct ReceiptedTimer<T: TriggerStore> {
    inner: UncommittedTrigger<T>,
}

/// Internal transaction state for an uncommitted timer.
///
/// Tracks whether the timer has been completed and delegates commit/abort
/// operations to the [`TimerManager`].
struct UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// The attempt identity.
    trigger: Trigger,

    /// Manager coordinating persistent and in-memory state.
    manager: TimerManager<T>,

    /// Suppresses the drop warning after an explicit final action.
    completed: bool,

    /// Global timer semaphore permit; released when this trigger is dropped.
    _permit: OwnedSemaphorePermit,
}

/// RAII guard that releases timer processing resources (spans) on drop.
///
/// Ensures deterministic cleanup when timer processing completes, rather than
/// waiting for unpredictable garbage collection timing.
pub struct TriggerProcessGuard(Arc<ArcSwap<TriggerTrace>>);

impl<T> PendingTimer<T>
where
    T: TriggerStore,
{
    /// Create a new pending timer from a delivered trigger.
    ///
    /// Holds `permit`, which bounds global timer concurrency, until this
    /// timer is dropped.
    #[must_use]
    pub fn new(trigger: Trigger, manager: TimerManager<T>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            uncommitted: UncommittedTrigger {
                trigger,
                manager,
                completed: false,
                _permit: permit,
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
    /// A live `FiringTimer` carries the tag from the store key row.
    /// A committed result has no key row and cannot reach a handler.
    pub(crate) async fn fire(self, shutdown: &watch::Receiver<ShutdownPhase>) -> Option<Fired<T>> {
        let mut uncommitted = self.uncommitted;
        let fire = loop {
            match uncommitted.manager.fire(&uncommitted.trigger).await {
                Ok(Some(fire)) => break fire,
                Ok(None) => {
                    uncommitted.completed = true;
                    return None;
                }
                Err(error) => {
                    if *shutdown.borrow() >= ShutdownPhase::Cancelling {
                        uncommitted.abort().await;
                        return None;
                    }
                    error!(error = %error, "failed to read timer receipt; retrying");
                    sleep(RETRY_DURATION).await;
                }
            }
        };
        Some(match fire {
            Fire::Live(tag) => {
                uncommitted.trigger.tag = tag;
                Fired::Live(FiringTimer { uncommitted })
            }
            Fire::Unswept(tag) => {
                uncommitted.trigger.tag = tag;
                Fired::Unswept(UnsweptTimer(FiringTimer { uncommitted }))
            }
            Fire::Committed => Fired::Committed(ReceiptedTimer { inner: uncommitted }),
        })
    }
}

impl<T> FiringTimer<T>
where
    T: TriggerStore,
{
    /// Returns a reference to the underlying [`Trigger`].
    ///
    /// The trigger tag comes from the store key row.
    #[must_use]
    pub fn trigger(&self) -> &Trigger {
        &self.uncommitted.trigger
    }

    /// Replaces the trigger's trace with a `"trigger"` dispatch span.
    ///
    /// Creates the dispatch span from the trigger's scheduling context,
    /// connecting it per `relation`. Called at fire time so that
    /// `trigger.span()` returns the dispatch span when the handler reads it.
    ///
    /// This is the single point where the configured timer span relation is
    /// applied for store-fired timers (deferred-retry reloads mint their own
    /// `timer_defer.load` dispatch span under the same relation), and it
    /// binds the dispatch span directly to the scheduling context — whether
    /// that context was captured in-process at scheduling time or restored
    /// from persistent storage — so memory- and Cassandra-backed timers
    /// produce identical dispatch-span topology.
    ///
    /// The span's level follows the fired timer's type
    /// ([`TimerType::is_application`]); two macro invocations because a
    /// tracing callsite's level is static.
    pub fn set_dispatch_span(&self, relation: SpanRelation) {
        let trigger = &self.uncommitted.trigger;
        let context = trigger.context();
        let span = if trigger.timer_type.is_application() {
            related_span!(
                level: Level::INFO,
                relation,
                context,
                "trigger",
                key = %trigger.key,
                timer.fire_time = %trigger.time.to_rfc3339(),
                timer.type = ?trigger.timer_type,
            )
        } else {
            related_span!(
                level: Level::DEBUG,
                relation,
                context,
                "trigger",
                key = %trigger.key,
                timer.fire_time = %trigger.time.to_rfc3339(),
                timer.type = ?trigger.timer_type,
            )
        };
        trigger.set_span(span);
    }
}

impl<T> Uncommitted for FiringTimer<T>
where
    T: TriggerStore,
{
    /// Commit this timer after successful processing.
    ///
    /// Retries each failed store write.
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

impl<T> Receipted for FiringTimer<T>
where
    T: TriggerStore,
{
    type Source = ReceiptedTimer<T>;

    async fn receipt(self) -> Self::Source {
        self.uncommitted.receipt().await
    }
}

impl<T> sealed::Sealed for FiringTimer<T> where T: TriggerStore {}

impl<T> Keyed for FiringTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.uncommitted.trigger.key
    }
}

impl<T> Keyed for PendingTimer<T>
where
    T: TriggerStore,
{
    type Key = Key;

    /// Returns the key associated with this timer.
    fn key(&self) -> &Self::Key {
        &self.uncommitted.trigger.key
    }
}

impl<T> Uncommitted for UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    /// Commit this timer after successful processing.
    async fn commit(mut self) {
        self.inner.commit().await;
    }

    /// Abort this timer without deleting persistent data.
    async fn abort(mut self) {
        self.inner.abort().await;
    }
}

impl<T> Receipted for UncommittedTriggerGuard<T>
where
    T: TriggerStore,
{
    type Source = ReceiptedTimer<T>;

    async fn receipt(self) -> Self::Source {
        self.inner.receipt().await
    }
}

impl<T> sealed::Sealed for UncommittedTriggerGuard<T> where T: TriggerStore {}

impl<T> UncommittedTrigger<T>
where
    T: TriggerStore,
{
    async fn receipt(self) -> ReceiptedTimer<T> {
        let manager = &self.manager;
        let trigger = &self.trigger;
        retry_until_ok("record timer receipt", || manager.receipt(trigger)).await;
        ReceiptedTimer { inner: self }
    }

    async fn commit(&mut self) {
        let manager = &self.manager;
        let trigger = &self.trigger;
        retry_until_ok("commit timer", || manager.complete(trigger)).await;
        self.completed = true;
    }

    async fn abort(&mut self) {
        self.manager.abort(&self.trigger).await;
        self.completed = true;
    }
}

impl<T: TriggerStore> sealed::Sealed for ReceiptedTimer<T> {}

impl<T: TriggerStore> Keyed for ReceiptedTimer<T> {
    type Key = Key;

    fn key(&self) -> &Key {
        &self.inner.trigger.key
    }
}

impl<T: TriggerStore> ReceiptedSource for ReceiptedTimer<T> {
    async fn retire(mut self) {
        let manager = &self.inner.manager;
        let trigger = &self.inner.trigger;
        retry_until_ok("retire timer", || manager.retire(trigger)).await;
        self.inner.completed = true;
    }

    async fn keep(mut self) {
        self.inner.abort().await;
    }
}

impl<T> UncommittedTimer for FiringTimer<T>
where
    T: TriggerStore,
{
    type CommitGuard = UncommittedTriggerGuard<T>;

    /// Scheduled execution time of this timer.
    fn time(&self) -> CompactDateTime {
        self.uncommitted.trigger.time
    }

    /// Timer type classification.
    fn timer_type(&self) -> TimerType {
        self.uncommitted.trigger.timer_type
    }

    /// Returns the tracing span associated with this timer.
    ///
    /// Returns `Span::none()` if processing resources have been released.
    fn span(&self) -> Span {
        self.uncommitted.trigger.span()
    }

    /// Decompose into the raw [`Trigger`] and the commit guard.
    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        let trigger = self.uncommitted.trigger.clone();
        (
            trigger,
            UncommittedTriggerGuard {
                inner: self.uncommitted,
            },
        )
    }
}

impl<T> Drop for UncommittedTrigger<T>
where
    T: TriggerStore,
{
    /// Warn if no explicit final action releases the timer.
    fn drop(&mut self) {
        if !self.completed {
            warn!("timer was dropped without committing or aborting");
        }
    }
}

impl Drop for TriggerProcessGuard {
    fn drop(&mut self) {
        self.0.store(Arc::clone(&RELEASED_TRACE));
    }
}

impl<T> ProcessScope for FiringTimer<T>
where
    T: TriggerStore,
{
    type Guard = TriggerProcessGuard;

    fn process_scope(&self) -> Self::Guard {
        TriggerProcessGuard(self.uncommitted.trigger.trace.clone())
    }
}

/// Retry the store operation until it succeeds.
async fn retry_until_ok<F, Fut, E>(action: &str, mut op: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Display,
{
    loop {
        match op().await {
            Ok(()) => return,
            Err(error) => {
                error!("failed to {action}: {error:#}; retrying");
                sleep(RETRY_DURATION).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
