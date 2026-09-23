//! Per-event keyed-state sessions.
//!
//! A session owns an overlay over the partition's committed cell store. Clones
//! share the dirty workspace and event identity. Collection handles use scoped
//! operations through the session gate. The gate serializes operations and
//! fences stale attempt handles.
//!
//! The framework closes the gate after the handler returns. Finalize stages
//! each `ReadCommitted` collection and returns a linear receipt.
//! `ReadUncommitted` collections write resolved values during finalize. The
//! boundary promotes the receipt before it records dedup evidence and commits
//! the source.
//!
//! `EventSession` bundles the writable session with sealed lifecycle and marker
//! interfaces. Downstream code cannot implement those interfaces. Retry resets
//! the overlay and advances the attempt epoch. A deferred reload retains its
//! last message identity across that reset.

use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::consumer::partition::ShutdownPhase;
use crate::state::access::StateAccessError;
use crate::state::backend::AdmissionChecks;
use crate::state::collection::WritableStateSession;
use crate::state::dirty::DirtyStore;
use crate::state::identity::CollectionRef;
use crate::state::marker::AttemptId;
use crate::state::overlay::Overlay;
use crate::state::registry::CollectionDefRegistry;
use crate::state::retry::{StepOutcome, retry_step};
use crate::state::store::CellStore;
use crate::state::{EventRef, STATE_FANOUT_CONCURRENCY, StateBackend, StateKey};
use crate::timers::duration::CompactDuration;
use parking_lot::{Mutex as SyncMutex, RwLock};
pub(in crate::state) use sealed::MutatePermit;
pub(crate) use sealed::{Finalized, MessageMarker, OpPermit, Promoted, SessionGate};
use sealed::{MarkerIdentity, StateLifecycle};
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

mod access;
mod keyed;
mod lifecycle;
mod stage;
#[cfg(test)]
mod tests;

pub(crate) use access::{LifecycleAccess, MarkerAccessExt};
pub use keyed::KeyedStateSession;
use stage::resolve_collections;

/// The per-event session bound: a writable collection session that also
/// carries the settle boundary's sealed lifecycle and message-marker identity.
///
/// Method-free by design — it names no cell, permit, or command. The framework
/// threads it wherever a signature needs a whole per-event session, and
/// [`EventContext::State`](crate::consumer::event_context::EventContext::State)
/// is the one downstream crates name. Sealed by its
/// supertraits: a downstream crate can bound on it but can implement none of
/// the three, so it can never supply one.
pub trait EventSession: StateLifecycle + MarkerIdentity + WritableStateSession {}

impl<S: StateLifecycle + MarkerIdentity + WritableStateSession> EventSession for S {}

/// The framework-only halves of a per-event session: the settlement lifecycle
/// and the message-marker identity.
///
/// The module is `pub(crate)`, so downstream crates can name [`EventSession`]
/// in bounds but can neither implement its supertraits nor reach the sealed
/// surfaces: staging, promoting, and discarding are framework-only moves.
pub(crate) mod sealed;

/// Clones of the partition's termination signals, captured when a session is
/// minted so descriptor handles can guard operations without holding a context.
#[derive(Clone, Debug)]
pub struct TerminationWatch {
    shutdown: watch::Receiver<ShutdownPhase>,
    cancel: watch::Receiver<bool>,
}

impl TerminationWatch {
    /// Captures the partition shutdown phase and per-event cancellation
    /// receivers.
    #[must_use]
    pub fn new(shutdown: watch::Receiver<ShutdownPhase>, cancel: watch::Receiver<bool>) -> Self {
        Self { shutdown, cancel }
    }

    /// `true` once the partition is `Cancelling` (or later) or the event has
    /// been cancelled.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        *self.shutdown.borrow() >= ShutdownPhase::Cancelling || *self.cancel.borrow()
    }
}

/// Construction parameters for [`KeyedStateSession`], bundled so the
/// constructor stays readable.
pub struct SessionParts<B, L>
where
    B: StateBackend,
{
    /// The partition's uniform committed cell store (the session wraps it in a
    /// per-event `Overlay`).
    pub cell: B::Cell,

    /// Per-partition shared dirty workspace; this event's `key` sub-range is
    /// cleared at each attempt/settle boundary.
    pub dirty: Arc<DirtyStore>,

    /// The dedup store for this partition.
    pub dedup: B::Dedup,

    /// Opaque per-session capability slot a [`CellResolver`] reads at resolve
    /// time.
    ///
    /// [`CellResolver`]: crate::state::descriptor::CellResolver
    pub loader: L,

    /// Registered collection definitions.
    pub(crate) registry: Arc<CollectionDefRegistry>,

    /// Segment-qualified key this session's collections live under.
    pub state_key: StateKey,

    /// The event whose stages this session owns.
    pub event: EventRef,

    /// The minimum retention for commit evidence.
    pub(crate) dedup_ttl: CompactDuration,

    /// The partition's admission checks.
    pub(crate) checks: B::Checks,

    /// Termination signals captured at mint.
    pub termination: TerminationWatch,
}

/// The per-event attempt epoch. Bumped once per retry attempt boundary
/// ([`StateLifecycle::reset`]); a
/// handle/stream/session clone pins the epoch that was live when it was minted,
/// and every cell op fails once its pin no longer equals the session's live
/// epoch (`ensure_live`, mutator admission, `rollback`). This is what turns a
/// handle leaked past its handler attempt into an enforced `Terminated` error
/// rather than a silent write into the next attempt's transaction.
///
/// Wrapping is deliberate: 2^64 attempt boundaries is unreachable in any
/// process lifetime, and wrapping keeps the retry-forever design panic-free (a
/// checked add would be a reachable panic).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AttemptEpoch(u64);

impl AttemptEpoch {
    /// The epoch a freshly-minted session pins (attempt 1).
    const INITIAL: Self = Self(0);

    /// The next epoch — see the wrapping rationale on the type.
    fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

struct SessionInner<B, L>
where
    B: StateBackend,
{
    overlay: Overlay<B::Cell>,
    dedup: B::Dedup,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
    stage_attempt: OnceLock<AttemptId>,
    dedup_ttl: CompactDuration,
    checks: B::Checks,
    termination: TerminationWatch,
    /// The deferred-reload identity override: the dedup id of the message
    /// the current dispatch loaded, on a timer session. Last-wins and never
    /// cleared — see [`sealed::MarkerIdentity::set_reload_marker`]. Carries
    /// identity only; the commit decision lives in the settle boundary's
    /// typed classification, never in this cell's occupancy.
    reload_marker: SyncMutex<Option<MessageMarker>>,
    /// Session-owned termination flag, flipped synchronously by
    /// [`StateLifecycle::terminate`]. It is
    /// the teardown half of the session's `is_terminated`: the
    /// [`EventStateScope`](crate::state::manager::EventStateScope)'s `Drop`
    /// runs on every dispatch exit — including a future dropped mid-flight
    /// (task cancellation), where no other teardown runs — so a handle leaked
    /// past its event finds `is_terminated() == true`. A monotonic
    /// `false → true` flag set through `&self`; `Relaxed` suffices because it
    /// publishes no other state, so a reader that observes `true` relies on no
    /// happens-before edge — it simply errors its op. On the paths that also
    /// bump the attempt epoch or close the gate this flag is a redundant
    /// backstop; the one path where it is the sole fence is task-cancellation
    /// teardown, whose already-benign forward-leak boundary is owned by
    /// [`EventStateScope`](crate::state::manager::EventStateScope)'s
    /// `# Residual`.
    terminated: AtomicBool,
    /// The per-event session operation gate (KV4) — see [`SessionGate`].
    gate: SessionGate,
    /// The shared live attempt epoch — the truth every clone of this session
    /// compares its pin against (see [`AttemptEpoch`]). Behind an `RwLock` so
    /// the store-visibility window a bare atomic exposes cannot let a racing
    /// emission read a pre-bump value; the guard is a leaf, dropped inside the
    /// one-line accessors and never held across an `.await`.
    epoch: RwLock<AttemptEpoch>,
}
