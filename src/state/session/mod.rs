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

use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::state::backend::AdmissionChecks;
use crate::state::cell::{Committed, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::collection::{StateSession, WritableStateSession};
use crate::state::descriptor::{
    DescriptorIdentity, Registered, SealedDescriptor, StateDescriptor, StructuralIdentity,
};
use crate::state::dirty::{CellSnapshot, ClearedSections, DirtyStore, DirtyVal, ResolvedCells};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::marker::{AttemptId, EventEvidence, EventMarker, SectionClear, evidence_ttl};
use crate::state::overlay::Overlay;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::{CELL_BATCH, CellBuffer, CellStore, CoordinateBatch};
use crate::state::{
    CollectionKindId, CommitMode, EventRef, SHARD_FANOUT_CONCURRENCY, STATE_FANOUT_CONCURRENCY,
    StateBackend, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use parking_lot::{Mutex as SyncMutex, RwLock};
pub(in crate::state) use sealed::MutatePermit;
pub(crate) use sealed::{Finalized, MessageMarker, OpPermit, SessionGate};
use sealed::{MarkerIdentity, Staged, StagedCollection, StateLifecycle};
use std::fmt;
use std::future::Future;
use std::iter::from_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// The per-event session bound: a writable collection session that also
/// carries the settle boundary's sealed lifecycle and message-marker identity.
///
/// Method-free by design — it names no cell, permit, or command. The framework
/// threads it wherever a signature needs a whole per-event session, and
/// [`EventContext::State`] is the one downstream crates name. Sealed by its
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
pub(crate) mod sealed {
    use super::{
        AdmissionChecks, CellKey, CellStore, CollectionRef, Duration, EventMarker, Future,
        MarkerWrite, ProvisionalWrite, RepinProof, StateAccessError, Uuid, resolve_collections,
    };
    use opentelemetry::global::meter;
    use opentelemetry::metrics::Counter;
    use std::ops::{Deref, DerefMut};
    use std::sync::LazyLock;
    use tokio::sync::{Mutex as TokioMutex, MutexGuard};
    use tokio::time::timeout;

    /// How long a settle-boundary gate acquire waits between rate-limited
    /// warnings while a still-running session op holds the gate.
    const GATE_WARN_INTERVAL: Duration = Duration::from_secs(10);

    /// Settle-boundary gate waits that crossed a warn interval, bumped once per
    /// tick (see [`SessionGate::close`]).
    static SETTLE_GATE_WAITS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        meter("prosody")
            .u64_counter("prosody.state.settle_gate_waits")
            .with_description("Settle-boundary session-gate waits past a warn interval")
            .with_unit("{wait}")
            .build()
    });

    /// The per-event **session operation gate** — the in-handler leg of KV4 (a
    /// read-back fill can never overwrite a newer write-through; the invariant
    /// lives on [`Cached`](crate::state::cached::Cached)'s module doc).
    ///
    /// One gate per event session, acquired by every collection-handle
    /// operation for its **whole body**: a `get` holds it from the overlay
    /// check through the fill's publish; `commit()` across snapshot →
    /// durable write → drain; `set`/`remove`/`clear` across their
    /// entry-and-meta updates. `join!`-ed ops therefore execute in *some*
    /// serial order — which also closes two lost-update races the dirty
    /// store's old "no handler op is in flight" comment papered
    /// over: `commit()`'s snapshot→drain window dropping a concurrent `set`,
    /// and the map keyset read-modify-write under `join!`-ed sets.
    ///
    /// **A stream acquires the gate at init and once per chunk**, each permit
    /// dropped before the next; every other public op acquires it once for its
    /// whole body. Nothing beneath a public wrapper re-acquires while holding —
    /// a tokio `Mutex` is not reentrant, so an internal re-acquire is a
    /// deadlock the KV4 pins would surface as a hang.
    ///
    /// **Streams hold the gate only per chunk (`StreamYieldFree`).** A
    /// point-get stream covers a sub-threshold deque window, or a `Tracked` map
    /// keyset within its bound. It takes the gate for its init metadata read
    /// (the map keyset cell, or the deque window cell) and releases it. It then
    /// fetches the listed entries in gate-scoped chunks: one permit per chunk,
    /// and ONE batch read each. Only the init metadata read stays a point read.
    /// One permit covers a chunk's fetch, decode, and resolve. The chunk
    /// future's scope drops that permit before any of the chunk's items reach
    /// user code. The permit is therefore **never held across a yield to user
    /// code, for items and errors alike**.
    ///
    /// A *scan-path* stream takes the gate only for its init metadata read, and
    /// is per-item live thereafter. Its per-item resolution is a pure **read**:
    /// a scan never writes a resolution back durably, because admission owns
    /// residue resolution. A concurrent mid-stream `commit()` on a scanned
    /// cell is therefore never clobbered.
    ///
    /// A mutator that races a live stream (`join!`, or a handler that mutates
    /// its own collection between stream items) waits at most one chunk fetch
    /// and resolve, never a whole materialization. Settle's closure acquire
    /// queues FIFO the same way.
    ///
    /// **The gate also closes the session lifecycle**: settle acquires it once
    /// via [`close`](Self::close) and marks the session `Closed`, holding
    /// the permit across the whole durability sequence. After closure,
    /// mutators error [`StateAccessError::SessionClosed`] (checked *after*
    /// acquiring, so an op parked behind the closing settle errors instead
    /// of mutating a session the boundary already snapshotted) while reads
    /// still proceed — they serialize after settle and observe
    /// fully-settled state, preserving the post-settle apply-hook read
    /// contract.
    ///
    /// **The one forbidden pattern** (futurelock): never hold a session-op
    /// future alive but un-polled while issuing more session ops — drop it
    /// instead. *Dropping* a session-op future is always safe (a dropped
    /// waiter leaves the FIFO queue; a granted-then-dropped guard
    /// releases), and is pinned by the cancel-safety test. An
    /// alive-but-un-polled future that was granted the gate wedges every
    /// later op, including settlement — settle warns loudly past
    /// [`GATE_WARN_INTERVAL`] but **never** proceeds without the gate (settling
    /// around a still-executing op would snapshot a half-applied session).
    ///
    /// A second shape — **detaching** a session clone, handle, or scan stream
    /// into a task, an un-awaited future, or a foreign promise that outlives
    /// the handler attempt that spawned it — is an **enforced error on every
    /// op**, not a convention. Session handles are `Clone + 'static`, but the
    /// gate only serializes ops *within* one event's dispatch. Between retry
    /// attempts the gate is Open (closure happens only at settle), so a leaked
    /// clone's `set` landing after an attempt boundary would once have joined
    /// the NEXT attempt's transaction. The attempt boundary
    /// ([`StateLifecycle::reset`]) bumps the session epoch under this gate. A
    /// detached clone keeps its stale pin. The leak therefore errors at the
    /// point its op takes effect. This holds uniformly across the whole
    /// surface: `Terminated` on a crossed attempt boundary, and
    /// `SessionClosed` in the post-settle hook window. Three seams enforce it:
    ///
    /// * **handle ops** (`get`/`set`/`clear`/…) — the pin compare in
    ///   `ensure_live` / `mutate_permit`'s ordered admission;
    /// * **apply-hook mutations** past the settle window — the closed gate and
    ///   attempt teardown;
    /// * **scans and streams** — the managed stream fence adapter (`fenced` in
    ///   `crate::state::collection::stream`), which runs the engine fence after
    ///   every stream completion, so a leaked stream errors at its next
    ///   emission and no buffered item crosses the boundary.
    ///
    /// Keep every session op inside the handler future that owns the event all
    /// the same; the fence is the backstop, not a license to detach.
    ///
    /// Perf posture: uncontended for any handler that does not `join!` its
    /// session ops — one uncontended tokio `Mutex` lock per op; the gate
    /// adds no other RAM structure.
    pub struct SessionGate {
        inner: TokioMutex<SessionPhase>,
    }

    /// Whether the session still accepts mutators, guarded by the gate's mutex.
    enum SessionPhase {
        /// The handler is (or may still be) running; all ops proceed.
        Open,
        /// The settle boundary closed the session; mutators error, reads
        /// proceed.
        Closed,
    }

    impl SessionGate {
        /// A fresh, open gate for one event session.
        pub(crate) fn new() -> Self {
            Self {
                inner: TokioMutex::new(SessionPhase::Open),
            }
        }

        /// Acquires the gate for a read. No closure check: reads stay legal
        /// after settlement closes the session (the apply hooks read
        /// state through it), serializing after the settle so they
        /// observe fully-settled state.
        pub(crate) async fn read(&self) -> OpPermit<'_> {
            OpPermit(self.inner.lock().await)
        }

        /// Closes the session for settlement: acquires the gate once, marks the
        /// phase `Closed`, and returns the held permit — the settle boundary
        /// retains it across the whole durability sequence and drops it just
        /// before the apply hooks fire.
        ///
        /// Pins ONE lock future and warns against `&mut` of it per
        /// [`GATE_WARN_INTERVAL`] tick (`warn_tick` receives the seconds
        /// waited; the caller tags it with the event and key) —
        /// re-issuing `lock()` per tick would forfeit FIFO position and
        /// could starve settlement. It never proceeds without the gate,
        /// whatever the wait: a diagnosable wedge beats snapshotting a
        /// half-applied session. Idempotent: a second close (retry's
        /// `abandon` after settle's own) re-acquires and re-marks `Closed`.
        pub(crate) async fn close(&self, mut warn_tick: impl FnMut(u64)) -> OpPermit<'_> {
            let lock = self.inner.lock();
            tokio::pin!(lock);
            let mut waited = 0u64;
            let mut guard = loop {
                match timeout(GATE_WARN_INTERVAL, lock.as_mut()).await {
                    Ok(guard) => break guard,
                    Err(_elapsed) => {
                        waited += GATE_WARN_INTERVAL.as_secs();
                        SETTLE_GATE_WAITS.add(1, &[]);
                        warn_tick(waited);
                    }
                }
            };
            *guard = SessionPhase::Closed;
            OpPermit(guard)
        }
    }

    /// A held [`SessionGate`] permit (RAII: dropping it releases the gate).
    ///
    /// Witnesses admission for a session **read**: the owner engine's
    /// journal-replay sinks (`stage_cell` and `stage_section_clear`) demand
    /// `&OpPermit<'_>`, so "forgot to acquire the gate" and "let the acquire
    /// outlive the op" cannot compile. The same type is both the owner
    /// engine's read state and the hold [`SessionGate::close`] returns, and a
    /// mutator's [`MutatePermit`] derefs to it. The read-vs-mutate split
    /// encodes the gate's closure
    /// check, **not** shared-vs-exclusive access: both permits are
    /// exclusive holds (a session read is not pure — a point-get miss does
    /// durable read-repair and publishes a cache fill, which KV4's
    /// fill-vs-write-through exclusion assumes runs under full
    /// mutual exclusion). [`SessionGate`] owns the conventional half of the
    /// contract: one acquire per public op, no re-acquire beneath it, same
    /// session.
    pub struct OpPermit<'a>(MutexGuard<'a, SessionPhase>);

    impl OpPermit<'_> {
        /// Whether the settle boundary has closed the session — consulted by
        /// the session's `rollback`, whose infallible contract answers a closed
        /// session with `NoOp` instead of an error.
        pub(crate) fn is_closed(&self) -> bool {
            matches!(*self.0, SessionPhase::Closed)
        }
    }

    /// A held gate permit that additionally witnesses a session **mutation**.
    ///
    /// Minted only through [`Self::witness`], from a held read permit once the
    /// caller has sequenced the mutator admission order (pin → closed →
    /// termination — see the session's `mutate_permit`). The write engine's
    /// `validate_write` and journal `apply`
    /// (`crate::state::collection::owner`) demand `&MutatePermit<'_>`, which is
    /// the owner engine's write state. A read permit at a write does not
    /// compile. A scoped write invocation holds one
    /// across its whole body and hands its [`Deref`] target to the
    /// journal-replay sinks [`OpPermit`] names at replay.
    /// It [`Deref`]s to [`OpPermit`], so a mutator's
    /// one permit also witnesses the reads inside its body — the read-under-
    /// mutate grade subtyping is one-directional and deliberate: a read is
    /// legal under a mutate hold, but the converse (mutate under a read
    /// permit) is a type error. Uncontended unless a handler `join!`s its
    /// session ops.
    pub struct MutatePermit<'a>(OpPermit<'a>);

    impl<'a> MutatePermit<'a> {
        /// Wraps a held read permit as a mutation witness. The caller (the
        /// session's `mutate_permit`) has already sequenced the
        /// pin/closed/termination admission checks under this same permit, so
        /// possessing a `MutatePermit` proves the session admitted the
        /// mutation.
        pub(in crate::state) fn witness(permit: OpPermit<'a>) -> Self {
            Self(permit)
        }
    }

    // The guard lifetime is named explicitly on both impls: `Target` is
    // `OpPermit<'a>`, so the elided `'_` (which binds to `&self`) does not
    // match — on either impl.
    impl<'a> Deref for MutatePermit<'a> {
        type Target = OpPermit<'a>;

        fn deref(&self) -> &OpPermit<'a> {
            &self.0
        }
    }

    impl<'a> DerefMut for MutatePermit<'a> {
        fn deref_mut(&mut self) -> &mut OpPermit<'a> {
            &mut self.0
        }
    }

    /// The message dedup id that settlement records and the duplicate filter
    /// reads. Message sessions use their event id. Deferred reloads supply
    /// an override through [`MarkerIdentity::set_reload_marker`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct MessageMarker(Uuid);

    impl MessageMarker {
        /// Wraps a message's dedup id.
        #[must_use]
        pub(crate) fn new(dedup_id: Uuid) -> Self {
            Self(dedup_id)
        }

        /// The raw id for dedup writes and lookups.
        #[must_use]
        pub(crate) fn into_uuid(self) -> Uuid {
            self.0
        }
    }

    /// One collection's staged cells and frozen marker, retained for promote.
    /// Only `ReadCommitted` collections enter this receipt. A clear-only
    /// collection has no cell writes. The durable marker stores
    /// coordinates. This receipt retains values so promote needs no cell
    /// reload.
    // `Vec`, not `SmallVec`, deliberately: the receipt is held across the
    // settle boundary's awaits, so inline entries bloat every such future past
    // clippy's `large_futures` bound. The `with_capacity` folds at the build
    // sites already bound the allocation.
    pub struct StagedCollection {
        pub(super) collection: CollectionRef,
        pub(super) writes: Vec<(CellKey, ProvisionalWrite)>,
        pub(super) marker: EventMarker,
    }

    /// The provisional work that a successful stage produced.
    #[must_use]
    pub enum Finalized<S: CellStore, K: AdmissionChecks> {
        /// No provisional work remains.
        Clean,
        /// Promote the staged collections before the dedup row.
        Staged(Staged<S, K>),
    }

    /// One event's staged collections. Only finalize constructs this value.
    #[must_use]
    pub struct Staged<S: CellStore, K: AdmissionChecks> {
        pub(super) store: S,
        pub(super) collections: Vec<StagedCollection>,
        pub(super) checks: K,
        pub(super) key: crate::Key,
    }

    impl<S: CellStore, K: AdmissionChecks> Staged<S, K> {
        /// Promotes all collections. Retries store failures until shutdown or
        /// permanent rejection. A failed promote removes the admission
        /// proof. Finalize owns the other removal site. Thus a checked
        /// key has no unresolved residue from an earlier settle.
        pub(crate) async fn promote(self, shutdown: impl Fn() -> bool + Sync) -> bool {
            let complete = resolve_collections(&self.store, self.collections, &shutdown).await;
            if !complete && self.checks.unmark(&self.key).await.is_err() {
                return false;
            }
            !shutdown()
        }
    }

    /// Framework-only lifecycle over a per-event session: the settle boundary's
    /// stage/record/promote moves plus the attempt and teardown verbs.
    pub trait StateLifecycle: Clone + Send + Sync + 'static {
        /// The uniform durable cell store the session settles against —
        /// [`KeyedStateSession`](super::KeyedStateSession) projects its
        /// backend's store (`B::Cell`).
        type Cell: CellStore;

        /// The admission proof store.
        type Checks: AdmissionChecks;

        /// The session's operation gate (KV4) — the engine acquires each
        /// operation's permit through this accessor. On the sealed lifecycle
        /// trait, not on [`EventSession`](super::EventSession): the gate is
        /// framework plumbing, never a handler surface.
        fn gate(&self) -> &SessionGate;

        /// Closes the session's gate for settlement — one acquire, phase
        /// `Closed`, permit returned — tagging the wait warnings with this
        /// session's event and key. See [`SessionGate::close`].
        fn close_gate(&self) -> impl Future<Output = OpPermit<'_>> + Send;

        /// Resolves every touched collection by its commit mode:
        /// `ReadCommitted` collections stage a provisional cell,
        /// `ReadUncommitted` collections write a resolved value — returning
        /// the staged work as the linear [`Finalized`] receipt the boundary
        /// consumes. Stages all collections before returning, so a stage
        /// error returns before the textually-later marker record; a staging
        /// failure is a type-erased store error with no receipt minted.
        ///
        /// On success the event's dirty range is drained — the stage consumes
        /// the buffered ops (including a [`Finalized::Clean`] return whose
        /// only work was `ReadUncommitted` resolved writes), so a second
        /// `finalize` finds an empty buffer and returns `Clean`: one stage
        /// mints at most one receipt. Failure paths leave the buffer whole so
        /// a retried `finalize` re-stages idempotently.
        fn finalize(
            &self,
        ) -> impl Future<Output = Result<Finalized<Self::Cell, Self::Checks>, StateAccessError>> + Send;

        /// Records the message dedup id with an idempotent upsert.
        /// The boundary retries failures before it commits the source.
        fn record_marker(
            &self,
            marker: MessageMarker,
            proof: MarkerWrite,
        ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

        /// Discards just this event's buffered dirty cells — the isolation step
        /// of the attempt-boundary [`Self::reset`] transition (which then bumps
        /// the epoch under the same gate hold), and the failure-path backstop
        /// that [`EventStateScope`](crate::state::manager::EventStateScope)'s
        /// `Drop` runs on every exit path (error, abandon, panic unwind).
        ///
        /// The dirty workspace is partition-lifetime (manager-owned, shared by
        /// every session clone), so it must be cleared explicitly per event.
        /// The reload identity override is deliberately **not** cleared here
        /// (never cleared at all — see [`MarkerIdentity::set_reload_marker`]).
        /// On the success path [`Self::finalize`] has already drained
        /// the buffer (the stage consumes it), making the scope-drop
        /// clear a no-op there; the receipt's promote/rollback read
        /// only receipt-owned data, never dirty.
        fn discard_dirty(&self);

        /// Flips this session terminated, synchronously and idempotently — the
        /// teardown half of the session's `is_terminated`.
        /// The [`EventStateScope`](crate::state::manager::EventStateScope)'s
        /// `Drop` calls it on every dispatch exit (including a future dropped
        /// mid-flight, where no gated teardown runs), and the panic-unwind
        /// catch calls it under the held closed gate. After it, every op on any
        /// clone of this session errors — reads/inits `Terminated`, and a
        /// current-pin mutation past the closed gate `SessionClosed` (the gate
        /// closes first in the catch). Writes no epoch: a genuinely-leaked
        /// stale clone stays fenced by its old pin regardless.
        fn terminate(&self);

        /// The attempt-boundary transition: acquire the gate, discard this
        /// attempt's dirty overlay, and bump the epoch — all under **one** gate
        /// hold, so a stale queued write cannot land between the clear and the
        /// bump and survive into the next attempt. This is the epoch's ONLY
        /// bump site. Gated by [`RepinProof`] so a lone bump (a partial reset
        /// with no matching re-pin) is unwritable outside the two mint sites.
        fn reset(&self, proof: RepinProof) -> impl Future<Output = ()> + Send;

        /// A session clone re-pinned to the CURRENT epoch — the crate-internal
        /// re-pin constructor. [`RepinProof`]-gated so only the two mint sites
        /// (the `next_attempt` verb and the settle final-hook stamp) can
        /// produce a live attempt-N+1 (or stamped-final) view.
        fn repin(&self, proof: RepinProof) -> Self;
    }

    /// The message commit-marker identity surface — the sole home of
    /// `set_reload_marker`/`message_marker` (they are deliberately **not** on
    /// [`StateLifecycle`], so the settlement surface carries no marker
    /// vocabulary). Exactly three audiences reach it, each through the narrow
    /// [`MarkerHandle`](super::MarkerHandle) rather than a raw session: the
    /// message-defer reload path *sets* the override, and the dedup filter and
    /// the settle boundary *read* it.
    pub trait MarkerIdentity {
        /// Sets the deferred-reload identity override: the dedup id of the
        /// message the current dispatch loaded from the defer queue, set at
        /// exactly one site — the message-defer reload path, immediately
        /// after the load succeeds and before the inner dispatch.
        ///
        /// **Last-wins and never cleared.** Last-wins is load-bearing: a
        /// retry re-dispatch of the same defer timer after an ambiguous
        /// durable queue advance loads the *next* queued message — a
        /// different id — and the override must re-point at it (a set-once
        /// cell would dispatch message B under message A's identity). Never
        /// cleared is safe because every settle arm that consults
        /// [`Self::message_marker`] implies the final attempt's inner ran,
        /// which implies that attempt's reload performed the set — a stale
        /// read is unreachable, not merely forbidden.
        fn set_reload_marker(&self, marker: MessageMarker);

        /// The message commit-marker identity for this event: a message
        /// session's [`EventRef::Message`](crate::state::EventRef) dedup id;
        /// on a timer session, the reload override; else `None` (a pure
        /// timer, whose trigger commit is its dedup). A message session
        /// never reads the override — the match arm structurally ignores it,
        /// so a divergent override is unreadable rather than forbidden.
        fn message_marker(&self) -> Option<MessageMarker>;
    }
}

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

/// The real per-event session over a partition's cell store.
///
/// One session is minted per event by the partition's state manager; clones
/// share the per-event dirty overlay and reload-marker override plus the
/// cross-event singletons. `B` is the per-partition `StateBackend` bundle;
/// `L` is the message loader.
pub struct KeyedStateSession<B, L>
where
    B: StateBackend,
{
    inner: Arc<SessionInner<B, L>>,
    /// This clone's pinned attempt epoch, copied verbatim by [`Clone`] and
    /// living OUTSIDE the shared `inner` so a leaked clone (or a clone of a
    /// clone) keeps its stale pin and can never re-pin itself. Only the
    /// crate-internal [`StateLifecycle::repin`]
    /// mints a clone re-pinned to the live epoch.
    pinned: AttemptEpoch,
}

impl<B, L> Clone for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            pinned: self.pinned,
        }
    }
}

impl<B, L> fmt::Debug for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedStateSession")
            .field("state_key", &self.inner.state_key)
            .field("event", &self.inner.event)
            .finish_non_exhaustive()
    }
}

impl<B, L> KeyedStateSession<B, L>
where
    B: StateBackend,
{
    /// Creates a session for one event, wrapping the partition's cell store in
    /// a per-event `Overlay` over the shared dirty workspace.
    #[must_use]
    pub fn new(parts: SessionParts<B, L>) -> Self {
        let SessionParts {
            cell,
            dirty,
            dedup,
            loader,
            registry,
            state_key,
            event,
            dedup_ttl,
            checks,
            termination,
        } = parts;
        Self {
            inner: Arc::new(SessionInner {
                stage_attempt: OnceLock::new(),
                overlay: Overlay::new(dirty, cell),
                dedup,
                loader,
                registry,
                state_key,
                event,
                dedup_ttl,
                checks,
                termination,
                reload_marker: SyncMutex::new(None),
                terminated: AtomicBool::new(false),
                gate: SessionGate::new(),
                epoch: RwLock::new(AttemptEpoch::INITIAL),
            }),
            // A freshly-minted session is attempt 1: `pinned == *epoch.read()`.
            pinned: AttemptEpoch::INITIAL,
        }
    }

    /// The collection id for `(state_type, name)` under this session's key.
    fn id_for(&self, state_type: StateType, name: &StateName) -> CollectionId {
        CollectionId::new(self.inner.state_key.clone(), state_type, name.clone())
    }

    /// The session's opaque capability slot — see
    /// [`StateSession::Loader`](crate::state::collection::StateSession::Loader).
    pub(in crate::state) fn message_loader(&self) -> &L {
        &self.inner.loader
    }

    /// The session's live attempt epoch. A one-line copy-out: the leaf
    /// `RwLock` read guard is dropped before returning, so it is never held
    /// across an `.await`.
    fn current_epoch(&self) -> AttemptEpoch {
        *self.inner.epoch.read()
    }

    /// Bumps the live attempt epoch to the next value. The **only** epoch
    /// writer, with exactly one call site: inside
    /// [`StateLifecycle::reset`], under the
    /// held gate permit. Lock ordering is always gate → epoch (the gate is an
    /// async tokio mutex, this is a `parking_lot` leaf), so there is no
    /// sync lock-order cycle; the write guard is dropped before returning and
    /// never crosses an `.await`.
    fn bump_epoch(&self) {
        let mut epoch = self.inner.epoch.write();
        *epoch = epoch.next();
    }

    /// Acquires the session operation gate for one command — the owner
    /// engine's read state. See [`SessionGate`].
    pub(in crate::state) async fn permit(&self) -> OpPermit<'_> {
        self.inner.gate.read().await
    }

    /// Whether this handle/session clone's pinned epoch still equals the live
    /// session epoch. `false` once a later attempt boundary
    /// ([`StateLifecycle::reset`]) bumped it — the pin half of the owner
    /// engine's live guard, of the mutator admission order, and of
    /// [`Self::rollback`]'s `NoOp` guard.
    pub(in crate::state) fn attempt_current(&self) -> bool {
        self.pinned == self.current_epoch()
    }

    /// The registry's operational settings for the name, defaults included. A
    /// collection binding captures it once, so every configuration query
    /// inside a scoped operation answers from one snapshot.
    pub(in crate::state) fn collection_def(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> CollectionDef {
        self.inner.registry.def_for(state_type, name)
    }

    /// Returns `true` once the partition is shutting down or the event has been
    /// cancelled. The owner engine guards every command on this.
    pub(in crate::state) fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated() || self.inner.terminated.load(Ordering::Relaxed)
    }

    /// Validates that the keyed-state collection named `(state_type, name)` is
    /// registered with the asserted structural identity, returning the
    /// canonical [`StateName`].
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unregistered`] for an unknown name, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the asserted one.
    pub(in crate::state) fn verify_state_registration(
        &self,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        let Some((state_name, registered)) = self.inner.registry.lookup(state_type, name) else {
            return Err(StateAccessError::Unregistered { name });
        };
        if registered.identity != *identity {
            return Err(StateAccessError::IdentityMismatch {
                stored: registered.identity.clone(),
                asserted: identity.clone(),
            });
        }
        Ok(state_name.clone())
    }

    /// Reads a cell's currently visible committed value within this event's
    /// transaction (cleared/absent → `None`) — the dirty overlay resolved
    /// through collection evidence.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the underlying store fails.
    pub(in crate::state) async fn get(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get(&id, cell)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(committed.into_inner())
    }

    /// Batch twin of [`Self::get`]: reads one `section`'s coordinates in one
    /// backend hop, aligned index-wise (`result[i]` answers `batch[i]`;
    /// duplicate coordinates co-observe; absent → `None`). The section is
    /// explicit alongside the batch — the point read carries it inside the
    /// [`CellKey`].
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the underlying store fails.
    pub(in crate::state) async fn get_many(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get_many(&id, section, batch)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(committed.into_iter().map(Committed::into_inner).collect())
    }

    /// The single-section, start-anchored, bidirectional range primitive: a
    /// lazy stream of the visible committed cells in `coordinate` byte order.
    pub(in crate::state) fn scan<'a>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        let id = self.id_for(state_type, name);
        // `id` is local to the generator, so `scan_cells` unifies its lifetime
        // with an owned overlay; the caller's `Copy` `Scan<'a>` rides in
        // directly (it is covariant, so it coerces to that shorter scope).
        let overlay = self.inner.overlay.clone();
        try_stream! {
            let inner = overlay.scan_cells(&id, scan);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateAccessError::store(&e))?;
            }
        }
    }

    /// Acquires the operation gate for a mutator, then applies the one total
    /// admission order under the held permit before returning the witness:
    ///
    /// 1. **pin** — a stale attempt (this handle outlived its dispatch and the
    ///    epoch was bumped) errors [`StateAccessError::Terminated`].
    /// 2. **closed** — the settle boundary already closed the session, so an
    ///    own-event mutation past the settle window errors
    ///    [`StateAccessError::SessionClosed`].
    /// 3. **termination** — shutdown or cancellation errors
    ///    [`StateAccessError::Terminated`].
    ///
    /// The pin check runs first. So a mutation under a still-current pin
    /// classifies `SessionClosed` rather than `Terminated`, even under
    /// shutdown or cancellation.
    ///
    /// The permit is held across all three checks. The epoch bump needs the
    /// gate exclusively, so the pin stays stable between the check and the
    /// return.
    ///
    /// # Errors
    ///
    /// [`StateAccessError::Terminated`] on a stale attempt or a
    /// shutting-down/cancelled session, or [`StateAccessError::SessionClosed`]
    /// once the settle boundary has closed the session.
    pub(in crate::state) async fn mutate_permit(
        &self,
    ) -> Result<MutatePermit<'_>, StateAccessError> {
        let permit = self.permit().await;
        self.check_write_admission(&permit)?;
        Ok(MutatePermit::witness(permit))
    }

    /// The one total write-admission order, applied under a held `permit` — the
    /// single definition [`Self::mutate_permit`] runs to mint its witness and
    /// the owner engine re-runs before each staged mutation. See
    /// [`Self::mutate_permit`] for the order and why it is that order.
    ///
    /// # Errors
    ///
    /// As [`Self::mutate_permit`].
    pub(in crate::state) fn check_write_admission(
        &self,
        permit: &OpPermit<'_>,
    ) -> Result<(), StateAccessError> {
        if !self.attempt_current() {
            return Err(StateAccessError::Terminated);
        }
        if permit.is_closed() {
            return Err(StateAccessError::SessionClosed);
        }
        if self.is_terminated() {
            return Err(StateAccessError::Terminated);
        }
        Ok(())
    }

    /// Stages one already-encoded mutation into this event's dirty overlay —
    /// the synchronous, infallible sink a scoped collection invocation replays
    /// its journal through. `Some(bytes)` stages a write, `None` an absence.
    ///
    /// `permit` is the admission witness: the replay runs under the write hold
    /// the invocation took, and a `&MutatePermit` derefs to one, so "staged
    /// without the gate" does not compile. It is never inspected.
    pub(in crate::state) fn stage_cell(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: Option<Bytes>,
    ) {
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        match value {
            Some(bytes) => dirty.set_owned(&id, cell, bytes),
            None => dirty.clear(&id, cell),
        }
    }

    /// Stages one section's dirty clear marker — the sink a whole-layout reset
    /// replays through, one entry per declared section. Within this event the
    /// section reads as "deleted at this program point": a read answers
    /// absence, a scan yields only cells staged after the clear, and later
    /// writes repopulate it. Same admission witness as [`Self::stage_cell`].
    pub(in crate::state) fn stage_section_clear(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear_section(&id, section);
    }

    /// Durably commits the collection's buffered changes mid-handler — the
    /// engine command behind every handle's `commit()`. The contract lives on
    /// the [`collection`](crate::state::collection) module's mid-handler
    /// durability section.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were written, or
    /// [`StoreOutcome::NoOp`] when nothing was buffered.
    ///
    /// `permit` is the admission witness: the whole sequence runs under the
    /// caller's hold, so it never re-enters the non-reentrant gate. It is never
    /// inspected. [`Self::rollback`] takes no witness, because it acquires the
    /// gate itself.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Store`] when the underlying store fails (the
    /// buffer is left intact, so the ops still ride the normal commit path).
    pub(in crate::state) async fn commit(
        &self,
        _permit: &OpPermit<'_>,
        state_type: StateType,
        name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        let cleared = dirty.cleared_sections(&id);
        let mut resolved = dirty.collection_snapshot(&id);
        if resolved.is_empty() && cleared.is_empty() {
            return Ok(StoreOutcome::NoOp);
        }
        // A `Cleared` cell in a cleared section is subsumed by the clear's gap
        // erase — dropping it keeps the batch row-disjoint (no written row
        // overlaps a gap range); the remaining present cells of a cleared
        // section are exactly its survivors.
        resolved.retain(|(cell, data)| data.is_some() || !cleared.contains(&cell.section));
        let clears: Vec<SectionClear> = cleared
            .iter()
            .map(|&section| SectionClear::frozen_resolved(section, &resolved))
            .collect();
        let ttl = self.inner.registry.ttl_for(state_type, name);
        let collection_ref = CollectionRef::new(id.clone(), ttl);
        self.inner
            .overlay
            .lower()
            .write_resolved(&collection_ref, &resolved, &clears)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        // Drain only after the write landed: a store failure leaves the
        // buffer intact, so the ops still ride the normal commit path. The
        // drain also drops the collection's dirty clear markers — sound
        // because the clears were applied durably in the same write.
        dirty.remove_collection(&id);
        Ok(StoreOutcome::Applied)
    }

    /// Discards the collection's buffered uncommitted ops mid-handler — the
    /// engine command behind every handle's `rollback()`, and
    /// [`Self::commit`]'s discard twin. The contract lives on the
    /// [`collection`](crate::state::collection) module's mid-handler durability
    /// section.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were discarded, or
    /// [`StoreOutcome::NoOp`] on an empty, closed, stale-pinned, or terminated
    /// session.
    pub(in crate::state) async fn rollback(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> StoreOutcome {
        // The gate acquire for rollback lives HERE, not in the engine (every
        // handle path is a single delegating call): the drain must serialize
        // with commit()'s snapshot→write→drain (KV4), and rollback's
        // infallible contract needs the gate's phase — a CLOSED session (the
        // settle boundary already snapshotted it) discards nothing, expressed
        // as a NoOp because the signature cannot surface `SessionClosed`.
        let permit = self.inner.gate.read().await;
        // Self-admission INSIDE the held gate: rollback expresses every other
        // cell op's admission checks as a `NoOp` (its infallible signature
        // cannot surface an error). A stale pin (this clone outlived its
        // attempt — the epoch was bumped) drains nothing, so it cannot touch
        // the next attempt's live buffer; a closed session (the settle boundary
        // already snapshotted it) and a terminated one (shutdown/cancel) do the
        // same. Without the pin check a stale clone of a retried event moved
        // into a spawned task could drain the next attempt's buffer: a silent
        // lost write.
        if !self.attempt_current() || permit.is_closed() || self.is_terminated() {
            return StoreOutcome::NoOp;
        }
        let id = self.id_for(state_type, name);
        let dirty = self.inner.overlay.dirty();
        // Peek-then-drain is race-free under the held gate permit: no other
        // session op can interleave between the probe and the drain.
        if !dirty.collection_dirty(&id) {
            return StoreOutcome::NoOp;
        }
        dirty.remove_collection(&id);
        StoreOutcome::Applied
    }
}

/// Seeding and inspection seams for the suites, so a test can arrange overlay
/// state without a collection handle.
///
/// The seams **arrange** state; they never model a mutation. Each seeding seam
/// takes the operation gate — so a seed serializes against a concurrent scoped
/// operation rather than tearing one — but skips the mutator admission order
/// (stale pin, closed session, termination), which is what the real handles
/// exercise in the gate suite. A seed therefore succeeds on a session a handle
/// would refuse, which is the point: a test can seed the state a refusal is
/// asserted against. Consecutive seeds are not one atomic unit; each releases
/// the gate.
#[cfg(test)]
impl<B, L> KeyedStateSession<B, L>
where
    B: StateBackend,
{
    /// Stages one cell (or an absence) into this event's dirty overlay. Must
    /// not be called while a permit is already held: the gate is not reentrant.
    pub(crate) async fn seed(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: Option<&[u8]>,
    ) {
        let permit = self.permit().await;
        self.stage_cell(
            &permit,
            state_type,
            name,
            cell,
            value.map(Bytes::copy_from_slice),
        );
    }

    /// [`Self::seed`]'s section-clear twin.
    pub(crate) async fn seed_section_clear(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) {
        let permit = self.permit().await;
        self.stage_section_clear(&permit, state_type, name, section);
    }

    /// The visible committed bytes of one cell, gate-free — the read seam for
    /// suites outside `crate::state`.
    ///
    /// # Errors
    ///
    /// As [`Self::get`].
    pub(crate) async fn peek(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        self.get(state_type, name, cell).await
    }
}

impl<B, L> StateLifecycle for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Cell = B::Cell;
    type Checks = B::Checks;

    fn gate(&self) -> &SessionGate {
        &self.inner.gate
    }

    async fn close_gate(&self) -> OpPermit<'_> {
        let event = self.inner.event;
        let key = &self.inner.state_key.key;
        self.inner
            .gate
            .close(|waited_s| {
                warn!(
                    event = ?event,
                    key = %key,
                    waited_s,
                    "settle waiting on the session operation gate; a session op future may be \
                     held un-polled"
                );
            })
            .await
    }

    async fn finalize(&self) -> Result<Finalized<B::Cell, B::Checks>, StateAccessError> {
        let touched = self
            .inner
            .overlay
            .dirty()
            .touched(&self.inner.state_key.key);
        let event = self.inner.event;
        let registry = &self.inner.registry;
        let lower = self.inner.overlay.lower();
        let state_key = &self.inner.state_key;
        let mut marker_touched = Vec::with_capacity(touched.len());
        for ((state_type, name), ..) in &touched {
            if registry.commit_mode_for(*state_type, name) == CommitMode::ReadCommitted {
                marker_touched.push((*state_type, name.clone()));
            }
        }
        marker_touched.sort_unstable();
        marker_touched.dedup();
        let ttl = evidence_ttl(
            self.inner.dedup_ttl,
            marker_touched
                .iter()
                .map(|(state_type, name)| registry.ttl_for(*state_type, name)),
        );
        // Sized once to the touched-collection cardinality — the fold in
        // place of an unconstrained `try_collect` keeps the receipt's vector
        // from re-growing on the per-event hot path (bounded-allocation rule).
        let evidence = EventEvidence {
            attempt: *self.inner.stage_attempt.get_or_init(AttemptId::new),
            touched: marker_touched.into(),
            evidence_ttl: ttl,
            dedup: self.message_marker().map(MessageMarker::into_uuid),
        };
        let capacity = touched.len();
        let collections = stream::iter(touched)
            .map(|((state_type, name), cleared, cells)| {
                let id = CollectionId::new(state_key.clone(), state_type, name);
                // `cooperative` adds a per-collection coop-budget yield point
                // so a key touching many collections does not drain the batch
                // in one poll; `buffer_unordered` keeps full concurrency.
                cooperative(stage_collection(
                    lower, registry, event, id, cleared, cells, &evidence,
                ))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_fold(Vec::with_capacity(capacity), |mut acc, staged| async move {
                acc.extend(staged);
                Ok(acc)
            })
            .await;
        let collections = match collections {
            Ok(collections) => collections,
            Err(error) => {
                self.inner
                    .checks
                    .unmark(&self.inner.state_key.key)
                    .await
                    .map_err(|error| StateAccessError::store(&error))?;
                return Err(error);
            }
        };
        self.discard_dirty();
        if collections.is_empty() {
            return Ok(Finalized::Clean);
        }
        Ok(Finalized::Staged(Staged {
            store: lower.clone(),
            collections,
            checks: self.inner.checks.clone(),
            key: self.inner.state_key.key.clone(),
        }))
    }

    async fn record_marker(
        &self,
        marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> Result<(), StateAccessError> {
        self.inner
            .dedup
            .insert(marker.into_uuid())
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    fn discard_dirty(&self) {
        // Sync and ungated (Drop paths cannot await). Every caller either holds
        // the gate — settle/unwind under the closed-gate permit, or `reset`
        // under its read-permit, which waits out any in-flight session op
        // before clearing — or is the ungated `EventStateScope::Drop` teardown,
        // whose already-admitted-op residual is documented on that type. A
        // clone detached past its attempt does not survive into the next
        // attempt: `reset` bumps the epoch under the same hold (see
        // `AttemptEpoch`), so the stale write errors `Terminated`. Keep session
        // ops inside the owning handler future all the same.
        self.inner
            .overlay
            .dirty()
            .clear_event(&self.inner.state_key.key);
    }

    fn terminate(&self) {
        self.inner.terminated.store(true, Ordering::Relaxed);
    }

    async fn reset(&self, _proof: RepinProof) {
        // ONE gate hold spanning discard-then-bump. Separate steps would let a
        // stale queued write acquire the gate after the clear, buffer under the
        // old epoch, and survive into attempt N+1. Holding the gate here also
        // waits out any in-flight session op (the no-un-polled-op contract
        // still applies), so the discard sees a quiescent dirty range.
        let _permit = self.inner.gate.read().await;
        self.discard_dirty();
        self.bump_epoch();
    }

    fn repin(&self, _proof: RepinProof) -> Self {
        Self {
            inner: self.inner.clone(),
            pinned: self.current_epoch(),
        }
    }
}

impl<B, L> MarkerIdentity for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn set_reload_marker(&self, marker: MessageMarker) {
        // Override implies timer session: only the deferred-message reload
        // sets it, and that reload always dispatches under a timer EventRef.
        debug_assert!(
            matches!(self.inner.event, EventRef::Timer(_)),
            "the reload override is set only on timer sessions"
        );
        *self.inner.reload_marker.lock() = Some(marker);
    }

    fn message_marker(&self) -> Option<MessageMarker> {
        match self.inner.event {
            // The message's own id — the override is never read here, so a
            // divergent override on a message session is unreadable.
            EventRef::Message { dedup_id } => Some(MessageMarker::new(dedup_id)),
            EventRef::Timer(_) => *self.inner.reload_marker.lock(),
        }
    }
}

/// One batch-read unit of a `ReadCommitted` stage: a section's contiguous
/// `≤CELL_BATCH` survivor run, its coordinate batch, and the dirty records —
/// in the same order — each batched committed base pairs with. `batch` aligns
/// 1:1 with `records` by construction (each is built from the same run).
struct StageChunk {
    section: Section,
    batch: CoordinateBatch,
    records: CellBuffer<(CellKey, Option<Bytes>)>,
}

/// Splits order-preserved survivors (sorted by `(section, coordinate)`) into
/// per-section `≤CELL_BATCH` [`StageChunk`]s. Concatenating the chunks' records
/// reproduces the input; each chunk's `batch` is built from its own records, so
/// it aligns 1:1 with them. Splitting per section (not purely by count) keeps
/// every `get_many` call single-section, as its `section` argument requires.
fn stage_chunks(
    survivors: impl Iterator<Item = (CellKey, Option<Bytes>)>,
) -> impl Iterator<Item = StageChunk> {
    let mut it = survivors.peekable();
    from_fn(move || {
        // `Section` is `Copy`, so the peek borrow ends here, before `next_if`.
        let section = it.peek()?.0.section;
        let mut records: CellBuffer<(CellKey, Option<Bytes>)> = CellBuffer::new();
        while let Some(record) =
            it.next_if(|(cell, _)| cell.section == section && records.len() < CELL_BATCH)
        {
            records.push(record);
        }
        // `records` is non-empty (peek showed a same-section head) and
        // `≤CELL_BATCH`, so `chunks` yields exactly one batch. Bind it in its
        // own statement so the borrow of `records` ends before it is moved.
        let batch =
            CoordinateBatch::chunks(records.iter().map(|(cell, _)| cell.coordinate.clone())).next();
        batch.map(|batch| StageChunk {
            section,
            batch,
            records,
        })
    })
}

/// Stages one collection's touched cells in a single batch, returning the
/// frozen [`StagedCollection`] record the receipt promotes / rolls back (or
/// `None` for a `ReadUncommitted` collection, which resolves at stage time).
/// A `Cleared` cell in a cleared section is dropped on both arms: the clear's
/// gap erase subsumes it, and dropping it keeps the batch row-disjoint (no
/// written row overlaps a gap range) — the section's remaining present cells
/// are exactly its frozen survivors. Free function so no `self` borrow
/// crosses the concurrent fan-out.
async fn stage_collection<S>(
    lower: &S,
    registry: &CollectionDefRegistry,
    event: EventRef,
    id: CollectionId,
    cleared: ClearedSections,
    cells: CellSnapshot,
    evidence: &EventEvidence,
) -> Result<Option<StagedCollection>, StateAccessError>
where
    S: CellStore,
{
    let collection_ref =
        CollectionRef::new(id.clone(), registry.ttl_for(id.state_type(), id.name()));
    let cleared = &cleared;
    let subsumed = |cell: &CellKey, value: &DirtyVal| {
        *value == DirtyVal::Cleared && cleared.contains(&cell.section)
    };
    match registry.commit_mode_for(id.state_type(), id.name()) {
        CommitMode::ReadCommitted => {
            let id = &id;
            // Read each surviving cell's committed base in per-section batches
            // instead of one point read per cell. Passing `event` as
            // `get_many`'s `own` returns this event's `prev` while its
            // provisional cell stands, so a retry re-stages over the same base
            // (idempotent) — a `Set` cell in a cleared section keeps its
            // committed pre-clear `prev` this way. `cooperative` adds a
            // per-batch coop-budget yield point; `buffered` keeps full
            // concurrency while preserving order — inert (marker/clear freezing
            // sort internally and settle is row-disjoint), with only
            // `≤SHARD_FANOUT_CONCURRENCY` result buffers in flight. Cells
            // subsumed by a section clear are dropped first, keeping the batch
            // row-disjoint (survivors == the section's present cells). Sized
            // once to the pre-filter snapshot cardinality (the filter can only
            // shrink it) — bounded-allocation rule.
            let capacity = cells.len();
            let survivors = cells
                .into_iter()
                .filter(|(cell, value)| !subsumed(cell, value))
                .map(|(cell, value)| (cell, value.into_data()));
            let writes: Vec<(CellKey, ProvisionalWrite)> = stream::iter(stage_chunks(survivors))
                .map(|chunk| {
                    cooperative(async move {
                        let StageChunk {
                            section,
                            batch,
                            records,
                        } = chunk;
                        let bases = lower
                            .get_many(id, section, &batch)
                            .await
                            .map_err(|e| StateAccessError::store(&e))?;
                        // `get_many`'s contract: bases.len() == batch.len()
                        // == records.len(). Pair this chunk's bases with
                        // EXACTLY its records BEFORE the fold flattens
                        // across chunks. The debug_assert mirrors the store
                        // default's / `Overlay::get_many`'s alignment
                        // posture (a hard panic is banned); by construction
                        // the lengths match.
                        debug_assert_eq!(
                            bases.len(),
                            records.len(),
                            "get_many must answer every batched cell"
                        );
                        let chunk_writes: CellBuffer<(CellKey, ProvisionalWrite)> = records
                            .into_iter()
                            .zip(bases)
                            .map(|((cell, data), prev)| {
                                (cell, ProvisionalWrite::new(data, prev, event))
                            })
                            .collect();
                        Ok::<_, StateAccessError>(chunk_writes)
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY)
                .try_fold(
                    Vec::with_capacity(capacity),
                    |mut acc, chunk_writes| async move {
                        acc.extend(chunk_writes);
                        Ok(acc)
                    },
                )
                .await?;
            if writes.is_empty() && cleared.is_empty() {
                return Ok(None);
            }
            // `finalize` builds the staged record exactly once per collection
            // from the post-`commit()` dirty buffer, so the marker lists
            // exactly this stage's writes and frozen clears; only a retry
            // attempt re-running `finalize` re-stages (an idempotent
            // same-event marker overwrite). A clears-only collection stages
            // `writes = []` under a marker whose `clears()` is non-empty: the
            // durable marker still lands and the stage-boundary
            // foreign-marker resolve still runs, and the returned entry makes
            // `finalize` returns a staged receipt so the boundary promotes the clear.
            let clears: Vec<SectionClear> = cleared
                .iter()
                .map(|&section| SectionClear::frozen(section, &writes))
                .collect();
            let marker = EventMarker::frozen(
                event,
                &writes,
                &clears,
                &evidence.touched,
                evidence.evidence_ttl,
                evidence.dedup,
                evidence.attempt,
            );
            lower
                .write_provisional(&collection_ref, &writes, Some(&marker))
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some(StagedCollection {
                collection: collection_ref,
                writes,
                marker,
            }))
        }
        CommitMode::ReadUncommitted => {
            let resolved: ResolvedCells = cells
                .into_iter()
                .filter(|(cell, value)| !subsumed(cell, value))
                .map(|(cell, value)| (cell, value.into_data()))
                .collect();
            if resolved.is_empty() && cleared.is_empty() {
                return Ok(None);
            }
            // The direct apply: cells plus the frozen gap erase in one write.
            // RU writes resolved values directly.
            // return `None` even for a clears-only collection.
            let clears: Vec<SectionClear> = cleared
                .iter()
                .map(|&section| SectionClear::frozen_resolved(section, &resolved))
                .collect();
            lower
                .write_resolved(&collection_ref, &resolved, &clears)
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(None)
        }
    }
}

/// Promotes each collection independently. Permanent errors leave residue for
/// admission.
async fn resolve_collections<S: CellStore>(
    store: &S,
    collections: Vec<StagedCollection>,
    shutdown: &(impl Fn() -> bool + Sync),
) -> bool {
    stream::iter(collections)
        .map(|staged| cooperative(async move {
            loop {
                if shutdown() {
                    return false;
                }
                match store.commit_provisional(&staged.collection, &staged.marker, &staged.writes).await {
                    Ok(()) => return true,
                    Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                        warn!(%error, "promote failed permanently; admission must resolve the key");
                        return false;
                    }
                    Err(error) => {
                        warn!(%error, "promote failed; retry");
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }))
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .fold(true, |all, complete| async move { all && complete })
        .await
}

/// Crate-private descriptor reaching a session through the one public
/// [`EventContext::state`] method — the sole state surface wrapper contexts
/// forward. Binding it yields the session itself (`Handle<S> = S`), so the
/// settlement boundary drives the full sealed [`StateLifecycle`] on it.
///
/// This is the **settlement surface**: the sealed [`StateLifecycle`] verbs.
/// It stays `pub(crate)` because the settle-module-private
/// [`SettlementAccess`](crate::consumer::middleware::settle) extension must
/// name it. Residual: no convenient crate-wide accessor exists (the old
/// `LifecycleAccessExt` is gone), so reaching this surface outside settle
/// requires writing `context.state(Registered::new(LifecycleAccess))` plus a
/// `use sealed::StateLifecycle` by hand — a deliberate, greppable act rather
/// than a one-call convenience. Dedup / defer-reload reach only the marker
/// identity, through the narrow [`MarkerHandle`].
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct LifecycleAccess;

impl DescriptorIdentity for LifecycleAccess {
    /// Inert: [`LifecycleAccess::bind`](StateDescriptor::bind) returns the
    /// session verbatim without validating registration, and `LifecycleAccess`
    /// is never registered, so neither `name` nor `structural_identity` is ever
    /// consulted. They exist only to satisfy the [`StateDescriptor`]
    /// supertrait.
    fn name(&self) -> &'static str {
        "\u{0}lifecycle"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            format_id: "\u{0}framework-lifecycle",
            resolver_id: None,
            key_format_id: "\u{0}framework-lifecycle",
        }
    }
}

impl SealedDescriptor for LifecycleAccess {}

impl StateDescriptor for LifecycleAccess {
    type Handle<S: StateSession> = S;

    /// Returns the session itself — the lifecycle tunnel binds no typed handle
    /// and validates no registration; the boundary drives the sealed
    /// [`StateLifecycle`] on the returned session. Every real caller binds a
    /// [`EventSession`], so the returned `S` carries the full lifecycle.
    fn bind<S: StateSession>(self, session: &S) -> Result<S, StateAccessError> {
        Ok(session.clone())
    }

    /// No-op: the lifecycle tunnel carries no operational settings, so it keeps
    /// the default [`collection_def`](StateDescriptor::collection_def) and the
    /// inherited fluent setters are unreachable no-ops.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// The narrow marker-identity handle handed to the three
/// [`MarkerIdentity`] audiences (defer-reload set,
/// dedup read, settle read). Wraps a session clone but exposes **only** the
/// two marker methods — never the raw session, so it cannot reach the
/// settlement surface. This is the tunnel-narrowing enforcement: dedup and
/// defer-reload bind [`MarkerAccess`] and get one of these, so they cannot
/// import `sealed::StateLifecycle` and call `close_gate` / `finalize`.
pub(crate) struct MarkerHandle<S>(S);

impl<S: MarkerIdentity> MarkerHandle<S> {
    /// Sets the deferred-reload identity override — see
    /// [`MarkerIdentity::set_reload_marker`].
    pub(crate) fn set_reload_marker(&self, marker: MessageMarker) {
        self.0.set_reload_marker(marker);
    }

    /// The message commit-marker identity — see
    /// [`MarkerIdentity::message_marker`].
    pub(crate) fn message_marker(&self) -> Option<MessageMarker> {
        self.0.message_marker()
    }
}

/// Crate-private descriptor binding a session's marker-identity surface,
/// forwarded through the one public [`EventContext::state`] method exactly as
/// [`LifecycleAccess`] is. Binding yields a [`MarkerHandle`], never the raw
/// session, so the audience reaches only `set_reload_marker`/`message_marker`.
///
/// Deliberately a full sibling of `LifecycleAccess` rather than a shared
/// generic descriptor — the two distinct bound `Handle` types are the tunnel
/// split itself.
///
/// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarkerAccess;

impl DescriptorIdentity for MarkerAccess {
    /// Inert, exactly as [`LifecycleAccess`]: `MarkerAccess` is never
    /// registered, so neither field is consulted — they satisfy the
    /// [`StateDescriptor`] supertrait only.
    fn name(&self) -> &'static str {
        "\u{0}marker"
    }

    fn structural_identity(&self) -> StructuralIdentity {
        StructuralIdentity {
            kind: CollectionKindId::Value,
            format_id: "\u{0}framework-marker",
            resolver_id: None,
            key_format_id: "\u{0}framework-marker",
        }
    }
}

impl SealedDescriptor for MarkerAccess {}

impl StateDescriptor for MarkerAccess {
    type Handle<S: StateSession> = MarkerHandle<S>;

    /// Wraps the session in a [`MarkerHandle`], validating no registration —
    /// the marker tunnel carries no typed collection.
    fn bind<S: StateSession>(self, session: &S) -> Result<MarkerHandle<S>, StateAccessError> {
        Ok(MarkerHandle(session.clone()))
    }

    /// No-op: the marker tunnel carries no operational settings.
    fn with_collection_def(self, _def: CollectionDef) -> Self {
        self
    }
}

/// Crate-private extension giving the marker-identity audiences (defer-reload,
/// dedup, and settle) one-call access to their event's [`MarkerHandle`]
/// through the public [`EventContext::state`] method — the narrow replacement
/// for the deleted crate-wide `lifecycle()` accessor.
pub(crate) trait MarkerAccessExt: EventContext {
    /// Binds the event's marker-identity handle. Fails with
    /// [`StateAccessError`] only when the context is terminated;
    /// [`MarkerAccess`] is otherwise registration-independent.
    fn marker_identity(&self) -> Result<MarkerHandle<Self::State>, StateAccessError> {
        self.state(Registered::new(MarkerAccess))
    }
}

impl<C: EventContext> MarkerAccessExt for C {}
