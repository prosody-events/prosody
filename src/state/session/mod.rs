//! Per-event keyed-state sessions.
//!
//! A session is the per-event view over a partition's keyed-state cell store:
//! byte-cell reads and writes buffer in a per-event dirty overlay, and the
//! framework drives the stage/promote lifecycle through a sealed supertrait
//! that downstream crates can neither implement nor call.
//!
//! [`KeyedStateSession`] is the sole implementation — the real session, minted
//! per event by the partition's state manager. It holds **one uniform
//! `Overlay`** (the per-event `DirtyStore` over the partition's committed
//! cell store): clones share the per-event dirty overlay and reload-marker
//! override plus the cross-event singletons (the commit oracle, the armed
//! backstop, the event, the registry), so repeated descriptor binds of one
//! collection accumulate into one write.
//!
//! # The session / lifecycle split
//!
//! The session surface is the handler surface plus two sealed supertraits:
//!
//! - [`CellSession`] — the read/buffer/mutate surface handlers reach through
//!   collection handles: `get`/`scan` + the buffering mutators
//!   `set`/`clear`/`clear_section` and the mid-handler transactional pair
//!   `commit`/`rollback`, plus
//!   `loader`/`is_terminated`/`verify_state_registration`.
//!   [`EventContext::State`] bounds this.
//! - `sealed::StateLifecycle` — the sealed, manager-driven lifecycle
//!   (`finalize` and the attempt/teardown verbs — settling moved onto the
//!   receipt `finalize` returns) — and `sealed::MarkerIdentity` — the
//!   boundary-readable message-marker identity. Both are `pub(crate)`
//!   supertraits of [`CellSession`] that seal it: downstream crates can name
//!   `CellSession` in bounds but can neither implement it nor reach either
//!   surface.
//!
//! # Lifecycle
//!
//! The framework's per-event sequence, driven by the durability boundary
//! (`crate::consumer::middleware`'s blanket `EventHandler` impl) in
//! straight-line code:
//!
//! 1. Handler ops buffer into the dirty overlay.
//! 2. On a final handler success, `finalize` groups the one dirty map by
//!    collection and stages each in one same-partition batch — `ReadCommitted`
//!    collections stage provisional cells, `ReadUncommitted` ones write
//!    resolved values — draining the event's dirty range (the stage consumes
//!    the buffered ops) and returning the staged work as a linear `Finalized`
//!    receipt.
//! 3. Strictly after the stage, the boundary records the message commit marker
//!    read from the session's event identity — the message `EventRef`'s dedup
//!    id, or the deferred-reload override (`message_marker`) — through the
//!    commit oracle. After the offset/trigger commit the boundary consumes the
//!    receipt: `certify().promote()` promotes the staged cells; on an
//!    arm-shutdown abort `rollback()` restores their committed bases.
//! 4. At attempt boundaries the retry loop runs the `next_attempt` verb, whose
//!    `reset` transition discards this event's dirty buffer (under the gate)
//!    and bumps the attempt epoch, so the next attempt starts clean and any
//!    handle leaked from the failed attempt is fenced; the identity override
//!    persists by design (it is identity, not decision).

use crate::consumer::event_context::EventContext;
use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::consumer::partition::ShutdownPhase;
use crate::state::access::StateAccessError;
use crate::state::cell::ProvisionalWrite;
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor::{
    DescriptorIdentity, Registered, SealedDescriptor, StateDescriptor, StructuralIdentity,
};
use crate::state::dirty::{CellSnapshot, ClearedSections, DirtyStore, DirtyVal, ResolvedCells};
use crate::state::identity::{CollectionId, CollectionRef};
use crate::state::manager::ArmedKeys;
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::oracle::CommitOracle;
use crate::state::overlay::Overlay;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::{
    CollectionKindId, CommitMode, EventRef, SHARD_FANOUT_CONCURRENCY, STATE_FANOUT_CONCURRENCY,
    StateBackend, StateKey, StateName, StateType, StoreOutcome,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use parking_lot::{Mutex as SyncMutex, RwLock};
pub(crate) use sealed::{Finalized, MessageMarker, MutatePermit, OpPermit, SessionGate};
use sealed::{MarkerIdentity, StagedCollection, StagedState, StateLifecycle};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::coop::cooperative;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// The per-event session collections read, buffer, and mutate through.
///
/// `get`/`scan` describe the session's **visible committed bytes** for a cell —
/// [`KeyedStateSession`] realises that through the dirty overlay + oracle
/// resolution — and `set`/`clear`/`clear_section` buffer this event's
/// mutations (`commit` writes them through mid-handler; `rollback` discards
/// them). After the settle boundary's stage drains the buffer (`finalize` on
/// success), a handle's reads fall through to the lower store: the apply
/// hooks observe the per-cell committed projection (an own-event provisional
/// cell reads as its committed base `prev`), not the event's pre-settle
/// overlay. The framework reaches the manager-driven lifecycle and the
/// message-marker identity through the sealed `StateLifecycle` and
/// `MarkerIdentity` supertraits, which seal `CellSession`: downstream crates
/// can name it in bounds (e.g. [`EventContext::State`]) but can neither
/// implement it nor reach either surface.
pub trait CellSession: StateLifecycle + MarkerIdentity + Clone + Send + Sync + 'static {
    /// Opaque per-session capability slot. The keyed-state machinery never
    /// interprets it; a
    /// [`CellResolver`](crate::state::descriptor::CellResolver)
    /// living outside `src/state` reads it from the session at resolve time.
    type Loader: Clone + Send + Sync + 'static;

    /// Returns the session's capability slot for a resolver to read.
    fn loader(&self) -> &Self::Loader;

    /// Returns `true` once the partition is shutting down or the event has been
    /// cancelled. Descriptor handles guard every operation on this.
    fn is_terminated(&self) -> bool;

    /// Whether the collection named `(state_type, name)` carries a TTL — the
    /// query the Map meta refresh consults to keep its keyset `Meta` cell
    /// renewed on every `set`, so it provably outlives every entry.
    /// No default impl: a silent `false` would disable the refresh for a
    /// real session.
    fn collection_has_ttl(&self, state_type: StateType, name: &StateName) -> bool;

    /// The Map keyset bound for `(state_type, name)` — the number of live
    /// distinct keys a map tracks before overflowing to the full-section scan.
    /// Read per `set`/`stream` on a Map handle. No default impl: a wrong
    /// silent default would mis-size the keyset for a real session.
    fn collection_keyset_limit(&self, state_type: StateType, name: &StateName) -> usize;

    /// Validates that the keyed-state collection named `(state_type, name)` is
    /// registered with the asserted structural identity, returning the
    /// canonical [`StateName`].
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session,
    /// [`StateAccessError::Unregistered`] for an unknown name, or
    /// [`StateAccessError::IdentityMismatch`] when the registered identity
    /// differs from the asserted one.
    fn verify_state_registration(
        &self,
        name: &'static str,
        state_type: StateType,
        identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError>;

    /// Reads a cell's currently visible committed value within this event's
    /// transaction (cleared/absent → `None`).
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails.
    fn get(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<Option<Bytes>, StateAccessError>> + Send;

    /// The single-section, start-anchored, bidirectional range primitive: a
    /// lazy stream of the visible committed cells in `coordinate` byte order.
    fn scan<'a>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a;

    /// Buffers a set of the cell's bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
    fn set(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: &[u8],
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers a clear of the cell.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
    fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Buffers an in-RAM dirty clear marker for one section of the
    /// collection: within this event the section reads as "deleted at this
    /// program point" — `get` answers absence, `scan` yields only cells set
    /// after the clear — and later `set`s repopulate it. Committed, the
    /// section holds exactly the survivors; aborted, it is untouched.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session.
    fn clear_section(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send;

    /// Durably commits the collection's buffered changes mid-handler, so they
    /// survive a restart after failure. This is why it exists: a complex or
    /// large handler (a materialization handler fanning one message into
    /// thousands of writes) commits incremental progress and resumes from
    /// it on retry or redelivery instead of starting from scratch. Handler
    /// idempotence across the resume is the contract.
    ///
    /// Every currently-buffered op of the collection is written straight to
    /// committed state ([`write_resolved`]) and dropped from the dirty buffer,
    /// so multi-cell kinds commit data and bookkeeping together (a Map's
    /// entries and keyset, a Deque's entries and window bounds). Within the
    /// batch budget those cells ride one atomic same-partition batch; an
    /// over-budget commit splits into the fewest fitting batches, and —
    /// `write_resolved` being marker-free — a crash mid-split can leave a
    /// torn committed write the store cannot reconstruct (the over-budget
    /// residual on the collection-grain atomicity invariant, [`CellStore`]),
    /// reconstructed only when the idempotent handler re-run re-issues the same
    /// ops. The guarantee is **at-least-once**: a
    /// `commit()`-landed write is durable and visible immediately — never
    /// provisional, never listed in any event marker, never rolled back. Ops
    /// buffered *after* the commit ride the collection's normal stage→settle
    /// path; reads already see buffered
    /// writes without committing.
    ///
    /// **Orthogonal to [`CommitMode`]:** the mode governs how *un-committed*
    /// writes settle at the event boundary — staged provisionally for
    /// `ReadCommitted` (external readers observe committed values only after
    /// the event commits), applied immediately for `ReadUncommitted`.
    /// `commit()` bypasses that staging entirely: a `commit()`-landed write
    /// on a `ReadCommitted` collection is externally visible at once and
    /// survives an event abort.
    ///
    /// Every collection handle exposes `commit()` — Value, Map, and Deque
    /// all do, and future collection kinds must too; the handles' docs link
    /// back here. [`Self::rollback`] is its discard twin.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were written, or
    /// [`StoreOutcome::NoOp`] when nothing was buffered.
    ///
    /// # Errors
    ///
    /// Returns [`StateAccessError::Unavailable`] on a stateless session, or
    /// [`StateAccessError::Store`] when the underlying store fails (the
    /// buffer is left intact, so the ops still ride the normal commit path).
    ///
    /// [`write_resolved`]: crate::state::store::CellStore::write_resolved
    fn commit(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = Result<StoreOutcome, StateAccessError>> + Send;

    /// Discards the collection's buffered uncommitted ops mid-handler — cells
    /// *and* dirty clear markers — reverting reads to the last
    /// [`Self::commit`], or the pre-event committed value if none. It is
    /// `commit()` minus the durable write: the same whole-collection drain,
    /// to nothing.
    ///
    /// **It cannot cross a `commit()` floor.** A `commit()`-landed row is
    /// durable and unreachable by rollback — only ops buffered since the last
    /// `commit()` are discarded.
    ///
    /// Async because it joins the session operation gate (`SessionGate`): a
    /// buffer drain racing `commit()`'s snapshot→write→drain could otherwise
    /// persist a partial set no serial order explains. Still infallible: it
    /// touches only the in-memory dirty buffer and cannot fail. A terminated
    /// session (partition shutting down, or the event cancelled) and a
    /// **closed** session (the settle boundary already snapshotted it) both
    /// discard nothing and return [`StoreOutcome::NoOp`] — the containment
    /// every other cell op gets from the live-guard and the gate's closure
    /// check, expressed as a `NoOp` because the infallible signature cannot
    /// surface an error. It keeps a stale clone that outlived its event from
    /// draining a later same-key event's buffer.
    ///
    /// Distinct from the settle boundary's rollback of staged provisional cells
    /// (the receipt's `StagedState::rollback`, framework-only, after the
    /// handler returns): this is the handler-facing mid-flight discard.
    ///
    /// Returns [`StoreOutcome::Applied`] when buffered ops were discarded, or
    /// [`StoreOutcome::NoOp`] on an empty buffer.
    fn rollback(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> impl Future<Output = StoreOutcome> + Send;
}

/// Crate-sealed lifecycle half of [`CellSession`].
///
/// The module is `pub(crate)`, so downstream crates can name [`CellSession`] in
/// bounds but can neither implement it nor reach the lifecycle: staging,
/// promoting, and discarding are framework-only moves.
pub(crate) mod sealed {
    use super::{
        CellKey, CellStore, CollectionRef, CompactDateTime, CompactDuration, Duration, Future,
        MarkerWrite, ProvisionalWrite, RepinProof, SectionClear, StateAccessError, Uuid,
        resolve_collections,
    };
    use opentelemetry::global::meter;
    use opentelemetry::metrics::Counter;
    use std::ops::Deref;
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
    /// point-get stream (a sub-threshold deque window, or a `Tracked` map
    /// keyset within its bound) takes the gate for its init metadata read
    /// (the map keyset cell / the deque window cell), releases it, then
    /// fetches the listed entries in gate-scoped chunks — one permit per
    /// chunk, ≤ the chunk width in point reads each. Each chunk is fetched,
    /// decoded, and resolved under that one permit, which is dropped with the
    /// chunk future's scope before any of the chunk's items reach user code, so
    /// the permit is **never held across a yield to user code (items and errors
    /// alike)**. A *scan-path* stream takes the gate only for its init metadata
    /// read and is per-item live thereafter — and its per-item resolution is a
    /// pure **read** (a scan never writes a resolution back durably; the
    /// point-read / first-touch / recovery-sweep paths own repair),
    /// so a concurrent mid-stream `commit()` on a scanned cell is never
    /// clobbered. A mutator racing a live stream (`join!`, or a handler
    /// mutating its own collection between stream items) therefore waits at
    /// most one chunk fetch+resolve — never a whole materialization — and
    /// settle's closure acquire queues FIFO the same way.
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
    /// ([`StateLifecycle::reset`](super::StateLifecycle::reset)) bumps the
    /// session epoch under this gate, and a detached clone keeps its stale pin,
    /// so the leak errors at the point its op takes effect — uniformly across
    /// the whole surface (`Terminated` on a crossed attempt boundary,
    /// `SessionClosed` in the post-settle hook window):
    ///
    /// * **handle ops** (`get`/`set`/`clear`/…) — the pin compare in
    ///   `ensure_live` / `mutate_permit`'s ordered admission;
    /// * **apply-hook mutations** past the settle window — the closed gate and
    ///   attempt teardown;
    /// * **scans and streams** — the scan shell's per-emission fence
    ///   (`CellView::fenced`), which runs `ensure_live` after every stream
    ///   completion, so a leaked stream errors at its next emission and no
    ///   buffered item crosses the boundary.
    ///
    /// Keep every session op inside the handler future that owns the event all
    /// the same; the fence is the backstop, not a license to detach.
    ///
    /// Perf posture: uncontended for any handler that does not `join!` its
    /// session ops — one uncontended tokio `Mutex` lock per op; the phase
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
    /// Witnesses admission for a session **read**: the descriptor's cell-op
    /// sinks demand `&OpPermit` so "forgot to acquire the gate" and "let the
    /// acquire outlive the op" cannot compile. The settle boundary's closure
    /// hold ([`SessionGate::close`]) is also this type — the name stays
    /// `OpPermit` (not `ReadPermit`) because `close` returns one and a
    /// mutator's [`MutatePermit`] derefs to it. The read-vs-mutate split
    /// encodes the gate's closure check, **not** shared-vs-exclusive access:
    /// both permits are exclusive holds (a session read is not pure — a
    /// point-get miss does durable read-repair and publishes a cache fill,
    /// which KV4's fill-vs-write-through exclusion assumes runs under full
    /// mutual exclusion). [`SessionGate`] owns the conventional half of the
    /// contract: one acquire per public op, no re-acquire beneath it, same
    /// session.
    pub struct OpPermit<'a>(MutexGuard<'a, SessionPhase>);

    impl OpPermit<'_> {
        /// Whether the settle boundary has closed the session — consulted by
        /// [`CellSession::rollback`](super::CellSession::rollback), whose
        /// infallible contract answers a closed session with `NoOp` instead of
        /// an error.
        pub(crate) fn is_closed(&self) -> bool {
            matches!(*self.0, SessionPhase::Closed)
        }
    }

    /// A held gate permit that additionally witnesses a session **mutation**.
    ///
    /// Minted only through [`Self::witness`], from a held read permit once the
    /// caller has sequenced the mutator admission order (pin → closed →
    /// termination — see
    /// [`CellView::mutate_permit`](crate::state::descriptor)). The descriptor's
    /// mutating sinks (`raw_set`/`raw_clear`/`clear_section`/`raw_commit`)
    /// demand `&MutatePermit`, so "acquired too weakly" (a read permit at a
    /// write) does not compile. It [`Deref`]s to [`OpPermit`], so a mutator's
    /// one permit also witnesses the reads inside its body — the read-under-
    /// mutate grade subtyping is one-directional and deliberate: a read is
    /// legal under a mutate hold, but the converse (mutate under a read
    /// permit) is a type error. Uncontended unless a handler `join!`s its
    /// session ops.
    pub struct MutatePermit<'a>(OpPermit<'a>);

    impl<'a> MutatePermit<'a> {
        /// Wraps a held read permit as a mutation witness. The caller
        /// (`CellView::mutate_permit`) has already sequenced the
        /// pin/closed/termination admission checks under this same permit, so
        /// possessing a `MutatePermit` proves the session admitted the
        /// mutation.
        pub(in crate::state) fn witness(permit: OpPermit<'a>) -> Self {
            Self(permit)
        }
    }

    impl<'a> Deref for MutatePermit<'a> {
        type Target = OpPermit<'a>;

        fn deref(&self) -> &OpPermit<'a> {
            &self.0
        }
    }

    /// The message commit-marker identity: the dedup id the settlement
    /// boundary records through the commit oracle and the deduplication
    /// filter reads. A newtype so it cannot be confused with any other
    /// `Uuid` at the oracle-write signature.
    ///
    /// Two sources feed it, one accessor reads it
    /// ([`StateLifecycle::message_marker`]): a message session's own
    /// [`EventRef::Message`](crate::state::EventRef) dedup id, or the
    /// deferred-reload identity override
    /// ([`StateLifecycle::set_reload_marker`]) on a timer session.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct MessageMarker(Uuid);

    impl MessageMarker {
        /// Wraps a message's dedup id.
        #[must_use]
        pub(crate) fn new(dedup_id: Uuid) -> Self {
            Self(dedup_id)
        }

        /// The raw dedup id, for the oracle write and the dedup-store lookup.
        #[must_use]
        pub(crate) fn into_uuid(self) -> Uuid {
            self.0
        }
    }

    /// One collection's frozen settlement record: the provisional cells
    /// `finalize` staged (the ref carries the TTL) plus the frozen
    /// [`SectionClear`]s its event marker carries — what the receipt promotes
    /// or rolls back.
    ///
    /// Built exactly once per collection, at `finalize`, from the
    /// post-`commit()` dirty buffer — `commit()`-landed cells are already
    /// durably committed and never marker-listed, so the record lists exactly
    /// the cells recovery owns. Only a retry attempt re-running `finalize`
    /// rebuilds it, overwriting the same-event marker idempotently. Only
    /// `ReadCommitted` collections appear; `ReadUncommitted` writes resolve at
    /// stage time with nothing to settle. Survivors nest inside their
    /// [`SectionClear`] — never a parallel vector — so promote/rollback
    /// structurally cannot recompute them from live provisional rows. The
    /// record and the skinny durable marker payload deliberately do not
    /// merge: the marker persists coordinates (recovery rebuilds writes by
    /// point-read); the record holds the full [`ProvisionalWrite`]s for the
    /// inline promote/rollback. Each `(cell, write)`'s `data` is the value to
    /// promote to, `prev` the committed base to roll back to; the clears
    /// apply on the commit arm only (rollback needs no clear leg — the stage
    /// touched nothing destructive). A clears-only collection appears with an
    /// empty write set — the entry that arms the recovery backstop.
    // `Vec`, not `SmallVec`, deliberately: an entry is ~140 bytes of `Bytes`
    // handles and the receipt is held across the settle boundary's awaits, so
    // inline storage bloats every such future past clippy's `large_futures`
    // bound (measured +45 warnings); the `with_capacity` folds at the build
    // sites already bound the allocation.
    pub struct StagedCollection {
        pub(super) collection: CollectionRef,
        pub(super) writes: Vec<(CellKey, ProvisionalWrite)>,
        pub(super) clears: Vec<SectionClear>,
    }

    /// Whether `finalize` staged any provisional cells — and, when it did,
    /// the linear receipt that owns settling them. Mintable only by
    /// `finalize` (module-private fields, non-`Clone`), so possession of a
    /// [`StagedState`] proves a successful stage: apply-before-finalize,
    /// double-settle, and the recovery-delay of a never-staged event are
    /// unrepresentable.
    #[must_use]
    pub enum Finalized<S: CellStore> {
        /// Nothing staged: no collection was dirtied, or every dirty
        /// collection was `ReadUncommitted` and written resolved during
        /// `finalize`.
        Clean,

        /// At least one `ReadCommitted` collection staged; the boundary must
        /// arm the `StateRecovery` backstop and consume the receipt.
        Staged(StagedState<S>),
    }

    /// The linear settlement receipt for one event's staged cells. Consumed
    /// exactly once: [`Self::rollback`] before any marker record attempt, or
    /// [`Self::certify`] → [`Promotable::promote`] after the commit.
    #[must_use]
    pub struct StagedState<S: CellStore> {
        /// Clone of the partition's lower committed store (an `Arc`-backed
        /// handle).
        pub(super) store: S,
        pub(super) collections: Vec<StagedCollection>,
        /// The `recovery_delay` floor tightened once, at finalize, by the
        /// smallest `recovery_within` among the staged collections.
        pub(super) recovery_delay: CompactDuration,
    }

    impl<S: CellStore> StagedState<S> {
        /// Delay between this stage and its `StateRecovery` sweep.
        pub(crate) fn recovery_delay(&self) -> CompactDuration {
            self.recovery_delay
        }

        /// Rolls every staged cell back to its committed base (`prev`) after
        /// the event aborted. Best-effort: each collection is driven to
        /// completion regardless of siblings, per-collection failures warn,
        /// and anything left provisional is the armed sweep's (or
        /// first-touch's) to resolve — there is no caller decision to feed,
        /// so no outcome is returned.
        pub(crate) async fn rollback(self) {
            resolve_collections(&self.store, self.collections, false).await;
        }

        /// Certifies the stage for promotion — entering the marker record
        /// phase forfeits rollback. Before any record attempt, rolling back to
        /// the committed base is sound; after one it is not: a record
        /// write-timeout is ambiguous — the marker may be durable — so a
        /// rollback could erase a committed write that redelivery then
        /// dedup-filters away. In that window the staged cells stay
        /// provisional and the (already-armed) sweep resolves them through
        /// the oracle, which reads whether the marker landed. Consuming the
        /// receipt here makes the rule structural: a [`Promotable`] has no
        /// rollback.
        pub(crate) fn certify(self) -> Promotable<S> {
            Promotable(self)
        }
    }

    /// A certified stage: after the durability-marker commit, the one
    /// remaining move is [`Self::promote`].
    #[must_use]
    pub struct Promotable<S: CellStore>(StagedState<S>);

    impl<S: CellStore> Promotable<S> {
        /// Promotes the staged cells to their committed data (null
        /// `event`/`prev`, O(1) per cell) after the event committed; the
        /// commit arm also applies the frozen clears' gap erase. Best-effort:
        /// failures warn per collection and fold into
        /// [`ApplyOutcome::Incomplete`] (the backstop, always left armed,
        /// lets the sweep retry).
        pub(crate) async fn promote(self) -> ApplyOutcome {
            let StagedState {
                store, collections, ..
            } = self.0;
            if resolve_collections(&store, collections, true).await {
                ApplyOutcome::Resolved
            } else {
                ApplyOutcome::Incomplete
            }
        }
    }

    /// Result of consuming a [`Promotable`] with [`Promotable::promote`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    #[must_use]
    pub enum ApplyOutcome {
        /// Every staged cell promoted to its committed data.
        Resolved,

        /// At least one resolution failed. Recovery is guaranteed without any
        /// point-clear: the durability boundary never unschedules the per-key
        /// `StateRecovery` backstop (only the sweep's own fire clears it), so
        /// the standing backstop fires and the sweep retries; a
        /// transient sweep failure reschedules a fresh backstop, a
        /// permanent per-cell skip is left to first-touch and the key's
        /// next commit. The backstop aborts only on shutdown.
        Incomplete,
    }

    /// Framework-only lifecycle over a per-event session.
    pub trait StateLifecycle {
        /// The uniform durable cell store the session settles against —
        /// [`KeyedStateSession`](super::KeyedStateSession) projects its
        /// backend's store (`B::Cell`).
        type Cell: CellStore;

        /// The session's operation gate (KV4) — the descriptor handles acquire
        /// their per-op permits through this accessor. On the sealed trait,
        /// not public [`CellSession`](super::CellSession): the gate is
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
        ) -> impl Future<Output = Result<Finalized<Self::Cell>, StateAccessError>> + Send;

        /// Records `marker` through the commit oracle. Idempotent — the
        /// oracle write is a bare upsert — so the boundary retries failures
        /// freely. [`MarkerWrite`] is constructible only inside the
        /// settlement module, so this does not compile anywhere else.
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
        /// (never cleared at all — see [`Self::set_reload_marker`]). On the
        /// success path [`Self::finalize`] has already drained the buffer
        /// (the stage consumes it), making the scope-drop clear a no-op
        /// there; the receipt's promote/rollback read only receipt-owned
        /// data, never dirty.
        fn discard_dirty(&self);

        /// Flips this session terminated, synchronously and idempotently — the
        /// teardown half of
        /// [`CellSession::is_terminated`](super::CellSession::is_terminated).
        /// The [`EventStateScope`](crate::state::manager::EventStateScope)'s
        /// `Drop` calls it on every dispatch exit (including a future dropped
        /// mid-flight, where no gated teardown runs), and the panic-unwind
        /// catch calls it under the held closed gate. After it, every op on any
        /// clone of this session errors — reads/inits `Terminated`, and a
        /// current-pin mutation past the closed gate `SessionClosed` (the gate
        /// closes first in the catch). Writes no epoch: a genuinely-leaked
        /// stale clone stays fenced by its old pin regardless.
        fn terminate(&self);

        /// Whether this handle/session clone's pinned epoch still equals the
        /// live session epoch. `false` once a later attempt boundary
        /// ([`Self::reset`]) bumped it — the pin half of `ensure_live`, the
        /// mutator admission order, and `rollback`'s `NoOp` guard.
        fn attempt_current(&self) -> bool;

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

        /// The always-on `recovery_delay` floor (a plain config read). The
        /// per-event tightened delay lives on the receipt
        /// ([`StagedState::recovery_delay`], folded once at finalize); the
        /// floor is for the defensive arm after a permanent finalize failure,
        /// where no receipt exists.
        fn recovery_floor(&self) -> CompactDuration;

        /// The fire time of the `StateRecovery` backstop recorded as standing
        /// for this session's key, or `None` when none has been recorded this
        /// acquisition. `None` means *unknown*, not *unarmed*: the durable
        /// trigger store may still hold a prior epoch's backstop, which
        /// `arm_backstop` consults (and records here) before deciding.
        /// `arm_backstop` re-arms only when its new fire is sooner.
        fn backstop_armed(&self) -> impl Future<Output = Option<CompactDateTime>> + Send;

        /// Records that a `StateRecovery` backstop firing at `fire` now stands
        /// for this session's key (overwriting any earlier standing fire).
        fn mark_backstop_armed(&self, fire: CompactDateTime) -> impl Future<Output = ()> + Send;
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

    /// Partition-lifetime commit oracle; the settle boundary records the
    /// message commit row through it via `record_marker`. The same instance is
    /// baked into `cell`.
    pub oracle: B::Oracle,

    /// Opaque per-session capability slot a [`CellResolver`] reads at resolve
    /// time.
    ///
    /// [`CellResolver`]: crate::state::descriptor::CellResolver
    pub loader: L,

    /// Registered collection definitions and middleware-wide defaults.
    pub(crate) registry: Arc<CollectionDefRegistry>,

    /// Segment-qualified key this session's collections live under.
    pub state_key: StateKey,

    /// The event whose stages this session owns.
    pub event: EventRef,

    /// Delay between staging and the `StateRecovery` sweep.
    pub recovery_delay: CompactDuration,

    /// Per-partition set of keys with a standing `StateRecovery` backstop.
    pub armed: ArmedKeys,

    /// Termination signals captured at mint.
    pub termination: TerminationWatch,
}

/// The per-event attempt epoch. Bumped once per retry attempt boundary
/// ([`StateLifecycle::reset`](sealed::StateLifecycle::reset)); a
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
    oracle: B::Oracle,
    loader: L,
    registry: Arc<CollectionDefRegistry>,
    state_key: StateKey,
    event: EventRef,
    recovery_delay: CompactDuration,
    armed: ArmedKeys,
    termination: TerminationWatch,
    /// The deferred-reload identity override: the dedup id of the message
    /// the current dispatch loaded, on a timer session. Last-wins and never
    /// cleared — see [`sealed::StateLifecycle::set_reload_marker`]. Carries
    /// identity only; the commit decision lives in the settle boundary's
    /// typed classification, never in this cell's occupancy.
    reload_marker: SyncMutex<Option<MessageMarker>>,
    /// Session-owned termination flag, flipped synchronously by
    /// [`StateLifecycle::terminate`](sealed::StateLifecycle::terminate). It is
    /// the teardown half of [`is_terminated`](CellSession::is_terminated): the
    /// [`EventStateScope`](crate::state::manager::EventStateScope)'s `Drop`
    /// runs on every dispatch exit — including a future dropped mid-flight
    /// (task cancellation), where no other teardown runs — so a handle leaked
    /// past its event finds `is_terminated() == true`. Only the sender is kept;
    /// `borrow()` reads and `send_replace(true)` sets it, both fine with zero
    /// receivers.
    terminated: watch::Sender<bool>,
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
/// cross-event singletons. `B` is the per-partition [`StateBackend`] bundle;
/// `L` is the message loader.
pub struct KeyedStateSession<B, L>
where
    B: StateBackend,
{
    inner: Arc<SessionInner<B, L>>,
    /// This clone's pinned attempt epoch, copied verbatim by [`Clone`] and
    /// living OUTSIDE the shared `inner` so a leaked clone (or a clone of a
    /// clone) keeps its stale pin and can never re-pin itself. Only the
    /// crate-internal [`StateLifecycle::repin`](sealed::StateLifecycle::repin)
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
            oracle,
            loader,
            registry,
            state_key,
            event,
            recovery_delay,
            armed,
            termination,
        } = parts;
        Self {
            inner: Arc::new(SessionInner {
                overlay: Overlay::new(dirty, cell),
                oracle,
                loader,
                registry,
                state_key,
                event,
                recovery_delay,
                armed,
                termination,
                reload_marker: SyncMutex::new(None),
                terminated: watch::channel(false).0,
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

    /// The session's live attempt epoch. A one-line copy-out: the leaf
    /// `RwLock` read guard is dropped before returning, so it is never held
    /// across an `.await`.
    fn current_epoch(&self) -> AttemptEpoch {
        *self.inner.epoch.read()
    }

    /// Bumps the live attempt epoch to the next value. The **only** epoch
    /// writer, with exactly one call site: inside
    /// [`StateLifecycle::reset`](sealed::StateLifecycle::reset), under the
    /// held gate permit. Lock ordering is always gate → epoch (the gate is an
    /// async tokio mutex, this is a `parking_lot` leaf), so there is no
    /// sync lock-order cycle; the write guard is dropped before returning and
    /// never crosses an `.await`.
    fn bump_epoch(&self) {
        let mut epoch = self.inner.epoch.write();
        *epoch = epoch.next();
    }
}

impl<B, L> CellSession for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Loader = L;

    fn loader(&self) -> &L {
        &self.inner.loader
    }

    fn is_terminated(&self) -> bool {
        self.inner.termination.is_terminated() || *self.inner.terminated.borrow()
    }

    fn collection_has_ttl(&self, state_type: StateType, name: &StateName) -> bool {
        self.inner.registry.ttl_for(state_type, name).is_some()
    }

    fn collection_keyset_limit(&self, state_type: StateType, name: &StateName) -> usize {
        self.inner.registry.keyset_limit_for(state_type, name)
    }

    fn verify_state_registration(
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

    async fn get(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        let id = self.id_for(state_type, name);
        let committed = self
            .inner
            .overlay
            .get(&id, cell, self.inner.event)
            .await
            .map_err(|e| StateAccessError::store(&e))?;
        Ok(committed.into_inner())
    }

    fn scan<'a>(
        &'a self,
        state_type: StateType,
        name: &'a StateName,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        let id = self.id_for(state_type, name);
        let event = self.inner.event;
        // `id` is local to the generator, so `scan_cells` unifies its lifetime
        // with an owned overlay; the caller's `Copy` `Scan<'a>` rides in
        // directly (it is covariant, so it coerces to that shorter scope).
        let overlay = self.inner.overlay.clone();
        try_stream! {
            let inner = overlay.scan_cells(&id, scan, event);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateAccessError::store(&e))?;
            }
        }
    }

    async fn set(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
        value: &[u8],
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().set(&id, cell, value);
        Ok(())
    }

    async fn clear(
        &self,
        state_type: StateType,
        name: &StateName,
        cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear(&id, cell);
        Ok(())
    }

    async fn clear_section(
        &self,
        state_type: StateType,
        name: &StateName,
        section: Section,
    ) -> Result<(), StateAccessError> {
        let id = self.id_for(state_type, name);
        self.inner.overlay.dirty().clear_section(&id, section);
        Ok(())
    }

    async fn commit(
        &self,
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

    async fn rollback(&self, state_type: StateType, name: &StateName) -> StoreOutcome {
        // The gate acquire for rollback lives HERE, not in the handles (every
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

impl<B, L> StateLifecycle for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Cell = B::Cell;

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

    async fn finalize(&self) -> Result<Finalized<B::Cell>, StateAccessError> {
        let touched = self
            .inner
            .overlay
            .dirty()
            .touched(&self.inner.state_key.key);
        let event = self.inner.event;
        let registry = &self.inner.registry;
        let lower = self.inner.overlay.lower();
        let state_key = &self.inner.state_key;
        // Sized once to the touched-collection cardinality — the fold in
        // place of an unconstrained `try_collect` keeps the receipt's vector
        // from re-growing on the per-event hot path (bounded-allocation rule).
        let capacity = touched.len();
        let collections: Vec<StagedCollection> = stream::iter(touched)
            .map(|((state_type, name), cleared, cells)| {
                let id = CollectionId::new(state_key.clone(), state_type, name);
                // `cooperative` adds a per-collection coop-budget yield point
                // so a key touching many collections does not drain the batch
                // in one poll; `buffer_unordered` keeps full concurrency.
                cooperative(stage_collection(lower, registry, event, id, cleared, cells))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .try_fold(Vec::with_capacity(capacity), |mut acc, staged| async move {
                acc.extend(staged);
                Ok(acc)
            })
            .await?;
        // Drain-on-success (invariant owned by the `finalize` trait doc): one
        // whole-key clear, strictly after the per-collection aggregate — a
        // mid-stage failure exits via the `?` above with the buffer whole.
        // Nothing past this point is fallible or `.await`s a store.
        self.discard_dirty();
        if collections.is_empty() {
            return Ok(Finalized::Clean);
        }
        // Tighten the always-on floor by the smallest `recovery_within` among
        // the staged collections — folded exactly once, here, onto the
        // receipt. `recovery_within` on a `ReadUncommitted` collection is
        // inert: such collections never appear in the receipt.
        let recovery_delay = collections
            .iter()
            .filter_map(|staged| {
                let id = staged.collection.id();
                registry.recovery_within_for(id.state_type(), id.name())
            })
            .fold(self.inner.recovery_delay, CompactDuration::min);
        Ok(Finalized::Staged(StagedState {
            store: lower.clone(),
            collections,
            recovery_delay,
        }))
    }

    async fn record_marker(
        &self,
        marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> Result<(), StateAccessError> {
        self.inner
            .oracle
            .record_message(marker.into_uuid())
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    fn discard_dirty(&self) {
        // Sync and ungated (Drop paths cannot await). Precondition: by the
        // time this runs, either the settle boundary held the closed gate
        // permit (boundary paths), or the dispatch future completed and the
        // session gate's no-un-polled-ops contract holds (retry attempt
        // boundaries) — so no session op is in flight over this key's range.
        // This is a handler-cooperation contract, NOT enforced here: a task
        // that DETACHED a session clone (see `SessionGate`'s forbidden
        // patterns) can `set` after this clear and, at an attempt boundary
        // where the gate is Open, land its write in the next attempt's
        // transaction. Keep session ops inside the owning handler future.
        self.inner
            .overlay
            .dirty()
            .clear_event(&self.inner.state_key.key);
    }

    fn terminate(&self) {
        self.inner.terminated.send_replace(true);
    }

    fn attempt_current(&self) -> bool {
        self.pinned == self.current_epoch()
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

    fn recovery_floor(&self) -> CompactDuration {
        self.inner.recovery_delay
    }

    async fn backstop_armed(&self) -> Option<CompactDateTime> {
        self.inner
            .armed
            .read_async(&self.inner.state_key.key, |_, &fire| fire)
            .await
    }

    async fn mark_backstop_armed(&self, fire: CompactDateTime) {
        self.inner
            .armed
            .upsert_async(self.inner.state_key.key.clone(), fire)
            .await;
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
            // Read each touched cell's committed base concurrently: the
            // own-event committed read returns this event's `prev` while its
            // provisional cell stands, so a retry re-stages over the same base
            // (idempotent) — a `Set` cell in a cleared section keeps its
            // committed pre-clear `prev` this way. `cooperative` adds a
            // per-cell coop-budget yield point; `buffer_unordered` keeps full
            // concurrency. Reordering is irrelevant — the cells are distinct
            // coordinates landing in one same-partition batch. These reads are
            // all within this one collection (one shard), so they fan out
            // under the within-partition `SHARD_FANOUT_CONCURRENCY`, not the
            // cross-partition bound.
            // Sized once to the snapshot cardinality (the subsumed filter can
            // only shrink it) — bounded-allocation rule, the fold replacing an
            // unconstrained `try_collect`.
            let capacity = cells.len();
            let writes: Vec<(CellKey, ProvisionalWrite)> = stream::iter(
                cells
                    .into_iter()
                    .filter(|(cell, value)| !subsumed(cell, value)),
            )
            .map(|(cell, value)| {
                let data = value.into_data();
                cooperative(async move {
                    let prev = lower
                        .get(id, &cell, event)
                        .await
                        .map_err(|e| StateAccessError::store(&e))?;
                    Ok((cell, ProvisionalWrite::new(data, prev, event)))
                })
            })
            .buffer_unordered(SHARD_FANOUT_CONCURRENCY)
            .try_fold(Vec::with_capacity(capacity), |mut acc, write| async move {
                acc.push(write);
                Ok(acc)
            })
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
            // `finalize` return [`Finalized::Staged`] so the boundary arms
            // the `StateRecovery` backstop.
            let clears: Vec<SectionClear> = cleared
                .iter()
                .map(|&section| SectionClear::frozen(section, &writes))
                .collect();
            let marker = EventMarker::frozen(event, &writes, &clears);
            lower
                .write_provisional(&collection_ref, &writes, Some(&marker))
                .await
                .map_err(|e| StateAccessError::store(&e))?;
            Ok(Some(StagedCollection {
                collection: collection_ref,
                writes,
                clears,
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
            // RU never stages, so there is nothing for recovery to arm —
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

/// Resolves every staged collection after the event's outcome is known:
/// `committed` ⇒ promote each cell's `data` and apply the frozen clears' gap
/// erase (the cell store's
/// [`commit_provisional`](CellStore::commit_provisional) /
/// [`abort_provisional`](CellStore::abort_provisional) carry the projection so
/// the write-through cache can publish it), otherwise roll each back to its
/// `prev` (the abort arm ignores the clears — rollback needs no clear leg).
/// Best-effort: drives every per-collection resolution to completion
/// regardless of siblings, returning whether all resolved.
async fn resolve_collections<S>(
    store: &S,
    collections: Vec<StagedCollection>,
    committed: bool,
) -> bool
where
    S: CellStore,
{
    stream::iter(collections)
        .map(
            |StagedCollection {
                 collection,
                 writes,
                 clears,
             }| {
                cooperative(async move {
                    let result = if committed {
                        store.commit_provisional(&collection, &writes, &clears).await
                    } else {
                        store.abort_provisional(&collection, &writes).await
                    };
                    match result {
                        Ok(()) => true,
                        Err(error) => {
                            warn!(error = ?error, "cell resolution failed; leaving provisional for the sweep");
                            false
                        }
                    }
                })
            },
        )
        .buffer_unordered(STATE_FANOUT_CONCURRENCY)
        .fold(true, |all, ok| async move { all && ok })
        .await
}

/// Crate-private descriptor reaching a session through the one public
/// [`EventContext::state`] method — the sole state surface wrapper contexts
/// forward. Binding it yields the session itself (`Handle<S> = S`), so the
/// settlement boundary drives the full sealed [`StateLifecycle`] on it.
///
/// This is the **settlement surface**: `close_gate` / `finalize` /
/// `record_marker` / `discard_dirty` / `terminate` / the backstop accessors.
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
    type Handle<S: CellSession> = S;

    /// Returns the session itself — the lifecycle tunnel binds no typed handle
    /// and validates no registration; the boundary drives the sealed
    /// [`StateLifecycle`] on the returned session.
    fn bind<S: CellSession>(self, session: &S) -> Result<S, StateAccessError> {
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
/// [`MarkerIdentity`](sealed::MarkerIdentity) audiences (defer-reload set,
/// dedup read, settle read). Wraps a session clone but exposes **only** the
/// two marker methods — never the raw session, so it cannot reach the
/// settlement surface. This is the tunnel-narrowing enforcement: dedup and
/// defer-reload bind [`MarkerAccess`] and get one of these, so they cannot
/// import `sealed::StateLifecycle` and call `close_gate` / `finalize`.
pub(crate) struct MarkerHandle<S>(S);

impl<S: MarkerIdentity> MarkerHandle<S> {
    /// Sets the deferred-reload identity override — see
    /// [`MarkerIdentity::set_reload_marker`](sealed::MarkerIdentity::set_reload_marker).
    pub(crate) fn set_reload_marker(&self, marker: MessageMarker) {
        self.0.set_reload_marker(marker);
    }

    /// The message commit-marker identity — see
    /// [`MarkerIdentity::message_marker`](sealed::MarkerIdentity::message_marker).
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
    type Handle<S: CellSession> = MarkerHandle<S>;

    /// Wraps the session in a [`MarkerHandle`], validating no registration —
    /// the marker tunnel carries no typed collection.
    fn bind<S: CellSession>(self, session: &S) -> Result<MarkerHandle<S>, StateAccessError> {
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
