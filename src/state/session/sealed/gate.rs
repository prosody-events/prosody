//! The session gate and the permits that serialize session operations.

use super::Duration;
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
/// and the map or set keyset updates under concurrent member writes.
///
/// **A stream acquires the gate at init and once per chunk**, each permit
/// dropped before the next; every other public op acquires it once for its
/// whole body. Nothing beneath a public wrapper re-acquires while holding —
/// a tokio `Mutex` is not reentrant, so an internal re-acquire is a
/// deadlock the KV4 pins would surface as a hang.
///
/// **Streams hold the gate only per chunk (`StreamYieldFree`).** A
/// point-get stream covers a sub-threshold deque window, or a `Tracked` map
/// or set keyset within its bound. It takes the gate for its init
/// metadata read (the map or set keyset cell, or the deque window cell)
/// and releases it. It then fetches the listed entries in gate-scoped
/// chunks: one permit per chunk, and ONE batch read each. Only the init
/// metadata read stays a point read. One permit covers a chunk's fetch,
/// decode, and resolve. The chunk future's scope drops that permit
/// before any of the chunk's items reach user code. The permit is
/// therefore **never held across a yield to user code, for items and
/// errors alike**.
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
/// * **handle ops** (`get`/`set`/`clear`/…) — the pin compare in `ensure_live`
///   / `mutate_permit`'s ordered admission;
/// * **apply-hook mutations** past the settle window — the closed gate and
///   attempt teardown;
/// * **scans and streams** — the managed stream fence adapter (`fenced` in
///   `crate::state::collection::stream`), which runs the engine fence after
///   every stream completion, so a leaked stream errors at its next emission
///   and no buffered item crosses the boundary.
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
