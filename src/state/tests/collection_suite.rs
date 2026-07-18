//! Trace + model-oracle property suites for the Map and Deque collections.
//!
//! Each runner drives a generated multi-event trace through the **real**
//! [`KeyedStateSession`] lifecycle — handler ops buffer into the dirty overlay,
//! `finalize` stages them in one co-stamped batch, then the event commits
//! (promote), aborts (rollback), or crashes (a fresh store over the same warm
//! `MemoryCells` recovers through the quiescence sweep). After every event the
//! collection's observable state must equal a plain `VecDeque`/`BTreeMap` model
//! — and intermediate `pop`/`get` return values are asserted as they happen, so
//! a mutation that corrupts the return but heals the final shape is still
//! caught. This single property proves dense-window / exact-keyset invariants,
//! key/positional ordering, containment, whole-collection `Clear` (in-event
//! emptiness, survivor repopulation, abort exactness), and the
//! keyset-and-entries-promote-together crash atomicity. The lifecycle
//! properties run in **both** commit modes: `ReadCommitted` settles along the
//! outcome, `ReadUncommitted` commits everything at `finalize` regardless of
//! the outcome.
//!
//! Both op alphabets include the mid-handler `Commit`: the runner
//! snapshots the scratch model at each `commit()`, and on a non-committing
//! outcome the committed read-back must equal the last `commit()`-landed
//! snapshot — `commit()`-landed ops (entries *and* bookkeeping cells, as one
//! batch) survive abort and crash-rollback while post-commit ops still roll
//! back, the at-least-once `commit()` contract. A commit-then-clear-then-abort
//! trace therefore pins that abort restores the `commit()`-landed state, never
//! the pre-event state.
//!
//! Memory-backed only: the `cell_suite` runners already prove memory ↔
//! Cassandra parity for the underlying store, and the collection logic lives
//! entirely in the descriptor layer above it.

use super::cell_suite::{MAX_TRACE_OPS, ScriptedOracle, capped_vec};
use super::support::assert_no_settlement_residue;
use crate::codec::{Codec, JsonCodec};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::descriptor::map::keyset_cell;
use crate::state::descriptor::{
    DequeHandle, MapHandle, StateDescriptor, deque, deque_state, map_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::manager::ArmedKeys;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::resolve::sweep_provisional;
use crate::state::session::sealed::{ApplyOutcome, StateLifecycle};
use crate::state::session::{
    CellSession, Finalized, KeyedStateSession, SessionParts, TerminationWatch,
};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, CommitMode, Direction, EventRef, PartitionBackend, StateKey,
    StateName, StateType,
};
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Display;
use std::future::Future;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;
use uuid::Uuid;

/// The interleave pins' hang-guard: the ONLY deadline in
/// [`run_map_stream_interleave`] / [`run_deque_stream_interleave`], and never
/// an assertion — a legal interleaving completes instantly, so this fires only
/// when the stream holds the gate across a yield (the `StreamYieldFree`
/// violation it guards against).
const INTERLEAVE_HANG_GUARD: Duration = Duration::from_secs(30);

/// Seed size for the interleave pins — spans more than one point-get chunk
/// (`> 16`) and stays under `DEQUE_POINT_ITERATION_MAX` (128), so both
/// collections take the chunked point-get arm.
const INTERLEAVE_SEED: usize = 20;

/// The per-partition backend for the suites: a memory cell store and a scripted
/// commit oracle, behind the standard [`PartitionBackend`] bundle.
type SuiteBackend = PartitionBackend<
    ScriptedOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<ScriptedOracle>,
>;

/// The real per-event session the handles bind over.
type SuiteSession = KeyedStateSession<SuiteBackend, MemoryLoader<Value>>;

/// The bounded key space the Map trace ranges over — small and spanning the
/// sign boundary so re-inserts, removes, and ordered scans across negative and
/// positive `i64` keys all occur.
const KEY_POOL: [i64; 5] = [-2, -1, 0, 1, 2];

/// Max ops per event, keeping each event's batch small while the trace as a
/// whole still grows and drains the collection.
const MAX_EVENT_OPS: usize = 4;

/// How an event resolved. Weighted toward `Commit` so state accumulates, with
/// real coverage of the rollback and crash-recovery arms.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Marker recorded, promoted inline.
    Commit,
    /// No marker, rolled back inline.
    Abort,
    /// Staged, marker recorded, crash → sweep promotes.
    CrashCommitted,
    /// Staged, no marker, crash → sweep rolls back.
    CrashAborted,
}

impl Outcome {
    /// Whether the event's writes become committed.
    fn commits(self) -> bool {
        matches!(self, Self::Commit | Self::CrashCommitted)
    }
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[
            Self::Commit,
            Self::Commit,
            Self::Commit,
            Self::Abort,
            Self::Abort,
            Self::CrashCommitted,
            Self::CrashAborted,
        ])
        .copied()
        .unwrap_or(Self::Commit)
    }
}

/// What one applied op observed: keep going, a mid-handler `commit()` landed
/// (the runner snapshots the scratch model as immediately durable), or a
/// return value diverged from the model (property failure).
enum OpOutcome {
    Continue,
    Committed,
    Mismatch,
}

/// One deque mutation. Payloads are single `u8`s wrapped as JSON numbers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DequeOp {
    PushBack(u8),
    PushFront(u8),
    PopBack,
    PopFront,
    Clear,
    Commit,
}

impl Arbitrary for DequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => Self::PushBack(u8::arbitrary(g)),
            1 => Self::PushFront(u8::arbitrary(g)),
            2 => Self::PopBack,
            3 => Self::PopFront,
            4 => Self::Clear,
            _ => Self::Commit,
        }
    }
}

/// One map mutation, mid-trace read, whole-map clear, or mid-handler
/// `commit()` over the bounded key pool.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MapOp {
    Set(i64, u8),
    Remove(i64),
    Get(i64),
    Clear,
    Commit,
}

impl Arbitrary for MapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = g.choose(&KEY_POOL).copied().unwrap_or(0);
        match u8::arbitrary(g) % 6 {
            0 | 1 => Self::Set(key, u8::arbitrary(g)),
            2 => Self::Remove(key),
            3 => Self::Get(key),
            4 => Self::Clear,
            _ => Self::Commit,
        }
    }
}

/// One event: a batch of ops and its resolution.
#[derive(Clone, Debug)]
struct Event<O> {
    ops: Vec<O>,
    outcome: Outcome,
}

impl<O: Arbitrary> Arbitrary for Event<O> {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_EVENT_OPS),
            outcome: Outcome::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        Box::new(self.ops.shrink().map(move |ops| Self { ops, outcome }))
    }
}

/// A shrinkable trace of events.
#[derive(Clone, Debug)]
pub(crate) struct Trace<O> {
    events: Vec<Event<O>>,
}

impl<O: Arbitrary> Arbitrary for Trace<O> {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// A deque trace.
pub(crate) type DequeTrace = Trace<DequeOp>;

/// A map trace.
pub(crate) type MapTrace = Trace<MapOp>;

/// The bounded window width the deque-holes property ranges over.
const MAX_DEQUE_WINDOW: usize = 8;

/// The head-index pool for the deque-holes property — small and spanning the
/// sign boundary so windows crossing zero are exercised.
const HEAD_POOL: [i64; 7] = [-3, -2, -1, 0, 1, 2, 3];

/// A directly-seeded sparse deque window: a `head` index and a run of per-index
/// cells — `Some(v)` present, `None` a hole (a TTL-expired entry not yet
/// swept). `tail = head + cells.len()`. Seeded straight into the store (never
/// produced by the handle, which keeps the window dense) to pin the TTL'd-hole
/// read contract: `len` an upper bound, `get`/`stream` skip holes without
/// error.
#[derive(Clone, Debug)]
pub(crate) struct DequeHoles {
    head: i64,
    cells: Vec<Option<u8>>,
}

impl Arbitrary for DequeHoles {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            head: g.choose(&HEAD_POOL).copied().unwrap_or(0),
            cells: capped_vec(g, MAX_DEQUE_WINDOW),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let head = self.head;
        Box::new(self.cells.shrink().map(move |cells| Self { head, cells }))
    }
}

/// Builds a fresh session for one event over the shared warm backing. Dropped
/// senders are fine — `watch::Receiver::borrow` keeps returning the last value,
/// so the session reads as non-terminated.
fn make_session(
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
    event: EventRef,
) -> SuiteSession {
    make_session_with_dirty(
        cells,
        oracle,
        registry,
        state_key,
        armed,
        event,
        Arc::default(),
    )
}

/// [`make_session`] over a caller-owned dirty workspace, so a test can snapshot
/// the per-event buffered cells (the Map TTL keyset-refresh property inspects
/// what `finalize` will stage through it).
fn make_session_with_dirty(
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    armed: &ArmedKeys,
    event: EventRef,
    dirty: Arc<DirtyStore>,
) -> SuiteSession {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<SuiteBackend, _> {
        cell: MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone()),
        dirty,
        oracle: oracle.clone(),
        loader: MemoryLoader::new(),
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        recovery_delay: CompactDuration::new(30),
        armed: armed.clone(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    })
}

/// Registers `name` under `def` and returns the shared registry plus the sweep
/// ref.
fn registry_and_ref<D>(
    descriptor: &D,
    name: &str,
    state_key: &StateKey,
    def: CollectionDef,
) -> Result<(Arc<CollectionDefRegistry>, CollectionRef)>
where
    D: StateDescriptor,
{
    let mut registry = CollectionDefRegistry::default();
    registry.register(descriptor, def)?;
    let collection_ref = CollectionRef::new(
        CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new(name)?,
        ),
        None,
    );
    Ok((Arc::new(registry), collection_ref))
}

/// Resolves the event along its outcome's path: consume the `finalize`
/// receipt inline (promote/rollback), or crash → fresh store → sweep.
/// Returns `false` only if a sweep strands a cell.
async fn resolve_event(
    session: SuiteSession,
    finalized: Finalized<MemoryCellStore<ScriptedOracle>>,
    outcome: Outcome,
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    collection_ref: &CollectionRef,
) -> Result<bool> {
    match outcome {
        Outcome::Commit => {
            if let Finalized::Staged(staged) = finalized
                && staged.certify().promote().await != ApplyOutcome::Resolved
            {
                return Err(eyre!("promote incomplete on a healthy store"));
            }
        }
        Outcome::Abort => {
            if let Finalized::Staged(staged) = finalized {
                staged.rollback().await;
            }
        }
        Outcome::CrashCommitted | Outcome::CrashAborted => {
            // Dropping the receipt (and session) IS the crash: the durable
            // staged cells and the oracle survive; the in-memory record dies.
            drop(finalized);
            drop(session);
            // A cold store over the same warm backing — exactly a restart.
            let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
            if !sweep_provisional(&store, oracle, collection_ref)
                .await
                .map_err(|e| eyre!("sweep: {e}"))?
            {
                return Ok(false);
            }
        }
    }
    // Every outcome in this runner's alphabet settles fully (promote and
    // rollback delete the marker; the sweep resolves it), so no settlement
    // residue may remain — checked raw, before the resolving read-back below
    // heals a skipped settle to identical bytes and masks it.
    assert_no_settlement_residue(cells, collection_ref.id())?;
    Ok(true)
}

/// Settles one committed event end-to-end for a directed pin: `finalize`,
/// record the event's marker, then consume the receipt with
/// `certify().promote()` — erroring if a healthy store reports `Incomplete`,
/// and asserting the promote left no settlement residue in `cells` (the raw
/// probe a resolving read cannot make; see [`assert_no_settlement_residue`]).
/// A `Clean` finalize just records the marker (nothing to promote).
pub(crate) async fn finalize_and_promote<L>(
    session: &L,
    oracle: &ScriptedOracle,
    dedup_id: Uuid,
    cells: &MemoryCells,
    collection: &CollectionId,
) -> Result<()>
where
    L: StateLifecycle,
{
    let finalized = session
        .finalize()
        .await
        .map_err(|e| eyre!("finalize: {e}"))?;
    oracle
        .record_message(dedup_id)
        .await
        .map_err(|e| eyre!("marker: {e}"))?;
    if let Finalized::Staged(staged) = finalized {
        if staged.certify().promote().await != ApplyOutcome::Resolved {
            bail!("promote incomplete on a healthy store");
        }
        assert_no_settlement_residue(cells, collection)?;
    }
    Ok(())
}

/// The warm backing shared across a trace's events, handed to the read-back
/// assertion so a kind whose invariant needs the raw cells (Map's
/// `KeysetPresence` check) can reach them.
struct Backing<'a> {
    cells: &'a MemoryCells,
    state_key: &'a StateKey,
}

/// Drives a generated trace through the real [`KeyedStateSession`] lifecycle
/// for any collection kind — the Deque and Map suites differ only in the op
/// alphabet, the model, and the assertions, so everything else lives here once:
/// bind a fresh session per event, apply each op (`apply_op`, which also
/// asserts mid-trace `pop`/`get` returns and reports mid-handler commits),
/// `finalize`, resolve along the event's outcome (promote / rollback / crash →
/// sweep), advance the model — the full scratch on a commit (or always, for a
/// `ReadUncommitted` collection, whose `finalize` commits everything), the
/// last `commit()`-landed snapshot otherwise — then assert the committed
/// collection through a fresh read-back session (`assert`, which absorbs any
/// kind-specific check such as Map's `KeysetPresence`).
async fn run_collection_trace<D, O, M, Apply, Assert>(
    trace: Trace<O>,
    descriptor: D,
    name: &str,
    def: CollectionDef,
    apply_op: Apply,
    assert: Assert,
) -> Result<bool>
where
    D: StateDescriptor,
    O: Copy,
    M: Clone + Default,
    Apply: AsyncFn(&D::Handle<SuiteSession>, O, &mut M) -> Result<OpOutcome>,
    Assert: AsyncFn(&D::Handle<SuiteSession>, &M, &Backing<'_>) -> Result<bool>,
{
    let commit_mode = def.commit_mode;
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let (registry, collection_ref) = registry_and_ref(&descriptor, name, &state_key, def)?;
    let armed: ArmedKeys = Arc::default();
    let backing = Backing {
        cells: &cells,
        state_key: &state_key,
    };
    let mut model = M::default();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        let mut scratch = model.clone();
        // The scratch as of the last mid-handler `commit()` — durable
        // regardless of the event's outcome (the at-least-once `commit()`
        // contract).
        let mut commit_floor: Option<M> = None;
        for op in &ev.ops {
            match apply_op(&handle, *op, &mut scratch).await? {
                OpOutcome::Continue => {}
                OpOutcome::Committed => commit_floor = Some(scratch.clone()),
                OpOutcome::Mismatch => return Ok(false),
            }
        }

        let finalized = session
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?;
        if ev.outcome.commits() {
            oracle
                .record_message(event_dedup(event))
                .await
                .map_err(|e| eyre!("marker: {e}"))?;
        }
        if !resolve_event(
            session,
            finalized,
            ev.outcome,
            &cells,
            &oracle,
            &registry,
            &collection_ref,
        )
        .await?
        {
            return Ok(false);
        }
        model = if commit_mode == CommitMode::ReadUncommitted || ev.outcome.commits() {
            // ReadUncommitted commits everything at `finalize` — any outcome
            // that reached it takes the full scratch (nothing provisional
            // exists to roll back or sweep).
            scratch
        } else {
            // Abort / crash-rollback revert only the post-commit
            // provisionals (their `prev` was captured after the `commit()`
            // landed); the `commit()`-landed snapshot is already committed.
            commit_floor.unwrap_or(model)
        };

        // Read back through a fresh session (clean overlay) — pure committed.
        let read = make_session(
            &cells,
            &oracle,
            &registry,
            &state_key,
            &armed,
            read_event(index),
        );
        let read_handle = descriptor
            .bind(&read)
            .map_err(|e| eyre!("bind read: {e}"))?;
        if !assert(&read_handle, &model, &backing).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Drives a deque trace, asserting the handle equals a `VecDeque` model after
/// every event and that each `pop` returns the model's value.
pub(crate) async fn run_deque_trace(trace: DequeTrace, commit_mode: CommitMode) -> Result<bool> {
    run_collection_trace(
        trace,
        deque_state::<JsonCodec>("dq"),
        "dq",
        CollectionDef {
            commit_mode,
            ..CollectionDef::new(None)
        },
        async |handle, op, scratch: &mut VecDeque<Value>| match op {
            DequeOp::PushBack(b) => {
                let v = Value::from(b);
                handle.push_back(v.clone()).await?;
                scratch.push_back(v);
                Ok(OpOutcome::Continue)
            }
            DequeOp::PushFront(b) => {
                let v = Value::from(b);
                handle.push_front(v.clone()).await?;
                scratch.push_front(v);
                Ok(OpOutcome::Continue)
            }
            DequeOp::PopBack => Ok(mismatch_unless(
                handle.pop_back().await? == scratch.pop_back(),
            )),
            DequeOp::PopFront => Ok(mismatch_unless(
                handle.pop_front().await? == scratch.pop_front(),
            )),
            DequeOp::Clear => {
                handle.clear().await?;
                scratch.clear();
                Ok(OpOutcome::Continue)
            }
            DequeOp::Commit => {
                handle.commit().await?;
                Ok(OpOutcome::Committed)
            }
        },
        async |handle, model, _backing: &Backing<'_>| assert_deque(handle, model).await,
    )
    .await
}

/// Drives a map trace, asserting the handle equals a `BTreeMap` model after
/// every event, that each mid-trace `get` returns the model's value and
/// `contains_key` agrees with it, and that `KeysetPresence` holds (any live
/// entry implies a present keyset cell).
pub(crate) async fn run_map_trace(trace: MapTrace, commit_mode: CommitMode) -> Result<bool> {
    run_collection_trace(
        trace,
        map_state::<I64KeyCodec, JsonCodec>("mp"),
        "mp",
        // A small keyset limit under the 5-key pool, so generated traces cross
        // Tracked → Overflowed, hit the already-tracked fast path, and
        // interleave removes/clears — while the BTreeMap model stays oblivious
        // to the keyset's existence (the property proves it cannot tell).
        CollectionDef {
            commit_mode,
            keyset_limit: 3,
            ..CollectionDef::new(None)
        },
        async |handle, op, scratch: &mut BTreeMap<i64, Value>| match op {
            MapOp::Set(k, b) => {
                let v = Value::from(b);
                handle.set(k, v.clone()).await?;
                scratch.insert(k, v);
                Ok(OpOutcome::Continue)
            }
            MapOp::Remove(k) => {
                handle.remove(&k).await?;
                scratch.remove(&k);
                Ok(OpOutcome::Continue)
            }
            MapOp::Get(k) => {
                let got = handle.get(&k).await?;
                let present = handle.contains_key(&k).await?;
                Ok(mismatch_unless(
                    got == scratch.get(&k).cloned() && present == got.is_some(),
                ))
            }
            MapOp::Clear => {
                handle.clear().await?;
                scratch.clear();
                Ok(OpOutcome::Continue)
            }
            MapOp::Commit => {
                handle.commit().await?;
                Ok(OpOutcome::Committed)
            }
        },
        async |handle, model, backing: &Backing<'_>| {
            Ok(assert_map(handle, model).await?
                && assert_keyset_present(backing.cells, backing.state_key, model)?)
        },
    )
    .await
}

/// Map TTL keyset-refresh (what `finalize` stages): on a collection **with a
/// TTL**, every `set` buffers the keyset cell — even a re-set of an
/// already-tracked key, and even once the map has overflowed — so its TTL is
/// refreshed and the keyset outlives every entry. Runs multiple committed
/// events over a fresh per-event dirty workspace so a later set lands over a
/// *committed* keyset: the case a single-event snapshot cannot reach, because
/// the first set always seeds the keyset into the dirty overlay, masking a
/// suppressed-refresh regression on the no-write fast paths (already-tracked
/// and `Overflowed`, both reached with pool 5 / limit 3).
pub(crate) async fn run_map_ttl_keyset_refresh_trace(trace: MapTrace) -> Result<bool> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(Some(CompactDuration::new(3_600)))
        },
    )?;
    let registry = Arc::new(registry);
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    let armed: ArmedKeys = Arc::default();
    let keyset_cell = keyset_cell();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let dirty = Arc::new(DirtyStore::new());
        let session = make_session_with_dirty(
            &cells,
            &oracle,
            &registry,
            &state_key,
            &armed,
            event,
            dirty.clone(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for op in &ev.ops {
            match *op {
                MapOp::Set(k, b) => {
                    handle.set(k, Value::from(b)).await?;
                    // Snapshot immediately, before any later Commit drains
                    // dirty: a TTL'd set always buffers the keyset cell.
                    let snapshot = dirty.collection_snapshot(&id);
                    if !snapshot.iter().any(|(c, _)| *c == keyset_cell) {
                        return Ok(false);
                    }
                }
                MapOp::Remove(k) => handle.remove(&k).await?,
                MapOp::Get(k) => {
                    handle.get(&k).await?;
                }
                MapOp::Clear => handle.clear().await?,
                MapOp::Commit => {
                    handle.commit().await?;
                }
            }
        }
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, &id).await?;
    }
    Ok(true)
}

/// Keyset exactness (no TTL): over an arbitrary committed
/// set/remove/get/clear/commit trace on a map whose `keyset_limit` (8) exceeds
/// the 5-key pool — so the frame never overflows on count, and i64 coordinates
/// (12 bytes each) stay far under the 64 KiB ceiling — the stored keyset
/// decodes to **exactly** the live key set after every settled event. Because
/// `remove` subtracts, a superset can never survive a removal (the pre-keyset
/// design's loose superset would fail here). An absent keyset counts as the
/// empty set (a fresh or `clear`ed map); a removed-to-empty map instead holds
/// the empty `Tracked` frame — both are the live-empty case.
pub(crate) async fn run_map_keyset_exact_trace(trace: MapTrace) -> Result<bool> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 8,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let armed: ArmedKeys = Arc::default();
    let mut model: BTreeMap<i64, Value> = BTreeMap::new();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for op in &ev.ops {
            match *op {
                MapOp::Set(k, b) => {
                    let v = Value::from(b);
                    handle.set(k, v.clone()).await?;
                    model.insert(k, v);
                }
                MapOp::Remove(k) => {
                    handle.remove(&k).await?;
                    model.remove(&k);
                }
                MapOp::Get(k) => {
                    handle.get(&k).await?;
                }
                MapOp::Clear => {
                    handle.clear().await?;
                    model.clear();
                }
                MapOp::Commit => {
                    handle.commit().await?;
                }
            }
        }
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, id).await?;

        // The committed keyset must be exactly the live key set: a present
        // frame must equal `tracked_frame(live)`; an absent keyset is the
        // live-empty case (a fresh or `clear`ed map).
        let live: Vec<i64> = model.keys().copied().collect();
        let stored = store
            .get(id, &keyset_cell(), read_event(index))
            .await?
            .into_inner();
        let exact = match stored {
            Some(bytes) => bytes[..] == tracked_frame(&live)[..],
            None => model.is_empty(),
        };
        if !exact {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Population pool for the `Map::get_many` parity property — 20 distinct keys,
/// enough content to distinguish present, absent, and duplicate answers. Query
/// keys at/above it (see below) are guaranteed absent.
const GET_MANY_POPULATE_KEYS: u8 = 20;
/// Query-key range `[lo, hi)`: it spans below `0` and at/above the populate
/// pool, so keys `< 0` and `>= 20` are guaranteed absent (never set), forcing
/// the absent-key path.
const GET_MANY_QUERY_LO: i64 = -4;
const GET_MANY_QUERY_HI: i64 = 24;
/// Max query-list length — `> CELL_BATCH` so multi-sub-batch calls occur often.
const GET_MANY_MAX_QUERIES: usize = 40;

/// A random map population plus a random query list for the `Map::get_many`
/// parity property. The independent pools guarantee absent keys; a small query
/// range over a long list guarantees duplicates; the length cap crosses
/// `CELL_BATCH`, so a single call spans more than one sub-batch. `commit`
/// selects the read arm: the same event's dirty overlay, or a fresh event over
/// the committed base.
#[derive(Clone, Debug)]
pub(crate) struct MapGetManyInput {
    entries: Vec<(i64, u8)>,
    queries: Vec<i64>,
    commit: bool,
}

impl Arbitrary for MapGetManyInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let entries = capped_vec::<(u8, u8)>(g, 24)
            .into_iter()
            .map(|(k, b)| (i64::from(k % GET_MANY_POPULATE_KEYS), b))
            .collect();
        let span = GET_MANY_QUERY_HI - GET_MANY_QUERY_LO;
        let qlen = usize::arbitrary(g) % (GET_MANY_MAX_QUERIES + 1);
        let queries = (0..qlen)
            .map(|_| GET_MANY_QUERY_LO + i64::arbitrary(g).rem_euclid(span))
            .collect();
        Self {
            entries,
            queries,
            commit: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let commit = self.commit;
        let entries = self.entries.clone();
        Box::new(self.queries.shrink().map(move |queries| Self {
            entries: entries.clone(),
            queries,
            commit,
        }))
    }
}

/// `Map::get_many(keys)` parity against the already-trusted point path:
/// `get_many(queries)` answers each position exactly as `queries.map(get)`,
/// including duplicate, present, and absent keys, across the sub-batch
/// boundary. No TTL is in play and the JSON identity resolver is deterministic,
/// so the observation rules collapse to exact point-parity and this isolates
/// the batch plumbing (coordinate lowering, dedupe/scatter, sub-batch
/// concatenation, and ordered `buffered` resolution).
/// Proven over both the dirty-overlay arm (uncommitted) and the committed arm.
pub(crate) async fn run_map_get_many_parity_trace(input: MapGetManyInput) -> Result<bool> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();

    // Event 0: populate.
    let ev0 = EventRef::Message {
        dedup_id: Uuid::from_u128(0),
    };
    let session0 = make_session(&cells, &oracle, &registry, &state_key, &armed, ev0);
    let handle0 = descriptor.bind(&session0).map_err(|e| eyre!("bind: {e}"))?;
    for (k, b) in &input.entries {
        handle0.set(*k, Value::from(*b)).await?;
    }

    // Read arm: the same (dirty) session, or a fresh event after committing 0.
    let batch;
    let mut point = Vec::with_capacity(input.queries.len());
    if input.commit {
        finalize_and_promote(&session0, &oracle, event_dedup(ev0), &cells, id).await?;
        let ev1 = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session1 = make_session(&cells, &oracle, &registry, &state_key, &armed, ev1);
        let handle1 = descriptor.bind(&session1).map_err(|e| eyre!("bind: {e}"))?;
        batch = Box::pin(handle1.get_many(&input.queries)).await?;
        for q in &input.queries {
            point.push(handle1.get(q).await?);
        }
    } else {
        batch = Box::pin(handle0.get_many(&input.queries)).await?;
        for q in &input.queries {
            point.push(handle0.get(q).await?);
        }
    }

    // Alignment is part of the contract: one answer per input position.
    if batch.len() != input.queries.len() {
        return Ok(false);
    }
    Ok(batch == point)
}

/// Deque TTL-hole read contract: over a directly-seeded sparse window, `len`
/// is the full span `tail − head` (an upper bound on the live count), `get`
/// returns `None` at a hole and past the span, and both stream directions yield
/// exactly the present values in index order (ascending forward, reversed
/// backward) without error. Seeded directly — never via wall-clock TTL — the
/// only way to reach a holed window the handle itself never produces.
pub(crate) async fn run_deque_holes(shape: DequeHoles) -> Result<bool> {
    use bytes::Bytes;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    let head = shape.head;
    let tail = head + shape.cells.len() as i64;
    store
        .write_resolved(
            &collection_ref,
            &[(
                deque::meta_cell(),
                Some(Bytes::from(deque::seed_frame(head, tail))),
            )],
            &[],
        )
        .await?;
    for (i, cell) in shape.cells.iter().enumerate() {
        if let Some(value) = cell {
            let coordinate = I64KeyCodec::encode(&(head + i as i64));
            let bytes = Bytes::from(serde_json::to_vec(&Value::from(*value))?);
            store
                .write_resolved(
                    &collection_ref,
                    &[(deque::entry_cell_for(&coordinate), Some(bytes))],
                    &[],
                )
                .await?;
        }
    }

    let armed: ArmedKeys = Arc::default();
    let session = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    let len = shape.cells.len();
    if handle.len().await? != len {
        return Ok(false);
    }
    for p in 0..len + 2 {
        let expected = shape.cells.get(p).copied().flatten().map(Value::from);
        if handle.get(p).await? != expected {
            return Ok(false);
        }
    }
    let present: Vec<Value> = shape
        .cells
        .iter()
        .copied()
        .filter_map(|c| c.map(Value::from))
        .collect();
    if collect_deque(&handle, Direction::Forward).await? != present {
        return Ok(false);
    }
    let reversed: Vec<Value> = present.iter().rev().cloned().collect();
    Ok(collect_deque(&handle, Direction::Backward).await? == reversed)
}

/// `KeysetPresence` (Map): whenever the map holds any live entry, the raw
/// keyset cell is physically present. Probed directly from the stored
/// coordinate set (not the resolving handle, which can never synthesize a
/// missing physical row), so it proves the keyset *cell* exists — the invariant
/// on which `stream`'s `Absent → Empty` fast path rests. An empty model is
/// vacuously true (the converse is not an invariant: a present keyset over an
/// empty map is legal).
fn assert_keyset_present(
    cells: &MemoryCells,
    state_key: &StateKey,
    model: &BTreeMap<i64, Value>,
) -> Result<bool> {
    if model.is_empty() {
        return Ok(true);
    }
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    Ok(cells.stored_coordinates(&id).contains(&keyset_cell()))
}

/// Asserts a deque handle equals the model: `len`, `is_empty`, both stream
/// directions (front-to-back for `Forward`, back-to-front for `Backward`), and
/// `get` at every position (including out of range → `None`).
async fn assert_deque<S, C>(handle: &DequeHandle<S, C>, model: &VecDeque<Value>) -> Result<bool>
where
    S: CellSession,
    C: Codec<Payload = Value>,
{
    if handle.len().await? != model.len() || handle.is_empty().await? != model.is_empty() {
        return Ok(false);
    }
    let forward: Vec<Value> = model.iter().cloned().collect();
    if collect_deque(handle, Direction::Forward).await? != forward {
        return Ok(false);
    }
    let backward: Vec<Value> = model.iter().rev().cloned().collect();
    if collect_deque(handle, Direction::Backward).await? != backward {
        return Ok(false);
    }
    for index in 0..model.len() + 2 {
        if handle.get(index).await? != model.get(index).cloned() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Collects a deque handle's `stream(dir)` into a vector.
async fn collect_deque<S, C>(handle: &DequeHandle<S, C>, dir: Direction) -> Result<Vec<Value>>
where
    S: CellSession,
    C: Codec<Payload = Value>,
{
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Asserts a map handle equals the model: `get` (with `contains_key` parity)
/// over the whole key pool and both stream directions (ascending for
/// `Forward`, descending for `Backward`).
async fn assert_map<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    model: &BTreeMap<i64, Value>,
) -> Result<bool>
where
    S: CellSession,
{
    for key in KEY_POOL {
        let got = handle.get(&key).await?;
        if got != model.get(&key).cloned() {
            return Ok(false);
        }
        if handle.contains_key(&key).await? != got.is_some() {
            return Ok(false);
        }
    }
    let ascending: Vec<(i64, Value)> = model.iter().map(|(k, v)| (*k, v.clone())).collect();
    if collect_map(handle, Direction::Forward).await? != ascending {
        return Ok(false);
    }
    let descending: Vec<(i64, Value)> = model.iter().rev().map(|(k, v)| (*k, v.clone())).collect();
    Ok(collect_map(handle, Direction::Backward).await? == descending)
}

/// Collects a map handle's `stream(dir)` into a `(key, value)` vector.
async fn collect_map<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    dir: Direction,
) -> Result<Vec<(i64, Value)>>
where
    S: CellSession,
{
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Corrupt-coordinate classification: a stored entry whose coordinate does not
/// decode as the collection's key codec (here 3 bytes where `I64KeyCodec`
/// requires 8) surfaces from `stream` as [`CellStateError::Key`] classified
/// `Permanent` — one skippable row, never Terminal — the only error arm no
/// well-formed trace can reach.
#[test]
fn map_stream_classifies_corrupt_coordinate_permanent() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::cell_key::Coordinate;
    use crate::state::descriptor::CellStateError;
    use crate::state::descriptor::map::{MapStateError, entry_cell_for};
    use bytes::Bytes;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    // An `Overflowed` keyset forces the full-section scan, which reaches the
    // corrupt entry — a 3-byte coordinate that cannot decode as `I64KeyCodec`
    // (which needs 8 bytes).
    let corrupt = entry_cell_for(&Coordinate::from_bytes(vec![0x80, 0x00, 0x00]));
    block_on(store.write_resolved(
        &collection_ref,
        &[
            (keyset_cell(), Some(Bytes::from(OVERFLOWED_FRAME.to_vec()))),
            (
                corrupt,
                Some(Bytes::from(serde_json::to_vec(&Value::from(0_u8))?)),
            ),
        ],
        &[],
    ))?;

    let armed: ArmedKeys = Arc::default();
    let session = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let error = block_on(async {
        let stream = handle.stream(Direction::Forward);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            if let Err(error) = item {
                return Ok(error);
            }
        }
        bail!("a corrupt coordinate must end the stream with an error")
    })?;
    assert!(matches!(error, MapStateError::Cell(CellStateError::Key(_))));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Durable meta-frame golden (Deque): after real pushes `commit()`, the raw
/// bounds cell sits at its frozen address — `Meta` section, *empty*
/// coordinate — and
/// stores exactly `head ‖ tail` as two plain big-endian `i64`s. The pair
/// codec's own goldens pin that frame in isolation; this pins the deque's
/// *binding* to it: the codec choice, the head-first tuple order, and the unit
/// address (a swapped tuple, a different meta codec, or a moved address all go
/// red here while every self-consistent trace stays green).
#[test]
fn deque_meta_cell_bytes_are_frozen() -> Result<()> {
    use crate::state::descriptor::deque::meta_cell;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, _) = registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let armed: ArmedKeys = Arc::default();
    let event = read_event(0);
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    // `push_back` then `push_front`: the window becomes `head = -1, tail = 1`,
    // so the frame crosses the sign boundary the plain-BE encoding must keep.
    block_on(async {
        handle.push_back(Value::from(7_u8)).await?;
        handle.push_front(Value::from(9_u8)).await?;
        handle.commit().await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("dq")?,
    );
    let Some(bytes) = block_on(store.get(&id, &meta_cell(), event))?.into_inner() else {
        bail!("bounds cell missing at the frozen address");
    };
    assert_eq!(
        &bytes[..],
        [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 1
        ],
        "meta frame must be head ‖ tail as plain big-endian i64s"
    );
    Ok(())
}

/// Deque clear, pinned at the physical grain a `VecDeque` model cannot reach
/// (the reset window makes stale rows unreachable through the handle, so only
/// raw reads can observe them):
///
/// * **Index-space reset** — `clear()` erases the window cell, so the next push
///   starts a fresh window at index 0. A preserved (non-reset) index space
///   would read `head = 2 ‖ tail = 3` here; the reset reads `head = 0 ‖ tail =
///   1`.
/// * **Committed erasure** — a committed `clear()` physically erases the
///   pre-clear entry rows. The row outside the reused window must read absent;
///   a lost clear leg would leave it standing as an orphan the window never
///   addresses (unbounded leaked storage).
#[test]
fn deque_clear_resets_the_index_space() -> Result<()> {
    use crate::state::descriptor::deque::{entry_cell_for, meta_cell};
    use bytes::Bytes;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();

    // Event 1: two pushes, committed — the window becomes [0, 2).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.push_back(Value::from(1_u8)).await?;
        handle.push_back(Value::from(2_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Event 2: clear then push, committed — the window resets to [0, 1).
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.clear().await?;
        handle.push_back(Value::from(9_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let Some(bytes) = block_on(store.get(id, &meta_cell(), read_event(0)))?.into_inner() else {
        bail!("bounds cell missing after the committed clear-then-push");
    };
    assert_eq!(
        &bytes[..],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        "clear resets the index space: the reused window is head = 0 ‖ tail = 1"
    );

    // The physical erasure half: index 1 sat outside the reused window, so
    // only the clear's gap erase removes it — a stale committed row here is
    // the leak the API can never surface.
    let stale = entry_cell_for(&I64KeyCodec::encode(&1));
    assert_eq!(
        block_on(store.get(id, &stale, read_event(0)))?.into_inner(),
        None,
        "the committed clear must physically erase the out-of-window row"
    );
    let reused = entry_cell_for(&I64KeyCodec::encode(&0));
    assert_eq!(
        block_on(store.get(id, &reused, read_event(0)))?.into_inner(),
        Some(Bytes::from(serde_json::to_vec(&Value::from(9_u8))?)),
        "the reused index holds exactly the post-clear push"
    );
    Ok(())
}

/// Map clear, pinned at the physical grain the `BTreeMap` model cannot reach: a
/// committed `clear()` erases the keyset cell with the entries, so the
/// absent-keyset ⇒ empty-map reading (`KeysetPresence`) survives a cleared map,
/// and a later set repopulates a fresh single-key `Tracked` keyset (clear
/// resets the tracking, not just the entries — not a stale pre-clear list).
#[test]
fn map_clear_erases_keyset_and_repopulates() -> Result<()> {
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    // Event 1: one committed set stamps the keyset cell.
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Event 2: committed clear — stages the entries' section clear plus the
    // Cleared keyset cell.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.clear().await?;
        finalize_and_promote(&session, &oracle, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(store.get(id, &keyset_cell(), read_event(0)))?.into_inner(),
        None,
        "the committed clear must erase the keyset cell"
    );

    // Event 3: one committed set repopulates a fresh single-key Tracked keyset
    // (keyset absent after clear ⇒ the empty map ⇒ a fresh singleton, not a
    // stale pre-clear list).
    let event3 = EventRef::Message {
        dedup_id: Uuid::from_u128(3),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event3);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event3), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(store.get(id, &keyset_cell(), read_event(1)))?.into_inner(),
        Some(bytes::Bytes::from(tracked_frame(&[7]))),
        "a set after clear writes a fresh single-key Tracked keyset"
    );
    Ok(())
}

/// `KeysetPresence` fresh-map pin: an empty map's first committed `set` writes
/// a physical keyset cell (and the entry is live). Deterministic guard so the
/// `assert_keyset_present` probe in `run_map_trace` cannot pass vacuously on a
/// retained cell from a prior incarnation — the FIRST set is checked, so the
/// co-staged keyset write is reached even before any random trace could seed a
/// stale cell.
#[test]
fn map_first_set_writes_keyset() -> Result<()> {
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();

    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert!(
        cells.stored_coordinates(id).contains(&keyset_cell()),
        "the first committed set writes a physical keyset cell"
    );

    let read = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    assert_eq!(
        block_on(read_handle.get(&7))?,
        Some(Value::from(1_u8)),
        "the entry is live"
    );
    Ok(())
}

/// The exact `Tracked` frame bytes over `i64` keys (assumed ascending): tag
/// `0`, `u32` BE count, then per key a `u32` BE length and the 8-byte
/// sign-flipped BE coordinate — built from the real codec so a
/// coordinate-encoding change moves with it.
fn tracked_frame(keys: &[i64]) -> Vec<u8> {
    let mut frame = vec![TRACKED_TAG_BYTE];
    frame.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for k in keys {
        let coordinate = I64KeyCodec::encode(k);
        frame.extend_from_slice(&(coordinate.as_bytes().len() as u32).to_be_bytes());
        frame.extend_from_slice(coordinate.as_bytes());
    }
    frame
}

/// The frozen `Tracked` tag byte (`MapKeysetCodec` — mirrored here so the
/// suite's golden frames pin the same value the codec writes).
const TRACKED_TAG_BYTE: u8 = 0;

/// The frozen `Overflowed` sentinel frame (`[1]`).
const OVERFLOWED_FRAME: [u8; 1] = [1];

/// Durable keyset frame golden: after committed sets the raw keyset cell holds
/// the exact `Tracked` frame — tag, `u32` count, per-key `u32` length +
/// sign-flipped BE coordinate, sort order, and the `[2]` address, all in one
/// probe — and the first set past the limit collapses it to the exact
/// `Overflowed` sentinel.
#[test]
fn map_keyset_cell_bytes_are_frozen() -> Result<()> {
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 2,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    // Event 1: keys 1 and 2 fill the limit-2 keyset — a two-key Tracked frame.
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(1_u8)).await?;
        handle.set(2, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(store.get(id, &keyset_cell(), read_event(0)))?.into_inner() else {
        bail!("the committed sets must have written a keyset cell");
    };
    // Golden literal on purpose — independent of `tracked_frame`, so a helper
    // bug and a codec bug can't drift together.
    assert_eq!(
        &bytes[..],
        [
            0, 0, 0, 0, 2, 0, 0, 0, 8, 0x80, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0x80, 0, 0, 0, 0, 0,
            0, 2
        ],
        "the two-key Tracked frame is frozen (tag, count, lengths, coordinates, order, address)"
    );

    // Event 2: key 3 exceeds the limit → the Overflowed sentinel.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(3, Value::from(3_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(store.get(id, &keyset_cell(), read_event(1)))?.into_inner() else {
        bail!("the overflowing set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "the first set past the limit writes the Overflowed sentinel"
    );
    Ok(())
}

/// On a **TTL'd** map every set rewrites the keyset with its current contents
/// (not `Overflowed`): two committed sets under the limit leave the exact
/// two-key `Tracked` frame, and re-setting an already-tracked key still
/// refreshes that same `Tracked` frame rather than collapsing it. Guards
/// against a TTL-refresh that writes `Overflowed` — invisible to a
/// presence-only snapshot assert. The re-set of an already-tracked key is what
/// exercises the `Ok(_)` TTL fast-path arm; two distinct fresh keys never reach
/// it.
#[test]
fn map_keyset_stays_tracked_under_ttl() -> Result<()> {
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(Some(CompactDuration::new(3_600)))
        },
    )?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(1_u8)).await?;
        handle.set(2, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(store.get(id, &keyset_cell(), read_event(0)))?
            .into_inner()
            .map(|b| b.to_vec()),
        Some(tracked_frame(&[1, 2])),
        "a TTL'd map keeps a Tracked keyset, never collapses to Overflowed"
    );

    // Re-set an ALREADY-tracked key on the TTL'd map: the already-tracked fast
    // path must still rewrite the keyset to refresh its TTL, and with the SAME
    // Tracked contents — never `Overflowed`. This is the `Ok(_)` TTL arm the two
    // fresh keys above never reach.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(store.get(id, &keyset_cell(), read_event(1)))?
            .into_inner()
            .map(|b| b.to_vec()),
        Some(tracked_frame(&[1, 2])),
        "re-setting a tracked key on a TTL'd map keeps the Tracked frame, never Overflowed"
    );
    Ok(())
}

/// A malformed keyset frame degrades iteration to the full-section scan (never
/// errors) and is healed by the next set (which writes `Overflowed`).
#[test]
fn map_keyset_malformed_frame_degrades_and_heals() -> Result<()> {
    use bytes::Bytes;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let armed: ArmedKeys = Arc::default();

    // Event 1: a committed two-key map (writes a valid Tracked keyset).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(1_u8)).await?;
        handle.set(2, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Corrupt the keyset cell raw (unknown tag).
    block_on(store.write_resolved(
        &collection_ref,
        &[(keyset_cell(), Some(Bytes::from(vec![9_u8])))],
        &[],
    ))?;

    // A fresh stream degrades to the full-section scan and yields both entries
    // (no error).
    let read = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    let items = block_on(collect_map(&read_handle, Direction::Forward))?;
    assert_eq!(
        items,
        vec![(1, Value::from(1_u8)), (2, Value::from(2_u8))],
        "a malformed keyset degrades to the full-section scan, not an error"
    );

    // Event 2: a committed set heals the cell (malformed → Overflowed).
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(9_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(store.get(id, &keyset_cell(), read_event(1)))?.into_inner() else {
        bail!("the healing set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "the next set heals the malformed frame to Overflowed"
    );
    Ok(())
}

/// An oversized (but valid) stored `Tracked` frame — more keys than the
/// registered limit — degrades iteration to the full-section scan, and the next
/// set of an **already-listed** key collapses it to `Overflowed`: the size
/// check runs before the already-present fast path. The set-side twin of the
/// remove-side heal (`map_keyset_removal_heals_oversized`): they pin opposite
/// directions of the same `is_oversized` boundary.
#[test]
fn map_keyset_oversized_frame_collapses_before_fast_path() -> Result<()> {
    use crate::state::descriptor::map::entry_cell_for;
    use bytes::Bytes;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let armed: ArmedKeys = Arc::default();

    // Seed a valid 5-key Tracked keyset (over limit 3) plus its 5 entries.
    let mut seed = vec![(
        keyset_cell(),
        Some(Bytes::from(tracked_frame(&[1, 2, 3, 4, 5]))),
    )];
    for k in 1..=5_i64 {
        seed.push((
            entry_cell_for(&I64KeyCodec::encode(&k)),
            Some(Bytes::from(serde_json::to_vec(&Value::from(
                u8::try_from(k)?,
            ))?)),
        ));
    }
    block_on(store.write_resolved(&collection_ref, &seed, &[]))?;

    // The oversized Tracked keyset degrades to the full-section scan (yields
    // all 5).
    let read = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    let items = block_on(collect_map(&read_handle, Direction::Forward))?;
    assert_eq!(
        items.len(),
        5,
        "the oversized keyset degrades to the full-section scan"
    );

    // A set of an ALREADY-LISTED key collapses to Overflowed — the size check
    // ran before the already-present fast path.
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(1, Value::from(11_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(store.get(id, &keyset_cell(), read_event(1)))?.into_inner() else {
        bail!("the set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "an oversized Tracked collapses to Overflowed even when the key is already listed"
    );
    Ok(())
}

/// The encoded-byte ceiling overflows the keyset even when the key count is far
/// under the limit: two committed sets of ~40 KiB string keys exceed the 64 KiB
/// frame ceiling on the second, writing `Overflowed`.
#[test]
fn map_keyset_byte_ceiling_overflows() -> Result<()> {
    use crate::state::order_codec::Utf8KeyCodec;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<Utf8KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let armed: ArmedKeys = Arc::default();

    let big_a = "a".repeat(40 * 1024);
    let big_b = "b".repeat(40 * 1024);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(big_a, Value::from(1_u8)).await?;
        handle.set(big_b, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(store.get(id, &keyset_cell(), read_event(0)))?.into_inner() else {
        bail!("the sets must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "two ~40 KiB keys exceed the 64 KiB frame ceiling (count far under the limit)"
    );
    Ok(())
}

/// On a non-TTL'd map a set of an already-tracked key writes **no** keyset cell
/// (a no-op content change), while `remove` **subtracts** — rewriting the
/// `Tracked` frame without the removed coordinate (here down to the empty
/// `Tracked([])`). Both are invisible to a value-only assert, so probed on the
/// dirty write set itself.
#[test]
fn map_keyset_subtracts_on_remove() -> Result<()> {
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&descriptor, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    let armed: ArmedKeys = Arc::default();

    // Event 1: set key 7 committed (a fresh single-key Tracked keyset).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &oracle, event_dedup(event1), &cells, &id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Event 2 over a caller-owned dirty workspace: re-set 7, then remove it.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let dirty = Arc::new(DirtyStore::new());
    let session = make_session_with_dirty(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        event2,
        dirty.clone(),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(7, Value::from(2_u8)).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let after_reset = dirty.collection_snapshot(&id);
    assert!(
        !after_reset.iter().any(|(c, _)| *c == keyset_cell()),
        "a non-TTL re-set of a tracked key writes no keyset cell"
    );
    block_on(async {
        handle.remove(&7).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let after_remove = dirty.collection_snapshot(&id);
    let keyset_write = after_remove
        .iter()
        .find(|(c, _)| *c == keyset_cell())
        .ok_or_else(|| eyre!("remove must rewrite the keyset cell (subtracting the key)"))?;
    assert_eq!(
        keyset_write.1.as_deref(),
        Some(&tracked_frame(&[])[..]),
        "remove subtracts the last key, leaving an empty Tracked frame"
    );
    Ok(())
}

/// `Continue` when a mid-trace return matched the model, `Mismatch` otherwise.
fn mismatch_unless(matched: bool) -> OpOutcome {
    if matched {
        OpOutcome::Continue
    } else {
        OpOutcome::Mismatch
    }
}

/// The dedup id of a message event (the suites stage only message events).
fn event_dedup(event: EventRef) -> Uuid {
    match event {
        EventRef::Message { dedup_id } => dedup_id,
        EventRef::Timer(_) => Uuid::nil(),
    }
}

/// A read-back event distinct from every staging event so own-event resolution
/// never short-circuits the read.
fn read_event(index: usize) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX - index as u128),
    }
}

/// One step of a map stream-interleave trace: advance the live stream one item,
/// or run a mutator on the same session between items.
#[derive(Clone, Debug)]
pub(crate) enum MapStreamStep {
    /// Pull the next stream item.
    Advance,
    /// `set(key, val)`.
    Set(i64, i64),
    /// `remove(key)`.
    Remove(i64),
    /// `clear()`.
    Clear,
    /// Mid-handler `commit()`.
    Commit,
    /// Mid-handler `rollback()`.
    Rollback,
}

impl Arbitrary for MapStreamStep {
    fn arbitrary(g: &mut Gen) -> Self {
        // Keys 0..24 span the 20-key seed plus a few added-after-init keys;
        // values 0..8 so re-sets collide. `Advance` is weighted heavily so the
        // stream actually drains between mutations.
        let key = i64::from(u8::arbitrary(g) % 24);
        let val = i64::from(u8::arbitrary(g) % 8);
        match g
            .choose(&[0_u8, 0, 0, 1, 1, 2, 3, 4, 5])
            .copied()
            .unwrap_or(0)
        {
            1 => Self::Set(key, val),
            2 => Self::Remove(key),
            3 => Self::Commit,
            4 => Self::Rollback,
            5 => Self::Clear,
            _ => Self::Advance,
        }
    }
}

/// A map stream-interleave trace plus the stream direction.
#[derive(Clone, Debug)]
pub(crate) struct MapInterleave {
    steps: Vec<MapStreamStep>,
    backward: bool,
}

impl Arbitrary for MapInterleave {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
            backward: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let backward = self.backward;
        Box::new(
            self.steps
                .shrink()
                .map(move |steps| Self { steps, backward }),
        )
    }
}

/// One step of a deque stream-interleave trace.
#[derive(Clone, Debug)]
pub(crate) enum DequeStreamStep {
    /// Pull the next stream item.
    Advance,
    /// `push_back(val)`.
    PushBack(i64),
    /// `push_front(val)`.
    PushFront(i64),
    /// `pop_back()`.
    PopBack,
    /// `pop_front()`.
    PopFront,
    /// `clear()`.
    Clear,
    /// Mid-handler `commit()`.
    Commit,
    /// Mid-handler `rollback()`.
    Rollback,
}

impl Arbitrary for DequeStreamStep {
    fn arbitrary(g: &mut Gen) -> Self {
        let val = i64::from(u8::arbitrary(g));
        match g
            .choose(&[0_u8, 0, 0, 1, 2, 3, 4, 5, 6, 7])
            .copied()
            .unwrap_or(0)
        {
            1 => Self::PushBack(val),
            2 => Self::PushFront(val),
            3 => Self::PopBack,
            4 => Self::PopFront,
            5 => Self::Commit,
            6 => Self::Rollback,
            7 => Self::Clear,
            _ => Self::Advance,
        }
    }
}

/// A deque stream-interleave trace plus the stream direction.
#[derive(Clone, Debug)]
pub(crate) struct DequeInterleave {
    steps: Vec<DequeStreamStep>,
    backward: bool,
}

impl Arbitrary for DequeInterleave {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
            backward: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let backward = self.backward;
        Box::new(
            self.steps
                .shrink()
                .map(move |steps| Self { steps, backward }),
        )
    }
}

/// Runs a fallible handle op under the interleave hang-guard, tagging both the
/// timeout and the op error with `label` — the mutator boilerplate the
/// interleave runners share.
async fn guarded<T, E: Display>(label: &str, fut: impl Future<Output = Result<T, E>>) -> Result<T> {
    timeout(INTERLEAVE_HANG_GUARD, fut)
        .await
        .map_err(|_| eyre!("{label} hung"))?
        .map_err(|e| eyre!("{label}: {e}"))
}

/// Weak-consistency check for one yielded map entry: its key was in the init
/// snapshot, is yielded at most once, and its value was held at that key at
/// some point (the per-arm consistency contract — a paged live read, not a
/// snapshot).
fn check_map_yield(
    key: i64,
    value: &Value,
    init_keys: &BTreeSet<i64>,
    yielded: &mut BTreeSet<i64>,
    ever_held: &BTreeMap<i64, BTreeSet<i64>>,
) -> Result<()> {
    if !init_keys.contains(&key) {
        bail!("yielded key {key} was not in the init-snapshot membership");
    }
    if !yielded.insert(key) {
        bail!("key {key} was yielded twice");
    }
    let value = value
        .as_i64()
        .ok_or_else(|| eyre!("non-integer value yielded for key {key}"))?;
    if !ever_held
        .get(&key)
        .is_some_and(|held| held.contains(&value))
    {
        bail!("value {value} yielded for key {key} was never held there");
    }
    Ok(())
}

/// The `StreamYieldFree` interleaving property (map): random `next()`/mutator
/// interleavings on ONE live session against a live map stream never deadlock
/// and never error, and
/// every yielded entry is weakly consistent with the init snapshot. A forced
/// first `Advance` locks the key-membership snapshot to the committed seed
/// before any mutator runs, so a yielded key must be a seed key and its value
/// one held there at some point (values are read live, chunk by chunk). Every
/// op is bounded by [`INTERLEAVE_HANG_GUARD`] — the only deadline, never the
/// assertion. FALSIFICATION: hold the permit across the yield by returning it
/// in the unfold state (`Some((chunk, permit, keys))`) so it lives into the
/// forwarding loop → the first mutator after an `Advance` blocks on the gate
/// the suspended generator holds → the hang-guard elapses → red.
pub(crate) async fn run_map_stream_interleave(input: MapInterleave) -> Result<bool> {
    let MapInterleave { steps, backward } = input;
    let dir = if backward {
        Direction::Backward
    } else {
        Direction::Forward
    };
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("iv");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "iv",
        &state_key,
        CollectionDef {
            // ≥ seed so the map stays Tracked (the chunked point-get arm).
            keyset_limit: 4096,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();

    // Seed a committed map of INTERLEAVE_SEED keys; `ever_held` records each.
    let mut ever_held: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    let seed_event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let seed_session = make_session(&cells, &oracle, &registry, &state_key, &armed, seed_event);
    let seed = descriptor
        .bind(&seed_session)
        .map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..INTERLEAVE_SEED {
        let key = i64::try_from(i)?;
        seed.set(key, Value::from(key)).await?;
        ever_held.entry(key).or_default().insert(key);
    }
    finalize_and_promote(&seed_session, &oracle, Uuid::from_u128(1), &cells, id).await?;
    let init_keys: BTreeSet<i64> = (0..i64::try_from(INTERLEAVE_SEED)?).collect();

    // A fresh live session; the stream and its racing mutators share it.
    let session = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);

    let mut yielded: BTreeSet<i64> = BTreeSet::new();
    // A forced first `Advance` locks the snapshot to the committed seed before
    // any mutator, then the generated steps interleave freely.
    for step in once(MapStreamStep::Advance).chain(steps) {
        match step {
            MapStreamStep::Advance => {
                if let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
                    .await
                    .map_err(|_| eyre!("Advance hung: the stream held the gate across a yield"))?
                {
                    let (key, value) =
                        item.map_err(|e| eyre!("stream yielded Err on a legal interleaving: {e}"))?;
                    check_map_yield(key, &value, &init_keys, &mut yielded, &ever_held)?;
                }
            }
            MapStreamStep::Set(key, val) => {
                guarded("set", handle.set(key, Value::from(val))).await?;
                ever_held.entry(key).or_default().insert(val);
            }
            MapStreamStep::Remove(key) => {
                guarded("remove", handle.remove(&key)).await?;
            }
            MapStreamStep::Clear => {
                guarded("clear", handle.clear()).await?;
            }
            MapStreamStep::Commit => {
                guarded("commit", handle.commit()).await?;
            }
            MapStreamStep::Rollback => {
                timeout(INTERLEAVE_HANG_GUARD, handle.rollback())
                    .await
                    .map_err(|_| eyre!("rollback hung"))?;
            }
        }
    }
    // Drain the remainder under the hang-guard.
    while let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
        .await
        .map_err(|_| eyre!("drain hung"))?
    {
        let (key, value) = item.map_err(|e| eyre!("stream yielded Err on drain: {e}"))?;
        check_map_yield(key, &value, &init_keys, &mut yielded, &ever_held)?;
    }
    Ok(true)
}

/// The `StreamYieldFree` interleaving property (deque): the structural twin of
/// [`run_map_stream_interleave`]. A forced first `Advance` locks the **position
/// window** snapshot to the committed seed; thereafter random push/pop/clear/
/// commit/rollback mutators interleave with `next()`. No op deadlocks, no
/// `Advance` errors, the yielded count never exceeds the init window length,
/// and every yielded value was pushed at some point (position identity — a
/// popped position reads absent and is skipped). Same FALSIFICATION as the map
/// twin.
pub(crate) async fn run_deque_stream_interleave(input: DequeInterleave) -> Result<bool> {
    let DequeInterleave { steps, backward } = input;
    let dir = if backward {
        Direction::Backward
    } else {
        Direction::Forward
    };
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("iv");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "iv", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let armed: ArmedKeys = Arc::default();

    // Seed a committed window of INTERLEAVE_SEED elements; `ever_pushed` records
    // every value the deque ever held.
    let mut ever_pushed: BTreeSet<i64> = BTreeSet::new();
    let seed_event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let seed_session = make_session(&cells, &oracle, &registry, &state_key, &armed, seed_event);
    let seed = descriptor
        .bind(&seed_session)
        .map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..INTERLEAVE_SEED {
        let value = i64::try_from(i)?;
        seed.push_back(Value::from(value)).await?;
        ever_pushed.insert(value);
    }
    finalize_and_promote(&seed_session, &oracle, Uuid::from_u128(1), &cells, id).await?;

    let session = make_session(
        &cells,
        &oracle,
        &registry,
        &state_key,
        &armed,
        read_event(0),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);

    let mut yielded = 0_usize;

    for step in once(DequeStreamStep::Advance).chain(steps) {
        match step {
            DequeStreamStep::Advance => {
                if let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
                    .await
                    .map_err(|_| eyre!("Advance hung: the stream held the gate across a yield"))?
                {
                    let value =
                        item.map_err(|e| eyre!("stream yielded Err on a legal interleaving: {e}"))?;
                    check_deque_yield(&value, &ever_pushed, &mut yielded)?;
                }
            }
            DequeStreamStep::PushBack(val) => {
                guarded("push_back", handle.push_back(Value::from(val))).await?;
                ever_pushed.insert(val);
            }
            DequeStreamStep::PushFront(val) => {
                guarded("push_front", handle.push_front(Value::from(val))).await?;
                ever_pushed.insert(val);
            }
            DequeStreamStep::PopBack => {
                guarded("pop_back", handle.pop_back()).await?;
            }
            DequeStreamStep::PopFront => {
                guarded("pop_front", handle.pop_front()).await?;
            }
            DequeStreamStep::Clear => {
                guarded("clear", handle.clear()).await?;
            }
            DequeStreamStep::Commit => {
                guarded("commit", handle.commit()).await?;
            }
            DequeStreamStep::Rollback => {
                timeout(INTERLEAVE_HANG_GUARD, handle.rollback())
                    .await
                    .map_err(|_| eyre!("rollback hung"))?;
            }
        }
    }
    while let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
        .await
        .map_err(|_| eyre!("drain hung"))?
    {
        let value = item.map_err(|e| eyre!("stream yielded Err on drain: {e}"))?;
        check_deque_yield(&value, &ever_pushed, &mut yielded)?;
    }
    Ok(true)
}

/// Weak-consistency check for one yielded deque element: the yielded count
/// stays within the init window length and the value was pushed at some point
/// (position identity — a popped position reads absent and is skipped).
fn check_deque_yield(
    value: &Value,
    ever_pushed: &BTreeSet<i64>,
    yielded: &mut usize,
) -> Result<()> {
    *yielded += 1;
    if *yielded > INTERLEAVE_SEED {
        bail!("yielded more items than the init window length");
    }
    let value = value
        .as_i64()
        .ok_or_else(|| eyre!("non-integer value yielded"))?;
    if !ever_pushed.contains(&value) {
        bail!("value {value} was never pushed");
    }
    Ok(())
}
