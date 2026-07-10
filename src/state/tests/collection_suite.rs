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
//! caught. This single property covers dense-window / loose-bounds invariants,
//! key/positional ordering, containment, and the bounds-and-entries-promote-
//! together crash atomicity.
//!
//! Both op alphabets include the mid-handler `Flush`: the runner snapshots the
//! scratch model at each flush, and on a non-committing outcome the committed
//! read-back must equal the last flushed snapshot — flushed ops (entries *and*
//! bookkeeping cells, as one batch) survive abort and crash-rollback while
//! post-flush ops still roll back, the at-least-once flush contract.
//!
//! Memory-backed only: the `cell_suite` runners already prove memory ↔
//! Cassandra parity for the underlying store, and the collection logic lives
//! entirely in the descriptor layer above it.

use super::cell_suite::{MAX_TRACE_OPS, ScriptedOracle, capped_vec};
use crate::codec::{Codec, JsonCodec};
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::descriptor::map::bound_cells;
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
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{CellSession, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::{
    CollectionId, CollectionRef, Direction, EventRef, PartitionBackend, StateKey, StateName,
    StateType,
};
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

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

/// What one applied op observed: keep going, a mid-event flush happened (the
/// runner snapshots the scratch model as immediately durable), or a return
/// value diverged from the model (property failure).
enum OpOutcome {
    Continue,
    Flushed,
    Mismatch,
}

/// One deque mutation. Payloads are single `u8`s wrapped as JSON numbers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DequeOp {
    PushBack(u8),
    PushFront(u8),
    PopBack,
    PopFront,
    Flush,
}

impl Arbitrary for DequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => Self::PushBack(u8::arbitrary(g)),
            1 => Self::PushFront(u8::arbitrary(g)),
            2 => Self::PopBack,
            3 => Self::PopFront,
            _ => Self::Flush,
        }
    }
}

/// One map mutation, mid-trace read, or mid-handler flush over the bounded
/// key pool.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MapOp {
    Set(i64, u8),
    Remove(i64),
    Get(i64),
    Flush,
}

impl Arbitrary for MapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = g.choose(&KEY_POOL).copied().unwrap_or(0);
        match u8::arbitrary(g) % 5 {
            0 | 1 => Self::Set(key, u8::arbitrary(g)),
            2 => Self::Remove(key),
            3 => Self::Get(key),
            _ => Self::Flush,
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
/// the per-event buffered cells (the Map TTL bound-refresh property inspects
/// the staged-set composition through it).
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

/// Registers `name` and returns the shared registry plus the sweep ref.
fn registry_and_ref<D>(
    descriptor: &D,
    name: &str,
    state_key: &StateKey,
) -> Result<(Arc<CollectionDefRegistry>, CollectionRef)>
where
    D: StateDescriptor,
{
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(descriptor, CollectionDef::new(None))?;
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

/// Resolves the event along its outcome's path: promote/rollback inline, or
/// crash → fresh store → sweep. Returns `false` only if a sweep strands a cell.
async fn resolve_event(
    session: SuiteSession,
    outcome: Outcome,
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    collection_ref: &CollectionRef,
) -> Result<bool> {
    match outcome {
        Outcome::Commit => {
            session.commit_apply().await;
        }
        Outcome::Abort => {
            session.rollback_aborted().await;
        }
        Outcome::CrashCommitted | Outcome::CrashAborted => {
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
    Ok(true)
}

/// The warm backing shared across a trace's events, handed to the read-back
/// assertion so a kind whose invariant needs the raw cells (Map's bound-cell
/// superset check) can reach them.
struct Backing<'a> {
    cells: &'a MemoryCells,
    oracle: &'a ScriptedOracle,
    registry: &'a Arc<CollectionDefRegistry>,
    state_key: &'a StateKey,
}

/// Drives a generated trace through the real [`KeyedStateSession`] lifecycle
/// for any collection kind — the Deque and Map suites differ only in the op
/// alphabet, the model, and the assertions, so everything else lives here once:
/// bind a fresh session per event, apply each op (`apply_op`, which also
/// asserts mid-trace `pop`/`get` returns and reports mid-handler flushes),
/// `finalize`, resolve along the event's outcome (promote / rollback / crash →
/// sweep), advance the model — the full scratch on a commit, the last flushed
/// snapshot otherwise — then assert the committed collection through a fresh
/// read-back session (`assert`, which absorbs any kind-specific check such as
/// Map's bound-cell superset).
async fn run_collection_trace<D, O, M, Apply, Assert>(
    trace: Trace<O>,
    descriptor: D,
    name: &str,
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
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let (registry, collection_ref) = registry_and_ref(&descriptor, name, &state_key)?;
    let armed: ArmedKeys = Arc::default();
    let backing = Backing {
        cells: &cells,
        oracle: &oracle,
        registry: &registry,
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
        // The scratch as of the last mid-handler flush — durable regardless of
        // the event's outcome (the at-least-once flush contract).
        let mut flushed: Option<M> = None;
        for op in &ev.ops {
            match apply_op(&handle, *op, &mut scratch).await? {
                OpOutcome::Continue => {}
                OpOutcome::Flushed => flushed = Some(scratch.clone()),
                OpOutcome::Mismatch => return Ok(false),
            }
        }

        session
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
        model = if ev.outcome.commits() {
            scratch
        } else {
            // Abort / crash-rollback revert only the post-flush provisionals
            // (their `prev` was captured after the flush landed); the flushed
            // snapshot is already committed.
            flushed.unwrap_or(model)
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
pub(crate) async fn run_deque_trace(trace: DequeTrace) -> Result<bool> {
    run_collection_trace(
        trace,
        deque_state::<JsonCodec>("dq"),
        "dq",
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
            DequeOp::Flush => {
                handle.flush().await?;
                Ok(OpOutcome::Flushed)
            }
        },
        async |handle, model, _backing: &Backing<'_>| assert_deque(handle, model).await,
    )
    .await
}

/// Drives a map trace, asserting the handle equals a `BTreeMap` model after
/// every event, that each mid-trace `get` returns the model's value, and that
/// the stored bounds cover a loose superset of the live key range.
pub(crate) async fn run_map_trace(trace: MapTrace) -> Result<bool> {
    run_collection_trace(
        trace,
        map_state::<I64KeyCodec, JsonCodec>("mp"),
        "mp",
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
            MapOp::Get(k) => Ok(mismatch_unless(
                handle.get(&k).await? == scratch.get(&k).cloned(),
            )),
            MapOp::Flush => {
                handle.flush().await?;
                Ok(OpOutcome::Flushed)
            }
        },
        async |handle, model, backing: &Backing<'_>| {
            Ok(assert_map(handle, model).await?
                && assert_map_bounds(
                    backing.cells,
                    backing.oracle,
                    backing.registry,
                    backing.state_key,
                    model,
                )
                .await?)
        },
    )
    .await
}

/// Map TTL bound-refresh (staged-set composition): on a collection **with a
/// TTL**, every `set` buffers *both* bound cells — even a re-set of a key
/// already within the committed bounds — so their TTL is refreshed and the
/// bounds outlive every entry. Runs multiple committed events over a fresh
/// per-event dirty workspace so a later set lands within *committed* bounds:
/// the case a single-event snapshot cannot reach, because the first set always
/// seeds both bounds into the dirty overlay, masking an extend-only regression.
pub(crate) async fn run_map_ttl_bounds_trace(trace: MapTrace) -> Result<bool> {
    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(
        &descriptor,
        CollectionDef::new(Some(CompactDuration::new(3_600))),
    )?;
    let registry = Arc::new(registry);
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    let armed: ArmedKeys = Arc::default();
    let (min_cell, max_cell) = bound_cells();

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
                    // Snapshot immediately, before any later Flush drains dirty.
                    let snapshot = dirty.collection_snapshot(&id);
                    let has_min = snapshot.iter().any(|(c, _)| *c == min_cell);
                    let has_max = snapshot.iter().any(|(c, _)| *c == max_cell);
                    if !(has_min && has_max) {
                        return Ok(false);
                    }
                }
                MapOp::Remove(k) => handle.remove(&k).await?,
                MapOp::Get(k) => {
                    handle.get(&k).await?;
                }
                MapOp::Flush => {
                    handle.flush().await?;
                }
            }
        }
        session
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?;
        oracle
            .record_message(event_dedup(event))
            .await
            .map_err(|e| eyre!("marker: {e}"))?;
        session.commit_apply().await;
    }
    Ok(true)
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
    let (registry, collection_ref) = registry_and_ref(&descriptor, "dq", &state_key)?;
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

/// `MetaBoundsCoverLive` (Map): the stored `MapBound::Min`/`MapBound::Max`
/// bound a loose **superset** of the live key range — every live key's
/// coordinate lies within `[min, max]`. Read directly from the raw bound cells,
/// so this proves the bound *values* are a correct superset, not just that
/// `stream` happens to yield the right keys. A stale bound may point at a
/// since-removed key, so containment is asserted over *live* keys only (the
/// loose-superset invariant).
async fn assert_map_bounds(
    cells: &MemoryCells,
    oracle: &ScriptedOracle,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    model: &BTreeMap<i64, Value>,
) -> Result<bool> {
    if model.is_empty() {
        // Bounds may persist after every key is removed (loose, never shrunk),
        // but they then constrain nothing.
        return Ok(true);
    }
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX / 2),
    };
    let (min_cell, max_cell) = bound_cells();
    let (Some(min), Some(max)) = (
        store.get(&id, &min_cell, probe).await?.into_inner(),
        store.get(&id, &max_cell, probe).await?.into_inner(),
    ) else {
        // A non-empty map must have stamped both bounds on its first `set`.
        return Ok(false);
    };
    for key in model.keys() {
        let coordinate = I64KeyCodec::encode(key);
        if coordinate.as_bytes() < &min[..] || coordinate.as_bytes() > &max[..] {
            return Ok(false);
        }
    }
    Ok(true)
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

/// Asserts a map handle equals the model: `get` over the whole key pool and
/// both stream directions (ascending for `Forward`, descending for
/// `Backward`).
async fn assert_map<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    model: &BTreeMap<i64, Value>,
) -> Result<bool>
where
    S: CellSession,
{
    for key in KEY_POOL {
        if handle.get(&key).await? != model.get(&key).cloned() {
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
    use crate::state::descriptor::map::{MapStateError, bound_cells, entry_cell_for};
    use bytes::Bytes;
    use futures::executor::block_on;

    let oracle = ScriptedOracle::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(&descriptor, "mp", &state_key)?;
    let store = MemoryCellStore::new(cells.clone(), oracle.clone(), registry.clone());

    // Bounds spanning the whole key space so a scan is issued and reaches the
    // corrupt entry — a 3-byte coordinate that cannot decode as `I64KeyCodec`
    // (which needs 8 bytes), placed inside `[encode(MIN), encode(MAX)]`.
    let (min_cell, max_cell) = bound_cells();
    let min_bytes = Bytes::copy_from_slice(I64KeyCodec::encode(&i64::MIN).as_bytes());
    let max_bytes = Bytes::copy_from_slice(I64KeyCodec::encode(&i64::MAX).as_bytes());
    let corrupt = entry_cell_for(&Coordinate::from_bytes(vec![0x80, 0x00, 0x00]));
    block_on(store.write_resolved(
        &collection_ref,
        &[
            (min_cell, Some(min_bytes)),
            (max_cell, Some(max_bytes)),
            (
                corrupt,
                Some(Bytes::from(serde_json::to_vec(&Value::from(0_u8))?)),
            ),
        ],
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

/// Durable meta-frame golden (Deque): after real pushes flush, the raw bounds
/// cell sits at its frozen address — `Meta` section, *empty* coordinate — and
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
    let (registry, _) = registry_and_ref(&descriptor, "dq", &state_key)?;
    let armed: ArmedKeys = Arc::default();
    let event = read_event(0);
    let session = make_session(&cells, &oracle, &registry, &state_key, &armed, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    // `push_back` then `push_front`: the window becomes `head = -1, tail = 1`,
    // so the frame crosses the sign boundary the plain-BE encoding must keep.
    block_on(async {
        handle.push_back(Value::from(7_u8)).await?;
        handle.push_front(Value::from(9_u8)).await?;
        handle.flush().await?;
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
