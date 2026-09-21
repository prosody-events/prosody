//! Trace + model-oracle property suites for Map, Set, and Deque collections.
//!
//! Each runner drives a generated multi-event trace through the **real**
//! [`KeyedStateSession`] lifecycle — handler ops buffer into the dirty overlay,
//! `finalize` stages them in one co-stamped batch, then the event commits
//! (promote), aborts (rollback), or crashes (a fresh store over the same warm
//! `MemoryCells` recovers through the admission). After every event the
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

use super::cell_suite::{MAX_TRACE_OPS, MemoryDeduplicationStore, capped_vec};
use super::support::CountingCellStore;
use super::support::assert_no_settlement_residue;
use crate::codec::{Codec, JsonCodec};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::KeyQuery;
use crate::state::cell::Values;
use crate::state::collection::StateSession;
use crate::state::descriptor::map::{entry_cell_for, keyset_cell};
use crate::state::descriptor::{
    DequeHandle, MapHandle, StateDescriptor, deque, deque_state, map_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Promoted;
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{Finalized, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellRead;
use crate::state::store::{CELL_BATCH, CellStore};
use crate::state::tests::support::admit_collection;
use crate::state::tests::support::seed_commit_evidence;
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
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::iter::once;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;
use uuid::Uuid;

mod queries;
mod set;
pub(crate) use queries::{DequeConstraints, StreamConstraints, run_deque_constraint_parity};

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

/// The per-partition backend for the suites: a memory cell store and dedup
/// store, behind the standard [`PartitionBackend`] bundle.
type SuiteBackend =
    PartitionBackend<MemoryDeduplicationStore, MemoryDescriptorIdentityStore, MemoryCellStore, ()>;

/// The real per-event session the handles bind over.
type SuiteSession = KeyedStateSession<SuiteBackend, MemoryLoader<Value>>;

/// The bounded key space the Map trace ranges over — small and spanning the
/// sign boundary so re-inserts, removes, and ordered scans across negative and
/// positive `i64` keys all occur.
pub(crate) const KEY_POOL: [i64; 5] = [-2, -1, 0, 1, 2];

/// Max ops per event, keeping each event's batch small while the trace as a
/// whole still grows and drains the collection.
const MAX_EVENT_OPS: usize = 4;

/// The bounded window width the deque-holes property ranges over.
const MAX_DEQUE_WINDOW: usize = 8;

/// The head-index pool for the deque-holes property — small and spanning the
/// sign boundary so windows crossing zero are exercised.
const HEAD_POOL: [i64; 7] = [-3, -2, -1, 0, 1, 2, 3];

/// The capacity pool the deque-capacity property ranges over — small so a
/// seeded over-wide window (`span = cap + D`) needs few catch-up pushes to
/// converge, and so eviction fires on nearly every push-to-full.
const CAP_POOL: [usize; 4] = [1, 2, 3, 4];

/// The bounded key-window width the map key-scan holes property ranges over.
const MAX_MAP_KEY_WINDOW: usize = 8;

/// Builds a fresh session for one event over the shared warm backing. Dropped
/// senders are fine — `watch::Receiver::borrow` keeps returning the last value,
/// so the session reads as non-terminated.
fn make_session(
    cells: &MemoryCells,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
) -> SuiteSession {
    make_session_with_dirty(cells, dedup, registry, state_key, event, Arc::default())
}

/// [`make_session`] over a caller-owned dirty workspace, so a test can snapshot
/// the per-event buffered cells (the Map TTL keyset-refresh property inspects
/// what `finalize` will stage through it).
fn make_session_with_dirty(
    cells: &MemoryCells,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
    dirty: Arc<DirtyStore>,
) -> SuiteSession {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts::<SuiteBackend, _> {
        cell: MemoryCellStore::new(cells.clone()),
        dirty,
        dedup: dedup.clone(),
        loader: MemoryLoader::new(),
        registry: registry.clone(),
        state_key: state_key.clone(),
        event,
        dedup_ttl: CompactDuration::new(30),
        checks: (),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    })
}

/// Registers `name` under `def` and returns the registry and collection
/// reference.
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

/// Promotes the event or runs admission after a rollback or crash.
/// Returns `false` if admission fails or leaves residue.
async fn resolve_event(
    session: SuiteSession,
    finalized: Finalized<MemoryCellStore, ()>,
    outcome: Outcome,
    cells: &MemoryCells,
    dedup: &MemoryDeduplicationStore,
    _registry: &Arc<CollectionDefRegistry>,
    collection_ref: &CollectionRef,
) -> Result<bool> {
    match outcome {
        Outcome::Commit => {
            if let Finalized::Staged(staged) = finalized
                && !matches!(staged.promote(|| false).await, Promoted::Complete)
            {
                return Err(eyre!("promote incomplete on a healthy store"));
            }
        }
        Outcome::Abort => {
            if let Finalized::Staged(staged) = finalized {
                drop(staged);
                admit_collection(&MemoryCellStore::new(cells.clone()), dedup, collection_ref)
                    .await?;
            }
        }
        Outcome::CrashCommitted | Outcome::CrashAborted => {
            // Dropping the receipt (and session) IS the crash: the durable
            // staged cells and the dedup store survive; the in-memory record dies.
            if matches!(outcome, Outcome::CrashCommitted)
                && matches!(finalized, Finalized::Staged(_))
            {
                seed_commit_evidence(&MemoryCellStore::new(cells.clone()), collection_ref).await?;
            }
            drop(finalized);
            drop(session);
            // A cold store over the same warm backing — exactly a restart.
            let store = MemoryCellStore::new(cells.clone());
            if !admit_collection(&store, dedup, collection_ref)
                .await
                .map_err(|e| eyre!("admission: {e}"))?
            {
                return Ok(false);
            }
        }
    }
    // Every outcome in this runner's alphabet settles fully (promote and
    // rollback delete the marker; admission resolves it), so no settlement
    // residue may remain — checked raw, before the resolving read-back below
    // heals a skipped settle to identical bytes and masks it.
    assert_no_settlement_residue(cells, collection_ref.id())?;
    Ok(true)
}

/// Finalizes and promotes an event, then records its dedup id.
/// The physical cells must contain no settlement residue afterward.
pub(crate) async fn finalize_and_promote<L>(
    session: &L,
    dedup: &MemoryDeduplicationStore,
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
    dedup
        .insert(dedup_id)
        .await
        .map_err(|e| eyre!("marker: {e}"))?;
    if let Finalized::Staged(staged) = finalized {
        if !matches!(staged.promote(|| false).await, Promoted::Complete) {
            bail!("promote incomplete on a healthy store");
        }
        assert_no_settlement_residue(cells, collection)?;
    }
    Ok(())
}

/// `KeysetPresence` (Map): whenever the map holds any live entry, the raw
/// keyset cell is physically present. Probed directly from the stored
/// coordinate set (not the resolving handle, which can never synthesize a
/// missing physical row), so it proves the keyset *cell* exists — the invariant
/// on which the query's `Absent → Empty` fast path rests. An empty model is
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
/// directions (front-to-back for `Forward`, back-to-front for `Backward`),
/// `get` at every position (including out of range → `None`), and the
/// endpoint peeks (`peek_front == get(0)`, `peek_back == get(len-1)`).
async fn assert_deque<S, C>(handle: &DequeHandle<S, C>, model: &VecDeque<Value>) -> Result<bool>
where
    S: StateSession,
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
    if !assert_peeks(handle).await? {
        return Ok(false);
    }
    Ok(true)
}

/// Handle-internal peek parity: `peek_front == get(0)` and
/// `peek_back == get(len-1)` (both `None` on an empty deque). Needs no model —
/// `len` is separately pinned to the model at every call site.
async fn assert_peeks<S, C>(handle: &DequeHandle<S, C>) -> Result<bool>
where
    S: StateSession,
    C: Codec<Payload = Value>,
{
    if handle.peek_front().await? != handle.get(0).await? {
        return Ok(false);
    }
    let len = handle.len().await?;
    let back = if len == 0 {
        None
    } else {
        handle.get(len - 1).await?
    };
    Ok(handle.peek_back().await? == back)
}

/// Drains a fallible stream into a vector.
async fn drain<T, E>(stream: impl futures::Stream<Item = Result<T, E>>) -> Result<Vec<T>>
where
    E: Error + Send + Sync + 'static,
{
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Collects deque values in the selected direction.
async fn collect_deque<S, C>(handle: &DequeHandle<S, C>, dir: Direction) -> Result<Vec<Value>>
where
    S: StateSession,
    C: Codec<Payload = Value>,
{
    drain(handle.values().direction(dir).stream()).await
}

/// Asserts a map handle equals the model: `get` (with `contains_key` parity)
/// over the whole key pool and both stream directions (ascending for
/// `Forward`, descending for `Backward`).
async fn assert_map<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    model: &BTreeMap<i64, Value>,
    constraints: StreamConstraints,
) -> Result<bool>
where
    S: StateSession,
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
    if collect_map(handle, Direction::Backward).await? != descending {
        return Ok(false);
    }
    // Key and entry streams return the same live keys in the same order.
    let ascending_keys: Vec<i64> = model.keys().copied().collect();
    if collect_map_keys(handle, Direction::Forward).await? != ascending_keys {
        return Ok(false);
    }
    let descending_keys: Vec<i64> = model.keys().rev().copied().collect();
    if collect_map_keys(handle, Direction::Backward).await? != descending_keys {
        return Ok(false);
    }
    for (dir, expected) in [
        (Direction::Forward, ascending),
        (Direction::Backward, descending),
    ] {
        let expected: Vec<_> = expected
            .into_iter()
            .filter(|(key, _)| constraints.contains(*key, dir))
            .take(constraints.limit.map_or(usize::MAX, NonZeroUsize::get))
            .collect();
        let entries = handle
            .entries()
            .with_query(constraints.apply(KeyQuery::new().direction(dir)))
            .stream();
        let keys = handle
            .keys()
            .with_query(constraints.apply(KeyQuery::new().direction(dir)))
            .stream();
        if drain(entries).await? != expected
            || drain(keys).await? != expected.iter().map(|(key, _)| *key).collect::<Vec<_>>()
        {
            return Ok(false);
        }
    }
    Ok(handle.is_empty().await? == model.is_empty())
}

/// Collects map entries in the selected direction.
async fn collect_map<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    dir: Direction,
) -> Result<Vec<(i64, Value)>>
where
    S: StateSession,
{
    drain(handle.entries().direction(dir).stream()).await
}

/// Collects map keys in the selected direction.
async fn collect_map_keys<S>(
    handle: &MapHandle<S, I64KeyCodec, JsonCodec>,
    dir: Direction,
) -> Result<Vec<i64>>
where
    S: StateSession,
{
    drain(handle.keys().direction(dir).stream()).await
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

/// Identifies a read session independently from each staged event.
fn read_event(index: usize) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX - index as u128),
    }
}

mod runner;
pub(crate) use runner::*;
mod lifecycle;
pub(crate) use lifecycle::*;
mod batch;
pub(crate) use batch::*;
mod windows;
pub(crate) use windows::*;
mod eviction;
pub(crate) use eviction::*;
mod presence;
pub(crate) use presence::*;
mod metadata;
mod projection;
mod registration;
use registration::*;
mod interleave;
mod keyset;
pub(crate) use interleave::*;
