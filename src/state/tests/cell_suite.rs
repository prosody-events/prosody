//! Backend-generic property suite for the provisional-cell durability model,
//! driven through [`PartitionStateStore`] over a real [`CellStore`] backend.
//!
//! The flagship is **crash-recovery equivalence**: a generated trace of events
//! stages provisional writes and then resolves them one of five ways — clean
//! promote, clean inline rollback, or a crash at one of three points followed
//! by recovery through the sweep *or* first-touch. After every event the
//! durable committed projection must equal a deliberately simple model,
//! whichever resolution path ran. The companion runners pin the
//! prev-is-committed / reader-projection invariant and sweep idempotence.
//!
//! The suite is generic over the [`CellStore`] backend (`run_*` take a cloned
//! backing store that shares durable state), so memory and Cassandra prove the
//! same invariants from one body — backend parity by transitivity through the
//! model (each backend tracks the model op-for-op).
//!
//! Faithfulness to production:
//!
//! * **Crash = a cold store over warm durable state.** Recovery builds a
//!   *fresh* [`PartitionStateStore`] over a clone of the same backing store, so
//!   the committed-value cache and status map start empty — the durable backend
//!   is the only thing that survives, exactly as after a process restart. We
//!   never forget or leak a value to fake a crash (the CLAUDE.md memory rule).
//! * **`prev` comes from `store.committed`**, never minted, so the staged
//!   `prev` is always the resolved committed base — the same path `finalize`
//!   uses (`session::ValueStateSession::finalize`).
//! * **A small key pool** so events repeatedly hit the same collections,
//!   exercising overwrite of a just-resolved cell and the implicit
//!   resolution-on-read inside `committed`.

use super::super::cell::{Cell, ProvisionalCell, ProvisionalWrite};
use super::super::memory::MemoryCommittedCache;
use super::super::oracle::CommitOracle;
use super::super::partition_store::PartitionStateStore;
use super::super::registry::CollectionDefRegistry;
use super::super::store::CellStore;
use super::super::value::ValueKind;
use super::super::{
    CollectionId, CollectionRef, CommitDecision, EventRef, StateKey, StateName, StateType,
};
use super::value_suite::{MAX_TRACE_OPS, bytes, capped_vec};
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::Stream;
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Distinct collections a trace cycles through. Small so events collide on the
/// same cell and exercise overwrite + resolution-on-read.
const POOL: u8 = 3;

/// A committed-marker oracle backed by an in-memory set of dedup ids.
///
/// Models the durable deduplication store: [`record_message`](Self) writes the
/// marker (the durable "this event committed" fact), and `resolve` answers
/// `Committed` iff the staged event's marker is present. The set is shared
/// across clones, so it survives the simulated crash exactly as the durable
/// dedup store does.
#[derive(Clone, Default)]
struct ScriptedOracle {
    committed: Arc<Mutex<HashSet<Uuid>>>,
}

impl CommitOracle for ScriptedOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.committed.lock().insert(dedup_id);
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        let committed = match event {
            EventRef::Message { dedup_id } => self.committed.lock().contains(&dedup_id),
            EventRef::Timer(_) => false,
        };
        Ok(if committed {
            CommitDecision::Committed
        } else {
            CommitDecision::NotCommitted
        })
    }
}

type Store<S> = PartitionStateStore<ValueKind, S, ScriptedOracle, MemoryCommittedCache>;

/// One Value mutation staged by an event.
#[derive(Clone, Copy, Debug)]
enum Mutation {
    Set(u8),
    Clear,
}

impl Mutation {
    /// The staged outcome (`Set` → present bytes, `Clear` → known-absent).
    fn value(self) -> Option<Bytes> {
        match self {
            Self::Set(b) => Some(bytes(b)),
            Self::Clear => None,
        }
    }
}

impl Arbitrary for Mutation {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Set(u8::arbitrary(g))
        } else {
            Self::Clear
        }
    }
}

/// How an event resolved — the five distinct cell outcomes.
///
/// `marker_flushed` is the single bit that decides committed-vs-rolled-back;
/// the crash variants additionally defer resolution to recovery, and
/// `CrashMidFanOut` stages only a prefix of the event's writes (the marker
/// never flushes, so every staged cell rolls back and the rest are untouched).
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Committed and promoted inline (the hot path).
    CleanCommitted,
    /// Staged then rolled back inline before any marker flush (abandon).
    CleanRolledBack,
    /// All cells staged, marker never flushed, crash → recover (rolls back).
    CrashAfterStage,
    /// All cells staged, marker flushed, crash → recover (promotes).
    CrashAfterMarker,
    /// Only a prefix staged, marker never flushed, crash → recover.
    CrashMidFanOut,
}

impl Outcome {
    fn marker_flushed(self) -> bool {
        matches!(self, Self::CleanCommitted | Self::CrashAfterMarker)
    }

    fn mid_fan_out(self) -> bool {
        matches!(self, Self::CrashMidFanOut)
    }
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[
            Self::CleanCommitted,
            Self::CleanRolledBack,
            Self::CrashAfterStage,
            Self::CrashAfterMarker,
            Self::CrashMidFanOut,
        ])
        .copied()
        .unwrap_or(Self::CleanCommitted)
    }
}

/// One event: a set of per-collection mutations, an outcome, and the recovery
/// path to use when the outcome is a crash.
#[derive(Clone, Debug)]
struct TraceEvent {
    writes: Vec<(u8, Mutation)>,
    outcome: Outcome,
    recover_by_sweep: bool,
}

impl Arbitrary for TraceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let writes = capped_vec::<(u8, Mutation)>(g, POOL as usize)
            .into_iter()
            .map(|(coll, m)| (coll % POOL, m))
            .collect();
        Self {
            writes,
            outcome: Outcome::arbitrary(g),
            recover_by_sweep: bool::arbitrary(g),
        }
    }
}

/// A shrinkable trace of events over the key pool.
#[derive(Clone, Debug)]
pub(crate) struct Trace {
    events: Vec<TraceEvent>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shortening the trace is the highest-value reduction; quickcheck's
        // Vec shrinker drops and halves the event list.
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// Drives `trace` through a fresh partition store over `backing`, asserting the
/// committed projection equals the model after every event regardless of the
/// resolution path. Returns `true` iff every assertion held.
///
/// # Errors
///
/// Propagates backend / oracle errors raised during the run.
/// Collapses an event's writes to one per collection (an event stages a cell
/// at most once), keeping the last mutation — last-writer-wins.
fn collapse_writes(writes: Vec<(u8, Mutation)>) -> Vec<(u8, Mutation)> {
    let mut out: Vec<(u8, Mutation)> = Vec::new();
    for (coll, mutation) in writes {
        match out.iter_mut().find(|(c, _)| *c == coll) {
            Some(slot) => slot.1 = mutation,
            None => out.push((coll, mutation)),
        }
    }
    out
}

/// Asserts every pooled cell is resolved and projects its model value.
async fn assert_converged<S>(
    store: &Store<S>,
    ids: &[CollectionId<ValueKind>],
    model: &[Option<Bytes>],
) -> Result<bool>
where
    S: CellStore<ValueKind>,
{
    for (id, expected) in ids.iter().zip(model) {
        let cell = store.read_cell(id, &()).await?;
        if !matches!(cell, Cell::Resolved(_)) || cell.project_committed().cloned() != *expected {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Recovers a crashed event over `cold` (a store with a fresh cache + status
/// map) via the sweep or first-touch. Returns the cold store, or `None` if a
/// sweep reported an unresolved cell (a property failure).
async fn recover_cold<S>(
    cold: Store<S>,
    refs: &[CollectionRef<ValueKind>],
    ids: &[CollectionId<ValueKind>],
    by_sweep: bool,
    recovery_event: EventRef,
) -> Result<Option<Store<S>>>
where
    S: CellStore<ValueKind>,
{
    if by_sweep {
        for r in refs {
            if !cold.sweep_collection(r).await? {
                return Ok(None);
            }
        }
    } else {
        // First-touch under a fresh event so own-event never short-circuits.
        for id in ids {
            cold.committed_value(id, &(), recovery_event).await?;
        }
    }
    Ok(Some(cold))
}

pub(crate) async fn run_crash_equivalence_trace<S>(backing: S, trace: Trace) -> Result<bool>
where
    S: CellStore<ValueKind>,
{
    let oracle = ScriptedOracle::default();
    let registry = Arc::new(CollectionDefRegistry::default());
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let names: Vec<StateName> = (0..POOL)
        .map(|c| StateName::try_new(format!("c{c}")))
        .collect::<Result<_, _>>()?;
    let ids: Vec<CollectionId<ValueKind>> = names
        .iter()
        .map(|n| CollectionId::new(state_key.clone(), StateType::Application, n.clone()))
        .collect();
    let refs: Vec<CollectionRef<ValueKind>> = ids
        .iter()
        .map(|id| CollectionRef::new(id.clone(), None))
        .collect();
    let make_store = |backing: &S| {
        Store::new(
            backing.clone(),
            oracle.clone(),
            MemoryCommittedCache::new(),
            registry.clone(),
        )
    };

    let mut store = make_store(&backing);
    // The model committed value of every pooled collection.
    let mut model: Vec<Option<Bytes>> = vec![None; POOL as usize];

    for (index, ev) in trace.events.into_iter().enumerate() {
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };

        let writes = collapse_writes(ev.writes);
        let staged_count = if ev.outcome.mid_fan_out() {
            writes.len().saturating_sub(1)
        } else {
            writes.len()
        };
        let staged = &writes[..staged_count];

        // Stage each cell over its committed base (the prev-is-committed
        // invariant at stage time: no provisional cell lingers between events).
        let mut prevs: Vec<Option<Bytes>> = Vec::with_capacity(staged.len());
        for &(coll, mutation) in staged {
            let prev = store.committed(&ids[coll as usize], &(), event).await?;
            let prev_value = prev.get().cloned();
            if prev_value != model[coll as usize] {
                return Ok(false);
            }
            prevs.push(prev_value);
            store
                .write_provisional(
                    &refs[coll as usize],
                    &(),
                    &ProvisionalWrite::new(mutation.value(), prev, event),
                )
                .await?;
        }

        // Marker strictly after staging; advance the model for committed cells.
        if ev.outcome.marker_flushed() {
            oracle.record_message(dedup_id).await?;
            for &(coll, mutation) in staged {
                model[coll as usize] = mutation.value();
            }
        }

        // Resolve along the outcome's path.
        match ev.outcome {
            Outcome::CleanCommitted => {
                for &(coll, mutation) in staged {
                    store
                        .promote(&refs[coll as usize], &(), mutation.value().as_ref())
                        .await?;
                }
            }
            Outcome::CleanRolledBack => {
                for (&(coll, _), prev) in staged.iter().zip(&prevs) {
                    store
                        .rollback_provisional(&refs[coll as usize], &(), prev.as_ref())
                        .await?;
                }
            }
            _ => {
                let recovery_event = EventRef::Message {
                    dedup_id: Uuid::from_u128(u128::MAX - index as u128),
                };
                match recover_cold(
                    make_store(&backing),
                    &refs,
                    &ids,
                    ev.recover_by_sweep,
                    recovery_event,
                )
                .await?
                {
                    Some(cold) => store = cold,
                    None => return Ok(false),
                }
            }
        }

        if !assert_converged(&store, &ids, &model).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// One projection-trace step: stage a cell (when it is resolved) or resolve the
/// in-flight one (when it is provisional). The interpretation makes every step
/// valid regardless of generation, so traces never desync from the model.
#[derive(Clone, Copy, Debug)]
struct ProjOp {
    coll: u8,
    mutation: Mutation,
    commit: bool,
}

impl Arbitrary for ProjOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            coll: u8::arbitrary(g) % POOL,
            mutation: Mutation::arbitrary(g),
            commit: bool::arbitrary(g),
        }
    }
}

/// A shrinkable projection trace.
#[derive(Clone, Debug)]
pub(crate) struct ProjTrace {
    ops: Vec<ProjOp>,
}

impl Arbitrary for ProjTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// In-flight provisional state of one cell in a projection trace: the staged
/// data, the committed base it superseded, and whether the event committed.
type Pending = Option<(Option<Bytes>, Option<Bytes>, bool)>;

/// Drives staged / resolved transitions and asserts the **pure projection** is
/// sound at every point: a provisional cell projects its `prev` (the committed
/// value before the in-flight event — stale by exactly that event, the
/// external `StateReader` contract), a resolved cell projects the committed
/// value, and a provisional cell's `prev` always equals the model committed
/// base (prev-is-committed).
///
/// # Errors
///
/// Propagates backend / oracle errors raised during the run.
pub(crate) async fn run_projection_trace<S>(backing: S, trace: ProjTrace) -> Result<bool>
where
    S: CellStore<ValueKind>,
{
    let oracle = ScriptedOracle::default();
    let registry = Arc::new(CollectionDefRegistry::default());
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let names: Vec<StateName> = (0..POOL)
        .map(|c| StateName::try_new(format!("c{c}")))
        .collect::<Result<_, _>>()?;
    let store = Store::new(
        backing,
        oracle.clone(),
        MemoryCommittedCache::new(),
        registry,
    );
    let id_of = |coll: u8| {
        CollectionId::new(
            state_key.clone(),
            StateType::Application,
            names[coll as usize].clone(),
        )
    };
    let ref_of = |coll: u8| CollectionRef::new(id_of(coll), None);

    // Per cell: the committed value, and (when in flight) the staged data, the
    // committed base it superseded, and whether that event committed.
    let mut committed: Vec<Option<Bytes>> = vec![None; POOL as usize];
    let mut pending: Vec<Pending> = vec![None; POOL as usize];

    for (index, op) in trace.ops.into_iter().enumerate() {
        let coll = op.coll;
        let slot = coll as usize;
        match pending[slot].take() {
            None => {
                // Resolved cell → stage a provisional write over the committed base.
                let event = EventRef::Message {
                    dedup_id: Uuid::from_u128(index as u128),
                };
                let prev = store.committed(&id_of(coll), &(), event).await?;
                let prev_value = prev.get().cloned();
                // prev-is-committed at stage time.
                if prev_value != committed[slot] {
                    return Ok(false);
                }
                let data = op.mutation.value();
                store
                    .write_provisional(
                        &ref_of(coll),
                        &(),
                        &ProvisionalWrite::new(data.clone(), prev, event),
                    )
                    .await?;
                pending[slot] = Some((data, prev_value, op.commit));
            }
            Some((data, prev, commit)) => {
                // Provisional cell → resolve durably: promote a committed event,
                // roll a non-committed one back to its base. (A warm-cache
                // `committed_value` would short-circuit without touching the
                // durable cell — promote/rollback are the durable arms.)
                if commit {
                    store.promote(&ref_of(coll), &(), data.as_ref()).await?;
                    committed[slot] = data;
                } else {
                    store
                        .rollback_provisional(&ref_of(coll), &(), prev.as_ref())
                        .await?;
                    committed[slot] = prev;
                }
            }
        }

        // Projection soundness for every cell after the step.
        for probe in 0..POOL {
            let probe_slot = probe as usize;
            let read = store.read_cell(&id_of(probe), &()).await?;
            match &pending[probe_slot] {
                Some((..)) => {
                    // In flight: provisional, projecting the committed base, and
                    // carrying that base as `prev`.
                    let Cell::Provisional(p) = &read else {
                        return Ok(false);
                    };
                    if read.project_committed().cloned() != committed[probe_slot]
                        || p.prev().cloned() != committed[probe_slot]
                    {
                        return Ok(false);
                    }
                }
                None => {
                    if !matches!(read, Cell::Resolved(_))
                        || read.project_committed().cloned() != committed[probe_slot]
                    {
                        return Ok(false);
                    }
                }
            }
        }
    }

    Ok(true)
}

/// One overwrite-trace step: a mutation on a pooled collection, and whether the
/// event commits. A non-committing event's provisional cell must roll back to
/// its committed base when its successor overwrites it; a committing one's must
/// promote to its data.
#[derive(Clone, Copy, Debug)]
struct OverwriteOp {
    coll: u8,
    mutation: Mutation,
    commit: bool,
}

impl Arbitrary for OverwriteOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            coll: u8::arbitrary(g) % POOL,
            mutation: Mutation::arbitrary(g),
            commit: bool::arbitrary(g),
        }
    }
}

/// A shrinkable overwrite trace.
#[derive(Clone, Debug)]
pub(crate) struct OverwriteTrace {
    ops: Vec<OverwriteOp>,
}

impl Arbitrary for OverwriteTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// Drives a sequence of events that NEVER promote or roll back explicitly: each
/// event reads its committed base through a **cold** store (fresh cache), so a
/// predecessor's still-provisional cell is resolved through the oracle on read
/// — the implicit-overwrite / first-touch path. After every step the staged
/// `prev` must equal the model committed base (prev-is-committed across the
/// overwrite), and at the end every cell, resolved purely by the next read,
/// must equal the model. Both oracle arms run: a committing predecessor
/// promotes to its `data`, a non-committing one rolls back to its `prev`.
///
/// This generalizes the `foreign_committed` / `foreign_uncommitted` example
/// tests into a property over random overwrite sequences, and — unlike the
/// crash-equivalence runner, which resolves each event explicitly — exercises
/// resolution *only* implicitly, as the side effect of the successor's read.
///
/// # Errors
///
/// Propagates backend / oracle errors raised during the run.
pub(crate) async fn run_overwrite_trace<S>(backing: S, trace: OverwriteTrace) -> Result<bool>
where
    S: CellStore<ValueKind>,
{
    let oracle = ScriptedOracle::default();
    let registry = Arc::new(CollectionDefRegistry::default());
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let ids: Vec<CollectionId<ValueKind>> = (0..POOL)
        .map(|c| {
            Ok(CollectionId::new(
                state_key.clone(),
                StateType::Application,
                StateName::try_new(format!("c{c}"))?,
            ))
        })
        .collect::<Result<_>>()?;
    let refs: Vec<CollectionRef<ValueKind>> = ids
        .iter()
        .map(|id| CollectionRef::new(id.clone(), None))
        .collect();
    // A fresh cold store over the shared backing: reads never hit a warm cache,
    // so every overwrite resolves its predecessor's provisional cell durably.
    let fresh = || {
        Store::new(
            backing.clone(),
            oracle.clone(),
            MemoryCommittedCache::new(),
            registry.clone(),
        )
    };

    let mut model: Vec<Option<Bytes>> = vec![None; POOL as usize];
    for (index, op) in trace.ops.into_iter().enumerate() {
        let coll = op.coll as usize;
        let dedup_id = Uuid::from_u128(index as u128);
        let event = EventRef::Message { dedup_id };
        let store = fresh();

        // Reading the committed base resolves any still-provisional predecessor
        // through the oracle (implicit overwrite). It must equal the model.
        let prev = store.committed(&ids[coll], &(), event).await?;
        if prev.get().cloned() != model[coll] {
            return Ok(false);
        }
        store
            .write_provisional(
                &refs[coll],
                &(),
                &ProvisionalWrite::new(op.mutation.value(), prev, event),
            )
            .await?;
        if op.commit {
            oracle.record_message(dedup_id).await?;
            model[coll] = op.mutation.value();
        }
    }

    // Every last provisional cell, resolved only by this final read, converges
    // to the model.
    let store = fresh();
    for (i, id) in ids.iter().enumerate() {
        let final_event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - i as u128),
        };
        if store.committed_value(id, &(), final_event).await? != model[i] {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A [`CellStore`] decorator that counts every durable operation, for the
/// op-budget and sweep-idempotence pins. It delegates to `inner` and shares
/// durable state through its `Clone` (the counters ride an `Arc`).
#[derive(Clone, Default)]
pub(crate) struct CountingCellStore<S> {
    inner: S,
    counts: Arc<OpCounts>,
}

#[derive(Default)]
struct OpCounts {
    read_cell: AtomicUsize,
    write_provisional: AtomicUsize,
    write_resolved: AtomicUsize,
    mark_resolved: AtomicUsize,
}

impl<S> CountingCellStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
        }
    }

    fn read_cells(&self) -> usize {
        self.counts.read_cell.load(Ordering::Relaxed)
    }

    fn provisional_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
    }

    fn resolved_writes(&self) -> usize {
        self.counts.write_resolved.load(Ordering::Relaxed)
    }

    fn promotes(&self) -> usize {
        self.counts.mark_resolved.load(Ordering::Relaxed)
    }

    /// Total durable mutations (excludes reads).
    fn durable_writes(&self) -> usize {
        self.provisional_writes() + self.resolved_writes() + self.promotes()
    }

    fn reset(&self) {
        self.counts.read_cell.store(0, Ordering::Relaxed);
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
    }
}

impl<S> CellStore<ValueKind> for CountingCellStore<S>
where
    S: CellStore<ValueKind>,
{
    type Error = S::Error;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        addr: &'a (),
    ) -> Result<Cell, Self::Error> {
        self.counts.read_cell.fetch_add(1, Ordering::Relaxed);
        self.inner.read_cell(collection, addr).await
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner.provisional_cells(collection)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
        write: &'a ProvisionalWrite,
    ) -> Result<(), Self::Error> {
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner.write_provisional(collection, addr, write).await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
        data: Option<&'a Bytes>,
    ) -> Result<(), Self::Error> {
        self.counts.write_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.write_resolved(collection, addr, data).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addr: &'a (),
    ) -> Result<(), Self::Error> {
        self.counts.mark_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.mark_resolved(collection, addr).await
    }
}

#[cfg(test)]
mod op_budget {
    use super::super::super::memory::MemoryCellStore;
    use super::*;

    type Backing = CountingCellStore<MemoryCellStore>;

    struct Fixture {
        store: Store<Backing>,
        counts: Backing,
        oracle: ScriptedOracle,
        refs: Vec<CollectionRef<ValueKind>>,
    }

    fn setup() -> Result<Fixture> {
        let counts = CountingCellStore::new(MemoryCellStore::new());
        let oracle = ScriptedOracle::default();
        let store = Store::new(
            counts.clone(),
            oracle.clone(),
            MemoryCommittedCache::new(),
            Arc::new(CollectionDefRegistry::default()),
        );
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let refs = (0..POOL)
            .map(|c| {
                let id = CollectionId::new(
                    state_key.clone(),
                    StateType::Application,
                    StateName::try_new(format!("c{c}"))?,
                );
                Ok(CollectionRef::new(id, None))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Fixture {
            store,
            counts,
            oracle,
            refs,
        })
    }

    /// The hot path for one event over `POOL` warm collections costs exactly
    /// one provisional stage and one promote per cell, and **zero reads** —
    /// the committed base is served from the cache. No pending index, no
    /// read-back.
    #[tokio::test]
    async fn warm_event_budget_is_one_stage_and_one_promote_per_cell() -> Result<()> {
        let Fixture {
            store,
            counts,
            refs,
            ..
        } = setup()?;

        // Warm the cache with a committed value for every collection.
        for r in &refs {
            store.write_resolved(r, &(), Some(&bytes(0))).await?;
        }
        counts.reset();

        // One event stages and promotes each cell, reading prev from the cache.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        for r in &refs {
            let prev = store.committed(r.id(), &(), event).await?;
            let data = bytes(1);
            store
                .write_provisional(
                    r,
                    &(),
                    &ProvisionalWrite::new(Some(data.clone()), prev, event),
                )
                .await?;
            store.promote(r, &(), Some(&data)).await?;
        }

        assert_eq!(counts.read_cells(), 0, "warm reads must hit the cache");
        assert_eq!(counts.provisional_writes(), POOL as usize);
        assert_eq!(counts.promotes(), POOL as usize);
        assert_eq!(counts.resolved_writes(), 0);
        Ok(())
    }

    /// The first sweep over provisional cells resolves them; a second sweep
    /// performs **zero durable writes** — the status map records every cell
    /// resolved and short-circuits before touching the backend.
    #[tokio::test]
    async fn second_sweep_is_a_no_op() -> Result<()> {
        let Fixture {
            store,
            counts,
            oracle,
            refs,
        } = setup()?;

        // Stage a committed provisional cell on every collection.
        for (i, r) in refs.iter().enumerate() {
            let dedup_id = Uuid::from_u128(i as u128);
            let event = EventRef::Message { dedup_id };
            let prev = store.committed(r.id(), &(), event).await?;
            store
                .write_provisional(r, &(), &ProvisionalWrite::new(Some(bytes(7)), prev, event))
                .await?;
            // Record the marker so the oracle promotes on sweep.
            oracle.record_message(dedup_id).await?;
        }

        // First sweep resolves every cell.
        for r in &refs {
            assert!(store.sweep_collection(r).await?);
        }
        assert!(
            counts.durable_writes() > 0,
            "first sweep must resolve provisional cells"
        );

        // Second sweep is a no-op: status says resolved, nothing touched.
        counts.reset();
        for r in &refs {
            assert!(store.sweep_collection(r).await?);
        }
        assert_eq!(counts.durable_writes(), 0);
        assert_eq!(counts.read_cells(), 0);
        Ok(())
    }
}
