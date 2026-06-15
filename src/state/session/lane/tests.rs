//! Direct `Lane<CounterKind>` trace + model property.
//!
//! This drives the **real generic [`Lane`](super::Lane) body**
//! (`stage`/`resolve`/`reset`) over the `#[cfg(test)]` [`CounterKind`] fixture
//! — a kind deliberately unlike Value on the axes the Value lane cannot
//! exercise: a non-`()` [`u32`] address (many addressed cells per collection,
//! incl. a reserved header cell), a non-LWW additive `combine`, and an `apply`
//! that reads the committed base. A generated `Vec<Txn>` of mutate / stage /
//! re-stage / commit / abort / reset operations drives the lane while a plain
//! `HashMap<u32, i64>` model tracks the expected committed counters;
//! equivalence is asserted after every operation. Three collection-grain
//! invariants ride along as model checks: exactly one batched
//! `write_provisional` per stage, one `mark_resolved` per commit, one
//! `write_resolved` per rollback (the store call-counters), and an in-place
//! re-stage recomputes the identical provisional write (the own-event-`prev`
//! idempotency contract `Lane::stage` relies on).
//!
//! Because every staged cell lives in one collection, the per-collection batch
//! is one underlying store call regardless of cell count — the bulk-apply pin.

use super::{Lane, Resolve};
use crate::state::cell::ProvisionalCell;
use crate::state::identity::CollectionId;
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::PartitionStateStore;
use crate::state::proof_kind::{
    CounterKind, HEADER_ADDR, MemoryCounterCache, MemoryCounterStore, decode_i64, encode_delta,
};
use crate::state::registry::CollectionDefRegistry;
use crate::state::session::sealed::ApplyOutcome;
use crate::state::store::CellStore;
use crate::state::{CommitDecision, EventRef, StateKey, StateName, StateType};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use uuid::Uuid;

/// The data addresses the trace touches, plus the reserved header cell — kept
/// small so traces are dense and shrink well.
const ADDRS: [u32; 4] = [0, 1, 2, HEADER_ADDR];

/// An oracle that never reports an event committed: a provisional cell left by
/// a `reset`-skipped event resolves to its committed `prev` (the unchanged
/// base) when a later stage reads over it. Commits promote directly, so the
/// resolve half is only consulted for those stranded-by-skip cells.
#[derive(Clone)]
struct NeverCommitted;

impl CommitOracle for NeverCommitted {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::NotCommitted)
    }
}

/// One buffered mutation: an additive set, or a clear (reset-to-zero).
#[derive(Clone, Debug)]
enum Mutation {
    Set { addr: u32, delta: i64 },
    Clear { addr: u32 },
}

impl Arbitrary for Mutation {
    fn arbitrary(g: &mut Gen) -> Self {
        let addr = ADDRS[usize::from(u8::arbitrary(g) % 4)];
        if u8::arbitrary(g) % 5 == 0 {
            Self::Clear { addr }
        } else {
            Self::Set {
                addr,
                delta: i64::from(i16::arbitrary(g)),
            }
        }
    }
}

/// Terminal outcome of one transaction.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    Commit,
    Abort,
    Reset,
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Reset,
            1 => Self::Abort,
            _ => Self::Commit,
        }
    }
}

/// One transaction over the lane: buffered mutations, an optional in-place
/// re-stage (the idempotency arm), and a terminal outcome.
#[derive(Clone, Debug)]
struct Txn {
    muts: Vec<Mutation>,
    restage: bool,
    outcome: Outcome,
}

impl Arbitrary for Txn {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            muts: Vec::<Mutation>::arbitrary(g).into_iter().take(6).collect(),
            restage: bool::arbitrary(g),
            outcome: Outcome::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let restage = self.restage;
        let outcome = self.outcome;
        Box::new(self.muts.shrink().map(move |muts| Self {
            muts,
            restage,
            outcome,
        }))
    }
}

/// A shrinkable trace of transactions over one collection on one key.
#[derive(Clone, Debug)]
struct Trace {
    txns: Vec<Txn>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            txns: Vec::<Txn>::arbitrary(g).into_iter().take(20).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.txns.shrink().map(|txns| Self { txns }))
    }
}

/// Folds a transaction's mutations into one combined `(reset, delta)` per
/// address — the plain-arithmetic analog of the lane's `combine` fold, an
/// independent oracle (the kind's own `combine`/`apply` are property-tested
/// separately in `proof_kind`).
fn fold_muts(muts: &[Mutation]) -> HashMap<u32, (bool, i64)> {
    let mut folded: HashMap<u32, (bool, i64)> = HashMap::new();
    for m in muts {
        match *m {
            Mutation::Set { addr, delta } => {
                let entry = folded.entry(addr).or_insert((false, 0));
                entry.1 = entry.1.wrapping_add(delta);
            }
            Mutation::Clear { addr } => {
                folded.insert(addr, (true, 0));
            }
        }
    }
    folded
}

/// The committed counter at `addr` as the raw durable store projects it (`prev`
/// for a provisional cell, `data` for a resolved one), decoded — absent ⇒ 0.
async fn committed_at(
    store: &MemoryCounterStore,
    id: &CollectionId<CounterKind>,
    addr: u32,
) -> i64 {
    let cell = match store.read_cell(id, &addr).await {
        Ok(cell) => cell,
        Err(never) => match never {},
    };
    cell.project_committed().map_or(0, |b| decode_i64(b))
}

/// Whether every address's durable committed projection matches the model.
async fn matches_model(
    store: &MemoryCounterStore,
    id: &CollectionId<CounterKind>,
    model: &HashMap<u32, i64>,
) -> bool {
    for addr in ADDRS {
        if committed_at(store, id, addr).await != model.get(&addr).copied().unwrap_or(0) {
            return false;
        }
    }
    true
}

/// The provisional cell's staged `data` at `addr`, decoded — `None` if the cell
/// is not provisional.
async fn provisional_data(
    store: &MemoryCounterStore,
    id: &CollectionId<CounterKind>,
    addr: u32,
) -> Option<i64> {
    let cell = match store.read_cell(id, &addr).await {
        Ok(cell) => cell,
        Err(never) => match never {},
    };
    cell.as_provisional()
        .map(|p| ProvisionalCell::data(p).map_or(0, |b| decode_i64(b)))
}

/// The lane under test: a `CounterKind` lane over the in-memory counter store,
/// the never-committed oracle, and the in-memory counter cache.
type CounterLane = Lane<CounterKind, MemoryCounterStore, NeverCommitted, MemoryCounterCache>;

/// The outcome `resolve` reports given whether the stage recorded a staged set.
fn expected_outcome(staged: bool) -> ApplyOutcome {
    if staged {
        ApplyOutcome::Resolved
    } else {
        ApplyOutcome::NothingStaged
    }
}

/// In-place re-stage (no reset): one more batched provisional write,
/// recomputing the identical staged data over the own-event `prev` (the
/// idempotency contract `Lane::stage` relies on for a transient retry). Returns
/// `false` on violation.
async fn verify_restage(
    lane: &CounterLane,
    store: &MemoryCounterStore,
    id: &CollectionId<CounterKind>,
    registry: &CollectionDefRegistry,
    event: EventRef,
    folded: &HashMap<u32, (bool, i64)>,
) -> Result<bool> {
    let mut before = HashMap::new();
    for &addr in folded.keys() {
        before.insert(addr, provisional_data(store, id, addr).await);
    }
    let pw0 = store.provisional_write_calls();
    lane.stage(event, registry)
        .await
        .map_err(|e| eyre!("re-stage failed: {e}"))?;
    if store.provisional_write_calls() - pw0 != 1 {
        return Ok(false);
    }
    for &addr in folded.keys() {
        if provisional_data(store, id, addr).await != before[&addr] {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Resolves the transaction's outcome and advances the model: commit promotes
/// (one batched `mark_resolved`, folds the combined op over the base), abort
/// rolls back (one batched `write_resolved`, model unchanged), reset discards
/// (model unchanged). Returns `false` on a call-count or outcome violation.
async fn apply_outcome(
    lane: &CounterLane,
    store: &MemoryCounterStore,
    outcome: Outcome,
    staged: bool,
    folded: &HashMap<u32, (bool, i64)>,
    model: &mut HashMap<u32, i64>,
) -> bool {
    match outcome {
        Outcome::Commit => {
            let mr_before = store.mark_resolved_calls();
            let out = lane.resolve(Resolve::Promote).await;
            if store.mark_resolved_calls() - mr_before != usize::from(staged) {
                return false;
            }
            for (&addr, &(reset, delta)) in folded {
                let base = if reset {
                    0
                } else {
                    model.get(&addr).copied().unwrap_or(0)
                };
                model.insert(addr, base.wrapping_add(delta));
            }
            out == expected_outcome(staged)
        }
        Outcome::Abort => {
            let wr_before = store.write_resolved_calls();
            let out = lane.resolve(Resolve::Rollback).await;
            store.write_resolved_calls() - wr_before == usize::from(staged)
                && out == expected_outcome(staged)
        }
        // Discards dirty + staged set; any provisional already written lingers
        // but projects its `prev` (the unchanged base).
        Outcome::Reset => {
            lane.reset();
            true
        }
    }
}

/// Drives the trace through the real lane, checking model equivalence and the
/// collection-grain call-count / idempotency invariants after each step.
async fn run(trace: Trace) -> Result<bool> {
    let store = MemoryCounterStore::new();
    let registry = Arc::new(CollectionDefRegistry::new(None));
    // One partition store (shared durable store + cache + status), cloned into a
    // fresh `Lane` per transaction — mirroring the manager minting a fresh
    // per-event session over its one partition store, so each event's dirty
    // workspace starts clean while committed state persists across events.
    let partition = PartitionStateStore::new(
        store.clone(),
        NeverCommitted,
        MemoryCounterCache::new(),
        registry.clone(),
    );
    let id = CollectionId::<CounterKind>::new(
        StateKey::new(Uuid::new_v4(), Arc::from("k")),
        StateType::Application,
        StateName::try_new("tally")?,
    );
    let mut model: HashMap<u32, i64> = HashMap::new();

    for (index, txn) in trace.txns.into_iter().enumerate() {
        let lane = Lane::new(partition.clone());
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128 + 1),
        };
        let folded = fold_muts(&txn.muts);

        for m in &txn.muts {
            match *m {
                Mutation::Set { addr, delta } => {
                    lane.set_cell(&id, &addr, &encode_delta(delta)).await;
                }
                Mutation::Clear { addr } => lane.clear_cell(&id, &addr).await,
            }
            // Mutations buffer only: committed projection is unchanged.
            if !matches_model(&store, &id, &model).await {
                return Ok(false);
            }
        }

        // Stage: one batched provisional write iff anything staged; the
        // committed projection (prev) still equals the model afterward.
        let pw_before = store.provisional_write_calls();
        let staged = lane
            .stage(event, &registry)
            .await
            .map_err(|e| eyre!("stage failed: {e}"))?;
        if store.provisional_write_calls() - pw_before != usize::from(staged)
            || !matches_model(&store, &id, &model).await
        {
            return Ok(false);
        }

        if txn.restage
            && staged
            && !verify_restage(&lane, &store, &id, &registry, event, &folded).await?
        {
            return Ok(false);
        }

        if !apply_outcome(&lane, &store, txn.outcome, staged, &folded, &mut model).await
            || !matches_model(&store, &id, &model).await
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Invariant: the generic lane body, driven over a non-Value kind, keeps the
/// durable committed projection equal to a plain per-address counter model
/// across arbitrary mutate/stage/re-stage/commit/abort/reset traces — while
/// staging, promoting, and rolling back each in exactly one batched store call
/// per collection and recomputing an identical write on an in-place re-stage.
#[test]
fn prop_lane_counter_lifecycle_matches_model() {
    fn prop(trace: Trace) -> TestResult {
        match executor::block_on(run(trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("lane diverged from the counter model or an invariant"),
            Err(error) => TestResult::error(format!("trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}
