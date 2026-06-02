//! Reusable property-test fixture for keyed Value store implementations.
//!
//! The trace runners are parametric over any [`DurableWalStore<ValueKind>`] +
//! [`DirectApplyStore<ValueKind>`] and any [`ValueStore`] +
//! [`PendingOpSource<ValueKind>`], so every backend (memory, Cassandra,
//! Fjall) can share the same property-test machinery.
//!
//! Three runners are exposed:
//!
//! * [`run_trace`] — drives a [`Trace`] under [`CommitMode::Wal`] against a
//!   model and asserts the per-op visibility, pending-ops, and applied
//!   invariants on every transition.
//! * [`run_idempotence_trace`] — additionally asserts that the durable
//!   resolution APIs return [`StoreOutcome::NoOp`] when invoked a second time
//!   on the just-finished event.
//! * [`run_direct_trace`] — drives a [`DirectTrace`] under
//!   [`CommitMode::Direct`] and asserts the partition never observes a sealed
//!   WAL, per the design summary's Direct-mode invariant.

use super::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, TransactionValueStore,
    TransactionValueStoreError, ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionRef, CommitDecision, CommitMode, DurableState, EventRef, LocalTx, Read,
    StateKey, StateName, StateType, StoreOutcome, TimerEventRef, ValueKind, ValueOp, ValueOverlay,
};
use crate::consumer::middleware::test_support::{MockEventContext, TimerOperation};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::middleware::{CollectionDefRegistry, recover_pending_entries};
use crate::state::oracle::CommitOracle;
use crate::state::pending::{PendingIndexScanner, PendingIndexStore};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use uuid::Uuid;

const TIMER_TYPE_POOL: [TimerType; 3] = [
    TimerType::Application,
    TimerType::DeferredMessage,
    TimerType::DeferredTimer,
];

const TIMER_TIME_MODULUS: u32 = 1_000_000;
const MAX_TRACE_OPS: usize = 40;
const MAX_TRACE_EVENTS: usize = 20;

/// Default TTL bound onto fixture [`CollectionRef`]s by [`collection_ref`].
///
/// One-hour duration — `Some(_)` so property runners exercise the
/// with-TTL query arm. Tests that need to assert the no-TTL arm must
/// construct `CollectionRef::new(id, None)` inline.
pub(crate) const TEST_TTL: Option<CompactDuration> = Some(CompactDuration::new(3_600));

/// Mock oracle behavior used by [`run_trace_with_policy`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum OraclePolicy {
    /// Oracle always returns [`super::CommitDecision::Committed`].
    AlwaysCommitted,

    /// Oracle always returns [`super::CommitDecision::NotCommitted`].
    AlwaysNotCommitted,
}

// Bundle traits collapse the four-trait bound carried by every helper in
// this file. Every body genuinely uses each constituent (the
// `TransactionValueStore` impl block demands the full set), so the
// bundles are sugar, not an over-bound.
pub(crate) trait DurableBundle:
    ValueStore<Error = <Self as DurableWalStore<ValueKind>>::Error>
    + DurableWalStore<ValueKind>
    + DirectApplyStore<ValueKind, Error = <Self as DurableWalStore<ValueKind>>::Error>
    + Clone
    + fmt::Debug
{
}

impl<T> DurableBundle for T where
    T: ValueStore<Error = <T as DurableWalStore<ValueKind>>::Error>
        + DurableWalStore<ValueKind>
        + DirectApplyStore<ValueKind, Error = <T as DurableWalStore<ValueKind>>::Error>
        + Clone
        + fmt::Debug
{
}

pub(crate) trait DirtyBundle:
    ValueStore + PendingOpSource<ValueKind, Error = <Self as ValueStore>::Error> + fmt::Debug
{
}

impl<T> DirtyBundle for T where
    T: ValueStore + PendingOpSource<ValueKind, Error = <T as ValueStore>::Error> + fmt::Debug
{
}

/// Drives a Value transaction trace in [`CommitMode::Wal`] against a model
/// and returns `true` when every per-op invariant held.
///
/// Delegates to [`run_trace_with_policy`] with
/// [`OraclePolicy::AlwaysCommitted`]; for traces without
/// [`TraceOp::Crash`] the policy is irrelevant.
///
/// # Errors
///
/// Propagates store/transaction errors; property mismatches return `Ok(false)`.
pub(crate) async fn run_trace<D, S, F>(durable: D, dirty_factory: F, trace: Trace) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    drive_wal_trace(
        durable,
        dirty_factory,
        trace,
        false,
        OraclePolicy::AlwaysCommitted,
    )
    .await
}

/// Adds to [`run_trace`] an idempotence check: every Commit/Abort that
/// reaches `Finished` is followed by a second `apply_sealed` and
/// `rollback_sealed` against the just-resolved event, both required to
/// return [`StoreOutcome::NoOp`].
///
/// # Errors
///
/// See [`run_trace`].
pub(crate) async fn run_idempotence_trace<D, S, F>(
    durable: D,
    dirty_factory: F,
    trace: Trace,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    drive_wal_trace(
        durable,
        dirty_factory,
        trace,
        true,
        OraclePolicy::AlwaysCommitted,
    )
    .await
}

/// Drives a trace with a configurable oracle policy. After a
/// [`TraceOp::Crash`] in the [`ModelPhase::Sealed`] phase, on the next
/// iteration the runner explicitly invokes
/// [`DurableWalStore::apply_sealed`] (for [`OraclePolicy::AlwaysCommitted`])
/// or [`DurableWalStore::rollback_sealed`] (for
/// [`OraclePolicy::AlwaysNotCommitted`]) so the durable and the model end
/// up in the same recovered state regardless of whether the durable is
/// wrapped in [`crate::state::recovering::RecoveringValueStore`]. Crash
/// outside [`ModelPhase::Sealed`] is treated as abort-without-cleanup: the
/// dirty workspace is **not** drained because a crash does not get to do
/// it.
///
/// # Errors
///
/// Propagates store/transaction errors; property mismatches return `Ok(false)`.
pub(crate) async fn run_trace_with_policy<D, S, F>(
    durable: D,
    dirty_factory: F,
    trace: Trace,
    policy: OraclePolicy,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    drive_wal_trace(durable, dirty_factory, trace, false, policy).await
}

/// Drives a direct-mode trace and asserts the partition is never sealed.
///
/// # Errors
///
/// See [`run_trace`].
pub(crate) async fn run_direct_trace<D, S, F>(
    durable: D,
    dirty_factory: F,
    trace: DirectTrace,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    let DirectTrace { ops, events } = trace;
    let collection_ref = collection_ref()?;
    let collection_id = collection_ref.id().clone();
    let mut tx_id = 1_u128;
    let mut event_idx = 0_usize;
    let mut tx = TransactionValueStore::new(
        durable.clone(),
        dirty_factory(),
        collection_ref.clone(),
        build_event_ref(event_at(&events, event_idx), tx_id),
        CommitMode::Direct,
    );

    for op in ops {
        if matches!(tx.local_tx(), LocalTx::Finished) {
            tx_id += 1;
            event_idx += 1;
            tx = TransactionValueStore::new(
                durable.clone(),
                dirty_factory(),
                collection_ref.clone(),
                build_event_ref(event_at(&events, event_idx), tx_id),
                CommitMode::Direct,
            );
        }

        apply_direct_op(&mut tx, &collection_id, op).await?;

        match durable.read_partition(&collection_id).await? {
            DurableState::Idle { .. } => {}
            DurableState::Sealed { .. } => return Ok(false),
        }
    }

    Ok(true)
}

pub(crate) fn collection_ref() -> Result<CollectionRef<ValueKind>> {
    Ok(CollectionRef::new(
        CollectionId::new(
            StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
            StateType::Application,
            StateName::try_new("profile")?,
        ),
        TEST_TTL,
    ))
}

pub(crate) fn inline(value: u8) -> StoredPayload {
    StoredPayload::Inline(Bytes::from(vec![value]))
}

/// Commit oracle that records every `resolve` call so the stale-pending
/// sweep can assert it was never consulted — a pending row over an Idle
/// partition has no sealed event to resolve.
#[derive(Clone, Default)]
struct NeverConsultedOracle {
    calls: Arc<AtomicUsize>,
}

#[derive(Debug, Error)]
#[error("commit oracle was unexpectedly consulted")]
struct NeverConsultedOracleError;

impl ClassifyError for NeverConsultedOracleError {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Permanent
    }
}

impl CommitOracle for NeverConsultedOracle {
    type Error = NeverConsultedOracleError;

    async fn resolve<'a>(
        &'a self,
        _collection: &'a CollectionId<ValueKind>,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(CommitDecision::Committed)
    }
}

/// Shared crash-recovery check, run from both the memory backend (always)
/// and Cassandra (integration). A pending-index row written without a WAL
/// reproduces a crash between `insert_pending` and the WAL write: the
/// partition is `Idle` but a pending row exists. The `StateRecovery` sweep
/// must delete that stale row, leave the partition `Idle`, clear the
/// recovery timer, and **never** consult the oracle (there is no sealed
/// event to resolve).
///
/// Reaching this requires a real pending index — when pending presence is
/// derived from `wal.is_some()` the crash state is unrepresentable and the
/// `Idle ⇒ delete_pending` sweep arm is dead code.
pub(crate) async fn run_stale_pending_index<D>(durable: D) -> Result<()>
where
    D: DurableWalStore<ValueKind>
        + PendingIndexStore<Error = <D as DurableWalStore<ValueKind>>::Error>
        + PendingIndexScanner,
{
    // Fresh segment per run so rows never collide with other iterations or
    // test functions (the Cassandra keyspace is shared).
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("stale-pending-key"));
    let id = CollectionId::<ValueKind>::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("stale")?,
    );

    // Crash between insert_pending and write_wal: pending row, Idle partition.
    PendingIndexStore::insert_pending::<ValueKind>(&durable, &id)
        .await
        .map_err(|e| eyre!("insert_pending failed: {e}"))?;

    match DurableWalStore::read_partition(&durable, &id)
        .await
        .map_err(|e| eyre!("read_partition failed: {e}"))?
    {
        DurableState::Idle { applied: None } => {}
        other => return Err(eyre!("expected Idle empty before sweep, got {other:?}")),
    }
    let before = collect_pending(&durable, &state_key).await?;
    assert_eq!(before, 1, "exactly one stale pending row before the sweep");

    let oracle = NeverConsultedOracle::default();
    let context = MockEventContext::new().with_timer_tracking();
    let registry = CollectionDefRegistry::new(None);
    recover_pending_entries(
        &context,
        &durable,
        &durable,
        &oracle,
        &registry,
        state_key.clone(),
    )
    .await
    .map_err(|e| eyre!("recovery sweep failed: {e}"))?;

    // Stale row deleted, partition still Idle, oracle untouched, timer cleared.
    let after = collect_pending(&durable, &state_key).await?;
    assert_eq!(after, 0, "stale pending row must be deleted by the sweep");
    match DurableWalStore::read_partition(&durable, &id)
        .await
        .map_err(|e| eyre!("read_partition failed: {e}"))?
    {
        DurableState::Idle { applied: None } => {}
        other => return Err(eyre!("expected Idle empty after sweep, got {other:?}")),
    }
    assert_eq!(
        oracle.calls.load(Ordering::Relaxed),
        0,
        "oracle must not be consulted for a stale pending row"
    );
    assert!(
        context
            .timer_operations()
            .iter()
            .any(|op| matches!(op, TimerOperation::ClearScheduled(TimerType::StateRecovery))),
        "the sweep must clear the StateRecovery timer"
    );
    Ok(())
}

/// Counts the pending-index rows on `state_key` via the streamed scanner.
async fn collect_pending<D>(durable: &D, state_key: &StateKey) -> Result<usize>
where
    D: PendingIndexScanner,
{
    let stream = durable.scan_pending(state_key);
    futures::pin_mut!(stream);
    let mut count = 0_usize;
    while let Some(entry) = stream.next().await {
        entry.map_err(|e| eyre!("scan_pending failed: {e}"))?;
        count += 1;
    }
    Ok(count)
}

async fn drive_wal_trace<D, S, F>(
    durable: D,
    dirty_factory: F,
    trace: Trace,
    check_idempotence: bool,
    policy: OraclePolicy,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    let Trace { ops, events } = trace;
    let collection_ref = collection_ref()?;
    let collection_id = collection_ref.id().clone();
    let mut model = Model::default();
    let mut tx_id = 1_u128;
    let mut event_idx = 0_usize;
    let mut current_event = build_event_ref(event_at(&events, event_idx), tx_id);
    let mut dirty = dirty_factory();
    let mut tx = TransactionValueStore::new(
        durable.clone(),
        dirty.clone(),
        collection_ref.clone(),
        current_event,
        CommitMode::Wal,
    );
    let mut just_crashed = false;

    for op in ops {
        if just_crashed || matches!(tx.local_tx(), LocalTx::Finished) {
            if let Some((pre_seal, crashed_ops, crash_event)) = model.crashed_sealed.take() {
                drive_recovery(&durable, &collection_ref, crash_event, &crashed_ops, policy)
                    .await?;
                model.applied = match policy {
                    OraclePolicy::AlwaysCommitted => fold_value_ops(pre_seal, &crashed_ops),
                    OraclePolicy::AlwaysNotCommitted => pre_seal,
                };
            }

            tx_id += 1;
            event_idx += 1;
            current_event = build_event_ref(event_at(&events, event_idx), tx_id);
            model.phase = ModelPhase::Clean;
            model.clear_dirty();
            model.sealed = None;
            dirty = dirty_factory();
            tx = TransactionValueStore::new(
                durable.clone(),
                dirty.clone(),
                collection_ref.clone(),
                current_event,
                CommitMode::Wal,
            );
            just_crashed = false;
        }

        if matches!(op, TraceOp::Crash) {
            apply_trace_crash(&mut model, current_event);
            just_crashed = true;
            continue;
        }

        if !apply_trace_op(&mut tx, &collection_id, &mut model, op).await? {
            return Ok(false);
        }

        if !check_per_op_invariants(&durable, &dirty, &tx, &collection_id, &model).await? {
            return Ok(false);
        }

        if check_idempotence
            && matches!(model.phase, ModelPhase::Finished)
            && !idempotent_resolution(&durable, &collection_ref, current_event).await?
        {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn drive_recovery<D>(
    durable: &D,
    collection: &CollectionRef<ValueKind>,
    crash_event: EventRef,
    _crashed_ops: &[ValueOp],
    policy: OraclePolicy,
) -> Result<()>
where
    D: DurableBundle,
{
    match policy {
        OraclePolicy::AlwaysCommitted => {
            durable.apply_sealed(collection, crash_event).await?;
        }
        OraclePolicy::AlwaysNotCommitted => {
            durable.rollback_sealed(collection, crash_event).await?;
        }
    }
    Ok(())
}

async fn idempotent_resolution<D>(
    durable: &D,
    collection: &CollectionRef<ValueKind>,
    event: EventRef,
) -> Result<bool>
where
    D: DurableBundle,
{
    if durable.apply_sealed(collection, event).await? != StoreOutcome::NoOp {
        return Ok(false);
    }
    if durable.rollback_sealed(collection, event).await? != StoreOutcome::NoOp {
        return Ok(false);
    }
    Ok(true)
}

async fn check_per_op_invariants<D, S>(
    durable: &D,
    dirty: &S,
    tx: &TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    model: &Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    if durable_applied_state(durable, collection).await? != model.applied {
        return Ok(false);
    }

    let has_pending = dirty.pending_ops(collection)?.is_some();
    if has_pending != model.has_pending() {
        return Ok(false);
    }

    // Visibility is only checked in Clean and Dirty phases. The Sealed
    // phase visibility cannot be asserted via `tx.get` because a
    // recovery-aware durable would resolve the current event's WAL
    // (oracle policy → apply or rollback) and return a post-recovery
    // payload that disagrees with the model's pre-seal `applied`. The
    // durable_applied_state check above still proves the WAL was sealed
    // without mutating `applied`.
    if matches!(model.phase, ModelPhase::Clean | ModelPhase::Dirty) {
        let visible = read_applied(tx.get(collection).await?);
        if visible != model.visible() {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn durable_applied_state<D>(
    durable: &D,
    collection: &CollectionId<ValueKind>,
) -> Result<Option<StoredPayload>>
where
    D: DurableBundle,
{
    Ok(match durable.read_partition(collection).await? {
        DurableState::Idle { applied } | DurableState::Sealed { applied, .. } => applied,
    })
}

fn read_applied(read: Read<StoredPayload>) -> Option<StoredPayload> {
    match read {
        Read::Present(payload) => Some(payload),
        Read::Absent | Read::Unknown => None,
    }
}

fn event_at(events: &[TraceEventKind], idx: usize) -> TraceEventKind {
    events.get(idx).copied().unwrap_or(TraceEventKind::Message)
}

fn build_event_ref(kind: TraceEventKind, tx_id: u128) -> EventRef {
    match kind {
        TraceEventKind::Message => EventRef::Message {
            dedup_id: Uuid::from_u128(tx_id),
        },
        TraceEventKind::Timer {
            timer_type,
            time,
            tag,
        } => EventRef::Timer(TimerEventRef::new(timer_type, time, tag)),
    }
}

async fn apply_trace_op<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    op: TraceOp,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match op {
        TraceOp::Set(byte) => {
            apply_trace_write(
                tx,
                collection,
                model,
                ValueOp::Set {
                    payload: inline(byte),
                },
            )
            .await
        }
        TraceOp::Clear => apply_trace_write(tx, collection, model, ValueOp::Clear).await,
        TraceOp::Seal => apply_trace_seal(tx, model).await,
        TraceOp::Commit => apply_trace_commit(tx, model).await,
        TraceOp::Abort => apply_trace_abort(tx, model).await,
        TraceOp::Flush => apply_trace_flush(tx, model).await,
        // Crash is handled by the driver, not dispatched here.
        TraceOp::Crash => Ok(true),
    }
}

/// Applies a [`TraceOp::Crash`] to the model. The runner is responsible
/// for advancing the transaction afterwards (the model's `crashed_sealed`
/// bookkeeping drives recovery on the next iteration).
fn apply_trace_crash(model: &mut Model, current_event: EventRef) {
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            // Outside Sealed: same as Abort but skip dirty cleanup — a crash
            // does not get to drain the dirty workspace. The next iteration
            // rebuilds the dirty store from scratch, so the model also
            // resets `dirty_ops` at advance time.
            model.phase = ModelPhase::Finished;
        }
        ModelPhase::Sealed => {
            // Capture the sealed snapshot plus the crashed event so the next
            // iteration can replay recovery (apply_sealed / rollback_sealed)
            // on the durable store under the oracle policy. `sealed` is
            // always `Some` in the Sealed phase, so the snapshot is present.
            model.crashed_sealed = model
                .sealed
                .take()
                .map(|(applied, ops)| (applied, ops, current_event));
            model.phase = ModelPhase::Finished;
        }
        ModelPhase::Finished => {
            // Already finished; second crash has no effect.
        }
    }
}

/// Applies a buffering write (`Set` or `Clear`) and advances the model in
/// lockstep.
///
/// The `Finished` phase must not issue the underlying `tx` call — that
/// matches the per-op handlers this replaced and keeps the model in step
/// with the store — so the early return guards it before the write fires.
async fn apply_trace_write<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    op: ValueOp,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    if matches!(model.phase, ModelPhase::Finished) {
        return Ok(false);
    }
    let overlay = match &op {
        ValueOp::Set { payload } => ValueOverlay::BufferedSet(payload.clone()),
        ValueOp::Clear => ValueOverlay::BufferedClear,
    };
    let result = match &op {
        ValueOp::Set { payload } => tx.set(collection, payload.clone()).await,
        ValueOp::Clear => tx.clear(collection).await,
    };
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if result.is_err() {
                return Ok(false);
            }
            model.dirty_ops.clear();
            model.dirty_ops.push(op);
            model.overlay = overlay;
            model.phase = ModelPhase::Dirty;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            result,
            Err(TransactionValueStoreError::AlreadySealed)
        )),
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_seal<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    model: &mut Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean => Ok(matches!(
            tx.seal().await,
            Err(TransactionValueStoreError::NoPendingOps)
        )),
        ModelPhase::Dirty => {
            if tx.seal().await.is_err() {
                return Ok(false);
            }
            model.sealed = Some((model.applied.clone(), model.dirty_ops.clone()));
            model.clear_dirty();
            model.phase = ModelPhase::Sealed;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            tx.seal().await,
            Err(TransactionValueStoreError::AlreadySealed)
        )),
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_commit<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    model: &mut Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean => {
            if tx.abort().await? != StoreOutcome::NoOp {
                return Ok(false);
            }
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Dirty => {
            if tx.seal().await.is_err() {
                return Ok(false);
            }
            if tx.apply_sealed().await? != StoreOutcome::Applied {
                return Ok(false);
            }
            model.applied = fold_value_ops(model.applied.clone(), &model.dirty_ops);
            model.clear_dirty();
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Sealed => {
            if tx.apply_sealed().await? != StoreOutcome::Applied {
                return Ok(false);
            }
            let Some((applied, ops)) = model.sealed.take() else {
                return Ok(false);
            };
            model.applied = fold_value_ops(applied, &ops);
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_abort<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    model: &mut Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if tx.abort().await? != StoreOutcome::NoOp {
                return Ok(false);
            }
            model.clear_dirty();
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Sealed => {
            if tx.abort().await? != StoreOutcome::Applied {
                return Ok(false);
            }
            let Some((applied, _ops)) = model.sealed.take() else {
                return Ok(false);
            };
            model.applied = applied;
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_flush<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    model: &mut Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean => Ok(tx.flush().await? == StoreOutcome::NoOp),
        ModelPhase::Dirty => {
            if tx.flush().await? != StoreOutcome::Applied {
                return Ok(false);
            }
            model.applied = fold_value_ops(model.applied.clone(), &model.dirty_ops);
            model.clear_dirty();
            model.phase = ModelPhase::Clean;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            tx.flush().await,
            Err(TransactionValueStoreError::AlreadySealed)
        )),
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_direct_op<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    op: DirectTraceOp,
) -> Result<()>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match op {
        DirectTraceOp::Set(byte) => tx.set(collection, inline(byte)).await?,
        DirectTraceOp::Clear => tx.clear(collection).await?,
        DirectTraceOp::Read => {
            let _ = tx.get(collection).await?;
        }
        DirectTraceOp::Flush => {
            let _ = tx.flush().await?;
        }
        DirectTraceOp::DirectApply => {
            let _ = tx.direct_apply().await?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct Trace {
    ops: Vec<TraceOp>,
    events: Vec<TraceEventKind>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = Vec::<TraceOp>::arbitrary(g)
            .into_iter()
            .take(MAX_TRACE_OPS)
            .collect();
        let events = Vec::<TraceEventKind>::arbitrary(g)
            .into_iter()
            .take(MAX_TRACE_EVENTS)
            .collect();
        Self { ops, events }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TraceOp {
    Set(u8),
    Clear,
    Seal,
    Commit,
    Abort,
    Flush,
    /// Simulates a process crash: drops the transaction without driving
    /// abort/apply/rollback. In the [`ModelPhase::Sealed`] phase this
    /// leaves a sealed WAL on the durable store; the runner resolves it
    /// on the next iteration via the configured [`OraclePolicy`].
    Crash,
}

impl Arbitrary for TraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 7 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Seal,
            3 => Self::Commit,
            4 => Self::Abort,
            5 => Self::Flush,
            _ => Self::Crash,
        }
    }
}

// Generated triples can collide across restarts; the durable store handles
// reused event refs (resolved WAL was already cleared by the prior tx), so
// nothing here relies on event uniqueness.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TraceEventKind {
    Message,
    Timer {
        timer_type: TimerType,
        time: CompactDateTime,
        tag: i32,
    },
}

impl Arbitrary for TraceEventKind {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Message
        } else {
            let timer_type = g
                .choose(&TIMER_TYPE_POOL)
                .copied()
                .unwrap_or(TimerType::Application);
            let time = CompactDateTime::from(u32::arbitrary(g) % TIMER_TIME_MODULUS);
            Self::Timer {
                timer_type,
                time,
                tag: i32::arbitrary(g),
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DirectTrace {
    ops: Vec<DirectTraceOp>,
    events: Vec<TraceEventKind>,
}

impl Arbitrary for DirectTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = Vec::<DirectTraceOp>::arbitrary(g)
            .into_iter()
            .take(MAX_TRACE_OPS)
            .collect();
        let events = Vec::<TraceEventKind>::arbitrary(g)
            .into_iter()
            .take(MAX_TRACE_EVENTS)
            .collect();
        Self { ops, events }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DirectTraceOp {
    Set(u8),
    Clear,
    Read,
    Flush,
    DirectApply,
}

impl Arbitrary for DirectTraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Read,
            3 => Self::Flush,
            _ => Self::DirectApply,
        }
    }
}

#[derive(Default)]
struct Model {
    applied: Option<StoredPayload>,
    dirty_ops: Vec<ValueOp>,
    overlay: ValueOverlay,
    sealed: Option<(Option<StoredPayload>, Vec<ValueOp>)>,
    /// Sealed snapshot captured by [`TraceOp::Crash`] in
    /// [`ModelPhase::Sealed`]: `(pre_seal_applied, sealed_ops, crash_event)`.
    /// Consumed by the runner on the next iteration to drive
    /// `apply_sealed` / `rollback_sealed` under the active [`OraclePolicy`].
    /// The event ref is carried because those APIs take the expected event
    /// explicitly. Both halves are always set and consumed together, so
    /// they live in one field.
    crashed_sealed: Option<(Option<StoredPayload>, Vec<ValueOp>, EventRef)>,
    phase: ModelPhase,
}

impl Model {
    fn visible(&self) -> Option<StoredPayload> {
        match &self.overlay {
            ValueOverlay::BufferedSet(payload) => Some(payload.clone()),
            ValueOverlay::BufferedClear => None,
            ValueOverlay::Untouched => self.applied.clone(),
        }
    }

    fn has_pending(&self) -> bool {
        !self.dirty_ops.is_empty()
    }

    fn clear_dirty(&mut self) {
        self.dirty_ops.clear();
        self.overlay = ValueOverlay::Untouched;
    }
}

#[derive(Clone, Copy, Default)]
enum ModelPhase {
    #[default]
    Clean,
    Dirty,
    Sealed,
    Finished,
}
