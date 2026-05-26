//! Reusable property-test fixture for keyed Value store implementations.
//!
//! The trace runners are parametric over any [`DurableWalStore<ValueKind>`] +
//! [`DirectApplyStore<ValueKind>`] and any [`ValueStore`] +
//! [`PendingOpSource<ValueKind>`], so Slice 5's Cassandra backend can share
//! the property-test machinery the memory backend uses today.
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
    CollectionId, CollectionRef, CommitMode, DurableState, EventRef, LocalTx, Read, StateKey,
    StateName, StateType, StoreOutcome, TimerEventRef, ValueKind, ValueOp, ValueOverlay,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen};
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

const TIMER_TYPE_POOL: [TimerType; 3] = [
    TimerType::Application,
    TimerType::DeferredMessage,
    TimerType::DeferredTimer,
];

const TIMER_TIME_MODULUS: u32 = 1_000_000;
const MAX_TRACE_OPS: usize = 40;
const MAX_TRACE_EVENTS: usize = 20;

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
/// # Errors
///
/// Propagates store/transaction errors; property mismatches return `Ok(false)`.
pub(crate) async fn run_trace<D, S, F>(durable: D, dirty_factory: F, trace: Trace) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
    F: Fn() -> S,
{
    drive_wal_trace(durable, dirty_factory, trace, false).await
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
    drive_wal_trace(durable, dirty_factory, trace, true).await
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
    Ok(CollectionRef::new(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new("profile")?,
    )))
}

pub(crate) fn inline(value: u8) -> StoredPayload {
    StoredPayload::Inline(Bytes::from(vec![value]))
}

async fn drive_wal_trace<D, S, F>(
    durable: D,
    dirty_factory: F,
    trace: Trace,
    check_idempotence: bool,
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

    for op in ops {
        if matches!(tx.local_tx(), LocalTx::Finished) {
            tx_id += 1;
            event_idx += 1;
            current_event = build_event_ref(event_at(&events, event_idx), tx_id);
            model.phase = ModelPhase::Clean;
            dirty = dirty_factory();
            tx = TransactionValueStore::new(
                durable.clone(),
                dirty.clone(),
                collection_ref.clone(),
                current_event,
                CommitMode::Wal,
            );
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

    if !matches!(model.phase, ModelPhase::Finished) {
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
        TraceOp::Set(byte) => apply_trace_set(tx, collection, model, byte).await,
        TraceOp::Clear => apply_trace_clear(tx, collection, model).await,
        TraceOp::Seal => apply_trace_seal(tx, model).await,
        TraceOp::Commit => apply_trace_commit(tx, model).await,
        TraceOp::Abort => apply_trace_abort(tx, model).await,
        TraceOp::Flush => apply_trace_flush(tx, model).await,
    }
}

async fn apply_trace_set<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    byte: u8,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if tx.set(collection, inline(byte)).await.is_err() {
                return Ok(false);
            }
            model.dirty_ops.clear();
            model.dirty_ops.push(ValueOp::Set {
                payload: inline(byte),
            });
            model.overlay = ValueOverlay::BufferedSet(inline(byte));
            model.phase = ModelPhase::Dirty;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            tx.set(collection, inline(byte)).await,
            Err(TransactionValueStoreError::AlreadySealed)
        )),
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_clear<D, S>(
    tx: &mut TransactionValueStore<D, S>,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
) -> Result<bool>
where
    D: DurableBundle,
    S: DirtyBundle,
{
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if tx.clear(collection).await.is_err() {
                return Ok(false);
            }
            model.dirty_ops.clear();
            model.dirty_ops.push(ValueOp::Clear);
            model.overlay = ValueOverlay::BufferedClear;
            model.phase = ModelPhase::Dirty;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            tx.clear(collection).await,
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
}

impl Arbitrary for TraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Seal,
            3 => Self::Commit,
            4 => Self::Abort,
            _ => Self::Flush,
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
