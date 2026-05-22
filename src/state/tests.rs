use super::memory::{MemoryDirtyValueStore, MemoryDurableValueStore, MemoryStateError};
use super::value::{
    DirectApplyStore, DurableWalStore, PendingOpSource, StoredPayload, TransactionValueStore,
    TransactionValueStoreError, ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionKindId, CollectionRef, CommitMode, DirtyCollection, DurableState,
    EventRef, LocalTx, Read, StateKey, StateName, StateType, StoreOutcome, ValueKind, ValueOp,
    ValueOverlay, WalEnvelope,
};
use crate::Key;
use bytes::Bytes;
use color_eyre::eyre::{self, Result};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::num::NonZeroU64;
use std::sync::Arc;
use uuid::Uuid;

type MemoryTx = TransactionValueStore<MemoryDurableValueStore, MemoryDirtyValueStore>;

fn key(value: &str) -> Key {
    Arc::from(value)
}

fn collection() -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), key("user-1")),
        StateType::Application,
        StateName::try_new("profile")?,
    ))
}

fn event(id: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(id),
    }
}

fn payload(value: u8) -> Bytes {
    Bytes::from(vec![value])
}

fn inline(value: u8) -> StoredPayload {
    StoredPayload::Inline(payload(value))
}

fn read_applied(read: Read<StoredPayload>) -> Option<StoredPayload> {
    match read {
        Read::Present(payload) => Some(payload),
        Read::Absent | Read::Unknown => None,
    }
}

async fn durable_applied(
    durable: &MemoryDurableValueStore,
    collection: &CollectionId<ValueKind>,
) -> Result<Option<StoredPayload>> {
    Ok(match durable.read_partition(collection).await? {
        DurableState::Idle { applied } | DurableState::Sealed { applied, .. } => applied,
    })
}

async fn seed_durable(
    durable: &MemoryDurableValueStore,
    collection: &CollectionId<ValueKind>,
    op: ValueOp,
) -> Result<()> {
    durable.direct_apply(collection, vec![op]).await?;
    Ok(())
}

#[test]
fn value_folding_uses_last_ordered_op() {
    let initial = Some(inline(1));
    let ops = vec![
        ValueOp::Set { payload: inline(2) },
        ValueOp::Clear,
        ValueOp::Set { payload: inline(3) },
    ];

    assert_eq!(fold_value_ops(initial, &ops), Some(inline(3)));
    assert_eq!(fold_value_ops(Some(inline(1)), &[ValueOp::Clear]), None);
    assert_eq!(
        fold_value_ops(None, &[ValueOp::Set { payload: inline(9) }]),
        Some(inline(9))
    );
}

#[test]
fn collection_identity_carries_value_kind() -> Result<()> {
    let collection = collection()?;
    assert_eq!(collection.kind(), CollectionKindId::Value);

    let envelope = WalEnvelope::<ValueKind>::try_from_ops(vec![ValueOp::Clear])?;
    assert_eq!(envelope.operation_count().get(), 1);
    Ok(())
}

#[test]
fn dirty_collection_requires_non_zero_operations() -> Result<()> {
    let reference = CollectionRef::new(collection()?);
    assert!(DirtyCollection::try_from_count(reference.clone(), 0).is_err());

    let Some(count) = NonZeroU64::new(1) else {
        return Err(eyre::eyre!("non-zero literal produced None"));
    };
    let dirty = DirtyCollection::new(reference, count);
    assert_eq!(dirty.operation_count(), count);
    Ok(())
}

#[tokio::test]
async fn dirty_memory_store_tracks_overlay_and_compacted_op() -> Result<()> {
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection()?;

    assert_eq!(dirty.get(&collection).await?, Read::Unknown);
    assert!(dirty.pending_ops(&collection)?.is_none());

    dirty.set(&collection, inline(7)).await?;
    assert_eq!(dirty.get(&collection).await?, Read::Present(inline(7)));

    dirty.clear(&collection).await?;
    assert_eq!(dirty.get(&collection).await?, Read::Absent);

    let Some(pending) = dirty.pending_ops(&collection)? else {
        return Err(eyre::eyre!("expected pending ops after clear"));
    };
    assert_eq!(pending.count.get(), 1);
    assert_eq!(pending.ops.collect::<Vec<_>>(), vec![ValueOp::Clear]);

    dirty.clear_pending_ops(&collection)?;
    assert_eq!(dirty.get(&collection).await?, Read::Unknown);
    assert!(dirty.pending_ops(&collection)?.is_none());
    Ok(())
}

#[tokio::test]
async fn durable_memory_store_seals_applies_and_rolls_back() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;

    assert_eq!(durable_applied(&durable, &collection).await?, None);
    seed_durable(&durable, &collection, ValueOp::Set { payload: inline(1) }).await?;
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );

    let sealed = durable
        .seal(
            &collection,
            event(1),
            vec![ValueOp::Set { payload: inline(2) }],
        )
        .await?;
    assert_eq!(sealed.event(), event(1));

    match durable.read_partition(&collection).await? {
        DurableState::Sealed { applied, wal } => {
            assert_eq!(wal.event(), event(1));
            assert_eq!(applied, Some(inline(1)));
        }
        DurableState::Idle { .. } => return Err(eyre::eyre!("expected sealed durable state")),
    }

    assert_eq!(
        durable.apply_sealed(&collection, event(1)).await?,
        StoreOutcome::Applied
    );
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(2))
    );

    let _sealed = durable
        .seal(&collection, event(2), vec![ValueOp::Clear])
        .await?;
    assert_eq!(
        durable.rollback_sealed(&collection, event(2)).await?,
        StoreOutcome::Applied
    );
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(2))
    );
    Ok(())
}

#[tokio::test]
async fn durable_memory_store_applies_idempotently() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;

    let _sealed = durable
        .seal(
            &collection,
            event(1),
            vec![ValueOp::Set { payload: inline(1) }],
        )
        .await?;

    assert_eq!(
        durable.apply_sealed(&collection, event(1)).await?,
        StoreOutcome::Applied
    );
    assert_eq!(
        durable.apply_sealed(&collection, event(1)).await?,
        StoreOutcome::NoOp
    );
    assert_eq!(
        durable.rollback_sealed(&collection, event(1)).await?,
        StoreOutcome::NoOp
    );
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );
    Ok(())
}

#[tokio::test]
async fn durable_memory_store_rejects_mismatched_event_resolution() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let _sealed = durable
        .seal(&collection, event(1), vec![ValueOp::Clear])
        .await?;

    let error = durable
        .apply_sealed(&collection, event(2))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected mismatched event error"))?;
    match error {
        MemoryStateError::EventMismatch { expected, actual } => {
            assert_eq!(expected, event(2));
            assert_eq!(actual, event(1));
        }
        other => return Err(eyre::eyre!("expected EventMismatch, got {other:?}")),
    }
    Ok(())
}

#[tokio::test]
async fn transaction_dirty_reads_override_durable_reads() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection()?;
    seed_durable(&durable, &collection, ValueOp::Set { payload: inline(1) }).await?;

    let tx = TransactionValueStore::new(
        durable,
        dirty,
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    assert_eq!(tx.get(&collection).await?, Read::Present(inline(1)));

    tx.set(&collection, inline(2)).await?;
    assert_eq!(tx.get(&collection).await?, Read::Present(inline(2)));

    tx.clear(&collection).await?;
    assert_eq!(tx.get(&collection).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn transaction_wal_commit_applies_and_abort_rolls_back() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;

    let mut commit_tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    commit_tx.set(&collection, inline(1)).await?;
    let _sealed = commit_tx.seal().await?;
    assert_eq!(commit_tx.apply_sealed().await?, StoreOutcome::Applied);
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );

    let mut abort_tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(2),
        CommitMode::Wal,
    );
    abort_tx.set(&collection, inline(2)).await?;
    let _sealed = abort_tx.seal().await?;
    assert_eq!(abort_tx.abort().await?, StoreOutcome::Applied);
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );
    Ok(())
}

#[tokio::test]
async fn transaction_unsealed_abort_clears_dirty_only() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection()?;

    let mut tx = TransactionValueStore::new(
        durable.clone(),
        dirty.clone(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    tx.set(&collection, inline(1)).await?;
    assert_eq!(tx.abort().await?, StoreOutcome::NoOp);
    assert_eq!(durable_applied(&durable, &collection).await?, None);
    assert_eq!(dirty.get(&collection).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn flush_applies_dirty_without_creating_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let mut tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );

    tx.set(&collection, inline(1)).await?;
    assert_eq!(tx.flush().await?, StoreOutcome::Applied);
    assert!(matches!(tx.local_tx(), LocalTx::Clean(_)));
    assert_eq!(tx.abort().await?, StoreOutcome::NoOp);
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );

    match durable.read_partition(&collection).await? {
        DurableState::Idle { applied } => assert_eq!(applied, Some(inline(1))),
        DurableState::Sealed { .. } => return Err(eyre::eyre!("flush must not create sealed WAL")),
    }
    Ok(())
}

#[tokio::test]
async fn direct_mode_applies_without_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let mut tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Direct,
    );

    tx.set(&collection, inline(1)).await?;
    assert_eq!(tx.direct_apply().await?, StoreOutcome::Applied);
    assert_eq!(
        durable_applied(&durable, &collection).await?,
        Some(inline(1))
    );

    match durable.read_partition(&collection).await? {
        DurableState::Idle { applied } => assert_eq!(applied, Some(inline(1))),
        DurableState::Sealed { .. } => return Err(eyre::eyre!("direct mode must not create WAL")),
    }
    Ok(())
}

#[tokio::test]
async fn finished_transaction_rejects_further_transitions() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let mut tx = TransactionValueStore::new(
        durable,
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );

    assert_eq!(tx.abort().await?, StoreOutcome::NoOp);
    let error = tx
        .set(&collection, inline(1))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected finished transaction error"))?;
    assert!(matches!(error, TransactionValueStoreError::Finished));
    Ok(())
}

#[test]
fn prop_value_transaction_trace_matches_model() {
    fn property(trace: Trace) -> bool {
        executor::block_on(run_trace(trace)).unwrap_or(false)
    }

    QuickCheck::new().quickcheck(property as fn(Trace) -> bool);
}

async fn run_trace(trace: Trace) -> Result<bool> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let mut model = Model::default();
    let mut tx_id = 1_u128;
    let mut tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(tx_id),
        CommitMode::Wal,
    );

    for op in trace.ops {
        restart_finished_transaction(&durable, &collection, &mut model, &mut tx_id, &mut tx);

        if !apply_trace_op(&mut tx, &collection, &mut model, op).await? {
            return Ok(false);
        }

        let durable_visible = durable_applied(&durable, &collection).await?;
        if durable_visible != model.applied {
            return Ok(false);
        }
    }

    Ok(true)
}

fn restart_finished_transaction(
    durable: &MemoryDurableValueStore,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    tx_id: &mut u128,
    tx: &mut MemoryTx,
) {
    if matches!(tx.local_tx(), LocalTx::Finished) {
        *tx_id += 1;
        model.phase = ModelPhase::Clean;
        *tx = TransactionValueStore::new(
            durable.clone(),
            MemoryDirtyValueStore::new(),
            collection.clone(),
            event(*tx_id),
            CommitMode::Wal,
        );
    }
}

async fn apply_trace_op(
    tx: &mut MemoryTx,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    op: TraceOp,
) -> Result<bool> {
    match op {
        TraceOp::Set(byte) => apply_trace_set(tx, collection, model, byte).await,
        TraceOp::Clear => apply_trace_clear(tx, collection, model).await,
        TraceOp::Read => {
            let visible = read_applied(tx.get(collection).await?);
            Ok(visible == model.visible())
        }
        TraceOp::Seal => apply_trace_seal(tx, model).await,
        TraceOp::Commit => apply_trace_commit(tx, model).await,
        TraceOp::Abort => apply_trace_abort(tx, model).await,
        TraceOp::Flush => apply_trace_flush(tx, model).await,
    }
}

async fn apply_trace_set(
    tx: &mut MemoryTx,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    byte: u8,
) -> Result<bool> {
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

async fn apply_trace_clear(
    tx: &mut MemoryTx,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
) -> Result<bool> {
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

async fn apply_trace_seal(tx: &mut MemoryTx, model: &mut Model) -> Result<bool> {
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

async fn apply_trace_commit(tx: &mut MemoryTx, model: &mut Model) -> Result<bool> {
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

async fn apply_trace_abort(tx: &mut MemoryTx, model: &mut Model) -> Result<bool> {
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

async fn apply_trace_flush(tx: &mut MemoryTx, model: &mut Model) -> Result<bool> {
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

#[derive(Clone, Debug)]
struct Trace {
    ops: Vec<TraceOp>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = Vec::<TraceOp>::arbitrary(g).into_iter().take(40).collect();
        Self { ops }
    }
}

#[derive(Clone, Debug)]
enum TraceOp {
    Set(u8),
    Clear,
    Read,
    Seal,
    Commit,
    Abort,
    Flush,
}

impl Arbitrary for TraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 7 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Read,
            3 => Self::Seal,
            4 => Self::Commit,
            5 => Self::Abort,
            _ => Self::Flush,
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
