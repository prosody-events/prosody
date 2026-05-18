use super::memory::{MemoryDirtyValueStore, MemoryDurableValueStore, MemoryStateError};
use super::value::{
    DurableWalStore, PendingOpSource, TransactionValueStore, TransactionValueStoreError,
    ValueStore, fold_value_ops,
};
use super::{
    CollectionId, CollectionKindId, CollectionRef, CommitDecision, CommitMode, DirtyCollection,
    DurableState, EventRef, EventScopeId, LocalTx, Read, StateKey, StateName, StateType, ValueKind,
    ValueOp, ValueOverlay,
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
    EventRef::Message(EventScopeId::new(id))
}

fn payload(value: u8) -> Bytes {
    Bytes::from(vec![value])
}

fn read_applied(read: Read<Bytes>) -> Option<Bytes> {
    match read {
        Read::Present(bytes) => Some(bytes),
        Read::Absent | Read::Unknown => None,
    }
}

#[test]
fn value_folding_uses_last_ordered_op() {
    let initial = Some(payload(1));
    let ops = vec![
        ValueOp::Set {
            payload: payload(2),
        },
        ValueOp::Clear,
        ValueOp::Set {
            payload: payload(3),
        },
    ];

    assert_eq!(fold_value_ops(initial, &ops), Some(payload(3)));
    assert_eq!(fold_value_ops(Some(payload(1)), &[ValueOp::Clear]), None);
    assert_eq!(
        fold_value_ops(
            None,
            &[ValueOp::Set {
                payload: payload(9)
            }]
        ),
        Some(payload(9))
    );
}

#[test]
fn collection_identity_carries_value_kind() -> Result<()> {
    let collection = collection()?;
    assert_eq!(collection.kind(), CollectionKindId::Value);

    let blob = super::WalBlob::<ValueKind>::new(collection, event(1), vec![ValueOp::Clear]);
    assert_eq!(blob.kind(), CollectionKindId::Value);
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

    dirty.set(&collection, payload(7)).await?;
    assert_eq!(dirty.get(&collection).await?, Read::Present(payload(7)));

    dirty.clear(&collection).await?;
    assert_eq!(dirty.get(&collection).await?, Read::Absent);
    assert_eq!(
        dirty.pending_ops(&collection)?.collect::<Vec<_>>(),
        vec![ValueOp::Clear]
    );

    dirty.clear_pending_ops(&collection)?;
    assert_eq!(dirty.get(&collection).await?, Read::Unknown);
    assert_eq!(dirty.pending_ops(&collection)?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn durable_memory_store_seals_applies_and_rolls_back() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;

    assert_eq!(durable.get(&collection).await?, Read::Absent);
    durable.set(&collection, payload(1)).await?;
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(1)));

    let sealed = durable
        .seal(
            &collection,
            event(1),
            vec![ValueOp::Set {
                payload: payload(2),
            }],
        )
        .await?;
    assert_eq!(sealed.applied(), &Some(payload(1)));

    match durable.read_partition(&collection).await? {
        DurableState::Sealed(sealed) => assert_eq!(sealed.event(), event(1)),
        DurableState::Idle { .. } => return Err(eyre::eyre!("expected sealed durable state")),
    }

    assert_eq!(
        durable.apply_sealed(&collection, event(1)).await?,
        CommitDecision::Committed
    );
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(2)));

    let _sealed = durable
        .seal(&collection, event(2), vec![ValueOp::Clear])
        .await?;
    assert_eq!(
        durable.rollback_sealed(&collection, event(2)).await?,
        CommitDecision::NotCommitted
    );
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(2)));
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
    assert_eq!(
        error,
        MemoryStateError::EventMismatch {
            expected: event(2),
            actual: event(1)
        }
    );
    Ok(())
}

#[tokio::test]
async fn transaction_dirty_reads_override_durable_reads() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection()?;
    durable.set(&collection, payload(1)).await?;

    let tx = TransactionValueStore::new(
        durable,
        dirty,
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    assert_eq!(tx.get(&collection).await?, Read::Present(payload(1)));

    tx.set(&collection, payload(2)).await?;
    assert_eq!(tx.get(&collection).await?, Read::Present(payload(2)));

    tx.clear(&collection).await?;
    assert_eq!(tx.get(&collection).await?, Read::Absent);
    Ok(())
}

#[tokio::test]
async fn transaction_wal_commit_applies_and_abort_rolls_back() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;

    let commit_tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    commit_tx.set(&collection, payload(1)).await?;
    assert_eq!(commit_tx.commit().await?, CommitDecision::Committed);
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(1)));

    let abort_tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(2),
        CommitMode::Wal,
    );
    abort_tx.set(&collection, payload(2)).await?;
    let _sealed = abort_tx.seal().await?;
    assert_eq!(abort_tx.abort().await?, CommitDecision::NotCommitted);
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(1)));
    Ok(())
}

#[tokio::test]
async fn transaction_unsealed_abort_clears_dirty_only() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection()?;

    let tx = TransactionValueStore::new(
        durable.clone(),
        dirty.clone(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );
    tx.set(&collection, payload(1)).await?;
    assert_eq!(tx.abort().await?, CommitDecision::NotCommitted);
    assert_eq!(durable.get(&collection).await?, Read::Absent);
    assert_eq!(dirty.get(&collection).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn flush_applies_dirty_without_creating_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );

    tx.set(&collection, payload(1)).await?;
    assert_eq!(tx.flush().await?, CommitDecision::Committed);
    assert!(matches!(tx.local_tx(), LocalTx::Clean(_)));
    assert_eq!(tx.abort().await?, CommitDecision::NotCommitted);
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(1)));

    match durable.read_partition(&collection).await? {
        DurableState::Idle { applied, .. } => assert_eq!(applied, Some(payload(1))),
        DurableState::Sealed(_) => return Err(eyre::eyre!("flush must not create sealed WAL")),
    }
    Ok(())
}

#[tokio::test]
async fn direct_mode_applies_without_sealed_wal() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let tx = TransactionValueStore::new(
        durable.clone(),
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Direct,
    );

    tx.set(&collection, payload(1)).await?;
    assert_eq!(tx.direct_apply().await?, CommitDecision::Committed);
    assert_eq!(durable.get(&collection).await?, Read::Present(payload(1)));

    match durable.read_partition(&collection).await? {
        DurableState::Idle { applied, .. } => assert_eq!(applied, Some(payload(1))),
        DurableState::Sealed(_) => return Err(eyre::eyre!("direct mode must not create WAL")),
    }
    Ok(())
}

#[tokio::test]
async fn finished_transaction_rejects_further_transitions() -> Result<()> {
    let durable = MemoryDurableValueStore::new();
    let collection = collection()?;
    let tx = TransactionValueStore::new(
        durable,
        MemoryDirtyValueStore::new(),
        collection.clone(),
        event(1),
        CommitMode::Wal,
    );

    assert_eq!(tx.commit().await?, CommitDecision::NotCommitted);
    let error = tx
        .set(&collection, payload(1))
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

        if !apply_trace_op(&tx, &collection, &mut model, op).await? {
            return Ok(false);
        }

        let durable_visible = read_applied(durable.get(&collection).await?);
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
    tx: &MemoryTx,
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
    tx: &MemoryTx,
    collection: &CollectionId<ValueKind>,
    model: &mut Model,
    byte: u8,
) -> Result<bool> {
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if tx.set(collection, payload(byte)).await.is_err() {
                return Ok(false);
            }
            model.dirty_ops.clear();
            model.dirty_ops.push(ValueOp::Set {
                payload: payload(byte),
            });
            model.overlay = Some(Some(payload(byte)));
            model.phase = ModelPhase::Dirty;
            Ok(true)
        }
        ModelPhase::Sealed => Ok(matches!(
            tx.set(collection, payload(byte)).await,
            Err(TransactionValueStoreError::AlreadySealed)
        )),
        ModelPhase::Finished => Ok(false),
    }
}

async fn apply_trace_clear(
    tx: &MemoryTx,
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
            model.overlay = Some(None);
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

async fn apply_trace_seal(tx: &MemoryTx, model: &mut Model) -> Result<bool> {
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

async fn apply_trace_commit(tx: &MemoryTx, model: &mut Model) -> Result<bool> {
    match model.phase {
        ModelPhase::Clean => {
            if tx.commit().await? != CommitDecision::NotCommitted {
                return Ok(false);
            }
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Dirty => {
            if tx.commit().await? != CommitDecision::Committed {
                return Ok(false);
            }
            model.applied = fold_value_ops(model.applied.clone(), &model.dirty_ops);
            model.clear_dirty();
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Sealed => {
            if tx.commit().await? != CommitDecision::Committed {
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

async fn apply_trace_abort(tx: &MemoryTx, model: &mut Model) -> Result<bool> {
    match model.phase {
        ModelPhase::Clean | ModelPhase::Dirty => {
            if tx.abort().await? != CommitDecision::NotCommitted {
                return Ok(false);
            }
            model.clear_dirty();
            model.phase = ModelPhase::Finished;
            Ok(true)
        }
        ModelPhase::Sealed => {
            if tx.abort().await? != CommitDecision::NotCommitted {
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

async fn apply_trace_flush(tx: &MemoryTx, model: &mut Model) -> Result<bool> {
    match model.phase {
        ModelPhase::Clean => Ok(tx.flush().await? == CommitDecision::NotCommitted),
        ModelPhase::Dirty => {
            if tx.flush().await? != CommitDecision::Committed {
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
    applied: Option<Bytes>,
    dirty_ops: Vec<ValueOp>,
    overlay: ValueOverlay,
    sealed: Option<(Option<Bytes>, Vec<ValueOp>)>,
    phase: ModelPhase,
}

impl Model {
    fn visible(&self) -> Option<Bytes> {
        self.overlay.clone().unwrap_or_else(|| self.applied.clone())
    }

    fn clear_dirty(&mut self) {
        self.dirty_ops.clear();
        self.overlay = None;
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
