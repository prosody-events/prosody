use super::dirty_value_test_suite::{self, DirtyTrace};
use super::memory::{MemoryDirtyValueStore, MemoryDurableValueStore, MemoryStateError};
use super::value::{
    DurableWalStore, TransactionValueStore, TransactionValueStoreError, ValueStore, fold_value_ops,
};
use super::value_test_suite::{self, DirectTrace, Trace, collection_ref, inline};
use super::{
    CollectionId, CollectionKindId, CollectionRef, CommitMode, DirtyCollection, EventRef, Read,
    StateKey, StateName, StateType, StoreOutcome, ValueKind, ValueOp, WalEnvelope,
};
use crate::Key;
use color_eyre::eyre::{self, Result};
use futures::executor;
use quickcheck::QuickCheck;
use std::num::NonZeroU64;
use std::sync::Arc;
use uuid::Uuid;

fn key(value: &str) -> Key {
    Arc::from(value)
}

fn collection_id() -> Result<CollectionId<ValueKind>> {
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

/// F4 (memory): the shared stale-pending sweep check against the memory
/// durable store. Runs always (no broker). The Cassandra counterpart lives
/// in `state::cassandra::tests` and drives the identical helper.
#[tokio::test]
async fn memory_sweep_deletes_stale_pending_index() -> Result<()> {
    value_test_suite::run_stale_pending_index(MemoryDurableValueStore::for_tests()).await
}

#[test]
fn collection_identity_carries_value_kind() -> Result<()> {
    let collection = collection_id()?;
    assert_eq!(collection.kind(), CollectionKindId::Value);

    let envelope = WalEnvelope::<ValueKind>::try_from_ops(vec![ValueOp::Clear])?;
    assert_eq!(envelope.operation_count().get(), 1);
    Ok(())
}

/// `CollectionRef` equality and hashing key on the inner `CollectionId`
/// only — the TTL is a per-write hint, not part of identity. Two refs to the
/// same collection with different TTLs must compare equal and hash equal, so
/// a `CollectionRef` used as a `HashSet`/`HashMap` key is not split by an
/// incidental TTL difference.
#[test]
fn collection_ref_eq_and_hash_ignore_ttl() -> Result<()> {
    use crate::timers::duration::CompactDuration;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let id = collection_id()?;
    let with_ttl = CollectionRef::new(id.clone(), Some(CompactDuration::new(3_600)));
    let without_ttl = CollectionRef::new(id.clone(), None);
    let other_ttl = CollectionRef::new(id, Some(CompactDuration::new(7_200)));

    assert_eq!(with_ttl, without_ttl);
    assert_eq!(with_ttl, other_ttl);

    let hash = |r: &CollectionRef<ValueKind>| {
        let mut h = DefaultHasher::new();
        r.hash(&mut h);
        h.finish()
    };
    assert_eq!(hash(&with_ttl), hash(&without_ttl));
    assert_eq!(hash(&with_ttl), hash(&other_ttl));
    Ok(())
}

#[test]
fn dirty_collection_requires_non_zero_operations() -> Result<()> {
    let reference = CollectionRef::new(collection_id()?, None);
    assert!(DirtyCollection::try_from_count(reference.clone(), 0).is_err());

    let Some(count) = NonZeroU64::new(1) else {
        return Err(eyre::eyre!("non-zero literal produced None"));
    };
    let dirty = DirtyCollection::new(reference, count);
    assert_eq!(dirty.operation_count(), count);
    Ok(())
}

#[tokio::test]
async fn durable_memory_store_rejects_mismatched_event_resolution() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
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
        other @ MemoryStateError::Encoding(_) => {
            return Err(eyre::eyre!("expected EventMismatch, got {other:?}"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn transaction_unsealed_abort_clears_dirty_only() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let dirty = MemoryDirtyValueStore::new();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();

    let mut tx = TransactionValueStore::new(
        durable.clone(),
        dirty.clone(),
        collection,
        event(1),
        CommitMode::Wal,
    );
    tx.set(&collection_id, inline(1)).await?;
    assert_eq!(tx.abort().await?, StoreOutcome::NoOp);

    let applied = match durable.read_partition(&collection_id).await? {
        super::DurableState::Idle { applied } | super::DurableState::Sealed { applied, .. } => {
            applied
        }
    };
    assert_eq!(applied, None);
    assert_eq!(dirty.get(&collection_id).await?, Read::Unknown);
    Ok(())
}

#[tokio::test]
async fn finished_transaction_rejects_further_transitions() -> Result<()> {
    let durable = MemoryDurableValueStore::for_tests();
    let collection = collection_ref()?;
    let collection_id = collection.id().clone();
    let mut tx = TransactionValueStore::new(
        durable,
        MemoryDirtyValueStore::new(),
        collection,
        event(1),
        CommitMode::Wal,
    );

    assert_eq!(tx.abort().await?, StoreOutcome::NoOp);
    let error = tx
        .set(&collection_id, inline(1))
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("expected finished transaction error"))?;
    assert!(matches!(error, TransactionValueStoreError::Finished));
    Ok(())
}

#[test]
fn prop_value_transaction_trace_matches_model() {
    fn property(trace: Trace) -> Result<bool> {
        executor::block_on(value_test_suite::run_trace(
            MemoryDurableValueStore::for_tests(),
            MemoryDirtyValueStore::new,
            trace,
        ))
    }

    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

#[test]
fn prop_durable_resolution_is_idempotent() {
    fn property(trace: Trace) -> Result<bool> {
        executor::block_on(value_test_suite::run_idempotence_trace(
            MemoryDurableValueStore::for_tests(),
            MemoryDirtyValueStore::new,
            trace,
        ))
    }

    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

#[test]
fn prop_direct_mode_never_creates_wal() {
    fn property(trace: DirectTrace) -> Result<bool> {
        executor::block_on(value_test_suite::run_direct_trace(
            MemoryDurableValueStore::for_tests(),
            MemoryDirtyValueStore::new,
            trace,
        ))
    }

    QuickCheck::new().quickcheck(property as fn(DirectTrace) -> Result<bool>);
}

#[test]
fn prop_memory_dirty_satisfies_invariants() {
    fn property(trace: DirtyTrace) -> Result<bool> {
        executor::block_on(dirty_value_test_suite::run_dirty_trace(
            MemoryDirtyValueStore::new(),
            trace,
        ))
    }

    QuickCheck::new().quickcheck(property as fn(DirtyTrace) -> Result<bool>);
}
