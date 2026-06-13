pub(crate) mod cell_suite;
pub(crate) mod dirty_value_suite;
mod identity;
pub(crate) mod value_suite;

use self::cell_suite::{
    OverwriteTrace, ProjTrace, Trace, run_crash_equivalence_trace, run_overwrite_trace,
    run_projection_trace,
};
use self::dirty_value_suite::DirtyTrace;
use self::value_suite::{bytes, collection_id};
use super::memory::{MemoryCellStore, MemoryDirtyValueStore};
use super::value::{ValueOp, fold_value_ops};
use super::{CollectionKindId, CollectionRef, ValueKind};
use color_eyre::eyre::Result;
use futures::executor;
use quickcheck::QuickCheck;

#[test]
fn value_folding_uses_last_ordered_op() {
    let initial = Some(bytes(1));
    let ops = vec![
        ValueOp::Set { payload: bytes(2) },
        ValueOp::Clear,
        ValueOp::Set { payload: bytes(3) },
    ];

    assert_eq!(fold_value_ops(initial, &ops), Some(bytes(3)));
    assert_eq!(fold_value_ops(Some(bytes(1)), &[ValueOp::Clear]), None);
    assert_eq!(
        fold_value_ops(None, &[ValueOp::Set { payload: bytes(9) }]),
        Some(bytes(9))
    );
}

#[test]
fn collection_identity_carries_value_kind() -> Result<()> {
    let collection = collection_id("profile")?;
    assert_eq!(collection.kind(), CollectionKindId::Value);
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

    let id = collection_id("profile")?;
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
fn prop_memory_dirty_satisfies_invariants() {
    fn property(trace: DirtyTrace) -> Result<bool> {
        executor::block_on(dirty_value_suite::run_dirty_trace(
            MemoryDirtyValueStore::new(),
            trace,
        ))
    }

    QuickCheck::new().quickcheck(property as fn(DirtyTrace) -> Result<bool>);
}

/// Crash-recovery equivalence over the memory cell store: every resolution
/// path (clean promote, inline rollback, crash → sweep / first-touch recovery)
/// converges each cell's committed projection to the model.
#[test]
fn prop_memory_cell_crash_equivalence() {
    fn property(trace: Trace) -> Result<bool> {
        executor::block_on(run_crash_equivalence_trace(MemoryCellStore::new(), trace))
    }

    QuickCheck::new().quickcheck(property as fn(Trace) -> Result<bool>);
}

/// Reader-projection soundness over the memory cell store: a provisional cell
/// always projects its committed `prev` (stale by the in-flight event), a
/// resolved cell projects the committed value, and prev-is-committed holds.
#[test]
fn prop_memory_cell_projection_is_sound() {
    fn property(trace: ProjTrace) -> Result<bool> {
        executor::block_on(run_projection_trace(MemoryCellStore::new(), trace))
    }

    QuickCheck::new().quickcheck(property as fn(ProjTrace) -> Result<bool>);
}

/// Implicit-overwrite soundness over the memory cell store: a sequence of
/// events that never promote or roll back explicitly converges every cell to
/// the model, each overwrite resolving its predecessor's provisional cell
/// through the oracle (both arms) on read.
#[test]
fn prop_memory_cell_implicit_overwrite() {
    fn property(trace: OverwriteTrace) -> Result<bool> {
        executor::block_on(run_overwrite_trace(MemoryCellStore::new(), trace))
    }

    QuickCheck::new().quickcheck(property as fn(OverwriteTrace) -> Result<bool>);
}
