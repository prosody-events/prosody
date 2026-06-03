//! Shared property-test fixture for dirty Value store implementations.
//!
//! Parallels [`super::value_test_suite`] in shape but tests only the
//! dirty side. The Memory and Fjall dirty implementations are both
//! proved against the same traces so higher layers can rely on
//! cross-backend equivalence.
//!
//! Universal dirty contract enforced by [`run_dirty_trace`]:
//!
//! 1. After `Set(x)`: `get` returns `Present(x)`; `pending_ops` is `Some`.
//! 2. After `Clear`: `get` returns `Absent`; `pending_ops` is `Some`.
//! 3. After `ClearPendingOps`: `get` returns `Unknown`; `pending_ops` is
//!    `None`.
//! 4. Fold equivalence: `fold_value_ops(None, pending_ops().ops)` converted to
//!    `Read<T>` equals `get()`.

use super::value::{PendingOpSource, ValueStore, fold_value_ops};
use super::value_test_suite::bytes;
use super::{CollectionId, Read, StateKey, StateName, StateType, ValueKind};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen};
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

const MAX_TRACE_OPS: usize = 40;

/// Bundle trait for any dirty Value backend.
pub(crate) trait DirtyStore:
    ValueStore<Error = <Self as PendingOpSource<ValueKind>>::Error>
    + PendingOpSource<ValueKind>
    + fmt::Debug
    + Clone
{
}

impl<T> DirtyStore for T where
    T: ValueStore<Error = <Self as PendingOpSource<ValueKind>>::Error>
        + PendingOpSource<ValueKind>
        + fmt::Debug
        + Clone
{
}

/// Drives `trace` against `store` and returns `true` iff every per-op
/// invariant held.
///
/// # Errors
///
/// Propagates store errors raised by the dirty backend.
pub(crate) async fn run_dirty_trace<S>(store: S, trace: DirtyTrace) -> Result<bool>
where
    S: DirtyStore,
{
    let collection = collection_id()?;
    let mut overlay = Read::Unknown;

    for op in trace.ops {
        match op {
            DirtyTraceOp::Set(byte) => {
                let payload = bytes(byte);
                store.set(&collection, payload.clone()).await?;
                overlay = Read::Present(payload);
            }
            DirtyTraceOp::Clear => {
                store.clear(&collection).await?;
                overlay = Read::Absent;
            }
            DirtyTraceOp::Get | DirtyTraceOp::PendingOps => { /* assertions below */ }
            DirtyTraceOp::ClearPendingOps => {
                store.clear_pending_ops(&collection)?;
                overlay = Read::Unknown;
            }
        }

        if !check_invariants(&store, &collection, &overlay).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn check_invariants<S>(
    store: &S,
    collection: &CollectionId<ValueKind>,
    overlay: &Read<Bytes>,
) -> Result<bool>
where
    S: DirtyStore,
{
    let read = store.get(collection).await?;
    if &read != overlay {
        return Ok(false);
    }

    // Pending ops exist iff the overlay has observed a value (Set or Clear).
    let pending = store.pending_ops(collection)?;
    if pending.is_some() == matches!(overlay, Read::Unknown) {
        return Ok(false);
    }

    if let Some(p) = pending {
        // Bloat guard: a Value dirty store must compact to exactly one op per
        // collection (last-writer-wins), never accumulate obviated ops. This
        // is the invariant that catches an append-only backend; it lives here
        // (run against every backend via `run_dirty_trace`) rather than in
        // `run_dirty_parity`, whose fold-to-last masks the count.
        if p.count.get() != 1 {
            return Ok(false);
        }
        let folded = fold_value_ops(None, p.ops.collect::<Vec<_>>().iter());
        let folded_read: Read<Bytes> = folded.map_or(Read::Absent, Read::Present);
        if folded_read != read {
            return Ok(false);
        }
    }

    Ok(true)
}

fn collection_id() -> Result<CollectionId<ValueKind>> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new("profile")?,
    ))
}

/// Drives the same trace against `lhs` and `rhs` and asserts they remain
/// observationally equivalent at every step. Used to prove cross-backend
/// equivalence (Memory dirty vs Fjall dirty).
///
/// # Errors
///
/// Propagates store errors raised by either backend.
pub(crate) async fn run_dirty_parity<A, B>(lhs: A, rhs: B, trace: DirtyTrace) -> Result<bool>
where
    A: DirtyStore,
    B: DirtyStore,
{
    let collection = collection_id()?;
    for op in trace.ops {
        match op {
            DirtyTraceOp::Set(byte) => {
                let payload = bytes(byte);
                lhs.set(&collection, payload.clone()).await?;
                rhs.set(&collection, payload).await?;
            }
            DirtyTraceOp::Clear => {
                lhs.clear(&collection).await?;
                rhs.clear(&collection).await?;
            }
            DirtyTraceOp::Get | DirtyTraceOp::PendingOps => {}
            DirtyTraceOp::ClearPendingOps => {
                lhs.clear_pending_ops(&collection)?;
                rhs.clear_pending_ops(&collection)?;
            }
        }

        let lhs_get = lhs.get(&collection).await?;
        let rhs_get = rhs.get(&collection).await?;
        if lhs_get != rhs_get {
            return Ok(false);
        }

        let lhs_pending = lhs.pending_ops(&collection)?;
        let rhs_pending = rhs.pending_ops(&collection)?;
        match (lhs_pending, rhs_pending) {
            (None, None) => {}
            (Some(a), Some(b)) => {
                let a_folded = fold_value_ops(None, a.ops.collect::<Vec<_>>().iter());
                let b_folded = fold_value_ops(None, b.ops.collect::<Vec<_>>().iter());
                if a_folded != b_folded {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Quickcheck-shrinking trace of dirty-store operations.
#[derive(Clone, Debug)]
pub(crate) struct DirtyTrace {
    ops: Vec<DirtyTraceOp>,
}

impl Arbitrary for DirtyTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ops = Vec::<DirtyTraceOp>::arbitrary(g)
            .into_iter()
            .take(MAX_TRACE_OPS)
            .collect();
        Self { ops }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DirtyTraceOp {
    Set(u8),
    Clear,
    Get,
    PendingOps,
    ClearPendingOps,
}

impl Arbitrary for DirtyTraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Get,
            3 => Self::PendingOps,
            _ => Self::ClearPendingOps,
        }
    }
}
