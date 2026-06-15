//! Property-test fixture for the in-memory dirty Value store.
//!
//! There is a single dirty implementation ([`DirtyValueStore`]); the trace is
//! the contract it must uphold over the lane-facing inherent API.
//!
//! Universal dirty contract enforced by [`run_dirty_trace`]:
//!
//! 1. After `Set(x)`: the buffered op overlays to `Present(x)`; a pending op
//!    exists.
//! 2. After `Clear`: the buffered op overlays to `Absent`; a pending op exists.
//! 3. After `ClearCell`: the overlay is `Unknown`; no pending op.
//! 4. Compaction: a Value cell keeps **one** compacted op (last-writer-wins),
//!    and `ValueKind::apply(None, &op)` (over the op the store kept) equals the
//!    overlay — never an accumulation of obviated ops.

use super::super::dirty::DirtyValueStore;
use super::super::identity::CollectionKind;
use super::super::{CollectionId, Read, ValueKind};
use super::value_suite::{MAX_TRACE_OPS, bytes, capped_vec, collection_id};
use bytes::Bytes;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen};

/// Drives `trace` against `store` and returns `true` iff every per-op
/// invariant held.
///
/// # Errors
///
/// Propagates store errors raised by the dirty store.
pub(crate) async fn run_dirty_trace(store: DirtyValueStore, trace: DirtyTrace) -> Result<bool> {
    let collection = collection_id("profile")?;
    let mut overlay = Read::Unknown;

    for op in trace.ops {
        match op {
            DirtyTraceOp::Set(byte) => {
                let payload = bytes(byte);
                store.set(&collection, &(), &payload).await;
                overlay = Read::Present(payload);
            }
            DirtyTraceOp::Clear => {
                store.clear(&collection, &()).await;
                overlay = Read::Absent;
            }
            DirtyTraceOp::Get | DirtyTraceOp::PendingOps => { /* assertions below */ }
            DirtyTraceOp::ClearCell => {
                store.clear_cell(&collection, &());
                overlay = Read::Unknown;
            }
        }

        if !check_invariants(&store, &collection, &overlay) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn check_invariants(
    store: &DirtyValueStore,
    collection: &CollectionId<ValueKind>,
    overlay: &Read<Bytes>,
) -> bool {
    let pending = store.pending_op(collection, &());

    // A pending op exists iff the overlay has observed a value (Set or Clear).
    if pending.is_some() == matches!(overlay, Read::Unknown) {
        return false;
    }

    let Some(op) = pending else {
        // Untouched cell: nothing to fold, overlay is Unknown.
        return matches!(overlay, Read::Unknown);
    };

    // The kept op overlays to the observed value (read-your-writes), and
    // applying it over the empty base agrees — proving exactly one compacted
    // op, never an accumulation.
    let read = ValueKind::read_overlay(&op);
    let applied: Read<Bytes> = ValueKind::apply(None, &op).map_or(Read::Absent, Read::Present);
    &read == overlay && applied == read
}

/// Quickcheck-shrinking trace of dirty-store operations.
#[derive(Clone, Debug)]
pub(crate) struct DirtyTrace {
    ops: Vec<DirtyTraceOp>,
}

impl Arbitrary for DirtyTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DirtyTraceOp {
    Set(u8),
    Clear,
    Get,
    PendingOps,
    ClearCell,
}

impl Arbitrary for DirtyTraceOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => Self::Set(u8::arbitrary(g)),
            1 => Self::Clear,
            2 => Self::Get,
            3 => Self::PendingOps,
            _ => Self::ClearCell,
        }
    }
}
