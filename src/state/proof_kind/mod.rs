//! A `#[cfg(test)]` second collection kind that drives the **real generic
//! [`Lane`](crate::state::session::lane) body** directly (the
//! `Lane<CounterKind>` trace+model property in that module), proving the
//! per-kind machinery the Value lane shares generalizes to a non-Value kind —
//! without a test kind in any production type. It is **not** a parallel test
//! copy of the lane, and it is no longer wired into the session.
//!
//! [`CounterKind`] is deliberately unlike Value on the three axes the machinery
//! must generalize over:
//!
//! * a **non-`()` `CellAddr`** ([`u32`]), so a collection holds many addressed
//!   cells (the composition test touches data cells *and* a header cell in one
//!   collection, staged in one batch);
//! * a **non-LWW `combine`** — additive deltas fold by addition, so
//!   `combine(existing, newest)` is order-dependent (not last-writer-wins) and
//!   the `combine`-then-`apply` path is genuinely exercised, not bypassed;
//! * an **`apply` that reads the committed base** ([`Read::Unknown`] overlay),
//!   so the own-event-`prev` idempotency-under-retry contract is tested on a
//!   kind where the base actually matters.
//!
//! The cells are little-endian `i64` counters. The stores are in-memory and
//! count their batch calls so the bulk-apply pin can assert one batched store
//! call per collection.

use super::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use super::identity::{CollectionId, CollectionKind, CollectionKindId, CollectionRef};
use super::partition_store::CommittedCache;
use super::store::CellStore;
use super::{EventRef, Read};
use ahash::RandomState;
use bytes::Bytes;
use futures::{Stream, stream};
use scc::hash_map::Entry;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The reserved header cell address: a collection's running sum, maintained
/// transactionally beside its data cells.
pub(crate) const HEADER_ADDR: u32 = u32::MAX;

/// Type marker for the proof kind: addressed `i64` counter cells.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct CounterKind;

/// An additive counter op: optionally reset to zero, then add `delta`.
///
/// One combined op per cell expresses any history: `clear` is "reset to 0",
/// `set`/increment is "add this delta", and [`CounterKind::combine`] folds them
/// in arrival order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CounterOp {
    /// Reset the cell to zero before applying `delta` (a `clear` in the same
    /// combined op wipes the accumulated delta before it).
    pub(crate) reset_first: bool,

    /// Signed delta added after any reset.
    pub(crate) delta: i64,
}

impl CollectionKind for CounterKind {
    type CellAddr = u32;
    type Op = CounterOp;

    const ID: CollectionKindId = CollectionKindId::TestSecondary;

    /// A set carries an encoded delta to add (non-resetting).
    fn set_op(cell: &[u8]) -> CounterOp {
        CounterOp {
            reset_first: false,
            delta: decode_i64(cell),
        }
    }

    /// A clear resets the cell to zero.
    fn clear_op() -> CounterOp {
        CounterOp {
            reset_first: true,
            delta: 0,
        }
    }

    /// Additive, **arrival-ordered** fold: a later reset wipes the prior
    /// history; otherwise the deltas add. Not commutative across a reset, so
    /// this genuinely exercises the non-LWW `combine` path.
    fn combine(existing: CounterOp, newest: CounterOp) -> CounterOp {
        if newest.reset_first {
            newest
        } else {
            CounterOp {
                reset_first: existing.reset_first,
                // Wrapping so a long fold cannot panic; addition is
                // associative under wraparound, so the compacted op still
                // matches naive replay.
                delta: existing.delta.wrapping_add(newest.delta),
            }
        }
    }

    /// The buffered op never determines the cell on its own — an additive delta
    /// needs the committed base — so the read always falls through to `apply`.
    fn read_overlay(_op: &CounterOp) -> Read<Bytes> {
        Read::Unknown
    }

    /// Folds the combined op over the committed base: reset zeroes the base
    /// first, then the delta is added.
    fn apply(committed_base: Option<Bytes>, op: &CounterOp) -> Option<Bytes> {
        let base = if op.reset_first {
            0
        } else {
            committed_base.as_ref().map_or(0, |b| decode_i64(b))
        };
        Some(Bytes::copy_from_slice(
            &base.wrapping_add(op.delta).to_le_bytes(),
        ))
    }
}

/// Decodes a little-endian `i64` from a cell's bytes (zero on a short/empty
/// cell — no `unwrap`/`ok`).
pub(crate) fn decode_i64(bytes: &[u8]) -> i64 {
    let mut buf = [0_u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    i64::from_le_bytes(buf)
}

/// Encodes a delta as the little-endian cell bytes the session's `set_cell`
/// takes.
pub(crate) fn encode_delta(delta: i64) -> [u8; 8] {
    delta.to_le_bytes()
}

/// In-memory [`CellStore`] for [`CounterKind`], counting its batch calls so the
/// bulk-apply pin can assert one batched store call per collection.
#[derive(Clone, Default)]
pub(crate) struct MemoryCounterStore {
    inner: Arc<CounterInner>,
}

#[derive(Default)]
struct CounterInner {
    cells: scc::HashMap<(CollectionId<CounterKind>, u32), StoredCounterCell, RandomState>,
    write_provisional_calls: AtomicUsize,
    write_resolved_calls: AtomicUsize,
    mark_resolved_calls: AtomicUsize,
}

#[derive(Clone)]
enum StoredCounterCell {
    Resolved(Option<Bytes>),
    Provisional {
        data: Option<Bytes>,
        prev: Option<Bytes>,
        event: EventRef,
    },
}

impl StoredCounterCell {
    fn to_cell(&self) -> Cell {
        match self {
            Self::Resolved(data) => Cell::Resolved(Committed::new(data.clone())),
            Self::Provisional { data, prev, event } => {
                Cell::Provisional(ProvisionalCell::new(data.clone(), prev.clone(), *event))
            }
        }
    }
}

impl MemoryCounterStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Batched provisional-write calls (one per collection-grain stage).
    pub(crate) fn provisional_write_calls(&self) -> usize {
        self.inner.write_provisional_calls.load(Ordering::Relaxed)
    }

    /// Batched promote calls (one per collection-grain commit).
    pub(crate) fn mark_resolved_calls(&self) -> usize {
        self.inner.mark_resolved_calls.load(Ordering::Relaxed)
    }

    /// Batched resolved-write calls (one per collection-grain rollback / RU
    /// write).
    pub(crate) fn write_resolved_calls(&self) -> usize {
        self.inner.write_resolved_calls.load(Ordering::Relaxed)
    }
}

impl CellStore<CounterKind> for MemoryCounterStore {
    type Error = Infallible;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<CounterKind>,
        addr: &'a u32,
    ) -> Result<Cell, Self::Error> {
        Ok(self
            .inner
            .cells
            .read_async(&(collection.clone(), *addr), |_, cell| cell.to_cell())
            .await
            .unwrap_or_else(|| Cell::Resolved(Committed::new(None))))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<CounterKind>,
    ) -> impl Stream<Item = Result<(u32, ProvisionalCell), Self::Error>> + Send + 'a {
        let mut found = Vec::new();
        self.inner.cells.iter_sync(|(id, addr), cell| {
            if id == collection
                && let StoredCounterCell::Provisional { data, prev, event } = cell
            {
                found.push((
                    *addr,
                    ProvisionalCell::new(data.clone(), prev.clone(), *event),
                ));
            }
            true
        });
        stream::iter(found.into_iter().map(Ok))
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<CounterKind>,
        writes: &'a [(u32, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_provisional_calls
            .fetch_add(1, Ordering::Relaxed);
        for (addr, write) in writes {
            self.inner
                .cells
                .upsert_async(
                    (collection.id().clone(), *addr),
                    StoredCounterCell::Provisional {
                        data: write.data().cloned(),
                        prev: write.prev().cloned(),
                        event: write.event(),
                    },
                )
                .await;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<CounterKind>,
        cells: &'a [(u32, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.inner
            .write_resolved_calls
            .fetch_add(1, Ordering::Relaxed);
        for (addr, data) in cells {
            self.inner
                .cells
                .upsert_async(
                    (collection.id().clone(), *addr),
                    StoredCounterCell::Resolved(data.clone()),
                )
                .await;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<CounterKind>,
        addrs: &'a [u32],
    ) -> Result<(), Self::Error> {
        self.inner
            .mark_resolved_calls
            .fetch_add(1, Ordering::Relaxed);
        for addr in addrs {
            if let Entry::Occupied(mut entry) = self
                .inner
                .cells
                .entry_async((collection.id().clone(), *addr))
                .await
                && let StoredCounterCell::Provisional { data, .. } = entry.get()
            {
                let data = data.clone();
                *entry.get_mut() = StoredCounterCell::Resolved(data);
            }
        }
        Ok(())
    }
}

/// In-memory [`CommittedCache`] for [`CounterKind`], keyed by `(collection,
/// addr)`.
#[derive(Clone, Default)]
pub(crate) struct MemoryCounterCache {
    inner: Arc<scc::HashMap<(CollectionId<CounterKind>, u32), Committed, RandomState>>,
}

impl MemoryCounterCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl CommittedCache<CounterKind> for MemoryCounterCache {
    type Error = Infallible;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<CounterKind>,
        addr: &'a u32,
    ) -> Result<Option<Committed>, Self::Error> {
        Ok(self
            .inner
            .read_async(&(collection.clone(), *addr), |_, v| v.clone())
            .await)
    }

    async fn put<'a>(
        &'a self,
        collection: &'a CollectionId<CounterKind>,
        addr: &'a u32,
        value: &'a Committed,
    ) -> Result<(), Self::Error> {
        self.inner
            .upsert_async((collection.clone(), *addr), value.clone())
            .await;
        Ok(())
    }

    async fn invalidate<'a>(
        &'a self,
        collection: &'a CollectionId<CounterKind>,
        addr: &'a u32,
    ) -> Result<(), Self::Error> {
        self.inner.remove_async(&(collection.clone(), *addr)).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
