//! The durable cell-store backend trait.
//!
//! [`CellStore`] is the single, uniform, **untyped** durable backend interface
//! for keyed state. It names no collection family: cells are addressed by
//! [`CellKey`] (a [`Section`](super::cell_key::Section) + ordered
//! [`Coordinate`](super::cell_key::Coordinate)), so Value/Map/Deque are
//! collection-layer handles over this one trait and the durability layer is
//! written exactly once.
//!
//! Its currency is the resolved [`Committed`] cell: `get` and `scan_cells`
//! oracle-resolve any in-flight provisional cell **inside the backend** before
//! yielding, so callers above it (the `Overlay`
//! dirty overlay, the [`Cached`](super::cached::Cached) write-through cache)
//! are oracle-free and merely delegate down. The `own: EventRef` argument lets
//! the bottom store short-circuit to `prev` for the running handler's own
//! provisional cell without an oracle consult (the own-event-base-is-prev
//! invariant); the per-event session injects it, so collections never pass it.
//!
//! # Collection-grain atomicity invariant
//!
//! The three mutators work at **collection grain**: each takes the touched
//! cells of one collection as a slice. The invariant every backend upholds is
//! **atomic multi-cell commit** — a single `write_provisional` /
//! `write_resolved` / `mark_resolved` call (hence `commit_provisional` /
//! `abort_provisional`, which delegate to them) applies *all* its cells
//! together, so no reader and no crash-recovery ever observes a torn subset
//! (some cells written, others not), and on the Cassandra backend every cell
//! shares one write timestamp and one TTL anchor (bounds and entries
//! co-expire).
//!
//! * **Cassandra** packs the cells into **one same-partition `UNLOGGED
//!   BATCH`**: a collection's cells share its row key, so the batch is a single
//!   atomic replica mutation — one round-trip, not one per cell. The **sole**
//!   split is the over-budget fallback: a collection whose cells exceed the
//!   backend batch budget is divided into the *fewest* atomic batches that fit.
//!   The accepted consequence is narrow — an **over-budget** `ReadUncommitted`
//!   multi-cell *resolved* write (which arms no recovery backstop) can crash
//!   between chunks, leaving a torn committed write recovery cannot
//!   reconstruct; within budget that window does not exist, and a staged
//!   (`write_provisional`) write is always recoverable regardless of chunking.
//! * **Memory** loops its writes cell by cell. It needs no batch: one handler
//!   per key system-wide means no observer can witness a partial multi-cell
//!   write, and an in-memory loop never crashes mid-write — atomicity holds by
//!   serialization, not by a transaction.
//!
//! Value is single-cell, so every slice is size-1 and the Cassandra batch
//! degenerates to one statement.

use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Scan};
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use crate::error::ClassifyError;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::error::Error;
use std::future::Future;

/// Uniform durable storage for the cells of one collection partition.
///
/// `get` is a resolving point read and `scan_cells` a resolving single-section
/// range stream; `provisional_cells` is the whole-partition recovery scan. The
/// three mutators take a collection's touched cells as a batch and map onto the
/// durability sequence:
///
/// * [`Self::write_provisional`] — *stage*: writes `data | prev | event` for
///   each cell (the `ReadCommitted` outcome path).
/// * [`Self::write_resolved`] — writes committed values with `event` and `prev`
///   null (the `ReadUncommitted` direct write, the mid-handler flush, and
///   rollback resolution, where the committed value is the staged `prev`).
/// * [`Self::mark_resolved`] — *promote*: nulls `event` and `prev`, keeping
///   `data`. O(1) regardless of value size; the commit arm of resolution.
pub trait CellStore: Clone + Send + Sync + 'static {
    /// Error type for cell-store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads one cell's visible committed value, resolving an in-flight
    /// provisional cell through the oracle (or short-circuiting to its `prev`
    /// when `own` owns it). A missing row resolves to `Committed(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure, a corrupt row shape, or an
    /// oracle failure.
    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a;

    /// Complete, ordered, single-section scan (start positional, section
    /// required). Provisional cells in range are oracle-resolved here; cleared
    /// or absent cells are skipped, so the stream yields only present committed
    /// bytes in `coordinate` byte order.
    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a;

    /// Cache-fill point read: the committed value **plus** the durable cell's
    /// remaining TTL, for the [`Cached`](super::cached::Cached) write-through
    /// cache to mirror with a co-expiring fjall entry. `None` TTL means the
    /// durable row has no expiry (the fjall entry is stamped "never expires").
    ///
    /// Backends with no per-write TTL inherit the default: the committed value
    /// from [`Self::get`] with a `None` TTL. Only the Cassandra store overrides
    /// it (selecting the TTL of whichever blob resolution returns); the TTL is
    /// a best-effort hint, so a wrong or missing value only makes the cache
    /// fall through early, never stale.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on any failure [`Self::get`] would.
    fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<(Committed, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        async move { Ok((self.get(collection, cell, own).await?, None)) }
    }

    /// Cache-fill scan: each present cell's committed bytes **plus** its
    /// remaining TTL, for the [`Cached`](super::cached::Cached) cache to mirror
    /// a covered gap with co-expiring fjall entries. The default delegates
    /// to [`Self::scan_cells`] with a `None` TTL per cell; only the
    /// Cassandra store overrides it (selecting the TTL of whichever blob
    /// resolution returns).
    fn scan_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        self.scan_cells(collection, scan, own)
            .map(|item| item.map(|(cell, bytes)| (cell, bytes, None)))
    }

    /// Streams the whole partition's provisional cells (all sections) for the
    /// recovery sweep, filtering resolved rows in code.
    ///
    /// This is the **cold** recovery source (the bounded durable `kind=Index`
    /// range read + point reads on the Cassandra store). The warm short-circuit
    /// that skips it on a quiescent sweep lives on
    /// [`Cached`](super::cached::Cached).
    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a;

    /// Point-reads one coordinate's provisional cell, or `None` when it is
    /// absent or resolved (over-report-safe). Drives the **warm** recovery
    /// sweep: [`Cached`](super::cached::Cached) has the provisional coordinates
    /// from its local fjall index and rebuilds each `ProvisionalCell` with
    /// this, with zero Cassandra range reads.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a;

    /// Stages each `(cell, write)`'s provisional cell (`data | prev | event`)
    /// in one same-partition batch, binding `collection`'s TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Writes each `(cell, data)` as a resolved cell (`data` committed;
    /// `event`/`prev` null) in one same-partition batch, binding `collection`'s
    /// TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Promotes each `cell`'s provisional cell to resolved: nulls `event` and
    /// `prev`, keeping `data`. O(1) bytes per cell. Idempotent — promoting a
    /// resolved cell is a harmless no-op write.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Settles a staged set as **committed**: each cell's provisional `data`
    /// becomes the committed value. The default is the O(1) promote
    /// ([`Self::mark_resolved`] over the keys); the
    /// [`Cached`](super::cached::Cached) cache overrides it to also publish the
    /// committed `data` projection into fjall, returning the **lower** result
    /// verbatim so a cache-publish failure never gates promotion.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        async move {
            let keys: Vec<CellKey> = writes.iter().map(|(cell, _)| cell.clone()).collect();
            self.mark_resolved(collection, &keys).await
        }
    }

    /// Settles a staged set as **aborted**: each cell's committed base `prev`
    /// is written back as the resolved value. The default is
    /// [`Self::write_resolved`] over the `prev`s; the
    /// [`Cached`](super::cached::Cached) cache overrides it to also publish the
    /// `prev` projection into fjall, returning the **lower** result verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        async move {
            let cells: Vec<(CellKey, Option<Bytes>)> = writes
                .iter()
                .map(|(cell, write)| (cell.clone(), write.prev().cloned()))
                .collect();
            self.write_resolved(collection, &cells).await
        }
    }
}
