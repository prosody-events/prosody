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
//! yielding, so callers above it (the [`Overlay`](super::overlay::Overlay)
//! dirty overlay, the [`Cached`](super::cached::Cached) write-through cache)
//! are oracle-free and merely delegate down. The `own: EventRef` argument lets
//! the bottom store short-circuit to `prev` for the running handler's own
//! provisional cell without an oracle consult (the own-event-base-is-prev
//! invariant); the per-event session injects it, so collections never pass it.
//!
//! # Collection-grain batches
//!
//! The three mutators work at **collection grain**: each takes the touched
//! cells of one collection as a slice, so a collection with many cells (a Map
//! entry set, a Deque slot range and its bounds) stages or promotes them in
//! **one same-partition `UNLOGGED BATCH`** rather than one round-trip per cell.
//! A collection's cells share its row key, so the batch is a single atomic
//! mutation on the replica. Value is single-cell, so every slice is size-1 and
//! the batch degenerates to one statement.

use super::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use super::cell_key::{CellKey, Scan};
use super::event_ref::EventRef;
use super::identity::{CollectionId, CollectionRef};
use crate::error::ClassifyError;
use bytes::Bytes;
use futures::Stream;
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

    /// Streams the whole partition's provisional cells (all sections) for the
    /// recovery sweep, filtering resolved rows in code.
    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a;

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
}
