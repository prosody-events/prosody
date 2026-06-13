//! The durable cell-store backend trait.
//!
//! [`CellStore`] is the single durable backend interface for keyed state. It
//! replaces the write-ahead-log-era split of `DurableWalStore` +
//! `DirectApplyStore` + the pending index: every durable mutation writes a
//! full, self-consistent column shape in one statement, and the only
//! resolution moves are "promote" ([`CellStore::mark_resolved`]) and "write a
//! resolved value" ([`CellStore::write_resolved`]).
//!
//! The trait is generic over the collection kind `K` and addresses cells by
//! [`K::CellAddr`](CollectionKind::CellAddr) — `()` for Value, the entry key
//! for Map, a slot marker for Deque — so the durability layer is written
//! once and only the per-kind table shape differs.

use super::cell::{Cell, ProvisionalCell, ProvisionalWrite};
use super::identity::{CollectionId, CollectionKind, CollectionRef};
use crate::error::ClassifyError;
use bytes::Bytes;
use futures::Stream;
use std::error::Error;
use std::future::Future;

/// Durable storage for the cells of one collection kind.
///
/// Every method writes or reads a single self-consistent cell shape. The
/// three mutators map onto the durability sequence:
///
/// * [`Self::write_provisional`] — *stage*: writes `data | prev | event` in one
///   statement (the `ReadCommitted` outcome path).
/// * [`CellStore::write_resolved`] — writes a committed value with `event` and
///   `prev` null (the `ReadUncommitted` direct write, the mid-handler flush,
///   and rollback resolution, where the committed value is the staged `prev`).
/// * [`CellStore::mark_resolved`] — *promote*: nulls `event` and `prev`,
///   keeping `data`. O(1) regardless of value size; the commit arm of
///   resolution.
pub trait CellStore<K>: Clone + Send + Sync + 'static
where
    K: CollectionKind,
{
    /// Error type for cell-store operations.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Reads one cell. A missing row decodes as
    /// [`Cell::Resolved`]`(Committed(None))`.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure or a corrupt row shape.
    fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
        addr: &'a K::CellAddr,
    ) -> impl Future<Output = Result<Cell, Self::Error>> + Send + 'a;

    /// Streams the provisional cells of a collection, for the recovery
    /// sweep. Value yields at most one entry (addr `()`); Map yields one per
    /// provisional entry row.
    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<K>,
    ) -> impl Stream<Item = Result<(K::CellAddr, ProvisionalCell), Self::Error>> + Send + 'a;

    /// Stages a provisional write (`data | prev | event`) in one statement,
    /// binding `collection`'s TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<K>,
        addr: &'a K::CellAddr,
        write: &'a ProvisionalWrite,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Writes a resolved cell (`data` committed; `event`/`prev` null) in one
    /// statement, binding `collection`'s TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<K>,
        addr: &'a K::CellAddr,
        data: Option<&'a Bytes>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Promotes a provisional cell to resolved: nulls `event` and `prev`,
    /// keeping `data`. O(1) bytes. Idempotent — promoting a resolved cell is
    /// a harmless no-op write.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] on a store failure.
    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<K>,
        addr: &'a K::CellAddr,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
