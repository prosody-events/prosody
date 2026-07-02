//! Cassandra-backed uniform cell store.
//!
//! [`CassandraStore`] implements the untyped [`CellStore`] over the
//! `keyed_state_cell` table provisioned by migration
//! `20260522_create_keyed_state.cql`. Every durable mutation writes one
//! self-consistent cell-column shape in a single statement, so the
//! applied-triple desync class the write-ahead-log design fought is unwritable
//! here.
//!
//! It is the **bottom** store: it owns the commit oracle (via the composed
//! [`Resolver`]) and oracle-resolves any in-flight provisional cell inside
//! `get`/`scan_cells` before yielding, so the layers above it
//! ([`Cached`](crate::state::cached::Cached),
//! [`Overlay`](crate::state::overlay::Overlay)) are oracle-free.
//!
//! # Cell rows and the provisional index
//!
//! The partition's leading clustering [`CellKind`] splits it into two disjoint
//! ranges. A `kind=Cell` row is one cell over the columns `data | prev_data |
//! encoding | version | event`, addressed by `(section, coordinate)`. Beside
//! every staged cell, a bare `kind=Index` marker row records its coordinate;
//! recovery ([`provisional_cells`](CellStore::provisional_cells)) reads only
//! that front-of-partition marker range and point-reads each marked cell, so
//! its cost is proportional to the number of provisional cells, never the
//! partition size. A cell's `kind=Cell` mutation and its `kind=Index` marker
//! are written as one atomic same-partition batch unit, so the two never tear
//! apart. The three mutators write exactly one cell-column shape each:
//!
//! * [`write_provisional`](CellStore::write_provisional) — *stage*: `data`,
//!   `prev_data`, `event`, and the shared `encoding`/`version` in one `UPDATE`.
//!   The encoding/version flags key on **either** blob being present (a
//!   clear-over-present stages `data = null` with a non-null `prev_data`, which
//!   still needs an encoding).
//! * [`write_resolved`](CellStore::write_resolved) — writes a committed value
//!   with `prev_data`/`event` nulled (the `ReadUncommitted` direct write, the
//!   mid-handler flush, and rollback resolution).
//! * [`mark_resolved`](CellStore::mark_resolved) — *promote*: nulls `prev_data`
//!   and `event` only, keeping `data` and its TTL. O(1) bytes.
//!
//! # Promote-of-clear residue
//!
//! Promoting a staged *clear* (`data = null`, `prev_data = Some`) nulls
//! `prev_data`/`event` but leaves the row's `encoding`/`version` populated with
//! `data` still null. That shape — encoding/version present, both blobs null —
//! is a legitimate `Resolved(Committed(None))`, **not** corruption. The decoder
//! validates encoding/version per-blob, never as a row-level "encoding implies
//! a blob" rule, precisely so this residue decodes cleanly.
//!
//! # Concurrency
//!
//! The framework guarantees one handler per key system-wide (Kafka partition
//! ownership + in-process per-key serialization), so this store never needs
//! LWTs or distributed locks.

mod decode;

#[cfg(test)]
mod tests;

use crate::cassandra::CassandraStore as CassandraSession;
use crate::cassandra::TABLE_KEYED_STATE_CELL;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{
    BatchRow, BatchUnit, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, PER_STATEMENT_OVERHEAD,
};
use crate::cassandra_queries;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Direction, Scan};
use crate::state::encoding::{Encoding, encode_payload};
use crate::state::event_ref::EventRef;
use crate::state::oracle::CommitOracle;
use crate::state::registry::CollectionDefRegistry;
use crate::state::resolve::{ResolveCellError, Resolver, flatten_resolve, resolve_read};
use crate::state::store::CellStore;
use crate::state::{CollectionId, CollectionRef, StateType};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use decode::{CellTtlRow, KeyedCellRow, RawCellRow};
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::client::session::Session;
use scylla::serialize::SerializationError;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::writers::RowWriter;
use scylla::statement::prepared::PreparedStatement;
use smallvec::smallvec;
use std::ops::Bound;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::coop::cooperative;

pub use crate::state::cassandra::error::CassandraCellStoreError;
pub use decode::CellCorruptReason;

/// Payload encoding for cell blobs written by this build.
const VALUE_ENCODING: Encoding = Encoding::RawZstdV1;

/// The only cell `version` stamp this build writes or accepts.
///
/// Every authoritative cell stamps the version its bytes were written under;
/// this build writes version 1 and rejects any other at decode
/// ([`decode::validate_version`]). Per-key identity migration is future work —
/// the stamp is the dormant hook it would build on.
pub const INITIAL_VERSION: i32 = 1;

/// The leading clustering discriminator that splits a collection's partition
/// into two disjoint front-to-back ranges.
///
/// [`Cell`](Self::Cell) rows carry the full cell columns;
/// [`Index`](Self::Index) rows are bare provisional-coordinate markers
/// co-clustered ahead of them, so recovery reads only the (bounded) `Index`
/// range instead of scanning every cell. It is **always bound as a constant
/// clustering predicate**, never decoded back into a value — hence
/// serialize-only ([`SerializeValue`] in [`super::serialize`]), with no
/// `TryFrom`/`DeserializeValue`.
///
/// # Reserved-`kind` safety (invariant 6)
///
/// `kind` is **cell-store-internal**: it splits the physical partition into the
/// data slice and the recovery-marker slice, and no collection may address the
/// marker slice. This is enforced structurally, not by a runtime check:
///
/// * `CellKind` and its `Index` variant are private to this module — never
///   re-exported, never reachable from a collection.
/// * A collection addresses a cell only through a [`CellKey`], which carries
///   **only** `(section, coordinate)` — it has no `kind` field, so the marker
///   slice is unnameable from the collection layer.
/// * This store binds `kind` itself, as the compile-time constant
///   `CellKind::Cell` on every data read/write and `CellKind::Index` only on
///   the marker mutators.
///
/// So "a collection reads or writes the index slice" is uncompilable, and no
/// assertion or property test is needed to defend it.
///
/// [`CellKey`]: crate::state::cell_key::CellKey
/// [`SerializeValue`]: scylla::serialize::value::SerializeValue
#[repr(i8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum CellKind {
    /// A cell row: the full `data | prev_data | encoding | version | event`
    /// column shape.
    Cell = 0,

    /// A bare provisional-coordinate marker row (only the clustering key is
    /// populated).
    Index = 1,
}

impl From<CellKind> for i8 {
    fn from(kind: CellKind) -> Self {
        kind as i8
    }
}

/// The bottom store's resolving read/sweep error: a raw store failure or an
/// oracle consult failure.
pub type CellStoreError<OracleErr> = ResolveCellError<CassandraCellStoreError, OracleErr>;

/// The session + prepared statements a [`CassandraStore`] is built from, shared
/// across partitions. The per-partition oracle and registry are supplied at
/// [`CassandraStateBackendFactory::for_partition`] time, so the resolving cell
/// store cannot be pre-built — this holds the partition-independent pieces.
///
/// [`CassandraStateBackendFactory::for_partition`]: crate::state::production::CassandraStateBackendFactory
#[derive(Clone)]
pub struct CassandraCellResources {
    pub(crate) session: CassandraSession,
    pub(crate) queries: Arc<CellQueries>,
}

impl CassandraCellResources {
    /// Bundles the shared session and prepared cell statements.
    #[must_use]
    pub fn new(session: CassandraSession, queries: Arc<CellQueries>) -> Self {
        Self { session, queries }
    }
}

/// Test-only recovery-read counters, incremented inside
/// [`provisional_cells`](CellStore::provisional_cells) — the **unconditional
/// cold seed**. The seeded-latch short-circuit lives one layer up on
/// [`Cached`](crate::state::cached::Cached), which calls this only on a cold
/// sweep, so a warm-clean sweep never enters here and both counters stay at
/// zero — the non-vacuous "zero queries on quiescence" signal.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecoveryReadCounts {
    /// `kind=Index` range queries issued — one per cold seed, zero when warm.
    pub(crate) index_range_reads: AtomicUsize,
    /// `kind=Cell` point reads issued during recovery — bounded by
    /// #provisional.
    pub(crate) cell_point_reads: AtomicUsize,
}

/// Cassandra-backed uniform cell store.
#[derive(Clone, Debug)]
pub struct CassandraStore<O> {
    session: CassandraSession,
    queries: Arc<CellQueries>,
    resolver: Resolver<O>,
    #[cfg(test)]
    counters: Arc<RecoveryReadCounts>,
}

impl<O> CassandraStore<O> {
    /// Creates a Cassandra cell store over an existing session, a prepared
    /// [`CellQueries`] set, the commit oracle it resolves provisional cells
    /// through, and the registry that supplies per-collection TTLs.
    #[must_use]
    pub fn new(
        session: CassandraSession,
        queries: Arc<CellQueries>,
        oracle: O,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self {
            session,
            queries,
            resolver: Resolver::new(oracle, registry),
            #[cfg(test)]
            counters: Arc::default(),
        }
    }

    /// Test handle on the recovery-read counters (shared across clones), for
    /// the zero-query and bounded-recovery assertions.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn recovery_reads(&self) -> Arc<RecoveryReadCounts> {
        self.counters.clone()
    }

    fn cql(&self) -> &Session {
        self.session.session()
    }

    async fn read_raw(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<RawCellRow>, CassandraCellStoreError> {
        let pk = Pk::of(id);
        let row = self
            .cql()
            .execute_unpaged(
                &self.queries.read_cell,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    CellKind::Cell,
                    i8::from(cell.section),
                    cell.coordinate.as_bytes(),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<RawCellRow>()
            .map_err(CassandraStoreError::from)?;
        Ok(row)
    }

    async fn read_raw_ttl(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<CellTtlRow>, CassandraCellStoreError> {
        let pk = Pk::of(id);
        let row = self
            .cql()
            .execute_unpaged(
                &self.queries.read_cell_ttl,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    CellKind::Cell,
                    i8::from(cell.section),
                    cell.coordinate.as_bytes(),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<CellTtlRow>()
            .map_err(CassandraStoreError::from)?;
        Ok(row)
    }

    async fn write_provisional_raw(
        &self,
        collection: &CollectionRef,
        writes: &[(CellKey, ProvisionalWrite)],
    ) -> Result<(), CassandraCellStoreError> {
        let pk = Pk::of(collection.id());
        // Encode every cell's blobs up front (this Vec owns the `Bytes`); the
        // bound rows borrow into it and into each input cell's coordinate slice,
        // so the whole batch is one `blobs` allocation with no per-cell copy.
        let mut blobs = Vec::with_capacity(writes.len());
        for (_, write) in writes {
            blobs.push(encode_cell_blobs(write.data(), write.prev())?);
        }

        // The collection TTL is uniform, so the with-TTL vs no-TTL choice — and
        // hence which statement each row carries — is made once for the whole
        // batch, never per cell.
        let ttl = collection.ttl().map(ttl_to_i32);
        let (cell_stmt, index_stmt) = if ttl.is_some() {
            (&self.queries.write_provisional, &self.queries.index_insert)
        } else {
            (
                &self.queries.write_provisional_no_ttl,
                &self.queries.index_insert_no_ttl,
            )
        };
        // One unit per cell holds the cell mutation and its index marker as an
        // indivisible pair, so `chunk_boundaries` never tears a cell's two rows
        // into separate batches. Weight = blob bytes + one overhead for the cell
        // row + one for the bare index row.
        let units: Vec<BatchUnit<CellBatchRow>> = blobs
            .iter()
            .zip(writes)
            .map(|(blob, (cell, write))| {
                let addr = CellAddr::new(pk, cell);
                BatchUnit::new(
                    blob_weight(blob) + PER_STATEMENT_OVERHEAD,
                    smallvec![
                        CellBatchRow::StageCell {
                            statement: cell_stmt,
                            row: StageRow {
                                ttl,
                                data: blob.data.as_deref(),
                                prev_data: blob.prev_data.as_deref(),
                                encoding: blob.encoding,
                                version: blob.version,
                                event: write.event(),
                                addr,
                            },
                        },
                        CellBatchRow::IndexUpsert {
                            statement: index_stmt,
                            row: IndexUpsertRow { ttl, addr },
                        },
                    ],
                )
            })
            .collect();
        self.session
            .execute_unlogged_batches(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS)
            .await?;
        Ok(())
    }

    async fn write_resolved_raw(
        &self,
        collection: &CollectionRef,
        cells: &[(CellKey, Option<Bytes>)],
    ) -> Result<(), CassandraCellStoreError> {
        let pk = Pk::of(collection.id());
        // Encode each cell's committed `data` up front (owns the `Bytes`); no
        // `prev`, so the blobs carry only `data` + its encoding/version.
        let mut blobs = Vec::with_capacity(cells.len());
        for (_, data) in cells {
            blobs.push(encode_cell_blobs(data.as_ref(), None)?);
        }

        let ttl = collection.ttl().map(ttl_to_i32);
        let cell_stmt = if ttl.is_some() {
            &self.queries.write_resolved
        } else {
            &self.queries.write_resolved_no_ttl
        };
        // Each unit pairs the resolved-value write with an `index_delete`. On the
        // rollback path (resolving a provisional cell to its `prev`) this clears
        // the cell's provisional-coordinate marker. On a fresh committed write (a
        // ReadUncommitted direct write / mid-handler flush that was never staged)
        // no marker exists, so the delete is a harmless no-op tombstone in the
        // `kind=Index` slice. Pairing unconditionally keeps a cell's rows one
        // atomic unit; differentiating rollback from fresh-commit to skip the
        // no-op is future work — `write_resolved` carries no provisional signal.
        let units: Vec<BatchUnit<CellBatchRow>> = blobs
            .iter()
            .zip(cells)
            .map(|(blob, (cell, _))| {
                let addr = CellAddr::new(pk, cell);
                BatchUnit::new(
                    blob_weight(blob) + PER_STATEMENT_OVERHEAD,
                    smallvec![
                        CellBatchRow::RollbackCell {
                            statement: cell_stmt,
                            row: ResolvedRow {
                                ttl,
                                data: blob.data.as_deref(),
                                encoding: blob.encoding,
                                version: blob.version,
                                addr,
                            },
                        },
                        CellBatchRow::IndexDelete {
                            statement: &self.queries.index_delete,
                            row: KeyRow {
                                kind: CellKind::Index,
                                addr,
                            },
                        },
                    ],
                )
            })
            .collect();
        self.session
            .execute_unlogged_batches(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS)
            .await?;
        Ok(())
    }

    async fn mark_resolved_raw(
        &self,
        collection: &CollectionRef,
        cells: &[CellKey],
    ) -> Result<(), CassandraCellStoreError> {
        let pk = Pk::of(collection.id());
        // Promotes carry no blob — only key columns — so every unit weighs a
        // fixed overhead per member row (the cell promote + its index-marker
        // delete) and the byte budget never bites; the count budget alone splits
        // an enormous promote set.
        let units: Vec<BatchUnit<CellBatchRow>> = cells
            .iter()
            .map(|cell| {
                let addr = CellAddr::new(pk, cell);
                BatchUnit::new(
                    2 * PER_STATEMENT_OVERHEAD,
                    smallvec![
                        CellBatchRow::PromoteCell {
                            statement: &self.queries.mark_resolved,
                            row: KeyRow {
                                kind: CellKind::Cell,
                                addr,
                            },
                        },
                        CellBatchRow::IndexDelete {
                            statement: &self.queries.index_delete,
                            row: KeyRow {
                                kind: CellKind::Index,
                                addr,
                            },
                        },
                    ],
                )
            })
            .collect();
        self.session
            .execute_unlogged_batches(&units, MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS)
            .await?;
        Ok(())
    }
}

impl<O> CassandraStore<O>
where
    O: CommitOracle,
{
    /// The single resolving section scan, yielding each present cell's
    /// committed bytes **and** its remaining `data` TTL.
    /// [`scan_cells`](CellStore::scan_cells) drops the TTL;
    /// [`scan_for_cache`](CellStore::scan_for_cache) keeps it.
    fn scan_inner<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<
        Item = Result<(CellKey, Bytes, Option<CompactDuration>), CellStoreError<O::Error>>,
    > + Send
    + 'a {
        let pk = Pk::of(collection).owned();
        let section = i8::from(scan.section);
        let dir = scan.dir;
        let limit = scan.limit;
        // The start bound goes into CQL (the comparator is chosen by selecting
        // one of the per-bound prepared statements; an unbounded start binds no
        // coordinate at all). The end bound is enforced in-code by `past_end`,
        // so it needs no statement variant — only an owned copy to compare
        // against each streamed coordinate.
        let statement = match (dir, &scan.start) {
            (Direction::Forward, Bound::Unbounded) => self.queries.scan_forward_all.clone(),
            (Direction::Forward, Bound::Included(_)) => self.queries.scan_forward_incl.clone(),
            (Direction::Forward, Bound::Excluded(_)) => self.queries.scan_forward_excl.clone(),
            (Direction::Backward, Bound::Unbounded) => self.queries.scan_backward_all.clone(),
            (Direction::Backward, Bound::Included(_)) => self.queries.scan_backward_incl.clone(),
            (Direction::Backward, Bound::Excluded(_)) => self.queries.scan_backward_excl.clone(),
        };
        let start_coord: Option<Vec<u8>> = match scan.start {
            Bound::Unbounded => None,
            Bound::Included(c) | Bound::Excluded(c) => Some(c.as_bytes().to_vec()),
        };
        let end: Bound<Vec<u8>> = match scan.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(c) => Bound::Included(c.as_bytes().to_vec()),
            Bound::Excluded(c) => Bound::Excluded(c.as_bytes().to_vec()),
        };
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            // Both arms yield the same `QueryPager`; the bounded arm binds the
            // start coordinate, the unbounded arm omits it (its statement has no
            // coordinate marker).
            let pager = match &start_coord {
                Some(coord) => self
                    .cql()
                    .execute_iter(
                        statement,
                        (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), CellKind::Cell, section, coord.as_slice()),
                    )
                    .await,
                None => self
                    .cql()
                    .execute_iter(
                        statement,
                        (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), CellKind::Cell, section),
                    )
                    .await,
            };
            let stream = pager
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?
                .rows_stream::<KeyedCellRow>()
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?;
            pin_mut!(stream);

            let mut yielded = 0usize;
            // Deliberately sequential, not an oversight: the common `resolve_read`
            // is a free no-op (own-event provisional / already-resolved cells
            // consult no oracle and write nothing), so steady-state scans gain
            // nothing from fan-out; the only payoff is mid-recovery across many
            // foreign provisional cells. And because `limit` counts *present*
            // yields — knowable only post-resolve — a buffered pipeline would
            // resolve up to N−1 foreign-provisional cells past the boundary, each
            // a durable write-back: read-path write amplification we won't pay for
            // a recovery-only win on a hot read path.
            while let Some(row) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?
            {
                // The limit bounds *yielded* (present) cells; check it before
                // processing the next row so `Some(0)` yields nothing (an absent
                // cell never consumes a slot — only a present yield does).
                if limit.is_some_and(|n| yielded >= n) {
                    break;
                }
                let (key, raw, ttl) =
                    decode::try_decode_keyed_cell_ttl(row).map_err(ResolveCellError::Store)?;
                if past_end(dir, &key, end.as_ref().map(Vec::as_slice)) {
                    break;
                }
                let committed =
                    resolve_read(self, self.resolver.oracle(), &collection_ref, &key, own, raw)
                        .await
                        .map_err(flatten_resolve)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes, ttl_seconds_to_duration(ttl));
                    yielded += 1;
                }
            }
        }
    }
}

impl<O> CellStore for CassandraStore<O>
where
    O: CommitOracle,
{
    type Error = CellStoreError<O::Error>;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<Committed, Self::Error> {
        let raw = match self
            .read_raw(collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        {
            Some(row) => decode::try_decode_cell(row).map_err(ResolveCellError::Store)?,
            None => Cell::Resolved(Committed::new(None)),
        };
        let collection_ref = self.resolver.collection_ref(collection);
        resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        let (raw, ttl) = match self
            .read_raw_ttl(collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        {
            Some(row) => {
                let (cell, ttl) =
                    decode::try_decode_cell_ttl(row).map_err(ResolveCellError::Store)?;
                (cell, ttl)
            }
            None => (Cell::Resolved(Committed::new(None)), None),
        };
        let collection_ref = self.resolver.collection_ref(collection);
        let committed = resolve_read(
            self,
            self.resolver.oracle(),
            &collection_ref,
            cell,
            own,
            raw,
        )
        .await
        .map_err(flatten_resolve)?;
        Ok((committed, ttl_seconds_to_duration(ttl)))
    }

    fn scan_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes, Option<CompactDuration>), Self::Error>> + Send + 'a
    {
        self.scan_inner(collection, scan, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // Same scan, dropping the per-cell TTL the cache-fill variant keeps.
        self.scan_inner(collection, scan, own)
            .map_ok(|(key, bytes, _ttl)| (key, bytes))
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        try_stream! {
            // The unconditional **cold seed**: one bounded front-of-partition
            // `kind=Index` range read yields the provisional coordinate list
            // (cost ∝ #provisional, never #cells) — the durable recovery source.
            // The warm short-circuit that skips this on a quiescent sweep lives
            // one layer up on `Cached` (which owns the fjall warm index), so this
            // is reached only on a cold sweep. There is no whole-partition scan.
            let coords = {
                let pk = Pk::of(collection).owned();
                #[cfg(test)]
                self.counters.index_range_reads.fetch_add(1, Ordering::Relaxed);
                let index = self
                    .cql()
                    .execute_iter(
                        self.queries.index_scan.clone(),
                        (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), CellKind::Index),
                    )
                    .await
                    .map_err(CassandraStoreError::from)
                    .map_err(into_store_err::<O>)?
                    .rows_stream::<decode::IndexRow>()
                    .map_err(CassandraStoreError::from)
                    .map_err(into_store_err::<O>)?;
                pin_mut!(index);
                // N (= #provisional) is bounded-small, so the drain stays
                // sequential behind the `cooperative` checkpoint that yields a
                // large recovery drain to the runtime every ~128 items.
                let mut coords: Vec<CellKey> = Vec::new();
                while let Some(row) = cooperative(index.try_next())
                    .await
                    .map_err(CassandraStoreError::from)
                    .map_err(into_store_err::<O>)?
                {
                    coords.push(decode::index_cell_key(row));
                }
                coords
            };

            // Point-read each coordinate's `kind=Cell` row to rebuild its
            // `ProvisionalCell`. A coordinate whose row is absent (a
            // compaction-window straggler — cell and marker share one TTL) or
            // decodes `Cell::Resolved` (concurrently resolved, or a leftover set
            // entry) is silently dropped — both over-report-safe.
            for key in coords {
                #[cfg(test)]
                self.counters.cell_point_reads.fetch_add(1, Ordering::Relaxed);
                let Some(raw) = self
                    .read_raw(collection, &key)
                    .await
                    .map_err(ResolveCellError::Store)?
                else {
                    continue;
                };
                let cell = decode::try_decode_cell(raw).map_err(ResolveCellError::Store)?;
                if let Cell::Provisional(provisional) = cell {
                    yield (key, provisional);
                }
            }
        }
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // Point-read the `kind=Cell` row and keep only a genuinely provisional
        // shape; an absent or resolved coordinate reads `None` (over-report-safe
        // — a coordinate the warm index over-reports is dropped here).
        #[cfg(test)]
        self.counters
            .cell_point_reads
            .fetch_add(1, Ordering::Relaxed);
        let Some(raw) = self
            .read_raw(collection, cell)
            .await
            .map_err(ResolveCellError::Store)?
        else {
            return Ok(None);
        };
        match decode::try_decode_cell(raw).map_err(ResolveCellError::Store)? {
            Cell::Provisional(provisional) => Ok(Some(provisional)),
            Cell::Resolved(_) => Ok(None),
        }
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.write_provisional_raw(collection, writes)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        self.write_resolved_raw(collection, cells)
            .await
            .map_err(ResolveCellError::Store)
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.mark_resolved_raw(collection, cells)
            .await
            .map_err(ResolveCellError::Store)
    }
}

/// The four partition-key column values of a collection's Cassandra partition.
#[derive(Clone, Copy)]
struct Pk<'a> {
    segment_id: &'a crate::SegmentId,
    key: &'a str,
    state_type: StateType,
    name: &'a str,
}

impl<'a> Pk<'a> {
    fn of(id: &'a CollectionId) -> Self {
        Self {
            segment_id: &id.state_key().segment_id,
            key: id.state_key().key.as_ref(),
            state_type: id.state_type(),
            name: id.name().as_str(),
        }
    }

    /// An owned snapshot, so a `try_stream!` can hold the partition key across
    /// its `.await`s without borrowing the collection id.
    fn owned(&self) -> OwnedPk {
        OwnedPk {
            segment_id: *self.segment_id,
            key: self.key.to_owned(),
            state_type: self.state_type,
            name: self.name.to_owned(),
        }
    }
}

/// Owned partition key, for streamed reads.
struct OwnedPk {
    segment_id: crate::SegmentId,
    key: String,
    state_type: StateType,
    name: String,
}

/// The cell column values bound by the stage / resolved-write paths.
///
/// `encoding` and `version` are shared by `data` and `prev_data` and present
/// iff **either** blob is present — a clear-over-present stage carries a null
/// `data` with a non-null `prev_data` and still needs an encoding to decode it.
struct CellBlobs {
    data: Option<Bytes>,
    prev_data: Option<Bytes>,
    encoding: Option<Encoding>,
    version: Option<i32>,
}

/// The key + clustering columns addressing one cell in its partition: the four
/// partition-key columns and the cell's `section`/`coordinate`. `kind` is
/// **not** carried — each [`CellBatchRow`] variant binds its own constant
/// `kind` (`Cell` vs `Index`), so one address serves both a cell row and its
/// index-marker row.
#[derive(Clone, Copy)]
struct CellAddr<'a> {
    pk: Pk<'a>,
    section: i8,
    coordinate: &'a [u8],
}

impl<'a> CellAddr<'a> {
    fn new(pk: Pk<'a>, cell: &'a CellKey) -> Self {
        Self {
            pk,
            section: i8::from(cell.section),
            coordinate: cell.coordinate.as_bytes(),
        }
    }
}

/// One durable row bound into a same-partition `UNLOGGED BATCH`: the prepared
/// statement it targets and a row-shape struct that binds exactly that
/// statement's columns. There is one variant per distinct cell statement
/// because [`scylla::batch::Batch`] binds its statement list 1:1 with the value
/// list, so a row must serialize precisely the columns of the statement
/// [`BatchRow::statement`] returns. `PromoteCell`/`IndexDelete` share the
/// key-only [`KeyRow`] shape but stay distinct variants because they select
/// different statements and bind a different constant `kind`.
enum CellBatchRow<'a> {
    /// Stage a provisional cell (`kind=Cell`): the full `data | prev_data |
    /// event` shape plus shared `encoding`/`version`.
    StageCell {
        statement: &'a PreparedStatement,
        row: StageRow<'a>,
    },
    /// Promote a provisional cell (`kind=Cell`): nulls `prev_data`/`event`,
    /// keeping `data` and its TTL. Key columns only.
    PromoteCell {
        statement: &'a PreparedStatement,
        row: KeyRow<'a>,
    },
    /// Write a resolved value (`kind=Cell`): committed `data` +
    /// encoding/version, nulling `prev_data`/`event`.
    RollbackCell {
        statement: &'a PreparedStatement,
        row: ResolvedRow<'a>,
    },
    /// Upsert a bare provisional-coordinate marker (`kind=Index`) at the cell's
    /// TTL, so the marker expires with it.
    IndexUpsert {
        statement: &'a PreparedStatement,
        row: IndexUpsertRow<'a>,
    },
    /// Delete a provisional-coordinate marker (`kind=Index`). Key columns only.
    IndexDelete {
        statement: &'a PreparedStatement,
        row: KeyRow<'a>,
    },
}

/// The `write_provisional[_no_ttl]` bind shape. `ttl` selects the with-/no-TTL
/// statement **and** the bound column count — kept consistent with the carried
/// statement at the single construction site.
struct StageRow<'a> {
    ttl: Option<i32>,
    data: Option<&'a [u8]>,
    prev_data: Option<&'a [u8]>,
    encoding: Option<Encoding>,
    version: Option<i32>,
    event: EventRef,
    addr: CellAddr<'a>,
}

/// The `write_resolved[_no_ttl]` bind shape (committed `data` +
/// encoding/version; `prev_data`/`event` nulled by the statement).
struct ResolvedRow<'a> {
    ttl: Option<i32>,
    data: Option<&'a [u8]>,
    encoding: Option<Encoding>,
    version: Option<i32>,
    addr: CellAddr<'a>,
}

/// The `index_insert[_no_ttl]` bind shape: key columns then, with TTL, the
/// trailing `USING TTL ?` value.
struct IndexUpsertRow<'a> {
    ttl: Option<i32>,
    addr: CellAddr<'a>,
}

/// The key-only bind shape shared by `mark_resolved` and `index_delete`: the
/// four PK columns, the constant `kind`, and the cell's `section`/`coordinate`.
struct KeyRow<'a> {
    kind: CellKind,
    addr: CellAddr<'a>,
}

impl BatchRow for CellBatchRow<'_> {
    fn statement(&self) -> &PreparedStatement {
        match self {
            CellBatchRow::StageCell { statement, .. }
            | CellBatchRow::PromoteCell { statement, .. }
            | CellBatchRow::RollbackCell { statement, .. }
            | CellBatchRow::IndexUpsert { statement, .. }
            | CellBatchRow::IndexDelete { statement, .. } => statement,
        }
    }
}

impl SerializeRow for CellBatchRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        match self {
            CellBatchRow::StageCell { row, .. } => row.serialize(ctx, writer),
            CellBatchRow::RollbackCell { row, .. } => row.serialize(ctx, writer),
            CellBatchRow::IndexUpsert { row, .. } => row.serialize(ctx, writer),
            // Promote and index-delete share the key-only `KeyRow` shape (they
            // differ only in statement + constant `kind`, both carried above).
            CellBatchRow::PromoteCell { row, .. } | CellBatchRow::IndexDelete { row, .. } => {
                row.serialize(ctx, writer)
            }
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for StageRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // The `ttl` arms differ only by the leading `USING TTL ?` column the
        // no-TTL statement omits; `kind` leads the clustering key.
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.prev_data,
                self.encoding,
                self.version,
                self.event,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for ResolvedRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        match self.ttl {
            Some(ttl) => (
                ttl,
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
            None => (
                self.data,
                self.encoding,
                self.version,
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Cell,
                a.section,
                a.coordinate,
            )
                .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for IndexUpsertRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        // `INSERT … (…, kind, section, coordinate) VALUES (…) USING TTL ?` binds
        // the TTL **after** the value list.
        match self.ttl {
            Some(ttl) => (
                a.pk.segment_id,
                a.pk.key,
                a.pk.state_type,
                a.pk.name,
                CellKind::Index,
                a.section,
                a.coordinate,
                ttl,
            )
                .serialize(ctx, writer),
            None => KeyRow {
                kind: CellKind::Index,
                addr: self.addr,
            }
            .serialize(ctx, writer),
        }
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SerializeRow for KeyRow<'_> {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let a = &self.addr;
        (
            a.pk.segment_id,
            a.pk.key,
            a.pk.state_type,
            a.pk.name,
            self.kind,
            a.section,
            a.coordinate,
        )
            .serialize(ctx, writer)
    }

    fn is_empty(&self) -> bool {
        false
    }
}

/// Maps a raw Cassandra error into the resolving store error.
fn into_store_err<O>(error: CassandraStoreError) -> CellStoreError<O::Error>
where
    O: CommitOracle,
{
    ResolveCellError::Store(CassandraCellStoreError::from(error))
}

/// Whether `key` has walked past the in-code `end` bound for the scan
/// direction. `Unbounded` never stops; an `Excluded` bound also stops *on* the
/// endpoint (the exclusive variant for coverage gap fall-through).
fn past_end(dir: Direction, key: &CellKey, end: Bound<&[u8]>) -> bool {
    let coordinate = key.coordinate.as_bytes();
    match (dir, end) {
        (_, Bound::Unbounded) => false,
        (Direction::Forward, Bound::Included(end)) => coordinate > end,
        (Direction::Forward, Bound::Excluded(end)) => coordinate >= end,
        (Direction::Backward, Bound::Included(end)) => coordinate < end,
        (Direction::Backward, Bound::Excluded(end)) => coordinate <= end,
    }
}

/// Encodes a cell's `data`/`prev` payloads into their bound columns, computing
/// the shared encoding/version flags off whether **either** blob is present.
fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraCellStoreError> {
    let data = data
        .map(|p| encode_payload(p, VALUE_ENCODING))
        .transpose()?;
    let prev_data = prev
        .map(|p| encode_payload(p, VALUE_ENCODING))
        .transpose()?;
    let any = data.is_some() || prev_data.is_some();
    Ok(CellBlobs {
        data,
        prev_data,
        encoding: any.then_some(VALUE_ENCODING),
        version: any.then_some(INITIAL_VERSION),
    })
}

/// The batch-packing weight of a cell row: its blob bytes plus the fixed
/// [`PER_STATEMENT_OVERHEAD`]. Over-counts rather than under-counts, so a
/// packed batch never exceeds the byte budget it was sized against.
fn blob_weight(blob: &CellBlobs) -> u64 {
    let blob_bytes = blob.data.as_ref().map_or(0_u64, |b| b.len() as u64)
        + blob.prev_data.as_ref().map_or(0_u64, |b| b.len() as u64);
    PER_STATEMENT_OVERHEAD + blob_bytes
}

/// Converts a per-write TTL to the `i32` the driver binds to `USING TTL ?`.
/// The input is pre-validated against Cassandra's ceiling at registration, so
/// the saturating conversion is only a defensive floor.
fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}

/// Converts a `TTL(data)` read into the cache-fill remaining duration. A NULL
/// (`None`) means the cell has no TTL — it never expires. A present value is
/// the whole remaining seconds (a FLOOR), so a fjall entry stamped
/// `now + remaining` never outlives the durable row — including `0`, which
/// means *sub-second remaining* and must stamp an (almost) immediate expiry,
/// never "never" (a negative is treated the same, defensively). Collapsing `0`
/// into `None` would let the fjall entry outlive a durable row that dies within
/// the second.
fn ttl_seconds_to_duration(ttl: Option<i32>) -> Option<CompactDuration> {
    ttl.map(|s| CompactDuration::new(u32::try_from(s).unwrap_or(0)))
}

cassandra_queries! {
    /// Container for the prepared CQL statements used by [`CassandraStore`].
    ///
    /// Every statement binds the leading clustering `kind` as a constant
    /// (`CellKind::Cell` for the cell statements, `CellKind::Index` for the
    /// three index statements) — a clustering-prefix column cannot be skipped.
    /// Each cell mutation is one `UPDATE`/`INSERT`/`DELETE` of one row; a
    /// multi-cell collection write binds these once per cell into one
    /// same-partition `UNLOGGED BATCH` (via `execute_unlogged_batches`), so all
    /// its cells share one write timestamp and TTL anchor. TTL/no-TTL pairs
    /// exist because Cassandra cannot bind `NULL` to `USING TTL ?`. The scans
    /// are single-section clustering ranges within the `kind=Cell` slice: the
    /// `ORDER BY` direction cannot be bound (forward/backward), and the
    /// **start-side comparator** cannot be bound either, so each direction
    /// carries three start variants — inclusive (`>=`/`<=`), exclusive (`>`/`<`,
    /// for coverage gap fall-through), and unbounded (no coordinate clause). The
    /// end bound is enforced in code (`past_end`), so it needs no statement
    /// variant. The `index_*` statements maintain and read the `kind=Index`
    /// marker range that bounds recovery. None use `ALLOW FILTERING`.
    pub struct CellQueries {
        /// Reads one cell's columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads one cell's columns plus the row's remaining `data` TTL, for the
        /// cache-fill point read. `TTL(data)` is a read function (no schema
        /// change); it returns NULL when `data` is NULL or the row has no TTL.
        read_cell_ttl: (
            "SELECT data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an exclusive `coordinate` anchor.
        scan_forward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate > ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan over the whole section (unbounded start).
        scan_forward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an exclusive `coordinate` anchor.
        scan_backward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate < ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan over the whole section (unbounded
        /// start).
        scan_backward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Inserts a bare `kind=Index` provisional-coordinate marker with TTL,
        /// anchored to the staged cell's TTL so the marker expires with it.
        index_insert: (
            "INSERT INTO $keyspace.{} \
             (segment_id, key, state_type, name, kind, section, coordinate) \
             VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Inserts a bare `kind=Index` marker without TTL.
        index_insert_no_ttl: (
            "INSERT INTO $keyspace.{} \
             (segment_id, key, state_type, name, kind, section, coordinate) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_KEYED_STATE_CELL
        ),

        /// Deletes a `kind=Index` marker (on cell resolution — promote or
        /// rollback). A delete of an absent marker is a harmless no-op.
        index_delete: (
            "DELETE FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ? AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads the whole `kind=Index` marker range for recovery — the
        /// provisional coordinate list, bounded by #provisional and disjoint
        /// from the `kind=Cell` slice. Recovery point-reads each coordinate's
        /// cell via `read_cell`.
        index_scan: (
            "SELECT section, coordinate \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND kind = ?",
            TABLE_KEYED_STATE_CELL
        ),
    }
}
