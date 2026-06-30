//! Cassandra-backed uniform cell store.
//!
//! [`CassandraStore`] implements the untyped [`CellStore`] over the
//! `keyed_state_cell` table provisioned by migration
//! `20260522_create_keyed_state.cql`. Every durable mutation writes one
//! self-consistent column shape in a single statement, so the applied-triple
//! desync class the write-ahead-log design fought is unwritable here.
//!
//! It is the **bottom** store: it owns the commit oracle (via the composed
//! [`Resolver`]) and oracle-resolves any in-flight provisional cell inside
//! `get`/`scan_cells` before yielding, so the layers above it
//! ([`Cached`](crate::state::cached::Cached),
//! [`Overlay`](crate::state::overlay::Overlay)) are oracle-free.
//!
//! # Cell column shape
//!
//! Each cell row is one cell over the columns `data | prev_data | encoding |
//! version | event`, addressed by the `(section, coordinate)` clustering key.
//! The three mutators write exactly one shape each:
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
use crate::cassandra::{MAX_BATCH_BYTES, MAX_BATCH_STATEMENTS, PER_STATEMENT_OVERHEAD, Weighed};
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
use std::ops::Bound;
use std::sync::Arc;
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

/// Cassandra-backed uniform cell store.
#[derive(Clone, Debug)]
pub struct CassandraStore<O> {
    session: CassandraSession,
    queries: Arc<CellQueries>,
    resolver: Resolver<O>,
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
        }
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
        // so the whole batch is one `rows` allocation with no per-cell tuple
        // copy. Both Vecs outlive the awaited batch.
        let mut blobs = Vec::with_capacity(writes.len());
        for (_, write) in writes {
            blobs.push(encode_cell_blobs(write.data(), write.prev())?);
        }

        // The collection TTL is uniform, so the with-TTL vs no-TTL choice — and
        // hence the bound param shape — is made once for the whole batch, never
        // per cell.
        if let Some(ttl) = collection.ttl().map(ttl_to_i32) {
            let mut rows = Vec::with_capacity(writes.len());
            for (blob, (cell, write)) in blobs.iter().zip(writes) {
                rows.push(Weighed::new(
                    blob_weight(blob),
                    (
                        ttl,
                        blob.data.as_deref(),
                        blob.prev_data.as_deref(),
                        blob.encoding,
                        blob.version,
                        write.event(),
                        pk.segment_id,
                        pk.key,
                        pk.state_type,
                        pk.name,
                        i8::from(cell.section),
                        cell.coordinate.as_bytes(),
                    ),
                ));
            }
            self.session
                .execute_unlogged_batches(
                    &self.queries.write_provisional,
                    &rows,
                    MAX_BATCH_BYTES,
                    MAX_BATCH_STATEMENTS,
                )
                .await?;
        } else {
            let mut rows = Vec::with_capacity(writes.len());
            for (blob, (cell, write)) in blobs.iter().zip(writes) {
                rows.push(Weighed::new(
                    blob_weight(blob),
                    (
                        blob.data.as_deref(),
                        blob.prev_data.as_deref(),
                        blob.encoding,
                        blob.version,
                        write.event(),
                        pk.segment_id,
                        pk.key,
                        pk.state_type,
                        pk.name,
                        i8::from(cell.section),
                        cell.coordinate.as_bytes(),
                    ),
                ));
            }
            self.session
                .execute_unlogged_batches(
                    &self.queries.write_provisional_no_ttl,
                    &rows,
                    MAX_BATCH_BYTES,
                    MAX_BATCH_STATEMENTS,
                )
                .await?;
        }
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

        if let Some(ttl) = collection.ttl().map(ttl_to_i32) {
            let mut rows = Vec::with_capacity(cells.len());
            for (blob, (cell, _)) in blobs.iter().zip(cells) {
                rows.push(Weighed::new(
                    blob_weight(blob),
                    (
                        ttl,
                        blob.data.as_deref(),
                        blob.encoding,
                        blob.version,
                        pk.segment_id,
                        pk.key,
                        pk.state_type,
                        pk.name,
                        i8::from(cell.section),
                        cell.coordinate.as_bytes(),
                    ),
                ));
            }
            self.session
                .execute_unlogged_batches(
                    &self.queries.write_resolved,
                    &rows,
                    MAX_BATCH_BYTES,
                    MAX_BATCH_STATEMENTS,
                )
                .await?;
        } else {
            let mut rows = Vec::with_capacity(cells.len());
            for (blob, (cell, _)) in blobs.iter().zip(cells) {
                rows.push(Weighed::new(
                    blob_weight(blob),
                    (
                        blob.data.as_deref(),
                        blob.encoding,
                        blob.version,
                        pk.segment_id,
                        pk.key,
                        pk.state_type,
                        pk.name,
                        i8::from(cell.section),
                        cell.coordinate.as_bytes(),
                    ),
                ));
            }
            self.session
                .execute_unlogged_batches(
                    &self.queries.write_resolved_no_ttl,
                    &rows,
                    MAX_BATCH_BYTES,
                    MAX_BATCH_STATEMENTS,
                )
                .await?;
        }
        Ok(())
    }

    async fn mark_resolved_raw(
        &self,
        collection: &CollectionRef,
        cells: &[CellKey],
    ) -> Result<(), CassandraCellStoreError> {
        let pk = Pk::of(collection.id());
        // Promotes carry no blob — only the key columns — so every row weighs the
        // fixed per-statement overhead and the byte budget never bites; the count
        // budget alone splits an enormous promote set.
        let mut rows = Vec::with_capacity(cells.len());
        for cell in cells {
            rows.push(Weighed::new(
                PER_STATEMENT_OVERHEAD,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    i8::from(cell.section),
                    cell.coordinate.as_bytes(),
                ),
            ));
        }
        self.session
            .execute_unlogged_batches(
                &self.queries.mark_resolved,
                &rows,
                MAX_BATCH_BYTES,
                MAX_BATCH_STATEMENTS,
            )
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
                        (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), section, coord.as_slice()),
                    )
                    .await,
                None => self
                    .cql()
                    .execute_iter(
                        statement,
                        (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), section),
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
        let pk = Pk::of(collection).owned();
        try_stream! {
            let stream = self
                .cql()
                .execute_iter(
                    self.queries.scan_partition.clone(),
                    (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str()),
                )
                .await
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?
                .rows_stream::<KeyedCellRow>()
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?;
            pin_mut!(stream);

            while let Some(row) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?
            {
                let (key, cell) =
                    decode::try_decode_keyed_cell(row).map_err(ResolveCellError::Store)?;
                if let Cell::Provisional(provisional) = cell {
                    yield (key, provisional);
                }
            }
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
    /// Each mutation is one `UPDATE` of one row; a multi-cell collection write
    /// binds the same prepared statement once per cell into one same-partition
    /// `UNLOGGED BATCH` (via `execute_unlogged_batches`), so all its cells share
    /// one write timestamp and TTL anchor. TTL/no-TTL
    /// pairs exist because Cassandra cannot bind `NULL` to `USING TTL ?`. The
    /// scans are single-section clustering ranges: the `ORDER BY` direction
    /// cannot be bound (forward/backward), and the **start-side comparator**
    /// cannot be bound either, so each direction carries three start variants —
    /// inclusive (`>=`/`<=`), exclusive (`>`/`<`, for coverage gap
    /// fall-through), and unbounded (no coordinate clause). The end bound is
    /// enforced in code (`past_end`), so it needs no statement variant. A
    /// whole-partition recovery scan completes the set; none use `ALLOW
    /// FILTERING`.
    pub struct CellQueries {
        /// Reads one cell's columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Reads one cell's columns plus the row's remaining `data` TTL, for the
        /// cache-fill point read. `TTL(data)` is a read function (no schema
        /// change); it returns NULL when `data` is NULL or the row has no TTL.
        read_cell_ttl: (
            "SELECT data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an exclusive `coordinate` anchor.
        scan_forward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate > ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan over the whole section (unbounded start).
        scan_forward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward_incl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an exclusive `coordinate` anchor.
        scan_backward_excl: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate < ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan over the whole section (unbounded
        /// start).
        scan_backward_all: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Whole-partition (all sections) provisional scan for recovery. Yields
        /// every clustering row; resolved rows are filtered in code.
        scan_partition: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event, TTL(data) \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),
    }
}
