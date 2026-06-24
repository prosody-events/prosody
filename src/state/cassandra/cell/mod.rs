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
use decode::{KeyedCellRow, RawCellRow};
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
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

    async fn execute_unpaged(
        &self,
        statement: &PreparedStatement,
        params: impl SerializeRow,
    ) -> Result<(), CassandraCellStoreError> {
        self.session
            .execute_unpaged_discard(statement, params)
            .await?;
        Ok(())
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

    async fn write_provisional_raw(
        &self,
        collection: &CollectionRef,
        writes: &[(CellKey, ProvisionalWrite)],
    ) -> Result<(), CassandraCellStoreError> {
        // Value is single-cell, so this slice is size-1: one `UPDATE`. The
        // multi-cell same-partition `UNLOGGED BATCH` lands with Map/Deque.
        let pk = Pk::of(collection.id());
        for (cell, write) in writes {
            let CellBlobs {
                data,
                prev_data,
                encoding,
                version,
            } = encode_cell_blobs(write.data(), write.prev())?;
            let data = data.as_ref().map(Bytes::as_ref);
            let prev_data = prev_data.as_ref().map(Bytes::as_ref);
            let event = write.event();
            let section = i8::from(cell.section);
            let coordinate = cell.coordinate.as_bytes();
            self.session
                .execute_with_optional_ttl(
                    collection.ttl().map(ttl_to_i32),
                    &self.queries.write_provisional,
                    &self.queries.write_provisional_no_ttl,
                    |ttl| {
                        (
                            ttl,
                            data,
                            prev_data,
                            encoding,
                            version,
                            event,
                            pk.segment_id,
                            pk.key,
                            pk.state_type,
                            pk.name,
                            section,
                            coordinate,
                        )
                    },
                    || {
                        (
                            data,
                            prev_data,
                            encoding,
                            version,
                            event,
                            pk.segment_id,
                            pk.key,
                            pk.state_type,
                            pk.name,
                            section,
                            coordinate,
                        )
                    },
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
        for (cell, data) in cells {
            let CellBlobs {
                data,
                encoding,
                version,
                ..
            } = encode_cell_blobs(data.as_ref(), None)?;
            let data = data.as_ref().map(Bytes::as_ref);
            let section = i8::from(cell.section);
            let coordinate = cell.coordinate.as_bytes();
            self.session
                .execute_with_optional_ttl(
                    collection.ttl().map(ttl_to_i32),
                    &self.queries.write_resolved,
                    &self.queries.write_resolved_no_ttl,
                    |ttl| {
                        (
                            ttl,
                            data,
                            encoding,
                            version,
                            pk.segment_id,
                            pk.key,
                            pk.state_type,
                            pk.name,
                            section,
                            coordinate,
                        )
                    },
                    || {
                        (
                            data,
                            encoding,
                            version,
                            pk.segment_id,
                            pk.key,
                            pk.state_type,
                            pk.name,
                            section,
                            coordinate,
                        )
                    },
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
        for cell in cells {
            self.execute_unpaged(
                &self.queries.mark_resolved,
                (
                    pk.segment_id,
                    pk.key,
                    pk.state_type,
                    pk.name,
                    i8::from(cell.section),
                    cell.coordinate.as_bytes(),
                ),
            )
            .await?;
        }
        Ok(())
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

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        let pk = Pk::of(collection).owned();
        let section = i8::from(scan.section);
        let start = scan.start.as_bytes().to_vec();
        let end = scan.end.map(|c| c.as_bytes().to_vec());
        let dir = scan.dir;
        let limit = scan.limit;
        let statement = match dir {
            Direction::Forward => self.queries.scan_forward.clone(),
            Direction::Backward => self.queries.scan_backward.clone(),
        };
        let collection_ref = self.resolver.collection_ref(collection);
        try_stream! {
            let stream = self
                .cql()
                .execute_iter(
                    statement,
                    (pk.segment_id, pk.key.as_str(), pk.state_type, pk.name.as_str(), section, start),
                )
                .await
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?
                .rows_stream::<KeyedCellRow>()
                .map_err(CassandraStoreError::from)
                .map_err(into_store_err::<O>)?;
            pin_mut!(stream);

            let mut yielded = 0usize;
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
                let (key, raw) =
                    decode::try_decode_keyed_cell(row).map_err(ResolveCellError::Store)?;
                if past_end(dir, &key, end.as_deref()) {
                    break;
                }
                let committed =
                    resolve_read(self, self.resolver.oracle(), &collection_ref, &key, own, raw)
                        .await
                        .map_err(flatten_resolve)?;
                if let Some(bytes) = committed.into_inner() {
                    yield (key, bytes);
                    yielded += 1;
                }
            }
        }
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

/// Whether `key` has walked past `end` for the scan direction (the in-code end
/// bound; `None` means unbounded).
fn past_end(dir: Direction, key: &CellKey, end: Option<&[u8]>) -> bool {
    match (dir, end) {
        (_, None) => false,
        (Direction::Forward, Some(end)) => key.coordinate.as_bytes() > end,
        (Direction::Backward, Some(end)) => key.coordinate.as_bytes() < end,
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

/// Converts a per-write TTL to the `i32` the driver binds to `USING TTL ?`.
/// The input is pre-validated against Cassandra's ceiling at registration, so
/// the saturating conversion is only a defensive floor.
fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}

cassandra_queries! {
    /// Container for the prepared CQL statements used by [`CassandraStore`].
    ///
    /// Each point mutation is a single `UPDATE` — Cassandra's row atomicity
    /// covers a multi-column write of one row, so no batch is needed. TTL/no-TTL
    /// pairs exist because Cassandra cannot bind `NULL` to `USING TTL ?`. The
    /// two scans are single-section clustering ranges (forward/backward — the
    /// `ORDER BY` direction cannot be bound) plus a whole-partition recovery
    /// scan; none use `ALLOW FILTERING`.
    pub struct CellQueries {
        /// Reads one cell's columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Forward single-section scan from an inclusive `coordinate` anchor.
        scan_forward: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate >= ? \
             ORDER BY coordinate ASC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Backward single-section scan from an inclusive `coordinate` anchor.
        scan_backward: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate <= ? \
             ORDER BY coordinate DESC",
            TABLE_KEYED_STATE_CELL
        ),

        /// Whole-partition (all sections) provisional scan for recovery. Yields
        /// every clustering row; resolved rows are filtered in code.
        scan_partition: (
            "SELECT section, coordinate, data, prev_data, encoding, version, event \
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
