//! Cassandra-backed provisional-cell store.
//!
//! [`CassandraCellStore`] implements [`CellStore<ValueKind>`] over the
//! `keyed_state_cell` table provisioned by migration
//! `20260522_create_keyed_state.cql`. It is the provisional-cell
//! replacement for the write-ahead-log-era `CassandraValueStore`: every durable
//! mutation writes one self-consistent column shape in a single statement, so
//! the applied-triple desync class the WAL design fought is unwritable here.
//!
//! # Cell column shape
//!
//! Each value row is one cell over the columns `data | prev_data |
//! encoding | version | event`. The three mutators write
//! exactly one shape each:
//!
//! * [`write_provisional`](CellStore::write_provisional) — *stage*: `data`,
//!   `prev_data`, `event`, and the shared `encoding`/`version` in one `UPDATE`.
//!   The encoding/version flags key on **either** blob being present (a
//!   clear-over-present stages `data = null` with a non-null `prev_data`, which
//!   still needs an encoding).
//! * [`write_resolved`](CellStore::write_resolved) — writes a committed value
//!   with `prev_data`/`event` nulled in one `UPDATE` (the `ReadUncommitted`
//!   direct write, the mid-handler flush, and rollback resolution).
//! * [`mark_resolved`](CellStore::mark_resolved) — *promote*: nulls `prev_data`
//!   and `event` only, keeping `data` and its TTL. O(1) bytes.
//!
//! # Promote-of-clear residue
//!
//! Promoting a staged *clear* (`data = null`, `prev_data = Some`) nulls
//! `prev_data`/`event` but leaves the row's `encoding`/
//! `version` populated with `data` still null. That shape —
//! encoding/version present, both blobs null — is a legitimate
//! `Resolved(Committed(None))`, **not** corruption. The decoder validates
//! encoding/version per-blob (a blob present without an encoding is corrupt),
//! never as a row-level "encoding implies a blob" rule, precisely so this
//! residue decodes cleanly.
//!
//! # Concurrency
//!
//! The framework guarantees one handler per key system-wide (Kafka partition
//! ownership + in-process per-key serialization), so this store never needs
//! LWTs or distributed locks.

mod decode;

#[cfg(test)]
mod tests;

use crate::SegmentId;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_KEYED_STATE_CELL};
use crate::cassandra_queries;
use crate::state::StateType;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::Section;
use crate::state::encoding::{Encoding, encode_payload};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CollectionRef};
use crate::timers::duration::CompactDuration;
use async_stream::try_stream;
use bytes::Bytes;
use decode::RawCellRow;
use futures::Stream;
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

pub use crate::state::cassandra::error::CassandraValueStoreError;
pub use decode::CellCorruptReason;

/// Payload encoding for cell blobs written by this build.
const VALUE_ENCODING: Encoding = Encoding::RawZstdV1;

/// The only value-cell `version` stamp this build writes or accepts.
///
/// Every authoritative value cell stamps the version its bytes were written
/// under; this build writes version 1 and rejects any other at decode
/// ([`decode::validate_version`]). Per-key identity migration is future work —
/// the stamp is the dormant hook it would build on.
pub const INITIAL_VERSION: i32 = 1;

/// Cassandra-backed provisional-cell store for the Value kind.
#[derive(Clone, Debug)]
pub struct CassandraCellStore {
    store: CassandraStore,
    queries: Arc<CellQueries>,
}

impl CassandraCellStore {
    /// Creates a Cassandra cell store over an existing [`CassandraStore`]
    /// session and a prepared [`CellQueries`] set.
    #[must_use]
    pub fn new(store: CassandraStore, queries: Arc<CellQueries>) -> Self {
        Self { store, queries }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    async fn execute_unpaged(
        &self,
        statement: &PreparedStatement,
        params: impl SerializeRow,
    ) -> Result<(), CassandraValueStoreError> {
        self.store
            .execute_unpaged_discard(statement, params)
            .await?;
        Ok(())
    }

    async fn read_raw(
        &self,
        id: &CollectionId<ValueKind>,
    ) -> Result<Option<RawCellRow>, CassandraValueStoreError> {
        let (segment_id, key, state_type, name, section, coordinate) = primary_components(id);
        let row = self
            .session()
            .execute_unpaged(
                &self.queries.read_cell,
                (segment_id, key, state_type, name, section, coordinate),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<RawCellRow>()
            .map_err(CassandraStoreError::from)?;
        Ok(row)
    }
}

impl CellStore<ValueKind> for CassandraCellStore {
    type Error = CassandraValueStoreError;

    async fn read_cell<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        (): &'a (),
    ) -> Result<Cell, Self::Error> {
        match self.read_raw(collection).await? {
            Some(row) => decode::try_decode_cell(row),
            None => Ok(Cell::Resolved(Committed::new(None))),
        }
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> impl Stream<Item = Result<((), ProvisionalCell), Self::Error>> + Send + 'a {
        // Value's collection is a single cell at addr `()`: read it and yield
        // it only when it is provisional. Map will stream its entry rows.
        try_stream! {
            if let Cell::Provisional(cell) = self.read_cell(collection, &()).await? {
                yield ((), cell);
            }
        }
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        writes: &'a [((), ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        // Value is single-cell, so this slice is size-1: one `UPDATE`, exactly
        // as before. A multi-cell kind would group the per-row statements into
        // one same-partition `UNLOGGED BATCH`; that path lands with that kind.
        let (segment_id, key, state_type, name, section, coordinate) =
            primary_components(collection.id());
        for ((), write) in writes {
            let CellBlobs {
                data,
                prev_data,
                encoding,
                version,
            } = encode_cell_blobs(write.data(), write.prev())?;
            let data = data.as_ref().map(Bytes::as_ref);
            let prev_data = prev_data.as_ref().map(Bytes::as_ref);
            let event = write.event();
            self.store
                .execute_with_optional_ttl(
                    collection.ttl().map(ttl_to_i32),
                    &self.queries.write_provisional,
                    &self.queries.write_provisional_no_ttl,
                    |ttl| {
                        (
                            ttl, data, prev_data, encoding, version, event, segment_id, key,
                            state_type, name, section, coordinate,
                        )
                    },
                    || {
                        (
                            data, prev_data, encoding, version, event, segment_id, key, state_type,
                            name, section, coordinate,
                        )
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        cells: &'a [((), Option<Bytes>)],
    ) -> Result<(), Self::Error> {
        let (segment_id, key, state_type, name, section, coordinate) =
            primary_components(collection.id());
        for ((), data) in cells {
            let CellBlobs {
                data,
                encoding,
                version,
                ..
            } = encode_cell_blobs(data.as_ref(), None)?;
            let data = data.as_ref().map(Bytes::as_ref);
            self.store
                .execute_with_optional_ttl(
                    collection.ttl().map(ttl_to_i32),
                    &self.queries.write_resolved,
                    &self.queries.write_resolved_no_ttl,
                    |ttl| {
                        (
                            ttl, data, encoding, version, segment_id, key, state_type, name,
                            section, coordinate,
                        )
                    },
                    || {
                        (
                            data, encoding, version, segment_id, key, state_type, name, section,
                            coordinate,
                        )
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        addrs: &'a [()],
    ) -> Result<(), Self::Error> {
        let (segment_id, key, state_type, name, section, coordinate) =
            primary_components(collection.id());
        for () in addrs {
            self.execute_unpaged(
                &self.queries.mark_resolved,
                (segment_id, key, state_type, name, section, coordinate),
            )
            .await?;
        }
        Ok(())
    }
}

/// The cell column values bound by [`CellStore::write_provisional`] and
/// [`CellStore::write_resolved`].
///
/// `encoding` and `version` are shared by `data` and
/// `prev_data` and present iff **either** blob is present — a
/// clear-over-present stage carries a null `data` with a non-null `prev_data`
/// and still needs an encoding to decode the latter.
struct CellBlobs {
    data: Option<Bytes>,
    prev_data: Option<Bytes>,
    encoding: Option<Encoding>,
    version: Option<i32>,
}

/// Encodes a cell's `data`/`prev` payloads into their bound columns, computing
/// the shared encoding/version flags off whether **either** blob is present.
fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraValueStoreError> {
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

/// The fixed section of Value's single cell.
///
/// **Phase-1 shim.** Value is a one-cell collection, so its cell lives at one
/// constant clustering key `(VALUE_SECTION, empty coordinate)`. The
/// discriminant is arbitrary while the section is opaque (no wire freeze until
/// the collection layer); Phase 3/4 introduces a `ValueNs` section enum and
/// removes this shim.
const VALUE_SECTION: Section = Section::new(1);

/// The partition-key columns plus the fixed Value cell's clustering key.
///
/// **Phase-1 shim.** Appends Value's constant cell address
/// `(VALUE_SECTION, empty coordinate)` to every cell bind. Phase 3 threads the
/// real [`CellKey`] through the store API and removes the shim.
///
/// [`CellKey`]: crate::state::cell_key::CellKey
fn primary_components(
    id: &CollectionId<ValueKind>,
) -> (&SegmentId, &str, StateType, &str, i8, &'static [u8]) {
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = id.state_type();
    let name = id.name().as_str();
    (
        segment_id,
        key,
        state_type,
        name,
        i8::from(VALUE_SECTION),
        &[],
    )
}

/// Converts a per-write TTL to the `i32` the driver binds to `USING TTL ?`.
/// The input is pre-validated against Cassandra's ceiling at registration, so
/// the saturating conversion is only a defensive floor.
fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}

cassandra_queries! {
    /// Container for the prepared CQL statements used by [`CassandraCellStore`].
    ///
    /// Every cell mutation is a single `UPDATE` — Cassandra's row atomicity
    /// already covers a multi-column write of one row, so no batch is needed.
    /// TTL/no-TTL pairs exist because Cassandra cannot bind `NULL` to
    /// `USING TTL ?`.
    pub struct CellQueries {
        /// Reads the cell columns (Resolved/Provisional/Corrupt shapes).
        read_cell: (
            "SELECT data, prev_data, encoding, version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND section = ? AND coordinate = ?",
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
