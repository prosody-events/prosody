//! Cassandra-backed provisional-cell store.
//!
//! [`CassandraCellStore`] implements [`CellStore<ValueKind>`] over the
//! `keyed_state_cell` table provisioned by migration
//! `20260522_create_keyed_state.cql`, and [`DescriptorIdentityStore`] over the
//! group-global `keyed_state_identity` table. It is the provisional-cell
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
use crate::cassandra::{CassandraStore, TABLE_KEYED_STATE_CELL, TABLE_KEYED_STATE_IDENTITY};
use crate::cassandra_queries;
use crate::state::StateType;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::Namespace;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
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
use scylla::value::{CqlValue, Row};
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
        let (segment_id, key, state_type, name, namespace, order_key) = primary_components(id);
        let row = self
            .session()
            .execute_unpaged(
                &self.queries.read_cell,
                (segment_id, key, state_type, name, namespace, order_key),
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
        let (segment_id, key, state_type, name, namespace, order_key) =
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
                            state_type, name, namespace, order_key,
                        )
                    },
                    || {
                        (
                            data, prev_data, encoding, version, event, segment_id, key, state_type,
                            name, namespace, order_key,
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
        let (segment_id, key, state_type, name, namespace, order_key) =
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
                            namespace, order_key,
                        )
                    },
                    || {
                        (
                            data, encoding, version, segment_id, key, state_type, name, namespace,
                            order_key,
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
        let (segment_id, key, state_type, name, namespace, order_key) =
            primary_components(collection.id());
        for () in addrs {
            self.execute_unpaged(
                &self.queries.mark_resolved,
                (segment_id, key, state_type, name, namespace, order_key),
            )
            .await?;
        }
        Ok(())
    }
}

impl DescriptorIdentityStore for CassandraCellStore {
    type Error = CassandraValueStoreError;

    async fn read_identity(
        &self,
        group_id: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, Self::Error> {
        let row = self
            .session()
            .execute_unpaged(&self.queries.read_identity, (group_id, state_type, name))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<IdentityColumns>()
            .map_err(CassandraStoreError::from)?;
        Ok(row.map(|cols| cols.into_identity(state_type, name)))
    }

    /// Registers the first-use identity row with `INSERT … IF NOT EXISTS`.
    ///
    /// **This is the one authorized lightweight transaction in keyed state.**
    /// The general LWT ban (Paxos round-trips serialize a partition's writes)
    /// holds for the hot path; first-use registration is off it — once per
    /// collection per group, racing only across group members at first use —
    /// and needs the atomic insert-if-absent that `IF NOT EXISTS` gives. A
    /// failed conditional insert echoes the existing row, so the losing
    /// registrant validates it in the same round-trip (no re-read), exactly
    /// as the migration lock ([`crate::cassandra::migrator`]) parses its LWT.
    async fn register_identity(
        &self,
        group_id: &str,
        row: &DurableDescriptorIdentity,
    ) -> Result<RegisterOutcome, Self::Error> {
        let result = self
            .session()
            .execute_unpaged(
                &self.queries.register_identity,
                (
                    group_id,
                    row.state_type,
                    row.name.as_str(),
                    row.kind,
                    row.resolver_id.as_deref(),
                    row.codec_id.as_str(),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?;
        // Read the `[applied]` flag and any echoed columns by name: the result
        // column order is the driver's, not the schema's, so never positional.
        let specs: Vec<String> = result
            .column_specs()
            .iter()
            .map(|c| c.name().to_owned())
            .collect();
        let lwt = result
            .maybe_first_row::<Row>()
            .map_err(CassandraStoreError::from)?
            .ok_or(CassandraValueStoreError::MalformedLwtResult)?;
        if column_bool(&specs, &lwt, "[applied]")? {
            Ok(RegisterOutcome::Applied)
        } else {
            Ok(RegisterOutcome::Conflict(conflict_identity(
                &specs, &lwt, row,
            )?))
        }
    }
}

/// Decodes the existing identity a failed `INSERT … IF NOT EXISTS` echoes.
///
/// `state_type` and `name` are the key we registered under (`asserted`), so the
/// conflict shares them by construction — only the contested
/// `kind`/`codec_id`/`resolver_id` are read from the echoed columns, by name
/// (the result column order is the driver's, not the schema's).
fn conflict_identity(
    specs: &[String],
    row: &Row,
    asserted: &DurableDescriptorIdentity,
) -> Result<DurableDescriptorIdentity, CassandraValueStoreError> {
    Ok(DurableDescriptorIdentity {
        state_type: asserted.state_type,
        name: asserted.name.clone(),
        kind: column_tinyint(specs, row, "kind")?,
        resolver_id: column_text_opt(specs, row, "resolver_id"),
        codec_id: column_text(specs, row, "codec_id")?,
    })
}

/// The value of `name` in `row`, positioned via `specs` (name-matched), or
/// `None` when the column is absent or NULL.
fn column_value<'a>(specs: &[String], row: &'a Row, name: &str) -> Option<&'a CqlValue> {
    let idx = specs.iter().position(|c| c == name)?;
    row.columns.get(idx)?.as_ref()
}

fn column_bool(specs: &[String], row: &Row, name: &str) -> Result<bool, CassandraValueStoreError> {
    match column_value(specs, row, name) {
        Some(CqlValue::Boolean(b)) => Ok(*b),
        _ => Err(CassandraValueStoreError::MalformedLwtResult),
    }
}

fn column_tinyint(specs: &[String], row: &Row, name: &str) -> Result<i8, CassandraValueStoreError> {
    match column_value(specs, row, name) {
        Some(CqlValue::TinyInt(v)) => Ok(*v),
        _ => Err(CassandraValueStoreError::MalformedLwtResult),
    }
}

fn column_text(
    specs: &[String],
    row: &Row,
    name: &str,
) -> Result<String, CassandraValueStoreError> {
    match column_value(specs, row, name) {
        Some(CqlValue::Text(s)) => Ok(s.clone()),
        _ => Err(CassandraValueStoreError::MalformedLwtResult),
    }
}

fn column_text_opt(specs: &[String], row: &Row, name: &str) -> Option<String> {
    match column_value(specs, row, name) {
        Some(CqlValue::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The non-key identity columns a [`CellQueries::read_identity`] point-read
/// returns, deserialized as raw integers so an unknown future discriminant
/// compares unequal rather than tearing the row down.
#[derive(scylla::DeserializeRow)]
struct IdentityColumns {
    kind: i8,
    resolver_id: Option<String>,
    codec_id: String,
}

impl IdentityColumns {
    fn into_identity(self, state_type: StateType, name: &str) -> DurableDescriptorIdentity {
        DurableDescriptorIdentity {
            state_type: state_type.into(),
            name: name.to_owned(),
            kind: self.kind,
            resolver_id: self.resolver_id,
            codec_id: self.codec_id,
        }
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

/// The partition-key columns plus the fixed Value cell's clustering key.
///
/// **Phase-1 shim.** Value is a one-cell collection at the fixed clustering key
/// `(Namespace::Entries, OrderKey::empty())`, so this appends that constant
/// address to every cell bind. Phase 3 threads the real [`CellKey`] through the
/// store API and removes the shim.
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
        i8::from(Namespace::Entries),
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
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, encoding = ?, version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, encoding = ?, version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ? \
             AND namespace = ? AND order_key = ?",
            TABLE_KEYED_STATE_CELL
        ),

        /// Point-reads the frozen identity row for one
        /// `(group_id, state_type, name)` — the steady-state validation path.
        read_identity: (
            "SELECT kind, resolver_id, codec_id \
             FROM $keyspace.{} WHERE group_id = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_IDENTITY
        ),

        /// Registers a first-use identity row. `IF NOT EXISTS` is the one
        /// authorized keyed-state LWT (see [`DescriptorIdentityStore::register_identity`]):
        /// a failed conditional insert echoes the existing row so the loser
        /// validates without a re-read.
        register_identity: (
            "INSERT INTO $keyspace.{} \
             (group_id, state_type, name, kind, resolver_id, codec_id) \
             VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS",
            TABLE_KEYED_STATE_IDENTITY
        ),
    }
}
