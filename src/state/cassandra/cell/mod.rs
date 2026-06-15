//! Cassandra-backed provisional-cell store.
//!
//! [`CassandraCellStore`] implements [`CellStore<ValueKind>`] over the
//! `keyed_state_value` table provisioned by migration
//! `20260522_create_keyed_state.cql`, and [`DescriptorIdentityStore`] over the
//! `keyed_state_descriptor` table. It is the provisional-cell replacement for
//! the write-ahead-log-era `CassandraValueStore`: every durable mutation
//! writes one self-consistent column shape in a single statement, so the
//! applied-triple desync class the WAL design fought is unwritable here.
//!
//! # Cell column shape
//!
//! Each value row is one cell over the columns `data | prev_data |
//! payload_encoding | identity_version | event`. The three mutators write
//! exactly one shape each:
//!
//! * [`write_provisional`](CellStore::write_provisional) — *stage*: `data`,
//!   `prev_data`, `event`, and the shared `payload_encoding`/`identity_version`
//!   in one `UPDATE`. The encoding/version flags key on **either** blob being
//!   present (a clear-over-present stages `data = null` with a non-null
//!   `prev_data`, which still needs an encoding).
//! * [`write_resolved`](CellStore::write_resolved) — writes a committed value
//!   with `prev_data`/`event` nulled in one `UPDATE` (the `ReadUncommitted`
//!   direct write, the mid-handler flush, and rollback resolution).
//! * [`mark_resolved`](CellStore::mark_resolved) — *promote*: nulls `prev_data`
//!   and `event` only, keeping `data` and its TTL. O(1) bytes.
//!
//! # Promote-of-clear residue
//!
//! Promoting a staged *clear* (`data = null`, `prev_data = Some`) nulls
//! `prev_data`/`event` but leaves the row's `payload_encoding`/
//! `identity_version` populated with `data` still null. That shape —
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

use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_KEYED_STATE_DESCRIPTOR, TABLE_KEYED_STATE_VALUE};
use crate::cassandra_queries;
use crate::state::StateType;
use crate::state::cell::{Cell, Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, INITIAL_IDENTITY_VERSION,
};
use crate::state::encoding::{PayloadEncoding, encode_payload};
use crate::state::store::CellStore;
use crate::state::value::ValueKind;
use crate::state::{CollectionId, CollectionRef};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use async_stream::try_stream;
use bytes::Bytes;
use decode::RawCellRow;
use futures::{Stream, TryStreamExt};
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

pub use crate::state::cassandra::error::CassandraValueStoreError;
pub use decode::CellCorruptReason;

/// Payload encoding for cell blobs written by this build.
const VALUE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::RawZstdV1;

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
        let (segment_id, key, state_type, name) = primary_components(id);
        let row = self
            .session()
            .execute_unpaged(&self.queries.read_cell, (segment_id, key, state_type, name))
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
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        for ((), write) in writes {
            let CellBlobs {
                data,
                prev_data,
                encoding,
                identity_version,
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
                            ttl,
                            data,
                            prev_data,
                            encoding,
                            identity_version,
                            event,
                            segment_id,
                            key,
                            state_type,
                            name,
                        )
                    },
                    || {
                        (
                            data,
                            prev_data,
                            encoding,
                            identity_version,
                            event,
                            segment_id,
                            key,
                            state_type,
                            name,
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
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        for ((), data) in cells {
            let CellBlobs {
                data,
                encoding,
                identity_version,
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
                            ttl,
                            data,
                            encoding,
                            identity_version,
                            segment_id,
                            key,
                            state_type,
                            name,
                        )
                    },
                    || {
                        (
                            data,
                            encoding,
                            identity_version,
                            segment_id,
                            key,
                            state_type,
                            name,
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
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        for () in addrs {
            self.execute_unpaged(
                &self.queries.mark_resolved,
                (segment_id, key, state_type, name),
            )
            .await?;
        }
        Ok(())
    }
}

impl DescriptorIdentityStore for CassandraCellStore {
    type Error = CassandraValueStoreError;

    async fn read_descriptor_identities(
        &self,
        segment_id: SegmentId,
    ) -> Result<Vec<DurableDescriptorIdentity>, Self::Error> {
        let rows = self
            .session()
            .execute_iter(
                self.queries.read_descriptor_identities.clone(),
                (segment_id,),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .rows_stream::<DurableDescriptorIdentity>()
            .map_err(CassandraStoreError::from)?;
        futures::pin_mut!(rows);
        let mut identities = Vec::new();
        while let Some(row) = rows.try_next().await.map_err(CassandraStoreError::from)? {
            identities.push(row);
        }
        Ok(identities)
    }

    /// Inserts the first-use identity rows in one same-partition
    /// `UNLOGGED BATCH` — every row shares the `segment_id` partition key,
    /// so the batch is a single atomic mutation on the replica.
    async fn write_descriptor_identities(
        &self,
        segment_id: SegmentId,
        rows: Vec<DurableDescriptorIdentity>,
    ) -> Result<(), Self::Error> {
        let mut batch = Batch::new(BatchType::Unlogged);
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            batch.append_statement(self.queries.insert_descriptor_identity.clone());
            values.push((
                segment_id,
                row.name,
                row.version,
                row.kind,
                row.cell_kind,
                row.codec_id,
            ));
        }
        self.session()
            .batch(&batch, values)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }
}

/// The cell column values bound by [`CellStore::write_provisional`] and
/// [`CellStore::write_resolved`].
///
/// `payload_encoding` and `identity_version` are shared by `data` and
/// `prev_data` and present iff **either** blob is present — a
/// clear-over-present stage carries a null `data` with a non-null `prev_data`
/// and still needs an encoding to decode the latter.
struct CellBlobs {
    data: Option<Bytes>,
    prev_data: Option<Bytes>,
    encoding: Option<PayloadEncoding>,
    identity_version: Option<i32>,
}

/// Encodes a cell's `data`/`prev` payloads into their bound columns, computing
/// the shared encoding/version flags off whether **either** blob is present.
fn encode_cell_blobs(
    data: Option<&Bytes>,
    prev: Option<&Bytes>,
) -> Result<CellBlobs, CassandraValueStoreError> {
    let data = data
        .map(|p| encode_payload(p, VALUE_PAYLOAD_ENCODING))
        .transpose()?;
    let prev_data = prev
        .map(|p| encode_payload(p, VALUE_PAYLOAD_ENCODING))
        .transpose()?;
    let any = data.is_some() || prev_data.is_some();
    Ok(CellBlobs {
        data,
        prev_data,
        encoding: any.then_some(VALUE_PAYLOAD_ENCODING),
        identity_version: any.then_some(INITIAL_IDENTITY_VERSION),
    })
}

fn primary_components(id: &CollectionId<ValueKind>) -> (&SegmentId, &str, StateType, &str) {
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = id.state_type();
    let name = id.name().as_str();
    (segment_id, key, state_type, name)
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
            "SELECT data, prev_data, payload_encoding, identity_version, event \
             FROM $keyspace.{} \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Stages a provisional cell with TTL (the full `data | prev_data |
        /// event` shape plus the shared encoding/version columns).
        write_provisional: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, prev_data = ?, payload_encoding = ?, identity_version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Stages a provisional cell without TTL.
        write_provisional_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, prev_data = ?, payload_encoding = ?, identity_version = ?, event = ? \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes a resolved cell with TTL: the committed `data` plus its
        /// encoding/version, nulling `prev_data` and `event`.
        write_resolved: (
            "UPDATE $keyspace.{} USING TTL ? \
             SET data = ?, payload_encoding = ?, identity_version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Writes a resolved cell without TTL.
        write_resolved_no_ttl: (
            "UPDATE $keyspace.{} \
             SET data = ?, payload_encoding = ?, identity_version = ?, prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Promotes a provisional cell: nulls `prev_data` and `event`, keeping
        /// `data` (and its original TTL). O(1) bytes; no TTL clause — the
        /// retained `data` keeps the TTL set at its provisional write.
        mark_resolved: (
            "UPDATE $keyspace.{} \
             SET prev_data = null, event = null \
             WHERE segment_id = ? AND key = ? AND state_type = ? AND name = ?",
            TABLE_KEYED_STATE_VALUE
        ),

        /// Reads every frozen descriptor-identity row for one segment
        /// (single-partition query).
        read_descriptor_identities: (
            "SELECT name, version, kind, cell_kind, codec_id \
             FROM $keyspace.{} WHERE segment_id = ?",
            TABLE_KEYED_STATE_DESCRIPTOR
        ),

        /// Inserts one frozen descriptor-identity row. Single owner per
        /// segment, so a plain INSERT — never an LWT.
        insert_descriptor_identity: (
            "INSERT INTO $keyspace.{} \
             (segment_id, name, version, kind, cell_kind, codec_id) \
             VALUES (?, ?, ?, ?, ?, ?)",
            TABLE_KEYED_STATE_DESCRIPTOR
        ),
    }
}
