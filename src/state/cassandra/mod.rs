//! Cassandra-backed durable Value store.
//!
//! `CassandraValueStore` implements [`DurableWalStore<ValueKind>`],
//! [`DirectApplyStore<ValueKind>`], and
//! [`PendingIndexStore`] over the `keyed_state_value` and
//! `keyed_state_pending` tables provisioned by migration
//! `20260522_create_keyed_state.cql`.
//!
//! # Ordering invariants
//!
//! These four invariants are load-bearing for the WAL recovery contract
//! (`docs/keyed-state/design-summary.md` §"Backend Contract") and are
//! preserved by both the Cassandra and memory backends:
//!
//! 1. **`seal` writes the pending row first, then the WAL columns.** A crash
//!    between the two writes leaves a stale pending row pointing at an Idle
//!    partition — design-acceptable per Crash Robustness §WAL Mode.
//! 2. **`apply_sealed` writes folded data + clears WAL first (atomically in a
//!    same-row `UNLOGGED BATCH`), then deletes the pending row.** Same crash
//!    residue shape; recovery resolves it.
//! 3. **`rollback_sealed` clears the WAL columns first, then deletes the
//!    pending row.** Same crash residue shape.
//! 4. **`direct_apply` writes only the `data` + `payload_encoding` columns.**
//!    Never touches WAL columns or the pending row.
//!
//! # Concurrency
//!
//! The framework guarantees one handler per key system-wide (Kafka
//! partition ownership + in-process per-key serialization). The
//! read-modify-write inside `apply_sealed` is safe because of that
//! invariant; this store never needs LWTs or distributed locks.
//!
//! # Apply shape
//!
//! Value's fold is last-writer-wins, so a one-shot `UPDATE` is the
//! correct shape for apply. Chunked collection kinds (Map/Deque) and their
//! recovery scanner are future work.

mod decode;
mod error;
mod queries;
mod scanner;
mod serialize;
mod udt;

#[cfg(test)]
mod tests;

pub use decode::{CorruptReason, WalColumnMask};
pub use error::{CassandraValueStoreError, CorruptUdtError};
pub use queries::ValueQueries;
pub use scanner::ScanPendingError;

use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::state::encoding::{
    EncodingError, PayloadEncoding, WalFormat, decode_wal, encode_payload, encode_wal,
};
use crate::state::middleware::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::pending::PendingIndexStore;
use crate::state::value::{
    DirectApplyStore, DurableWalStore, ValueApplied, ValueKind, ValueOp, ValueStore, fold_value_ops,
};
use crate::state::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, DurableState, EventRef, Read,
    SealedCollection, SealedWal, StateType, StoreOutcome, WalEnvelope,
};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use bytes::Bytes;
use decode::RawValueRow;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

/// Payload encoding for Value cells written by this build.
const VALUE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::RawZstdV1;

/// WAL format for Value WALs written by this build.
const VALUE_WAL_FORMAT: WalFormat = WalFormat::MsgpackStreamZstdV1;

/// Cassandra-backed durable Value store.
///
/// The constructor-supplied `default_ttl` is threaded through every
/// [`CollectionRef`] built by [`ValueStore::set`] / [`ValueStore::clear`]
/// so production writes carry the application's configured TTL via
/// `USING TTL ?`. `None` is reserved for indefinite retention or the
/// Cassandra over-20-year overflow fallback; production wiring sources it
/// once from [`crate::cassandra::CassandraStore::base_ttl`] at build
/// time.
#[derive(Clone, Debug)]
pub struct CassandraValueStore {
    store: CassandraStore,
    queries: Arc<ValueQueries>,
    default_ttl: Option<CompactDuration>,
}

impl CassandraValueStore {
    /// Creates a Cassandra Value store backed by an existing
    /// [`CassandraStore`] session, a prepared [`ValueQueries`] set, and
    /// a default TTL applied to every [`ValueStore::set`] /
    /// [`ValueStore::clear`] write. Pass `Some(d)` to bind a TTL via
    /// `USING TTL ?`; pass `None` for indefinite retention or as the
    /// over-20-year overflow fallback.
    #[must_use]
    pub fn new(
        store: CassandraStore,
        queries: Arc<ValueQueries>,
        default_ttl: Option<CompactDuration>,
    ) -> Self {
        Self {
            store,
            queries,
            default_ttl,
        }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    async fn execute_unpaged(
        &self,
        statement: &PreparedStatement,
        params: impl SerializeRow,
    ) -> Result<(), CassandraValueStoreError> {
        self.session()
            .execute_unpaged(statement, params)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    async fn read_row(
        &self,
        id: &CollectionId<ValueKind>,
    ) -> Result<Option<RawValueRow>, CassandraValueStoreError> {
        let (segment_id, key, state_type) = primary_read_components(id);
        let name = id.name().as_str();
        let row = self
            .session()
            .execute_unpaged(
                &self.queries.read_value_partition,
                (segment_id, key, state_type, name),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<RawValueRow>()
            .map_err(CassandraStoreError::from)?;
        Ok(row)
    }

    async fn read_durable_state(
        &self,
        id: &CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, CassandraValueStoreError> {
        let Some(row) = self.read_row(id).await? else {
            return Ok(DurableState::Idle { applied: None });
        };
        decode::try_decode_row(row)
    }

    async fn write_wal_columns(
        &self,
        collection: &CollectionRef<ValueKind>,
        event: EventRef,
        ops_bytes: &[u8],
    ) -> Result<(), CassandraValueStoreError> {
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        let payload_encoding = VALUE_PAYLOAD_ENCODING;
        let wal_format = VALUE_WAL_FORMAT;
        match collection.ttl() {
            Some(ttl) => {
                let ttl = ttl_to_i32(ttl);
                self.execute_unpaged(
                    &self.queries.write_wal,
                    (
                        ttl,
                        event,
                        ops_bytes,
                        wal_format,
                        payload_encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
            None => {
                self.execute_unpaged(
                    &self.queries.write_wal_no_ttl,
                    (
                        event,
                        ops_bytes,
                        wal_format,
                        payload_encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
        }
    }

    /// Clears the WAL columns; selects the variant that also wipes
    /// `payload_encoding` when no authoritative `data` is present, so the
    /// row never lands in the `PayloadEncodingWithoutData` shape after a
    /// rollback over a previously-empty row.
    async fn clear_wal_columns(
        &self,
        id: &CollectionId<ValueKind>,
        keep_payload_encoding: bool,
    ) -> Result<(), CassandraValueStoreError> {
        let (segment_id, key, state_type, name) = primary_components(id);
        let statement = if keep_payload_encoding {
            &self.queries.clear_wal
        } else {
            &self.queries.clear_wal_and_encoding
        };
        self.execute_unpaged(statement, (segment_id, key, state_type, name))
            .await
    }

    async fn apply_wal_atomic(
        &self,
        collection: &CollectionRef<ValueKind>,
        applied: &ValueApplied,
    ) -> Result<(), CassandraValueStoreError> {
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        let (data, encoding) = encode_applied_payload(applied)?;
        match collection.ttl() {
            Some(ttl) => {
                let ttl = ttl_to_i32(ttl);
                self.execute_unpaged(
                    &self.queries.batch_apply_wal,
                    (
                        ttl,
                        data.as_ref().map(Bytes::as_ref),
                        encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
            None => {
                self.execute_unpaged(
                    &self.queries.batch_apply_wal_no_ttl,
                    (
                        data.as_ref().map(Bytes::as_ref),
                        encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
        }
    }

    async fn write_data_only(
        &self,
        collection: &CollectionRef<ValueKind>,
        applied: &ValueApplied,
    ) -> Result<(), CassandraValueStoreError> {
        let (segment_id, key, state_type, name) = primary_components(collection.id());
        let (data, encoding) = encode_applied_payload(applied)?;
        match collection.ttl() {
            Some(ttl) => {
                let ttl = ttl_to_i32(ttl);
                self.execute_unpaged(
                    &self.queries.write_data_only,
                    (
                        ttl,
                        data.as_ref().map(Bytes::as_ref),
                        encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
            None => {
                self.execute_unpaged(
                    &self.queries.write_data_only_no_ttl,
                    (
                        data.as_ref().map(Bytes::as_ref),
                        encoding,
                        segment_id,
                        key,
                        state_type,
                        name,
                    ),
                )
                .await
            }
        }
    }

    async fn extract_applied(
        &self,
        id: &CollectionId<ValueKind>,
    ) -> Result<ValueApplied, CassandraValueStoreError> {
        let state = self.read_durable_state(id).await?;
        Ok(match state {
            DurableState::Idle { applied } | DurableState::Sealed { applied, .. } => applied,
        })
    }

    /// Reads the durable state and resolves it against `expected_event`,
    /// the shared prefix of [`apply_sealed`](Self::apply_sealed) and
    /// [`rollback_sealed`](Self::rollback_sealed):
    ///
    /// - `Ok(None)` — the partition is `Idle` (no WAL); the caller returns
    ///   [`StoreOutcome::NoOp`].
    /// - `Ok(Some((applied, wal)))` — a WAL is sealed and its event matches.
    /// - `Err(EventMismatch)` — a WAL is sealed for a different event.
    async fn read_sealed_matching(
        &self,
        id: &CollectionId<ValueKind>,
        expected_event: EventRef,
    ) -> Result<Option<(ValueApplied, SealedWal<ValueKind>)>, CassandraValueStoreError> {
        match self.read_durable_state(id).await? {
            DurableState::Idle { .. } => Ok(None),
            DurableState::Sealed { applied, wal } => {
                if wal.event() != expected_event {
                    return Err(CassandraValueStoreError::EventMismatch {
                        expected: expected_event,
                        actual: wal.event(),
                    });
                }
                Ok(Some((applied, wal)))
            }
        }
    }
}

impl ValueStore for CassandraValueStore {
    type Error = CassandraValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        let applied = self.extract_applied(collection).await?;
        Ok(applied.map_or(Read::Absent, Read::Present))
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        let collection_ref = CollectionRef::new(collection.clone(), self.default_ttl);
        self.direct_apply(&collection_ref, vec![ValueOp::Set { payload }])
            .await?;
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        let collection_ref = CollectionRef::new(collection.clone(), self.default_ttl);
        self.direct_apply(&collection_ref, vec![ValueOp::Clear])
            .await?;
        Ok(())
    }
}

impl DurableWalStore<ValueKind> for CassandraValueStore {
    type Error = CassandraValueStoreError;

    /// Reads the durable partition state. Returns `Idle { applied: None }`
    /// when no row exists.
    async fn read_partition<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<DurableState<ValueKind>, Self::Error> {
        self.read_durable_state(collection).await
    }

    /// Seals non-empty ordered operations for `event`. Writes the pending
    /// row first, then the WAL columns. Silently overwrites any
    /// pre-existing WAL on the row.
    async fn seal<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        event: EventRef,
        ops: I,
    ) -> Result<SealedCollection<ValueKind>, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let envelope = WalEnvelope::<ValueKind>::try_from_ops(ops.into_iter().collect())
            .map_err(EncodingError::from)?;
        let wal = encode_wal::<ValueKind>(&envelope, VALUE_WAL_FORMAT)?;

        self.insert_pending::<ValueKind>(collection.id()).await?;
        self.write_wal_columns(collection, event, wal.bytes())
            .await?;
        Ok(SealedCollection::new(collection.clone(), event))
    }

    /// Applies a sealed WAL when it matches `expected_event`. Folds the
    /// WAL ops over the current applied state, writes the folded result
    /// while atomically clearing the WAL columns, then deletes the
    /// pending row. Returns `NoOp` when no WAL is present.
    async fn apply_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        let Some((applied, wal)) = self
            .read_sealed_matching(collection.id(), expected_event)
            .await?
        else {
            return Ok(StoreOutcome::NoOp);
        };
        let envelope = decode_wal::<ValueKind>(wal.wal())?;
        let folded = fold_value_ops(applied, envelope.ops().iter());

        self.apply_wal_atomic(collection, &folded).await?;
        self.delete_pending::<ValueKind>(collection.id()).await?;
        Ok(StoreOutcome::Applied)
    }

    /// Rolls back a sealed WAL when it matches `expected_event`. Clears
    /// the WAL columns first, then deletes the pending row. The applied
    /// cell's TTL is not refreshed; `data` is untouched. Returns `NoOp`
    /// when no WAL is present.
    async fn rollback_sealed<'a>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        expected_event: EventRef,
    ) -> Result<StoreOutcome, Self::Error> {
        let Some((applied, _)) = self
            .read_sealed_matching(collection.id(), expected_event)
            .await?
        else {
            return Ok(StoreOutcome::NoOp);
        };
        self.clear_wal_columns(collection.id(), applied.is_some())
            .await?;
        self.delete_pending::<ValueKind>(collection.id()).await?;
        Ok(StoreOutcome::Applied)
    }
}

impl DirectApplyStore<ValueKind> for CassandraValueStore {
    type Error = CassandraValueStoreError;

    /// Applies ordered operations directly to authoritative state. Never
    /// touches WAL columns or the pending row.
    async fn direct_apply<'a, I>(
        &'a self,
        collection: &'a CollectionRef<ValueKind>,
        ops: I,
    ) -> Result<StoreOutcome, Self::Error>
    where
        I: IntoIterator<Item = ValueOp> + Send + 'a,
    {
        let ops: Vec<ValueOp> = ops.into_iter().collect();
        if ops.is_empty() {
            return Ok(StoreOutcome::NoOp);
        }

        let current = self.extract_applied(collection.id()).await?;
        let folded = fold_value_ops(current, &ops);
        self.write_data_only(collection, &folded).await?;
        Ok(StoreOutcome::Applied)
    }
}

impl DescriptorIdentityStore for CassandraValueStore {
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
            .rows_stream::<(String, i8, i16, i16, Option<String>)>()
            .map_err(CassandraStoreError::from)?;
        futures::pin_mut!(rows);
        let mut identities = Vec::new();
        while let Some((name, kind, cell_kind, codec_id, schema_label)) =
            rows.try_next().await.map_err(CassandraStoreError::from)?
        {
            identities.push(DurableDescriptorIdentity {
                name,
                kind,
                cell_kind,
                codec_id,
                schema_label,
            });
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
                row.kind,
                row.cell_kind,
                row.codec_id,
                row.schema_label,
            ));
        }
        self.session()
            .batch(&batch, values)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }
}

impl PendingIndexStore for CassandraValueStore {
    type Error = CassandraValueStoreError;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.execute_unpaged(&self.queries.insert_pending, pending_components(id))
            .await
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        self.execute_unpaged(&self.queries.delete_pending, pending_components(id))
            .await
    }
}

/// Encodes the authoritative applied payload for a `data` write.
///
/// Returns the encoded `data` cell and its `payload_encoding`
/// discriminator, or `(None, None)` when the applied state is empty (a
/// cleared cell). Shared by `apply_wal_atomic` and `write_data_only`,
/// which differ only in the query they bind these values into.
fn encode_applied_payload(
    applied: &ValueApplied,
) -> Result<(Option<Bytes>, Option<PayloadEncoding>), CassandraValueStoreError> {
    Ok(match applied {
        Some(payload) => (
            Some(encode_payload(payload, VALUE_PAYLOAD_ENCODING)?),
            Some(VALUE_PAYLOAD_ENCODING),
        ),
        None => (None, None),
    })
}

fn primary_components<K>(id: &CollectionId<K>) -> (&SegmentId, &str, StateType, &str)
where
    K: CollectionKind,
{
    let (segment_id, key, state_type) = primary_read_components(id);
    let name = id.name().as_str();
    (segment_id, key, state_type, name)
}

/// Primary-key components for the `keyed_state_pending` table: the value
/// primary key minus `name`'s position, plus the collection `kind`
/// discriminator. Shared by `insert_pending` and `delete_pending`.
fn pending_components<K>(
    id: &CollectionId<K>,
) -> (&SegmentId, &str, StateType, CollectionKindId, &str)
where
    K: CollectionKind,
{
    let (segment_id, key, state_type) = primary_read_components(id);
    (segment_id, key, state_type, K::ID, id.name().as_str())
}

fn primary_read_components<K>(id: &CollectionId<K>) -> (&SegmentId, &str, StateType)
where
    K: CollectionKind,
{
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = id.state_type();
    (segment_id, key, state_type)
}

fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}
