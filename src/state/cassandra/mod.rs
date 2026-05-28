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
//! # Slice 4 scope
//!
//! Value's fold is last-writer-wins, so a one-shot `UPDATE` is the
//! correct shape for apply. Map/Deque chunking and the recovery scanner
//! land in later slices.

mod decode;
mod error;
mod queries;
mod scanner;
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
use crate::state::pending::PendingIndexStore;
use crate::state::value::{
    DirectApplyStore, DurableWalStore, StoredPayload, ValueApplied, ValueKind, ValueOp, ValueStore,
    fold_value_ops,
};
use crate::state::{
    CollectionId, CollectionKind, CollectionKindId, CollectionRef, DurableState, EventRef, Read,
    SealedCollection, StateType, StoreOutcome, WalEnvelope,
};
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentId;
use bytes::Bytes;
use decode::RawValueRow;
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

/// Payload encoding for Value cells written by this build.
const VALUE_PAYLOAD_ENCODING: PayloadEncoding = PayloadEncoding::MsgpackZstdV1;

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
        let payload_encoding = VALUE_PAYLOAD_ENCODING.as_i16();
        let wal_format = VALUE_WAL_FORMAT.as_i16();
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
}

impl ValueStore for CassandraValueStore {
    type Error = CassandraValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        let applied = self.extract_applied(collection).await?;
        Ok(applied.map_or(Read::Absent, Read::Present))
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
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
        match self.read_durable_state(collection.id()).await? {
            DurableState::Idle { .. } => Ok(StoreOutcome::NoOp),
            DurableState::Sealed { applied, wal } => {
                if wal.event() != expected_event {
                    return Err(CassandraValueStoreError::EventMismatch {
                        expected: expected_event,
                        actual: wal.event(),
                    });
                }
                let envelope = decode_wal::<ValueKind>(wal.wal())?;
                let folded = fold_value_ops(applied, envelope.ops().iter());

                self.apply_wal_atomic(collection, &folded).await?;
                self.delete_pending::<ValueKind>(collection.id()).await?;
                Ok(StoreOutcome::Applied)
            }
        }
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
        match self.read_durable_state(collection.id()).await? {
            DurableState::Idle { .. } => Ok(StoreOutcome::NoOp),
            DurableState::Sealed { applied, wal } => {
                if wal.event() != expected_event {
                    return Err(CassandraValueStoreError::EventMismatch {
                        expected: expected_event,
                        actual: wal.event(),
                    });
                }
                self.clear_wal_columns(collection.id(), applied.is_some())
                    .await?;
                self.delete_pending::<ValueKind>(collection.id()).await?;
                Ok(StoreOutcome::Applied)
            }
        }
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

impl PendingIndexStore for CassandraValueStore {
    type Error = CassandraValueStoreError;

    async fn insert_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        let segment_id = id.state_key().segment_id;
        let key = id.state_key().key.as_ref();
        let state_type = state_type_to_i8(id.state_type());
        let kind = kind_to_i8(K::ID);
        let name = id.name().as_str();
        self.execute_unpaged(
            &self.queries.insert_pending,
            (segment_id, key, state_type, kind, name),
        )
        .await
    }

    async fn delete_pending<'a, K>(&'a self, id: &'a CollectionId<K>) -> Result<(), Self::Error>
    where
        K: CollectionKind,
    {
        let segment_id = id.state_key().segment_id;
        let key = id.state_key().key.as_ref();
        let state_type = state_type_to_i8(id.state_type());
        let kind = kind_to_i8(K::ID);
        let name = id.name().as_str();
        self.execute_unpaged(
            &self.queries.delete_pending,
            (segment_id, key, state_type, kind, name),
        )
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
) -> Result<(Option<Bytes>, Option<i16>), CassandraValueStoreError> {
    Ok(match applied {
        Some(payload) => (
            Some(encode_payload(payload, VALUE_PAYLOAD_ENCODING)?),
            Some(VALUE_PAYLOAD_ENCODING.as_i16()),
        ),
        None => (None, None),
    })
}

fn primary_components<K>(id: &CollectionId<K>) -> (&SegmentId, &str, i8, &str)
where
    K: CollectionKind,
{
    let (segment_id, key, state_type) = primary_read_components(id);
    let name = id.name().as_str();
    (segment_id, key, state_type, name)
}

fn primary_read_components<K>(id: &CollectionId<K>) -> (&SegmentId, &str, i8)
where
    K: CollectionKind,
{
    let segment_id = &id.state_key().segment_id;
    let key = id.state_key().key.as_ref();
    let state_type = state_type_to_i8(id.state_type());
    (segment_id, key, state_type)
}

fn state_type_to_i8(state_type: StateType) -> i8 {
    match state_type {
        StateType::Application => 0,
    }
}

fn kind_to_i8(kind: CollectionKindId) -> i8 {
    kind as u8 as i8
}

fn ttl_to_i32(ttl: CompactDuration) -> i32 {
    ttl.seconds().try_into().unwrap_or(i32::MAX)
}
