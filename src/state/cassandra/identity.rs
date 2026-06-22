//! Cassandra-backed descriptor-identity store.
//!
//! [`CassandraDescriptorIdentityStore`] implements [`DescriptorIdentityStore`]
//! over the group-global `keyed_state_identity` table provisioned by migration
//! `20260522_create_keyed_state.cql`. It is the control-plane half of keyed
//! state — decoupled from any kind's cell data so "which kind owns identity?"
//! is un-askable. The steady-state read is a point-read; first-use registration
//! is the one authorized keyed-state LWT (see [`Self::register_identity`]).
//!
//! # Concurrency
//!
//! Identity is group-global, so any partition's store handle is equivalent.
//! First-use registration races only across group members; the `IF NOT EXISTS`
//! insert echoes the existing row so the loser validates without a re-read.

use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraStore, TABLE_KEYED_STATE_IDENTITY};
use crate::cassandra_queries;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::StateType;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use scylla::client::session::Session;
use scylla::value::{CqlValue, Row};
use std::sync::Arc;
use thiserror::Error;

/// Cassandra-backed group-global descriptor-identity store.
#[derive(Clone, Debug)]
pub struct CassandraDescriptorIdentityStore {
    store: CassandraStore,
    queries: Arc<IdentityQueries>,
}

impl CassandraDescriptorIdentityStore {
    /// Creates a Cassandra identity store over an existing [`CassandraStore`]
    /// session and a prepared [`IdentityQueries`] set.
    #[must_use]
    pub fn new(store: CassandraStore, queries: Arc<IdentityQueries>) -> Self {
        Self { store, queries }
    }

    fn session(&self) -> &Session {
        self.store.session()
    }
}

impl DescriptorIdentityStore for CassandraDescriptorIdentityStore {
    type Error = CassandraDescriptorIdentityError;

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
            .ok_or(CassandraDescriptorIdentityError::MalformedLwtResult)?;
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
) -> Result<DurableDescriptorIdentity, CassandraDescriptorIdentityError> {
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

fn column_bool(
    specs: &[String],
    row: &Row,
    name: &str,
) -> Result<bool, CassandraDescriptorIdentityError> {
    match column_value(specs, row, name) {
        Some(CqlValue::Boolean(b)) => Ok(*b),
        _ => Err(CassandraDescriptorIdentityError::MalformedLwtResult),
    }
}

fn column_tinyint(
    specs: &[String],
    row: &Row,
    name: &str,
) -> Result<i8, CassandraDescriptorIdentityError> {
    match column_value(specs, row, name) {
        Some(CqlValue::TinyInt(v)) => Ok(*v),
        _ => Err(CassandraDescriptorIdentityError::MalformedLwtResult),
    }
}

fn column_text(
    specs: &[String],
    row: &Row,
    name: &str,
) -> Result<String, CassandraDescriptorIdentityError> {
    match column_value(specs, row, name) {
        Some(CqlValue::Text(s)) => Ok(s.clone()),
        _ => Err(CassandraDescriptorIdentityError::MalformedLwtResult),
    }
}

fn column_text_opt(specs: &[String], row: &Row, name: &str) -> Option<String> {
    match column_value(specs, row, name) {
        Some(CqlValue::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The non-key identity columns a [`IdentityQueries::read_identity`] point-read
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

cassandra_queries! {
    /// Container for the prepared CQL statements used by
    /// [`CassandraDescriptorIdentityStore`].
    pub struct IdentityQueries {
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

/// Errors that can occur during Cassandra descriptor-identity operations.
#[derive(Debug, Error)]
pub enum CassandraDescriptorIdentityError {
    /// Wrapped Cassandra driver error.
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// An `INSERT … IF NOT EXISTS` identity registration returned a result
    /// row without the expected `[applied]` flag or echoed columns. Only
    /// reachable from a driver/cluster contract break, not application data.
    #[error("descriptor identity registration returned a malformed LWT result")]
    MalformedLwtResult,
}

impl ClassifyError for CassandraDescriptorIdentityError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::MalformedLwtResult => ErrorCategory::Permanent,
        }
    }
}
