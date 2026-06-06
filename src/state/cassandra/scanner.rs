//! Streamed `PendingIndexScanner` implementation for the Cassandra Value
//! store.
//!
//! Lives in its own file to keep `mod.rs` readable. Stream shape mirrors
//! `crate::timers::store::cassandra::trigger_store::get_slabs`: an
//! `async_stream::try_stream!` block that pulls rows via `execute_iter` +
//! `rows_stream` and yields one [`PendingEntry`] per row. The scan is a
//! single-partition query (partition key `(segment_id, key)`), so it never
//! pages the whole table.

use super::CassandraValueStore;
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::pending::{PendingEntry, PendingIndexScanner};
use crate::state::{CollectionKindId, StateKey, StateName, StateNameError, StateType};
use crate::timers::store::SegmentId;
use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use scylla::statement::prepared::PreparedStatement;
use tokio::task::coop::cooperative;
use tracing::warn;

impl PendingIndexScanner for CassandraValueStore {
    type Error = ScanPendingError;

    fn scan_pending(
        &self,
        state_key: &StateKey,
    ) -> impl Stream<Item = Result<PendingEntry, Self::Error>> + Send {
        let store = self.store.clone();
        let statement = self.queries.scan_pending.clone();
        let segment_id = state_key.segment_id;
        let key = state_key.key.as_ref().to_owned();

        scan_stream(store, statement, segment_id, key)
    }
}

fn scan_stream(
    store: CassandraStore,
    statement: PreparedStatement,
    segment_id: SegmentId,
    key: String,
) -> impl Stream<Item = Result<PendingEntry, ScanPendingError>> + Send {
    try_stream! {
        let rows = store
            .session()
            .execute_iter(statement, (segment_id, key.as_str()))
            .await
            .map_err(db_err)?
            .rows_stream::<(i8, i8, String)>()
            .map_err(db_err)?;

        futures::pin_mut!(rows);
        while let Some((state_type_i8, kind_i8, name)) = cooperative(rows.try_next())
            .await
            .map_err(db_err)?
        {
            let Some(entry) = decode_row(state_type_i8, kind_i8, name)? else {
                continue;
            };
            yield entry;
        }
    }
}

/// Decodes a streamed row, skipping rows whose discriminators this build
/// does not recognise. Unknown kind and state-type bytes are logged at WARN
/// and dropped because the row may have been written by a newer build that
/// added variants this one cannot map; the recovery handler should not
/// classify the entire partition as corrupt over a single forward-compatible
/// row.
fn decode_row(
    state_type_i8: i8,
    kind_i8: i8,
    name: String,
) -> Result<Option<PendingEntry>, ScanPendingError> {
    let Ok(state_type) = StateType::try_from(state_type_i8) else {
        warn!(
            state_type = state_type_i8,
            "skipping unknown state_type in pending scan"
        );
        return Ok(None);
    };

    let Ok(kind) = CollectionKindId::try_from(kind_i8) else {
        warn!(
            kind = kind_i8,
            "skipping unknown collection kind in pending scan"
        );
        return Ok(None);
    };

    let name = StateName::try_new(name)?;
    Ok(Some(PendingEntry::new(state_type, kind, name)))
}

/// Wraps any scylla driver error as a [`ScanPendingError::Database`], folding
/// the two-step `CassandraStoreError::from` conversion the scan path repeats
/// at every `execute_iter` / `rows_stream` / `try_next` boundary.
fn db_err<E: Into<CassandraStoreError>>(error: E) -> ScanPendingError {
    ScanPendingError::Database(error.into())
}

/// Error type for [`CassandraValueStore::scan_pending`].
///
/// Distinct from [`super::CassandraValueStoreError`] so the middleware error
/// enum has a typed scanner arm — the scanner does not surface WAL or UDT
/// decode failures; those live on the WAL read path.
#[derive(Debug, thiserror::Error)]
pub enum ScanPendingError {
    /// Wrapped Cassandra driver error.
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// A pending row carried an empty `name` cell.
    #[error(transparent)]
    Name(#[from] StateNameError),
}

impl ClassifyError for ScanPendingError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::Name(e) => e.classify_error(),
        }
    }
}
