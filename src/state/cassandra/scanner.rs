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
use futures::{Stream, StreamExt, TryStreamExt};
use scylla::statement::prepared::PreparedStatement;
use std::pin::Pin;
use tokio::task::coop::cooperative;
use tracing::warn;

impl PendingIndexScanner for CassandraValueStore {
    type Error = ScanPendingError;
    type Stream = Pin<Box<dyn Stream<Item = Result<PendingEntry, Self::Error>> + Send>>;

    fn scan_pending(&self, state_key: &StateKey) -> Self::Stream {
        let store = self.store.clone();
        let statement = self.queries.scan_pending.clone();
        let segment_id = state_key.segment_id;
        let key = state_key.key.clone();

        scan_stream(store, statement, segment_id, key.as_ref().to_owned()).boxed()
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
            .map_err(CassandraStoreError::from)
            .map_err(ScanPendingError::Database)?
            .rows_stream::<(i8, i8, String)>()
            .map_err(CassandraStoreError::from)
            .map_err(ScanPendingError::Database)?;

        futures::pin_mut!(rows);
        while let Some((state_type_i8, kind_i8, name)) = cooperative(rows.try_next())
            .await
            .map_err(CassandraStoreError::from)
            .map_err(ScanPendingError::Database)?
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
    let Some(state_type) = StateType::from_i8(state_type_i8) else {
        warn!(
            state_type = state_type_i8,
            "skipping unknown state_type in pending scan"
        );
        return Ok(None);
    };

    let Some(kind) = CollectionKindId::from_i8(kind_i8) else {
        warn!(
            kind = kind_i8,
            "skipping unknown collection kind in pending scan"
        );
        return Ok(None);
    };

    let name = StateName::try_new(name)?;
    Ok(Some(PendingEntry::new(state_type, kind, name)))
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
