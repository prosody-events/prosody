use super::CassandraCellStoreError;
use super::decode::decode_blob;
use super::encoding::Encoding;
use super::queries::{CellQueries, ReadStatements};
use crate::state::cell::{Presence, Projection, Values};
use bytes::Bytes;
use scylla::deserialize::value::DeserializeValue;

/// The columns, decoder, and statements for one Cassandra projection.
/// Public visibility permits generic bounds; the private module keeps this
/// sealed trait internal.
pub trait CassandraProjection: Projection {
    /// The driver type for one projected blob column.
    type Column: for<'frame, 'metadata> DeserializeValue<'frame, 'metadata> + Send + 'static;
    /// The column expressions for `data` and `prev_data`, in that order.
    const SELECT: &'static str;
    /// Decodes one column with the shared row encoding.
    fn decode_column(
        column: Option<Self::Column>,
        encoding: Option<Encoding>,
    ) -> Result<Option<Self::Payload>, CassandraCellStoreError>;
    /// Returns the prepared statements for this projection.
    fn statements(queries: &CellQueries) -> &ReadStatements;
}

impl CassandraProjection for Values {
    type Column = Bytes;

    const SELECT: &'static str = "data, prev_data";

    fn decode_column(
        column: Option<Self::Column>,
        encoding: Option<Encoding>,
    ) -> Result<Option<Self::Payload>, CassandraCellStoreError> {
        decode_blob(column, encoding)
    }

    fn statements(queries: &CellQueries) -> &ReadStatements {
        &queries.values
    }
}

impl CassandraProjection for Presence {
    type Column = i64;

    const SELECT: &'static str = "WRITETIME(data), WRITETIME(prev_data)";

    fn decode_column(
        column: Option<Self::Column>,
        _encoding: Option<Encoding>,
    ) -> Result<Option<Self::Payload>, CassandraCellStoreError> {
        Ok(column.map(|_| ()))
    }

    fn statements(queries: &CellQueries) -> &ReadStatements {
        &queries.presence
    }
}
