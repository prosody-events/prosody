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
    type Column: for<'frame, 'metadata> DeserializeValue<'frame, 'metadata> + Send + 'static;
    const SELECT: &'static str;
    fn decode_column(
        column: Option<Self::Column>,
        encoding: Option<Encoding>,
    ) -> Result<Option<Self::Payload>, CassandraCellStoreError>;
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
