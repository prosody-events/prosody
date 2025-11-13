use crate::cassandra::migrator::MigrationError;
use crate::consumer::middleware::{ClassifyError, ErrorCategory};
use crate::timers::duration::CompactDurationError;
use scylla::_macro_internal::TypeCheckError;
use scylla::client::pager::NextPageError;
use scylla::errors::{
    BadKeyspaceName, BadQuery, BrokenConnectionError, ConnectionPoolError, CqlErrorParseError,
    CqlRequestSerializationError, CqlResultParseError, DbError, DeserializationError,
    ExecutionError, IntoRowsResultError, KeyspaceStrategyError, KeyspacesMetadataError,
    MaybeFirstRowError, MetadataError, MetadataFetchError, MetadataFetchErrorKind, NewSessionError,
    NextRowError, PagerExecutionError, PeersMetadataError, PrepareError, RequestAttemptError,
    RequestError, RowsError, SchemaAgreementError, SerializationError, SingleRowError,
    TablesMetadataError, UdtMetadataError, UseKeyspaceError,
};
use scylla::frame::frame_errors::{
    ColumnSpecParseError, FrameBodyExtensionsParseError, LowLevelDeserializationError,
    ResultMetadataAndRowsCountParseError, ResultMetadataParseError, TableSpecParseError,
};
use scylla::statement::prepared::{
    PartitionKeyError, PartitionKeyExtractionError, TokenCalculationError,
};
use thiserror::Error;

/// Error type for Cassandra store operations.
#[derive(Debug, Error)]
pub enum CassandraStoreError {
    /// Expected integer type but got something else.
    #[error("expected integer type")]
    IntExpected,

    /// Invalid duration
    #[error("Invalid duration: {0:#}")]
    Duration(#[from] CompactDurationError),

    /// Failed to create Cassandra session.
    #[error("Failed to create session: {0:#}")]
    Session(Box<NewSessionError>),

    /// Schema migration failed.
    #[error(transparent)]
    Migration(#[from] MigrationError),

    /// Failed to set keyspace.
    #[error("Failed to set keyspace: {0:#}")]
    UseKeyspace(Box<UseKeyspaceError>),

    /// Failed to prepare statement.
    #[error("Failed to prepare statement: {0:#}")]
    Prepare(Box<PrepareError>),

    /// Failed to execute statement.
    #[error("Failed to execute statement: {0:#}")]
    Execution(Box<ExecutionError>),

    /// Failed to retrieve the next page.
    #[error("Failed to retrieve the next page: {0:#}")]
    PageExecution(Box<PagerExecutionError>),

    /// Failed to retrieve the next row.
    #[error("Failed to retrieve the next row: {0:#}")]
    NextRow(Box<NextRowError>),

    /// Failed to retrieve the first row.
    #[error("Failed to retrieve the first row: {0:#}")]
    MaybeFirstRow(Box<MaybeFirstRowError>),

    /// Failed to get rows.
    #[error("Failed to get rows: {0:#}")]
    IntoRows(Box<IntoRowsResultError>),

    /// Rows error.
    #[error("Rows error: {0:#}")]
    Rows(Box<RowsError>),

    /// Invalid column type.
    #[error("Invalid type: {0:#}")]
    TypeCheck(Box<TypeCheckError>),
}

impl From<NewSessionError> for CassandraStoreError {
    fn from(error: NewSessionError) -> Self {
        Self::Session(Box::new(error))
    }
}

impl From<UseKeyspaceError> for CassandraStoreError {
    fn from(error: UseKeyspaceError) -> Self {
        Self::UseKeyspace(Box::new(error))
    }
}

impl From<PrepareError> for CassandraStoreError {
    fn from(error: PrepareError) -> Self {
        Self::Prepare(Box::new(error))
    }
}

impl From<ExecutionError> for CassandraStoreError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(Box::new(error))
    }
}

impl From<PagerExecutionError> for CassandraStoreError {
    fn from(error: PagerExecutionError) -> Self {
        Self::PageExecution(Box::new(error))
    }
}

impl From<NextRowError> for CassandraStoreError {
    fn from(error: NextRowError) -> Self {
        Self::NextRow(Box::new(error))
    }
}

impl From<MaybeFirstRowError> for CassandraStoreError {
    fn from(error: MaybeFirstRowError) -> Self {
        Self::MaybeFirstRow(Box::new(error))
    }
}

impl From<IntoRowsResultError> for CassandraStoreError {
    fn from(error: IntoRowsResultError) -> Self {
        Self::IntoRows(Box::new(error))
    }
}

impl From<RowsError> for CassandraStoreError {
    fn from(error: RowsError) -> Self {
        Self::Rows(Box::new(error))
    }
}

impl From<TypeCheckError> for CassandraStoreError {
    fn from(error: TypeCheckError) -> Self {
        Self::TypeCheck(Box::new(error))
    }
}

impl ClassifyError for CassandraStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Database returned non-integer type when code expects integer. Schema mismatch where
            // code type expectations don't match database schema. ALL similar operations will fail
            // until schema or code is fixed. Terminal because schema issues require intervention.
            Self::IntExpected => ErrorCategory::Terminal,

            Self::Duration(e) => e.classify_error(),
            Self::Session(e) => e.classify_error(),
            Self::Migration(e) => e.classify_error(),
            Self::UseKeyspace(e) => e.classify_error(),
            Self::Prepare(e) => e.classify_error(),
            Self::Execution(e) => e.classify_error(),
            Self::PageExecution(e) => e.classify_error(),
            Self::NextRow(e) => e.classify_error(),
            Self::MaybeFirstRow(e) => e.classify_error(),
            Self::IntoRows(e) => e.classify_error(),
            Self::Rows(e) => e.classify_error(),
            Self::TypeCheck(e) => e.classify_error(),
        }
    }
}

// ClassifyError implementations for external scylla error types

impl ClassifyError for ConnectionPoolError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Connection pool broken, last connection attempt failed. Network or cluster issue
            // that could be temporary (node down, network partition). Retry may succeed.
            Self::Broken { .. } => ErrorCategory::Transient,

            // Connection pool still initializing. Temporary state that will resolve once
            // initialization completes. Retry may succeed.
            Self::Initializing => ErrorCategory::Transient,

            // Host filter explicitly excludes this node. While this is a configuration setting,
            // the filter might be updated dynamically or temporarily. Routing to this node may
            // succeed later if filter is updated, or operation may succeed on other nodes.
            Self::NodeDisabledByHostFilter => ErrorCategory::Transient,

            // Non-exhaustive enum: safe default for unknown variants. New error types should be
            // explicitly classified after reviewing scylla driver updates.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for RequestAttemptError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::SerializationError(e) => e.classify_error(),
            Self::CqlRequestSerialization(e) => e.classify_error(),

            // Failed to allocate stream ID for request. Driver resource pool exhausted,
            // typically due to too many concurrent requests. Temporary resource constraint that
            // should resolve as requests complete. Retry may succeed after backoff.
            Self::UnableToAllocStreamId => ErrorCategory::Transient,

            Self::BrokenConnectionError(e) => e.classify_error(),
            Self::BodyExtensionsParseError(e) => e.classify_error(),
            Self::CqlResultParseError(e) => e.classify_error(),
            Self::CqlErrorParseError(e) => e.classify_error(),
            Self::DbError(e, _) => e.classify_error(),

            // Server sent unexpected response type. Likely protocol violation or driver bug, but
            // could be transient server state. May resolve when server restarts or reconnects.
            Self::UnexpectedResponse(_) => ErrorCategory::Transient,

            // Prepared statement ID changed after re-preparation. Protocol anomaly that may be
            // transient server state. May resolve on retry or reconnection.
            Self::RepreparedIdChanged { .. } => ErrorCategory::Transient,

            // Re-prepared statement missing from batch. Driver state issue that may resolve on
            // retry or reconnection.
            Self::RepreparedIdMissingInBatch => ErrorCategory::Transient,

            // Unpaged query returned paging state. Protocol anomaly that may be transient server
            // state. May resolve on retry.
            Self::NonfinishedPagingState => ErrorCategory::Transient,

            // Non-exhaustive enum: safe default for unknown variants. New error types should be
            // explicitly classified after reviewing scylla driver updates.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NextPageError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::PartitionKeyError(e) => e.classify_error(),
            Self::RequestFailure(e) => e.classify_error(),
            Self::ResultMetadataParseError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants. Future variants may include
            // type check errors or other validation failures that should be explicitly classified.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for SchemaAgreementError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::ConnectionPoolError(e) => e.classify_error(),
            Self::RequestError(e) => e.classify_error(),
            Self::TracesEventsIntoRowsResultError(e) => e.classify_error(),
            Self::SingleRowError(e) => e.classify_error(),

            // Schema agreement check timed out waiting for all nodes to converge on same schema
            // version. Transient cluster state during schema propagation. Retry may succeed once
            // schema converges across nodes.
            Self::Timeout(_) => ErrorCategory::Transient,

            // Required host not present in connection pool. Transient topology state during node
            // joins/leaves or pool initialization. Retry may succeed once topology stabilizes.
            Self::RequiredHostAbsent { .. } => ErrorCategory::Transient,

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for PeersMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Cluster peers metadata query returned no peers. Transient state during cluster
            // initialization or metadata refresh. Retry may succeed once topology stabilizes.
            Self::EmptyPeers => ErrorCategory::Transient,

            // Cluster peers have empty token ownership lists. Transient state during cluster
            // topology changes or token assignment. Retry may succeed once tokens are assigned.
            Self::EmptyTokenLists => ErrorCategory::Transient,

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for KeyspacesMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Strategy { error, .. } => error.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for UdtMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Database schema has invalid CQL type in UDT definition. Schema error affecting ALL
            // operations using this UDT. Requires schema fix to resolve. Terminal because schema
            // issues won't resolve without intervention.
            Self::InvalidCqlType { .. } => ErrorCategory::Terminal,

            // Database schema has circular UDT dependency. Schema design error affecting ALL
            // operations with this UDT. Requires schema fix. Terminal because schema issues need
            // manual intervention.
            Self::CircularTypeDependency => ErrorCategory::Terminal,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for TablesMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Database schema has invalid CQL type in table definition. Schema error affecting
            // ALL operations on this table. Requires schema fix. Terminal because schema issues
            // need manual intervention.
            Self::InvalidCqlType { .. } => ErrorCategory::Terminal,

            // Database schema has unknown column kind. Schema error affecting ALL operations on
            // this table. Requires schema fix. Terminal because schema issues need manual
            // intervention.
            Self::UnknownColumnKind { .. } => ErrorCategory::Terminal,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NewSessionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // ALL configured hostnames failed DNS resolution. Could be temporary DNS server
            // failure, network partition, or invalid hostnames. DNS infrastructure failures are
            // transient and may resolve. Retry may succeed when DNS recovers.
            Self::FailedToResolveAnyHostname(_) => ErrorCategory::Transient,

            // No Cassandra nodes configured for connection. Structural configuration error - cannot
            // connect to cluster without any node addresses. Terminal because requires adding node
            // addresses to configuration.
            Self::EmptyKnownNodesList => ErrorCategory::Terminal,

            Self::MetadataError(e) => e.classify_error(),
            Self::UseKeyspaceError(e) => e.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for UseKeyspaceError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // BadKeyspaceName: Invalid keyspace name format. Delegate to nested error.
            Self::BadKeyspaceName(e) => e.classify_error(),

            // RequestError: Network, timeout, or permissions. Delegate to nested error.
            Self::RequestError(e) => e.classify_error(),

            // Protocol violation during connection setup: server successfully processed USE command
            // but returned different keyspace name than requested. Indicates server bug, driver
            // bug, or protocol corruption. Transient because may resolve on reconnection to
            // different node or server restart.
            Self::KeyspaceNameMismatch { .. } => ErrorCategory::Transient,

            // Keyspace operation timed out. Cluster overload or slow query response. Transient
            // performance issue. Retry may succeed.
            Self::RequestTimeout(_) => ErrorCategory::Transient,

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for PrepareError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // ConnectionPoolError: Network/cluster issue. Delegate to nested error.
            Self::ConnectionPoolError(e) => e.classify_error(),

            // Failed to prepare statement on all contacted nodes. Could be network issues, all
            // nodes down, or invalid CQL. Network issues are transient. Invalid CQL should be
            // caught in testing. Conservative classification allows retry.
            Self::AllAttemptsFailed { .. } => ErrorCategory::Transient,

            // Prepared statement IDs don't match across nodes. Could be transient state during
            // cluster topology changes or statement propagation. May resolve on retry.
            Self::PreparedStatementIdsMismatch => ErrorCategory::Transient,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for ExecutionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::BadQuery(e) => e.classify_error(),

            // Load balancing policy returned empty execution plan. All nodes unavailable, down,
            // or filtered out. Transient cluster state. Retry may succeed when nodes recover.
            Self::EmptyPlan => ErrorCategory::Transient,

            Self::PrepareError(e) => e.classify_error(),
            Self::ConnectionPoolError(e) => e.classify_error(),
            Self::LastAttemptError(e) => e.classify_error(),

            // Query execution timed out. Cluster overload or slow query. Transient performance
            // issue. Retry may succeed.
            Self::RequestTimeout(_) => ErrorCategory::Transient,

            Self::UseKeyspaceError(e) => e.classify_error(),
            Self::SchemaAgreementError(e) => e.classify_error(),
            Self::MetadataError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for PagerExecutionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::PrepareError(e) => e.classify_error(),
            Self::SerializationError(e) => e.classify_error(),
            Self::NextPageError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NextRowError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::NextPageError(e) => e.classify_error(),
            Self::RowDeserializationError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for MaybeFirstRowError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::TypeCheckFailed(e) => e.classify_error(),
            Self::DeserializationFailed(e) => e.classify_error(),
        }
    }
}

impl ClassifyError for IntoRowsResultError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Code called into_rows() on a non-rows result (e.g., void result). Most likely a
            // code bug calling the wrong method for this query type, but could potentially vary
            // by data or query path. Conservative classification allows retry in case of edge
            // cases.
            Self::ResultNotRows(_) => ErrorCategory::Transient,

            Self::ResultMetadataLazyDeserializationError(e) => e.classify_error(),
        }
    }
}

impl ClassifyError for RowsError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::TypeCheckFailed(e) => e.classify_error(),
        }
    }
}

impl ClassifyError for TypeCheckError {
    fn classify_error(&self) -> ErrorCategory {
        // Type validation failed - code expects different type than database schema
        // provides. Schema mismatch affecting ALL similar operations. Requires
        // schema migration or code fix. Terminal because schema issues won't
        // resolve without intervention.
        ErrorCategory::Terminal
    }
}

impl ClassifyError for MetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::ConnectionPoolError(e) => e.classify_error(),
            Self::FetchError(e) => e.classify_error(),
            Self::Peers(e) => e.classify_error(),
            Self::Keyspaces(e) => e.classify_error(),
            Self::Udts(e) => e.classify_error(),
            Self::Tables(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for BadQuery {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Code provides wrong number of bound values to prepared statement. Prepared statement
            // expects N values (based on table schema), but code provides M values. This is a
            // programming error - code is calling function with wrong number of arguments. Will
            // fail for ALL messages on this code path, not data-dependent. Terminal to crash and
            // force fix rather than silently losing data.
            Self::PartitionKeyExtraction => ErrorCategory::Terminal,

            Self::SerializationError(e) => e.classify_error(),

            // Serialized partition key values exceed maximum size. Query will always fail.
            Self::ValuesTooLongForKey { .. } => ErrorCategory::Permanent,

            // Batch statement contains too many queries. Could be code bug or could vary with
            // batch size. Conservative classification allows retry.
            Self::TooManyQueriesInBatchStatement(_) => ErrorCategory::Transient,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for MetadataFetchError {
    fn classify_error(&self) -> ErrorCategory {
        match &self.error {
            MetadataFetchErrorKind::InvalidColumnType(e) => e.classify_error(),
            MetadataFetchErrorKind::PrepareError(e) => e.classify_error(),
            MetadataFetchErrorKind::SerializationError(e) => e.classify_error(),
            MetadataFetchErrorKind::NextRowError(e) => e.classify_error(),
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for RequestError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Load balancing policy returned empty execution plan. All nodes unavailable, down,
            // or filtered out. Transient cluster state. Retry may succeed when nodes recover.
            Self::EmptyPlan => ErrorCategory::Transient,

            Self::ConnectionPoolError(e) => e.classify_error(),

            // Client-side request timeout exceeded. Cluster overload or slow response. Transient
            // performance issue. Retry may succeed.
            Self::RequestTimeout(_) => ErrorCategory::Transient,

            Self::LastAttemptError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for SerializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to serialize query parameters. Data-dependent failure - specific
        // values in this message fail serialization (e.g., value too large, out
        // of range for CQL type, too many values). Other messages with valid
        // values will succeed. Permanent to drop this bad message rather than
        // retry endlessly.
        ErrorCategory::Permanent
    }
}

impl ClassifyError for DeserializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to deserialize database response. Schema mismatch where code type
        // expectations don't match database schema. Terminal because schema
        // issues require intervention.
        ErrorCategory::Terminal
    }
}

impl ClassifyError for PartitionKeyError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            PartitionKeyError::PartitionKeyExtraction(e) => e.classify_error(),
            PartitionKeyError::TokenCalculation(e) => e.classify_error(),
            PartitionKeyError::Serialization(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for ResultMetadataAndRowsCountParseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ResultMetadataAndRowsCountParseError::ResultMetadataParseError(e) => e.classify_error(),
            ResultMetadataAndRowsCountParseError::RowsCountParseError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for SingleRowError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Query returned unexpected number of rows. Could be code bug, but also could vary
            // with data or transient state. Conservative classification allows retry.
            SingleRowError::UnexpectedRowCount(_) => ErrorCategory::Transient,
            SingleRowError::TypeCheckFailed(e) => e.classify_error(),
            SingleRowError::DeserializationFailed(e) => e.classify_error(),
        }
    }
}

impl ClassifyError for BadKeyspaceName {
    fn classify_error(&self) -> ErrorCategory {
        // Invalid keyspace name format in configuration or code. Structural
        // configuration error that won't resolve without fixing the keyspace
        // name. Terminal because requires code or configuration change.
        ErrorCategory::Terminal
    }
}

impl ClassifyError for ResultMetadataParseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::FlagsParseError(e) => e.classify_error(),
            Self::ColumnCountParseError(e) => e.classify_error(),
            Self::PagingStateParseError(e) => e.classify_error(),
            Self::GlobalTableSpecParseError(e) => e.classify_error(),
            Self::ColumnSpecParseError(e) => e.classify_error(),

            // Non-exhaustive enum: safe default for unknown variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for LowLevelDeserializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to deserialize protocol frame at low level. Network corruption,
        // protocol version mismatch, or malformed response. Transient
        // network/protocol issue. Retry may succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for TableSpecParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to parse table specification from protocol frame. Malformed response
        // or protocol issue. Transient network/protocol error. Retry may
        // succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for ColumnSpecParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to parse column specification from protocol frame. Malformed response
        // or protocol issue. Transient network/protocol error. Retry may
        // succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlRequestSerializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to serialize CQL request to protocol format. Driver or protocol issue.
        // Transient error. Retry may succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for BrokenConnectionError {
    fn classify_error(&self) -> ErrorCategory {
        // Connection broken during operation. Network failure, node crash, or timeout.
        // Transient network issue. Retry may succeed with new connection.
        ErrorCategory::Transient
    }
}

impl ClassifyError for FrameBodyExtensionsParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to parse frame body extensions from protocol frame. Protocol version
        // mismatch or malformed response. Transient protocol error. Retry may
        // succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlResultParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to parse RESULT frame from protocol response. Malformed response or
        // protocol issue. Transient network/protocol error. Retry may succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlErrorParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Failed to parse ERROR frame from protocol response. Malformed error response
        // or protocol issue. Transient network/protocol error. Retry may
        // succeed.
        ErrorCategory::Transient
    }
}

impl ClassifyError for DbError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // CQL syntax error in query. Since queries are in program code, this is a code bug
            // that will fail for ALL executions. Terminal to crash and force fix.
            DbError::SyntaxError => ErrorCategory::Terminal,

            // Syntactically valid but semantically invalid query. Since queries are in program
            // code, this is a code bug that will fail for ALL executions. Terminal to crash and
            // force fix.
            DbError::Invalid => ErrorCategory::Terminal,

            // Keyspace or table already exists. Could be race condition with concurrent creation
            // or intentional idempotent operation. Permanent allows dropping message for
            // idempotent creates without crashing program.
            DbError::AlreadyExists { .. } => ErrorCategory::Permanent,

            // User-defined function execution failed. Could be UDF bug or could vary with input
            // data. Conservative classification allows retry.
            DbError::FunctionFailure { .. } => ErrorCategory::Transient,

            // Authentication failed. Could be wrong credentials or could be transient auth service
            // issue. Conservative classification allows retry in case auth service recovers.
            DbError::AuthenticationError => ErrorCategory::Transient,

            // Insufficient permissions. Could be config issue or could be transient permissions
            // state. Conservative classification allows retry.
            DbError::Unauthorized => ErrorCategory::Transient,

            // Invalid Cassandra configuration. Server-side configuration error. Terminal because
            // requires server configuration fix.
            DbError::ConfigError => ErrorCategory::Terminal,

            // Insufficient replicas available for consistency level. Transient cluster state that
            // may resolve as nodes recover.
            DbError::Unavailable { .. } => ErrorCategory::Transient,

            // Coordinator node overloaded. Transient load condition that may resolve.
            DbError::Overloaded => ErrorCategory::Transient,

            // Node still initializing. Transient state that resolves when bootstrap completes.
            DbError::IsBootstrapping => ErrorCategory::Transient,

            // Truncate operation failed. Could be transient cluster state. Retry may succeed.
            DbError::TruncateError => ErrorCategory::Transient,

            // Read operation timed out. Transient performance issue. Retry may succeed.
            DbError::ReadTimeout { .. } => ErrorCategory::Transient,

            // Write operation timed out. Transient performance issue. Retry may succeed.
            DbError::WriteTimeout { .. } => ErrorCategory::Transient,

            // Read failed on replica nodes. Transient cluster state. Retry may succeed.
            DbError::ReadFailure { .. } => ErrorCategory::Transient,

            // Write failed on replica nodes. Transient cluster state. Retry may succeed.
            DbError::WriteFailure { .. } => ErrorCategory::Transient,

            // Prepared statement not found on server (likely after node restart). Transient state
            // that driver handles by re-preparing. Retry may succeed.
            DbError::Unprepared { .. } => ErrorCategory::Transient,

            // Internal server error. Server bug or transient state. May resolve after server
            // restart or retry.
            DbError::ServerError => ErrorCategory::Transient,

            // Protocol violation between driver and server. Could be version mismatch or could be
            // transient server state. May resolve after server restart.
            DbError::ProtocolError => ErrorCategory::Transient,

            // Rate limit exceeded for partition. Transient throttling. Retry with backoff may
            // succeed.
            DbError::RateLimitReached { .. } => ErrorCategory::Transient,

            // Unknown/unrecognized error code. Conservative classification allows retry.
            DbError::Other(_) => ErrorCategory::Transient,

            // Non-exhaustive enum catch-all. Conservative default for future error variants.
            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for KeyspaceStrategyError {
    fn classify_error(&self) -> ErrorCategory {
        // Invalid keyspace replication strategy in configuration. Configuration error
        // requiring manual intervention to fix keyspace definition. Terminal
        // because won't resolve without configuration change.
        ErrorCategory::Terminal
    }
}

impl ClassifyError for PartitionKeyExtractionError {
    fn classify_error(&self) -> ErrorCategory {
        // Code provides wrong number of bound values to prepared statement. Prepared
        // statement expects N values (based on table schema), but code provides
        // M values. This is a programming error - code is calling function with
        // wrong number of arguments. Will fail for ALL messages on this code
        // path, not data-dependent. Terminal to crash and force fix rather than
        // silently losing data.
        ErrorCategory::Terminal
    }
}

impl ClassifyError for TokenCalculationError {
    fn classify_error(&self) -> ErrorCategory {
        // Partition key value exceeds maximum size (65,535 bytes). Data-dependent -
        // this specific message has oversized partition key value. Other
        // messages with valid-sized keys will succeed. Permanent to drop this
        // bad message. Note: Gets mapped to BadQuery::ValuesTooLongForKey which
        // is also Permanent.
        ErrorCategory::Permanent
    }
}
