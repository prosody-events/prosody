//! Error types and classification for Cassandra operations.
//!
//! This module defines error types for Cassandra store operations and
//! implements error classification for retry logic. Errors are classified as:
//!
//! - **Terminal**: Programming bugs or configuration errors that won't resolve
//!   without code/config changes. Requires process restart or deployment.
//! - **Permanent**: Data-dependent failures specific to this message. Other
//!   messages will succeed. Message should be dropped or moved to dead letter
//!   queue.
//! - **Transient**: Network, timeout, or cluster state issues that may resolve
//!   on retry. Retry with backoff recommended.

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
            // Schema mismatch: code expects integer but database has different type
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
            // All variants are transient cluster/network states:
            // - Broken: connection failed, may recover when network/node restores
            // - Initializing: pool starting up, will complete shortly
            // - NodeDisabledByHostFilter: host filter may change, or other nodes may succeed
            // - _: unknown variants conservatively treated as transient
            Self::Broken { .. } | Self::Initializing | Self::NodeDisabledByHostFilter | _ => {
                ErrorCategory::Transient
            }
        }
    }
}

impl ClassifyError for RequestAttemptError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::SerializationError(e) => e.classify_error(),
            Self::CqlRequestSerialization(e) => e.classify_error(),
            Self::BrokenConnectionError(e) => e.classify_error(),
            Self::BodyExtensionsParseError(e) => e.classify_error(),
            Self::CqlResultParseError(e) => e.classify_error(),
            Self::CqlErrorParseError(e) => e.classify_error(),
            Self::DbError(e, _) => e.classify_error(),

            // All remaining variants are transient driver/protocol states:
            // - UnableToAllocStreamId: resource pool exhausted, resolves as requests complete
            // - UnexpectedResponse: protocol anomaly, may resolve on reconnection
            // - RepreparedIdChanged: statement re-preparation issue, retry may succeed
            // - RepreparedIdMissingInBatch: driver state inconsistency, retry may succeed
            // - NonfinishedPagingState: protocol violation, retry may succeed
            // - _: unknown variants conservatively treated as transient
            Self::UnableToAllocStreamId
            | Self::UnexpectedResponse(_)
            | Self::RepreparedIdChanged { .. }
            | Self::RepreparedIdMissingInBatch
            | Self::NonfinishedPagingState
            | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NextPageError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::PartitionKeyError(e) => e.classify_error(),
            Self::RequestFailure(e) => e.classify_error(),
            Self::ResultMetadataParseError(e) => e.classify_error(),

            // Unknown variants conservatively treated as transient
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

            // Transient cluster states during schema propagation:
            // - Timeout: schema agreement check took too long
            // - RequiredHostAbsent: host unavailable during topology check
            Self::Timeout(_) | Self::RequiredHostAbsent { .. } | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for PeersMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        // All variants are transient topology states (cluster initialization, metadata
        // refresh)
        match self {
            Self::EmptyPeers | Self::EmptyTokenLists | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for KeyspacesMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Strategy { error, .. } => error.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for UdtMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Schema errors require manual fix: invalid type or circular UDT dependency
            Self::InvalidCqlType { .. } | Self::CircularTypeDependency => ErrorCategory::Terminal,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for TablesMetadataError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Schema errors require manual fix: invalid type or unknown column kind
            Self::InvalidCqlType { .. } | Self::UnknownColumnKind { .. } => ErrorCategory::Terminal,

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NewSessionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Configuration error: no nodes configured, requires adding node addresses
            Self::EmptyKnownNodesList => ErrorCategory::Terminal,

            Self::MetadataError(e) => e.classify_error(),
            Self::UseKeyspaceError(e) => e.classify_error(),

            // DNS failures are transient infrastructure issues
            Self::FailedToResolveAnyHostname(_) | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for UseKeyspaceError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::BadKeyspaceName(e) => e.classify_error(),
            Self::RequestError(e) => e.classify_error(),

            // Protocol violations and timeouts are transient
            Self::KeyspaceNameMismatch { .. } | Self::RequestTimeout(_) | _ => {
                ErrorCategory::Transient
            }
        }
    }
}

impl ClassifyError for PrepareError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::ConnectionPoolError(e) => e.classify_error(),

            // Statement preparation failures (network, topology changes, or invalid CQL)
            // Invalid CQL should be caught in testing; conservatively treat as transient
            Self::AllAttemptsFailed { .. } | Self::PreparedStatementIdsMismatch | _ => {
                ErrorCategory::Transient
            }
        }
    }
}

impl ClassifyError for ExecutionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::BadQuery(e) => e.classify_error(),
            Self::PrepareError(e) => e.classify_error(),
            Self::ConnectionPoolError(e) => e.classify_error(),
            Self::LastAttemptError(e) => e.classify_error(),
            Self::UseKeyspaceError(e) => e.classify_error(),
            Self::SchemaAgreementError(e) => e.classify_error(),
            Self::MetadataError(e) => e.classify_error(),

            // Transient cluster states: no nodes available, timeouts, overload
            Self::EmptyPlan | Self::RequestTimeout(_) | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for PagerExecutionError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::PrepareError(e) => e.classify_error(),
            Self::SerializationError(e) => e.classify_error(),
            Self::NextPageError(e) => e.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for NextRowError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::NextPageError(e) => e.classify_error(),
            Self::RowDeserializationError(e) => e.classify_error(),

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
            // Wrong result type (e.g., void instead of rows). Likely code bug but could
            // vary by query path. Conservatively treat as transient for edge cases.
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
        // Schema mismatch: code type expectations don't match database schema
        // Requires schema migration or code fix
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

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for BadQuery {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Programming error: wrong number of bound values for prepared statement
            // Will fail for ALL messages on this code path (not data-dependent)
            Self::PartitionKeyExtraction => ErrorCategory::Terminal,

            Self::SerializationError(e) => e.classify_error(),

            // Data-dependent: this message's partition key exceeds max size
            Self::ValuesTooLongForKey { .. } => ErrorCategory::Permanent,

            // Could be code bug or could vary with batch size; conservatively transient
            Self::TooManyQueriesInBatchStatement(_) | _ => ErrorCategory::Transient,
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
            Self::ConnectionPoolError(e) => e.classify_error(),
            Self::LastAttemptError(e) => e.classify_error(),

            // Transient cluster states: no nodes available, timeouts, overload
            Self::EmptyPlan | Self::RequestTimeout(_) | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for SerializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Data-dependent: this message has invalid values (too large, out of range,
        // etc.) Other messages with valid values will succeed
        ErrorCategory::Permanent
    }
}

impl ClassifyError for DeserializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Schema mismatch: code type expectations don't match database schema
        ErrorCategory::Terminal
    }
}

impl ClassifyError for PartitionKeyError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            PartitionKeyError::PartitionKeyExtraction(e) => e.classify_error(),
            PartitionKeyError::TokenCalculation(e) => e.classify_error(),
            PartitionKeyError::Serialization(e) => e.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for ResultMetadataAndRowsCountParseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            ResultMetadataAndRowsCountParseError::ResultMetadataParseError(e) => e.classify_error(),
            ResultMetadataAndRowsCountParseError::RowsCountParseError(e) => e.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for SingleRowError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Could be code bug or could vary with data; conservatively transient
            SingleRowError::UnexpectedRowCount(_) => ErrorCategory::Transient,
            SingleRowError::TypeCheckFailed(e) => e.classify_error(),
            SingleRowError::DeserializationFailed(e) => e.classify_error(),
        }
    }
}

impl ClassifyError for BadKeyspaceName {
    fn classify_error(&self) -> ErrorCategory {
        // Configuration error: invalid keyspace name format
        ErrorCategory::Terminal
    }
}

impl ClassifyError for ResultMetadataParseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::FlagsParseError(e)
            | Self::ColumnCountParseError(e)
            | Self::PagingStateParseError(e) => e.classify_error(),
            Self::GlobalTableSpecParseError(e) => e.classify_error(),
            Self::ColumnSpecParseError(e) => e.classify_error(),

            _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for LowLevelDeserializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (network corruption, protocol mismatch)
        ErrorCategory::Transient
    }
}

impl ClassifyError for TableSpecParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (malformed table spec)
        ErrorCategory::Transient
    }
}

impl ClassifyError for ColumnSpecParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (malformed column spec)
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlRequestSerializationError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol serialization failure (driver or protocol issue)
        ErrorCategory::Transient
    }
}

impl ClassifyError for BrokenConnectionError {
    fn classify_error(&self) -> ErrorCategory {
        // Network failure: connection broken during operation
        ErrorCategory::Transient
    }
}

impl ClassifyError for FrameBodyExtensionsParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (malformed body extensions)
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlResultParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (malformed RESULT frame)
        ErrorCategory::Transient
    }
}

impl ClassifyError for CqlErrorParseError {
    fn classify_error(&self) -> ErrorCategory {
        // Protocol frame parsing failure (malformed ERROR frame)
        ErrorCategory::Transient
    }
}

impl ClassifyError for DbError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            // Terminal: code bugs or server config requiring manual fix
            DbError::SyntaxError | DbError::Invalid | DbError::ConfigError => {
                ErrorCategory::Terminal
            }

            // Permanent: idempotent operations (e.g., CREATE IF NOT EXISTS race)
            DbError::AlreadyExists { .. } => ErrorCategory::Permanent,

            // Transient: cluster/server states that may resolve on retry
            // - Auth/permissions: AuthenticationError, Unauthorized
            // - Availability: Unavailable, Overloaded, IsBootstrapping
            // - Timeouts/failures: ReadTimeout, WriteTimeout, ReadFailure, WriteFailure
            // - Operations: FunctionFailure, TruncateError, Unprepared
            // - Protocol: ServerError, ProtocolError, RateLimitReached
            DbError::FunctionFailure { .. }
            | DbError::AuthenticationError
            | DbError::Unauthorized
            | DbError::Unavailable { .. }
            | DbError::Overloaded
            | DbError::IsBootstrapping
            | DbError::TruncateError
            | DbError::ReadTimeout { .. }
            | DbError::WriteTimeout { .. }
            | DbError::ReadFailure { .. }
            | DbError::WriteFailure { .. }
            | DbError::Unprepared { .. }
            | DbError::ServerError
            | DbError::ProtocolError
            | DbError::RateLimitReached { .. }
            | DbError::Other(_)
            | _ => ErrorCategory::Transient,
        }
    }
}

impl ClassifyError for KeyspaceStrategyError {
    fn classify_error(&self) -> ErrorCategory {
        // Configuration error: invalid keyspace replication strategy
        ErrorCategory::Terminal
    }
}

impl ClassifyError for PartitionKeyExtractionError {
    fn classify_error(&self) -> ErrorCategory {
        // Programming error: wrong number of bound values for prepared statement
        // Will fail for ALL messages on this code path (not data-dependent)
        ErrorCategory::Terminal
    }
}

impl ClassifyError for TokenCalculationError {
    fn classify_error(&self) -> ErrorCategory {
        // Data-dependent: this message's partition key exceeds max size (65,535 bytes)
        // Maps to BadQuery::ValuesTooLongForKey
        ErrorCategory::Permanent
    }
}
