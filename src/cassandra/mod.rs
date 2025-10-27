//! Cassandra infrastructure for all Prosody components.
//!
//! This module provides the foundational Cassandra connectivity, configuration,
//! and migration system used by all stateful components in Prosody. It manages
//! a single session and unified migration system.

use crate::propagator::new_propagator;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::duration::CompactDurationError;
use opentelemetry::propagation::TextMapCompositePropagator;
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, DeserializeValue, FrameSlice, SerializationError,
    SerializeValue, TypeCheckError, WrittenCellProof,
};
use scylla::client::Compression;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::NativeType;
use scylla::errors::{
    ExecutionError, IntoRowsResultError, MaybeFirstRowError, NewSessionError, NextRowError,
    PagerExecutionError, PrepareError, RowsError, UseKeyspaceError,
};
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::DefaultRetryPolicy;
use scylla::statement::Consistency;
use std::sync::Arc;
use thiserror::Error;

pub mod config;
pub mod migrator;

pub use config::CassandraConfiguration;
pub use migrator::CassandraMigrator;

/// Table name for storing segment metadata and slab IDs.
pub const TABLE_SEGMENTS: &str = "timer_segments";

/// Table name for storing timer triggers organized by time slabs.
pub const TABLE_SLABS: &str = "timer_slabs";

/// Table name for storing timer triggers indexed by key for efficient key-based
/// lookups.
pub const TABLE_KEYS: &str = "timer_keys";

/// Table name for storing v2 timer triggers with timer type support, organized
/// by time slabs.
pub const TABLE_TYPED_SLABS: &str = "timer_typed_slabs";

/// Table name for storing v2 timer triggers with timer type support, indexed by
/// key for efficient key-based lookups.
pub const TABLE_TYPED_KEYS: &str = "timer_typed_keys";

/// Table name for tracking applied database migrations.
pub const TABLE_SCHEMA_MIGRATIONS: &str = "schema_migrations";

/// Table name for distributed migration locking.
pub const TABLE_LOCKS: &str = "locks";

/// Unified Cassandra store providing session and infrastructure for all
/// components.
///
/// This store manages the single Cassandra session, runs migrations for all
/// components, and provides common utilities like TTL calculation and
/// OpenTelemetry propagation.
#[derive(Clone, Debug)]
pub struct CassandraStore {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    session: Session,
    propagator: TextMapCompositePropagator,
    base_ttl: CompactDuration,
}

impl CassandraStore {
    /// Creates a new Cassandra store with the given configuration.
    ///
    /// Initializes the connection to Cassandra and runs schema migrations
    /// for all registered components.
    ///
    /// # Arguments
    ///
    /// * `config` - Cassandra connection and TTL configuration
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] if:
    /// - Connection to Cassandra fails
    /// - Schema migration fails
    pub async fn new(config: &CassandraConfiguration) -> Result<Self, CassandraStoreError> {
        let session = Box::pin(create_session(config)).await?;

        // Run all migrations
        let migrator = CassandraMigrator::new(&session, &config.keyspace).await?;
        migrator.migrate().await?;

        let base_ttl = config.retention.try_into()?;

        Ok(Self {
            inner: Arc::new(Inner {
                session,
                propagator: new_propagator(),
                base_ttl,
            }),
        })
    }

    /// Returns a reference to the Cassandra session.
    ///
    /// This session is shared across all components and should be used
    /// for all database operations.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.inner.session
    }

    /// Returns a reference to the OpenTelemetry propagator.
    ///
    /// Used for distributed tracing context propagation in stored spans.
    #[must_use]
    pub fn propagator(&self) -> &TextMapCompositePropagator {
        &self.inner.propagator
    }

    /// Returns the base TTL duration for this store.
    ///
    /// This is the retention period configured for the store, used as
    /// the base for TTL calculations.
    #[must_use]
    pub fn base_ttl(&self) -> CompactDuration {
        self.inner.base_ttl
    }

    /// Calculates an appropriate TTL value for Cassandra.
    ///
    /// Computes a TTL by adding the base retention period to the time
    /// remaining until the target time, with overflow protection for
    /// Cassandra's maximum TTL limit.
    ///
    /// # Arguments
    ///
    /// * `target_time` - The target time for the data
    ///
    /// # Returns
    ///
    /// * `Some(ttl_seconds)` if a valid TTL can be calculated
    /// * `None` if the TTL would exceed Cassandra's limits or calculation fails
    #[must_use]
    pub fn calculate_ttl(&self, target_time: CompactDateTime) -> Option<i32> {
        const MAX_TTL: i32 = 630_720_000; // Cassandra's maximum TTL

        let Ok(duration) = target_time.compact_duration_from_now() else {
            // Return the base TTL if the time is in the past
            return self.base_ttl().seconds().try_into().ok();
        };

        duration
            .checked_add(self.base_ttl())
            .ok()?
            .seconds()
            .try_into()
            .ok()
            .filter(|&ttl| ttl < MAX_TTL)
    }
}

/// Creates and configures a Cassandra session with the given configuration.
async fn create_session(config: &CassandraConfiguration) -> Result<Session, CassandraStoreError> {
    let mut lb_policy = DefaultPolicy::builder()
        .token_aware(true)
        .permit_dc_failover(true);

    if let Some(dc) = &config.datacenter {
        lb_policy = match &config.rack {
            None => lb_policy.prefer_datacenter(dc.clone()),
            Some(rack) => lb_policy.prefer_datacenter_and_rack(dc.clone(), rack.clone()),
        }
    }

    let _profile = ExecutionProfile::builder()
        .consistency(Consistency::LocalQuorum)
        .load_balancing_policy(lb_policy.build())
        .retry_policy(Arc::new(DefaultRetryPolicy::new()));

    let mut session = SessionBuilder::new()
        .known_nodes(&config.nodes)
        .compression(Some(Compression::Lz4));

    if let Some(user) = &config.user {
        session = session.user(user.clone(), config.password.clone().unwrap_or_default());
    }

    Ok(session.build().await?)
}

/// Error type for Cassandra store operations.
#[derive(Debug, Error)]
pub enum CassandraStoreError {
    /// Invalid duration
    #[error("Invalid duration: {0:#}")]
    Duration(#[from] CompactDurationError),

    /// Failed to create Cassandra session.
    #[error("Failed to create session: {0:#}")]
    Session(Box<NewSessionError>),

    /// Schema migration failed.
    #[error("Migration failed: {0}")]
    Migration(String),

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

    /// Expected integer type but got something else.
    #[error("expected integer type")]
    IntExpected,
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

// Scylla trait implementations for Prosody types

impl SerializeValue for CompactDuration {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl SerializeValue for CompactDateTime {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i32::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDuration {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDuration::from(i32::deserialize(typ, v)?))
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CompactDateTime {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDateTime::from(i32::deserialize(typ, v)?))
    }
}
