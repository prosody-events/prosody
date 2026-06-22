//! Cassandra infrastructure for all Prosody components.
//!
//! This module provides the foundational Cassandra connectivity, configuration,
//! and migration system used by all stateful components in Prosody. It manages
//! a single session and unified migration system.

use crate::propagator::new_propagator;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::SegmentVersion;
use opentelemetry::propagation::TextMapCompositePropagator;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::client::Compression;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::DefaultRetryPolicy;
use scylla::policies::timestamp_generator::MonotonicTimestampGenerator;
use scylla::serialize::SerializationError;
use scylla::serialize::row::SerializeRow;
use scylla::serialize::value::SerializeValue;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

pub mod config;
pub mod errors;
pub mod macros;
pub mod migrator;

pub use config::CassandraConfiguration;
use errors::CassandraStoreError;
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

/// Table name for storing defer segment metadata.
pub const TABLE_DEFERRED_SEGMENTS: &str = "deferred_segments";

/// Table name for storing deferred offsets awaiting retry.
pub const TABLE_DEFERRED_OFFSETS: &str = "deferred_offsets";

/// Table name for storing deferred timers awaiting retry.
pub const TABLE_DEFERRED_TIMERS: &str = "deferred_timers";

/// Table name for message deduplication records.
pub const TABLE_DEDUPLICATION: &str = "deduplication";

/// Table name for keyed-state cell-store collection partitions.
pub const TABLE_KEYED_STATE_CELL: &str = "keyed_state_cell";

/// Table for the frozen group-global keyed-state descriptor identities.
pub const TABLE_KEYED_STATE_IDENTITY: &str = "keyed_state_identity";

/// Cassandra's maximum TTL in seconds (~20 years).
pub const MAX_CASSANDRA_TTL_SECS: i64 = 630_720_000;

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
            .filter(|&ttl: &i32| i64::from(ttl) < MAX_CASSANDRA_TTL_SECS)
    }

    /// Executes an unpaged mutation and discards the result.
    ///
    /// Fire-and-forget wrapper for mutations that need only error
    /// propagation; shared by the timer and keyed-state stores.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] when the driver fails to execute.
    pub async fn execute_unpaged_discard<P>(
        &self,
        query: &PreparedStatement,
        params: P,
    ) -> Result<(), CassandraStoreError>
    where
        P: SerializeRow,
    {
        self.session()
            .execute_unpaged(query, params)
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    /// Executes `query_with_ttl` when `ttl` is `Some`, otherwise
    /// `query_no_ttl`, building each statement's params on demand.
    ///
    /// `ttl` is the already-resolved TTL in seconds; `None` means indefinite
    /// retention and routes to the `*_no_ttl` query variant. The two param
    /// builders let each query carry a different parameter shape — the
    /// `with_ttl` tuple leads with the TTL, the `no_ttl` tuple omits it.
    ///
    /// # Errors
    ///
    /// Returns [`CassandraStoreError`] when the driver fails to execute.
    pub async fn execute_with_optional_ttl<P1, P2, F1, F2>(
        &self,
        ttl: Option<i32>,
        query_with_ttl: &PreparedStatement,
        query_no_ttl: &PreparedStatement,
        params_with_ttl: F1,
        params_no_ttl: F2,
    ) -> Result<(), CassandraStoreError>
    where
        P1: SerializeRow,
        P2: SerializeRow,
        F1: FnOnce(i32) -> P1,
        F2: FnOnce() -> P2,
    {
        match ttl {
            Some(ttl) => {
                self.execute_unpaged_discard(query_with_ttl, params_with_ttl(ttl))
                    .await
            }
            None => {
                self.execute_unpaged_discard(query_no_ttl, params_no_ttl())
                    .await
            }
        }
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

    let profile = ExecutionProfile::builder()
        .consistency(Consistency::LocalQuorum)
        .load_balancing_policy(lb_policy.build())
        .retry_policy(Arc::new(DefaultRetryPolicy::new()))
        .build();

    let mut session = SessionBuilder::new()
        .known_nodes(&config.nodes)
        .compression(Some(Compression::Lz4))
        .default_execution_profile_handle(profile.into_handle())
        .timestamp_generator(Arc::new(MonotonicTimestampGenerator::new()));

    if let Some(user) = &config.user {
        session = session.user(user.clone(), config.password.clone().unwrap_or_default());
    }

    Ok(Box::pin(session.build()).await?)
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

impl SerializeValue for TimerType {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for TimerType {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::TinyInt) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::TinyIntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = i8::deserialize(typ, v)?;
        TimerType::try_from(value).map_err(DeserializationError::new)
    }
}

impl SerializeValue for SegmentVersion {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        i8::from(*self).serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for SegmentVersion {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Native(NativeType::TinyInt) => Ok(()),
            _ => Err(TypeCheckError::new(CassandraStoreError::TinyIntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = i8::deserialize(typ, v)?;
        SegmentVersion::try_from(value).map_err(DeserializationError::new)
    }
}
