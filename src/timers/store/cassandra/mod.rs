use crate::Key;
use crate::propagator::new_propagator;
use crate::timers::Trigger;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::{CompactDuration, CompactDurationError};
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::{Segment, SegmentId, TriggerStore};
use crate::util::{
    from_duration_env_with_fallback, from_env_with_fallback, from_option_env, from_vec_env,
};
use async_stream::try_stream;
use derive_builder::Builder;
use educe::Educe;
use futures::{Stream, TryStreamExt, pin_mut};
use humantime::Duration;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, DeserializeValue, FrameSlice, SerializationError,
    SerializeValue, TypeCheckError, WrittenCellProof,
};
use scylla::client::session::Session;
use scylla::cluster::metadata::NativeType;
use scylla::errors::{
    ExecutionError, IntoRowsResultError, MaybeFirstRowError, NewSessionError, NextRowError,
    PagerExecutionError, PrepareError, RowsError, UseKeyspaceError,
};
use std::collections::HashMap;
use std::error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::{info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use validator::Validate;

mod migrator;
mod queries;

/// Table name for storing segment metadata and slab IDs.
pub const TABLE_SEGMENTS: &str = "timer_segments";

/// Table name for storing timer triggers organized by time slabs.
pub const TABLE_SLABS: &str = "timer_slabs";

/// Table name for storing timer triggers indexed by key for efficient key-based
/// lookups.
pub const TABLE_KEYS: &str = "timer_keys";

/// Table name for tracking applied database migrations.
pub const TABLE_SCHEMA_MIGRATIONS: &str = "schema_migrations";

/// Table name for distributed migration locking.
pub const TABLE_LOCKS: &str = "locks";

/// Configuration for the Cassandra-based timer storage backend.
#[derive(Builder, Clone, Educe, Validate)]
#[educe(Debug)]
pub struct CassandraConfiguration {
    /// List of Cassandra contact nodes (hostnames or IPs).
    ///
    /// Environment variable: `PROSODY_CASSANDRA_NODES`
    /// Default: None (must be specified)
    ///
    /// At least one Cassandra node must be provided to establish an initial
    /// connection with the cluster. Multiple nodes improve failover behavior.
    #[builder(default = "from_vec_env(\"PROSODY_CASSANDRA_NODES\")?", setter(into))]
    #[validate(length(min = 1_u64))]
    pub nodes: Vec<String>,

    /// Keyspace to use for storing timer data.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_KEYSPACE`
    /// Default: `"prosody"`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_CASSANDRA_KEYSPACE\", \"prosody\".to_owned())?",
        setter(into)
    )]
    #[validate(length(min = 1_u64))]
    pub keyspace: String,

    /// Preferred datacenter for query routing.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_DATACENTER`
    /// Default: None
    #[builder(
        default = "from_option_env(\"PROSODY_CASSANDRA_DATACENTER\")?",
        setter(into)
    )]
    pub datacenter: Option<String>,

    /// Preferred rack identifier for topology-aware routing.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_RACK`
    /// Default: None
    #[builder(default = "from_option_env(\"PROSODY_CASSANDRA_RACK\")?", setter(into))]
    pub rack: Option<String>,

    /// Username for authenticating with Cassandra.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_USER`
    /// Default: None
    #[builder(default = "from_option_env(\"PROSODY_CASSANDRA_USER\")?", setter(into))]
    pub user: Option<String>,

    /// Password for authenticating with Cassandra.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_PASSWORD`
    /// Default: None
    #[builder(
        default = "from_option_env(\"PROSODY_CASSANDRA_PASSWORD\")?",
        setter(into)
    )]
    #[educe(Debug(ignore))]
    pub password: Option<String>,

    /// Retention period for failed/unprocessed timer data in Cassandra.
    ///
    /// This defines how long timer data remains available for timers that are
    /// not successfully processed (aborted or failed). Successfully committed
    /// timers are immediately deleted from storage and do not rely on this
    /// retention period.
    ///
    /// The retention period is added to the timer's execution time to calculate
    /// the Cassandra TTL, ensuring failed timers don't accumulate indefinitely.
    ///
    /// Structural metadata (e.g., segment definitions) may persist beyond this
    /// period and is not subject to TTL-based eviction.
    ///
    /// Environment variable: `PROSODY_CASSANDRA_RETENTION`
    /// Default: 30 days
    #[builder(
        default = "Duration::from(from_duration_env_with_fallback(\"PROSODY_CASSANDRA_RETENTION\", \
        std::time::Duration::from_secs(30 * 24 * 60 * 60))?)",
        setter(into)
    )]
    pub retention: Duration,
}

/// Cassandra-based implementation of [`TriggerStore`].
///
/// Provides persistent storage for timer triggers using Apache Cassandra
/// with automatic schema migration and optimized TTL management.
#[derive(Clone, Debug)]
pub struct CassandraTriggerStore(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    queries: Queries,
    propagator: TextMapCompositePropagator,
    base_ttl: CompactDuration,
}

impl CassandraTriggerStore {
    /// Creates a new Cassandra trigger store with the given configuration.
    ///
    /// Initializes the connection to Cassandra, runs schema migrations,
    /// and prepares all required queries.
    ///
    /// # Arguments
    ///
    /// * `config` - Cassandra connection and TTL configuration
    ///
    /// # Errors
    ///
    /// Returns [`CassandraTriggerStoreError`] if:
    /// - Connection to Cassandra fails
    /// - Schema migration fails
    /// - Query preparation fails
    pub async fn new(config: &CassandraConfiguration) -> Result<Self, CassandraTriggerStoreError> {
        let base_ttl: StdDuration = config.retention.into();
        let base_ttl = base_ttl.try_into()?;

        Ok(Self(Arc::new(Inner {
            queries: Box::pin(Queries::new(config.clone())).await?,
            propagator: new_propagator(),
            base_ttl,
        })))
    }

    fn session(&self) -> &Session {
        &self.0.queries.session
    }

    fn queries(&self) -> &Queries {
        &self.0.queries
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        &self.0.propagator
    }

    fn base_ttl(&self) -> CompactDuration {
        self.0.base_ttl
    }

    fn calculate_ttl(&self, time: CompactDateTime) -> Option<i32> {
        const MAX_TTL: i32 = 630_720_000;

        let Ok(duration) = time.compact_duration_from_now() else {
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

impl TriggerStore for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().insert_segment,
                (segment.id, segment.name, segment.slab_size),
            )
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_segment, (segment_id,))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(String, CompactDuration)>()?;

        let Some((name, slab_size)) = row else {
            return Ok(None);
        };

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
        }))
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries().delete_segment, (segment_id,))
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_slabs.clone(), (segment_id,))
                .await?
                .rows_stream::<(Option<i32>,)>()?;

            pin_mut!(stream);
            while let Some((value,)) = cooperative(stream.try_next()).await? {
                let Some(value) = value else {
                    continue;
                };

                yield SlabId::from_le_bytes(value.to_le_bytes())
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    // Note: This method handles a complex edge case due to the mismatch between
    // SlabId (u32) and the database storage (i32).
    //
    // Problem: SlabId is u32 (0 to 4,294,967,295) but Cassandra stores slab_id as
    // int (i32). When we convert u32 to i32 using byte reinterpretation:
    // - u32 values 0 to 2,147,483,647 → positive i32 values
    // - u32 values 2,147,483,648 to 4,294,967,295 → negative i32 values
    //
    // This causes "wrap-around" where a range like [2,147,483,640, 2,147,483,660]
    // becomes [2,147,483,640, -2,147,483,636] in signed representation.
    // A single SQL query "WHERE slab_id >= start AND slab_id <= end" fails when
    // start > end.
    //
    // Solution: Detect wrap-around and split into two queries:
    // 1. slab_id >= start AND slab_id <= i32::MAX (for low u32 values, positive
    //    i32)
    // 2. slab_id >= i32::MIN AND slab_id <= end (for high u32 values, negative i32)
    fn get_slab_range(
        &self,
        segment_id: &SegmentId,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        try_stream! {
            // First, validate that this is a proper range in u32 space
            // If start > end in u32 terms, this is an invalid range and we should return
            // nothing
            if range.start() > range.end() {
                // Invalid range - return empty stream
                return;
            }

            let start = i32::from_le_bytes(range.start().to_le_bytes());
            let end = i32::from_le_bytes(range.end().to_le_bytes());

            // Detect wrap-around: start > end in signed representation means the range
            // crosses the u32/i32 boundary (around 2^31), not that it's an invalid range
            if start > end {
                // Wrap-around case: split into two queries to cover the full range

                // Query 1: Handle the "low" u32 values that remain as positive i32 values
                // This covers slab_id >= start up to the maximum i32 value
                let stream1 = self
                    .session()
                    .execute_iter(
                        self.queries().get_slab_range.clone(),
                        (segment_id, start, i32::MAX),
                    )
                    .await?
                    .rows_stream::<(Option<i32>,)>()?;

                pin_mut!(stream1);
                while let Some((value,)) = cooperative(stream1.try_next()).await? {
                    let Some(value) = value else {
                        continue;
                    };

                    yield SlabId::from_le_bytes(value.to_le_bytes())
                }

                // Query 2: Handle the "high" u32 values that appear as negative i32 values
                // This covers from minimum i32 value up to slab_id <= end
                let stream2 = self
                    .session()
                    .execute_iter(
                        self.queries().get_slab_range.clone(),
                        (segment_id, i32::MIN, end),
                    )
                    .await?
                    .rows_stream::<(Option<i32>,)>()?;

                pin_mut!(stream2);
                while let Some((value,)) = cooperative(stream2.try_next()).await? {
                    let Some(value) = value else {
                        continue;
                    };

                    yield SlabId::from_le_bytes(value.to_le_bytes())
                }
            } else {
                // Normal case: no wrap-around, single query is sufficient
                // Both start and end have the same sign in i32 representation
                let stream = self
                    .session()
                    .execute_iter(
                        self.queries().get_slab_range.clone(),
                        (segment_id, start, end),
                    )
                    .await?
                    .rows_stream::<(Option<i32>,)>()?;

                pin_mut!(stream);
                while let Some((value,)) = cooperative(stream.try_next()).await? {
                    let Some(value) = value else {
                        continue;
                    };

                    yield SlabId::from_le_bytes(value.to_le_bytes())
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab(&self, segment_id: &SegmentId, slab: Slab) -> Result<(), Self::Error> {
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        match self.calculate_ttl(slab.range().end) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(&self.queries().insert_slab, (segment_id, slab_id, ttl))
                    .await?;
            }
            None => {
                self.session()
                    .execute_unpaged(&self.queries().insert_slab_no_ttl, (segment_id, slab_id))
                    .await?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab,
                (segment_id, i32::from_le_bytes(slab_id.to_le_bytes())),
            )
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = slab.segment_id();
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_slab_triggers.clone(), (segment_id, slab_id))
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger");
                span.set_parent(context);

                yield Trigger {
                    key: key.into(),
                    time,
                    span,
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator()
            .inject_context(&trigger.span.context(), &mut span_map);

        let segment_id = slab.segment_id();
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
        let key = trigger.key.as_str();
        let time = trigger.time;

        match self.calculate_ttl(slab.range().end) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_slab_trigger,
                        (segment_id, slab_id, key, time, span_map, ttl),
                    )
                    .await?;
            }
            None => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_slab_trigger_no_ttl,
                        (segment_id, slab_id, key, time, span_map),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab_trigger,
                (
                    slab.segment_id(),
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                    key.as_str(),
                    time,
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_slab_triggers,
                (
                    slab.segment_id(),
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_key_times.clone(), (segment_id, key.as_str()))
                .await?
                .rows_stream::<(CompactDateTime,)>()?;

            pin_mut!(stream);
            while let Some((time,)) = cooperative(stream.try_next()).await? {
                yield time
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(self.queries().get_key_triggers.clone(), (segment_id, key.as_str()))
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger");
                span.set_parent(context);

                yield Trigger {
                    key: key.into(),
                    time,
                    span,
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator()
            .inject_context(&trigger.span.context(), &mut span_map);

        let key = trigger.key.as_str();
        let time = trigger.time;

        match self.calculate_ttl(trigger.time) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_key_trigger,
                        (segment_id, key, time, span_map, ttl),
                    )
                    .await?;
            }
            None => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_key_trigger_no_ttl,
                        (segment_id, key, time, span_map),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_key_trigger,
                (segment_id, key.as_str(), time),
            )
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_key_triggers,
                (segment_id, key.as_str()),
            )
            .await?;

        Ok(())
    }
}

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
            _ => Err(TypeCheckError::new(InnerError::IntExpected)),
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
            _ => Err(TypeCheckError::new(InnerError::IntExpected)),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        Ok(CompactDateTime::from(i32::deserialize(typ, v)?))
    }
}

/// Internal error types for Cassandra operations.
#[derive(Debug, Error)]
pub enum InnerError {
    /// Invalid duration
    #[error(transparent)]
    Duration(#[from] CompactDurationError),

    /// Failed to create Cassandra session.
    #[error(transparent)]
    Session(#[from] NewSessionError),

    /// Schema migration failed.
    #[error("migration failed: {0:#}")]
    Migration(String),

    /// Failed to set keyspace.
    #[error("failed to set keyspace: {0:#}")]
    UseKeyspace(#[from] UseKeyspaceError),

    /// Failed to prepare statement.
    #[error("failed to prepare statement: {0:#}")]
    Prepare(#[from] PrepareError),

    /// Failed to execute statement.
    #[error("failed to execute statement: {0:#}")]
    Execution(#[from] ExecutionError),

    /// Invalid column type.
    #[error("invalid type: {0:#}")]
    TypeCheck(#[from] TypeCheckError),

    /// Failed to retrieve the next row.
    #[error("failed to retrieve the next row: {0:#}")]
    NextRow(#[from] NextRowError),

    /// Failed to retrieve the next page.
    #[error("failed to retrieve the next page: {0:#}")]
    PageExecution(#[from] PagerExecutionError),

    /// Failed to retrieve the first row.
    #[error("failed to retrieve the first row: {0:#}")]
    MaybeFirstRow(#[from] MaybeFirstRowError),

    /// Failed to get rows.
    #[error("failed to get rows: {0:#}")]
    IntoRows(#[from] IntoRowsResultError),

    /// Rows error.
    #[error("rows error: {0:#}")]
    Rows(#[from] RowsError),

    /// Expected integer type but got something else.
    #[error("expected and integer type")]
    IntExpected,

    /// Segment not found in storage.
    #[error("segment not found")]
    SegmentNotFound,

    /// Failed to calculate TTL duration.
    #[error("failed to calculate TTL duration")]
    TtlCalculationFailed,

    /// TTL calculation overflow.
    #[error("TTL calculation overflow")]
    TtlOverflow,
}

/// Error type for Cassandra trigger store operations.
pub struct CassandraTriggerStoreError(Box<InnerError>);

impl<E> From<E> for CassandraTriggerStoreError
where
    InnerError: From<E>,
{
    fn from(err: E) -> Self {
        Self(Box::new(InnerError::from(err)))
    }
}

impl Debug for CassandraTriggerStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.0, f)
    }
}

impl Display for CassandraTriggerStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.0, f)
    }
}

impl error::Error for CassandraTriggerStoreError {}

#[cfg(test)]
mod test {
    use super::{CassandraConfiguration, CassandraTriggerStore};
    use crate::timers::duration::CompactDuration;
    use crate::timers::slab::{Slab, SlabId};
    use crate::timers::store::{Segment, SegmentId, TriggerStore};
    use crate::trigger_store_tests;
    use color_eyre::Result;
    use futures::pin_mut;
    use futures::stream::StreamExt;
    use std::collections::HashSet;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use uuid::Uuid;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    trigger_store_tests!(
        CassandraTriggerStore,
        CassandraTriggerStore::new(&CassandraConfiguration {
            datacenter: None,
            rack: None,
            nodes: vec!["localhost:9042".to_owned()],
            keyspace: "prosody".to_owned(),
            user: None,
            password: None,
            retention: Duration::from_secs(10 * 60).into(),
        }),
        25
    );

    #[tokio::test]
    async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
        let store = CassandraTriggerStore::new(&CassandraConfiguration {
            datacenter: None,
            rack: None,
            nodes: vec!["localhost:9042".to_owned()],
            keyspace: "prosody_test".to_owned(),
            user: None,
            password: None,
            retention: Duration::from_secs(10 * 60).into(),
        })
        .await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "test_segment".to_owned(),
            slab_size: CompactDuration::new(60), // 1 minute slabs
        };

        // Insert the test segment
        store.insert_segment(segment.clone()).await?;

        // Test SlabId values that will cause wrap-around issues
        let boundary = 2_147_483_648u32; // 2^31, becomes negative in i32
        let test_slab_ids = vec![
            boundary - 2,    // 2147483646 -> positive i32
            boundary - 1,    // 2147483647 -> i32::MAX
            boundary,        // 2147483648 -> i32::MIN (negative)
            boundary + 1,    // 2147483649 -> negative i32
            SlabId::MAX - 1, // 4294967294 -> negative i32
            SlabId::MAX,     // 4294967295 -> -1 in i32
        ];

        // Insert test slabs
        for &slab_id in &test_slab_ids {
            let slab = Slab::new(segment_id, slab_id, segment.slab_size);
            store.insert_slab(&segment_id, slab).await?;
        }

        // Test Case 1: Range that crosses the wrap-around boundary
        let cross_boundary_range = RangeInclusive::new(boundary - 1, boundary + 1);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, cross_boundary_range)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashSet<_>, _>>()?;

        let expected: HashSet<SlabId> = vec![boundary - 1, boundary, boundary + 1]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "Cross-boundary range failed");

        // Test Case 2: Range entirely in "negative" i32 space (high u32 values)
        let high_range = RangeInclusive::new(boundary, SlabId::MAX);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, high_range)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashSet<_>, _>>()?;

        let expected: HashSet<SlabId> = vec![boundary, boundary + 1, SlabId::MAX - 1, SlabId::MAX]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "High range (negative i32) failed");

        // Test Case 3: Range entirely in "positive" i32 space (low u32 values)
        let low_range = RangeInclusive::new(boundary - 2, boundary - 1);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, low_range)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashSet<_>, _>>()?;

        let expected: HashSet<SlabId> = vec![boundary - 2, boundary - 1].into_iter().collect();
        assert_eq!(result, expected, "Low range (positive i32) failed");

        // Test Case 4: Single element at boundary
        let single_boundary_range = RangeInclusive::new(boundary, boundary);
        let result: Vec<SlabId> = store
            .get_slab_range(&segment_id, single_boundary_range)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(result, vec![boundary], "Single boundary element failed");

        // Test Case 5: Invalid range (start > end in u32 space)
        let invalid_range = RangeInclusive::new(SlabId::MAX - 1, boundary - 2);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, invalid_range)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashSet<_>, _>>()?;

        let expected: HashSet<SlabId> = HashSet::new();
        assert_eq!(result, expected, "Invalid range should return empty set");

        // Cleanup
        store.delete_segment(&segment_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_wrap_around() -> Result<()> {
        let store = CassandraTriggerStore::new(&CassandraConfiguration {
            datacenter: None,
            rack: None,
            nodes: vec!["localhost:9042".to_owned()],
            keyspace: "prosody_test_simple".to_owned(),
            user: None,
            password: None,
            retention: Duration::from_secs(10 * 60).into(),
        })
        .await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "simple_test".to_owned(),
            slab_size: CompactDuration::new(60),
        };

        store.insert_segment(segment.clone()).await?;

        // The critical boundary: 2^31 = 2,147,483,648
        // Values below this are positive i32, values at/above are negative i32
        let boundary = 2_147_483_648u32;
        let test_ids = vec![boundary - 1, boundary, boundary + 1];

        // Insert test slabs
        for &slab_id in &test_ids {
            let slab = Slab::new(segment_id, slab_id, segment.slab_size);
            store.insert_slab(&segment_id, slab).await?;
        }

        // Test the critical range that crosses the wrap-around boundary
        let wrap_range = RangeInclusive::new(boundary - 1, boundary + 1);
        let mut results = Vec::new();

        let stream = store.get_slab_range(&segment_id, wrap_range);
        pin_mut!(stream);
        while let Some(result) = stream.next().await {
            results.push(result?);
        }

        // Sort results for consistent comparison
        results.sort_unstable();
        let mut expected = test_ids.clone();
        expected.sort_unstable();

        assert_eq!(results, expected, "Wrap-around range query failed");

        // Cleanup
        store.delete_segment(&segment_id).await?;

        Ok(())
    }
}
