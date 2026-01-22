use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::error::ParseError;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{InvalidSegmentVersionError, Segment, SegmentId, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::{debug, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod queries;

/// V1 schema operations (internal, Cassandra-only).
pub(crate) mod v1;

/// Migration utilities for V1→V2 and slab size changes (internal,
/// Cassandra-only).
pub(crate) mod migration;

/// Cassandra-based implementation of [`TriggerStore`](super::TriggerStore).
///
/// Provides persistent storage for timer triggers using Apache Cassandra
/// with automatic schema migration and optimized TTL management.
#[derive(Clone, Debug)]
pub struct CassandraTriggerStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    slab_size: CompactDuration,
}

impl CassandraTriggerStore {
    /// Creates a new Cassandra trigger store using an existing
    /// `CassandraStore`.
    ///
    /// This allows sharing a single Cassandra session across multiple stores
    /// (e.g., trigger store and defer store), avoiding the creation of multiple
    /// sessions which is not allowed.
    ///
    /// # Arguments
    ///
    /// * `store` - Existing `CassandraStore` to share
    /// * `keyspace` - Cassandra keyspace name for query preparation
    /// * `slab_size` - Target slab size for automatic migration
    ///
    /// # Errors
    ///
    /// Returns error if query preparation fails.
    pub async fn with_store(
        store: CassandraStore,
        keyspace: &str,
        slab_size: CompactDuration,
    ) -> Result<Self, CassandraTriggerStoreError> {
        let queries = Arc::new(Queries::new(store.session(), keyspace).await?);

        Ok(Self {
            store,
            queries,
            slab_size,
        })
    }

    fn session(&self) -> &Session {
        self.store.session()
    }

    fn queries(&self) -> &Queries {
        &self.queries
    }

    fn propagator(&self) -> &TextMapCompositePropagator {
        self.store.propagator()
    }

    fn calculate_ttl(&self, time: CompactDateTime) -> Option<i32> {
        self.store.calculate_ttl(time)
    }

    /// Helper to execute a query conditionally based on TTL.
    ///
    /// Executes `query_with_ttl` if TTL is available, otherwise executes
    /// `query_no_ttl`. The `params_with_ttl` builder receives the TTL value.
    async fn execute_with_optional_ttl<P1, P2>(
        &self,
        time: CompactDateTime,
        query_with_ttl: &PreparedStatement,
        query_no_ttl: &PreparedStatement,
        params_with_ttl: impl FnOnce(i32) -> P1,
        params_no_ttl: impl FnOnce() -> P2,
    ) -> Result<(), CassandraTriggerStoreError>
    where
        P1: SerializeRow,
        P2: SerializeRow,
    {
        match self.calculate_ttl(time) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(query_with_ttl, params_with_ttl(ttl))
                    .await
                    .map_err(CassandraStoreError::from)?;
            }
            None => {
                self.session()
                    .execute_unpaged(query_no_ttl, params_no_ttl())
                    .await
                    .map_err(CassandraStoreError::from)?;
            }
        }
        Ok(())
    }

    /// Creates V1 operations on-demand for migration support.
    ///
    /// This method constructs a `V1Operations` instance that provides access
    /// to V1 schema operations (reading/writing data without `timer_type`).
    /// Only used during V1→V2 migration.
    pub(crate) fn v1(&self) -> v1::V1Operations {
        v1::V1Operations::new(self.store.clone(), Arc::clone(&self.queries))
    }

    /// Reads a segment from the database without applying any migrations.
    ///
    /// This is an internal helper used during migration to reload segments
    /// after version or slab size updates, avoiding infinite recursion.
    async fn get_segment_unchecked(
        &self,
        segment_id: &SegmentId,
    ) -> Result<Option<Segment>, CassandraTriggerStoreError> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_segment, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(String, CompactDuration, Option<i8>)>()
            .map_err(CassandraStoreError::from)?;

        let Some((name, slab_size, version)) = row else {
            return Ok(None);
        };

        let version = version
            .map(SegmentVersion::try_from)
            .transpose()?
            .unwrap_or(SegmentVersion::V1);

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
            version,
        }))
    }
}

impl TriggerOperations for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
        // Validate that the segment's slab_size matches the configured slab_size
        if segment.slab_size != self.slab_size {
            return Err(CassandraTriggerStoreError::SlabSizeMismatch {
                segment_id: segment.id,
                segment_slab_size: segment.slab_size,
                configured_slab_size: self.slab_size,
            });
        }

        self.session()
            .execute_unpaged(
                &self.queries().insert_segment,
                (
                    segment.id,
                    segment.name,
                    segment.slab_size,
                    i8::from(segment.version),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_segment(&self, segment_id: &SegmentId) -> Result<Option<Segment>, Self::Error> {
        let Some(segment) = self.get_segment_unchecked(segment_id).await? else {
            return Ok(None);
        };

        let segment = migration::migrate_segment_if_needed(self, segment, self.slab_size).await?;

        Ok(Some(segment))
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_segment(&self, segment_id: &SegmentId) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries().delete_segment, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?;

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
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(Option<i32>,)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((value,)) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)?
            {
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
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream::<(Option<i32>,)>()
                    .map_err(CassandraStoreError::from)?;

                pin_mut!(stream1);
                while let Some((value,)) = cooperative(stream1.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
                {
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
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream::<(Option<i32>,)>()
                    .map_err(CassandraStoreError::from)?;

                pin_mut!(stream2);
                while let Some((value,)) = cooperative(stream2.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
                {
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
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream::<(Option<i32>,)>()
                    .map_err(CassandraStoreError::from)?;

                pin_mut!(stream);
                while let Some((value,)) = cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
                {
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

        self.execute_with_optional_ttl(
            slab.range().end,
            &self.queries().insert_slab,
            &self.queries().insert_slab_no_ttl,
            |ttl| (segment_id, slab_id, ttl),
            || (segment_id, slab_id),
        )
        .await
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
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = slab.segment_id();
        let slab_size = slab.size().seconds() as i32;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
        let timer_type_i8 = i8::from(timer_type);

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_slab_triggers.clone(),
                    (segment_id, slab_size, slab_id, timer_type_i8),
                )
                .await.map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>().map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) =
                cooperative(stream.try_next()).await.map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned)?;

                yield Trigger::new(key.into(), time, timer_type, span);
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = slab.segment_id();
        let slab_size = slab.size().seconds() as i32;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_slab_triggers_all_types.clone(),
                    (segment_id, slab_size, slab_id),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger_all_types");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned)?;
                yield Trigger::new(key.into(), time, timer_type, span);
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        let context = trigger.span.load().context();
        self.propagator().inject_context(&context, &mut span_map);

        let segment_id = slab.segment_id();
        let slab_size = slab.size().seconds() as i32;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
        let key = trigger.key.as_ref();
        let time = trigger.time;
        let timer_type = i8::from(trigger.timer_type);

        self.execute_with_optional_ttl(
            slab.range().end,
            &self.queries().insert_slab_trigger,
            &self.queries().insert_slab_trigger_no_ttl,
            |ttl| {
                (
                    segment_id, slab_size, slab_id, timer_type, key, time, &span_map, ttl,
                )
            },
            || {
                (
                    segment_id, slab_size, slab_id, timer_type, key, time, &span_map,
                )
            },
        )
        .await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab_trigger,
                (
                    slab.segment_id(),
                    slab.size().seconds() as i32,
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                    i8::from(timer_type),
                    key.as_ref(),
                    time,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_slab_triggers(&self, slab: &Slab) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_slab_triggers,
                (
                    slab.segment_id(),
                    slab.size().seconds() as i32,
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_times(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_key_times.clone(),
                    (segment_id, key.as_ref(), i8::from(timer_type)),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(CompactDateTime,)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((time,)) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)?
            {
                yield time
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_key_triggers.clone(),
                    (segment_id, key.as_ref(), i8::from(timer_type)),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned)?;
                yield Trigger::new(key.into(), time, timer_type, span);
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_key_triggers_all_types.clone(),
                    (segment_id, key.as_ref()),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger_all_types");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned)?;
                yield Trigger::new(key.into(), time, timer_type, span);
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
        let context = trigger.span.load().context();
        self.propagator().inject_context(&context, &mut span_map);

        let key = trigger.key.as_ref();
        let time = trigger.time;
        let timer_type = i8::from(trigger.timer_type);

        self.execute_with_optional_ttl(
            trigger.time,
            &self.queries().insert_key_trigger,
            &self.queries().insert_key_trigger_no_ttl,
            |ttl| (segment_id, key, timer_type, time, &span_map, ttl),
            || (segment_id, key, timer_type, time, &span_map),
        )
        .await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_key_trigger,
                (segment_id, key.as_ref(), i8::from(timer_type), time),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_key_triggers,
                (segment_id, key.as_ref(), i8::from(timer_type)),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_key_triggers_all_types,
                (segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    // -- V1 migration methods --

    /// Updates the segment's version field after v1 to v2 migration.
    #[instrument(level = "debug", skip(self), err)]
    async fn update_segment_version(
        &self,
        segment_id: &SegmentId,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().update_segment_version,
                (
                    i8::from(new_version),
                    new_slab_size.seconds() as i32,
                    segment_id,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }
}

/// Creates a new Cassandra trigger store.
///
/// Returns an implementation of `TriggerStore` backed by Apache Cassandra.
/// This is the recommended way to create a Cassandra store.
///
/// # Arguments
///
/// * `config` - Cassandra connection and TTL configuration
/// * `slab_size` - Target slab size for automatic migration
///
/// # Errors
///
/// Returns [`CassandraTriggerStoreError`] if:
/// - Connection to Cassandra fails
/// - Schema migration fails
/// - Query preparation fails
///
/// # Example
///
/// ```rust,ignore
/// let config = CassandraConfiguration { ... };
/// let slab_size = CompactDuration::new(3600);
/// let store = cassandra_store(&config, slab_size).await?;
/// let manager = TimerManager::new(..., store);
/// ```
pub async fn cassandra_store(
    config: &CassandraConfiguration,
    slab_size: CompactDuration,
) -> Result<TableAdapter<CassandraTriggerStore>, CassandraTriggerStoreError> {
    let store = CassandraStore::new(config).await?;
    let cassandra = CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await?;
    Ok(TableAdapter::new(cassandra))
}

/// Errors that can occur during Cassandra trigger store operations.
#[derive(Debug, Error)]
pub enum CassandraTriggerStoreError {
    /// Database error
    #[error("database error: {0:#}")]
    Database(#[from] CassandraStoreError),

    /// Invalid segment version value.
    #[error("Invalid segment version: {0:#}")]
    InvalidSegmentVersion(#[from] InvalidSegmentVersionError),

    /// Slab size mismatch during segment insertion.
    #[error(
        "Cannot insert segment {segment_id} with slab_size {segment_slab_size} that differs from \
         configured slab_size {configured_slab_size}"
    )]
    SlabSizeMismatch {
        /// The ID of the segment being inserted.
        segment_id: SegmentId,
        /// The slab size of the segment being inserted.
        segment_slab_size: CompactDuration,
        /// The configured slab size for this store.
        configured_slab_size: CompactDuration,
    },

    /// Segment disappeared during data migration.
    #[error("Segment {segment_id} disappeared during {operation}")]
    SegmentDisappeared {
        /// The ID of the segment that disappeared.
        segment_id: SegmentId,
        /// The operation that was being performed when the segment disappeared.
        operation: &'static str,
    },

    /// Invalid timer type value in database.
    #[error("Invalid timer type: {0:#}")]
    Parse(#[from] ParseError),
}

impl ClassifyError for CassandraTriggerStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::InvalidSegmentVersion(e) => e.classify_error(),

            // Attempting to insert segment with slab_size different from store's configured
            // slab_size. Configuration mismatch that prevents ALL insertions of mismatched
            // segments. Requires configuration or data fix.
            Self::SlabSizeMismatch { .. } => ErrorCategory::Terminal,

            // Segment disappeared during data migration, likely due to concurrent deletion or
            // race condition. Retrying might succeed if segment reappears or operation completes.
            Self::SegmentDisappeared { .. } => ErrorCategory::Transient,

            Self::Parse(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{CassandraConfiguration, CassandraTriggerStore, cassandra_store};
    use crate::cassandra::CassandraStore;
    use crate::timers::duration::CompactDuration;
    use crate::timers::slab::{Slab, SlabId};
    use crate::timers::store::operations::TriggerOperations;
    use crate::timers::store::{Segment, SegmentId, SegmentVersion};
    use crate::tracing::init_test_logging;
    use crate::trigger_store_tests;
    use color_eyre::Result;
    use futures::TryStreamExt;
    use futures::pin_mut;
    use futures::stream::StreamExt;
    use std::collections::HashSet;
    use std::env;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use uuid::Uuid;

    /// Creates a test configuration for Cassandra integration tests.
    fn test_cassandra_config(keyspace: &str) -> CassandraConfiguration {
        CassandraConfiguration {
            datacenter: None,
            rack: None,
            nodes: vec!["localhost:9042".to_owned()],
            keyspace: keyspace.to_owned(),
            user: None,
            password: None,
            retention: Duration::from_secs(10 * 60),
        }
    }

    // Determine the number of tests to run from an environment variable,
    // defaulting to 25 if the variable is not set or invalid.
    // Uses INTEGRATION_TESTS since these tests hit a real Cassandra database.
    fn get_test_count() -> u64 {
        env::var("INTEGRATION_TESTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(25)
    }

    // Run the full suite of TriggerStore compliance tests on this implementation.
    // Low-level tests use CassandraTriggerStore directly
    // High-level tests use TableAdapter<CassandraTriggerStore>
    trigger_store_tests!(
        CassandraTriggerStore,
        |slab_size| async move {
            let config = test_cassandra_config("prosody_test");
            let store = CassandraStore::new(&config).await?;
            CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await
        },
        crate::timers::store::adapter::TableAdapter<CassandraTriggerStore>,
        |slab_size| async move {
            let config = test_cassandra_config("prosody_test");
            cassandra_store(&config, slab_size).await
        },
        get_test_count()
    );

    #[tokio::test]
    async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60); // 1 minute slabs
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "test_segment".to_owned(),
            slab_size,
            version: SegmentVersion::V1,
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
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = vec![boundary - 1, boundary, boundary + 1]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "Cross-boundary range failed");

        // Test Case 2: Range entirely in "negative" i32 space (high u32 values)
        let high_range = RangeInclusive::new(boundary, SlabId::MAX);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, high_range)
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = vec![boundary, boundary + 1, SlabId::MAX - 1, SlabId::MAX]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "High range (negative i32) failed");

        // Test Case 3: Range entirely in "positive" i32 space (low u32 values)
        let low_range = RangeInclusive::new(boundary - 2, boundary - 1);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, low_range)
            .try_collect()
            .await?;

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
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = HashSet::new();
        assert_eq!(result, expected, "Invalid range should return empty set");

        // Cleanup
        store.delete_segment(&segment_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_wrap_around() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "simple_test".to_owned(),
            slab_size,
            version: SegmentVersion::V1,
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
