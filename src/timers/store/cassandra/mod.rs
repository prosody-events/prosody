use crate::Key;
use crate::cassandra::{CassandraConfiguration, CassandraStore, CassandraStoreError};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore, TriggerV1};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use scylla::client::session::Session;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use tokio::task::coop::cooperative;
use tracing::{debug_span, error, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod queries;

/// Cassandra-based implementation of [`TriggerStore`].
///
/// Provides persistent storage for timer triggers using Apache Cassandra
/// with automatic schema migration and optimized TTL management.
#[derive(Clone, Debug)]
pub struct CassandraTriggerStore {
    store: CassandraStore,
    queries: Arc<Queries>,
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
        let store = CassandraStore::new(config).await?;
        let queries = Arc::new(Queries::new(store.session(), &config.keyspace).await?);

        Ok(Self { store, queries })
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
}

impl TriggerStore for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_segment(&self, segment: Segment) -> Result<(), Self::Error> {
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
            .maybe_first_row::<(String, CompactDuration, Option<i8>)>()?;

        let Some((name, slab_size, version)) = row else {
            return Ok(None);
        };

        let version = version
            .map(SegmentVersion::try_from)
            .transpose()?
            .unwrap_or_default();

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
            version,
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
                .await?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned).unwrap_or(TimerType::Application);
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

        match self.calculate_ttl(slab.range().end) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_slab_trigger,
                        (
                            segment_id, slab_size, slab_id, timer_type, key, time, span_map, ttl,
                        ),
                    )
                    .await?;
            }
            None => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_slab_trigger_no_ttl,
                        (
                            segment_id, slab_size, slab_id, timer_type, key, time, span_map,
                        ),
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
                    slab.size().seconds() as i32,
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
                .await?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = debug_span!("fetch_key_trigger");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned).unwrap_or(TimerType::Application);
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
                .await?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger_all_types");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned).unwrap_or(TimerType::Application);
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
                .await?
                .rows_stream::<(String, CompactDateTime, i8, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, timer_type_returned, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = debug_span!("fetch_key_trigger_all_types");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                let timer_type = TimerType::try_from(timer_type_returned).unwrap_or(TimerType::Application);
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

        match self.calculate_ttl(trigger.time) {
            Some(ttl) => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_key_trigger,
                        (segment_id, key, timer_type, time, span_map, ttl),
                    )
                    .await?;
            }
            None => {
                self.session()
                    .execute_unpaged(
                        &self.queries().insert_key_trigger_no_ttl,
                        (segment_id, key, timer_type, time, span_map),
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
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_key_trigger,
                (segment_id, key.as_ref(), i8::from(timer_type), time),
            )
            .await?;

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
            .await?;

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
            .await?;

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
            .await?;

        Ok(())
    }

    async fn insert_slab_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(&self.queries().insert_slab_v1, (segment_id, slab_id as i32))
            .await?;
        Ok(())
    }

    async fn insert_slab_trigger_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        trigger: TriggerV1,
    ) -> Result<(), Self::Error> {
        // Extract span context from tracing::Span
        // For v1 test data, we use an empty span map since this is test-only
        let span_map: HashMap<String, String> = HashMap::new();

        self.session()
            .execute_unpaged(
                &self.queries().insert_slab_trigger_v1,
                (
                    segment_id,
                    slab_id as i32,
                    trigger.key.as_ref(),
                    trigger.time,
                    span_map,
                ),
            )
            .await?;
        Ok(())
    }

    /// Streams slab IDs from v1 schema.
    ///
    /// Enumerates active v1 slabs for a segment.
    ///
    /// Queries the segments table which stores one row per active slab.
    ///
    /// Note: Cassandra returns NULL for clustering columns when a partition has
    /// only static columns. We handle this by deserializing to `Option<i32>`
    /// and filtering.
    #[instrument(level = "debug", skip(self), fields(segment_id = %segment_id))]
    fn get_slabs_v1(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        let segment_id = *segment_id;

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_slabs_v1.clone(),
                    (segment_id,),
                )
                .await?
                .rows_stream::<(Option<i32>,)>()?;

            pin_mut!(stream);
            while let Some((slab_id_opt,)) = cooperative(stream.try_next()).await? {
                // Filter out NULL slab_ids (partitions with only static columns)
                if let Some(slab_id) = slab_id_opt {
                    yield SlabId::from_le_bytes(slab_id.to_le_bytes());
                }
            }
        }
    }

    /// Streams v1 triggers from a slab.
    ///
    /// Queries the v1 `timer_slabs` table with PK `((segment_id, id), key,
    /// time)`.
    #[instrument(level = "debug", skip(self), fields(segment_id = %segment_id, slab_id = %slab_id))]
    fn get_slab_triggers_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<TriggerV1, Self::Error>> + Send {
        let segment_id = *segment_id;
        let slab_id = i32::from_le_bytes(slab_id.to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_slab_triggers_v1.clone(),
                    (segment_id, slab_id),
                )
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger_v1");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                yield TriggerV1 {
                    key: key.into(),
                    time,
                    span,
                };
            }
        }
    }

    /// Deletes v1 slab metadata from the `segments` table.
    ///
    /// Low-level method that removes a single slab entry from the segments
    /// table. Part of the v1 migration API.
    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab_metadata_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab_metadata_v1,
                (segment_id, slab_id as i32),
            )
            .await?;

        Ok(())
    }

    /// Deletes v1 slab triggers from the `timer_slabs` table.
    ///
    /// Low-level method that removes all triggers for a slab from the
    /// `timer_slabs` table. Uses v1 PK `((segment_id, id), key, time)`.
    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab_triggers_v1(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab_triggers_v1,
                (segment_id, slab_id as i32),
            )
            .await?;

        Ok(())
    }

    /// Inserts a v1 trigger into the `timer_keys` table.
    ///
    /// Low-level method that inserts into the v1 `timer_keys` table without
    /// `timer_type`. Part of the v1 migration API.
    #[instrument(level = "debug", skip(self), err)]
    async fn insert_key_trigger_v1(
        &self,
        segment_id: &SegmentId,
        trigger: TriggerV1,
    ) -> Result<(), Self::Error> {
        // Extract span context for v1 trigger
        let mut span_map: HashMap<String, String> = HashMap::new();
        let context = trigger.span.context();
        self.propagator().inject_context(&context, &mut span_map);

        self.session()
            .execute_unpaged(
                &self.queries().insert_key_trigger_v1,
                (segment_id, trigger.key.as_ref(), trigger.time, span_map),
            )
            .await?;

        Ok(())
    }

    /// Retrieves v1 triggers for a key from the `timer_keys` table.
    ///
    /// Queries v1 `timer_keys` table using partition key (`segment_id`, key).
    #[instrument(level = "debug", skip(self), fields(segment_id = %segment_id, key = %key))]
    fn get_key_triggers_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<TriggerV1, Self::Error>> + Send {
        let segment_id = *segment_id;
        let key = key.clone();

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_key_triggers_v1.clone(),
                    (segment_id, key.as_ref()),
                )
                .await?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next()).await? {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger_v1");
                if let Err(error) = span.set_parent(context) {
                    error!("failed to set parent span: {error:#}");
                }

                yield TriggerV1 {
                    key: key.into(),
                    time,
                    span,
                };
            }
        }
    }

    /// Clears v1 triggers for a key from the `timer_keys` table.
    ///
    /// Uses v1 partition key (`segment_id`, key).
    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers_v1(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        self.session()
            .execute_unpaged(
                &self.queries().clear_key_triggers_v1,
                (segment_id, key.as_ref()),
            )
            .await?;

        Ok(())
    }
}

/// Error type for Cassandra trigger store operations.
pub type CassandraTriggerStoreError = CassandraStoreError;

#[cfg(test)]
mod test {
    use super::{CassandraConfiguration, CassandraTriggerStore};
    use std::time::Duration;

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
    use crate::timers::duration::CompactDuration;
    use crate::timers::slab::{Slab, SlabId};
    use crate::timers::store::{Segment, SegmentId, SegmentVersion, TriggerStore};
    use crate::trigger_store_tests;
    use color_eyre::Result;
    use futures::pin_mut;
    use futures::stream::StreamExt;
    use std::collections::HashSet;
    use std::ops::RangeInclusive;
    use uuid::Uuid;

    // Run the full suite of TriggerStore compliance tests on this implementation.
    trigger_store_tests!(
        CassandraTriggerStore,
        CassandraTriggerStore::new(&test_cassandra_config("prosody")),
        25
    );

    #[tokio::test]
    async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
        let store = CassandraTriggerStore::new(&test_cassandra_config("prosody_test")).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "test_segment".to_owned(),
            slab_size: CompactDuration::new(60), // 1 minute slabs
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
        let store =
            CassandraTriggerStore::new(&test_cassandra_config("prosody_test_simple")).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "simple_test".to_owned(),
            slab_size: CompactDuration::new(60),
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
