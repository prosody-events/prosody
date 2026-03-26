//! V1 schema operations for Cassandra (internal).
//!
//! This module contains V1 schema operations used for backwards compatibility
//! and migration. These operations work with the legacy schema that lacks the
//! `timer_type` field.
//!
//! **This is Cassandra-internal only and not part of the public API.**

#![allow(dead_code, reason = "methods used in tests")]

use crate::Key;
use crate::cassandra::CassandraStore;
use crate::cassandra::errors::CassandraStoreError;
use crate::consumer::SpanLink;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::SlabId;
use crate::timers::store::cassandra::CassandraTriggerStoreError;
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::{SegmentId, TriggerV1};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::TextMapPropagator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::coop::cooperative;
use tracing::{info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(test)]
pub mod tests;

/// V1 schema operations for backwards compatibility and migration.
///
/// This struct encapsulates all V1-specific Cassandra operations that work with
/// the legacy schema (without `timer_type` field). These operations are used
/// during schema migration and for backwards compatibility.
///
/// **Internal use only** - not part of the public API.
#[derive(Clone, Debug)]
pub(crate) struct V1Operations {
    store: CassandraStore,
    queries: Arc<Queries>,
    timer_linking: SpanLink,
}

impl V1Operations {
    /// Creates a new `V1Operations` instance.
    ///
    /// # Arguments
    ///
    /// * `store` - Cassandra store providing session and propagator access
    /// * `queries` - Shared prepared CQL queries
    /// * `timer_linking` - Strategy for linking timer spans to propagated
    ///   contexts
    pub(crate) fn new(
        store: CassandraStore,
        queries: Arc<Queries>,
        timer_linking: SpanLink,
    ) -> Self {
        Self {
            store,
            queries,
            timer_linking,
        }
    }

    // ========================================================================
    // Segment Metadata Operations (V1-specific)
    // ========================================================================

    /// Inserts segment metadata with V1 schema (version=NULL).
    ///
    /// Creates a segment with static columns `name` and `slab_size`, but leaves
    /// `version=NULL` to simulate segments created before the version column
    /// was added to the schema.
    ///
    /// This is used for testing V1→V2 migration and should not be used in
    /// production code.
    pub(crate) async fn insert_segment_v1(
        &self,
        segment_id: &SegmentId,
        name: &str,
        slab_size: CompactDuration,
    ) -> Result<(), CassandraTriggerStoreError> {
        // Insert segment with version=NULL by only setting static columns
        // CQL: INSERT INTO segments (id, name, slab_size) VALUES (?, ?, ?)
        // Note: This requires a prepared statement that omits the version column
        self.store
            .session()
            .execute_unpaged(
                &self.queries.insert_segment_v1,
                (segment_id, name, slab_size),
            )
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    // ========================================================================
    // Slab Metadata Operations (V1-specific)
    // ========================================================================

    /// Inserts a slab entry into v1 schema.
    ///
    /// Inserts a row into the `segments` table with partition key `segment_id`
    /// and clustering column `slab_id` (v1 schema).
    pub(crate) async fn insert_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(&self.queries.insert_slab_v1, (segment_id, slab_id as i32))
            .await
            .map_err(CassandraStoreError::from)?;
        Ok(())
    }

    /// Inserts a trigger into v1 slab table.
    ///
    /// Inserts into the v1 `timer_slabs` table without `timer_type`.
    pub(crate) async fn insert_slab_trigger(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        trigger: TriggerV1,
    ) -> Result<(), CassandraTriggerStoreError> {
        // Extract span context from tracing::Span
        let mut span_map: HashMap<String, String> = HashMap::new();
        let context = trigger.span.context();
        self.store
            .propagator()
            .inject_context(&context, &mut span_map);

        self.store
            .session()
            .execute_unpaged(
                &self.queries.insert_slab_trigger_v1,
                (
                    segment_id,
                    slab_id as i32,
                    trigger.key.as_ref(),
                    trigger.time,
                    span_map,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;
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
    pub(crate) fn get_slabs(
        &self,
        segment_id: &SegmentId,
    ) -> impl Stream<Item = Result<SlabId, CassandraTriggerStoreError>> + Send {
        let segment_id = *segment_id;
        let store = self.store.clone();
        let queries = Arc::clone(&self.queries);

        try_stream! {
            let stream = store
                .session()
                .execute_iter(queries.get_slabs_v1.clone(), (segment_id,))
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(Option<i32>,)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((slab_id_opt,)) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)?
            {
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
    pub(crate) fn get_slab_triggers(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<TriggerV1, CassandraTriggerStoreError>> + Send {
        let segment_id = *segment_id;
        let slab_id = i32::from_le_bytes(slab_id.to_le_bytes());
        let store = self.store.clone();
        let queries = Arc::clone(&self.queries);
        let timer_linking = self.timer_linking;

        try_stream! {
            let stream = store
                .session()
                .execute_iter(queries.get_slab_triggers_v1.clone(), (segment_id, slab_id))
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)?
            {
                let context = store.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger_v1");
                timer_linking.apply(&span, context);

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
    pub(crate) async fn delete_slab_metadata(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(
                &self.queries.delete_slab_metadata_v1,
                (segment_id, slab_id as i32),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Deletes a single v1 trigger from the `timer_slabs` table.
    ///
    /// Low-level method that removes a specific trigger identified by
    /// `(segment_id, slab_id, key, time)` from the `timer_slabs` table.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn delete_slab_trigger(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(
                &self.queries.delete_slab_trigger_v1,
                (segment_id, slab_id as i32, key.as_ref(), time),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Deletes v1 slab triggers from the `timer_slabs` table.
    ///
    /// Low-level method that removes all triggers for a slab from the
    /// `timer_slabs` table. Uses v1 PK `((segment_id, id), key, time)`.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn clear_slab_triggers(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(
                &self.queries.clear_slab_triggers_v1,
                (segment_id, slab_id as i32),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Inserts a v1 trigger into the `timer_keys` table.
    ///
    /// Low-level method that inserts into the v1 `timer_keys` table without
    /// `timer_type`. Part of the v1 migration API.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: TriggerV1,
    ) -> Result<(), CassandraTriggerStoreError> {
        // Extract span context for v1 trigger
        let mut span_map: HashMap<String, String> = HashMap::new();
        let context = trigger.span.context();
        self.store
            .propagator()
            .inject_context(&context, &mut span_map);

        self.store
            .session()
            .execute_unpaged(
                &self.queries.insert_key_trigger_v1,
                (segment_id, trigger.key.as_ref(), trigger.time, span_map),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Retrieves v1 triggers for a key from the `timer_keys` table.
    ///
    /// Queries v1 `timer_keys` table using partition key (`segment_id`, key).
    #[instrument(level = "debug", skip(self), fields(segment_id = %segment_id, key = %key))]
    pub(crate) fn get_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<TriggerV1, CassandraTriggerStoreError>> + Send {
        let segment_id = *segment_id;
        let key = key.clone();
        let store = self.store.clone();
        let queries = Arc::clone(&self.queries);
        let timer_linking = self.timer_linking;

        try_stream! {
            let stream = store
                .session()
                .execute_iter(
                    queries.get_key_triggers_v1.clone(),
                    (segment_id, key.as_ref()),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, span_map)) = cooperative(stream.try_next())
                .await
                .map_err(CassandraStoreError::from)?
            {
                let context = store.propagator().extract(&span_map);
                let span = info_span!("fetch_key_trigger_v1");
                timer_linking.apply(&span, context);

                yield TriggerV1 {
                    key: key.into(),
                    time,
                    span,
                };
            }
        }
    }

    /// Deletes a single v1 trigger from the `timer_keys` table.
    ///
    /// Low-level method that removes a specific trigger identified by
    /// `(segment_id, key, time)` from the `timer_keys` table.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(
                &self.queries.delete_key_trigger_v1,
                (segment_id, key.as_ref(), time),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Clears v1 triggers for a key from the `timer_keys` table.
    ///
    /// Uses v1 partition key (`segment_id`, key).
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.store
            .session()
            .execute_unpaged(
                &self.queries.clear_key_triggers_v1,
                (segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    // ========================================================================
    // High-Level Coordinated Operations (Dual-Index Consistency)
    // ========================================================================

    /// Adds a v1 trigger to both slab and key indices.
    ///
    /// High-level coordinated operation that maintains dual-index consistency
    /// by inserting the trigger into both:
    /// 1. The slab index (`timer_slabs` table) - for time-based queries
    /// 2. The key index (`timer_keys` table) - for entity-based queries
    ///
    /// Both operations execute in parallel using `try_join!` for efficiency.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab ID (time partition)
    /// * `trigger` - The trigger to add
    ///
    /// # Errors
    ///
    /// Returns an error if either insertion fails. Partial completion may
    /// occur if one insert succeeds and the other fails.
    #[instrument(level = "debug", skip(self, trigger), err)]
    pub(crate) async fn add_trigger(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        trigger: TriggerV1,
    ) -> Result<(), CassandraTriggerStoreError> {
        use tokio::try_join;

        // Insert into both indices in parallel
        try_join!(
            self.insert_slab_trigger(segment_id, slab_id, trigger.clone()),
            self.insert_key_trigger(segment_id, trigger)
        )?;

        Ok(())
    }

    /// Removes a v1 trigger from both slab and key indices.
    ///
    /// High-level coordinated operation that maintains dual-index consistency
    /// by deleting the trigger from both:
    /// 1. The slab index (`timer_slabs` table)
    /// 2. The key index (`timer_keys` table)
    ///
    /// Both operations execute in parallel using `try_join!` for efficiency.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab ID (time partition)
    /// * `key` - The trigger key
    /// * `time` - The trigger time
    ///
    /// # Errors
    ///
    /// Returns an error if either deletion fails. Partial completion may
    /// occur if one delete succeeds and the other fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn remove_trigger(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), CassandraTriggerStoreError> {
        use tokio::try_join;

        // Delete from both indices in parallel
        try_join!(
            self.delete_slab_trigger(segment_id, slab_id, key, time),
            self.delete_key_trigger(segment_id, key, time)
        )?;

        Ok(())
    }

    /// Clears all v1 triggers for a key from both slab and key indices.
    ///
    /// High-level coordinated operation that maintains dual-index consistency
    /// by:
    /// 1. Querying all triggers for the key from the key index
    /// 2. Calculating the slab ID for each trigger using the provided slab size
    /// 3. Deleting each trigger from its respective slab index (concurrently)
    /// 4. Clearing all triggers from the key index
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `key` - The key whose triggers should be cleared
    /// * `slab_size` - The slab size used to calculate slab IDs from trigger
    ///   times
    ///
    /// # Errors
    ///
    /// Returns an error if any operation fails. Partial completion may occur
    /// if an error interrupts processing.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn clear_triggers_for_key(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        slab_size: CompactDuration,
    ) -> Result<(), CassandraTriggerStoreError> {
        use crate::timers::DELETE_CONCURRENCY;
        use crate::timers::slab::Slab;
        use futures::TryStreamExt;

        let segment_id_copy = *segment_id;
        let stream = self.get_key_triggers(segment_id, key);

        // Delete each trigger from its slab index
        stream
            .try_for_each_concurrent(DELETE_CONCURRENCY, move |trigger| {
                let self_clone = self.clone();
                async move {
                    // Calculate which slab this trigger belongs to
                    let slab = Slab::from_time(slab_size, trigger.time);
                    self_clone
                        .delete_slab_trigger(
                            &segment_id_copy,
                            slab.id(),
                            &trigger.key,
                            trigger.time,
                        )
                        .await
                }
            })
            .await?;

        // Clear all triggers from key index
        self.clear_key_triggers(segment_id, key).await?;

        Ok(())
    }

    /// Deletes a v1 slab and all its triggers from both slab and key indices.
    ///
    /// High-level coordinated operation that maintains dual-index consistency
    /// by:
    /// 1. Getting all triggers in the slab
    /// 2. Deleting each trigger from the key index
    /// 3. Clearing all triggers from the slab index
    /// 4. Deleting the slab metadata
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment identifier
    /// * `slab_id` - The slab to delete
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails. Partial completion may occur
    /// if an error interrupts processing.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn delete_slab(
        &self,
        segment_id: &SegmentId,
        slab_id: SlabId,
    ) -> Result<(), CassandraTriggerStoreError> {
        use crate::timers::DELETE_CONCURRENCY;
        use futures::TryStreamExt;
        use tokio::try_join;

        let segment_id_copy = *segment_id;
        let stream = self.get_slab_triggers(segment_id, slab_id);

        // Delete each trigger from key index
        stream
            .try_for_each_concurrent(DELETE_CONCURRENCY, move |trigger| {
                let self_clone = self.clone();
                async move {
                    self_clone
                        .delete_key_trigger(&segment_id_copy, &trigger.key, trigger.time)
                        .await
                }
            })
            .await?;

        // Clear slab triggers and metadata in parallel
        try_join!(
            self.clear_slab_triggers(&segment_id_copy, slab_id),
            self.delete_slab_metadata(&segment_id_copy, slab_id)
        )?;

        Ok(())
    }
}
