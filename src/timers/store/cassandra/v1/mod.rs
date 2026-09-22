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
use tracing::instrument;

mod coordinated;
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
}

impl V1Operations {
    /// Creates a new `V1Operations` instance.
    pub(crate) fn new(store: CassandraStore, queries: Arc<Queries>) -> Self {
        Self { store, queries }
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
        // Serialize the trigger's scheduling context for storage.
        let mut span_map: HashMap<String, String> = HashMap::new();
        self.store
            .propagator()
            .inject_context(&trigger.context, &mut span_map);

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
    pub(crate) fn get_slabs<'a>(
        &'a self,
        segment_id: &'a SegmentId,
    ) -> impl Stream<Item = Result<SlabId, CassandraTriggerStoreError>> + Send + use<'a> {
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
    pub(crate) fn get_slab_triggers<'a>(
        &'a self,
        segment_id: &'a SegmentId,
        slab_id: SlabId,
    ) -> impl Stream<Item = Result<TriggerV1, CassandraTriggerStoreError>> + Send + use<'a> {
        let segment_id = *segment_id;
        let slab_id = i32::from_le_bytes(slab_id.to_le_bytes());
        let store = self.store.clone();
        let queries = Arc::clone(&self.queries);
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

                yield TriggerV1 {
                    key: key.into(),
                    time,
                    context,
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
        // Serialize the trigger's scheduling context for storage.
        let mut span_map: HashMap<String, String> = HashMap::new();
        self.store
            .propagator()
            .inject_context(&trigger.context, &mut span_map);

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
    pub(crate) fn get_key_triggers<'a>(
        &'a self,
        segment_id: &'a SegmentId,
        key: &'a Key,
    ) -> impl Stream<Item = Result<TriggerV1, CassandraTriggerStoreError>> + Send + use<'a> {
        let segment_id = *segment_id;
        let key = key.clone();
        let store = self.store.clone();
        let queries = Arc::clone(&self.queries);

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

                yield TriggerV1 {
                    key: key.into(),
                    time,
                    context,
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
}
