//! The Cassandra catalog.
//!
//! This file is the only place in the crate that uses `ALLOW FILTERING`. The
//! deferred message and timer tables are keyed by `(segment_id, key)`, and a
//! maintenance run knows only the segment, so Cassandra must filter the rest.
//! Production code must never copy this: every production read names a whole
//! partition key.
//!
//! The scans read at one replica and fetch small pages, so a maintenance run
//! never competes with a live consumer group for a coordinator. A stale row
//! costs one redundant idempotent repair, and a missed row leaves a stranded
//! queue for the next run.
//!
//! The timer segment row keeps the session default consistency. A stale version
//! there would hide a whole segment behind a legacy layout report.

use super::Catalog;
use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{
    CassandraStore, TABLE_DEFERRED_OFFSETS, TABLE_DEFERRED_SEGMENTS, TABLE_DEFERRED_TIMERS,
    TABLE_SEGMENTS,
};
use crate::cassandra_queries;
use crate::maintenance::identity::{DeferSegmentId, GroupId, Segment, TimerSegmentId};
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment as TimerSegment, SegmentVersion};
use crate::{Key, Topic};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;
use tokio::task::coop::cooperative;
use tracing::warn;
use uuid::Uuid;

/// Rows fetched per page.
pub(crate) const CATALOG_PAGE_SIZE: i32 = 100;

cassandra_queries! {
    /// The maintenance scans. Every one is a read.
    struct CatalogQueries {
        /// Full read of the small registry. It has no `WHERE` clause, so it
        /// restricts nothing.
        segments: (
            "SELECT id, topic, partition, consumer_group FROM $keyspace.{}",
            TABLE_DEFERRED_SEGMENTS
        ),

        /// The production timer segment row read, without the migration the
        /// production store performs. The production method is private to the
        /// timer store, so the statement is written twice on purpose.
        timer_segment: (
            "SELECT name, slab_size, version FROM $keyspace.{} WHERE id = ? ORDER BY slab_id DESC LIMIT 1",
            TABLE_SEGMENTS
        ),

        /// One row per partition of the segment. `DISTINCT` needs the whole
        /// partition key in the select list.
        message_keys: (
            "SELECT DISTINCT segment_id, key FROM $keyspace.{} WHERE segment_id = ? ALLOW FILTERING",
            TABLE_DEFERRED_OFFSETS
        ),

        /// The twin of `message_keys` over the deferred timer rows.
        timer_keys: (
            "SELECT DISTINCT segment_id, key FROM $keyspace.{} WHERE segment_id = ? ALLOW FILTERING",
            TABLE_DEFERRED_TIMERS
        ),
    }
}

/// Reads the maintenance scans from Cassandra.
#[derive(Clone, Debug)]
pub struct CassandraCatalog {
    store: CassandraStore,
    queries: Arc<CatalogQueries>,
}

impl CassandraCatalog {
    /// Prepares the scans against `store`.
    ///
    /// # Errors
    ///
    /// Returns the driver's error when a statement cannot be prepared.
    pub async fn new(store: CassandraStore) -> Result<Self, CassandraStoreError> {
        let mut queries = CatalogQueries::new(store.session(), store.keyspace()).await?;
        for statement in [
            &mut queries.segments,
            &mut queries.message_keys,
            &mut queries.timer_keys,
        ] {
            statement.set_consistency(Consistency::LocalOne);
            statement.set_page_size(CATALOG_PAGE_SIZE);
        }

        Ok(Self {
            store,
            queries: Arc::new(queries),
        })
    }

    /// The page size each key scan fetches with.
    ///
    /// The page boundary test reads it to prove that its seed spans more than
    /// one page.
    #[cfg(test)]
    pub(crate) fn key_scan_page_sizes(&self) -> [i32; 2] {
        [
            self.queries.message_keys.get_page_size(),
            self.queries.timer_keys.get_page_size(),
        ]
    }

    /// Streams the keys one prepared scan reports for `id`.
    ///
    /// Do not swap in `execute_unpaged` here: it puts every key of the segment
    /// in one response, and a scan has no bound on how many that is.
    fn scan_keys(
        &self,
        statement: &PreparedStatement,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, CassandraStoreError>> + Send + 'static {
        let store = self.store.clone();
        let statement = statement.clone();
        try_stream! {
            let rows = store
                .session()
                .execute_iter(statement, (id.as_uuid(),))
                .await?
                .rows_stream::<(Uuid, String)>()?;
            pin_mut!(rows);

            while let Some((_, key)) = cooperative(rows.try_next()).await? {
                yield Key::from(key);
            }
        }
    }
}

impl Catalog for CassandraCatalog {
    type Error = CassandraStoreError;

    fn segments(&self) -> impl Stream<Item = Result<Segment, Self::Error>> + Send + 'static {
        let store = self.store.clone();
        let statement = self.queries.segments.clone();
        try_stream! {
            let rows = store
                .session()
                .execute_iter(statement, ())
                .await?
                .rows_stream::<(Uuid, Option<String>, Option<i32>, Option<String>)>()?;
            pin_mut!(rows);

            while let Some((id, topic, partition, group)) = cooperative(rows.try_next()).await? {
                // A registry row that is missing a column was hand-edited or
                // partly deleted. Skip it and name it: a strict decode would
                // turn one such row into a Terminal error that kills the run.
                if let (Some(topic), Some(partition), Some(group)) = (topic, partition, group) {
                    yield Segment::new(GroupId::new(&group), Topic::from(topic.as_str()), partition);
                } else {
                    warn!(segment.id = %id, "skipped a deferred segment row that names no group, topic, and partition");
                }
            }
        }
    }

    async fn timer_segment(&self, id: TimerSegmentId) -> Result<Option<TimerSegment>, Self::Error> {
        let row = self
            .store
            .session()
            .execute_unpaged(&self.queries.timer_segment, (id.as_uuid(),))
            .await?
            .into_rows_result()?
            .maybe_first_row::<(String, CompactDuration, Option<SegmentVersion>)>()?;

        Ok(row.map(|(name, slab_size, version)| TimerSegment {
            id: id.as_uuid(),
            name,
            slab_size,
            // A row written before the version column existed reads as NULL.
            version: version.unwrap_or(SegmentVersion::V1),
        }))
    }

    fn message_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static {
        self.scan_keys(&self.queries.message_keys, id)
    }

    fn timer_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static {
        self.scan_keys(&self.queries.timer_keys, id)
    }
}
