//! [`TriggerOperations`] implementation for [`CassandraTriggerStore`].
//!
//! Each method resolves the current [`TimerState`] (via the cache or a DB
//! read) before issuing writes, so it can pick the cheapest Cassandra
//! operation: a plain `UPDATE` for the common inline path, or a tombstone-free
//! BATCH only when clustering rows actually need to change.
//!
//! **Locking contract.** State-mutating methods call `resolve_state` to obtain
//! the per-`(key, timer_type)` [`CachedState`](super::state::CachedState) mutex
//! (see [`crate::timers::store::cassandra::state`]) and hold it from the state
//! check through the DB write — that is what makes the read-decide-write
//! sequence linearisable against concurrent callers on the same
//! `(key, timer_type)`. Read-only stream methods take the same lock briefly
//! to snapshot the state, then release before any DB I/O: a concurrent
//! transition just shifts the snapshot, which is acceptable for a stream.
//!
//! `get_key_triggers_all_types` is the most complex method: it reads the full
//! `state` map in one query, then merges inline entries (sorted by
//! `TimerType` discriminant) with a clustering-row stream in a single pass,
//! yielding triggers in `(timer_type, time)` order without issuing a
//! clustering scan for types that are already inline-or-absent.
//!
//! **Restored triggers carry the persisted scheduling context, never a
//! span.** A fetched trigger is rebuilt via [`Trigger::restored`] from the
//! stored span map, so its trace is exactly the scheduling-time context —
//! the origin trace is never touched after scheduling. The configured
//! `timer_spans` relation is applied exactly once, at fire time, by
//! [`FiringTimer::set_dispatch_span`] against that context, which is what
//! makes memory- and Cassandra-backed timers produce identical dispatch-span
//! topology.
//!
//! [`FiringTimer::set_dispatch_span`]: crate::timers::uncommitted::FiringTimer::set_dispatch_span
//! [`TriggerOperations`]: crate::timers::store::operations::TriggerOperations
//! [`CassandraTriggerStore`]: crate::timers::store::cassandra::CassandraTriggerStore
//! [`TimerState`]: crate::timers::store::cassandra::TimerState

use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::migration;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;
use tokio::task::coop::cooperative;
use tracing::instrument;

mod read;
mod slab;
mod write;

impl TriggerOperations for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    fn segment(&self) -> &Segment {
        &self.segment
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_segment(&self) -> Result<(), Self::Error> {
        let segment = &self.segment;
        self.session()
            .execute_unpaged(
                &self.queries().insert_segment,
                (
                    segment.id,
                    &segment.name,
                    segment.slab_size,
                    segment.version,
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_segment(&self) -> Result<Option<Segment>, Self::Error> {
        let segment_id = &self.segment.id;
        let Some(segment) = self.get_segment_unchecked(segment_id).await? else {
            return Ok(None);
        };

        let segment =
            migration::migrate_segment_if_needed(self, segment, self.segment.slab_size).await?;

        Ok(Some(segment))
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_segment(&self) -> Result<(), Self::Error> {
        let segment_id = &self.segment.id;
        self.session()
            .execute_unpaged(&self.queries().delete_segment, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slabs(&self) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send + use<'_> {
        let segment_id = self.segment.id;
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
    fn get_slab_range(
        &self,
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send + use<'_> {
        let segment_id = self.segment.id;
        try_stream! {
            // An invalid range (start > end in u32 terms) yields nothing.
            if range.start() > range.end() {
                return;
            }

            let start = i32::from_le_bytes(range.start().to_le_bytes());
            let end = i32::from_le_bytes(range.end().to_le_bytes());

            // Reinterpreting a u32 range as i32 bytes can flip `start > end`
            // even though the u32 range above was valid: u32 values at or
            // past 2^31 become negative i32 values, so a range straddling
            // that boundary needs two queries — one for the (still
            // positive) low half up to `i32::MAX`, one for the (now
            // negative) high half from `i32::MIN`.
            let bounds: [Option<(i32, i32)>; 2] = if start > end {
                [Some((start, i32::MAX)), Some((i32::MIN, end))]
            } else {
                [Some((start, end)), None]
            };

            for (lo, hi) in bounds.into_iter().flatten() {
                let stream = self
                    .session()
                    .execute_iter(self.queries().get_slab_range.clone(), (segment_id, lo, hi))
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream::<(Option<i32>,)>()
                    .map_err(CassandraStoreError::from)?;

                pin_mut!(stream);
                while let Some((value,)) = cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
                {
                    let Some(value) = value else { continue };
                    yield SlabId::from_le_bytes(value.to_le_bytes());
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab(&self, slab: Slab) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        let ttl = self.calculate_ttl(slab.range().end);
        self.execute_unpaged_discard(&self.queries().insert_slab, (segment_id, slab_id, ttl))
            .await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn delete_slab(&self, slab_id: SlabId) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        self.session()
            .execute_unpaged(
                &self.queries().delete_slab,
                (segment_id, i32::from_le_bytes(slab_id.to_le_bytes())),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn get_slab_watermark(&self) -> Result<Option<SlabId>, Self::Error> {
        let segment_id = self.segment.id;
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_slab_watermark, (segment_id,))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<i32>,)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row
            .and_then(|(w,)| w)
            .map(|w| SlabId::from_le_bytes(w.to_le_bytes())))
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn set_slab_watermark(&self, watermark: Option<SlabId>) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let watermark_i32 = watermark.map(|w| i32::from_le_bytes(w.to_le_bytes()));

        // TTL anchor uses the natural slab end; `calculate_ttl` adds the
        // configured `base_ttl` grace period (default 1 year), matching the
        // same lifetime as `insert_slab` and slab triggers.
        let anchor_time = anchor_after_watermark(watermark, self.segment.slab_size);

        let ttl = self.calculate_ttl(anchor_time);
        self.execute_unpaged_discard(
            &self.queries().set_slab_watermark,
            (ttl, watermark_i32, segment_id),
        )
        .await
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn batch_insert_slab_with_watermark(
        &self,
        slab: Slab,
        watermark: Option<SlabId>,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
        let watermark_i32 = watermark.map(|w| i32::from_le_bytes(w.to_le_bytes()));

        // Same anchor as `insert_slab` so the slab row and watermark hint
        // share a lifetime. `calculate_ttl` adds the configured `base_ttl`
        // grace period (default 1 year) on top of `slab.range().end` — that
        // grace is what lets a lagging client process past-time slabs
        // without finding them already TTL'd out.
        let anchor_time = slab.range().end;

        let ttl = self.calculate_ttl(anchor_time);
        self.execute_unpaged_discard(
            &self.queries().batch_insert_slab_with_watermark,
            (segment_id, slab_id, ttl, ttl, watermark_i32, segment_id),
        )
        .await
    }

    fn get_slab_triggers<'a>(
        &'a self,
        slab: &'a Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'a> {
        slab::triggers(self, slab, timer_type)
    }

    fn get_slab_triggers_all_types(
        &self,
        slab: Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'_> {
        slab::triggers_all_types(self, &slab)
    }

    fn insert_slab_trigger(
        &self,
        slab: Slab,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        slab::insert(self, slab, trigger)
    }

    fn delete_slab_trigger(
        &self,
        slab: &Slab,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        slab::delete(self, slab, timer_type, key, time)
    }

    fn clear_slab_triggers(
        &self,
        slab: &Slab,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        slab::clear(self, slab)
    }

    fn get_key_times<'a>(
        &'a self,
        timer_type: TimerType,
        key: &'a Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + use<'a> {
        read::times(self, timer_type, key)
    }

    fn get_key_triggers<'a>(
        &'a self,
        timer_type: TimerType,
        key: &'a Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'a> {
        read::triggers(self, timer_type, key)
    }

    fn get_key_triggers_all_types<'a>(
        &'a self,
        key: &'a Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send + use<'a> {
        read::triggers_all_types(self, key)
    }

    fn upsert_key_trigger(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        write::upsert(self, trigger)
    }

    fn delete_key_trigger(
        &self,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        write::delete(self, timer_type, key, time)
    }

    fn clear_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        write::clear(self, timer_type, key)
    }

    fn clear_and_schedule_key(
        &self,
        trigger: Trigger,
    ) -> impl Future<Output = Result<SmallVec<[CompactDateTime; 1]>, Self::Error>> + Send {
        write::clear_and_schedule(self, trigger)
    }

    fn clear_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        write::clear_all_types(self, key)
    }

    fn current_trigger(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Option<Trigger>, Self::Error>> + Send {
        read::current(self, key, time, timer_type)
    }

    // -- V1 migration methods --

    /// Updates the segment's version field after v1 to v2 migration.
    #[instrument(level = "debug", skip(self), err)]
    async fn update_segment_version(
        &self,
        new_version: SegmentVersion,
        new_slab_size: CompactDuration,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        self.session()
            .execute_unpaged(
                &self.queries().update_segment_version,
                (new_version, new_slab_size.seconds() as i32, segment_id),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }
}

/// Returns the TTL anchor time for a watermark update.
///
/// Anchors on the natural end of the slab at `watermark + 1`. The actual
/// TTL is `(anchor - now) + base_ttl` because `calculate_ttl` adds the
/// configured grace period (default 1 year) — slabs and the watermark hint
/// deliberately outlive their natural end so a lagging consumer can still
/// process past-time slabs.
fn anchor_after_watermark(
    watermark: Option<SlabId>,
    slab_size: CompactDuration,
) -> CompactDateTime {
    let next_id = watermark.map_or(0, |w| w.saturating_add(1));
    Slab::new(next_id, slab_size).range().end
}

/// Injects the trigger's span context into a new `HashMap` for Cassandra
/// storage.
pub(super) fn extract_span_map(
    propagator: &TextMapCompositePropagator,
    trigger: &Trigger,
) -> HashMap<String, String> {
    let mut span_map = HashMap::with_capacity(2);
    let context = trigger.context();
    propagator.inject_context(&context, &mut span_map);
    span_map
}
