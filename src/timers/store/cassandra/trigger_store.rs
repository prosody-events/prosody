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
//! `(key, timer_type)`. The time scan holds this lock until it fills the tag
//! list. Other streams release it after they copy the state.
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

use super::state::{OverflowTags, TagLookup};
use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::migration;
use crate::timers::store::cassandra::state::{ClusteringEntry, InlineTimer, TimerState};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentId, SegmentVersion};
use crate::timers::{TimerType, Trigger, TriggerId};
use async_stream::try_stream;
use futures::{
    Stream, TryStreamExt,
    future::{join_all, ready},
    pin_mut,
};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use strum::VariantArray;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::coop::cooperative;
use tracing::field::Empty;
use tracing::{Span, instrument};

impl TriggerOperations for CassandraTriggerStore {
    type Error = CassandraTriggerStoreError;

    #[cfg(test)]
    fn cold(&self) -> Self {
        Self::with_shared(
            self.store.clone(),
            self.queries.clone(),
            self.segment.clone(),
        )
    }

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
    fn get_slabs(&self) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
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
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
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

        self.execute_with_optional_ttl(
            anchor_time,
            &self.queries().set_slab_watermark,
            &self.queries().set_slab_watermark_no_ttl,
            |ttl| (ttl, watermark_i32, segment_id),
            || (watermark_i32, segment_id),
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

        self.execute_with_optional_ttl(
            anchor_time,
            &self.queries().batch_insert_slab_with_watermark,
            &self.queries().batch_insert_slab_with_watermark_no_ttl,
            |ttl| (segment_id, slab_id, ttl, ttl, watermark_i32, segment_id),
            || (segment_id, slab_id, watermark_i32, segment_id),
        )
        .await
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers(
        &self,
        slab: &Slab,
        timer_type: TimerType,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = self.segment.id;
        let slab_size = slab.size().seconds() as i32;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

        try_stream! {
            let stream = self
                .session()
                .execute_iter(
                    self.queries().get_slab_triggers.clone(),
                    (segment_id, slab_size, slab_id, timer_type),
                )
                .await.map_err(CassandraStoreError::from)?
                .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>, Option<i32>)>().map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type, span_map, tag_opt)) =
                cooperative(stream.try_next()).await.map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let tag = tag_opt.unwrap_or(0_i32);

                yield Trigger::restored(key.into(), time, timer_type, tag, context);
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers_all_types(
        &self,
        slab: Slab,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let segment_id = self.segment.id;
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
                .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>, Option<i32>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type, span_map, tag_opt)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let tag = tag_opt.unwrap_or(0_i32);

                yield Trigger::restored(key.into(), time, timer_type, tag, context);
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let span_map = extract_span_map(self.propagator(), &trigger);

        let segment_id = self.segment.id;
        let slab_size = slab.size().seconds() as i32;
        let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
        let key = trigger.key.as_ref();
        let time = trigger.time;
        let timer_type = trigger.timer_type;
        let tag = trigger.tag;

        self.execute_with_optional_ttl(
            slab.range().end,
            &self.queries().insert_slab_trigger,
            &self.queries().insert_slab_trigger_no_ttl,
            |ttl| {
                (
                    segment_id, slab_size, slab_id, timer_type, key, time, &span_map, tag, ttl,
                )
            },
            || {
                (
                    segment_id, slab_size, slab_id, timer_type, key, time, &span_map, tag,
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
                    self.segment.id,
                    slab.size().seconds() as i32,
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                    timer_type,
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
                    self.segment.id,
                    slab.size().seconds() as i32,
                    i32::from_le_bytes(slab.id().to_le_bytes()),
                ),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(state_cached = Empty))]
    fn get_key_times(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<(CompactDateTime, i32), Self::Error>> + Send {
        let key_clone = key.clone();
        let segment_id = self.segment.id;

        try_stream! {
            let (handle, cached) = self.resolve_state(&segment_id, &key_clone, timer_type).await?;
            Span::current().record("state_cached", cached);

            let mut state = handle.lock().await;
            match &mut *state {
                TimerState::Inline(timer) => {
                    // Inline: yield time from cache (0 clustering query).
                    yield (timer.time, timer.tag);
                }
                TimerState::Overflow(tags) => {
                    let mut scanned = OverflowTags::Complete(SmallVec::new());
                    // Overflow: scan clustering rows.
                    let stream = self
                        .session()
                        .execute_iter(
                            self.queries().get_key_times.clone(),
                            (segment_id, key_clone.as_ref(), timer_type),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?
                        .rows_stream::<(CompactDateTime, Option<i32>)>()
                        .map_err(CassandraStoreError::from)?;

                    pin_mut!(stream);
                    while let Some((time, tag)) =
                        cooperative(stream.try_next())
                            .await
                            .map_err(CassandraStoreError::from)?
                    {
                        let tag = tag.unwrap_or_default();
                        scanned.insert(time, tag);
                        yield (time, tag);
                    }
                    *tags = scanned;
                }
                TimerState::Absent => {
                    // Post-V3 Absent is unambiguous: 0 timers, yield nothing.
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(state_cached = Empty))]
    fn get_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let key_clone = key.clone();
        let segment_id = self.segment.id;

        try_stream! {
            let (handle, cached) = self.resolve_state(&segment_id, &key_clone, timer_type).await?;
            Span::current().record("state_cached", cached);

            // Snapshot the state and release the lock before any DB I/O — a
            // concurrent transition would just shift the snapshot, which is
            // fine for a read-only stream.
            let state = handle.lock().await.clone();
            match state {
                TimerState::Inline(timer) => {
                    // Inline: yield trigger from cache (0 clustering query).
                    let context = self.propagator().extract(&timer.span);
                    yield Trigger::restored(key_clone.clone(), timer.time, timer_type, timer.tag, context);
                }
                TimerState::Overflow(_) => {
                    // Overflow: scan clustering rows.
                    let stream = self
                        .session()
                        .execute_iter(
                            self.queries().get_key_triggers.clone(),
                            (segment_id, key_clone.as_ref(), timer_type),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?
                        .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>, Option<i32>)>()
                        .map_err(CassandraStoreError::from)?;

                    pin_mut!(stream);
                    while let Some((_key_str, time, _timer_type, span_map, tag_opt)) =
                        cooperative(stream.try_next())
                            .await
                            .map_err(CassandraStoreError::from)?
                    {
                        let context = self.propagator().extract(&span_map);
                        let tag = tag_opt.unwrap_or(0_i32);
                        yield Trigger::restored(key_clone.clone(), time, timer_type, tag, context);
                    }
                }
                TimerState::Absent => {
                    // Post-V3 Absent is unambiguous: 0 timers, yield nothing.
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(state_cached = Empty))]
    fn get_key_triggers_all_types(
        &self,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let key_clone = key.clone();
        let segment_id = self.segment.id;

        try_stream! {
            // Read all types in a single query, then build the state_map.
            let raw_map = self.fetch_state_map(&segment_id, &key_clone).await?;
            let raw_map = raw_map.unwrap_or_default();

            let mut state_map: HashMap<TimerType, TimerState> =
                HashMap::with_capacity(TimerType::VARIANTS.len());

            for &tt in TimerType::VARIANTS {
                let state = raw_map
                    .get(&tt)
                    .and_then(Option::as_ref)
                    .cloned()
                    .unwrap_or(TimerState::Absent);
                state_map.insert(tt, state);
            }

            // Warm the per-type cache from the bulk read so subsequent
            // single-type operations on this key avoid separate DB reads.
            // Use get_value_or_guard_async to avoid overwriting an existing
            // Arc<AsyncMutex> handle held by a concurrent mutator.
            for (&tt, state) in &state_map {
                let cache_key = (key_clone.clone(), tt);
                if let Err(guard) = self.state_cache.get_value_or_guard_async(&cache_key).await {
                    let _ = guard.insert(Arc::new(AsyncMutex::new(state.clone())));
                }
            }

            // This path always reads from the DB (fetch_state_map is not
            // cache-aware).
            Span::current().record("state_cached", false);

            // Check if any type is Overflow — if so, clustering scan needed.
            let has_overflow = state_map.values().any(|s| matches!(s, TimerState::Overflow(_)));

            if has_overflow {
                // At least one type needs clustering — run the merge.
                let clustering_stream = self.session()
                    .execute_iter(
                        self.queries().get_key_triggers_all_types.clone(),
                        (segment_id, key_clone.as_ref()),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream::<(Option<String>, Option<CompactDateTime>, Option<TimerType>, Option<HashMap<String, String>>, Option<i32>)>()
                    .map_err(CassandraStoreError::from)?;

                pin_mut!(clustering_stream);

                // Merge two sorted sources by (timer_type, time):
                //
                //   Inline entries — TimerType::VARIANTS (i8-ascending), at
                //   most one trigger per type.
                //
                //   Clustering — Cassandra stream in (timer_type, time)
                //   order, skipping NULL static-only rows.

                let mut variants_iter = TimerType::VARIANTS.iter();
                let mut inline_next = advance_inline(
                    &key_clone,
                    &state_map,
                    &mut variants_iter,
                    self.propagator(),
                );

                // For each clustering row, flush any inline entries that
                // sort before it.
                while let Some(clustering) = advance_clustering(
                    &key_clone,
                    &mut clustering_stream,
                    self.propagator(),
                ).await? {
                    while let Some(s) = inline_next.take() {
                        if (s.timer_type, s.time) <= (clustering.timer_type, clustering.time) {
                            yield s;
                            inline_next = advance_inline(
                                &key_clone,
                                &state_map,
                                &mut variants_iter,
                                self.propagator(),
                            );
                        } else {
                            inline_next = Some(s);
                            break;
                        }
                    }
                    yield clustering;
                }

                // Drain remaining inline entries.
                while let Some(trigger) = inline_next {
                    yield trigger;
                    inline_next = advance_inline(
                        &key_clone,
                        &state_map,
                        &mut variants_iter,
                        self.propagator(),
                    );
                }
            } else {
                // All types are Inline or Absent — yield inline entries in
                // type order, no clustering query needed.
                for &tt in TimerType::VARIANTS {
                    if let Some(TimerState::Inline(timer)) = state_map.get(&tt) {
                        let context = self.propagator().extract(&timer.span);
                        yield Trigger::restored(key_clone.clone(), timer.time, tt, timer.tag, context);
                    }
                }
            }
        }
    }

    /// Upserts a trigger into the key index with state-aware transitions.
    ///
    /// Uses `resolve_state` (cache-first, warms all types on miss):
    /// - **Inline(old)**: Promote old timer to clustering + write new to
    ///   clustering + set overflow state → `Overflow`
    /// - **Overflow**: Write clustering only (1 query) → stays `Overflow`
    /// - **Absent**: Set inline state with new timer directly → `Inline(new)`
    ///   (post-V3 Absent is unambiguous: 0 timers, no clustering rows)
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn upsert_key_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let pending = PendingKeyTrigger::from_trigger(self.propagator(), &trigger);

        let (handle, cached) = self
            .resolve_state(&segment_id, &pending.id.key, pending.id.timer_type)
            .await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        let transition = KeyUpsertTransition::from_state(&guard, pending);
        *guard = transition.apply(self, &segment_id).await?;

        Ok(())
    }

    /// Deletes a specific trigger from the key index with state-aware
    /// demotion.
    ///
    /// Uses `resolve_state` (cache-first, warms all types on miss):
    /// - **Inline(timer), time matches**: Remove state entry → `Absent`
    /// - **Inline(timer), time mismatch**: No-op (Inline guarantees zero
    ///   clustering rows) → stays `Inline`
    /// - **Overflow**: One pre-delete read (LIMIT 3) drives a single atomic
    ///   batch:
    ///   - 0 surviving rows → batch DELETE target + DELETE `state[type]` →
    ///     `Absent`
    ///   - 1 surviving row → batch DELETE target + DELETE survivor + UPDATE
    ///     state Inline → `Inline(survivor)`
    ///   - 2+ surviving rows → single DELETE target → stays `Overflow`
    /// - **Absent**: No-op (post-V3 Absent is unambiguous: 0 timers, no rows)
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn delete_key_trigger(
        &self,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;

        let (handle, cached) = self.resolve_state(&segment_id, key, timer_type).await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        match &mut *guard {
            TimerState::Inline(timer) if timer.time == time => {
                // Inline timer matches the delete target → remove state, become Absent.
                // No clustering row exists for inline timers, so only remove state.
                self.remove_state_entry(&segment_id, key, timer_type)
                    .await?;
                *guard = TimerState::Absent;
            }
            TimerState::Overflow(tags) => {
                // Read pre-delete clustering rows (LIMIT 3) in one round-trip.
                // After filtering the target out, the survivor count drives
                // the post-delete state — and any survivor's data is already
                // in hand to feed straight into the atomic write batch below.
                let triggers = self
                    .peek_three_key_triggers(&segment_id, key, timer_type)
                    .await?;
                let mut survivors = triggers.into_iter().filter(|(t, ..)| *t != time);

                match (survivors.next(), survivors.next()) {
                    (None, _) => {
                        // No survivor → atomic DELETE target + DELETE state[type].
                        self.batch_delete_to_absent(&segment_id, key, timer_type, time)
                            .await?;
                        *guard = TimerState::Absent;
                    }
                    (Some((survivor_time, span_map, tag_opt)), None) => {
                        // Exactly one survivor → atomic DELETE target +
                        // DELETE survivor's clustering row + UPDATE state
                        // Inline(survivor). Heals any cross-process drift
                        // (e.g. cache says Overflow but DB has only the
                        // survivor) by promoting the survivor to Inline.
                        let new_state = TimerState::Inline(InlineTimer {
                            time: survivor_time,
                            span: span_map,
                            tag: tag_opt.unwrap_or(0_i32),
                        });
                        self.batch_delete_to_inline(
                            &segment_id,
                            key,
                            timer_type,
                            time,
                            survivor_time,
                            &new_state,
                        )
                        .await?;
                        *guard = new_state;
                    }
                    (Some(_), Some(_)) => {
                        // 2+ survivors → state stays Overflow, just delete
                        // the target row. Some target rows past clustering
                        // position 3 won't appear in the LIMIT 3 read — the
                        // DELETE is correct regardless (idempotent if absent).
                        self.execute_unpaged_discard(
                            &self.queries().delete_key_trigger,
                            (&segment_id, key.as_ref(), timer_type, time),
                        )
                        .await?;
                        tags.remove(time);
                    }
                }
            }
            TimerState::Inline(_) | TimerState::Absent => {
                // Inline state guarantees zero clustering rows — nothing to
                // delete. Post-V3 Absent is unambiguous: 0
                // timers, no clustering rows.
            }
        }

        Ok(())
    }

    /// Clears all triggers for a key/type with state awareness.
    ///
    /// Uses `resolve_state` to read the real DB state on cache miss, avoiding
    /// stale `Absent` entries if the DB operation later fails.
    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers(
        &self,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let (handle, _) = self.resolve_state(&segment_id, key, timer_type).await?;
        let mut guard = handle.lock().await;

        if matches!(*guard, TimerState::Absent) {
            return Ok(());
        }

        // Atomic BATCH: clear clustering rows + remove state entry.
        self.execute_unpaged_discard(
            &self.queries().batch_clear_key_triggers,
            (
                &segment_id,
                key.as_ref(),
                timer_type,
                timer_type,
                &segment_id,
                key.as_ref(),
            ),
        )
        .await?;

        *guard = TimerState::Absent;
        Ok(())
    }

    /// Atomically clears existing timers and schedules a new one in the key
    /// index.
    ///
    /// Uses `resolve_state` (cache-first) to select the write strategy:
    /// - **Inline or Absent**: plain UPDATE on the static column (0
    ///   tombstones). Post-V3 Absent is unambiguous — no clustering rows to
    ///   delete.
    /// - **Overflow**: BATCH (DELETE clustering + UPDATE state).
    ///
    /// `resolve_state` returns the per-`(key, timer_type)` mutex; holding
    /// `handle.lock().await` for the entire match serialises the
    /// read-decide-write sequence against concurrent same-key writers.
    async fn replace_key_trigger(&self, old: &Trigger, new: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let (handle, _) = self
            .resolve_state(&segment_id, &old.key, old.timer_type)
            .await?;
        let mut state = handle.lock().await;
        let pending = PendingKeyTrigger::from_trigger(self.propagator(), &new);
        match &mut *state {
            TimerState::Inline(_) | TimerState::Absent => {
                let (_, next) = pending.into_inline_state();
                self.set_state_inline(&segment_id, &old.key, old.timer_type, &next)
                    .await?;
                *state = next;
            }
            TimerState::Overflow(tags) => {
                self.execute_with_optional_ttl(
                    new.time,
                    &self.queries().replace_key_trigger,
                    &self.queries().replace_key_trigger_no_ttl,
                    |ttl| {
                        (
                            segment_id,
                            old.key.as_ref(),
                            old.timer_type,
                            old.time,
                            segment_id,
                            new.key.as_ref(),
                            new.timer_type,
                            new.time,
                            &pending.span_map,
                            new.tag,
                            ttl,
                        )
                    },
                    || {
                        (
                            segment_id,
                            old.key.as_ref(),
                            old.timer_type,
                            old.time,
                            segment_id,
                            new.key.as_ref(),
                            new.timer_type,
                            new.time,
                            &pending.span_map,
                            new.tag,
                        )
                    },
                )
                .await?;
                tags.remove(old.time);
                tags.insert(new.time, new.tag);
            }
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn clear_and_schedule_key(
        &self,
        trigger: Trigger,
    ) -> Result<SmallVec<[CompactDateTime; 1]>, Self::Error> {
        let segment_id = self.segment.id;
        // Extract span context for storage.
        let span_map = extract_span_map(self.propagator(), &trigger);

        let new_state = TimerState::Inline(InlineTimer {
            time: trigger.time,
            span: span_map,
            tag: trigger.tag,
        });

        let (handle, cached) = self
            .resolve_state(&segment_id, &trigger.key, trigger.timer_type)
            .await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        let old_times: SmallVec<[CompactDateTime; 1]> = match &*guard {
            TimerState::Absent => {
                // Fast path: no prior timer — plain UPDATE, no tombstone, no old times.
                self.set_state_inline(&segment_id, &trigger.key, trigger.timer_type, &new_state)
                    .await?;
                SmallVec::new()
            }
            TimerState::Inline(t) => {
                // Fast path: one prior timer already in the state column.
                // Return its time (if distinct) so the adapter can clean the slab index.
                let old_time = t.time;
                self.set_state_inline(&segment_id, &trigger.key, trigger.timer_type, &new_state)
                    .await?;
                if old_time == trigger.time {
                    SmallVec::new()
                } else {
                    SmallVec::from_buf([old_time])
                }
            }
            TimerState::Overflow(_) => {
                clear_overflow_and_schedule_key(self, &segment_id, &trigger, &new_state).await?
            }
        };

        *guard = new_state;
        Ok(old_times)
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers_all_types(&self, key: &Key) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;

        // Acquire per-key locks for all timer types before mutating.
        // Order is deterministic (matches TimerType discriminant order) so
        // two concurrent clear_key_triggers_all_types calls cannot deadlock.
        // Use resolve_state to read real DB state on cache miss, avoiding
        // stale Absent entries if DB operations later fail.
        let mut handles = Vec::with_capacity(TimerType::VARIANTS.len());
        for &tt in TimerType::VARIANTS {
            let (handle, _) = self.resolve_state(&segment_id, key, tt).await?;
            handles.push(handle);
        }
        let mut guards: Vec<_> = join_all(handles.iter().map(|h| h.lock())).await;

        if guards.iter().all(|g| matches!(**g, TimerState::Absent)) {
            return Ok(());
        }

        // Atomic BATCH: clear all clustering rows + clear entire state column.
        self.execute_unpaged_discard(
            &self.queries().batch_clear_key_triggers_all_types,
            (&segment_id, key.as_ref(), &segment_id, key.as_ref()),
        )
        .await?;

        // Update all cached states to Absent.
        for guard in &mut guards {
            **guard = TimerState::Absent;
        }

        Ok(())
    }

    /// Rotates the commit-oracle tag on an existing timer at `time`.
    ///
    /// **Precondition:** the caller must have observed the timer at `(key,
    /// time, timer_type)` as currently scheduled (today: from
    /// `complete()`-from-`FiringRescheduled`, where the row was just loaded
    /// into the active scheduler). Holding the per-key mutex serialises
    /// against concurrent in-process writers, so the row is guaranteed to
    /// exist for the duration of the write.
    ///
    /// Uses `resolve_state` (cache-first):
    /// - **Inline(timer), time matches**: rewrite the UDT in place.
    /// - **Inline(_), time mismatch** or **Absent**: no-op (target absent).
    /// - **Overflow**: bare `UPDATE` on the clustering row — no LWT, no
    ///   existence check.
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn update_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
        new_tag: i32,
    ) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let (handle, cached) = self.resolve_state(&segment_id, key, timer_type).await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        match &mut *guard {
            TimerState::Inline(timer) if timer.time == time => {
                let new_state = TimerState::Inline(InlineTimer {
                    time: timer.time,
                    span: timer.span.clone(),
                    tag: new_tag,
                });
                self.set_state_inline(&segment_id, key, timer_type, &new_state)
                    .await?;
                *guard = new_state;
            }
            // This coordinate has no key row to update. `mint` reuses a standing tag.
            // A same-coordinate `clear_and_schedule` during an attempt keeps that tag.
            // The later Complete from FiringRescheduled rotates it.
            TimerState::Inline(_) | TimerState::Absent => {}
            TimerState::Overflow(tags) => {
                self.execute_unpaged_discard(
                    &self.queries().update_tag,
                    (new_tag, &segment_id, key.as_ref(), timer_type, time),
                )
                .await?;
                tags.insert(time, new_tag);
            }
        }
        Ok(())
    }

    /// Reads a tag from the shared state cache when the list can answer.
    /// An unknown tag needs one clustering row read. Clones share this cache.
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn current_tag(
        &self,
        key: &Key,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> Result<Option<i32>, Self::Error> {
        let segment_id = self.segment.id;
        let (handle, cached) = self.resolve_state(&segment_id, key, timer_type).await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        match &mut *guard {
            TimerState::Inline(timer) if timer.time == time => Ok(Some(timer.tag)),
            TimerState::Inline(_) | TimerState::Absent => Ok(None),
            TimerState::Overflow(tags) => match tags.get(time) {
                TagLookup::Known(tag) => Ok(Some(tag)),
                TagLookup::Absent => Ok(None),
                TagLookup::Unknown => {
                    let row = self
                        .session()
                        .execute_unpaged(
                            &self.queries().current_tag_key,
                            (&segment_id, key.as_ref(), timer_type, time),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?
                        .into_rows_result()
                        .map_err(CassandraStoreError::from)?
                        .maybe_first_row::<(Option<i32>,)>()
                        .map_err(CassandraStoreError::from)?;
                    let tag = row.map(|(tag_opt,)| tag_opt.unwrap_or(0_i32));
                    if let Some(tag) = tag {
                        tags.insert(time, tag);
                    }
                    Ok(tag)
                }
            },
        }
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

async fn clear_overflow_and_schedule_key(
    store: &CassandraTriggerStore,
    segment_id: &SegmentId,
    trigger: &Trigger,
    new_state: &TimerState,
) -> Result<SmallVec<[CompactDateTime; 1]>, CassandraTriggerStoreError> {
    // Overflow: fetch clustering times directly while the caller holds the
    // per-key state lock. State is known Overflow, so skip resolve_state to
    // avoid a self-deadlock. Reuses the get_key_times prepared statement.
    let times = store
        .session()
        .execute_iter(
            store.queries().get_key_times.clone(),
            (segment_id, trigger.key.as_ref(), trigger.timer_type),
        )
        .await
        .map_err(CassandraStoreError::from)?
        .rows_stream::<(CompactDateTime, Option<i32>)>()
        .map_err(CassandraStoreError::from)?
        .map_err(CassandraStoreError::from)
        .map_ok(|(time, _)| time)
        .try_filter(|&time| ready(time != trigger.time))
        .try_collect()
        .await?;

    // BATCH (DELETE clustering + UPDATE state). Runs after the SELECT while
    // the lock is still held, so there is no TOCTOU window.
    store
        .batch_clear_and_set_inline(segment_id, &trigger.key, trigger.timer_type, new_state)
        .await?;

    Ok(times)
}

#[derive(Debug)]
struct PendingKeyTrigger {
    id: TriggerId,
    span_map: HashMap<String, String>,
    tag: i32,
}

impl PendingKeyTrigger {
    fn from_trigger(propagator: &TextMapCompositePropagator, trigger: &Trigger) -> Self {
        Self {
            id: trigger.id(),
            span_map: extract_span_map(propagator, trigger),
            tag: trigger.tag,
        }
    }

    fn into_inline_state(self) -> (TriggerId, TimerState) {
        let state = TimerState::Inline(InlineTimer {
            time: self.id.time,
            span: self.span_map,
            tag: self.tag,
        });
        (self.id, state)
    }

    fn clustering_entry(&self) -> ClusteringEntry<'_> {
        ClusteringEntry {
            time: self.id.time,
            span: &self.span_map,
            tag: self.tag,
        }
    }

    async fn insert_clustering(
        &self,
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
    ) -> Result<(), CassandraTriggerStoreError> {
        store
            .execute_with_optional_ttl(
                self.id.time,
                &store.queries().insert_key_trigger_clustering,
                &store.queries().insert_key_trigger_clustering_no_ttl,
                |ttl| {
                    (
                        segment_id,
                        self.id.key.as_ref(),
                        self.id.timer_type,
                        self.id.time,
                        &self.span_map,
                        self.tag,
                        ttl,
                    )
                },
                || {
                    (
                        segment_id,
                        self.id.key.as_ref(),
                        self.id.timer_type,
                        self.id.time,
                        &self.span_map,
                        self.tag,
                    )
                },
            )
            .await
    }
}

#[derive(Debug)]
enum KeyUpsertTransition {
    WriteInline(PendingKeyTrigger),
    PromoteToOverflow {
        existing: InlineTimer,
        new: PendingKeyTrigger,
    },
    UpsertClustering(PendingKeyTrigger, OverflowTags),
}

impl KeyUpsertTransition {
    fn from_state(state: &TimerState, new: PendingKeyTrigger) -> Self {
        match state {
            TimerState::Absent => Self::WriteInline(new),
            TimerState::Inline(existing) if existing.time == new.id.time => Self::WriteInline(new),
            TimerState::Inline(existing) => Self::PromoteToOverflow {
                existing: existing.clone(),
                new,
            },
            TimerState::Overflow(tags) => Self::UpsertClustering(new, tags.clone()),
        }
    }

    async fn apply(
        self,
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
    ) -> Result<TimerState, CassandraTriggerStoreError> {
        match self {
            Self::WriteInline(new) => {
                let (id, new_state) = new.into_inline_state();
                store
                    .set_state_inline(segment_id, &id.key, id.timer_type, &new_state)
                    .await?;
                Ok(new_state)
            }
            Self::PromoteToOverflow { existing, new } => {
                store
                    .batch_promote_and_set_overflow(
                        segment_id,
                        &new.id.key,
                        new.id.timer_type,
                        ClusteringEntry {
                            time: existing.time,
                            span: &existing.span,
                            tag: existing.tag,
                        },
                        new.clustering_entry(),
                    )
                    .await?;
                let mut tags = OverflowTags::Complete(SmallVec::new());
                tags.insert(existing.time, existing.tag);
                tags.insert(new.id.time, new.tag);
                Ok(TimerState::Overflow(tags))
            }
            Self::UpsertClustering(new, mut tags) => {
                new.insert_clustering(store, segment_id).await?;
                tags.insert(new.id.time, new.tag);
                Ok(TimerState::Overflow(tags))
            }
        }
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

/// Returns the next inline trigger in type order, or `None` when exhausted.
fn advance_inline<'a>(
    key: &Key,
    state_map: &HashMap<TimerType, TimerState>,
    variants_iter: &mut impl Iterator<Item = &'a TimerType>,
    propagator: &TextMapCompositePropagator,
) -> Option<Trigger> {
    let (&timer_type, timer) = variants_iter.find_map(|tt| {
        if let Some(TimerState::Inline(timer)) = state_map.get(tt) {
            Some((tt, timer))
        } else {
            None
        }
    })?;

    let context = propagator.extract(&timer.span);
    Some(Trigger::restored(
        key.clone(),
        timer.time,
        timer_type,
        timer.tag,
        context,
    ))
}

/// Returns the next clustering trigger, skipping NULL static-only rows.
async fn advance_clustering(
    key: &Key,
    stream: &mut (
             impl Stream<
        Item = Result<
            (
                Option<String>,
                Option<CompactDateTime>,
                Option<TimerType>,
                Option<HashMap<String, String>>,
                Option<i32>,
            ),
            impl Into<CassandraStoreError>,
        >,
    > + Unpin
         ),
    propagator: &TextMapCompositePropagator,
) -> Result<Option<Trigger>, CassandraTriggerStoreError> {
    while let Some((_key, time_opt, type_opt, span_opt, tag_opt)) =
        cooperative(stream.try_next()).await.map_err(Into::into)?
    {
        // Skip static-only rows (NULL clustering columns).
        let (Some(time), Some(timer_type), Some(span_map)) = (time_opt, type_opt, span_opt) else {
            continue;
        };
        let tag = tag_opt.unwrap_or(0_i32);

        let context = propagator.extract(&span_map);
        return Ok(Some(Trigger::restored(
            key.clone(),
            time,
            timer_type,
            tag,
            context,
        )));
    }
    Ok(None)
}
