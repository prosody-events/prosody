//! [`TriggerOperations`] implementation for [`CassandraTriggerStore`].
//!
//! Each method resolves the current [`TimerState`] (via the cache or a DB
//! read) before issuing writes, so it can pick the cheapest Cassandra
//! operation: a plain `UPDATE` for the common inline path, or a tombstone-free
//! BATCH only when clustering rows actually need to change.
//!
//! `get_key_triggers_all_types` is the most complex method: it reads the full
//! `state` map in one query, then merges inline entries (sorted by
//! `TimerType` discriminant) with a clustering-row stream in a single pass,
//! yielding triggers in `(timer_type, time)` order without issuing a
//! clustering scan for types that are already inline-or-absent.
//!
//! [`TriggerOperations`]: crate::timers::store::operations::TriggerOperations
//! [`CassandraTriggerStore`]: crate::timers::store::cassandra::CassandraTriggerStore
//! [`TimerState`]: crate::timers::store::cassandra::TimerState

use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::otel::SpanRelation;
use crate::related_span;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::migration;
use crate::timers::store::cassandra::state::{ClusteringEntry, InlineTimer, TimerState};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{Segment, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, future::join_all, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use strum::VariantArray;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::coop::cooperative;
use tracing::field::Empty;
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

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
        range: RangeInclusive<SlabId>,
    ) -> impl Stream<Item = Result<SlabId, Self::Error>> + Send {
        let segment_id = self.segment.id;
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
                .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>)>().map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type, span_map)) =
                cooperative(stream.try_next()).await.map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = related_span!(SpanRelation::Child, context.clone(), "fetch_slab_trigger");

                yield Trigger::new(key.into(), time, timer_type, span);
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_slab_triggers_all_types(
        &self,
        slab: &Slab,
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
                .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type, span_map)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = related_span!(SpanRelation::Child, context.clone(), "fetch_slab_trigger_all_types");

                yield Trigger::new(key.into(), time, timer_type, span);
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
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        let key_clone = key.clone();
        let segment_id = self.segment.id;

        try_stream! {
            let (handle, cached) = self.resolve_state(&segment_id, &key_clone, timer_type).await?;
            Span::current().record("state_cached", cached);

            // Lock briefly to read the state, then release before any DB I/O.
            let state = handle.lock().await.clone();
            match state {
                TimerState::Inline(timer) => {
                    // Inline: yield time from cache (0 clustering query).
                    yield timer.time;
                }
                TimerState::Overflow => {
                    // Overflow: scan clustering rows.
                    let stream = self
                        .session()
                        .execute_iter(
                            self.queries().get_key_times.clone(),
                            (segment_id, key_clone.as_ref(), timer_type),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?
                        .rows_stream::<(CompactDateTime,)>()
                        .map_err(CassandraStoreError::from)?;

                    pin_mut!(stream);
                    while let Some((time,)) =
                        cooperative(stream.try_next())
                            .await
                            .map_err(CassandraStoreError::from)?
                    {
                        yield time;
                    }
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

            // Lock briefly to read the state, then release before any DB I/O.
            let state = handle.lock().await.clone();
            match state {
                TimerState::Inline(timer) => {
                    // Inline: yield trigger from cache (0 clustering query).
                    let context = self.propagator().extract(&timer.span);
                    let span = related_span!(SpanRelation::Child, context.clone(), "fetch_key_trigger_inline");
                    yield Trigger::new(key_clone.clone(), timer.time, timer_type, span);
                }
                TimerState::Overflow => {
                    // Overflow: scan clustering rows.
                    let stream = self
                        .session()
                        .execute_iter(
                            self.queries().get_key_triggers.clone(),
                            (segment_id, key_clone.as_ref(), timer_type),
                        )
                        .await
                        .map_err(CassandraStoreError::from)?
                        .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>)>()
                        .map_err(CassandraStoreError::from)?;

                    pin_mut!(stream);
                    while let Some((_key_str, time, _timer_type, span_map)) =
                        cooperative(stream.try_next())
                            .await
                            .map_err(CassandraStoreError::from)?
                    {
                        let context = self.propagator().extract(&span_map);
                        let span = related_span!(SpanRelation::Child, context.clone(), "fetch_key_trigger");
                        yield Trigger::new(key_clone.clone(), time, timer_type, span);
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
            let has_overflow = state_map.values().any(|s| matches!(s, TimerState::Overflow));

            if has_overflow {
                // At least one type needs clustering — run the merge.
                let clustering_stream = self.session()
                    .execute_iter(
                        self.queries().get_key_triggers_all_types.clone(),
                        (segment_id, key_clone.as_ref()),
                    )
                    .await
                    .map_err(CassandraStoreError::from)?
                    .rows_stream()
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
                    &key_clone, &state_map, &mut variants_iter, self.propagator(),
                );

                // For each clustering row, flush any inline entries that
                // sort before it.
                while let Some(clustering) = advance_clustering(
                    &key_clone, &mut clustering_stream, self.propagator(),
                ).await? {
                    while let Some(s) = inline_next.take() {
                        if (s.timer_type, s.time) <= (clustering.timer_type, clustering.time) {
                            yield s;
                            inline_next = advance_inline(
                                &key_clone, &state_map, &mut variants_iter, self.propagator(),
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
                        &key_clone, &state_map, &mut variants_iter, self.propagator(),
                    );
                }
            } else {
                // All types are Inline or Absent — yield inline entries in
                // type order, no clustering query needed.
                for &tt in TimerType::VARIANTS {
                    if let Some(TimerState::Inline(timer)) = state_map.get(&tt) {
                        let context = self.propagator().extract(&timer.span);
                        let span = related_span!(SpanRelation::Child, context.clone(), "fetch_key_trigger_inline");
                        yield Trigger::new(key_clone.clone(), timer.time, tt, span);
                    }
                }
            }
        }
    }

    /// Inserts a trigger into the key index with state-aware transitions.
    ///
    /// Uses `resolve_state` (cache-first, warms all types on miss):
    /// - **Inline(old)**: Promote old timer to clustering + write new to
    ///   clustering + set overflow state → `Overflow`
    /// - **Overflow**: Write clustering only (1 query) → stays `Overflow`
    /// - **Absent**: Set inline state with new timer directly → `Inline(new)`
    ///   (post-V3 Absent is unambiguous: 0 timers, no clustering rows)
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn insert_key_trigger(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        let timer_type = trigger.timer_type;
        let key = trigger.key.clone();

        let (handle, cached) = self.resolve_state(&segment_id, &key, timer_type).await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        match &*guard {
            TimerState::Inline(old_timer) if old_timer.time == trigger.time => {
                // Same time already stored inline — no-op.
            }
            TimerState::Inline(old_timer) => {
                // Promote: old inline → clustering, new → clustering, state → Overflow.
                // All three writes are issued as a single UNLOGGED BATCH so the
                // transition is atomic at the partition level.
                let new_span_map = extract_span_map(self.propagator(), &trigger);

                self.batch_promote_and_set_overflow(
                    &segment_id,
                    &key,
                    timer_type,
                    ClusteringEntry {
                        time: old_timer.time,
                        span: &old_timer.span.clone(),
                    },
                    ClusteringEntry {
                        time: trigger.time,
                        span: &new_span_map,
                    },
                )
                .await?;
                *guard = TimerState::Overflow;
            }
            TimerState::Overflow => {
                // Already overflow: write clustering only. State is unchanged.
                self.add_key_trigger_clustering(&segment_id, trigger)
                    .await?;
            }
            TimerState::Absent => {
                // Post-V3 Absent is unambiguous: 0 timers, no clustering rows.
                // Set inline state directly.
                let span_map = extract_span_map(self.propagator(), &trigger);

                let new_state = TimerState::Inline(InlineTimer {
                    time: trigger.time,
                    span: span_map,
                });
                self.set_state_inline(&segment_id, &key, timer_type, &new_state)
                    .await?;
                *guard = new_state;
            }
        }

        Ok(())
    }

    /// Deletes a specific trigger from the key index with state-aware
    /// demotion.
    ///
    /// Uses `resolve_state` (cache-first, warms all types on miss):
    /// - **Inline(timer), time matches**: Remove state entry → `Absent`
    /// - **Inline(timer), time mismatch**: No-op (Inline guarantees zero
    ///   clustering rows) → stays `Inline`
    /// - **Overflow**: Delete clustering row + peek remaining (LIMIT 2):
    ///   - 0 remaining → remove state entry → `Absent`
    ///   - 1 remaining → set inline state + delete clustering row →
    ///     `Inline(remaining)`
    ///   - 2+ remaining → no state change → stays `Overflow`
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
        match &*guard {
            TimerState::Inline(timer) if timer.time == time => {
                // Inline timer matches the delete target → remove state, become Absent.
                // No clustering row exists for inline timers, so only remove state.
                self.remove_state_entry(&segment_id, key, timer_type)
                    .await?;
                *guard = TimerState::Absent;
            }
            TimerState::Overflow => {
                // Delete the clustering row first.
                self.execute_unpaged_discard(
                    &self.queries().delete_key_trigger,
                    (&segment_id, key.as_ref(), timer_type, time),
                )
                .await?;

                // Count remaining times (LIMIT 2, no span — cheap).
                let times = self
                    .peek_trigger_times(&segment_id, key, timer_type)
                    .await?;

                match times.as_slice() {
                    [] => {
                        // 0 remaining → Absent.
                        self.remove_state_entry(&segment_id, key, timer_type)
                            .await?;
                        *guard = TimerState::Absent;
                    }
                    [remaining_time] => {
                        // 1 remaining → fetch span and demote to Inline.
                        let Some((confirmed_time, span_map)) = self
                            .peek_first_key_trigger(&segment_id, key, timer_type)
                            .await?
                        else {
                            // Row vanished between count and read — treat as Absent.
                            self.remove_state_entry(&segment_id, key, timer_type)
                                .await?;
                            *guard = TimerState::Absent;
                            return Ok(());
                        };

                        let new_state = TimerState::Inline(InlineTimer {
                            time: *remaining_time,
                            span: span_map,
                        });

                        // Atomic batch: set inline state + delete remaining clustering row.
                        self.batch_demote_to_inline(
                            &segment_id,
                            key,
                            timer_type,
                            confirmed_time,
                            &new_state,
                        )
                        .await?;

                        *guard = new_state;
                    }
                    _ => {
                        // 2+ remaining → stays Overflow, no state change
                        // needed.
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
    /// `resolve_state` returns a per-key `Arc<AsyncMutex<TimerState>>`; holding
    /// the `handle.lock().await` guard serializes the read-decide-write
    /// sequence.
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn clear_and_schedule_key(&self, trigger: Trigger) -> Result<(), Self::Error> {
        let segment_id = self.segment.id;
        // Extract span context for storage.
        let span_map = extract_span_map(self.propagator(), &trigger);

        let new_state = TimerState::Inline(InlineTimer {
            time: trigger.time,
            span: span_map,
        });

        let (handle, cached) = self
            .resolve_state(&segment_id, &trigger.key, trigger.timer_type)
            .await?;
        Span::current().record("state_cached", cached);

        let mut guard = handle.lock().await;
        match &*guard {
            TimerState::Inline(_) | TimerState::Absent => {
                // Fast path: Inline or Absent → plain UPDATE, no tombstone.
                // Post-V3 Absent guarantees no clustering rows.
                self.set_state_inline(&segment_id, &trigger.key, trigger.timer_type, &new_state)
                    .await?;
            }
            TimerState::Overflow => {
                // Overflow: BATCH (DELETE clustering + UPDATE state).
                self.batch_clear_and_set_inline(
                    &segment_id,
                    &trigger.key,
                    trigger.timer_type,
                    &new_state,
                )
                .await?;
            }
        }

        *guard = new_state;
        Ok(())
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

/// Injects the trigger's span context into a new `HashMap` for Cassandra
/// storage.
pub(super) fn extract_span_map(
    propagator: &TextMapCompositePropagator,
    trigger: &Trigger,
) -> HashMap<String, String> {
    let mut span_map = HashMap::with_capacity(2);
    let context = trigger.span.load().context();
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
    let span = related_span!(
        SpanRelation::Child,
        context.clone(),
        "fetch_key_trigger_all_types_inline"
    );
    Some(Trigger::new(key.clone(), timer.time, timer_type, span))
}

/// Returns the next clustering trigger, skipping NULL static-only rows.
pub(super) async fn advance_clustering(
    key: &Key,
    stream: &mut (
             impl Stream<
        Item = Result<
            (
                Option<String>,
                Option<CompactDateTime>,
                Option<TimerType>,
                Option<HashMap<String, String>>,
            ),
            impl Into<CassandraStoreError>,
        >,
    > + Unpin
         ),
    propagator: &TextMapCompositePropagator,
) -> Result<Option<Trigger>, CassandraTriggerStoreError> {
    while let Some((_key, time_opt, type_opt, span_opt)) =
        cooperative(stream.try_next()).await.map_err(Into::into)?
    {
        // Skip static-only rows (NULL clustering columns).
        let (Some(time), Some(timer_type), Some(span_map)) = (time_opt, type_opt, span_opt) else {
            continue;
        };

        let context = propagator.extract(&span_map);
        let span = related_span!(SpanRelation::Child, context.clone(), "fetch_key_trigger_all_types");

        return Ok(Some(Trigger::new(key.clone(), time, timer_type, span)));
    }
    Ok(None)
}
