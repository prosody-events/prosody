use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::SegmentId;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::state::{
    CachedState, ClusteringEntry, PeekedTrigger, TimerState,
};
use crate::timers::store::cassandra::trigger_store::extract_span_map;
use crate::timers::{TimerType, Trigger};
use scylla::errors::MaybeFirstRowError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

impl CassandraTriggerStore {
    /// Fetches the `state` map from the database.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn fetch_state_map(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<Option<HashMap<TimerType, Option<TimerState>>>, CassandraTriggerStoreError> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_state, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<HashMap<TimerType, Option<TimerState>>>,)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row.and_then(|(map,)| map))
    }

    /// Reads a single entry from the `state` map for a key and timer type.
    ///
    /// Uses `SELECT state[?]` to read only the requested type. Both a missing
    /// partition (no row) and a `NULL` map entry resolve to `Absent`.
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn fetch_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<TimerState, CassandraTriggerStoreError> {
        let row = self
            .session()
            .execute_unpaged(
                &self.queries().get_state_entry,
                (timer_type, segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<TimerState>,)>()
            .map_err(CassandraStoreError::from)?;

        // No row → partition absent. Some(None) → map entry absent. Both → Absent.
        Ok(row.and_then(|(entry,)| entry).unwrap_or(TimerState::Absent))
    }

    /// Resolves the timer state for a `(key, timer_type)` with cache-first
    /// semantics.
    ///
    /// Logic:
    /// 1. Cache hit → clone the `Arc<AsyncMutex<TimerState>>` and return
    ///    `(handle, true)` immediately.
    /// 2. Cache miss → fetch state from DB, wrap in a new
    ///    `Arc<AsyncMutex<TimerState>>`, insert into cache, return `(handle,
    ///    false)`.
    ///
    /// The returned `Arc` handle lets callers lock the mutex after dropping
    /// all cache-internal guards (no await across a shard lock).  The `bool`
    /// indicates whether the result was a cache hit; callers record it as the
    /// `state_cached` tracing field.
    ///
    /// Post-V3, a NULL/missing MAP entry unambiguously means "new key, 0
    /// timers," so all states (including `Absent`) are cached.
    ///
    /// # Errors
    ///
    /// Returns error if the DB read fails on a cache miss.
    pub(super) async fn resolve_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(CachedState, bool), CassandraTriggerStoreError> {
        let cache_key = (key.clone(), timer_type);

        // Fast path: cache hit — clone the Arc, shard lock drops immediately.
        match self.state_cache.get_value_or_guard_async(&cache_key).await {
            Ok(handle) => Ok((handle, true)),
            Err(guard) => {
                // Cache miss: read only the requested type from DB.
                let state = self.fetch_state(segment_id, key, timer_type).await?;
                let handle = Arc::new(AsyncMutex::new(state));
                // Ignore eviction: if the entry was evicted between the miss
                // and here, the next caller will re-fetch from DB.
                let _ = guard.insert(handle.clone());
                Ok((handle, false))
            }
        }
    }

    /// Atomically clears clustering rows and sets inline state.
    ///
    /// Uses a Cassandra BATCH to:
    /// 1. DELETE all clustering rows for this key/type (removes old timers)
    /// 2. UPDATE the `state[type]` with the new inline timer data
    ///
    /// This is the safe path for Overflow→Inline and DB-Absent→Inline
    /// transitions.
    ///
    /// # Errors
    ///
    /// Returns error if the database batch execution fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn batch_clear_and_set_inline(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        state: &TimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        let TimerState::Inline(timer) = state else {
            return Err(CassandraTriggerStoreError::AbsentStateNotSerializable);
        };
        self.execute_with_optional_ttl(
            timer.time,
            &self.queries().batch_clear_and_set_inline,
            &self.queries().batch_clear_and_set_inline_no_ttl,
            |ttl| {
                (
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    ttl,
                    timer_type,
                    state,
                    segment_id,
                    key.as_ref(),
                )
            },
            || {
                (
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    timer_type,
                    state,
                    segment_id,
                    key.as_ref(),
                )
            },
        )
        .await
    }

    /// Sets inline timer state (static column only) without touching clustering
    /// rows.
    ///
    /// This is the fast path for Inline→Inline replacement: no BATCH,
    /// no DELETE, no range tombstone. Safe because the caller knows the state
    /// is `Inline` (no clustering rows for this key/type).
    ///
    /// # Errors
    ///
    /// Returns error if the database update fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn set_state_inline(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        state: &TimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        let TimerState::Inline(timer) = state else {
            return Err(CassandraTriggerStoreError::AbsentStateNotSerializable);
        };
        self.execute_with_optional_ttl(
            timer.time,
            &self.queries().set_state_inline,
            &self.queries().set_state_inline_no_ttl,
            |ttl| (ttl, timer_type, state, segment_id, key.as_ref()),
            || (timer_type, state, segment_id, key.as_ref()),
        )
        .await
    }

    /// Sets overflow state marker for a key/type.
    ///
    /// Writes `{inline: false, time: null, span: null}` to the state column.
    ///
    /// # Errors
    ///
    /// Returns error if the database update fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn set_state_overflow(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.session()
            .execute_unpaged(
                &self.queries().set_state_overflow,
                (timer_type, &TimerState::Overflow, segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Removes a state entry for a single timer type (returns to Absent).
    ///
    /// # Errors
    ///
    /// Returns error if the database update fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn remove_state_entry(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.session()
            .execute_unpaged(
                &self.queries().remove_state_entry,
                (timer_type, segment_id, key.as_ref()),
            )
            .await
            .map_err(CassandraStoreError::from)?;

        Ok(())
    }

    /// Reads up to 2 remaining clustering trigger times for a key/type.
    ///
    /// Used for the demotion check after deleting a clustering row:
    /// - 0 remaining → state becomes `Absent`
    /// - 1 remaining → demote to `Inline`
    /// - 2 remaining → state stays `Overflow`
    ///
    /// Also used by migration tests to verify singleton normalization deleted
    /// the clustering row.
    ///
    /// Fetches only `time` — no span — to keep the result small.
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn peek_trigger_times(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, CassandraTriggerStoreError> {
        let rows_result = self
            .session()
            .execute_unpaged(
                &self.queries().count_key_triggers,
                (segment_id, key.as_ref(), timer_type),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?;

        let mut out = Vec::with_capacity(2);
        for row in rows_result
            .rows::<(CompactDateTime,)>()
            .map_err(CassandraStoreError::from)?
        {
            let (time,) = row.map_err(|e| {
                CassandraStoreError::from(MaybeFirstRowError::DeserializationFailed(e))
            })?;
            out.push(time);
        }
        Ok(out)
    }

    /// Reads the first remaining clustering trigger (time + span) for a
    /// key/type.
    ///
    /// Used in the `1`-remaining demotion branch of `delete_key_trigger` and
    /// `backfill_key_state` to retrieve the span needed to build inline state.
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn peek_first_key_trigger(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Option<PeekedTrigger>, CassandraTriggerStoreError> {
        Ok(self
            .session()
            .execute_unpaged(
                &self.queries().peek_first_key_trigger,
                (segment_id, key.as_ref(), timer_type),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<PeekedTrigger>()
            .map_err(CassandraStoreError::from)?)
    }

    /// Backfills the key state MAP entry for a single `(key, timer_type)` pair.
    ///
    /// Used during V2→V3 migration. For keys that have clustering rows but no
    /// state MAP entry, this method writes the appropriate state:
    /// - If state already non-Absent: skip (idempotent).
    /// - 0 clustering rows: skip (stale slab entry, truly absent).
    /// - 1 clustering row: normalize to inline state (concurrent
    ///   `set_state_inline` + delete clustering row).
    /// - ≥2 clustering rows: set overflow marker.
    ///
    /// # Errors
    ///
    /// Returns error if any DB operation fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn backfill_key_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), CassandraTriggerStoreError> {
        use crate::timers::store::cassandra::state::InlineTimer;

        // Skip if already has state (idempotency guard).
        // Uses fetch_state (DB-direct) because backfill writes via set_state_inline /
        // set_state_overflow without updating the in-memory cache.
        let state = self.fetch_state(segment_id, key, timer_type).await?;
        if !matches!(state, TimerState::Absent) {
            return Ok(());
        }

        // Count clustering rows (LIMIT 2, time only — no span allocation).
        let times = self.peek_trigger_times(segment_id, key, timer_type).await?;

        match times.as_slice() {
            [] => {
                // Stale slab entry — no clustering rows exist, nothing to do.
            }
            [remaining_time] => {
                // Exactly 1 row: fetch the span and normalize to inline state.
                let Some((confirmed_time, span_map)) = self
                    .peek_first_key_trigger(segment_id, key, timer_type)
                    .await?
                else {
                    // Row vanished between count and read — skip.
                    return Ok(());
                };

                let state = TimerState::Inline(InlineTimer {
                    time: *remaining_time,
                    span: span_map,
                });

                // Concurrent: write inline state + delete clustering row.
                tokio::try_join!(
                    self.set_state_inline(segment_id, key, timer_type, &state),
                    self.execute_unpaged_discard(
                        &self.queries().delete_key_trigger,
                        (segment_id, key.as_ref(), timer_type, confirmed_time),
                    )
                )?;
            }
            _ => {
                // ≥2 rows: mark as overflow.
                self.set_state_overflow(segment_id, key, timer_type).await?;
            }
        }

        Ok(())
    }

    /// Atomically inserts two clustering rows and sets overflow state.
    ///
    /// Uses a Cassandra UNLOGGED BATCH to:
    /// 1. INSERT the promoted (previously-inline) timer into clustering rows
    /// 2. INSERT the new timer into clustering rows
    /// 3. UPDATE the `state[type]` to `Overflow`
    ///
    /// All three statements target the same `(segment_id, key)` partition, so
    /// an unlogged batch is correct and carries no cross-partition coordination
    /// overhead. This is the safe path for the Inline→Overflow transition.
    ///
    /// TTL is computed from the later of the two trigger times so that neither
    /// clustering row expires before the other timer fires. The batch-level
    /// `USING TTL` applies to both INSERTs; the state UPDATE carries no TTL,
    /// matching the behaviour of the standalone `set_state_overflow` call.
    ///
    /// # Errors
    ///
    /// Returns error if the database batch execution fails.
    #[instrument(level = "debug", skip(self), err)]
    pub(super) async fn batch_promote_and_set_overflow(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        promoted: ClusteringEntry<'_>,
        new: ClusteringEntry<'_>,
    ) -> Result<(), CassandraTriggerStoreError> {
        let overflow_state = TimerState::Overflow;
        let ttl_time = promoted.time.max(new.time);
        self.execute_with_optional_ttl(
            ttl_time,
            &self.queries().batch_promote_and_set_overflow,
            &self.queries().batch_promote_and_set_overflow_no_ttl,
            |ttl| {
                (
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    promoted.time,
                    promoted.span,
                    ttl,
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    new.time,
                    new.span,
                    ttl,
                    timer_type,
                    &overflow_state,
                    segment_id,
                    key.as_ref(),
                )
            },
            || {
                (
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    promoted.time,
                    promoted.span,
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    new.time,
                    new.span,
                    timer_type,
                    &overflow_state,
                    segment_id,
                    key.as_ref(),
                )
            },
        )
        .await
    }

    /// Inserts a trigger into clustering columns only.
    ///
    /// Used when multiple timers exist for a key/type. Does not touch the
    /// state column.
    ///
    /// # Errors
    ///
    /// Returns error if the database insert fails.
    #[instrument(level = "debug", skip(self), err)]
    pub async fn add_key_trigger_clustering(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), CassandraTriggerStoreError> {
        let span_map = extract_span_map(self.propagator(), &trigger);

        let key = trigger.key.as_ref();
        let time = trigger.time;
        let timer_type = trigger.timer_type;

        self.execute_with_optional_ttl(
            trigger.time,
            &self.queries().insert_key_trigger_clustering,
            &self.queries().insert_key_trigger_clustering_no_ttl,
            |ttl| (segment_id, key, timer_type, time, &span_map, ttl),
            || (segment_id, key, timer_type, time, &span_map),
        )
        .await
    }
}
