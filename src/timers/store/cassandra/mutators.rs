//! Cassandra-specific state-column helpers.
//!
//! The `state` map column is the core of the V3 tombstone-reduction design:
//! every write to the key index goes through one of the helpers here, which
//! choose the cheapest Cassandra operation for the current state transition.
//! Specifically:
//!
//! - `Absent → Inline`: plain `UPDATE state[type] = …` — zero tombstones.
//! - `Inline → Inline`: same plain `UPDATE` — no BATCH, no DELETE.
//! - `Inline → Overflow`: an unlogged BATCH that promotes the old inline timer
//!   into a new clustering row at the same time as the new one is inserted,
//!   keeping the transition atomic at the partition level.
//! - `Overflow → Inline` and `Overflow → Absent`: a BATCH that range-deletes
//!   the type's clustering rows and writes the new state atomically.
//!
//! Tombstone invariant: every clustering delete is a range or partition
//! tombstone. A delete of one timer covers the whole gap between its live
//! neighbors, so it also covers every earlier tombstone in that gap. A read
//! therefore counts a few range markers for each gap it crosses, and no read
//! scans a graveyard of fired timers.
//!
//! A range tombstone also hides an older write in its gap. The owner's session
//! stamps writes in order, so its later schedules survive. After a rebalance,
//! a new owner whose clock lags can lose a schedule in a recent gap.
//!
//! `resolve_state` shares cached state across clones of the partition's store.
//! Callers hold the returned lock through each read or write for that key and
//! type.

use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::SegmentId;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::state::{
    CachedState, ClusteringEntry, InlineTimer, PeekedTrigger, TimerState,
};
use crate::timers::store::cassandra::trigger_store::extract_span_map;
use crate::timers::{TimerType, Trigger};
use scylla::SerializeRow;
use scylla::errors::MaybeFirstRowError;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

/// Bind values for `batch_promote_and_set_overflow` (TTL variant).
///
/// Named markers are required because the bind value count exceeds the
/// 16-element `SerializeRow` tuple limit.
#[derive(SerializeRow)]
struct PromoteOverflowParams<'a> {
    // INSERT 1: promoted timer
    p_segment_id: &'a SegmentId,
    p_key: &'a str,
    p_timer_type: TimerType,
    p_time: CompactDateTime,
    p_span: &'a HashMap<String, String>,
    p_tag: i32,
    p_ttl: i32,
    // INSERT 2: new timer
    n_segment_id: &'a SegmentId,
    n_key: &'a str,
    n_timer_type: TimerType,
    n_time: CompactDateTime,
    n_span: &'a HashMap<String, String>,
    n_tag: i32,
    n_ttl: i32,
    // UPDATE: state
    s_ttl: i32,
    s_timer_type: TimerType,
    s_state: &'a TimerState,
    s_segment_id: &'a SegmentId,
    s_key: &'a str,
}

/// Returns the inline timer inside `state`, or `AbsentStateNotSerializable`
/// for `Overflow`/`Absent`. Every write that persists `state` to the static
/// column needs the payload it carries, and only `Inline` carries one.
fn require_inline(state: &TimerState) -> Result<&InlineTimer, CassandraTriggerStoreError> {
    match state {
        TimerState::Inline(timer) => Ok(timer),
        TimerState::Overflow | TimerState::Absent => {
            Err(CassandraTriggerStoreError::AbsentStateNotSerializable)
        }
    }
}

impl CassandraTriggerStore {
    /// Fetches the `state` map from the database.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
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
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
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

        // No row → partition absent. Some(None) → map entry absent. Both →
        // Absent.
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
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn batch_clear_and_set_inline(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        state: &TimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        let timer = require_inline(state)?;
        let ttl = self.calculate_ttl(timer.time);
        self.execute_unpaged_discard(
            &self.queries().batch_clear_and_set_inline,
            (
                segment_id,
                key.as_ref(),
                timer_type,
                ttl,
                timer_type,
                state,
                segment_id,
                key.as_ref(),
            ),
        )
        .await
    }

    /// Atomically clears clustering rows and removes the state entry.
    ///
    /// One BATCH range-deletes the type's clustering rows and deletes
    /// `state[type]`. Every transition to `Absent` that can leave clustering
    /// rows goes through here.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn batch_clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), CassandraTriggerStoreError> {
        let key = key.as_ref();
        self.execute_unpaged_discard(
            &self.queries().batch_clear_key_triggers,
            (segment_id, key, timer_type, timer_type, segment_id, key),
        )
        .await
    }

    /// Sets inline timer state (static column only) without touching clustering
    /// rows.
    ///
    /// This is the fast path for Inline→Inline replacement: no BATCH,
    /// no DELETE, no range tombstone. Safe because the caller knows the state
    /// is `Inline` (no clustering rows for this key/type).
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn set_state_inline(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        state: &TimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        let timer = require_inline(state)?;
        let ttl = self.calculate_ttl(timer.time);
        self.execute_unpaged_discard(
            &self.queries().set_state_inline,
            (ttl, timer_type, state, segment_id, key.as_ref()),
        )
        .await
    }

    /// Sets overflow state marker for a key/type.
    ///
    /// Writes `{inline: false, time: null, span: null}` to the state column.
    /// A TTL is applied (derived from `ttl_time`) so the marker expires no
    /// later than the clustering rows it describes, preventing zombie Overflow
    /// entries if those rows expire via TTL without an explicit deletion.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn set_state_overflow(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
        ttl_time: CompactDateTime,
    ) -> Result<(), CassandraTriggerStoreError> {
        let ttl = self.calculate_ttl(ttl_time);
        self.execute_unpaged_discard(
            &self.queries().set_state_overflow,
            (
                ttl,
                timer_type,
                &TimerState::Overflow,
                segment_id,
                key.as_ref(),
            ),
        )
        .await
    }

    /// Removes a state entry for a single timer type (returns to Absent).
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
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

    /// Reads at most 2 clustering rows of one type with a `peek_*_key_triggers`
    /// statement.
    #[instrument(level = "debug", skip(self, statement), err(level = "warn"))]
    pub(super) async fn peek_key_triggers(
        &self,
        statement: &PreparedStatement,
        values: impl SerializeRow + Debug,
    ) -> Result<SmallVec<[InlineTimer; 2]>, CassandraTriggerStoreError> {
        let rows_result = self
            .session()
            .execute_unpaged(statement, values)
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?;

        let rows = rows_result
            .rows::<PeekedTrigger>()
            .map_err(CassandraStoreError::from)?;
        Ok(rows
            .map(|row| {
                row.map(InlineTimer::from).map_err(|e| {
                    CassandraStoreError::from(MaybeFirstRowError::DeserializationFailed(e))
                })
            })
            .collect::<Result<_, _>>()?)
    }

    /// Backfills the key state MAP entry for a single `(key, timer_type)` pair.
    ///
    /// Used during V2→V3 migration. For keys that have clustering rows but no
    /// state MAP entry, this method writes the appropriate state:
    /// - If state already non-Absent: skip (idempotent).
    /// - 0 clustering rows: skip (stale slab entry, truly absent).
    /// - 1 clustering row: move it into inline state.
    /// - ≥2 clustering rows: set overflow marker.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn backfill_key_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(), CassandraTriggerStoreError> {
        // Skip if already has state (idempotency guard).
        // Uses fetch_state (DB-direct) because backfill writes without updating
        // the in-memory cache.
        let state = self.fetch_state(segment_id, key, timer_type).await?;
        if !matches!(state, TimerState::Absent) {
            return Ok(());
        }

        let mut rows = self
            .peek_key_triggers(
                &self.queries().peek_last_key_triggers,
                (segment_id, key.as_ref(), timer_type),
            )
            .await?
            .into_iter();

        match (rows.next(), rows.next()) {
            // Stale slab entry: no clustering rows exist.
            (None, _) => {}
            (Some(row), None) => {
                let state = TimerState::Inline(row);
                self.batch_clear_and_set_inline(segment_id, key, timer_type, &state)
                    .await?;
            }
            // The marker expires with the latest row.
            (Some(latest), Some(_)) => {
                self.set_state_overflow(segment_id, key, timer_type, latest.time)
                    .await?;
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
    /// clustering row expires before the other timer fires. The same TTL is
    /// applied to both INSERTs and to the state UPDATE, so the Overflow marker
    /// expires together with the last clustering row it describes — preventing
    /// zombie markers if those rows later expire via TTL without explicit
    /// deletion.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
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
        let key_ref = key.as_ref();

        // The bind count exceeds the 16-element `SerializeRow` tuple limit,
        // so `PromoteOverflowParams` carries the parameters.
        let ttl = self.calculate_ttl(ttl_time);
        self.execute_unpaged_discard(
            &self.queries().batch_promote_and_set_overflow,
            PromoteOverflowParams {
                p_segment_id: segment_id,
                p_key: key_ref,
                p_timer_type: timer_type,
                p_time: promoted.time,
                p_span: promoted.span,
                p_tag: promoted.tag,
                p_ttl: ttl,
                n_segment_id: segment_id,
                n_key: key_ref,
                n_timer_type: timer_type,
                n_time: new.time,
                n_span: new.span,
                n_tag: new.tag,
                n_ttl: ttl,
                s_ttl: ttl,
                s_timer_type: timer_type,
                s_state: &overflow_state,
                s_segment_id: segment_id,
                s_key: key_ref,
            },
        )
        .await
    }

    /// Inserts a trigger into clustering columns only.
    ///
    /// Used when multiple timers exist for a key/type. Does not touch the
    /// state column.
    #[instrument(level = "debug", skip(self), err(level = "warn"))]
    pub(super) async fn add_key_trigger_clustering(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), CassandraTriggerStoreError> {
        let span_map = extract_span_map(self.propagator(), &trigger);

        let key = trigger.key.as_ref();
        let time = trigger.time;
        let timer_type = trigger.timer_type;
        let tag = trigger.tag;

        let ttl = self.calculate_ttl(trigger.time);
        self.execute_unpaged_discard(
            &self.queries().insert_key_trigger_clustering,
            (segment_id, key, timer_type, time, &span_map, tag, ttl),
        )
        .await
    }
}
