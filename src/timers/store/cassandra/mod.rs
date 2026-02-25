use crate::cassandra::errors::CassandraStoreError;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::error::ParseError;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::TriggerStoreProvider;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::cassandra::queries::Queries;
use crate::timers::store::operations::TriggerOperations;
use crate::timers::store::{InvalidSegmentVersionError, Segment, SegmentId, SegmentVersion};
use crate::timers::{TimerType, Trigger};
use crate::{Key, Partition, Topic};
use async_stream::try_stream;
use educe::Educe;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use quick_cache::sync::Cache;
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, DeserializeValue, FrameSlice, SerializationError,
    SerializeValue, TypeCheckError, WrittenCellProof,
};
use scylla::client::session::Session;
use scylla::errors::MaybeFirstRowError;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use strum::VariantArray;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::field::Empty;
use tracing::{Span, debug, info_span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod queries;

#[cfg(test)]
mod tests;

/// V1 schema operations (internal, Cassandra-only).
pub(crate) mod v1;

/// Migration utilities for V1→V2 and slab size changes (internal,
/// Cassandra-only).
pub(crate) mod migration;

/// Cache key for the per-partition timer state cache.
type StateCacheKey = (Key, TimerType);

/// A single clustering-row entry passed to `batch_promote_and_set_overflow`.
#[derive(Debug)]
struct ClusteringEntry<'a> {
    time: CompactDateTime,
    span: &'a HashMap<String, String>,
}

/// A single row returned by `peek_first_key_trigger`: trigger time and span
/// context.
type PeekedTrigger = (CompactDateTime, HashMap<String, String>);

/// Capacity for the per-partition state cache.
///
/// Sized to cover the active working set of keys within a single partition.
/// Cache misses simply fall back to a DB read, so undersizing only costs an
/// extra query.
const STATE_CACHE_CAPACITY: usize = 8_192;

/// Timer data for a single inlined timer.
///
/// This is the resolved domain type for a key with exactly one timer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineTimer {
    /// Timer trigger time.
    pub time: CompactDateTime,
    /// OpenTelemetry span context for trace continuity.
    pub span: HashMap<String, String>,
}

/// Resolved three-state enum for a `(key, timer_type)` pair within a partition.
///
/// Determined by reading the `state` static map column:
/// - No map entry → `Absent` (post-V3: unambiguously 0 timers)
/// - `inline = true` with valid time → `Inline` (exactly 1 timer, stored in
///   state column)
/// - `inline = false/null` or corrupt data → `Overflow` (>1 timers, stored in
///   clustering rows)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerState {
    /// 0 timers for this key/type. Post-V3, `None` → `Absent` is
    /// unambiguous: no clustering rows exist.
    Absent,
    /// Exactly one timer, stored inline in the state column.
    Inline(InlineTimer),
    /// Multiple timers exist; stored in clustering rows.
    Overflow,
}

/// Cassandra UDT serde type for `key_timer_state`.
///
/// Private implementation detail of the `TimerState` serde impls.
#[derive(Clone, Debug, DeserializeValue, SerializeValue)]
struct RawTimerState {
    /// `true` = inline data present; `false`/`null` = overflow marker.
    inline: Option<bool>,
    /// Timer time (present only when `inline = true`).
    time: Option<CompactDateTime>,
    /// Span context (present only when `inline = true`).
    span: Option<HashMap<String, String>>,
}

impl SerializeValue for TimerState {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let raw = match self {
            Self::Inline(timer) => RawTimerState {
                inline: Some(true),
                time: Some(timer.time),
                span: Some(timer.span.clone()),
            },
            Self::Overflow => RawTimerState {
                inline: Some(false),
                time: None,
                span: None,
            },
            Self::Absent => {
                return Err(SerializationError::new(
                    CassandraTriggerStoreError::AbsentStateNotSerializable,
                ));
            }
        };
        raw.serialize(typ, writer)
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for TimerState {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        <RawTimerState as DeserializeValue>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let raw = RawTimerState::deserialize(typ, v)?;
        if raw.inline.unwrap_or_default() {
            match raw.time {
                Some(time) => Ok(Self::Inline(InlineTimer {
                    time,
                    span: raw.span.unwrap_or_default(),
                })),
                None => Ok(Self::Overflow),
            }
        } else {
            Ok(Self::Overflow)
        }
    }
}

/// Cassandra-based implementation of [`TriggerStore`](super::TriggerStore).
///
/// Each instance is scoped to a single partition and has its own state cache.
/// Created by [`CassandraTriggerStoreProvider`].
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct CassandraTriggerStore {
    store: CassandraStore,
    queries: Arc<Queries>,
    slab_size: CompactDuration,
    /// Per-partition cache of `(Key, TimerType) → TimerState`.
    ///
    /// Tracks the current state of each key/type pair:
    /// - `Inline(timer)` — exactly 1 timer, stored in the state column
    /// - `Overflow` — >1 timers, stored in clustering rows
    /// - `Absent` — 0 timers (post-V3: all states including Absent are cached)
    ///
    /// Cache miss → `resolve_state` reads only the requested type via
    /// `fetch_state` (single map-entry query) and caches the result.
    #[educe(Debug(ignore))]
    state_cache: Arc<Cache<StateCacheKey, TimerState>>,
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
            state_cache: Arc::new(Cache::new(STATE_CACHE_CAPACITY)),
        })
    }

    /// Creates a store using pre-existing shared resources and a fresh cache.
    ///
    /// Used by `CassandraTriggerStoreProvider` to create per-partition stores
    /// that share the session and queries but have independent caches.
    fn with_shared(
        store: CassandraStore,
        queries: Arc<Queries>,
        slab_size: CompactDuration,
    ) -> Self {
        Self {
            store,
            queries,
            slab_size,
            state_cache: Arc::new(Cache::new(STATE_CACHE_CAPACITY)),
        }
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

    /// Executes an unpaged query and discards the result.
    ///
    /// Convenience wrapper for fire-and-forget mutations that only need
    /// error propagation.
    async fn execute_unpaged_discard(
        &self,
        query: &PreparedStatement,
        params: impl SerializeRow,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.session()
            .execute_unpaged(query, params)
            .await
            .map_err(CassandraStoreError::from)?;
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
            .maybe_first_row::<(String, CompactDuration, Option<SegmentVersion>)>()
            .map_err(CassandraStoreError::from)?;

        let Some((name, slab_size, version)) = row else {
            return Ok(None);
        };

        let version = version.unwrap_or(SegmentVersion::V1);

        Ok(Some(Segment {
            id: *segment_id,
            name,
            slab_size,
            version,
        }))
    }

    // =========================================================================
    // State Column Operations (Cassandra-specific, not part of TriggerOperations)
    // =========================================================================

    /// Fetches the `state` map from the database.
    #[instrument(level = "debug", skip(self), err)]
    async fn fetch_state_map(
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
    async fn fetch_state(
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
    /// 1. Cache hit → return `(state, true)` immediately.
    /// 2. Cache miss → call `fetch_state` (reads only the requested type),
    ///    insert into cache, return `(state, false)`.
    ///
    /// The `bool` indicates whether the result was served from the cache
    /// (`true`) or required a DB round-trip (`false`). Callers record this as
    /// the `state_cached` tracing field.
    ///
    /// Post-V3, a NULL/missing MAP entry unambiguously means "new key, 0
    /// timers," so all states (including `Absent`) are cached.
    ///
    /// # Errors
    ///
    /// Returns error if the DB read fails on a cache miss.
    async fn resolve_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<(TimerState, bool), CassandraTriggerStoreError> {
        let cache_key = (key.clone(), timer_type);

        // Fast path: cache hit.
        if let Some(state) = self.state_cache.get(&cache_key) {
            return Ok((state, true));
        }

        // Cache miss: read only the requested type from DB.
        let state = self.fetch_state(segment_id, key, timer_type).await?;
        self.state_cache.insert(cache_key, state.clone());

        Ok((state, false))
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
    async fn batch_clear_and_set_inline(
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
    async fn set_state_inline(
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
    async fn set_state_overflow(
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
    async fn remove_state_entry(
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
    async fn peek_first_key_trigger(
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
    async fn batch_promote_and_set_overflow(
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
                (segment.id, segment.name, segment.slab_size, segment.version),
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
                let span = info_span!("fetch_slab_trigger");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

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
                .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;

            pin_mut!(stream);
            while let Some((key, time, timer_type, span_map)) =
                cooperative(stream.try_next())
                    .await
                    .map_err(CassandraStoreError::from)?
            {
                let context = self.propagator().extract(&span_map);
                let span = info_span!("fetch_slab_trigger_all_types");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }

                yield Trigger::new(key.into(), time, timer_type, span);
            }
        }
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn insert_slab_trigger(&self, slab: Slab, trigger: Trigger) -> Result<(), Self::Error> {
        let span_map = extract_span_map(self.propagator(), &trigger);

        let segment_id = slab.segment_id();
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
                    slab.segment_id(),
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
                    slab.segment_id(),
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
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send {
        let key_clone = key.clone();

        try_stream! {
            let (state, cached) = self.resolve_state(segment_id, &key_clone, timer_type).await?;
            Span::current().record("state_cached", cached);

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
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let key_clone = key.clone();

        try_stream! {
            let (state, cached) = self.resolve_state(segment_id, &key_clone, timer_type).await?;
            Span::current().record("state_cached", cached);

            match state {
                TimerState::Inline(timer) => {
                    // Inline: yield trigger from cache (0 clustering query).
                    let context = self.propagator().extract(&timer.span);
                    let span = info_span!("fetch_key_trigger_inline");
                    if let Err(error) = span.set_parent(context) {
                        debug!("failed to set parent span: {error:#}");
                    }
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
                        let span = info_span!("fetch_key_trigger");
                        if let Err(error) = span.set_parent(context) {
                            debug!("failed to set parent span: {error:#}");
                        }
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
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let key_clone = key.clone();

        try_stream! {
            // Read all types in a single query, then build the state_map.
            let raw_map = self.fetch_state_map(segment_id, &key_clone).await?;
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
            for (&tt, state) in &state_map {
                self.state_cache.insert((key_clone.clone(), tt), state.clone());
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
                        let span = info_span!("fetch_key_trigger_inline");
                        if let Err(error) = span.set_parent(context) {
                            debug!("failed to set parent span: {error:#}");
                        }
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
    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let timer_type = trigger.timer_type;
        let key = trigger.key.clone();
        let cache_key = (key.clone(), timer_type);

        let (state, cached) = self.resolve_state(segment_id, &key, timer_type).await?;
        Span::current().record("state_cached", cached);

        match state {
            TimerState::Inline(old_timer) => {
                // Promote: old inline → clustering, new → clustering, state → Overflow.
                // All three writes are issued as a single UNLOGGED BATCH so the
                // transition is atomic at the partition level.
                let new_span_map = extract_span_map(self.propagator(), &trigger);

                self.batch_promote_and_set_overflow(
                    segment_id,
                    &key,
                    timer_type,
                    ClusteringEntry {
                        time: old_timer.time,
                        span: &old_timer.span,
                    },
                    ClusteringEntry {
                        time: trigger.time,
                        span: &new_span_map,
                    },
                )
                .await?;
                self.state_cache.insert(cache_key, TimerState::Overflow);
            }
            TimerState::Overflow => {
                // Already overflow: write clustering only. State is unchanged.
                self.add_key_trigger_clustering(segment_id, trigger).await?;
            }
            TimerState::Absent => {
                // Post-V3 Absent is unambiguous: 0 timers, no clustering rows.
                // Set inline state directly.
                let span_map = extract_span_map(self.propagator(), &trigger);

                let state = TimerState::Inline(InlineTimer {
                    time: trigger.time,
                    span: span_map,
                });
                self.set_state_inline(segment_id, &key, timer_type, &state)
                    .await?;
                self.state_cache.insert(cache_key, state);
            }
        }

        Ok(())
    }

    /// Deletes a specific trigger from the key index with state-aware
    /// demotion.
    ///
    /// Uses `resolve_state` (cache-first, warms all types on miss):
    /// - **Inline(timer), time matches**: Remove state entry → `Absent`
    /// - **Inline(timer), time mismatch**: Delete clustering row (may be a
    ///   no-op) → stays `Inline`
    /// - **Overflow**: Delete clustering row + peek remaining (LIMIT 2):
    ///   - 0 remaining → remove state entry → `Absent`
    ///   - 1 remaining → set inline state + delete clustering row →
    ///     `Inline(remaining)`
    ///   - 2+ remaining → no state change → stays `Overflow`
    /// - **Absent**: No-op (post-V3 Absent is unambiguous: 0 timers, no rows)
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let cache_key = (key.clone(), timer_type);

        let (state, cached) = self.resolve_state(segment_id, key, timer_type).await?;
        Span::current().record("state_cached", cached);

        match state {
            TimerState::Inline(timer) if timer.time == time => {
                // Inline timer matches the delete target → remove state, become Absent.
                // No clustering row exists for inline timers, so only remove state.
                self.remove_state_entry(segment_id, key, timer_type).await?;
                self.state_cache.insert(cache_key, TimerState::Absent);
            }
            TimerState::Inline(_) => {
                // Inline timer does not match → delete from clustering (may be a no-op).
                // State stays Inline (the inline timer is untouched).
                self.execute_unpaged_discard(
                    &self.queries().delete_key_trigger,
                    (segment_id, key.as_ref(), timer_type, time),
                )
                .await?;
            }
            TimerState::Overflow => {
                // Delete the clustering row first.
                self.execute_unpaged_discard(
                    &self.queries().delete_key_trigger,
                    (segment_id, key.as_ref(), timer_type, time),
                )
                .await?;

                // Count remaining times (LIMIT 2, no span — cheap).
                let times = self.peek_trigger_times(segment_id, key, timer_type).await?;

                match times.as_slice() {
                    [] => {
                        // 0 remaining → Absent.
                        self.remove_state_entry(segment_id, key, timer_type).await?;
                        self.state_cache.insert(cache_key, TimerState::Absent);
                    }
                    [remaining_time] => {
                        // 1 remaining → fetch span and demote to Inline.
                        let Some((confirmed_time, span_map)) = self
                            .peek_first_key_trigger(segment_id, key, timer_type)
                            .await?
                        else {
                            // Row vanished between count and read — treat as Absent.
                            self.remove_state_entry(segment_id, key, timer_type).await?;
                            self.state_cache.insert(cache_key, TimerState::Absent);
                            return Ok(());
                        };

                        let state = TimerState::Inline(InlineTimer {
                            time: *remaining_time,
                            span: span_map,
                        });

                        // Concurrent: set inline state + delete remaining clustering row.
                        tokio::try_join!(
                            self.set_state_inline(segment_id, key, timer_type, &state),
                            self.execute_unpaged_discard(
                                &self.queries().delete_key_trigger,
                                (segment_id, key.as_ref(), timer_type, confirmed_time),
                            )
                        )?;

                        self.state_cache.insert(cache_key, state);
                    }
                    _ => {
                        // 2+ remaining → stays Overflow, no state change
                        // needed.
                    }
                }
            }
            TimerState::Absent => {
                // Post-V3 Absent is unambiguous: 0 timers, no clustering rows.
            }
        }

        Ok(())
    }

    /// Clears all triggers for a key/type with state awareness.
    ///
    /// Concurrently removes state entry AND clears clustering rows.
    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<(), Self::Error> {
        // Concurrent: remove state entry AND clear clustering rows
        tokio::try_join!(
            self.remove_state_entry(segment_id, key, timer_type),
            self.execute_unpaged_discard(
                &self.queries().clear_key_triggers,
                (segment_id, key.as_ref(), timer_type),
            )
        )?;

        // After success, cache Absent.
        self.state_cache
            .insert((key.clone(), timer_type), TimerState::Absent);

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
    /// Per-key serialization (`KeyManager`) guarantees the state is
    /// stable between the read and the write.
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn clear_and_schedule_key(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        // Extract span context for storage.
        let span_map = extract_span_map(self.propagator(), &trigger);

        let new_state = TimerState::Inline(InlineTimer {
            time: trigger.time,
            span: span_map,
        });

        let cache_key = (trigger.key.clone(), trigger.timer_type);

        let (resolved, cached) = self
            .resolve_state(segment_id, &trigger.key, trigger.timer_type)
            .await?;
        Span::current().record("state_cached", cached);

        match resolved {
            TimerState::Inline(_) | TimerState::Absent => {
                // Fast path: Inline or Absent → plain UPDATE, no tombstone.
                // Post-V3 Absent guarantees no clustering rows.
                self.set_state_inline(segment_id, &trigger.key, trigger.timer_type, &new_state)
                    .await?;
            }
            TimerState::Overflow => {
                // Overflow: BATCH (DELETE clustering + UPDATE state).
                self.batch_clear_and_set_inline(
                    segment_id,
                    &trigger.key,
                    trigger.timer_type,
                    &new_state,
                )
                .await?;
            }
        }

        // After success, cache Inline state.
        self.state_cache.insert(cache_key, new_state);
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    async fn clear_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<(), Self::Error> {
        // Concurrent: clear clustering rows AND the state static column.
        tokio::try_join!(
            self.execute_unpaged_discard(
                &self.queries().clear_key_triggers_all_types,
                (segment_id, key.as_ref()),
            ),
            self.execute_unpaged_discard(&self.queries().clear_state, (segment_id, key.as_ref()),)
        )?;

        // After success, cache Absent for all types.
        for &timer_type in TimerType::VARIANTS {
            self.state_cache
                .insert((key.clone(), timer_type), TimerState::Absent);
        }

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
                (new_version, new_slab_size.seconds() as i32, segment_id),
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

/// Factory holding shared Cassandra resources for creating per-partition
/// stores.
///
/// Each call to `create_store` produces a `CassandraTriggerStore` with its own
/// independent `state_cache` but sharing the Cassandra session and prepared
/// statements.
#[derive(Clone)]
pub struct CassandraTriggerStoreProvider {
    store: CassandraStore,
    queries: Arc<Queries>,
    slab_size: CompactDuration,
}

impl CassandraTriggerStoreProvider {
    /// Creates a new provider from an existing store setup.
    ///
    /// # Arguments
    ///
    /// * `store` - Shared Cassandra session
    /// * `queries` - Shared prepared statements
    /// * `slab_size` - Time partitioning size
    #[must_use]
    pub fn new(store: CassandraStore, queries: Arc<Queries>, slab_size: CompactDuration) -> Self {
        Self {
            store,
            queries,
            slab_size,
        }
    }

    /// Creates a new provider by preparing queries against an existing
    /// `CassandraStore`.
    ///
    /// This is the high-level constructor used by `StorePair` to create the
    /// provider from raw configuration. Queries are prepared once and shared
    /// across all stores created by this provider.
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
}

impl TriggerStoreProvider for CassandraTriggerStoreProvider {
    type Store = TableAdapter<CassandraTriggerStore>;

    fn create_store(
        &self,
        _topic: Topic,
        _partition: Partition,
        _consumer_group: &str,
    ) -> Self::Store {
        TableAdapter::new(CassandraTriggerStore::with_shared(
            self.store.clone(),
            Arc::clone(&self.queries),
            self.slab_size,
        ))
    }
}

/// Injects the trigger's span context into a new `HashMap` for Cassandra
/// storage.
fn extract_span_map(
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
    let span = info_span!("fetch_key_trigger_all_types_inline");
    if let Err(error) = span.set_parent(context) {
        debug!("failed to set parent span: {error:#}");
    }
    Some(Trigger::new(key.clone(), timer.time, timer_type, span))
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
        let span = info_span!("fetch_key_trigger_all_types");
        if let Err(error) = span.set_parent(context) {
            debug!("failed to set parent span: {error:#}");
        }

        return Ok(Some(Trigger::new(key.clone(), time, timer_type, span)));
    }
    Ok(None)
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

    /// `TimerState::Absent` cannot be serialized as a Cassandra UDT value.
    ///
    /// `Absent` is represented as a missing map entry, never as a UDT value.
    #[error("TimerState::Absent cannot be serialized as a UDT value")]
    AbsentStateNotSerializable,
}

impl ClassifyError for CassandraTriggerStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::InvalidSegmentVersion(e) => e.classify_error(),

            // Attempting to insert segment with slab_size different from store's configured
            // slab_size, or serializing Absent as a UDT — both are programming/configuration
            // errors that are not retryable.
            Self::SlabSizeMismatch { .. } | Self::AbsentStateNotSerializable => {
                ErrorCategory::Terminal
            }

            // Segment disappeared during data migration, likely due to concurrent deletion or
            // race condition. Retrying might succeed if segment reappears or operation completes.
            Self::SegmentDisappeared { .. } => ErrorCategory::Transient,

            Self::Parse(e) => e.classify_error(),
        }
    }
}
