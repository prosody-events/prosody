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
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use scylla::{DeserializeValue, SerializeValue};
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

/// V1 schema operations (internal, Cassandra-only).
pub(crate) mod v1;

/// Migration utilities for V1→V2 and slab size changes (internal,
/// Cassandra-only).
pub(crate) mod migration;

/// Cache key for the per-partition timer state cache.
type StateCacheKey = (Key, TimerType);

/// Capacity for the per-partition state cache.
///
/// Sized to cover the active working set of keys within a single partition.
/// Cache misses simply fall back to a DB read, so undersizing only costs an
/// extra query.
const STATE_CACHE_CAPACITY: usize = 8_192;

/// Timer data for a single inlined timer.
///
/// This is the resolved domain type for a key with exactly one timer.
/// Unlike `TimerSlot`, this does NOT derive `SerializeValue`/`DeserializeValue`
/// — the raw Cassandra serde type is `RawTimerState`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineTimer {
    /// Timer trigger time.
    pub time: CompactDateTime,
    /// OpenTelemetry span context for trace continuity.
    pub span: HashMap<String, String>,
}

/// Resolved three-state enum for a `(key, timer_type)` pair within a partition.
///
/// Determined by reading the `state` static map column and converting via
/// `into_timer_state`:
/// - No map entry → `Absent` (no timers or pre-migration data)
/// - `inline = true` with valid time → `Inline` (exactly 1 timer, stored in
///   state column)
/// - `inline = false/null` or corrupt data → `Overflow` (>1 timers, stored in
///   clustering rows)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerState {
    /// No timers for this key/type.
    Absent,
    /// Exactly one timer, stored inline in the state column.
    Inline(InlineTimer),
    /// Multiple timers exist; stored in clustering rows.
    Overflow,
}

/// Cassandra UDT serde type for `key_timer_state`.
///
/// Used only at the module boundary for serialization/deserialization.
/// Convert to `TimerState` via `into_timer_state` for domain logic.
#[derive(Clone, Debug, DeserializeValue, SerializeValue)]
struct RawTimerState {
    /// `true` = inline data present; `false`/`null` = overflow marker.
    inline: Option<bool>,
    /// Timer time (present only when `inline = true`).
    time: Option<CompactDateTime>,
    /// Span context (present only when `inline = true`).
    span: Option<HashMap<String, String>>,
}

/// Converts a raw Cassandra UDT value into a resolved `TimerState`.
///
/// Conversion rules:
/// - `None` → `Absent`
/// - `Some(inline=true, time=Some(t))` → `Inline(InlineTimer { time: t, span
///   })`
/// - `Some(inline=true, time=None)` → `Overflow` (corrupt data, safe fallback)
/// - `Some(inline=false/null, ...)` → `Overflow`
fn into_timer_state(raw: Option<RawTimerState>) -> TimerState {
    let Some(raw) = raw else {
        return TimerState::Absent;
    };

    if raw.inline.unwrap_or_default() {
        match raw.time {
            Some(time) => TimerState::Inline(InlineTimer {
                time,
                span: raw.span.unwrap_or_default(),
            }),
            None => TimerState::Overflow,
        }
    } else {
        TimerState::Overflow
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
    /// - `Absent` — 0 timers (only cached after our own writes, never from DB
    ///   reads)
    ///
    /// Cache miss → fall back to DB read.
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
    ) -> Result<Option<HashMap<TimerType, Option<RawTimerState>>>, CassandraTriggerStoreError> {
        let row = self
            .session()
            .execute_unpaged(&self.queries().get_state, (segment_id, key.as_ref()))
            .await
            .map_err(CassandraStoreError::from)?
            .into_rows_result()
            .map_err(CassandraStoreError::from)?
            .maybe_first_row::<(Option<HashMap<TimerType, Option<RawTimerState>>>,)>()
            .map_err(CassandraStoreError::from)?;

        Ok(row.and_then(|(map,)| map))
    }

    /// Reads the timer state for a key and timer type from the database.
    ///
    /// Returns the resolved `TimerState` which determines the read/write
    /// strategy:
    /// - `Absent`: No state entry exists (pre-migration data or 0 timers)
    /// - `Inline(timer)`: Exactly 1 timer stored in state column
    /// - `Overflow`: Multiple timers exist, stored in clustering rows
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    pub async fn get_timer_state(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<TimerState, CassandraTriggerStoreError> {
        let Some(map) = self.fetch_state_map(segment_id, key).await? else {
            return Ok(TimerState::Absent);
        };

        match map.get(&timer_type) {
            None => Ok(TimerState::Absent),
            Some(raw) => Ok(into_timer_state(raw.clone())),
        }
    }

    /// Reads the state map for a partition (all timer types at once).
    ///
    /// Returns the entire `state` map for use by `get_key_triggers_all_types`
    /// where per-type state must be evaluated for each type simultaneously.
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    async fn get_timer_state_map(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> Result<Option<HashMap<TimerType, TimerState>>, CassandraTriggerStoreError> {
        let Some(raw_map) = self.fetch_state_map(segment_id, key).await? else {
            return Ok(None);
        };

        let resolved: HashMap<TimerType, TimerState> = raw_map
            .into_iter()
            .map(|(tt, raw)| (tt, into_timer_state(raw)))
            .collect();

        Ok(Some(resolved))
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
        state: RawTimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.execute_with_optional_ttl(
            state.time.unwrap_or(CompactDateTime::MIN),
            &self.queries().batch_clear_and_set_inline,
            &self.queries().batch_clear_and_set_inline_no_ttl,
            |ttl| {
                (
                    segment_id,
                    key.as_ref(),
                    timer_type,
                    ttl,
                    timer_type,
                    &state,
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
                    &state,
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
        state: RawTimerState,
    ) -> Result<(), CassandraTriggerStoreError> {
        self.execute_with_optional_ttl(
            state.time.unwrap_or(CompactDateTime::MIN),
            &self.queries().set_state_inline,
            &self.queries().set_state_inline_no_ttl,
            |ttl| (ttl, timer_type, &state, segment_id, key.as_ref()),
            || (timer_type, &state, segment_id, key.as_ref()),
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
                (timer_type, segment_id, key.as_ref()),
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

    /// Counts remaining triggers for a key/type (LIMIT 2).
    ///
    /// Used for demotion check after `delete_key_trigger`:
    /// - 0 remaining → state becomes `Absent`
    /// - 1 remaining → state becomes `Inline` (demote)
    /// - 2+ remaining → state stays `Overflow`
    ///
    /// Returns a vector of up to 2 trigger times.
    ///
    /// # Errors
    ///
    /// Returns error if the database query fails.
    #[instrument(level = "debug", skip(self), err)]
    async fn count_remaining_triggers(
        &self,
        segment_id: &SegmentId,
        key: &Key,
        timer_type: TimerType,
    ) -> Result<Vec<CompactDateTime>, CassandraTriggerStoreError> {
        let stream = self
            .session()
            .execute_iter(
                self.queries().count_key_triggers.clone(),
                (segment_id, key.as_ref(), timer_type),
            )
            .await
            .map_err(CassandraStoreError::from)?
            .rows_stream::<(CompactDateTime,)>()
            .map_err(CassandraStoreError::from)?;

        pin_mut!(stream);
        let mut times = Vec::with_capacity(2);
        while let Some((time,)) = stream.try_next().await.map_err(CassandraStoreError::from)? {
            times.push(time);
        }

        Ok(times)
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
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        let context = trigger.span.load().context();
        self.propagator().inject_context(&context, &mut span_map);

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
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        let context = trigger.span.load().context();
        self.propagator().inject_context(&context, &mut span_map);

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
        let cache_key = (key.clone(), timer_type);

        try_stream! {
            let cached_state = self.state_cache.get(&cache_key);
            let was_cached = cached_state.is_some();
            Span::current().record("state_cached", was_cached);

            match cached_state {
                Some(TimerState::Inline(timer)) => {
                    // Cache hit Inline: yield time from cache (0 DB queries).
                    yield timer.time;
                }
                Some(TimerState::Overflow) => {
                    // Cache hit Overflow: skip state read, scan clustering only.
                    let stream = self
                        .session()
                        .execute_iter(
                            self.queries().get_key_times.clone(),
                            (segment_id, key.as_ref(), timer_type),
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
                Some(TimerState::Absent) => {
                    // Cache hit Absent (from our own clear/delete): yield nothing
                    // (0 DB queries, known 0 timers).
                }
                None => {
                    // Cache miss: read state from DB.
                    let db_state = self.get_timer_state(segment_id, key, timer_type).await?;

                    // Cache Inline and Overflow from DB, but NOT Absent
                    // (ambiguous: 0 timers or pre-migration data).
                    match &db_state {
                        TimerState::Inline(_) | TimerState::Overflow => {
                            self.state_cache.insert(cache_key, db_state.clone());
                        }
                        TimerState::Absent => {}
                    }

                    match db_state {
                        TimerState::Inline(timer) => {
                            yield timer.time;
                        }
                        TimerState::Overflow | TimerState::Absent => {
                            // DB Overflow: scan clustering only.
                            // DB Absent: scan clustering (pre-migration safe — may
                            // have timers in clustering with no state entry).
                            let stream = self
                                .session()
                                .execute_iter(
                                    self.queries().get_key_times.clone(),
                                    (segment_id, key.as_ref(), timer_type),
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
                    }
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
        let cache_key = (key.clone(), timer_type);

        try_stream! {
            let cached_state = self.state_cache.get(&cache_key);
            let was_cached = cached_state.is_some();
            Span::current().record("state_cached", was_cached);

            match cached_state {
                Some(TimerState::Inline(timer)) => {
                    // Cache hit Inline: create span from stored HashMap and yield
                    // trigger (0 DB queries).
                    let context = self.propagator().extract(&timer.span);
                    let span = info_span!("fetch_key_trigger_inline");
                    if let Err(error) = span.set_parent(context) {
                        debug!("failed to set parent span: {error:#}");
                    }
                    yield Trigger::new(key_clone.clone(), timer.time, timer_type, span);
                }
                Some(TimerState::Overflow) => {
                    // Cache hit Overflow: skip state read, scan clustering only.
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
                Some(TimerState::Absent) => {
                    // Cache hit Absent (from our own clear/delete): yield nothing
                    // (0 DB queries, known 0 timers).
                }
                None => {
                    // Cache miss: read state from DB.
                    let db_state = self.get_timer_state(segment_id, &key_clone, timer_type).await?;

                    // Cache Inline and Overflow from DB, but NOT Absent
                    // (ambiguous: 0 timers or pre-migration data).
                    match &db_state {
                        TimerState::Inline(_) | TimerState::Overflow => {
                            self.state_cache.insert(cache_key, db_state.clone());
                        }
                        TimerState::Absent => {}
                    }

                    match db_state {
                        TimerState::Inline(timer) => {
                            let context = self.propagator().extract(&timer.span);
                            let span = info_span!("fetch_key_trigger_inline");
                            if let Err(error) = span.set_parent(context) {
                                debug!("failed to set parent span: {error:#}");
                            }
                            yield Trigger::new(key_clone.clone(), timer.time, timer_type, span);
                        }
                        TimerState::Overflow | TimerState::Absent => {
                            // DB Overflow: scan clustering only.
                            // DB Absent: scan clustering (pre-migration safe).
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
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn get_key_triggers_all_types(
        &self,
        segment_id: &SegmentId,
        key: &Key,
    ) -> impl Stream<Item = Result<Trigger, Self::Error>> + Send {
        let key_clone = key.clone();

        try_stream! {
            // Read state map FIRST (sequential, not concurrent) to get a
            // consistent snapshot of which types are in inline state before
            // reading clustering rows. This avoids the race where a concurrent
            // insert_key_trigger promotes an inline to clustering between the
            // two reads.
            let empty = HashMap::new();
            let state_map = self.get_timer_state_map(segment_id, &key_clone).await?;
            let state_map = state_map.as_ref().unwrap_or(&empty);

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
            //   Inline entries — TimerType::VARIANTS (i8-ascending), at most
            //   one trigger per type.
            //
            //   Clustering — Cassandra stream in (timer_type, time) order,
            //   skipping NULL static-only rows.

            let mut variants_iter = TimerType::VARIANTS.iter();
            let mut inline_next = advance_inline(
                &key_clone, state_map, &mut variants_iter, self.propagator(),
            );

            // For each clustering row, flush any inline entries that sort before it.
            while let Some(clustering) = advance_clustering(
                &key_clone, &mut clustering_stream, self.propagator(),
            ).await? {
                while let Some(s) = inline_next.take() {
                    if (s.timer_type, s.time) <= (clustering.timer_type, clustering.time) {
                        yield s;
                        inline_next = advance_inline(
                            &key_clone, state_map, &mut variants_iter, self.propagator(),
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
                    &key_clone, state_map, &mut variants_iter, self.propagator(),
                );
            }
        }
    }

    /// Inserts a trigger into the key index with state-aware transitions.
    ///
    /// Cache-first logic determines the write strategy:
    /// - **Inline(old) hit**: Promote old timer to clustering + write new to
    ///   clustering + set overflow state → `Overflow`
    /// - **Overflow hit**: Write clustering only (1 query) → stays `Overflow`
    /// - **Absent hit** (from our own clear/delete — known 0 timers): Set
    ///   inline state with new timer → `Inline(new)`
    /// - **Cache miss, DB Absent** (ambiguous: pre-migration safe): Write
    ///   clustering only, do NOT cache
    /// - **Cache miss, DB Inline/Overflow**: Same as cache hit logic, cache
    ///   result
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn insert_key_trigger(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        let timer_type = trigger.timer_type;
        let key = trigger.key.clone();
        let cache_key = (key.clone(), timer_type);
        let cached_state = self.state_cache.get(&cache_key);
        let was_cached = cached_state.is_some();
        Span::current().record("state_cached", was_cached);

        let state = match cached_state {
            Some(state) => state,
            None => self.get_timer_state(segment_id, &key, timer_type).await?,
        };

        match state {
            TimerState::Inline(old_timer) => {
                // Promote: old inline → clustering, new → clustering, state → Overflow.
                let context = self.propagator().extract(&old_timer.span);
                let span = info_span!("promote_inline_to_clustering");
                if let Err(error) = span.set_parent(context) {
                    debug!("failed to set parent span: {error:#}");
                }
                let promoted = Trigger::new(key.clone(), old_timer.time, timer_type, span);
                // Write both clustering rows and set overflow concurrently.
                tokio::try_join!(
                    self.add_key_trigger_clustering(segment_id, promoted),
                    self.add_key_trigger_clustering(segment_id, trigger),
                    self.set_state_overflow(segment_id, &key, timer_type),
                )?;
                self.state_cache.insert(cache_key, TimerState::Overflow);
            }
            TimerState::Overflow => {
                // Already overflow: write clustering only.
                self.add_key_trigger_clustering(segment_id, trigger).await?;
                // Cache stays Overflow (re-insert to refresh).
                self.state_cache.insert(cache_key, TimerState::Overflow);
            }
            TimerState::Absent => {
                if was_cached {
                    // Cache Absent = from our own clear/delete → known 0 timers.
                    // Set inline state directly.
                    let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
                    let context = trigger.span.load().context();
                    self.propagator().inject_context(&context, &mut span_map);

                    let raw_state = RawTimerState {
                        inline: Some(true),
                        time: Some(trigger.time),
                        span: Some(span_map.clone()),
                    };
                    self.set_state_inline(segment_id, &key, timer_type, raw_state)
                        .await?;
                    self.state_cache.insert(
                        cache_key,
                        TimerState::Inline(InlineTimer {
                            time: trigger.time,
                            span: span_map,
                        }),
                    );
                } else {
                    // DB Absent = ambiguous (0 timers or pre-migration).
                    // Write clustering only, do NOT cache.
                    self.add_key_trigger_clustering(segment_id, trigger).await?;
                }
            }
        }

        Ok(())
    }

    /// Deletes a specific trigger from the key index with state-aware
    /// demotion.
    ///
    /// Cache-first logic determines the state transition:
    /// - **Inline(timer) hit, time matches**: Remove state entry → `Absent`
    /// - **Inline(timer) hit, time mismatch**: Delete clustering row (no-op on
    ///   state) → stays `Inline`
    /// - **Overflow hit**: Delete clustering row + count remaining (LIMIT 2):
    ///   - 0 remaining → remove state entry → `Absent`
    ///   - 1 remaining → BATCH: set inline state + delete clustering row →
    ///     `Inline(remaining)`
    ///   - 2+ remaining → no state change → stays `Overflow`
    /// - **Absent hit**: Delete clustering only, cache unchanged
    /// - **Cache miss**: Read DB state, then branch as above
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn delete_key_trigger(
        &self,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        time: CompactDateTime,
    ) -> Result<(), Self::Error> {
        let cache_key = (key.clone(), timer_type);
        let cached_state = self.state_cache.get(&cache_key);
        Span::current().record("state_cached", cached_state.is_some());

        let state = match cached_state {
            Some(state) => state,
            None => self.get_timer_state(segment_id, key, timer_type).await?,
        };

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

                // Count remaining triggers (LIMIT 2) for demotion check.
                let remaining = self
                    .count_remaining_triggers(segment_id, key, timer_type)
                    .await?;

                match remaining.len() {
                    0 => {
                        // 0 remaining → Absent.
                        self.remove_state_entry(segment_id, key, timer_type).await?;
                        self.state_cache.insert(cache_key, TimerState::Absent);
                    }
                    1 => {
                        // 1 remaining → demote to Inline.
                        // Read the remaining trigger's span from clustering.
                        let remaining_time = remaining[0];
                        let mut triggers: Vec<Trigger> = self
                            .get_key_triggers(segment_id, timer_type, key)
                            .try_collect()
                            .await?;

                        if let Some(remaining_trigger) = triggers.pop() {
                            let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
                            let context = remaining_trigger.span.load().context();
                            self.propagator().inject_context(&context, &mut span_map);

                            let raw_state = RawTimerState {
                                inline: Some(true),
                                time: Some(remaining_time),
                                span: Some(span_map.clone()),
                            };

                            // BATCH: set inline state + delete remaining clustering row.
                            tokio::try_join!(
                                self.set_state_inline(segment_id, key, timer_type, raw_state),
                                self.execute_unpaged_discard(
                                    &self.queries().delete_key_trigger,
                                    (segment_id, key.as_ref(), timer_type, remaining_time),
                                )
                            )?;

                            self.state_cache.insert(
                                cache_key,
                                TimerState::Inline(InlineTimer {
                                    time: remaining_time,
                                    span: span_map,
                                }),
                            );
                        }
                    }
                    _ => {
                        // 2+ remaining → stays Overflow, no state change
                        // needed.
                    }
                }
            }
            TimerState::Absent => {
                // No state data → just delete from clustering (pre-migration safe).
                self.execute_unpaged_discard(
                    &self.queries().delete_key_trigger,
                    (segment_id, key.as_ref(), timer_type, time),
                )
                .await?;
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
    /// Uses inline state optimization to avoid unnecessary tombstones:
    /// - **Inline state (cache hit)**: executes a plain UPDATE on the static
    ///   column (no BATCH, no DELETE, no range tombstone — 0 tombstones).
    /// - **Overflow (cache hit)**: executes a BATCH (DELETE clustering + UPDATE
    ///   state).
    /// - **Absent (cache hit, from our own clear)**: executes a plain UPDATE
    ///   (known 0 clustering rows).
    /// - **Cache miss**: reads DB, branches on result. DB `Absent` is ambiguous
    ///   (0 timers or pre-migration) → BATCH (safe default).
    ///
    /// Per-key serialization (`KeyManager`) guarantees the state is
    /// stable between the read and the write.
    #[instrument(level = "debug", skip(self), fields(state_cached = Empty), err)]
    async fn clear_and_schedule_key(
        &self,
        segment_id: &SegmentId,
        trigger: Trigger,
    ) -> Result<(), Self::Error> {
        // Extract span context for storage
        let mut span_map: HashMap<String, String> = HashMap::with_capacity(2);
        let context = trigger.span.load().context();
        self.propagator().inject_context(&context, &mut span_map);

        let raw_state = RawTimerState {
            inline: Some(true),
            time: Some(trigger.time),
            span: Some(span_map.clone()),
        };

        let cache_key = (trigger.key.clone(), trigger.timer_type);
        let cached_state = self.state_cache.get(&cache_key);
        Span::current().record("state_cached", cached_state.is_some());

        match cached_state {
            Some(TimerState::Inline(_) | TimerState::Absent) => {
                // Fast path: cache confirms Inline or Absent (from our own clear) —
                // plain UPDATE, no BATCH, no range tombstone.
                self.set_state_inline(segment_id, &trigger.key, trigger.timer_type, raw_state)
                    .await?;
            }
            Some(TimerState::Overflow) => {
                // Overflow: BATCH (DELETE clustering + UPDATE state).
                self.batch_clear_and_set_inline(
                    segment_id,
                    &trigger.key,
                    trigger.timer_type,
                    raw_state,
                )
                .await?;
            }
            None => {
                // Cache miss: read from DB to determine state.
                let db_state = self
                    .get_timer_state(segment_id, &trigger.key, trigger.timer_type)
                    .await?;

                match db_state {
                    TimerState::Inline(_) => {
                        self.set_state_inline(
                            segment_id,
                            &trigger.key,
                            trigger.timer_type,
                            raw_state,
                        )
                        .await?;
                    }
                    TimerState::Absent | TimerState::Overflow => {
                        // DB Absent is ambiguous (0 timers or pre-migration) → BATCH
                        self.batch_clear_and_set_inline(
                            segment_id,
                            &trigger.key,
                            trigger.timer_type,
                            raw_state,
                        )
                        .await?;
                    }
                }
            }
        }

        // After success, cache Inline state.
        self.state_cache.insert(
            cache_key,
            TimerState::Inline(InlineTimer {
                time: trigger.time,
                span: span_map,
            }),
        );
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
}

impl ClassifyError for CassandraTriggerStoreError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Database(e) => e.classify_error(),
            Self::InvalidSegmentVersion(e) => e.classify_error(),

            // Attempting to insert segment with slab_size different from store's configured
            // slab_size. Configuration mismatch that prevents ALL insertions of mismatched
            // segments. Requires configuration or data fix.
            Self::SlabSizeMismatch { .. } => ErrorCategory::Terminal,

            // Segment disappeared during data migration, likely due to concurrent deletion or
            // race condition. Retrying might succeed if segment reappears or operation completes.
            Self::SegmentDisappeared { .. } => ErrorCategory::Transient,

            Self::Parse(e) => e.classify_error(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{CassandraConfiguration, CassandraTriggerStore, cassandra_store};
    use super::{InlineTimer, TimerState};
    use crate::Key;
    use crate::cassandra::CassandraStore;
    use crate::timers::TimerType;
    use crate::timers::Trigger;
    use crate::timers::datetime::CompactDateTime;
    use crate::timers::duration::CompactDuration;
    use crate::timers::slab::{Slab, SlabId};
    use crate::timers::store::operations::TriggerOperations;
    use crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput;
    use crate::timers::store::{Segment, SegmentId, SegmentVersion};
    use crate::tracing::init_test_logging;
    use crate::trigger_store_tests;
    use color_eyre::Result;
    use futures::TryStreamExt;
    use futures::pin_mut;
    use futures::stream::StreamExt;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::env;
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use strum::VariantArray;
    use uuid::Uuid;

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

    // Determine the number of tests to run from an environment variable,
    // defaulting to 25 if the variable is not set or invalid.
    // Uses INTEGRATION_TESTS since these tests hit a real Cassandra database.
    fn get_test_count() -> u64 {
        env::var("INTEGRATION_TESTS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(25)
    }

    // Run the full suite of TriggerStore compliance tests on this implementation.
    // Low-level tests use CassandraTriggerStore directly
    // High-level tests use TableAdapter<CassandraTriggerStore>
    trigger_store_tests!(
        CassandraTriggerStore,
        |slab_size| async move {
            let config = test_cassandra_config("prosody_test");
            let store = CassandraStore::new(&config).await?;
            CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await
        },
        crate::timers::store::adapter::TableAdapter<CassandraTriggerStore>,
        |slab_size| async move {
            let config = test_cassandra_config("prosody_test");
            cassandra_store(&config, slab_size).await
        },
        get_test_count()
    );

    #[tokio::test]
    async fn test_slab_range_wrap_around_edge_cases() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60); // 1 minute slabs
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "test_segment".to_owned(),
            slab_size,
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
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = vec![boundary - 1, boundary, boundary + 1]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "Cross-boundary range failed");

        // Test Case 2: Range entirely in "negative" i32 space (high u32 values)
        let high_range = RangeInclusive::new(boundary, SlabId::MAX);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, high_range)
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = vec![boundary, boundary + 1, SlabId::MAX - 1, SlabId::MAX]
            .into_iter()
            .collect();
        assert_eq!(result, expected, "High range (negative i32) failed");

        // Test Case 3: Range entirely in "positive" i32 space (low u32 values)
        let low_range = RangeInclusive::new(boundary - 2, boundary - 1);
        let result: HashSet<SlabId> = store
            .get_slab_range(&segment_id, low_range)
            .try_collect()
            .await?;

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
            .try_collect()
            .await?;

        let expected: HashSet<SlabId> = HashSet::new();
        assert_eq!(result, expected, "Invalid range should return empty set");

        // Cleanup
        store.delete_segment(&segment_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_wrap_around() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "simple_test".to_owned(),
            slab_size,
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

    /// Collects sorted times from `get_key_times`.
    async fn collect_key_times(
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>> {
        let mut times: Vec<CompactDateTime> = store
            .get_key_times(segment_id, timer_type, key)
            .try_collect()
            .await?;
        times.sort();
        Ok(times)
    }

    /// Collects sorted times from `get_key_triggers`.
    async fn collect_trigger_times(
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>> {
        let mut times: Vec<CompactDateTime> = store
            .get_key_triggers(segment_id, timer_type, key)
            .map_ok(|t| t.time)
            .try_collect()
            .await?;
        times.sort();
        Ok(times)
    }

    /// Collects sorted times for a specific type from
    /// `get_key_triggers_all_types`.
    async fn collect_all_types_times(
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
    ) -> Result<Vec<CompactDateTime>> {
        let mut times: Vec<CompactDateTime> = store
            .get_key_triggers_all_types(segment_id, key)
            .try_filter_map(|t| async move {
                if t.timer_type == timer_type {
                    Ok(Some(t.time))
                } else {
                    Ok(None)
                }
            })
            .try_collect()
            .await?;
        times.sort();
        Ok(times)
    }

    /// Asserts that all three read paths return the expected sorted times for
    /// a `(segment_id, key, timer_type)`.
    async fn assert_key_reads(
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        expected: &[CompactDateTime],
        phase: &str,
    ) -> Result<()> {
        assert_eq!(
            collect_key_times(store, segment_id, timer_type, key).await?,
            expected,
            "{phase}: get_key_times"
        );
        assert_eq!(
            collect_trigger_times(store, segment_id, timer_type, key).await?,
            expected,
            "{phase}: get_key_triggers"
        );
        assert_eq!(
            collect_all_types_times(store, segment_id, timer_type, key).await?,
            expected,
            "{phase}: get_key_triggers_all_types"
        );
        Ok(())
    }

    /// Asserts the timer state matches the expected variant, with reads
    /// verification.
    ///
    /// Also verifies the in-memory cache state: after mutations that populate
    /// the cache, the cached state must match the expected state. This ensures
    /// subsequent reads (verified by `assert_key_reads`) are served from cache
    /// (0 DB queries for Inline/Absent, skip-state for Overflow).
    async fn assert_state_and_reads(
        store: &CassandraTriggerStore,
        segment_id: &SegmentId,
        timer_type: TimerType,
        key: &Key,
        expected_state: &TimerState,
        expected_times: &[CompactDateTime],
        phase: &str,
    ) -> Result<()> {
        let state = store.get_timer_state(segment_id, key, timer_type).await?;
        match expected_state {
            TimerState::Absent => {
                assert_eq!(state, TimerState::Absent, "{phase}: expected Absent");
            }
            TimerState::Inline(expected) => {
                assert!(
                    matches!(&state, TimerState::Inline(t) if t.time == expected.time),
                    "{phase}: expected Inline({}), got {state:?}",
                    expected.time
                );
            }
            TimerState::Overflow => {
                assert_eq!(state, TimerState::Overflow, "{phase}: expected Overflow");
            }
        }

        // Verify cache state matches expectations. After mutations, the cache
        // should be warm, so subsequent reads use the cache-optimized paths
        // (Inline → 0 DB queries, Overflow → skip state read, Absent → 0 DB
        // queries).
        let cache_key = (key.clone(), timer_type);
        let cached = store.state_cache.get(&cache_key);
        match expected_state {
            TimerState::Inline(expected) => {
                assert!(cached.is_some(), "{phase}: cache should have Inline entry");
                assert!(
                    matches!(&cached, Some(TimerState::Inline(t)) if t.time == expected.time),
                    "{phase}: cached state should be Inline({}), got {cached:?}",
                    expected.time,
                );
            }
            TimerState::Overflow => {
                assert!(
                    cached.is_some(),
                    "{phase}: cache should have Overflow entry"
                );
                assert_eq!(
                    cached,
                    Some(TimerState::Overflow),
                    "{phase}: cached state should be Overflow"
                );
            }
            TimerState::Absent => {
                // Absent is cached only after our own writes (clear/delete),
                // not from DB reads. If the cache has an entry, it must be
                // Absent.
                if let Some(cached) = cached {
                    assert_eq!(
                        cached,
                        TimerState::Absent,
                        "{phase}: cached state should be Absent if present"
                    );
                }
            }
        }

        assert_key_reads(store, segment_id, timer_type, key, expected_times, phase).await
    }

    /// Absent → Inline → Overflow → demotion via delete → Absent.
    ///
    /// Covers: Absent→Inline (schedule), Inline→Overflow (insert/promote),
    /// Overflow→Inline (delete demotion 2→1), Inline→Absent (delete 1→0).
    #[tokio::test]
    async fn test_state_transitions_schedule_promote_demote() -> Result<()> {
        init_test_logging();
        let (store, segment_id) = setup_test_store("promote_demote").await?;

        let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);
        let absent = TimerState::Absent;
        let inline_t1 = TimerState::Inline(InlineTimer {
            time: t1,
            span: HashMap::new(),
        });
        let inline_t2 = TimerState::Inline(InlineTimer {
            time: t2,
            span: HashMap::new(),
        });

        // Absent (0 timers)
        assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "absent").await?;

        // Absent → Inline via clear_and_schedule_key
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t1, &[t1], "schedule")
            .await?;

        // Inline → Overflow via insert_key_trigger (promotion)
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
            .await?;
        assert_state_and_reads(
            &store,
            &segment_id,
            tt,
            &key,
            &TimerState::Overflow,
            &[t1, t2],
            "promote",
        )
        .await?;

        // Overflow → Inline(t2) via delete_key_trigger (2→1 demotion)
        store.delete_key_trigger(&segment_id, tt, &key, t1).await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &inline_t2, &[t2], "demote").await?;

        // Inline → Absent via delete_key_trigger (1→0)
        store.delete_key_trigger(&segment_id, tt, &key, t2).await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete last").await?;

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Overflow→Inline via `clear_and_schedule_key`, `clear_key_triggers`
    /// paths, Inline→Inline reschedule.
    ///
    /// Covers: Overflow→Inline (`clear_and_schedule`), Inline→Absent (clear),
    /// Overflow→Absent (clear), Inline→Inline (reschedule, 0 tombstones).
    #[tokio::test]
    async fn test_state_transitions_clear_and_reschedule() -> Result<()> {
        init_test_logging();
        let (store, segment_id) = setup_test_store("clear_reschedule").await?;

        let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);
        let t3 = CompactDateTime::from(3_000_000u32);
        let absent = TimerState::Absent;
        let inline_t2 = TimerState::Inline(InlineTimer {
            time: t2,
            span: HashMap::new(),
        });
        let inline_t3 = TimerState::Inline(InlineTimer {
            time: t3,
            span: HashMap::new(),
        });

        // Overflow → Inline via clear_and_schedule_key
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
            .await?;
        assert_key_reads(&store, &segment_id, tt, &key, &[t1, t2], "overflow setup").await?;

        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t3, tt))
            .await?;
        assert_state_and_reads(
            &store,
            &segment_id,
            tt,
            &key,
            &inline_t3,
            &[t3],
            "overflow→inline",
        )
        .await?;

        // Inline → Absent via clear_key_triggers
        store.clear_key_triggers(&segment_id, tt, &key).await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "clear inline").await?;

        // Overflow → Absent via clear_key_triggers
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
            .await?;
        store.clear_key_triggers(&segment_id, tt, &key).await?;
        assert_state_and_reads(
            &store,
            &segment_id,
            tt,
            &key,
            &absent,
            &[],
            "clear overflow",
        )
        .await?;

        // Inline → Inline via clear_and_schedule_key (no tombstone)
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t2, tt))
            .await?;
        assert_state_and_reads(
            &store,
            &segment_id,
            tt,
            &key,
            &inline_t2,
            &[t2],
            "inline→inline",
        )
        .await?;

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Absent (cached) → Inline via insert, Inline → Absent via delete,
    /// pre-migration backward compatibility.
    ///
    /// Covers: cached Absent→Inline (insert), Inline→Absent (delete match),
    /// DB Absent backward-compat (insert on cold cache).
    #[tokio::test]
    async fn test_state_transitions_insert_and_delete() -> Result<()> {
        init_test_logging();
        let (store, segment_id) = setup_test_store("insert_delete").await?;

        let key: Key = format!("state-test-{}", Uuid::new_v4()).into();
        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);
        let absent = TimerState::Absent;
        let inline_t1 = TimerState::Inline(InlineTimer {
            time: t1,
            span: HashMap::new(),
        });

        // DB Absent backward-compat: insert on cold cache writes clustering
        // only, state stays Absent. Reads must find timer via clustering scan.
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[t1], "cold insert")
            .await?;

        // Clean up.
        store.clear_key_triggers(&segment_id, tt, &key).await?;

        // Cached Absent → Inline via insert_key_trigger
        // After clear_key_triggers, cache has Absent → insert sets inline.
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;
        assert_state_and_reads(
            &store,
            &segment_id,
            tt,
            &key,
            &inline_t1,
            &[t1],
            "cached absent→inline",
        )
        .await?;

        // Inline → Absent via delete_key_trigger (time match)
        store.delete_key_trigger(&segment_id, tt, &key, t1).await?;
        assert_state_and_reads(&store, &segment_id, tt, &key, &absent, &[], "delete inline")
            .await?;

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Creates a test store and segment, returning `(store, segment_id)`.
    async fn setup_test_store(name: &str) -> Result<(CassandraTriggerStore, SegmentId)> {
        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: name.to_owned(),
            slab_size,
            version: SegmentVersion::V2,
        };
        store.insert_segment(segment).await?;
        Ok((store, segment_id))
    }

    /// Simulates pre-migration production data: timers written directly to
    /// clustering columns with no `state` MAP entry (as old code would have).
    /// Verifies reads and `clear_and_schedule_key` migration.
    ///
    /// Uses `add_key_trigger_clustering` to write clustering rows without
    /// touching the state column.
    #[tokio::test]
    async fn test_pre_migration_reads_and_migration() -> Result<()> {
        init_test_logging();
        let (store, segment_id) = setup_test_store("pre_mig_reads").await?;

        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);
        let t3 = CompactDateTime::from(3_000_000u32);

        // Scenario A: 1 pre-migration clustering timer — reads find it.
        let key_a: Key = format!("pre-mig-a-{}", Uuid::new_v4()).into();
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_a.clone(), t1, tt))
            .await?;
        let state = store.get_timer_state(&segment_id, &key_a, tt).await?;
        assert_eq!(state, TimerState::Absent, "A: state should be Absent");
        assert_key_reads(&store, &segment_id, tt, &key_a, &[t1], "A read").await?;

        // clear_and_schedule_key migrates to inline.
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_a.clone(), t2, tt))
            .await?;
        let state = store.get_timer_state(&segment_id, &key_a, tt).await?;
        assert!(
            matches!(&state, TimerState::Inline(t) if t.time == t2),
            "A migrated: expected Inline(t2), got {state:?}"
        );
        assert_key_reads(&store, &segment_id, tt, &key_a, &[t2], "A migrated").await?;

        // Scenario B: 2 pre-migration clustering timers — reads find both.
        let key_b: Key = format!("pre-mig-b-{}", Uuid::new_v4()).into();
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_b.clone(), t1, tt))
            .await?;
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_b.clone(), t2, tt))
            .await?;
        let state = store.get_timer_state(&segment_id, &key_b, tt).await?;
        assert_eq!(state, TimerState::Absent, "B: state should be Absent");
        assert_key_reads(&store, &segment_id, tt, &key_b, &[t1, t2], "B read").await?;

        // clear_and_schedule_key replaces both with inline state.
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_b.clone(), t3, tt))
            .await?;
        let state = store.get_timer_state(&segment_id, &key_b, tt).await?;
        assert!(
            matches!(&state, TimerState::Inline(t) if t.time == t3),
            "B migrated: expected Inline(t3), got {state:?}"
        );
        assert_key_reads(&store, &segment_id, tt, &key_b, &[t3], "B migrated").await?;

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Simulates pre-migration production data and verifies that mutation
    /// operations (`insert`, `delete`, `clear`) work correctly on timers
    /// that exist only in clustering columns with no `state` MAP entry.
    #[tokio::test]
    async fn test_pre_migration_mutations() -> Result<()> {
        init_test_logging();
        let (store, segment_id) = setup_test_store("pre_mig_mutations").await?;

        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);
        let t3 = CompactDateTime::from(3_000_000u32);

        // Scenario C: insert_key_trigger on 1 pre-migration timer — both
        // end up in clustering since state is Absent (no promotion).
        let key_c: Key = format!("pre-mig-c-{}", Uuid::new_v4()).into();
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_c.clone(), t1, tt))
            .await?;
        store
            .insert_key_trigger(&segment_id, Trigger::for_testing(key_c.clone(), t2, tt))
            .await?;
        assert_key_reads(&store, &segment_id, tt, &key_c, &[t1, t2], "C insert").await?;

        // clear_and_schedule_key migrates to inline.
        store
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key_c.clone(), t3, tt))
            .await?;
        assert_key_reads(&store, &segment_id, tt, &key_c, &[t3], "C migrated").await?;

        // Scenario D: clear_key_triggers on pre-migration data.
        let key_d: Key = format!("pre-mig-d-{}", Uuid::new_v4()).into();
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_d.clone(), t1, tt))
            .await?;
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_d.clone(), t2, tt))
            .await?;
        store.clear_key_triggers(&segment_id, tt, &key_d).await?;
        assert_key_reads(&store, &segment_id, tt, &key_d, &[], "D cleared").await?;

        // Scenario E: delete_key_trigger on pre-migration data.
        let key_e: Key = format!("pre-mig-e-{}", Uuid::new_v4()).into();
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_e.clone(), t1, tt))
            .await?;
        store
            .add_key_trigger_clustering(&segment_id, Trigger::for_testing(key_e.clone(), t2, tt))
            .await?;
        store
            .delete_key_trigger(&segment_id, tt, &key_e, t1)
            .await?;
        assert_key_reads(&store, &segment_id, tt, &key_e, &[t2], "E delete one").await?;
        store
            .delete_key_trigger(&segment_id, tt, &key_e, t2)
            .await?;
        assert_key_reads(&store, &segment_id, tt, &key_e, &[], "E delete all").await?;

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Verifies `clear_key_triggers_all_types` clears both inline and
    /// overflow states across different timer types simultaneously.
    #[tokio::test]
    async fn test_clear_all_types_clears_inline_and_overflow() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "clear_all_types".to_owned(),
            slab_size,
            version: SegmentVersion::V2,
        };
        store.insert_segment(segment).await?;

        let key: Key = format!("clear-all-{}", Uuid::new_v4()).into();
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);

        // Set up: Application inline (1 timer), DeferredMessage overflow (2 timers).
        store
            .clear_and_schedule_key(
                &segment_id,
                Trigger::for_testing(key.clone(), t1, TimerType::Application),
            )
            .await?;
        store
            .clear_and_schedule_key(
                &segment_id,
                Trigger::for_testing(key.clone(), t1, TimerType::DeferredMessage),
            )
            .await?;
        store
            .insert_key_trigger(
                &segment_id,
                Trigger::for_testing(key.clone(), t2, TimerType::DeferredMessage),
            )
            .await?;

        // Verify setup.
        assert_key_reads(
            &store,
            &segment_id,
            TimerType::Application,
            &key,
            &[t1],
            "setup app",
        )
        .await?;
        assert_key_reads(
            &store,
            &segment_id,
            TimerType::DeferredMessage,
            &key,
            &[t1, t2],
            "setup dm",
        )
        .await?;

        // Clear all types.
        store
            .clear_key_triggers_all_types(&segment_id, &key)
            .await?;

        // Verify all types are Absent with no data.
        for &variant in TimerType::VARIANTS {
            let state = store.get_timer_state(&segment_id, &key, variant).await?;
            assert_eq!(state, TimerState::Absent, "{variant:?} should be Absent");
            assert_key_reads(
                &store,
                &segment_id,
                variant,
                &key,
                &[],
                &format!("{variant:?}"),
            )
            .await?;
        }

        store.delete_segment(&segment_id).await?;
        Ok(())
    }

    /// Verifies the inline timer state machine lifecycle:
    /// Absent → Inline → Inline (replacement) → Overflow (after promotion)
    /// → Inline
    ///
    /// This confirms the tombstone-free optimization actually transitions
    /// through the expected states, and that type isolation holds between
    /// timer types.
    #[tokio::test]
    async fn test_inline_state_round_trip() -> Result<()> {
        init_test_logging();

        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let cassandra_store = CassandraStore::new(&config).await?;
        let store =
            CassandraTriggerStore::with_store(cassandra_store, &config.keyspace, slab_size).await?;

        // Unique segment + key per test run to avoid cross-test interference.
        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "inline_state_round_trip".to_owned(),
            slab_size,
            version: SegmentVersion::V1,
        };
        store.insert_segment(segment).await?;

        let key: Key = format!("inline-test-{}", Uuid::new_v4()).into();
        let t1 = CompactDateTime::from(1_000_000u32);
        let t2 = CompactDateTime::from(2_000_000u32);
        let t3 = CompactDateTime::from(3_000_000u32);
        let t4 = CompactDateTime::from(4_000_000u32);

        // Phase 1: Initial state — no data, state is Absent.
        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert_eq!(state, TimerState::Absent, "phase 1: expected Absent");

        // Phase 2: clear_and_schedule_key(t1) → Inline(t1)
        let trigger1 = Trigger::for_testing(key.clone(), t1, TimerType::Application);
        store.clear_and_schedule_key(&segment_id, trigger1).await?;

        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert!(
            matches!(&state, TimerState::Inline(t) if t.time == t1),
            "phase 2: expected Inline(t1), got {state:?}"
        );

        // Phase 3: clear_and_schedule_key(t2) → Inline(t2) (Inline→Inline, no
        // tombstone)
        let trigger2 = Trigger::for_testing(key.clone(), t2, TimerType::Application);
        store.clear_and_schedule_key(&segment_id, trigger2).await?;

        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert!(
            matches!(&state, TimerState::Inline(t) if t.time == t2),
            "phase 3: expected Inline(t2), got {state:?}"
        );

        // Phase 4: insert_key_trigger(t3) promotes inline to clustering → state
        // becomes Overflow
        let trigger3 = Trigger::for_testing(key.clone(), t3, TimerType::Application);
        store.insert_key_trigger(&segment_id, trigger3).await?;

        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert_eq!(
            state,
            TimerState::Overflow,
            "phase 4: expected Overflow after promotion"
        );

        // Phase 5: clear_and_schedule_key(t4) on an overflow key → back to
        // Inline(t4)
        let trigger4 = Trigger::for_testing(key.clone(), t4, TimerType::Application);
        store.clear_and_schedule_key(&segment_id, trigger4).await?;

        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert!(
            matches!(&state, TimerState::Inline(t) if t.time == t4),
            "phase 5: expected Inline(t4), got {state:?}"
        );

        // Phase 6: Verify get_key_times returns exactly [t4].
        let times: Vec<CompactDateTime> = store
            .get_key_times(&segment_id, TimerType::Application, &key)
            .try_collect()
            .await?;
        assert_eq!(
            times,
            vec![t4],
            "phase 6: get_key_times should return exactly [t4]"
        );

        // Phase 7: Type isolation — DeferredMessage state is still Absent.
        let state = store
            .get_timer_state(&segment_id, &key, TimerType::DeferredMessage)
            .await?;
        assert_eq!(
            state,
            TimerState::Absent,
            "phase 7: DeferredMessage state should be Absent"
        );

        // Phase 8: Cleanup — clear_key_triggers_all_types resets everything.
        store
            .clear_key_triggers_all_types(&segment_id, &key)
            .await?;

        let state = store
            .get_timer_state(&segment_id, &key, TimerType::Application)
            .await?;
        assert_eq!(
            state,
            TimerState::Absent,
            "phase 8: expected Absent after cleanup"
        );

        store.delete_segment(&segment_id).await?;

        Ok(())
    }

    /// Property test verifying the timer state invariant:
    ///
    /// - **1 timer** for a `(segment_id, key, timer_type)` → state must be
    ///   `Inline` holding it, no clustering rows.
    /// - **>1 timer** → state must be `Overflow`, all timers in clustering
    ///   rows.
    /// - **0 timers** → state must be `Absent`.
    ///
    /// Applies a random sequence of operations then inspects every
    /// `(segment_id, key, timer_type)` combination against the reference model.
    #[test]
    fn test_prop_timer_state_invariant() {
        use crate::test_util::TEST_RUNTIME;
        use crate::timers::store::tests::prop_key_triggers::KeyTriggerTestInput;
        use quickcheck::{QuickCheck, TestResult};
        use tracing::Instrument;

        fn prop(input: KeyTriggerTestInput) -> TestResult {
            let runtime = &*TEST_RUNTIME;
            let span = tracing::Span::current();

            let slab_size = input.slab_size;
            let store = match runtime.block_on(
                async {
                    let config = test_cassandra_config("prosody_test");
                    let store = CassandraStore::new(&config).await?;
                    CassandraTriggerStore::with_store(store, &config.keyspace, slab_size).await
                }
                .instrument(span.clone()),
            ) {
                Ok(s) => s,
                Err(e) => return TestResult::error(format!("Failed to create store: {e:?}")),
            };

            match runtime.block_on(
                async { prop_timer_state_invariant(&store, input).await }.instrument(span),
            ) {
                Ok(()) => TestResult::passed(),
                Err(e) => TestResult::error(format!("{e:?}")),
            }
        }

        init_test_logging();
        QuickCheck::new()
            .tests(get_test_count())
            .quickcheck(prop as fn(KeyTriggerTestInput) -> TestResult);
    }

    /// Verifies that `CassandraTriggerStoreProvider` creates stores with
    /// independent caches but a shared Cassandra session.
    ///
    /// 1. Create provider, call `create_store` twice.
    /// 2. Write via store A → store A cache is warm.
    /// 3. Store B cache is cold (no entry for same key).
    /// 4. Store B can still read the data via DB (shared session).
    #[tokio::test]
    async fn test_provider_creates_independent_stores() -> Result<()> {
        use crate::timers::store::TriggerStoreProvider;
        use crate::timers::store::cassandra::CassandraTriggerStoreProvider;

        init_test_logging();

        let slab_size = CompactDuration::new(60);
        let config = test_cassandra_config("prosody_test");
        let provider = CassandraTriggerStoreProvider::with_store(
            CassandraStore::new(&config).await?,
            &config.keyspace,
            slab_size,
        )
        .await?;

        // Create two independent stores from the same provider.
        let store_a = provider.create_store("test-topic".into(), 0, "test-group");
        let store_b = provider.create_store("test-topic".into(), 1, "test-group");

        // Access inner CassandraTriggerStore for direct TriggerOperations use.
        let ops_a = store_a.operations();
        let ops_b = store_b.operations();

        // Set up a shared segment (both stores share the session, so both see it).
        let segment_id = SegmentId::from(Uuid::new_v4());
        let segment = Segment {
            id: segment_id,
            name: "provider_independent".to_owned(),
            slab_size,
            version: SegmentVersion::V2,
        };
        ops_a.insert_segment(segment).await?;

        let key: Key = format!("provider-test-{}", Uuid::new_v4()).into();
        let tt = TimerType::Application;
        let t1 = CompactDateTime::from(1_000_000u32);

        // Write via store A: clear_and_schedule_key populates store A's cache.
        ops_a
            .clear_and_schedule_key(&segment_id, Trigger::for_testing(key.clone(), t1, tt))
            .await?;

        // Store A cache is warm: Inline(t1).
        let cache_key = (key.clone(), tt);
        let cached_a = ops_a.state_cache.get(&cache_key);
        assert!(
            matches!(&cached_a, Some(TimerState::Inline(timer)) if timer.time == t1),
            "store A cache should have Inline(t1), got {cached_a:?}"
        );

        // Store B cache is cold: no entry for this key.
        let cached_b = ops_b.state_cache.get(&cache_key);
        assert!(
            cached_b.is_none(),
            "store B cache should be cold (None), got {cached_b:?}"
        );

        // Store B can still read the data (proves shared session).
        let times: Vec<CompactDateTime> = ops_b
            .get_key_times(&segment_id, tt, &key)
            .try_collect()
            .await?;
        assert_eq!(times, vec![t1], "store B should read t1 via shared session");

        // After the read, store B's cache should now be warm (Inline cached from DB).
        let warm_b = ops_b.state_cache.get(&cache_key);
        assert!(
            matches!(&warm_b, Some(TimerState::Inline(t)) if t.time == t1),
            "store B cache should be warm after read, got {warm_b:?}"
        );

        // Cleanup.
        ops_a.delete_segment(&segment_id).await?;

        Ok(())
    }

    /// Applies operations from [`KeyTriggerTestInput`] and verifies the
    /// timer state invariant holds for every `(segment_id, key, timer_type)`.
    async fn prop_timer_state_invariant(
        store: &CassandraTriggerStore,
        input: KeyTriggerTestInput,
    ) -> Result<()> {
        use crate::timers::store::tests::prop_key_triggers::{
            KeyTriggerModel, KeyTriggerOperation,
        };

        let key_pool = ["key-a", "key-b", "key-c"];

        // Clean up before test
        for segment_id in &input.segment_ids {
            for key_str in &key_pool {
                let key = Key::from(*key_str);
                store.clear_key_triggers_all_types(segment_id, &key).await?;
            }
        }

        // Apply all operations to both model and store
        let mut model = KeyTriggerModel::new();
        for op in &input.operations {
            model.apply(op);
            match op {
                KeyTriggerOperation::Insert {
                    segment_id,
                    trigger,
                } => {
                    store
                        .insert_key_trigger(segment_id, trigger.clone())
                        .await?;
                }
                KeyTriggerOperation::Delete {
                    segment_id,
                    timer_type,
                    key,
                    time,
                } => {
                    store
                        .delete_key_trigger(segment_id, *timer_type, key, *time)
                        .await?;
                }
                KeyTriggerOperation::ClearByType {
                    segment_id,
                    timer_type,
                    key,
                } => {
                    store
                        .clear_key_triggers(segment_id, *timer_type, key)
                        .await?;
                }
                KeyTriggerOperation::ClearAllTypes { segment_id, key } => {
                    store.clear_key_triggers_all_types(segment_id, key).await?;
                }
                KeyTriggerOperation::ClearAndSchedule {
                    segment_id,
                    trigger,
                } => {
                    store
                        .clear_and_schedule_key(segment_id, trigger.clone())
                        .await?;
                }
                KeyTriggerOperation::GetTimes { .. }
                | KeyTriggerOperation::GetTriggers { .. }
                | KeyTriggerOperation::GetAllTypes { .. } => {}
            }
        }

        // Verify timer state invariant for every (segment_id, key, timer_type)
        for (segment_id, key) in &model.all_keys() {
            for &timer_type in TimerType::VARIANTS {
                let expected_count = model.get_times(segment_id, timer_type, key).len();
                let timer_state = store.get_timer_state(segment_id, key, timer_type).await?;

                match expected_count {
                    0 => {
                        assert!(
                            matches!(timer_state, TimerState::Absent),
                            "Invariant violation: 0 timers for ({segment_id}, {key}, \
                             {timer_type:?}) but state is {timer_state:?}"
                        );
                    }
                    1 => {
                        let expected_time = model.get_times(segment_id, timer_type, key)[0];
                        assert!(
                            matches!(&timer_state, TimerState::Inline(t) if t.time == expected_time),
                            "Invariant violation: exactly 1 timer (time={expected_time:?}) for \
                             ({segment_id}, {key}, {timer_type:?}) but state is {timer_state:?} — \
                             expected Inline"
                        );
                    }
                    n => {
                        assert!(
                            matches!(timer_state, TimerState::Overflow),
                            "Invariant violation: {n} timers for ({segment_id}, {key}, \
                             {timer_type:?}) but state is {timer_state:?} — expected Overflow"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
