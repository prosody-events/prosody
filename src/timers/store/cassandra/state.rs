//! The `state` column domain model and per-partition cache.
//!
//! The V3 schema stores a `state` static map column alongside clustering rows.
//! Each `(key, timer_type)` entry in that map is a `key_timer_state` UDT,
//! represented here as [`TimerState`]. Reading this single column tells the
//! store how many timers exist for a key without touching the clustering rows
//! at all — the common single-timer path never issues a clustering scan.
//!
//! The in-memory cache (`CachedState` — an `Arc<AsyncMutex<TimerState>>`)
//! keeps the resolved state hot between operations. Wrapping in
//! `Arc<AsyncMutex>` lets callers clone the handle out of the cache before
//! awaiting the lock, so no shard lock is held across any async boundary.
//! All read-modify-write sequences hold this mutex for their full duration,
//! eliminating the TOCTOU window that existed before per-key serialisation was
//! added.

use crate::Key;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use quick_cache::sync::Cache;
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, DeserializeValue, FrameSlice, SerializationError,
    SerializeValue, TypeCheckError, WrittenCellProof,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Cache key for the per-partition timer state cache.
pub(super) type StateCacheKey = (Key, TimerType);

/// A single clustering-row entry passed to `batch_promote_and_set_overflow`.
#[derive(Debug)]
pub(super) struct ClusteringEntry<'a> {
    pub(super) time: CompactDateTime,
    pub(super) span: &'a HashMap<String, String>,
}

/// A single row returned by `peek_first_key_trigger`: trigger time and span
/// context.
pub(super) type PeekedTrigger = (CompactDateTime, HashMap<String, String>);

/// Capacity for the per-partition state cache.
///
/// Sized to cover the active working set of keys within a single partition.
/// Cache misses fall back to a DB read, so undersizing only costs an extra
/// query. On eviction of a hot key, the placeholder guard mechanism in
/// `quick_cache` ensures the next two concurrent accessors still serialize
/// correctly via the re-created mutex.
pub(super) const STATE_CACHE_CAPACITY: usize = 8_192;

/// Cached value type: an async mutex wrapping the resolved timer state.
///
/// Wrapping in `Arc<AsyncMutex<…>>` allows callers to clone the handle out of
/// the cache (dropping the internal shard lock), then `.lock().await` without
/// holding any cache internals across an await point.  All state-mutating
/// operations hold this lock for their entire read-then-write sequence,
/// preventing TOCTOU races between concurrent `EventContext` callers.
pub(super) type CachedState = Arc<AsyncMutex<TimerState>>;

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

/// Creates a new state cache with the standard capacity.
pub(super) fn new_state_cache() -> Arc<Cache<StateCacheKey, CachedState>> {
    Arc::new(Cache::new(STATE_CACHE_CAPACITY))
}
