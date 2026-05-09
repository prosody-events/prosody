//! The `state` column domain model and per-partition cache.
//!
//! The V3 schema stores a `state` static map column alongside clustering rows.
//! Each `(key, timer_type)` entry in that map is a `key_timer_state` UDT,
//! represented here as [`TimerState`]. Reading this single column tells the
//! store how many timers exist for a key without touching the clustering rows
//! at all — the common single-timer path never issues a clustering scan.
//!
//! The in-memory cache (`CachedState` — an `Arc<AsyncMutex<TimerState>>`)
//! keeps the resolved state hot between operations and serialises mutations
//! per `(key, timer_type)`. Callers clone the handle out of the cache before
//! awaiting the mutex, so no `quick_cache` shard lock is held across an await.
//! Every state-mutating operation in the trigger store holds this mutex for
//! its full read-decide-write sequence; that is the only synchronisation
//! point that makes those operations linearisable against each other.

use crate::Key;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use quick_cache::sync::Cache;
use scylla::_macro_internal::{CellWriter, ColumnType, WrittenCellProof};
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
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
    pub(super) tag: i32,
}

/// A single row returned by `peek_first_key_trigger`: trigger time, span, and
/// tag.
pub(super) type PeekedTrigger = (CompactDateTime, HashMap<String, String>, Option<i32>);

/// Capacity for the per-partition state cache.
///
/// Sized to cover the active working set of keys within a single partition.
/// Cache misses fall back to a DB read, so undersizing only costs an extra
/// query. On eviction of a hot key, the placeholder guard mechanism in
/// `quick_cache` ensures the next two concurrent accessors still serialize
/// correctly via the re-created mutex.
pub(super) const STATE_CACHE_CAPACITY: usize = 8_192;

/// Per-`(key, timer_type)` mutex wrapping the resolved timer state.
///
/// Callers obtain the handle via `resolve_state` (which drops `quick_cache`'s
/// internal shard lock as soon as the `Arc` is cloned), then `.lock().await`
/// without holding any cache internals across an await. State-mutating
/// `TriggerOperations` methods hold this mutex from the state read through
/// the DB write, so concurrent operations on the same `(key, timer_type)`
/// linearise; operations on different `(key, timer_type)` pairs do not block
/// each other.
pub(super) type CachedState = Arc<AsyncMutex<TimerState>>;

/// Timer data for a single inlined timer.
///
/// This is the resolved domain type for a key with exactly one timer.
/// The `tag` field mirrors the commit-oracle tag stored on the `Trigger`.
/// Pre-migration rows have `tag = NULL` which normalises to `0`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineTimer {
    /// Timer trigger time.
    pub time: CompactDateTime,
    /// OpenTelemetry span context for trace continuity.
    pub span: HashMap<String, String>,
    /// Commit-oracle tag. `0` for pre-migration rows (tag column absent).
    pub tag: i32,
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
#[derive(Clone, Debug, scylla::DeserializeValue, scylla::SerializeValue)]
struct RawTimerState {
    /// `true` = inline data present; `false`/`null` = overflow marker.
    inline: Option<bool>,
    /// Timer time (present only when `inline = true`).
    time: Option<CompactDateTime>,
    /// Span context (present only when `inline = true`).
    span: Option<HashMap<String, String>>,
    /// Commit-oracle tag (added by `20260507_add_tag_to_udt` migration).
    /// `None` for rows written before migration; normalised to `0`.
    tag: Option<i32>,
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
                tag: Some(timer.tag),
            },
            Self::Overflow => RawTimerState {
                inline: Some(false),
                time: None,
                span: None,
                tag: None,
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
                    tag: raw.tag.unwrap_or(0_i32),
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
