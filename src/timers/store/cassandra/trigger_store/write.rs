//! Key-index writes of the Cassandra trigger store.

use super::extract_span_map;
use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::SegmentId;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::state::{ClusteringEntry, InlineTimer, TimerState};
use crate::timers::{TimerType, Trigger, TriggerId};
use futures::{
    TryStreamExt,
    future::{join_all, ready},
};
use opentelemetry::propagation::TextMapCompositePropagator;
use smallvec::SmallVec;
use std::collections::HashMap;
use strum::VariantArray;
use tracing::field::Empty;
use tracing::{Span, instrument};

/// Upserts a trigger into the key index with state-aware transitions.
///
/// Uses `resolve_state` (cache-first, warms all types on miss):
/// - **Inline(old)**: Promote old timer to clustering + write new to clustering
///   + set overflow state → `Overflow`
/// - **Overflow**: Write clustering only (1 query) → stays `Overflow`
/// - **Absent**: Set inline state with new timer directly → `Inline(new)`
///   (post-V3 Absent is unambiguous: 0 timers, no clustering rows)
#[instrument(name = "upsert_key_trigger", level = "debug", skip(store), fields(state_cached = Empty), err)]
pub(super) async fn upsert(
    store: &CassandraTriggerStore,
    trigger: Trigger,
) -> Result<(), CassandraTriggerStoreError> {
    let segment_id = store.segment.id;
    let pending = PendingKeyTrigger::from_trigger(store.propagator(), &trigger);

    let (handle, cached) = store
        .resolve_state(&segment_id, &pending.id.key, pending.id.timer_type)
        .await?;
    Span::current().record("state_cached", cached);

    let mut guard = handle.lock().await;
    let transition = KeyUpsertTransition::from_state(&guard, pending);
    *guard = transition.apply(store, &segment_id).await?;

    Ok(())
}

/// Deletes a specific trigger from the key index with state-aware
/// demotion.
///
/// Uses `resolve_state` (cache-first, warms all types on miss):
/// - **Inline(timer), time matches**: Remove state entry → `Absent`
/// - **Inline(timer), time mismatch**: No-op (Inline guarantees zero clustering
///   rows) → stays `Inline`
/// - **Overflow**: One pre-delete read (LIMIT 3) drives a single atomic batch:
///   - 0 surviving rows → batch DELETE target + DELETE `state[type]` → `Absent`
///   - 1 surviving row → batch DELETE target + DELETE survivor + UPDATE state
///     Inline → `Inline(survivor)`
///   - 2+ surviving rows → single DELETE target → stays `Overflow`
/// - **Absent**: No-op (post-V3 Absent is unambiguous: 0 timers, no rows)
#[instrument(name = "delete_key_trigger", level = "debug", skip(store), fields(state_cached = Empty), err)]
pub(super) async fn delete(
    store: &CassandraTriggerStore,
    timer_type: TimerType,
    key: &Key,
    time: CompactDateTime,
) -> Result<(), CassandraTriggerStoreError> {
    let segment_id = store.segment.id;

    let (handle, cached) = store.resolve_state(&segment_id, key, timer_type).await?;
    Span::current().record("state_cached", cached);

    let mut guard = handle.lock().await;
    match &*guard {
        TimerState::Inline(timer) if timer.time == time => {
            // Inline timer matches the delete target → remove state, become Absent.
            // No clustering row exists for inline timers, so only remove state.
            store
                .remove_state_entry(&segment_id, key, timer_type)
                .await?;
            *guard = TimerState::Absent;
        }
        TimerState::Overflow => {
            // Read pre-delete clustering rows (LIMIT 3) in one round-trip.
            // After filtering the target out, the survivor count drives
            // the post-delete state — and any survivor's data is already
            // in hand to feed straight into the atomic write batch below.
            let triggers = store
                .peek_three_key_triggers(&segment_id, key, timer_type)
                .await?;
            let mut survivors = triggers.into_iter().filter(|(t, ..)| *t != time);

            match (survivors.next(), survivors.next()) {
                (None, _) => {
                    // No survivor → atomic DELETE target + DELETE state[type].
                    store
                        .batch_delete_to_absent(&segment_id, key, timer_type, time)
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
                    store
                        .batch_delete_to_inline(
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
                    store
                        .execute_unpaged_discard(
                            &store.queries().delete_key_trigger,
                            (&segment_id, key.as_ref(), timer_type, time),
                        )
                        .await?;
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
#[instrument(name = "clear_key_triggers", level = "debug", skip(store), err)]
pub(super) async fn clear(
    store: &CassandraTriggerStore,
    timer_type: TimerType,
    key: &Key,
) -> Result<(), CassandraTriggerStoreError> {
    let segment_id = store.segment.id;
    let (handle, _) = store.resolve_state(&segment_id, key, timer_type).await?;
    let mut guard = handle.lock().await;

    if matches!(*guard, TimerState::Absent) {
        return Ok(());
    }

    // Atomic BATCH: clear clustering rows + remove state entry.
    store
        .execute_unpaged_discard(
            &store.queries().batch_clear_key_triggers,
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
/// - **Inline or Absent**: plain UPDATE on the static column (0 tombstones).
///   Post-V3 Absent is unambiguous — no clustering rows to delete.
/// - **Overflow**: BATCH (DELETE clustering + UPDATE state).
///
/// `resolve_state` returns the per-`(key, timer_type)` mutex; holding
/// `handle.lock().await` for the entire match serialises the
/// read-decide-write sequence against concurrent same-key writers.
#[instrument(name = "clear_and_schedule_key", level = "debug", skip(store), fields(state_cached = Empty), err)]
pub(super) async fn clear_and_schedule(
    store: &CassandraTriggerStore,
    trigger: Trigger,
) -> Result<SmallVec<[CompactDateTime; 1]>, CassandraTriggerStoreError> {
    let segment_id = store.segment.id;
    // Extract span context for storage.
    let span_map = extract_span_map(store.propagator(), &trigger);

    let new_state = TimerState::Inline(InlineTimer {
        time: trigger.time,
        span: span_map,
        tag: trigger.tag,
    });

    let (handle, cached) = store
        .resolve_state(&segment_id, &trigger.key, trigger.timer_type)
        .await?;
    Span::current().record("state_cached", cached);

    let mut guard = handle.lock().await;
    let old_times: SmallVec<[CompactDateTime; 1]> = match &*guard {
        TimerState::Absent => {
            // Fast path: no prior timer — plain UPDATE, no tombstone, no old times.
            store
                .set_state_inline(&segment_id, &trigger.key, trigger.timer_type, &new_state)
                .await?;
            SmallVec::new()
        }
        TimerState::Inline(t) => {
            // Fast path: one prior timer already in the state column.
            // Return its time (if distinct) so the adapter can clean the slab index.
            let old_time = t.time;
            store
                .set_state_inline(&segment_id, &trigger.key, trigger.timer_type, &new_state)
                .await?;
            if old_time == trigger.time {
                SmallVec::new()
            } else {
                SmallVec::from_buf([old_time])
            }
        }
        TimerState::Overflow => {
            clear_overflow_and_schedule_key(store, &segment_id, &trigger, &new_state).await?
        }
    };

    *guard = new_state;
    Ok(old_times)
}

#[instrument(
    name = "clear_key_triggers_all_types",
    level = "debug",
    skip(store),
    err
)]
pub(super) async fn clear_all_types(
    store: &CassandraTriggerStore,
    key: &Key,
) -> Result<(), CassandraTriggerStoreError> {
    let segment_id = store.segment.id;

    // Acquire per-key locks for all timer types before mutating.
    // Order is deterministic (matches TimerType discriminant order) so
    // two concurrent clear_key_triggers_all_types calls cannot deadlock.
    // Use resolve_state to read real DB state on cache miss, avoiding
    // stale Absent entries if DB operations later fail.
    let mut handles = Vec::with_capacity(TimerType::VARIANTS.len());
    for &tt in TimerType::VARIANTS {
        let (handle, _) = store.resolve_state(&segment_id, key, tt).await?;
        handles.push(handle);
    }
    let mut guards: Vec<_> = join_all(handles.iter().map(|h| h.lock())).await;

    if guards.iter().all(|g| matches!(**g, TimerState::Absent)) {
        return Ok(());
    }

    // Atomic BATCH: clear all clustering rows + clear entire state column.
    store
        .execute_unpaged_discard(
            &store.queries().batch_clear_key_triggers_all_types,
            (&segment_id, key.as_ref(), &segment_id, key.as_ref()),
        )
        .await?;

    // Update all cached states to Absent.
    for guard in &mut guards {
        **guard = TimerState::Absent;
    }

    Ok(())
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
        .rows_stream::<(CompactDateTime,)>()
        .map_err(CassandraStoreError::from)?
        .map_err(CassandraStoreError::from)
        .map_ok(|(time,)| time)
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
        let ttl = store.calculate_ttl(self.id.time);
        store
            .execute_unpaged_discard(
                &store.queries().insert_key_trigger_clustering,
                (
                    segment_id,
                    self.id.key.as_ref(),
                    self.id.timer_type,
                    self.id.time,
                    &self.span_map,
                    self.tag,
                    ttl,
                ),
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
    UpsertClustering(PendingKeyTrigger),
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
            TimerState::Overflow => Self::UpsertClustering(new),
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
                Ok(TimerState::Overflow)
            }
            Self::UpsertClustering(new) => {
                new.insert_clustering(store, segment_id).await?;
                Ok(TimerState::Overflow)
            }
        }
    }
}
