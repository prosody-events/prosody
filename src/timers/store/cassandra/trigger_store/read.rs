//! Key-index reads of the Cassandra trigger store.

use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::store::cassandra::state::TimerState;
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use std::collections::HashMap;
use std::sync::Arc;
use strum::VariantArray;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::coop::cooperative;
use tracing::field::Empty;
use tracing::{Span, instrument};

#[instrument(name = "get_key_times", level = "debug", skip(store), fields(state_cached = Empty))]
pub(super) fn times<'a>(
    store: &'a CassandraTriggerStore,
    timer_type: TimerType,
    key: &'a Key,
) -> impl Stream<Item = Result<CompactDateTime, CassandraTriggerStoreError>> + Send + use<'a> {
    let key_clone = key.clone();
    let segment_id = store.segment.id;

    try_stream! {
        let (handle, cached) = store.resolve_state(&segment_id, &key_clone, timer_type).await?;
        Span::current().record("state_cached", cached);

        // Snapshot the state and release the lock before any DB I/O — a
        // concurrent transition would just shift the snapshot, which is
        // fine for a read-only stream.
        let state = handle.lock().await.clone();
        match state {
            TimerState::Inline(timer) => {
                // Inline: yield time from cache (0 clustering query).
                yield timer.time;
            }
            TimerState::Overflow => {
                // Overflow: scan clustering rows.
                let stream = store
                    .session()
                    .execute_iter(
                        store.queries().get_key_times.clone(),
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

#[instrument(name = "get_key_triggers", level = "debug", skip(store), fields(state_cached = Empty))]
pub(super) fn triggers<'a>(
    store: &'a CassandraTriggerStore,
    timer_type: TimerType,
    key: &'a Key,
) -> impl Stream<Item = Result<Trigger, CassandraTriggerStoreError>> + Send + use<'a> {
    let key_clone = key.clone();
    let segment_id = store.segment.id;

    try_stream! {
        let (handle, cached) = store.resolve_state(&segment_id, &key_clone, timer_type).await?;
        Span::current().record("state_cached", cached);

        // Snapshot the state and release the lock before any DB I/O — a
        // concurrent transition would just shift the snapshot, which is
        // fine for a read-only stream.
        let state = handle.lock().await.clone();
        match state {
            TimerState::Inline(timer) => {
                // Inline: yield trigger from cache (0 clustering query).
                let context = store.propagator().extract(&timer.span);
                yield Trigger::restored(key_clone.clone(), timer.time, timer_type, timer.tag, context);
            }
            TimerState::Overflow => {
                // Overflow: scan clustering rows.
                let stream = store
                    .session()
                    .execute_iter(
                        store.queries().get_key_triggers.clone(),
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
                    let context = store.propagator().extract(&span_map);
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

#[instrument(name = "get_key_triggers_all_types", level = "debug", skip(store), fields(state_cached = Empty))]
pub(super) fn triggers_all_types<'a>(
    store: &'a CassandraTriggerStore,
    key: &'a Key,
) -> impl Stream<Item = Result<Trigger, CassandraTriggerStoreError>> + Send + use<'a> {
    let key_clone = key.clone();
    let segment_id = store.segment.id;

    try_stream! {
        // Read all types in a single query, then build the state_map.
        let raw_map = store.fetch_state_map(&segment_id, &key_clone).await?;
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
            if let Err(guard) = store.state_cache.get_value_or_guard_async(&cache_key).await {
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
            let clustering_stream = store.session()
                .execute_iter(
                    store.queries().get_key_triggers_all_types.clone(),
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
                store.propagator(),
            );

            // For each clustering row, flush any inline entries that
            // sort before it.
            while let Some(clustering) = advance_clustering(
                &key_clone,
                &mut clustering_stream,
                store.propagator(),
            ).await? {
                while let Some(s) = inline_next.take() {
                    if (s.timer_type, s.time) <= (clustering.timer_type, clustering.time) {
                        yield s;
                        inline_next = advance_inline(
                            &key_clone,
                            &state_map,
                            &mut variants_iter,
                            store.propagator(),
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
                    store.propagator(),
                );
            }
        } else {
            // All types are Inline or Absent — yield inline entries in
            // type order, no clustering query needed.
            for &tt in TimerType::VARIANTS {
                if let Some(TimerState::Inline(timer)) = state_map.get(&tt) {
                    let context = store.propagator().extract(&timer.span);
                    yield Trigger::restored(key_clone.clone(), timer.time, tt, timer.tag, context);
                }
            }
        }
    }
}

/// Reads the current trigger through the partition writer's shared cache.
/// Per-key serialization orders admission after every prior store mutation.
#[instrument(name = "current_trigger", level = "debug", skip(store), fields(state_cached = Empty), err)]
pub(super) async fn current(
    store: &CassandraTriggerStore,
    key: &Key,
    time: CompactDateTime,
    timer_type: TimerType,
) -> Result<Option<Trigger>, CassandraTriggerStoreError> {
    let segment_id = store.segment.id;
    let (handle, cached) = store.resolve_state(&segment_id, key, timer_type).await?;
    Span::current().record("state_cached", cached);

    let guard = handle.lock().await;
    match &*guard {
        TimerState::Inline(timer) if timer.time == time => Ok(Some(Trigger::restored(
            key.clone(),
            time,
            timer_type,
            timer.tag,
            store.propagator().extract(&timer.span),
        ))),
        TimerState::Inline(_) | TimerState::Absent => Ok(None),
        TimerState::Overflow => {
            let row = store
                .session()
                .execute_unpaged(
                    &store.queries().current_trigger_key,
                    (&segment_id, key.as_ref(), timer_type, time),
                )
                .await
                .map_err(CassandraStoreError::from)?
                .into_rows_result()
                .map_err(CassandraStoreError::from)?
                .maybe_first_row::<(Option<i32>, HashMap<String, String>)>()
                .map_err(CassandraStoreError::from)?;
            Ok(row.map(|(tag, span)| {
                Trigger::restored(
                    key.clone(),
                    time,
                    timer_type,
                    tag.unwrap_or(0_i32),
                    store.propagator().extract(&span),
                )
            }))
        }
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
