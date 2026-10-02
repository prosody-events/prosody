//! Slab-index trigger operations of the Cassandra trigger store.

use super::extract_span_map;
use crate::Key;
use crate::cassandra::errors::CassandraStoreError;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::Slab;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::error::CassandraTriggerStoreError;
use crate::timers::{TimerType, Trigger};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt, pin_mut};
use opentelemetry::propagation::TextMapPropagator;
use std::collections::HashMap;
use tokio::task::coop::cooperative;
use tracing::instrument;

#[instrument(name = "get_slab_triggers", level = "debug", skip(store))]
pub(super) fn triggers<'a>(
    store: &'a CassandraTriggerStore,
    slab: &'a Slab,
    timer_type: TimerType,
) -> impl Stream<Item = Result<Trigger, CassandraTriggerStoreError>> + Send + use<'a> {
    let segment_id = store.segment.id;
    let slab_size = slab.size().seconds() as i32;
    let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

    try_stream! {
        let stream = store
            .session()
            .execute_iter(
                store.queries().get_slab_triggers.clone(),
                (segment_id, slab_size, slab_id, timer_type),
            )
            .await.map_err(CassandraStoreError::from)?
            .rows_stream::<(String, CompactDateTime, TimerType, HashMap<String, String>, Option<i32>)>().map_err(CassandraStoreError::from)?;

        pin_mut!(stream);
        while let Some((key, time, timer_type, span_map, tag_opt)) =
            cooperative(stream.try_next()).await.map_err(CassandraStoreError::from)?
        {
            let context = store.propagator().extract(&span_map);
            let tag = tag_opt.unwrap_or(0_i32);

            yield Trigger::restored(key.into(), time, timer_type, tag, context);
        }
    }
}

#[instrument(name = "get_slab_triggers_all_types", level = "debug", skip(store))]
pub(super) fn triggers_all_types<'a>(
    store: &'a CassandraTriggerStore,
    slab: &Slab,
) -> impl Stream<Item = Result<Trigger, CassandraTriggerStoreError>> + Send + use<'a> {
    let segment_id = store.segment.id;
    let slab_size = slab.size().seconds() as i32;
    let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());

    try_stream! {
        let stream = store
            .session()
            .execute_iter(
                store.queries().get_slab_triggers_all_types.clone(),
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
            let context = store.propagator().extract(&span_map);
            let tag = tag_opt.unwrap_or(0_i32);

            yield Trigger::restored(key.into(), time, timer_type, tag, context);
        }
    }
}

#[instrument(name = "insert_slab_trigger", level = "debug", skip(store), err)]
pub(super) async fn insert(
    store: &CassandraTriggerStore,
    slab: Slab,
    trigger: Trigger,
) -> Result<(), CassandraTriggerStoreError> {
    let span_map = extract_span_map(store.propagator(), &trigger);

    let segment_id = store.segment.id;
    let slab_size = slab.size().seconds() as i32;
    let slab_id = i32::from_le_bytes(slab.id().to_le_bytes());
    let key = trigger.key.as_ref();
    let time = trigger.time;
    let timer_type = trigger.timer_type;
    let tag = trigger.tag;

    let ttl = store.calculate_ttl(slab.range().end);
    store
        .execute_unpaged_discard(
            &store.queries().insert_slab_trigger,
            (
                segment_id, slab_size, slab_id, timer_type, key, time, &span_map, tag, ttl,
            ),
        )
        .await
}

#[instrument(name = "delete_slab_trigger", level = "debug", skip(store), err)]
pub(super) async fn delete(
    store: &CassandraTriggerStore,
    slab: &Slab,
    timer_type: TimerType,
    key: &Key,
    time: CompactDateTime,
) -> Result<(), CassandraTriggerStoreError> {
    store
        .session()
        .execute_unpaged(
            &store.queries().delete_slab_trigger,
            (
                store.segment.id,
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

#[instrument(name = "clear_slab_triggers", level = "debug", skip(store), err)]
pub(super) async fn clear(
    store: &CassandraTriggerStore,
    slab: &Slab,
) -> Result<(), CassandraTriggerStoreError> {
    store
        .session()
        .execute_unpaged(
            &store.queries().clear_slab_triggers,
            (
                store.segment.id,
                slab.size().seconds() as i32,
                i32::from_le_bytes(slab.id().to_le_bytes()),
            ),
        )
        .await
        .map_err(CassandraStoreError::from)?;

    Ok(())
}
