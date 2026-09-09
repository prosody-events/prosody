//! Explicit actor steps for admission after partial timer writes.

use super::TriggerScheduler;
use super::actor::{ActorState, load_step, process_command};
use crate::timers::duration::CompactDuration;
use crate::timers::manager::TimerManager;
use crate::timers::queue::TriggerQueue;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::test_support::test_segment;
use crate::timers::{CompactDateTime, TimerType, Trigger};
use color_eyre::eyre::{Result, ensure, eyre};
use futures::TryStreamExt;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::Span;

/// Loads before and after admission to check both durable indexes and the
/// queue.
pub(crate) async fn retirement_trace<F, Fut>(mode: u8, tag: i32, admit: F) -> Result<()>
where
    F: FnOnce(TimerManager<TableAdapter<InMemoryTriggerStore>>, Trigger) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let segment = test_segment("admission", 300_u32);
    let store = memory_store(segment.clone());
    let time = CompactDateTime::from(CompactDateTime::now()?.epoch_seconds().saturating_sub(600));
    let slab = Slab::from_time(segment.slab_size, time);
    let old = Trigger::with_tag(
        Arc::from("timer-admission"),
        time,
        TimerType::Application,
        tag,
        Span::current(),
    );
    let mut replacement = old.clone();
    replacement.tag += 1_i32;
    store.insert_segment().await?;
    store.insert_slab(slab.clone()).await?;
    if mode % 3 != 2 {
        store
            .operations()
            .insert_slab_trigger(slab.clone(), old.clone())
            .await?;
    }
    if !mode.is_multiple_of(3) {
        store
            .operations()
            .upsert_key_trigger(replacement.clone())
            .await?;
    }
    let mut queue = TriggerQueue::new();
    let active = queue.active_triggers().clone();
    let (command_tx, mut commands) = mpsc::channel(1);
    let manager = TimerManager::with_test_scheduler(
        store.clone(),
        TriggerScheduler {
            command_tx,
            active_triggers: active.clone(),
        },
    );
    let mut state = ActorState {
        store: store.clone(),
        segment,
        known_slab_ids: BTreeSet::new(),
        last_persisted_watermark: None,
        highest_loaded_slab_id: None,
        preload_window: CompactDuration::new(60),
        next_load_at: Instant::now(),
    };
    // The empty arm removes slab metadata before admission repairs the key-only
    // schedule.
    load_step(&mut state, &mut queue).await;
    ensure!(
        state
            .highest_loaded_slab_id
            .is_some_and(|end| end >= slab.id())
    );
    let admission = admit(manager, old.clone());
    let actor = async {
        let command = commands
            .recv()
            .await
            .ok_or_else(|| eyre!("admission sent no timer command"))?;
        let rows: Vec<_> = store
            .get_slab_triggers_all_types(slab.id())
            .try_collect()
            .await?;
        let expected = (!mode.is_multiple_of(3)).then_some(tag + 1_i32);
        let repaired = rows.iter().map(|t| t.tag).collect::<Vec<_>>()
            == expected.into_iter().collect::<Vec<_>>();
        process_command(&mut state, &mut queue, command).await;
        ensure!(repaired, "command preceded repair");
        Result::<()>::Ok(())
    };
    let (admitted, handled) = tokio::join!(admission, actor);
    admitted?;
    handled?;
    load_step(&mut state, &mut queue).await;
    let expected = (!mode.is_multiple_of(3)).then_some(tag + 1_i32);
    ensure!(
        active.get_tag(&old.key, time, old.timer_type).await == expected,
        "retirement left the wrong queued attempt"
    );
    ensure!(store.current_tag(&old.key, time, old.timer_type).await? == expected);
    let rows: Vec<_> = store
        .get_slab_triggers_all_types(slab.id())
        .map_ok(|t| t.tag)
        .try_collect()
        .await?;
    ensure!(rows == expected.into_iter().collect::<Vec<_>>());
    if expected.is_some() {
        let metadata: Vec<_> = store
            .get_slab_range(slab.id()..=slab.id())
            .try_collect()
            .await?;
        ensure!(metadata.len() == 1, "replacement lost slab metadata");
    }
    Ok(())
}
