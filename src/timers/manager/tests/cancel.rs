//! A timer call dropped at any await point leaves no hold behind, and its
//! written row stays loadable.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::active::TimerState;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::TimerManagerError;
use crate::timers::scheduler::BUFFER_SIZE;
use crate::timers::scheduler::TimerSchedulerError;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::test_support::{setup_timer_manager_with_store, test_segment};
use crate::timers::{TimerManager, TimerRequest, TimerType};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, pin_mut, poll};
use quickcheck::QuickCheck;
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::coop::unconstrained;
use tokio::task::yield_now;
use tokio::time::timeout;
use tracing::Span;

const SLAB_SECONDS: u32 = 60;

type Manager = TimerManager<TableAdapter<InMemoryTriggerStore>>;

/// Drops a schedule after `polls` polls, with `pressure` choosing how many
/// earlier schedules fill the command channel. A full channel parks a command
/// send, which is where a dropped call once left a row in an unregistered
/// slab. A `due` victim lands in a slab below the seeded watermark, which the
/// actor owns from the start, and a stored one must fire. `op` picks the
/// victim's call; see [`victim`].
#[test]
fn prop_dropped_schedule_keeps_rows_loadable() {
    fn property(op: u8, pressure: u8, polls: u8, due: bool) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(op, pressure, polls, due))
    }
    QuickCheck::new().quickcheck(property as fn(u8, u8, u8, bool) -> Result<()>);
}

async fn run(op: u8, pressure: u8, polls: u8, due: bool) -> Result<()> {
    let clear = op % 2 == 1;
    let segment = test_segment("dropped", SLAB_SECONDS);
    let store = memory_store(segment.clone());
    let now = CompactDateTime::now()?;
    store.insert_segment().await?;
    if due {
        let current = Slab::from_time(segment.slab_size, now).id();
        store.set_slab_watermark(current.checked_sub(1)).await?;
    }
    let (stream, manager, _shutdown) =
        setup_timer_manager_with_store(store.clone(), ShutdownPhase::default()).await?;
    pin_mut!(stream);
    let time = now.add_duration(CompactDuration::new(3_600))?;
    // The victim has its own slab, so no other schedule registers it.
    let victim_time = if due {
        CompactDateTime::from(now.epoch_seconds().saturating_sub(2 * SLAB_SECONDS))
    } else {
        time.add_duration(CompactDuration::new(SLAB_SECONDS))?
    };

    // Start every filler without yielding to the actor.
    let count = [0, BUFFER_SIZE - 1, BUFFER_SIZE][usize::from(pressure) % 3];
    let mut fillers: Vec<_> = (0..count)
        .map(|i| Box::pin(schedule(&manager, format!("filler-{i}"), time, clear)))
        .collect();
    for filler in &mut fillers {
        let pending = unconstrained(async { poll!(filler.as_mut()).is_pending() }).await;
        ensure!(pending, "a filler finished before the actor ran");
    }

    let mut victim = Box::pin(victim(&manager, victim_time, op));
    for _ in 0..polls % 4 {
        if let Poll::Ready(result) = unconstrained(async { poll!(victim.as_mut()) }).await {
            result?;
            break;
        }
        yield_now().await;
    }
    drop(victim);

    // Commands are FIFO, so the barrier returns after every earlier command.
    try_join_all(fillers).await?;
    schedule(&manager, "barrier".to_owned(), time, clear).await?;

    let slab_id = Slab::from_time(segment.slab_size, victim_time).id();
    let rows: Vec<_> = store
        .get_slab_triggers_all_types(slab_id)
        .try_collect()
        .await?;
    let registered: Vec<_> = store
        .get_slab_range(slab_id..=slab_id)
        .try_collect()
        .await?;
    ensure!(
        rows.is_empty() || !registered.is_empty(),
        "a dropped schedule left its row in unregistered slab {slab_id}"
    );
    let entry = manager
        .0
        .scheduler
        .active_triggers()
        .get(&Key::from("victim"), victim_time, TimerType::Application)
        .await;
    ensure!(
        entry.is_none_or(|entry| entry.state != TimerState::Aborted),
        "a dropped schedule left its hold"
    );

    if due && !rows.is_empty() {
        let pending = timeout(Duration::from_secs(10), stream.next())
            .await?
            .ok_or_eyre("timer stream ended")?;
        ensure!(
            pending.trigger().time == victim_time,
            "a filler fired first"
        );
        ensure!(
            pending.fire().await.is_some(),
            "the stored victim did not fire"
        );
    }
    Ok(())
}

/// Schedules the victim through the manager, or through a scheduler insert
/// whose write stores the row and then stays pending or fails. A Cassandra
/// write can do either.
async fn victim(manager: &Manager, time: CompactDateTime, op: u8) -> Result<()> {
    if op % 4 < 2 {
        return schedule(manager, "victim".to_owned(), time, op % 2 == 1).await;
    }
    let fails = op % 4 == 3;
    let request = TimerRequest::new(
        Key::from("victim"),
        time,
        TimerType::Application,
        Span::current(),
    );
    let trigger = request.into_trigger();
    let write = async {
        manager
            .0
            .store
            .add_trigger(trigger.clone())
            .await
            .map_err(TimerManagerError::Store)?;
        if fails {
            return Err(TimerManagerError::Scheduler(TimerSchedulerError::Shutdown));
        }
        yield_now().await;
        Ok(())
    };
    let result = manager
        .0
        .scheduler
        .insert(trigger.clone(), TimerState::Scheduled, write)
        .await;
    ensure!(result.is_err() == fails, "the write outcome was lost");
    Ok(())
}

async fn schedule(
    manager: &Manager,
    key: String,
    time: CompactDateTime,
    clear: bool,
) -> Result<()> {
    let request = TimerRequest::new(
        Key::from(key),
        time,
        TimerType::Application,
        Span::current(),
    );
    if clear {
        manager.clear_and_schedule(request).await?;
    } else {
        manager.schedule(request).await?;
    }
    Ok(())
}
