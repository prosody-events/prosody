//! A timer call dropped at any await point leaves every written row in a
//! registered slab.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::scheduler::BUFFER_SIZE;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::test_support::{setup_timer_manager_with_store, test_segment};
use crate::timers::{TimerManager, TimerRequest, TimerType};
use color_eyre::eyre::{Result, ensure};
use futures::future::try_join_all;
use futures::{TryStreamExt, poll};
use quickcheck::QuickCheck;
use std::task::Poll;
use tokio::runtime::Builder;
use tokio::task::coop::unconstrained;
use tokio::task::yield_now;
use tracing::Span;

const SLAB_SECONDS: u32 = 60;

type Manager = TimerManager<TableAdapter<InMemoryTriggerStore>>;

/// Drops a schedule after `polls` polls, with `pressure` choosing how many
/// earlier schedules fill the command channel. A full channel parks a command
/// send, which is where a dropped call once left a row in an unregistered
/// slab.
#[test]
fn prop_dropped_schedule_keeps_rows_loadable() {
    fn property(clear: bool, pressure: u8, polls: u8) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(clear, pressure, polls))
    }
    QuickCheck::new().quickcheck(property as fn(bool, u8, u8) -> Result<()>);
}

async fn run(clear: bool, pressure: u8, polls: u8) -> Result<()> {
    let segment = test_segment("dropped", SLAB_SECONDS);
    let store = memory_store(segment.clone());
    let (_stream, manager, _shutdown) =
        setup_timer_manager_with_store(store.clone(), ShutdownPhase::default()).await?;
    let time = CompactDateTime::now()?.add_duration(CompactDuration::new(3_600))?;
    // The victim has its own slab, so no other schedule registers it.
    let victim_time = time.add_duration(CompactDuration::new(SLAB_SECONDS))?;

    // Start every filler without yielding to the actor.
    let count = [0, BUFFER_SIZE - 1, BUFFER_SIZE][usize::from(pressure) % 3];
    let mut fillers: Vec<_> = (0..count)
        .map(|i| Box::pin(schedule(&manager, format!("filler-{i}"), time, clear)))
        .collect();
    for filler in &mut fillers {
        let pending = unconstrained(async { poll!(filler.as_mut()).is_pending() }).await;
        ensure!(pending, "a filler finished before the actor ran");
    }

    let mut victim = Box::pin(schedule(&manager, "victim".to_owned(), victim_time, clear));
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
