//! A timer call dropped at any await point leaves no hold behind, and its
//! stored row stays loadable.

use crate::Key;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::active::{Queued, TimerState};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::TimerManagerError;
use crate::timers::scheduler::TimerSchedulerError;
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::test_support::{setup_timer_manager_with_store, test_segment};
use crate::timers::{TimerManager, TimerRequest, TimerType, Trigger, UncommittedTimer};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt, pin_mut, poll};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::cell::Cell;
use std::task::Poll;
use std::time::Duration;
use strum::VariantArray;
use tokio::runtime::Builder;
use tokio::task::coop::unconstrained;
use tokio::task::yield_now;
use tokio::time::timeout;
use tracing::Span;

const SLAB_SECONDS: u32 = 60;

/// Commands queued ahead of the victim, so its polls stop at different points.
const BACKLOGS: [usize; 3] = [0, 1, 64];

type Store = TableAdapter<InMemoryTriggerStore>;
type Manager = TimerManager<Store>;

/// Drops a [`Victim`] call after `polls` polls, with `backlog` choosing how
/// many earlier schedules wait in the command channel. A `due` victim lands in
/// a slab that the actor owns from the start, and a stored one must fire.
#[test]
fn prop_dropped_call_keeps_rows_loadable() {
    fn property(victim: Victim, backlog: u8, polls: u8, due: bool) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(victim, backlog, polls, due))
    }
    QuickCheck::new().quickcheck(property as fn(Victim, u8, u8, bool) -> Result<()>);
}

async fn run(victim: Victim, backlog: u8, polls: u8, due: bool) -> Result<()> {
    let (store, now) = seeded_store(due).await?;
    let (stream, manager, _shutdown) =
        setup_timer_manager_with_store(store.clone(), ShutdownPhase::default()).await?;
    pin_mut!(stream);
    let time = now.add_duration(CompactDuration::new(3_600))?;
    // The victim has its own slab, so no other schedule registers it.
    let victim_time = if due {
        past(now)
    } else {
        time.add_duration(CompactDuration::new(SLAB_SECONDS))?
    };
    if victim.removes() {
        schedule(&manager, "victim", victim_time, false).await?;
    }

    // Start every filler without yielding to the actor.
    let clear = victim.clears();
    let count = BACKLOGS[usize::from(backlog) % BACKLOGS.len()];
    let fillers: Vec<_> = (0..count).map(|i| format!("filler-{i}")).collect();
    let mut fillers: Vec<_> = fillers
        .iter()
        .map(|key| Box::pin(schedule(&manager, key, time, clear)))
        .collect();
    for filler in &mut fillers {
        let pending = unconstrained(async { poll!(filler.as_mut()).is_pending() }).await;
        ensure!(pending, "a filler finished before the actor ran");
    }

    let mut call = Box::pin(victim.call(&manager, victim_time, time));
    for _ in 0..polls % 4 {
        if let Poll::Ready(result) = unconstrained(async { poll!(call.as_mut()) }).await {
            result?;
            break;
        }
        yield_now().await;
    }
    drop(call);

    // Commands are FIFO, so the barrier returns after every earlier command.
    try_join_all(fillers).await?;
    schedule(&manager, "barrier", time, clear).await?;

    let slab_id = slab_id(victim_time);
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
        "a dropped call left its row in unregistered slab {slab_id}"
    );
    let entry = manager
        .0
        .scheduler
        .active_triggers()
        .get(&Key::from("victim"), victim_time, TimerType::Application)
        .await;
    ensure!(
        entry.is_none_or(|entry| entry.state != TimerState::Aborted),
        "a dropped call left its hold"
    );

    if due && !rows.is_empty() {
        ensure!(
            entry.is_some(),
            "a stored victim in an owned slab left memory"
        );
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

/// A handler reschedules its own firing timer, and the call is dropped after
/// the row write lands. The delivery's commit must keep the replacement, so
/// the timer fires again.
#[tokio::test]
async fn dropped_reschedule_of_a_firing_timer_fires_after_commit() -> Result<()> {
    let (store, now) = seeded_store(true).await?;
    let (stream, manager, _shutdown) =
        setup_timer_manager_with_store(store, ShutdownPhase::default()).await?;
    pin_mut!(stream);

    let time = past(now);
    schedule(&manager, "victim", time, false).await?;
    let firing = timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or_eyre("timer stream ended")?
        .fire()
        .await
        .ok_or_eyre("the timer did not fire")?;

    let replacement = trigger("victim", time);
    let written = Cell::new(false);
    let mut call = Box::pin(manager.0.scheduler.insert(
        replacement.clone(),
        Queued::FiringRescheduled,
        write_row(&manager, &replacement, false, &written),
    ));
    while !written.get() {
        let polled = unconstrained(async { poll!(call.as_mut()) }).await;
        ensure!(
            polled.is_pending(),
            "the reschedule finished before the drop"
        );
        yield_now().await;
    }
    drop(call);

    // Commands are FIFO, so the barrier returns after the dropped call's last
    // command.
    let later = now.add_duration(CompactDuration::new(3_600))?;
    schedule(&manager, "barrier", later, false).await?;
    let (_, guard) = firing.into_inner();
    guard.commit().await;

    let times = manager
        .scheduled_times(&replacement.key, TimerType::Application)
        .await?;
    ensure!(times == [time], "the commit deleted the replacement row");
    let refired = timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or_eyre("timer stream ended")?;
    ensure!(refired.trigger().time == time, "another timer fired");
    ensure!(
        refired.fire().await.is_some(),
        "the replacement did not fire"
    );
    Ok(())
}

/// The call that the property drops.
#[derive(Clone, Copy, Debug, VariantArray)]
enum Victim {
    Schedule,
    ClearAndSchedule,
    /// A scheduler insert whose write stores the row and then stays pending.
    PendingWrite,
    /// A scheduler insert whose write stores the row and then fails.
    FailedWrite,
    /// Unschedules a stored victim.
    Unschedule,
    /// Replaces a stored victim with a timer in another slab.
    Replace,
}

impl Arbitrary for Victim {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::VARIANTS[usize::from(u8::arbitrary(g)) % Self::VARIANTS.len()]
    }
}

impl Victim {
    /// Whether the call removes a stored victim.
    fn removes(self) -> bool {
        matches!(self, Self::Unschedule | Self::Replace)
    }

    /// Whether the fillers use `clear_and_schedule`.
    fn clears(self) -> bool {
        matches!(self, Self::ClearAndSchedule | Self::Replace)
    }

    /// Runs the call on the victim at `time`. A replacement moves it to
    /// `elsewhere`.
    async fn call(
        self,
        manager: &Manager,
        time: CompactDateTime,
        elsewhere: CompactDateTime,
    ) -> Result<()> {
        let fails = match self {
            Self::Schedule | Self::ClearAndSchedule => {
                return schedule(manager, "victim", time, self.clears()).await;
            }
            Self::Unschedule => {
                let key = Key::from("victim");
                return Ok(manager
                    .unschedule(&key, time, TimerType::Application)
                    .await?);
            }
            Self::Replace => return schedule(manager, "victim", elsewhere, true).await,
            Self::PendingWrite => false,
            Self::FailedWrite => true,
        };
        let trigger = trigger("victim", time);
        let written = Cell::new(false);
        let write = write_row(manager, &trigger, fails, &written);
        let result = manager
            .0
            .scheduler
            .insert(trigger.clone(), Queued::Scheduled, write)
            .await;
        ensure!(result.is_err() == fails, "the write outcome was lost");
        Ok(())
    }
}

/// A memory store for a new segment. If `due`, the watermark sits below the
/// current slab, so the actor owns [`past`] slabs from the start.
async fn seeded_store(due: bool) -> Result<(Store, CompactDateTime)> {
    let store = memory_store(test_segment("dropped", SLAB_SECONDS));
    let now = CompactDateTime::now()?;
    store.insert_segment().await?;
    if due {
        store
            .set_slab_watermark(slab_id(now).checked_sub(1))
            .await?;
    }
    Ok((store, now))
}

/// A time two slabs before `now`.
fn past(now: CompactDateTime) -> CompactDateTime {
    CompactDateTime::from(now.epoch_seconds().saturating_sub(2 * SLAB_SECONDS))
}

fn slab_id(time: CompactDateTime) -> SlabId {
    Slab::from_time(CompactDuration::new(SLAB_SECONDS), time).id()
}

fn trigger(key: &str, time: CompactDateTime) -> Trigger {
    TimerRequest::new(
        Key::from(key),
        time,
        TimerType::Application,
        Span::current(),
    )
    .into_trigger()
}

/// Stores the row of `trigger`, sets `written`, and then fails or stays
/// pending for one poll. A Cassandra write can do either.
async fn write_row(
    manager: &Manager,
    trigger: &Trigger,
    fails: bool,
    written: &Cell<bool>,
) -> Result<(), TimerManagerError<<Store as TriggerStore>::Error>> {
    manager
        .0
        .store
        .add_trigger(trigger.clone())
        .await
        .map_err(TimerManagerError::Store)?;
    written.set(true);
    if fails {
        return Err(TimerManagerError::Scheduler(TimerSchedulerError::Shutdown));
    }
    yield_now().await;
    Ok(())
}

async fn schedule(manager: &Manager, key: &str, time: CompactDateTime, clear: bool) -> Result<()> {
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
