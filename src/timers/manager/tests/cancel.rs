//! A timer call dropped at any await point leaves memory that covers its rows
//! and nothing more, and its stored row stays loadable. A dropped delivery
//! leaves no delivery running.

use super::{SLAB_SECONDS, Store, listed, past, seeded_store, slab_id};
use crate::Key;
use crate::consumer::Uncommitted;
use crate::consumer::partition::ShutdownPhase;
use crate::timers::active::{Insert, Listed};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::TimerManagerError;
use crate::timers::scheduler::TimerSchedulerError;
use crate::timers::store::TriggerStore;
use crate::timers::test_support::setup_timer_manager_with_store;
use crate::timers::{
    PendingTimer, TimerManager, TimerRequest, TimerType, Trigger, UncommittedTimer,
};
use color_eyre::eyre::{OptionExt, Result, ensure};
use futures::future::try_join_all;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut, poll};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::cell::Cell;
use std::future::pending;
use std::task::Poll;
use std::time::Duration;
use strum::VariantArray;
use tokio::runtime::Builder;
use tokio::task::coop::unconstrained;
use tokio::task::yield_now;
use tokio::time::timeout;
use tracing::Span;

/// Commands queued ahead of the victim, so its polls stop at different points.
const BACKLOGS: [usize; 3] = [0, 1, 64];

type Manager = TimerManager<Store>;

/// Drops a [`Victim`] call after `polls` polls, with `backlog` choosing how
/// many earlier schedules wait in the command channel. A [`Followup`] then
/// runs before the actor reads the dropped call's last command. A `due` victim
/// lands in a slab that the actor owns from the start, and a stored one must
/// fire. A delivery victim is always due.
#[test]
fn prop_dropped_call_keeps_rows_loadable() {
    fn property(
        victim: Victim,
        followup: Followup,
        backlog: u8,
        polls: u8,
        due: bool,
    ) -> Result<()> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run(victim, followup, backlog, polls, due))
    }
    QuickCheck::new().quickcheck(property as fn(Victim, Followup, u8, u8, bool) -> Result<()>);
}

async fn run(victim: Victim, followup: Followup, backlog: u8, polls: u8, due: bool) -> Result<()> {
    let due = due || victim.delivers();
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
    if victim.removes() || victim.delivers() {
        schedule(&manager, "victim", victim_time, false).await?;
    }
    let delivered = if victim.delivers() {
        let pending = timeout(Duration::from_secs(10), stream.next())
            .await?
            .ok_or_eyre("timer stream ended")?;
        Some(pending)
    } else {
        None
    };

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

    // The actor runs between polls. An even `polls` drops the call before the
    // actor reads its last command.
    let mut call = Box::pin(victim.call(&manager, victim_time, time, delivered));
    for poll in 0..polls / 2 % 4 {
        if poll > 0 {
            yield_now().await;
        }
        if let Poll::Ready(result) = unconstrained(async { poll!(call.as_mut()) }).await {
            result?;
            break;
        }
    }
    if polls % 2 == 1 {
        yield_now().await;
    }
    drop(call);
    followup.call(&manager, victim_time).await?;

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
    // Only a stored victim in an owned slab has an entry: the fillers and the
    // barrier sit in an unowned slab, and a load reads them later.
    let key = Key::from("victim");
    let snapshot = manager.snapshot().await?;
    ensure!(snapshot.in_flight == 0, "a dropped delivery still runs");
    let entries = snapshot.active;
    ensure!(
        entries == u32::from(due && !rows.is_empty()),
        "memory holds {entries} entries for {} victim rows",
        rows.len()
    );
    match followup {
        Followup::None => {}
        Followup::Schedule => ensure!(!rows.is_empty(), "the later schedule left no row"),
        Followup::UnscheduleAll => {
            for at in [victim_time, time] {
                let listed = listed(&manager, &key, at, TimerType::Application).await?;
                ensure!(
                    !matches!(listed, Some(Listed::Waits | Listed::Firing { again: true })),
                    "unschedule_all left a waiting victim"
                );
            }
        }
    }

    // A dropped delivery keeps its row for a reload, and only a later schedule
    // makes it wait again.
    let rescheduled = matches!(followup, Followup::Schedule);
    if victim.delivers() && !rescheduled && !rows.is_empty() {
        let listed = listed(&manager, &key, victim_time, TimerType::Application).await?;
        ensure!(listed.is_none(), "a dropped delivery is still listed");
    } else if due && !rows.is_empty() {
        waits_and_fires(&manager, stream.as_mut(), victim_time).await?;
    }
    Ok(())
}

/// Checks that the stored victim at `time` waits, leaves the queue first,
/// and fires.
async fn waits_and_fires<S>(manager: &Manager, mut stream: S, time: CompactDateTime) -> Result<()>
where
    S: Stream<Item = PendingTimer<Store>> + Unpin,
{
    let key = Key::from("victim");
    let listed = listed(manager, &key, time, TimerType::Application).await?;
    ensure!(
        listed == Some(Listed::Waits),
        "a stored victim in an owned slab does not wait"
    );
    let pending = timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or_eyre("timer stream ended")?;
    ensure!(pending.trigger().time == time, "a filler fired first");
    ensure!(
        pending.fire().await.is_some(),
        "the stored victim did not fire"
    );
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
    let mut call = Box::pin(insert(&manager, replacement.clone(), false, &written));
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

/// A call on the victim's key that runs right after the drop.
#[derive(Clone, Copy, Debug, VariantArray)]
enum Followup {
    None,
    /// Schedules the victim again, which must store its row.
    Schedule,
    /// Must leave no queued victim.
    UnscheduleAll,
}

impl Arbitrary for Followup {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::VARIANTS[usize::from(u8::arbitrary(g)) % Self::VARIANTS.len()]
    }
}

impl Followup {
    async fn call(self, manager: &Manager, time: CompactDateTime) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Schedule => schedule(manager, "victim", time, false).await,
            Self::UnscheduleAll => {
                let key = Key::from("victim");
                Ok(manager.unschedule_all(&key, TimerType::Application).await?)
            }
        }
    }
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
    /// Fires a stored victim that left the queue, and holds its delivery
    /// until the drop.
    Deliver,
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

    /// Whether the call delivers a stored victim.
    fn delivers(self) -> bool {
        matches!(self, Self::Deliver)
    }

    /// Whether the fillers use `clear_and_schedule`.
    fn clears(self) -> bool {
        matches!(self, Self::ClearAndSchedule | Self::Replace)
    }

    /// Runs the call on the victim at `time`. A replacement moves it to
    /// `elsewhere`, and a delivery fires `delivered`.
    async fn call(
        self,
        manager: &Manager,
        time: CompactDateTime,
        elsewhere: CompactDateTime,
        delivered: Option<PendingTimer<Store>>,
    ) -> Result<()> {
        let fails = match self {
            Self::Deliver => {
                let delivered = delivered.ok_or_eyre("no victim left the queue")?;
                ensure!(delivered.trigger().time == time, "a filler left the queue");
                let _firing = delivered.fire().await;
                return pending().await;
            }
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
        let written = Cell::new(false);
        let result = insert(manager, trigger("victim", time), fails, &written).await;
        ensure!(result.is_err() == fails, "the write outcome was lost");
        Ok(())
    }
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

/// Schedules `trigger` through the scheduler, with [`write_row`] as its row
/// write.
async fn insert(
    manager: &Manager,
    trigger: Trigger,
    fails: bool,
    written: &Cell<bool>,
) -> Result<(), TimerManagerError<<Store as TriggerStore>::Error>> {
    let (trigger, ticket) = manager
        .0
        .scheduler
        .insert(trigger, Insert::Schedule)
        .await?;
    ticket
        .run(write_row(manager, &trigger, fails, written))
        .await
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

/// Inputs that `prop_dropped_call_keeps_rows_loadable` found.
#[tokio::test]
async fn dropped_call_regressions() -> Result<()> {
    // A replacement dropped after its write left the replaced timer queued.
    run(Victim::Replace, Followup::UnscheduleAll, 0, 5, true).await?;
    // A schedule trusted an entry that a dropped unschedule then removed.
    run(Victim::Unschedule, Followup::Schedule, 0, 2, true).await
}
