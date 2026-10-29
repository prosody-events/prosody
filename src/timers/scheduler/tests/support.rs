//! A scheduler actor that tests step by hand, and the retirement trace that
//! admission tests share.

use super::super::actor::load::load_step;
use super::super::actor::{ActorState, process_command};
use super::super::{Command, TriggerScheduler};
use crate::Topic;
use crate::state::TimerEventRef;
use crate::telemetry::Telemetry;
use crate::telemetry::event::{Data, TelemetryEvent, TimerEventType, TimerTelemetryEvent};
use crate::timers::active::Timers;
use crate::timers::duration::CompactDuration;
use crate::timers::manager::{TimerManager, TimerManagerConfig};
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::adapter::TableAdapter;
use crate::timers::store::memory::{InMemoryTriggerStore, memory_store};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::test_support::test_segment;
use crate::timers::{CompactDateTime, TimerType, Trigger};
use color_eyre::eyre::{Result, ensure};
use futures::TryStreamExt;
use std::pin::pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::Span;

pub(crate) type TestStore = TableAdapter<InMemoryTriggerStore>;
type Error = <TestStore as TriggerStore>::Error;

/// A scheduler actor that a test steps by hand.
pub(super) struct Actor {
    pub(super) state: ActorState<TestStore>,
    pub(super) timers: Timers,
    scheduler: TriggerScheduler<Error>,
    commands: UnboundedReceiver<Command<Error>>,
}

impl Actor {
    pub(super) fn new(state: ActorState<TestStore>) -> Self {
        let (scheduler, commands) = TriggerScheduler::channel();
        Self {
            state,
            timers: Timers::default(),
            scheduler,
            commands,
        }
    }

    /// A handle that sends commands to this actor.
    pub(super) fn scheduler(&self) -> TriggerScheduler<Error> {
        self.scheduler.clone()
    }

    /// Serves commands until `call` ends, and then the commands that remain.
    pub(super) async fn serve<R>(&mut self, call: impl Future<Output = R>) -> R {
        let mut call = pin!(call);
        loop {
            select! {
                biased;
                result = &mut call => {
                    self.drain().await;
                    return result;
                }
                Some(command) = self.commands.recv() => self.process(command).await,
            }
        }
    }

    /// Serves the commands that wait, such as the ends that tickets sent.
    pub(super) async fn drain(&mut self) {
        while let Ok(command) = self.commands.try_recv() {
            self.process(command).await;
        }
    }

    async fn process(&mut self, command: Command<Error>) {
        let tickets = self.scheduler.command_tx.downgrade();
        process_command(&mut self.state, &mut self.timers, &tickets, command).await;
    }
}

/// Retires an old attempt through `admit` and then twice more. Checks that
/// the key row's replacement, and nothing else, stays in both indexes and in
/// memory, and that retirement reports no cancel.
///
/// `mode` picks which old rows exist: the slab row of the old attempt, the
/// key row of a replacement with the next tag, or both.
pub(crate) async fn retirement_trace<F, Fut>(
    mode: u8,
    tag: i32,
    retire_replacement: bool,
    admit: F,
) -> Result<()>
where
    F: FnOnce(TimerManager<TestStore>, Trigger) -> Fut,
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
    let replaced = !mode.is_multiple_of(3);
    if replaced {
        store
            .operations()
            .upsert_key_trigger(replacement.clone())
            .await?;
    }

    let mut state = ActorState::start(store.clone(), segment.into(), CompactDateTime::MIN);
    state.preload_window = CompactDuration::new(60);
    let mut actor = Actor::new(state);
    load_step(&mut actor.state, &mut actor.timers).await;
    ensure!(
        actor
            .state
            .highest_loaded_slab_id
            .is_some_and(|end| end >= slab.id())
    );

    let telemetry = Telemetry::new();
    let mut events = telemetry.subscribe();
    let config = TimerManagerConfig {
        store: store.clone(),
        telemetry: telemetry.partition_sender(Topic::from("admission"), 0),
        source: Arc::from("admission"),
    };
    let manager = TimerManager::assemble(config, actor.scheduler());
    let event = TimerEventRef::new(old.timer_type, old.time, old.tag);
    actor
        .serve(async {
            admit(manager.clone(), old.clone()).await?;
            manager.retire_committed(&old.key, event).await?;
            Ok::<_, color_eyre::Report>(manager.retire_committed(&old.key, event).await?)
        })
        .await?;

    ensure!(!cancelled(&mut events), "retirement reported a cancel");
    let expected = (replaced && !retire_replacement).then_some(replacement.tag);
    let current = store
        .current_trigger(&old.key, old.time, old.timer_type)
        .await?;
    ensure!(
        current.map(|trigger| trigger.tag) == expected,
        "wrong key row"
    );
    let rows: Vec<_> = store
        .get_slab_triggers_all_types(slab.id())
        .map_ok(|trigger| trigger.tag)
        .try_collect()
        .await?;
    ensure!(rows == Vec::from_iter(expected), "wrong slab rows");
    if expected.is_some() {
        let slabs: Vec<_> = store
            .get_slab_range(slab.id()..=slab.id())
            .try_collect()
            .await?;
        ensure!(slabs.len() == 1, "replacement lost slab metadata");
    }

    // The attempt that stays fires once, and memory holds nothing else.
    let fired = actor.timers.next().await;
    ensure!(
        fired.as_ref().map(|trigger| trigger.tag) == expected,
        "memory holds the wrong attempt"
    );
    if let Some(trigger) = fired {
        ensure!(
            actor.timers.fire(&trigger),
            "the stored attempt did not fire"
        );
    }
    ensure!(
        actor.timers.next().await.is_none(),
        "an attempt fired twice"
    );
    ensure!(
        actor.timers.snapshot(time).active == u32::from(expected.is_some()),
        "memory keeps an entry for a deleted row"
    );
    Ok(())
}

/// Whether `events` holds a timer cancel.
fn cancelled(events: &mut broadcast::Receiver<TelemetryEvent>) -> bool {
    while let Ok(event) = events.try_recv() {
        if let Data::Timer(TimerTelemetryEvent {
            event_type: TimerEventType::Cancelled,
            ..
        }) = &*event.data
        {
            return true;
        }
    }
    false
}
