//! The fixture that applies each [`Op`] to the actor and to the model.

use super::super::{SLAB_SIZE_SECS, TestStore, fresh_state, start_state};
use super::checks::{assert_past_watermark_pull, check_cleanup_effects};
use super::{ModelActiveState, Op, Stop, TriggerModel, TriggerModels, TriggerSpec, build_universe};
use crate::Key;
use crate::timers::active::{Queued, Row, TimerState};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::queue::TriggerQueue;
use crate::timers::scheduler::actor::load::load_step;
use crate::timers::scheduler::actor::{ActorState, handle_add, handle_hold, owns_slab, release};
use crate::timers::slab::{Slab, SlabId};
use crate::timers::store::memory::memory_store;
use crate::timers::store::{Segment, TriggerStore};
use crate::timers::test_support::test_segment;
use crate::timers::{TimerType, Trigger};
use std::result::Result as StdResult;
use tracing::Span;

/// The actor under test and the model that it must match.
pub(super) struct Fixture {
    pub(super) store: TestStore,
    pub(super) segment: Segment,
    pub(super) state: ActorState<TestStore>,
    pub(super) triggers: TriggerQueue,
    /// Authoritative per-trigger model.
    pub(super) expected: TriggerModels,
    /// Gives each add command a distinct input tag.
    next_tag: i32,
    /// Anchored at construction so all ops within one iteration use a
    /// stable `now_slab_id` regardless of wall-clock drift.
    now_slab: SlabId,
    /// All `(key, time, type)` triples the generator can produce, used
    /// for the universe-iteration check after every op.
    pub(super) universe: Vec<(Key, CompactDateTime, TimerType)>,
}

impl Fixture {
    pub(super) async fn new() -> StdResult<Self, String> {
        let segment = test_segment("scheduler-test", SLAB_SIZE_SECS);
        let store = memory_store(segment.clone());
        store
            .insert_segment()
            .await
            .map_err(|e| format!("insert_segment: {e:?}"))?;

        let now = CompactDateTime::now().map_err(|e| format!("wall-clock now: {e:?}"))?;
        let now_slab = Slab::from_time(segment.slab_size, now).id();

        let universe = build_universe(now_slab);

        let state = fresh_state(store.clone(), segment.clone());
        let triggers = TriggerQueue::new();
        Ok(Self {
            store,
            segment,
            state,
            triggers,
            expected: TriggerModels::default(),
            next_tag: 0,
            now_slab,
            universe,
        })
    }

    pub(super) async fn apply(&mut self, op: Op) -> StdResult<(), String> {
        match op {
            Op::Schedule(spec, stop) => self.apply_schedule(spec, stop).await,
            Op::Unschedule(spec) => self.apply_unschedule(spec).await,
            Op::Fire(spec) => self.apply_fire(spec).await,
            Op::Abort(spec) => self.apply_abort(spec).await,
            Op::LoadStep => {
                load_step(&mut self.state, &mut self.triggers).await;
                self.reconcile_loaded_models();
                Ok(())
            }
            Op::CleanupStep => check_cleanup_effects(self).await,
            Op::Restart { stale_watermark } => self.apply_restart(stale_watermark).await,
        }
    }

    async fn apply_schedule(&mut self, spec: TriggerSpec, stop: Stop) -> StdResult<(), String> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        self.next_tag += 1_i32;
        let trigger = Trigger::with_tag(key.clone(), time, ty, self.next_tag, Span::current());
        let mut model = self.model_for(&key, time, ty);

        let slab_id = Slab::from_time(self.segment.slab_size, time).id();
        let past_unregistered = self
            .state
            .last_persisted_watermark
            .is_some_and(|w| slab_id <= w)
            && !self.state.known_slab_ids.contains(&slab_id);
        let old_watermark = self.state.last_persisted_watermark;

        handle_hold(&mut self.state, &self.triggers, trigger.clone())
            .await
            .map_err(|e| format!("handle_hold: {e:?}"))?;
        if past_unregistered {
            assert_past_watermark_pull(self, &key, time, ty, slab_id, old_watermark).await?;
        }
        if stop == Stop::BeforeWrite {
            release(&self.triggers, &trigger).await;
            return Ok(());
        }

        if stop != Stop::FailedWrite {
            self.store
                .add_trigger(trigger.clone())
                .await
                .map_err(|e| format!("add_trigger: {e:?}"))?;
            model.in_store = true;
        }
        let row = if stop == Stop::Never {
            Row::Written
        } else {
            Row::Unconfirmed
        };
        let in_memory = owns_slab(&self.state, slab_id) || model.active_state.is_some();
        handle_add(
            &self.state,
            &mut self.triggers,
            trigger,
            Queued::Scheduled,
            row,
        )
        .await;
        model.active_state = in_memory.then_some(ModelActiveState::Scheduled(row));

        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_unschedule(&mut self, spec: TriggerSpec) -> StdResult<(), String> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        self.store
            .remove_trigger(&key, time, ty)
            .await
            .map_err(|e| format!("remove_trigger: {e:?}"))?;
        let trigger = Trigger::new(key.clone(), time, ty, Span::current());
        self.triggers.remove(&trigger).await;
        self.set_model(
            key,
            time,
            ty,
            TriggerModel {
                in_store: false,
                active_state: None,
            },
        );
        Ok(())
    }

    async fn apply_fire(&mut self, spec: TriggerSpec) -> StdResult<(), String> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        if !matches!(
            self.triggers
                .active_triggers()
                .get(&key, time, ty)
                .await
                .map(|entry| entry.state),
            Some(TimerState::Scheduled)
        ) {
            return Ok(());
        }

        let trigger = Trigger::new(key.clone(), time, ty, Span::current());
        self.triggers.remove_queue_only(&trigger);
        self.triggers
            .active_triggers()
            .set_state(&key, time, ty, TimerState::Firing)
            .await;
        let mut model = self.model_for(&key, time, ty);
        model.active_state = Some(ModelActiveState::Firing);
        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_abort(&mut self, spec: TriggerSpec) -> StdResult<(), String> {
        let (key, time, ty) = spec.resolve(self.now_slab);
        // Only queued and firing timers are abortable; a queued timer also
        // has a `DelayQueue` entry that needs explicit removal.
        let needs_dequeue = match self
            .triggers
            .active_triggers()
            .get(&key, time, ty)
            .await
            .map(|entry| entry.state)
        {
            Some(TimerState::Scheduled) => true,
            Some(TimerState::Firing) => false,
            Some(TimerState::Aborted | TimerState::FiringRescheduled) | None => return Ok(()),
        };

        self.triggers
            .active_triggers()
            .set_state(&key, time, ty, TimerState::Aborted)
            .await;
        if needs_dequeue {
            let trigger = Trigger::new(key.clone(), time, ty, Span::current());
            self.triggers.remove_queue_only(&trigger);
        }

        let mut model = self.model_for(&key, time, ty);
        model.active_state = Some(ModelActiveState::Aborted);
        self.set_model(key, time, ty, model);
        Ok(())
    }

    async fn apply_restart(&mut self, stale_watermark: bool) -> StdResult<(), String> {
        let now = CompactDateTime::now().map_err(|e| format!("wall-clock now: {e:?}"))?;
        if stale_watermark {
            let old_slab = Slab::from_time(CompactDuration::new(60), now);
            let Ok(()) = self
                .store
                .set_slab_watermark(old_slab.id().checked_sub(1))
                .await;
        }
        let Ok(segment) = self.store.get_segment().await;
        let segment = segment.ok_or("restart: segment missing")?;
        self.state = start_state(self.store.clone(), segment, now);
        self.triggers = TriggerQueue::new();
        for model in self.expected.values_mut() {
            model.active_state = None;
        }
        Ok(())
    }

    /// The slab of the wall clock, as cleanup and actor start compute it.
    pub(super) fn current_slab(&self) -> SlabId {
        match CompactDateTime::now() {
            Ok(t) => Slab::from_time(self.segment.slab_size, t).id(),
            Err(_) => self.now_slab,
        }
    }

    pub(super) fn model_for(
        &self,
        key: &Key,
        time: CompactDateTime,
        ty: TimerType,
    ) -> TriggerModel {
        self.expected
            .get(&(key.clone(), time, ty))
            .copied()
            .unwrap_or_default()
    }

    fn set_model(&mut self, key: Key, time: CompactDateTime, ty: TimerType, model: TriggerModel) {
        let triple = (key, time, ty);
        if model.in_store || model.active_state.is_some() {
            self.expected.insert(triple, model);
        } else {
            self.expected.remove(&triple);
        }
    }

    fn reconcile_loaded_models(&mut self) {
        for (key, time, ty) in &self.universe {
            let slab_id = Slab::from_time(self.segment.slab_size, *time).id();
            if !owns_slab(&self.state, slab_id) {
                continue;
            }
            let triple = (key.clone(), *time, *ty);
            if let Some(model) = self.expected.get_mut(&triple)
                && model.in_store
                && model.active_state.is_none()
            {
                model.active_state = Some(ModelActiveState::Scheduled(Row::Written));
            }
        }
    }
}
