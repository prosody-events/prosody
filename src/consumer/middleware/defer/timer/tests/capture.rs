//! The mock key context for the timer defer tests.
//!
//! [`KeyedMockContext`] records each timer call and injects a fault from its
//! slot. [`TimerCapture`] registers the key contexts and shares one [`Phase`]
//! with each one, so a rule check can read the retry timers a dispatch left.

use super::faults::record_operation;
use super::*;
use crate::consumer::Keyed;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::event_context::TerminationSignals;
use crate::consumer::middleware::RepinProof;
use crate::consumer::middleware::tests::test_support::faults::{
    FaultSlot, Phase, TimerError, TimerOp,
};
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::tests::support::UnavailableState;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use std::future::{Future, ready};
use std::sync::Arc;

/// Holds the key contexts whose retry timers the rule checks.
#[derive(Clone, Default)]
pub struct TimerCapture {
    phase: Arc<Phase>,
    contexts: Arc<Mutex<Vec<KeyedMockContext>>>,
}

impl TimerCapture {
    /// Registers the key contexts and shares the phase with each one.
    pub(super) fn watch(&self, contexts: &mut [KeyedMockContext]) {
        for context in contexts.iter_mut() {
            context.next_fault = FaultSlot::sharing(&self.phase);
        }
        self.contexts.lock().extend_from_slice(contexts);
    }

    /// Returns the phase the store and the contexts share.
    pub(super) fn phase(&self) -> &Arc<Phase> {
        &self.phase
    }

    /// Reports whether the key keeps a retry timer other than `fired`. An
    /// unregistered key has none.
    pub(super) fn fires_again(&self, key: &Key, fired: Option<CompactDateTime>) -> bool {
        self.contexts
            .lock()
            .iter()
            .find(|context| context.key() == key)
            .is_some_and(|context| {
                context
                    .active_deferred_timers()
                    .iter()
                    .any(|time| Some(*time) != fired)
            })
    }
}

/// Mock context with `Keyed` trait for testing `TimerDeferContext`.
#[derive(Clone)]
pub struct KeyedMockContext {
    pub(super) inner: MockContext,
    pub(super) next_fault: FaultSlot<TimerOp>,
    pub(super) key: Key,
    /// Tracks timer times by type for verification.
    pub(super) active_timers: Arc<Mutex<Vec<(CompactDateTime, TimerType)>>>,
}

impl KeyedMockContext {
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            inner: MockContext::new(),
            next_fault: FaultSlot::default(),
            key: Arc::from(key),
            active_timers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns all active Application timers for this context.
    #[must_use]
    pub fn active_application_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::Application)
            .map(|(time, _)| *time)
            .collect()
    }

    /// Records that the retry timer at this time fires again.
    fn rearm(&self, time: CompactDateTime, timer_type: TimerType) {
        if timer_type == TimerType::DeferredTimer {
            self.next_fault.phase().rearm(time);
        }
    }

    /// Returns all active `DeferredTimer` timers for this context.
    #[must_use]
    pub fn active_deferred_timers(&self) -> Vec<CompactDateTime> {
        self.active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == TimerType::DeferredTimer)
            .map(|(time, _)| *time)
            .collect()
    }
}

impl Keyed for KeyedMockContext {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.key
    }
}

impl TerminationSignals for KeyedMockContext {
    fn is_shutdown(&self) -> bool {
        self.next_fault.phase().is_revoked()
    }

    fn is_message_cancelled(&self) -> bool {
        self.inner.is_message_cancelled()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_shutdown()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_message_cancelled()
    }
}

impl EventContext for KeyedMockContext {
    type Error = TimerError;
    type Payload = serde_json::Value;
    type State = UnavailableState<serde_json::Value>;

    fn state<DESC>(
        &self,
        registered: Registered<DESC>,
    ) -> Result<DESC::Handle<Self::State>, StateAccessError>
    where
        DESC: StateDescriptor,
    {
        registered.descriptor().bind(&UnavailableState::new())
    }

    fn redispatch(&self, proof: RepinProof) -> Self {
        Self {
            inner: self.inner.redispatch(proof),
            next_fault: self.next_fault.clone(),
            key: self.key.clone(),
            active_timers: self.active_timers.clone(),
        }
    }

    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.on_cancel()
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn uncancel(&self) {
        self.inner.uncancel();
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.next_fault.check_timer(TimerOp::Schedule) {
            return ready(Err(TimerError(error)));
        }
        self.rearm(time, timer_type);
        self.active_timers.lock().push((time, timer_type));
        record_operation(self, TimerOperation::Schedule(time, timer_type))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.next_fault.check_timer(TimerOp::ClearAndSchedule) {
            return ready(Err(TimerError(error)));
        }
        self.rearm(time, timer_type);
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        self.active_timers.lock().push((time, timer_type));
        record_operation(self, TimerOperation::ClearAndSchedule(time, timer_type))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.active_timers
            .lock()
            .retain(|(t, tt)| !(*t == time && *tt == timer_type));
        ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.next_fault.check_timer(TimerOp::ClearScheduled) {
            return ready(Err(TimerError(error)));
        }
        self.active_timers.lock().retain(|(_, t)| *t != timer_type);
        record_operation(self, TimerOperation::ClearScheduled(timer_type))
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        if let Some(error) = self.next_fault.check_timer(TimerOp::Scheduled) {
            return ready(Err(TimerError(error)));
        }
        let times: Vec<CompactDateTime> = self
            .active_timers
            .lock()
            .iter()
            .filter(|(_, t)| *t == timer_type)
            .map(|(time, _)| *time)
            .collect();
        ready(Ok(times))
    }
}
