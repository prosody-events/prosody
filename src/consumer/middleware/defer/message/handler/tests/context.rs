//! Records timer operations for trace properties.
//! Tracks active timers by key and time.

use crate::Key;
use crate::consumer::TerminationSignals;
use crate::consumer::event_context::EventContext;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::middleware::RepinProof;
use crate::consumer::middleware::tests::test_support::faults::{FaultSlot, TimerError, TimerOp};
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::tests::support::UnavailableState;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use ahash::RandomState;
use std::collections::BTreeSet;
use std::future::{self, Future, ready};
use std::sync::Arc;

/// Shares the active timer times of every trace key.
#[derive(Clone)]
pub struct TimerCapture {
    pub(super) next_fault: FaultSlot<TimerOp>,
    /// Currently active timers: key -> set of scheduled times.
    active_timers: Arc<scc::HashMap<Key, BTreeSet<CompactDateTime>, RandomState>>,
}

impl Default for TimerCapture {
    fn default() -> Self {
        Self {
            next_fault: FaultSlot::default(),
            active_timers: Arc::new(scc::HashMap::with_hasher(RandomState::new())),
        }
    }
}

impl TimerCapture {
    /// Creates a new empty timer capture.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Removes active timers without a handler operation.
    pub fn drop_timers(&self, key: &Key) {
        let _ = self.active_timers.remove_sync(key);
    }

    /// Adds one timer for the key. The key fires again at this time.
    pub fn record_schedule(&self, key: Key, time: CompactDateTime) {
        self.next_fault.phase().rearm(time);

        let _ = self
            .active_timers
            .entry_sync(key)
            .and_modify(|times| {
                times.insert(time);
            })
            .or_insert_with(|| {
                let mut set = BTreeSet::new();
                set.insert(time);
                set
            });
    }

    /// Removes one timer of the key.
    pub fn record_clear(&self, key: &Key, time: CompactDateTime) {
        let should_remove = self
            .active_timers
            .read_sync(key, |_, times| times.len() == 1 && times.contains(&time))
            .unwrap_or(false);

        if should_remove {
            let _ = self.active_timers.remove_sync(key);
        } else if let Some(mut entry) = self.active_timers.get_sync(key) {
            entry.get_mut().remove(&time);
        }
    }

    /// Removes every timer of the key.
    pub fn record_clear_all(&self, key: &Key) {
        let _ = self.active_timers.remove_sync(key);
    }

    /// Returns true if there is an active timer for the given key (any time).
    #[must_use]
    pub fn has_active_timer(&self, key: &Key) -> bool {
        self.active_timers
            .read_sync(key, |_, times| !times.is_empty())
            .unwrap_or(false)
    }

    /// Returns the earliest scheduled time for the key's active timers, if any.
    #[must_use]
    pub fn get_timer_time(&self, key: &Key) -> Option<CompactDateTime> {
        self.active_timers
            .read_sync(key, |_, times| times.first().copied())
            .flatten()
    }

    /// Reports whether the key keeps a retry timer other than `fired`. A key
    /// with no recorded timer has none.
    pub(super) fn fires_again(&self, key: &Key, fired: Option<CompactDateTime>) -> bool {
        self.active_timers
            .read_sync(key, |_, times| {
                times.iter().any(|time| Some(*time) != fired)
            })
            .unwrap_or(false)
    }

    /// Returns the number of scheduled timers for one key. Distinguishes a
    /// `clear_and_schedule` singleton (1) from an accumulating `schedule` (>1).
    #[must_use]
    pub fn key_timer_count(&self, key: &Key) -> usize {
        self.active_timers
            .read_sync(key, |_, times| times.len())
            .unwrap_or(0)
    }
}

/// Records timer operations for one key in the shared capture.
#[derive(Clone)]
pub struct KeyedCapturingContext {
    /// The message key this context is scoped to.
    key: Key,
    /// Shared timer capture state.
    capture: TimerCapture,
}

impl KeyedCapturingContext {
    /// Creates a new context for the given key.
    #[must_use]
    pub fn new(key: Key, capture: TimerCapture) -> Self {
        Self { key, capture }
    }
}

impl TerminationSignals for KeyedCapturingContext {
    fn is_shutdown(&self) -> bool {
        self.capture.next_fault.phase().is_revoked()
    }

    fn is_message_cancelled(&self) -> bool {
        false
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }
}

impl EventContext for KeyedCapturingContext {
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

    fn redispatch(&self, _proof: RepinProof) -> Self {
        // Leaf mock over stateless keyed state: nothing to re-pin.
        self.clone()
    }

    fn should_cancel(&self) -> bool {
        false
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.capture.next_fault.check_timer(TimerOp::Schedule) {
            return ready(Err(TimerError(error)));
        }
        if timer_type == TimerType::DeferredMessage {
            self.capture.record_schedule(self.key.clone(), time);
        }
        ready(Ok(()))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self
            .capture
            .next_fault
            .check_timer(TimerOp::ClearAndSchedule)
        {
            return ready(Err(TimerError(error)));
        }
        if timer_type == TimerType::DeferredMessage {
            self.capture.record_clear_all(&self.key);
            self.capture.record_schedule(self.key.clone(), time);
        }
        ready(Ok(()))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.capture.next_fault.check_timer(TimerOp::Unschedule) {
            return ready(Err(TimerError(error)));
        }
        if timer_type == TimerType::DeferredMessage {
            self.capture.record_clear(&self.key, time);
        }
        ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Some(error) = self.capture.next_fault.check_timer(TimerOp::ClearScheduled) {
            return ready(Err(TimerError(error)));
        }
        if timer_type == TimerType::DeferredMessage {
            self.capture.record_clear_all(&self.key);
        }
        ready(Ok(()))
    }

    fn cancel(&self) {}

    fn uncancel(&self) {}

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        if let Some(error) = self.capture.next_fault.check_timer(TimerOp::Scheduled) {
            return ready(Err(TimerError(error)));
        }
        let times = if timer_type == TimerType::DeferredMessage {
            self.capture
                .active_timers
                .read_sync(&self.key, |_, times| times.iter().copied().collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        ready(Ok(times))
    }
}

#[cfg(test)]
mod tests {
    use super::super::TEST_RUNTIME;
    use super::*;
    use crate::tracing::init_test_logging;
    use std::sync::Arc;

    fn test_key(s: &str) -> Key {
        Arc::from(s)
    }

    /// The capture holds `DeferredMessage` timers only, so the coverage
    /// checks never count an application timer.
    #[test]
    fn keyed_context_ignores_application_timers() -> color_eyre::Result<()> {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now()?;
        TEST_RUNTIME.block_on(async {
            ctx.schedule(time, TimerType::Application).await?;
            Ok::<_, color_eyre::Report>(())
        })?;

        assert!(!capture.has_active_timer(&test_key("k1")));
        Ok(())
    }
}
