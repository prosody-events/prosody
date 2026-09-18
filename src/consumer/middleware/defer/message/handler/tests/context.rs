//! Records timer operations and injects timer faults.

use super::FaultKind;
use super::types::OutputEvent;
use crate::Key;
use crate::consumer::TerminationSignals;
use crate::consumer::event_context::EventContext;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::middleware::RepinProof;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{Registered, StateDescriptor};
use crate::state::tests::support::UnavailableState;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use ahash::RandomState;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::future::{self, Future, ready};
use std::sync::Arc;
use thiserror::Error;

// Timer Capture State (shared across all contexts)

/// Selects the timer call that receives a fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerOp {
    /// Adds a timer.
    Schedule,
    /// Replaces the key timers.
    ClearAndSchedule,
    /// Removes the key timers.
    ClearScheduled,
    /// Reads the key timers.
    Scheduled,
}

/// Shared state for capturing timer operations across all keys.
///
/// This is shared by all [`KeyedCapturingContext`] instances, allowing
/// the test harness to query timer state for any key.
///
/// Timers are tracked per key with a set of scheduled times, allowing
/// multiple timers per key and precise removal of specific timers.
#[derive(Clone)]
pub struct TimerCapture {
    /// Recorded operations in order (for debugging/verification).
    events: Arc<scc::Queue<OutputEvent>>,
    next_fault: Arc<Mutex<Option<(TimerOp, FaultKind)>>>,
    /// Currently active timers: key -> set of scheduled times.
    active_timers: Arc<scc::HashMap<Key, BTreeSet<CompactDateTime>, RandomState>>,
}

impl Default for TimerCapture {
    fn default() -> Self {
        Self {
            events: Arc::new(scc::Queue::default()),
            next_fault: Arc::default(),
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

    /// Arms one timer fault for the next dispatch.
    pub fn set_fault(&self, fault: Option<(TimerOp, FaultKind)>) {
        *self.next_fault.lock() = fault;
    }

    /// Reports whether a fault still awaits its selected call.
    pub fn fault_pending(&self) -> bool {
        self.next_fault.lock().is_some()
    }

    fn check(&self, op: TimerOp) -> Result<(), TimerError> {
        let mut slot = self.next_fault.lock();
        if let Some((target, kind)) = *slot
            && target == op
        {
            *slot = None;
            return Err(TimerError(kind));
        }
        Ok(())
    }

    /// Removes active timers without a handler operation.
    pub fn drop_timers(&self, key: &Key) {
        let _ = self.active_timers.remove_sync(key);
    }

    /// Records a timer schedule operation for a specific (key, time).
    pub fn record_schedule(&self, key: Key, time: CompactDateTime) {
        self.events.push(OutputEvent::Scheduled {
            key: key.clone(),
            time,
        });

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

    /// Records clearing a specific timer by (key, time).
    ///
    /// Used when `commit()` is called after a timer fires - removes only the
    /// specific timer that was fired, not any newly scheduled timers.
    pub fn record_clear(&self, key: &Key, time: CompactDateTime) {
        self.events.push(OutputEvent::Cleared { key: key.clone() });

        // Check if this is the only timer for the key
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

    /// Records clearing all timers for a key (any time).
    ///
    /// Used by `clear_scheduled` and the clear part of `clear_and_schedule`.
    pub fn record_clear_all(&self, key: &Key) {
        self.events.push(OutputEvent::Cleared { key: key.clone() });

        let _ = self.active_timers.remove_sync(key);
    }

    /// Detaches the fired source without a handler operation.
    pub fn take_timer(&self, key: &Key, time: CompactDateTime) {
        let _ = self.active_timers.remove_if_sync(key, |times| {
            times.remove(&time);
            times.is_empty()
        });
    }

    /// Pops and returns the oldest recorded event, if any.
    #[must_use]
    pub fn pop_event(&self) -> Option<OutputEvent> {
        self.events.pop().map(|entry| (**entry).clone())
    }

    /// Returns all recorded events (draining the queue).
    #[must_use]
    pub fn drain_events(&self) -> Vec<OutputEvent> {
        let mut result = Vec::with_capacity(self.events.len());
        while let Some(entry) = self.events.pop() {
            result.push((**entry).clone());
        }
        result
    }

    /// Returns the number of pending events.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.len()
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

    /// Returns the number of keys with active timers.
    ///
    /// Note: This returns the number of keys, not the total count of timer
    /// instances. Each key may have multiple scheduled times in its
    /// `BTreeSet`.
    #[must_use]
    pub fn active_timer_count(&self) -> usize {
        self.active_timers.len()
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

// Keyed Capturing Context (per-key EventContext implementation)

/// Context for a specific key that captures timer operations.
///
/// This implements [`EventContext`] and records all timer operations to
/// the shared [`TimerCapture`] state.
///
/// # Usage
///
/// ```ignore
/// let capture = TimerCapture::new();
/// let context = KeyedCapturingContext::new(key.clone(), capture.clone());
///
/// // Pass context to handler
/// handler.on_message(context, message, demand_type).await?;
///
/// // Verify timer was scheduled
/// assert!(capture.has_active_timer(&key));
/// ```
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
        false
    }

    fn is_message_cancelled(&self) -> bool {
        false
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        ready(())
    }

    fn on_message_cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        ready(())
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
        if let Err(error) = self.capture.check(TimerOp::Schedule) {
            return ready(Err(error));
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
        if let Err(error) = self.capture.check(TimerOp::ClearAndSchedule) {
            return ready(Err(error));
        }
        if timer_type == TimerType::DeferredMessage {
            // Clear all timers for this key first, then schedule new one
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
        if timer_type == TimerType::DeferredMessage {
            // Remove specific timer by (key, time)
            self.capture.record_clear(&self.key, time);
        }
        ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if let Err(error) = self.capture.check(TimerOp::ClearScheduled) {
            return ready(Err(error));
        }
        if timer_type == TimerType::DeferredMessage {
            // Remove all timers for this key
            self.capture.record_clear_all(&self.key);
        }
        ready(Ok(()))
    }

    fn cancel(&self) {
        // No-op for tests
    }

    fn uncancel(&self) {
        // No-op for tests
    }

    fn scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<Vec<CompactDateTime>, Self::Error>> + Send + 'static {
        if let Err(error) = self.capture.check(TimerOp::Scheduled) {
            return ready(Err(error));
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

/// Reports a timer fault before the capture changes.
#[derive(Debug, Error)]
#[error("injected timer failure: {0:?}")]
pub struct TimerError(FaultKind);

impl ClassifyError for TimerError {
    fn classify_error(&self) -> ErrorCategory {
        match self.0 {
            FaultKind::Transient => ErrorCategory::Transient,
            FaultKind::Permanent => ErrorCategory::Permanent,
        }
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

    #[test]
    fn timer_capture_records_schedule() -> color_eyre::Result<()> {
        init_test_logging();

        let capture = TimerCapture::new();
        let time = CompactDateTime::now()?;

        capture.record_schedule(test_key("k1"), time);

        assert!(capture.has_active_timer(&test_key("k1")));
        assert_eq!(capture.get_timer_time(&test_key("k1")), Some(time));
        assert_eq!(capture.active_timer_count(), 1);

        let event = capture.pop_event();
        assert!(matches!(
            event,
            Some(OutputEvent::Scheduled { key, time: t }) if key == test_key("k1") && t == time
        ));
        Ok(())
    }

    #[test]
    fn timer_capture_records_clear() -> color_eyre::Result<()> {
        init_test_logging();

        let capture = TimerCapture::new();
        let time = CompactDateTime::now()?;

        capture.record_schedule(test_key("k1"), time);
        capture.record_clear(&test_key("k1"), time);

        assert!(!capture.has_active_timer(&test_key("k1")));
        assert_eq!(capture.active_timer_count(), 0);

        let events = capture.drain_events();
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], OutputEvent::Scheduled { .. }));
        assert!(matches!(&events[1], OutputEvent::Cleared { .. }));
        Ok(())
    }

    #[test]
    fn keyed_context_schedule_records_to_capture() -> color_eyre::Result<()> {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now()?;
        TEST_RUNTIME.block_on(async {
            ctx.schedule(time, TimerType::DeferredMessage).await?;
            Ok::<_, color_eyre::Report>(())
        })?;

        assert!(capture.has_active_timer(&test_key("k1")));
        Ok(())
    }

    #[test]
    fn keyed_context_clear_and_schedule_records_both() -> color_eyre::Result<()> {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now()?;
        // Pre-schedule to have something to clear
        capture.record_schedule(test_key("k1"), time);

        TEST_RUNTIME.block_on(async {
            ctx.clear_and_schedule(time, TimerType::DeferredMessage)
                .await?;
            Ok::<_, color_eyre::Report>(())
        })?;

        // Should have 3 events: initial schedule, clear, new schedule
        assert_eq!(capture.event_count(), 3);
        assert!(capture.has_active_timer(&test_key("k1")));
        Ok(())
    }

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

        // Application timers should not be captured
        assert!(!capture.has_active_timer(&test_key("k1")));
        assert_eq!(capture.event_count(), 0);
        Ok(())
    }
}
