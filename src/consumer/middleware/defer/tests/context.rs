//! Capturing context for trace-based property testing.
//!
//! Provides [`CapturingContext`] that implements [`EventContext`] and records
//! all timer operations for verification. Used to verify timer coverage,
//! cleanup, and backoff timing invariants.

use super::types::OutputEvent;
use crate::Key;
use crate::consumer::event_context::EventContext;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use ahash::HashMap;
use futures::stream::{self, Stream};
use parking_lot::Mutex;
use std::convert::Infallible;
use std::future::{self, Future};
use std::mem;
use std::sync::Arc;

// ============================================================================
// Timer Capture State (shared across all contexts)
// ============================================================================

/// Shared state for capturing timer operations across all keys.
///
/// This is shared by all [`KeyedCapturingContext`] instances, allowing
/// the test harness to query timer state for any key.
#[derive(Clone, Default)]
pub struct TimerCapture {
    /// Recorded operations in order (for debugging/verification).
    events: Arc<Mutex<Vec<OutputEvent>>>,
    /// Currently active timers per key.
    active_timers: Arc<Mutex<HashMap<Key, CompactDateTime>>>,
}

impl TimerCapture {
    /// Creates a new empty timer capture.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a timer schedule operation.
    pub fn record_schedule(&self, key: Key, time: CompactDateTime) {
        let mut events = self.events.lock();
        events.push(OutputEvent::Scheduled {
            key: key.clone(),
            time,
        });

        let mut timers = self.active_timers.lock();
        timers.insert(key, time);
    }

    /// Records a timer clear operation.
    pub fn record_clear(&self, key: &Key) {
        let mut events = self.events.lock();
        events.push(OutputEvent::Cleared { key: key.clone() });

        let mut timers = self.active_timers.lock();
        timers.remove(key);
    }

    /// Pops and returns the oldest recorded event, if any.
    #[must_use]
    pub fn pop_event(&self) -> Option<OutputEvent> {
        let mut events = self.events.lock();
        if events.is_empty() {
            None
        } else {
            Some(events.remove(0))
        }
    }

    /// Returns all recorded events (draining the queue).
    #[must_use]
    pub fn drain_events(&self) -> Vec<OutputEvent> {
        let mut events = self.events.lock();
        mem::take(&mut *events)
    }

    /// Returns the number of pending events.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.events.lock().len()
    }

    /// Returns true if there is an active timer for the given key.
    #[must_use]
    pub fn has_active_timer(&self, key: &Key) -> bool {
        self.active_timers.lock().contains_key(key)
    }

    /// Returns the scheduled time for the key's active timer, if any.
    #[must_use]
    pub fn get_timer_time(&self, key: &Key) -> Option<CompactDateTime> {
        self.active_timers.lock().get(key).copied()
    }

    /// Returns the number of active timers.
    #[must_use]
    pub fn active_timer_count(&self) -> usize {
        self.active_timers.lock().len()
    }
}

// ============================================================================
// Keyed Capturing Context (per-key EventContext implementation)
// ============================================================================

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

impl EventContext for KeyedCapturingContext {
    type Error = Infallible;

    fn should_shutdown(&self) -> bool {
        false
    }

    fn should_cancel(&self) -> bool {
        false
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }

    fn on_cancel(&self) -> impl Future<Output = ()> + Send + 'static {
        future::pending::<()>()
    }

    fn schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            self.capture.record_schedule(self.key.clone(), time);
        }
        future::ready(Ok(()))
    }

    fn clear_and_schedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            // Clear first, then schedule
            self.capture.record_clear(&self.key);
            self.capture.record_schedule(self.key.clone(), time);
        }
        future::ready(Ok(()))
    }

    fn unschedule(
        &self,
        _time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            self.capture.record_clear(&self.key);
        }
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            self.capture.record_clear(&self.key);
        }
        future::ready(Ok(()))
    }

    fn invalidate(self) {
        // No-op for tests
    }

    fn scheduled(
        &self,
        _timer_type: TimerType,
    ) -> impl Stream<Item = Result<CompactDateTime, Self::Error>> + Send + 'static {
        stream::empty()
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
    fn timer_capture_records_schedule() {
        init_test_logging();

        let capture = TimerCapture::new();
        let time = CompactDateTime::now().ok();

        if let Some(time) = time {
            capture.record_schedule(test_key("k1"), time);

            assert!(capture.has_active_timer(&test_key("k1")));
            assert_eq!(capture.get_timer_time(&test_key("k1")), Some(time));
            assert_eq!(capture.active_timer_count(), 1);

            let event = capture.pop_event();
            assert!(matches!(
                event,
                Some(OutputEvent::Scheduled { key, time: t }) if key == test_key("k1") && t == time
            ));
        }
    }

    #[test]
    fn timer_capture_records_clear() {
        init_test_logging();

        let capture = TimerCapture::new();
        let time = CompactDateTime::now().ok();

        if let Some(time) = time {
            capture.record_schedule(test_key("k1"), time);
            capture.record_clear(&test_key("k1"));

            assert!(!capture.has_active_timer(&test_key("k1")));
            assert_eq!(capture.active_timer_count(), 0);

            let events = capture.drain_events();
            assert_eq!(events.len(), 2);
            assert!(matches!(&events[0], OutputEvent::Scheduled { .. }));
            assert!(matches!(&events[1], OutputEvent::Cleared { .. }));
        }
    }

    #[test]
    fn keyed_context_schedule_records_to_capture() {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now().ok();
        if let Some(time) = time {
            TEST_RUNTIME.block_on(async {
                let _ = ctx.schedule(time, TimerType::DeferRetry).await;
            });

            assert!(capture.has_active_timer(&test_key("k1")));
        }
    }

    #[test]
    fn keyed_context_clear_and_schedule_records_both() {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now().ok();
        if let Some(time) = time {
            // Pre-schedule to have something to clear
            capture.record_schedule(test_key("k1"), time);

            TEST_RUNTIME.block_on(async {
                let _ = ctx.clear_and_schedule(time, TimerType::DeferRetry).await;
            });

            // Should have 3 events: initial schedule, clear, new schedule
            assert_eq!(capture.event_count(), 3);
            assert!(capture.has_active_timer(&test_key("k1")));
        }
    }

    #[test]
    fn keyed_context_ignores_application_timers() {
        init_test_logging();

        let capture = TimerCapture::new();
        let ctx = KeyedCapturingContext::new(test_key("k1"), capture.clone());

        let time = CompactDateTime::now().ok();
        if let Some(time) = time {
            TEST_RUNTIME.block_on(async {
                let _ = ctx.schedule(time, TimerType::Application).await;
            });

            // Application timers should not be captured
            assert!(!capture.has_active_timer(&test_key("k1")));
            assert_eq!(capture.event_count(), 0);
        }
    }
}
