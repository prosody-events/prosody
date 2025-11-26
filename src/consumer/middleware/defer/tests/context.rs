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
use futures::stream::{self, Stream};
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::future::{self, Future};
use std::sync::Arc;

// ============================================================================
// Timer Capture State (shared across all contexts)
// ============================================================================

/// Shared state for capturing timer operations across all keys.
///
/// This is shared by all [`KeyedCapturingContext`] instances, allowing
/// the test harness to query timer state for any key.
///
/// Timers are tracked per key with a set of scheduled times, allowing
/// multiple timers per key and precise removal of specific timers.
#[derive(Clone, Default)]
pub struct TimerCapture {
    /// Recorded operations in order (for debugging/verification).
    events: Arc<scc::Queue<OutputEvent>>,
    /// Currently active timers: key -> set of scheduled times.
    active_timers: Arc<scc::HashMap<Key, BTreeSet<CompactDateTime>>>,
}

impl TimerCapture {
    /// Creates a new empty timer capture.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
            // Clear all timers for this key first, then schedule new one
            self.capture.record_clear_all(&self.key);
            self.capture.record_schedule(self.key.clone(), time);
        }
        future::ready(Ok(()))
    }

    fn unschedule(
        &self,
        time: CompactDateTime,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            // Remove specific timer by (key, time)
            self.capture.record_clear(&self.key, time);
        }
        future::ready(Ok(()))
    }

    fn clear_scheduled(
        &self,
        timer_type: TimerType,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        if timer_type == TimerType::DeferRetry {
            // Remove all timers for this key
            self.capture.record_clear_all(&self.key);
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
            capture.record_clear(&test_key("k1"), time);

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
