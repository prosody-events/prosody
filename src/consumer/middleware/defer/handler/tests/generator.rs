//! Trace generator for property-based testing.
//!
//! Generates valid [`Trace`] instances that respect all validity invariants,
//! enabling `QuickCheck` to explore the state space while shrinking to minimal
//! failing cases.

use super::types::{MessageEvent, MessageOutcome, TimerEvent, TimerOutcome, Trace, TraceEvent};
use super::{TEST_BASE_BACKOFF_SECS, TEST_MAX_BACKOFF_SECS};
use crate::Offset;
use crate::timers::duration::CompactDuration;
use ahash::HashMap;
use quickcheck::{Arbitrary, Gen};
use std::collections::VecDeque;

// ============================================================================
// Backoff Calculation & Helper Types
// ============================================================================

/// Calculates expected max backoff for a given retry count.
///
/// Formula: `min(base * 2^retry_count, max_delay)`
/// This is the upper bound - actual backoff will be `rand() * this`.
fn expected_max_backoff(retry_count: u32) -> CompactDuration {
    let multiplier = 1_u32.checked_shl(retry_count).unwrap_or(u32::MAX);
    let exp_backoff = TEST_BASE_BACKOFF_SECS.saturating_mul(multiplier);
    let capped = exp_backoff.min(TEST_MAX_BACKOFF_SECS);
    CompactDuration::new(capped)
}

/// Timer outcome type (without `max_backoff` - calculated during state
/// transition).
#[derive(Clone, Copy)]
enum TimerOutcomeType {
    Success,
    Permanent,
    Transient,
    LoaderPermanent,
    LoaderTransient,
}

// ============================================================================
// Trace Builder (maintains validity during generation)
// ============================================================================

/// Builder for constructing valid traces.
///
/// Tracks state during generation to ensure all validity invariants are
/// maintained:
///
/// 1. `MessageEvent.key_idx < key_count`
/// 2. `TimerEvent.key_idx < key_count`
/// 3. `TimerEvent` only when key is deferred with matching offset at head
/// 4. `MessageOutcome::Queued` only when key is already deferred
/// 5. `MessageOutcome::{Success,Permanent,Transient}` only when key NOT
///    deferred
pub struct TraceBuilder {
    /// Number of distinct keys.
    key_count: usize,
    /// Generated events so far.
    events: Vec<TraceEvent>,
    /// Deferred queues per key: `key_idx` -> queue of offsets (FIFO).
    deferred: HashMap<usize, VecDeque<Offset>>,
    /// Retry count per (`key_idx`, offset).
    retry_counts: HashMap<(usize, Offset), u32>,
    /// Global offset counter for monotonic generation (Kafka offsets are
    /// partition-global).
    next_offset: i64,
}

impl TraceBuilder {
    /// Creates a new trace builder with the given key count.
    pub fn new(key_count: usize) -> Self {
        Self {
            key_count,
            events: Vec::new(),
            deferred: HashMap::default(),
            retry_counts: HashMap::default(),
            next_offset: 0,
        }
    }

    /// Returns true if the key at index is currently deferred.
    fn is_deferred(&self, key_idx: usize) -> bool {
        self.deferred.get(&key_idx).is_some_and(|q| !q.is_empty())
    }

    /// Returns the offset at the head of the deferred queue for the key.
    fn head_offset(&self, key_idx: usize) -> Option<Offset> {
        self.deferred.get(&key_idx).and_then(|q| q.front().copied())
    }

    /// Gets the next monotonic offset (global across all keys, like Kafka
    /// partitions).
    fn get_next_offset(&mut self) -> Offset {
        self.next_offset += 1;
        Offset::from(self.next_offset)
    }

    /// Adds a message event with the given outcome (if valid).
    fn add_message_event(&mut self, g: &mut Gen, key_idx: usize, outcome: MessageOutcome) {
        let offset = self.get_next_offset();

        // Apply state transitions
        match &outcome {
            MessageOutcome::Success
            | MessageOutcome::Permanent
            | MessageOutcome::Transient { defer: false, .. } => {
                // No state change - key was not deferred or error propagates
            }
            MessageOutcome::Transient { defer: true, .. } => {
                // Key becomes deferred
                self.deferred.entry(key_idx).or_default().push_back(offset);
                self.retry_counts.insert((key_idx, offset), 0);
            }
            MessageOutcome::Queued => {
                // Message queued to existing deferred key
                self.deferred.entry(key_idx).or_default().push_back(offset);
                self.retry_counts.insert((key_idx, offset), 0);
            }
        }

        self.events.push(TraceEvent::Message(MessageEvent {
            key_idx,
            offset,
            outcome,
        }));

        // Suppress unused warning - g is used for future randomization
        let _ = g;
    }

    /// Adds a timer event (if valid - key must be deferred).
    ///
    /// For `Transient` outcomes, calculates `max_backoff` based on retry count
    /// AFTER incrementing (since that's what the handler will use for
    /// scheduling).
    fn add_timer_event(
        &mut self,
        g: &mut Gen,
        key_idx: usize,
        outcome_type: TimerOutcomeType,
    ) -> bool {
        let Some(offset) = self.head_offset(key_idx) else {
            return false; // Key not deferred - invalid
        };

        // Apply state transitions and calculate outcome
        let outcome = match outcome_type {
            TimerOutcomeType::Success => {
                // Pop offset from queue
                if let Some(queue) = self.deferred.get_mut(&key_idx) {
                    queue.pop_front();
                }
                self.retry_counts.remove(&(key_idx, offset));
                TimerOutcome::Success
            }
            TimerOutcomeType::Permanent => {
                // Pop offset from queue
                if let Some(queue) = self.deferred.get_mut(&key_idx) {
                    queue.pop_front();
                }
                self.retry_counts.remove(&(key_idx, offset));
                TimerOutcome::Permanent
            }
            TimerOutcomeType::Transient => {
                // Increment retry count first
                let new_retry_count =
                    if let Some(count) = self.retry_counts.get_mut(&(key_idx, offset)) {
                        *count += 1;
                        *count
                    } else {
                        0
                    };
                // Calculate max_backoff for the NEW retry count
                TimerOutcome::Transient {
                    max_backoff: expected_max_backoff(new_retry_count),
                }
            }
            TimerOutcomeType::LoaderPermanent => {
                // Same state transition as Success/Permanent: pop head, clear retry count
                if let Some(queue) = self.deferred.get_mut(&key_idx) {
                    queue.pop_front();
                }
                self.retry_counts.remove(&(key_idx, offset));
                TimerOutcome::LoaderPermanent
            }
            TimerOutcomeType::LoaderTransient => {
                // Keep offset at head, DON'T increment retry_count
                // Use current retry_count for max_backoff calculation
                let current_retry_count = self
                    .retry_counts
                    .get(&(key_idx, offset))
                    .copied()
                    .unwrap_or(0);
                TimerOutcome::LoaderTransient {
                    max_backoff: expected_max_backoff(current_retry_count),
                }
            }
        };

        self.events.push(TraceEvent::Timer(TimerEvent {
            key_idx,
            offset,
            outcome,
        }));

        // Suppress unused warning
        let _ = g;

        true
    }

    /// Adds a valid event based on current state.
    pub fn add_valid_event(&mut self, g: &mut Gen) {
        let key_idx = usize::arbitrary(g) % self.key_count;
        let is_deferred = self.is_deferred(key_idx);

        // Decide event type based on state
        let event_type = u8::arbitrary(g) % 10;

        if event_type < 6 {
            // 60% message events
            let outcome = if is_deferred {
                // Key is deferred - can only queue
                MessageOutcome::Queued
            } else {
                // Key not deferred - can succeed, fail permanently, or fail transiently
                match u8::arbitrary(g) % 4 {
                    0 => MessageOutcome::Success,
                    1 => MessageOutcome::Permanent,
                    2 => MessageOutcome::Transient {
                        // First deferral: retry_count = 0
                        max_backoff: expected_max_backoff(0),
                        defer: true,
                    },
                    _ => MessageOutcome::Transient {
                        max_backoff: expected_max_backoff(0),
                        defer: false,
                    },
                }
            };
            self.add_message_event(g, key_idx, outcome);
        } else if is_deferred {
            // 40% timer events (only if key is deferred)
            // Distribution: ~20% each Success/Permanent/Transient, ~14% each
            // LoaderPermanent/LoaderTransient
            let outcome_type = match u8::arbitrary(g) % 7 {
                0 | 5 => TimerOutcomeType::Success,     // ~28%
                1 => TimerOutcomeType::Permanent,       // ~14%
                2 | 6 => TimerOutcomeType::Transient,   // ~28%
                3 => TimerOutcomeType::LoaderPermanent, // ~14%
                _ => TimerOutcomeType::LoaderTransient, // ~14%
            };
            self.add_timer_event(g, key_idx, outcome_type);
        } else {
            // Key not deferred, can't fire timer - generate message instead
            let outcome = match u8::arbitrary(g) % 3 {
                0 => MessageOutcome::Success,
                1 => MessageOutcome::Permanent,
                _ => MessageOutcome::Transient {
                    max_backoff: expected_max_backoff(0),
                    defer: bool::arbitrary(g),
                },
            };
            self.add_message_event(g, key_idx, outcome);
        }
    }

    /// Builds the final trace.
    pub fn build(self) -> Trace {
        Trace::new(self.events, self.key_count)
    }
}

// ============================================================================
// Arbitrary Implementation
// ============================================================================

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        // 1-5 keys
        let key_count = (usize::arbitrary(g) % 5) + 1;
        let mut builder = TraceBuilder::new(key_count);

        // Generate 5-20 events based on generator size
        let event_count = (usize::arbitrary(g) % (g.size().max(1))) + 5;

        for _ in 0..event_count {
            builder.add_valid_event(g);
        }

        builder.build()
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrink by removing events from the end (preserves validity for prefix)
        // This works because our generator builds valid prefixes
        let events = self.events.clone();
        let key_count = self.key_count;

        Box::new((1..events.len()).rev().filter_map(move |len| {
            let prefix = events[..len].to_vec();
            Trace::from_events_if_valid(prefix, key_count)
        }))
    }
}

impl Trace {
    /// Creates a new trace with the given events and key count.
    #[must_use]
    pub fn new(events: Vec<TraceEvent>, key_count: usize) -> Self {
        Self { events, key_count }
    }

    /// Creates a trace from events if they form a valid sequence.
    ///
    /// Returns `None` if the events violate validity invariants.
    pub fn from_events_if_valid(events: Vec<TraceEvent>, key_count: usize) -> Option<Self> {
        let mut deferred: HashMap<usize, VecDeque<Offset>> = HashMap::default();

        for event in &events {
            match event {
                TraceEvent::Message(msg) => {
                    if msg.key_idx >= key_count {
                        return None;
                    }

                    let is_deferred = deferred.get(&msg.key_idx).is_some_and(|q| !q.is_empty());

                    match &msg.outcome {
                        MessageOutcome::Queued if !is_deferred => return None,
                        MessageOutcome::Success
                        | MessageOutcome::Permanent
                        | MessageOutcome::Transient { .. }
                            if is_deferred =>
                        {
                            return None;
                        }
                        MessageOutcome::Transient { defer: true, .. } | MessageOutcome::Queued => {
                            deferred
                                .entry(msg.key_idx)
                                .or_default()
                                .push_back(msg.offset);
                        }
                        _ => {}
                    }
                }
                TraceEvent::Timer(timer) => {
                    if timer.key_idx >= key_count {
                        return None;
                    }

                    let queue = deferred.get_mut(&timer.key_idx)?;
                    let head = queue.front()?;

                    if *head != timer.offset {
                        return None; // FIFO violation
                    }

                    match &timer.outcome {
                        TimerOutcome::Success
                        | TimerOutcome::Permanent
                        | TimerOutcome::LoaderPermanent => {
                            queue.pop_front();
                        }
                        TimerOutcome::Transient { .. } | TimerOutcome::LoaderTransient { .. } => {
                            // Offset stays at head
                        }
                    }
                }
            }
        }

        Some(Trace::new(events, key_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_builder_creates_valid_traces() {
        let mut g = Gen::new(20);

        for _ in 0_i32..100_i32 {
            let trace = Trace::arbitrary(&mut g);

            // Verify key indices are valid
            for event in &trace.events {
                match event {
                    TraceEvent::Message(msg) => {
                        assert!(msg.key_idx < trace.key_count);
                    }
                    TraceEvent::Timer(timer) => {
                        assert!(timer.key_idx < trace.key_count);
                    }
                }
            }

            // Verify the trace is valid by re-validating
            let validated = Trace::from_events_if_valid(trace.events.clone(), trace.key_count);
            assert!(validated.is_some(), "Generated invalid trace");
        }
    }

    #[test]
    fn trace_shrink_preserves_validity() {
        let mut g = Gen::new(20);

        for _ in 0_i32..50_i32 {
            let trace = Trace::arbitrary(&mut g);

            for shrunk in trace.shrink().take(10) {
                let validated =
                    Trace::from_events_if_valid(shrunk.events.clone(), shrunk.key_count);
                assert!(validated.is_some(), "Shrunk to invalid trace");
            }
        }
    }

    #[test]
    fn from_events_rejects_invalid_message_when_deferred() {
        let events = vec![
            // First message defers key 0
            TraceEvent::Message(MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            }),
            // Second message tries Success (invalid - key is deferred)
            TraceEvent::Message(MessageEvent {
                key_idx: 0,
                offset: Offset::from(2_i64),
                outcome: MessageOutcome::Success,
            }),
        ];

        let result = Trace::from_events_if_valid(events, 1);
        assert!(result.is_none());
    }

    #[test]
    fn from_events_rejects_invalid_queued_when_not_deferred() {
        let events = vec![TraceEvent::Message(MessageEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: MessageOutcome::Queued,
        })];

        let result = Trace::from_events_if_valid(events, 1);
        assert!(result.is_none());
    }

    #[test]
    fn from_events_rejects_timer_when_not_deferred() {
        let events = vec![TraceEvent::Timer(TimerEvent {
            key_idx: 0,
            offset: Offset::from(1_i64),
            outcome: TimerOutcome::Success,
        })];

        let result = Trace::from_events_if_valid(events, 1);
        assert!(result.is_none());
    }

    #[test]
    fn from_events_rejects_timer_with_wrong_offset() {
        let events = vec![
            TraceEvent::Message(MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            }),
            // Timer for wrong offset
            TraceEvent::Timer(TimerEvent {
                key_idx: 0,
                offset: Offset::from(2_i64), // Should be 1
                outcome: TimerOutcome::Success,
            }),
        ];

        let result = Trace::from_events_if_valid(events, 1);
        assert!(result.is_none());
    }

    #[test]
    fn from_events_accepts_valid_sequence() {
        let events = vec![
            // Message defers key 0
            TraceEvent::Message(MessageEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: MessageOutcome::Transient {
                    max_backoff: CompactDuration::new(60),
                    defer: true,
                },
            }),
            // Second message is queued
            TraceEvent::Message(MessageEvent {
                key_idx: 0,
                offset: Offset::from(2_i64),
                outcome: MessageOutcome::Queued,
            }),
            // Timer fires for first message
            TraceEvent::Timer(TimerEvent {
                key_idx: 0,
                offset: Offset::from(1_i64),
                outcome: TimerOutcome::Success,
            }),
            // Timer fires for second message
            TraceEvent::Timer(TimerEvent {
                key_idx: 0,
                offset: Offset::from(2_i64),
                outcome: TimerOutcome::Success,
            }),
        ];

        let result = Trace::from_events_if_valid(events, 1);
        assert!(result.is_some());
    }
}
