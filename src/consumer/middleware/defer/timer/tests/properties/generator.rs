//! Generates valid timer traces and tracks their expected queues.

use super::super::types::{ApplicationTimerEvent, ApplicationTimerOutcome, DeferredTimerEvent};
use super::*;
use crate::Key;
use ahash::HashMap;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeSet;
use std::sync::Arc;

// ============================================================================
// Trace Model (for invariant verification)
// ============================================================================

/// Simple model tracking deferred state per key.
#[derive(Debug, Default)]
pub(super) struct TraceModel {
    /// Key -> (sorted times, retry count)
    deferred: HashMap<Key, (BTreeSet<CompactDateTime>, u32)>,
}

impl TraceModel {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn is_deferred(&self, key: &Key) -> bool {
        self.deferred
            .get(key)
            .is_some_and(|(times, _)| !times.is_empty())
    }

    fn defer_first(&mut self, key: Key, time: CompactDateTime) {
        let entry = self
            .deferred
            .entry(key)
            .or_insert_with(|| (BTreeSet::new(), 0));
        entry.0.insert(time);
        entry.1 = 0;
    }

    fn queue_behind(&mut self, key: &Key, time: CompactDateTime) {
        if let Some(entry) = self.deferred.get_mut(key) {
            entry.0.insert(time);
        }
    }

    fn complete_head(&mut self, key: &Key) -> Option<CompactDateTime> {
        let entry = self.deferred.get_mut(key)?;
        let head = *entry.0.first()?;
        entry.0.remove(&head);
        if entry.0.is_empty() {
            self.deferred.remove(key);
            None
        } else {
            entry.1 = 0;
            entry.0.first().copied()
        }
    }

    fn increment_retry(&mut self, key: &Key) {
        if let Some(entry) = self.deferred.get_mut(key) {
            entry.1 = entry.1.saturating_add(1);
        }
    }

    pub(super) fn get_head(&self, key: &Key) -> Option<CompactDateTime> {
        self.deferred
            .get(key)
            .and_then(|(times, _)| times.first().copied())
    }
}

// ============================================================================
// Trace Generator
// ============================================================================

impl Arbitrary for TimerTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let key_count = (usize::arbitrary(g) % 3) + 2; // 2-4 keys
        let event_count = (usize::arbitrary(g) % 15) + 5; // 5-19 events

        let mut model = TraceModel::new();
        let mut events = Vec::with_capacity(event_count);

        for _ in 0..event_count {
            let key_idx = usize::arbitrary(g) % key_count;
            let key = test_key(key_idx);
            let time = CompactDateTime::from(u32::arbitrary(g) % 10000 + 1000);

            if model.is_deferred(&key) {
                generate_deferred_key_event(g, &mut model, &mut events, key_idx, &key, time);
            } else {
                generate_non_deferred_event(g, &mut model, &mut events, key_idx, key, time);
            }
        }

        Self { events, key_count }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrink by removing events from end (preserves validity)
        let events = self.events.clone();
        let key_count = self.key_count;

        Box::new((1..events.len()).rev().map(move |len| TimerTrace {
            events: events[..len].to_vec(),
            key_count,
        }))
    }
}

/// Generates an event for a key that is already deferred.
fn generate_deferred_key_event(
    g: &mut Gen,
    model: &mut TraceModel,
    events: &mut Vec<TimerTraceEvent>,
    key_idx: usize,
    key: &Key,
    time: CompactDateTime,
) {
    // Key is deferred - can either queue or fire retry
    if bool::arbitrary(g) {
        // Fire retry timer
        if let Some(expected_time) = model.get_head(key) {
            let outcome = match u8::arbitrary(g) % 3 {
                0 => DeferredTimerOutcome::Success,
                1 => DeferredTimerOutcome::Permanent,
                _ => DeferredTimerOutcome::Transient,
            };

            // Update model
            match outcome {
                DeferredTimerOutcome::Success | DeferredTimerOutcome::Permanent => {
                    model.complete_head(key);
                }
                DeferredTimerOutcome::Transient => {
                    model.increment_retry(key);
                }
            }

            events.push(TimerTraceEvent::DeferredTimer(DeferredTimerEvent {
                key_idx,
                expected_time,
                outcome,
            }));
        }
    } else {
        // Queue behind existing
        model.queue_behind(key, time);
        events.push(TimerTraceEvent::ApplicationTimer(ApplicationTimerEvent {
            key_idx,
            time,
            outcome: ApplicationTimerOutcome::Queued,
            lost_timer: u8::arbitrary(g).is_multiple_of(3),
        }));
    }
}

/// Generates an event for a key that is not yet deferred.
fn generate_non_deferred_event(
    g: &mut Gen,
    model: &mut TraceModel,
    events: &mut Vec<TimerTraceEvent>,
    key_idx: usize,
    key: Key,
    time: CompactDateTime,
) {
    // Key not deferred - application timer fires
    let outcome = match u8::arbitrary(g) % 4 {
        0 => ApplicationTimerOutcome::Success,
        1 => ApplicationTimerOutcome::Permanent,
        _ => {
            let defer = bool::arbitrary(g);
            if defer {
                model.defer_first(key, time);
            }
            ApplicationTimerOutcome::Transient { defer }
        }
    };

    events.push(TimerTraceEvent::ApplicationTimer(ApplicationTimerEvent {
        key_idx,
        time,
        outcome,
        lost_timer: false,
    }));
}

pub(super) fn test_key(idx: usize) -> Key {
    Arc::from(format!("timer-test-key-{idx}"))
}

pub(super) fn update_model(model: &mut TraceModel, event: &TimerTraceEvent) {
    match event {
        TimerTraceEvent::ApplicationTimer(app_event) => {
            let key = test_key(app_event.key_idx);
            match &app_event.outcome {
                ApplicationTimerOutcome::Success | ApplicationTimerOutcome::Permanent => {
                    // No model change
                }
                ApplicationTimerOutcome::Transient { defer } => {
                    if *defer {
                        model.defer_first(key, app_event.time);
                    }
                }
                ApplicationTimerOutcome::Queued => {
                    model.queue_behind(&key, app_event.time);
                }
            }
        }
        TimerTraceEvent::DeferredTimer(def_event) => {
            let key = test_key(def_event.key_idx);
            match &def_event.outcome {
                DeferredTimerOutcome::Success | DeferredTimerOutcome::Permanent => {
                    model.complete_head(&key);
                }
                DeferredTimerOutcome::Transient => {
                    model.increment_retry(&key);
                }
            }
        }
    }
}
