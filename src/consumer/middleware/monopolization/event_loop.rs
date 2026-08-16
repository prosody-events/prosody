use std::time::Duration;

use interval::interval_set::ToIntervalSet;
use interval::prelude::{Intersection, Union};
use quanta::Instant;
use tokio::sync::broadcast;
use tracing::{debug, trace, warn};

use super::KeyIntervals;
use crate::TopicPartitionKey;
use crate::telemetry::event::{Data, KeyEvent, KeyState, TelemetryEvent};

pub(super) async fn run_event_loop(
    reference_instant: Instant,
    key_intervals: KeyIntervals,
    window_duration: Duration,
    mut telemetry_rx: broadcast::Receiver<TelemetryEvent>,
) {
    let window_nanos = window_duration.as_nanos() as u64;
    debug!("monopolization event loop started");

    loop {
        let event = match telemetry_rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(
                    skipped_events = skipped,
                    "telemetry lag can make key intervals inaccurate"
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
        let Data::Key(KeyEvent { key, state, .. }) = &*event.data else {
            continue;
        };
        let tp_key = TopicPartitionKey::new(event.topic, event.partition, key.clone());
        let elapsed_nanos = event
            .timestamp
            .saturating_duration_since(reference_instant)
            .as_nanos() as u64;

        match *state {
            KeyState::HandlerInvoked => {
                const MAX_NANOS: u64 = u64::MAX - 1;
                let open = [(elapsed_nanos, MAX_NANOS)].to_interval_set();
                let intervals = key_intervals
                    .get(&tp_key)
                    .map_or_else(|| open.clone(), |current| current.union(&open));
                key_intervals.insert(tp_key.clone(), intervals);
                trace!(
                    topic = %tp_key.topic,
                    partition = tp_key.partition,
                    key = %tp_key.key,
                    "handler interval opened"
                );
            }
            KeyState::HandlerSucceeded | KeyState::HandlerFailed => {
                let window_start = elapsed_nanos.saturating_sub(window_nanos);
                let window = [(window_start, elapsed_nanos)].to_interval_set();
                if let Some(intervals) = key_intervals.get(&tp_key) {
                    key_intervals.insert(tp_key.clone(), intervals.intersection(&window));
                    trace!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        ?state,
                        "handler interval closed"
                    );
                } else {
                    debug!(
                        topic = %tp_key.topic,
                        partition = tp_key.partition,
                        key = %tp_key.key,
                        ?state,
                        "handler interval was not open"
                    );
                }
            }
        }
    }
    debug!("monopolization event loop stopped");
}
