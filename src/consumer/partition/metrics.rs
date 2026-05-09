//! Per-partition timer-metrics task.
//!
//! Owns the `OTel` gauges for the four timer-lag metrics. Exits on
//! `ShutdownPhase::Terminating`, recording 0 first to avoid stale-value
//! contributions to KEDA `sum` aggregations across pods/partitions.

use crate::consumer::partition::ShutdownPhase;
use crate::timers::store::TriggerStore;
use crate::timers::{TimerManager, TimerSnapshot};
use crate::{Partition, Topic};
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Gauge;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;

const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

pub async fn run<T: TriggerStore>(
    timer_manager: TimerManager<T>,
    group_id: Arc<str>,
    topic: Topic,
    partition: Partition,
    mut shutdown_rx: watch::Receiver<ShutdownPhase>,
) {
    let meter = meter("prosody");
    let attrs = [
        KeyValue::new("group_id", group_id.to_string()),
        KeyValue::new("topic", topic.to_string()),
        KeyValue::new("partition", i64::from(partition)),
    ];

    let g_active: Gauge<u64> = meter
        .u64_gauge("prosody.timers.active")
        .with_description("Incomplete timer work in the loaded scheduling window (any state)")
        .with_unit("{timer}")
        .build();
    let g_in_flight: Gauge<u64> = meter
        .u64_gauge("prosody.timers.in_flight")
        .with_description("Timers with handlers currently running (Firing | FiringRescheduled)")
        .with_unit("{timer}")
        .build();
    let g_overdue: Gauge<u64> = meter
        .u64_gauge("prosody.timers.overdue")
        .with_description("Timers past their fire time (any state); steady-state floor ≈ in_flight")
        .with_unit("{timer}")
        .build();
    let g_oldest: Gauge<u64> = meter
        .u64_gauge("prosody.timers.oldest_overdue_seconds")
        .with_description("Age of the oldest overdue timer in seconds; 0 when none")
        .with_unit("s")
        .build();

    let record = |s: TimerSnapshot| {
        g_active.record(u64::from(s.active), &attrs);
        g_in_flight.record(u64::from(s.in_flight), &attrs);
        g_overdue.record(u64::from(s.overdue), &attrs);
        g_oldest.record(u64::from(s.oldest_overdue_secs), &attrs);
    };

    let mut tick = interval(REFRESH_INTERVAL);
    loop {
        // Await outside select! so the non-Send WaitForFuture is not held
        // across the snapshot().await point.
        tokio::select! {
            _ = tick.tick() => {},
            _ = shutdown_rx.wait_for(|v| *v >= ShutdownPhase::Terminating) => break,
        }
        if let Some(s) = timer_manager.snapshot().await {
            record(s);
        }
    }

    // Zero out before drop. The OTel SDK retains sync Gauge state until
    // MeterProvider shutdown. Without this, a revoked partition keeps reporting
    // its last value and KEDA's `sum(prosody_timers_overdue)` double-counts.
    record(TimerSnapshot::default());
}
