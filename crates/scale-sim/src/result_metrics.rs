use std::time::Duration;

use crate::{MetricPoint, MetricTrace, PlantError, SimulationResult};

impl SimulationResult {
    /// Returns source events in insertion order.
    #[must_use]
    pub fn events(&self) -> &[crate::EventSpec] {
        &self.events
    }

    /// Returns settlements in completion order.
    #[must_use]
    pub fn settlements(&self) -> &[crate::Settlement] {
        &self.settlements
    }

    /// Converts the completed plant run into fixed virtual-time samples.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero window or a trace that exceeds platform
    /// limits.
    pub fn metric_trace(
        &self,
        window_micros: u64,
        budget_micros: u64,
    ) -> Result<MetricTrace, PlantError> {
        let final_micros = self
            .settlements
            .last()
            .map_or(0, |settlement| settlement.settle_micros);
        self.metric_trace_until(window_micros, budget_micros, final_micros)
    }

    pub(crate) fn metric_trace_until(
        &self,
        window_micros: u64,
        budget_micros: u64,
        final_micros: u64,
    ) -> Result<MetricTrace, PlantError> {
        if window_micros == 0 {
            return Err(PlantError::ZeroBound {
                name: "window_micros",
            });
        }
        let point_count = final_micros / window_micros + 1;
        let point_count_u32 = u32::try_from(point_count).map_err(|_| PlantError::PlatformLimit)?;
        let mut trace = MetricTrace::new(point_count_u32)?;
        trace.mark_plant_only();
        let mut samples = WindowSamples::new(self.settlements.len());
        let window_seconds = Duration::from_micros(window_micros).as_secs_f64();
        for point_index in 0..point_count {
            let start = point_index * window_micros;
            let end = start.saturating_add(window_micros);
            let mut point = MetricPoint::zero(end);
            samples.clear();
            for event in &self.events {
                if event.release_micros >= start && event.release_micros < end {
                    point.arrivals += 1;
                    point.timers += u64::from(event.source == crate::EventSource::Timer);
                }
            }
            let mut released = 0_u64;
            let mut completed = 0_u64;
            let mut missed = 0_u64;
            for settlement in &self.settlements {
                if settlement.release_micros < end {
                    released += 1;
                }
                if settlement.settle_micros < end {
                    completed += 1;
                }
                if settlement.settle_micros >= start && settlement.settle_micros < end {
                    point.useful_completions += 1;
                    point.transient_failures += u64::from(settlement.attempts.saturating_sub(1));
                    point.permanent_rejections += u64::from(matches!(
                        settlement.final_outcome,
                        crate::FinalOutcome::PermanentFailure
                    ));
                    let latency = settlement
                        .settle_micros
                        .saturating_sub(settlement.release_micros);
                    missed += u64::from(latency > budget_micros);
                    samples.record(settlement, latency);
                }
            }
            point.backlog = released.saturating_sub(completed);
            point.queue_mean = mean_u32(&samples.queue);
            point.queue_max = u64::from(samples.queue.iter().copied().max().unwrap_or(0));
            point.useful_throughput_per_second =
                count_as_f64(point.useful_completions)? / window_seconds;
            point.replicas = self.replicas_at(end);
            point.replica_seconds = self.replica_seconds_until(end);
            if point.useful_completions > 0 {
                point.miss_fraction =
                    count_as_f64(missed)? / count_as_f64(point.useful_completions)?;
            }
            samples.apply(self, &mut point, start, end, window_seconds);
            trace.push(point)?;
        }
        Ok(trace)
    }

    fn replicas_at(&self, at_micros: u64) -> u32 {
        let mut replicas = self.initial_replicas;
        let mut latest = 0_u64;
        for change in &self.changes {
            if change.at_micros <= at_micros && change.at_micros >= latest {
                latest = change.at_micros;
                replicas = change.replicas;
            }
        }
        replicas
    }

    fn replica_seconds_until(&self, end_micros: u64) -> f64 {
        let mut replicas = self.initial_replicas;
        let mut cursor = 0_u64;
        let mut area = 0.0_f64;
        for change in &self.changes {
            if change.at_micros >= end_micros || change.at_micros < cursor {
                continue;
            }
            area += f64::from(replicas)
                * Duration::from_micros(change.at_micros - cursor).as_secs_f64();
            cursor = change.at_micros;
            replicas = change.replicas;
        }
        area + f64::from(replicas) * Duration::from_micros(end_micros - cursor).as_secs_f64()
    }
}

struct WindowSamples {
    latency: Vec<u64>,
    permit_wait: Vec<u64>,
    handler: Vec<u64>,
    in_flight: Vec<u32>,
    queue: Vec<u32>,
    dependency_service_seconds: f64,
}

impl WindowSamples {
    fn new(capacity: usize) -> Self {
        Self {
            latency: Vec::with_capacity(capacity),
            permit_wait: Vec::with_capacity(capacity),
            handler: Vec::with_capacity(capacity),
            in_flight: Vec::with_capacity(capacity),
            queue: Vec::with_capacity(capacity),
            dependency_service_seconds: 0.0_f64,
        }
    }

    fn clear(&mut self) {
        self.latency.clear();
        self.permit_wait.clear();
        self.handler.clear();
        self.in_flight.clear();
        self.queue.clear();
        self.dependency_service_seconds = 0.0_f64;
    }

    fn record(&mut self, settlement: &crate::Settlement, latency: u64) {
        self.latency.push(latency);
        self.permit_wait.push(settlement.permit_wait_micros);
        self.handler.push(settlement.handler_elapsed_micros());
        self.in_flight.push(settlement.in_flight_at_dispatch);
        self.queue.push(settlement.queue_at_dispatch);
        self.dependency_service_seconds +=
            Duration::from_micros(settlement.dependency_micros).as_secs_f64();
    }

    fn apply(
        &mut self,
        result: &SimulationResult,
        point: &mut MetricPoint,
        start: u64,
        end: u64,
        window_seconds: f64,
    ) {
        self.latency.sort_unstable();
        self.permit_wait.sort_unstable();
        self.handler.sort_unstable();
        self.in_flight.sort_unstable();
        point.latency_p50_micros = percentile(&self.latency, 500);
        point.latency_p90_micros = percentile(&self.latency, 900);
        point.latency_p99_micros = percentile(&self.latency, 990);
        point.latency_p999_micros = percentile(&self.latency, 999);
        point.permit_wait_p99_micros = percentile(&self.permit_wait, 990);
        point.handler_elapsed_p99_micros = percentile(&self.handler, 990);
        point.requests_in_flight_p50 = u64::from(percentile_u32(&self.in_flight, 500));
        point.requests_in_flight_p99 = u64::from(percentile_u32(&self.in_flight, 990));
        self.apply_utilization(result, point, start, end, window_seconds);
    }

    fn apply_utilization(
        &self,
        result: &SimulationResult,
        point: &mut MetricPoint,
        start: u64,
        end: u64,
        window_seconds: f64,
    ) {
        let slot_seconds = (result.replica_seconds_until(end)
            - result.replica_seconds_until(start))
            * f64::from(result.slots_per_replica);
        let handler_seconds = self
            .handler
            .iter()
            .map(|&micros| Duration::from_micros(micros).as_secs_f64())
            .sum::<f64>();
        if slot_seconds > 0.0_f64 {
            point.handler_utilization_mean = (handler_seconds / slot_seconds).min(1.0_f64);
            let maximum_in_flight = self.in_flight.last().copied().unwrap_or(0);
            let available = point.replicas.saturating_mul(result.slots_per_replica);
            if available > 0 {
                point.handler_utilization_max =
                    (f64::from(maximum_in_flight) / f64::from(available)).min(1.0_f64);
            }
        }
        point.handler_utilization_cv = coefficient_of_variation(&self.handler);
        let dependency_slot_seconds = f64::from(result.dependency_slots) * window_seconds;
        if dependency_slot_seconds > 0.0_f64 {
            point.dependency_utilization =
                (self.dependency_service_seconds / dependency_slot_seconds).min(1.0_f64);
        }
    }
}

fn count_as_f64(value: u64) -> Result<f64, PlantError> {
    let value_u32 = u32::try_from(value).map_err(|_| PlantError::PlatformLimit)?;
    Ok(f64::from(value_u32))
}

fn percentile(sorted: &[u64], permille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * permille / 1_000;
    sorted[index]
}

fn percentile_u32(sorted: &[u32], permille: usize) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * permille / 1_000;
    sorted[index]
}

fn mean_u32(values: &[u32]) -> f64 {
    if values.is_empty() {
        return 0.0_f64;
    }
    let sum = values.iter().copied().map(u64::from).sum::<u64>();
    u64_f64(sum) / usize_f64(values.len())
}

fn coefficient_of_variation(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0_f64;
    }
    let mean = values
        .iter()
        .map(|&micros| Duration::from_micros(micros).as_secs_f64())
        .sum::<f64>()
        / usize_f64(values.len());
    if mean <= f64::EPSILON {
        return 0.0_f64;
    }
    let variance = values
        .iter()
        .map(|&micros| {
            let difference = Duration::from_micros(micros).as_secs_f64() - mean;
            difference * difference
        })
        .sum::<f64>()
        / usize_f64(values.len());
    variance.sqrt() / mean
}

fn u64_f64(value: u64) -> f64 {
    let high = (value >> 32_u32) as u32;
    let low = value as u32;
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn usize_f64(value: usize) -> f64 {
    match u64::try_from(value) {
        Ok(value) => u64_f64(value),
        Err(_) => f64::INFINITY,
    }
}
