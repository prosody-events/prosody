use crate::PlantError;

/// One complete laboratory sample.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricPoint {
    /// Virtual sample time.
    pub at_micros: u64,
    /// Offered arrivals in this interval.
    pub arrivals: u64,
    /// Known backlog at interval end.
    pub backlog: u64,
    /// Known timer releases in this interval.
    pub timers: u64,
    /// Useful final completions.
    pub useful_completions: u64,
    /// Transient failures.
    pub transient_failures: u64,
    /// Permanent rejections.
    pub permanent_rejections: u64,
    /// Requests that exceeded the simulation timeout.
    pub timeouts: u64,
    /// Median sojourn in microseconds.
    pub latency_p50_micros: u64,
    /// 90th percentile sojourn in microseconds.
    pub latency_p90_micros: u64,
    /// 99th percentile sojourn in microseconds.
    pub latency_p99_micros: u64,
    /// 99.9th percentile sojourn in microseconds.
    pub latency_p999_micros: u64,
    /// 99th percentile permit wait in microseconds.
    pub permit_wait_p99_micros: u64,
    /// 99th percentile wall-clock handler time in microseconds.
    pub handler_elapsed_p99_micros: u64,
    /// 99th percentile settle time in microseconds.
    pub settle_p99_micros: u64,
    /// Mean queue length.
    pub queue_mean: f64,
    /// Maximum queue length.
    pub queue_max: u64,
    /// Median requests in flight.
    pub requests_in_flight_p50: u64,
    /// 99th percentile requests in flight.
    pub requests_in_flight_p99: u64,
    /// Mean handler utilization.
    pub handler_utilization_mean: f64,
    /// Maximum handler utilization.
    pub handler_utilization_max: f64,
    /// Handler utilization coefficient of variation.
    pub handler_utilization_cv: f64,
    /// Mean dependency utilization.
    pub dependency_utilization: f64,
    /// Useful throughput per second.
    pub useful_throughput_per_second: f64,
    /// Live operation concurrency for resource inference.
    pub resource_concurrency: f64,
    /// Low posterior capacity rate.
    pub capacity_low_per_second: f64,
    /// Median posterior capacity rate.
    pub capacity_median_per_second: f64,
    /// High posterior capacity rate.
    pub capacity_high_per_second: f64,
    /// Probability that a dependency limits the target.
    pub saturation_probability: f64,
    /// Probability that no knee exists in the supported range.
    pub no_knee_probability: f64,
    /// Low posterior arrival prediction.
    pub prediction_low: f64,
    /// Median posterior arrival prediction.
    pub prediction_median: f64,
    /// High posterior arrival prediction.
    pub prediction_high: f64,
    /// Active replica count.
    pub replicas: u32,
    /// Requested replica target.
    pub target: u32,
    /// Saturation cap.
    pub cap: u32,
    /// Whether the controller returned Hold.
    pub hold: bool,
    /// Realized SLO miss fraction.
    pub miss_fraction: f64,
    /// Cumulative replica-seconds.
    pub replica_seconds: f64,
    /// Probe work divided by useful work.
    /// Oldest accepted reporter age.
    pub snapshot_age_micros: u64,
    /// Reporter rows absent from the current view.
    pub missing_reporters: u32,
    /// Cumulative replica changes.
    pub scale_actions: u32,
    /// Time since the last adverse transition.
    pub recovery_micros: u64,
    /// Posterior expected fractional loss.
    pub expected_loss: f64,
    /// Posterior expected one-replica scale-up lead time.
    pub lead_time_up_seconds: f64,
    /// Posterior expected one-replica scale-down lead time.
    pub lead_time_down_seconds: f64,
    /// Posterior expected lead time for the selected or last transition bucket.
    pub lead_time_seconds: f64,
    /// Controller execution time.
    pub step_nanos: u64,
    /// Allocations during one controller step.
    pub step_allocations: u64,
    /// Retained controller and scratch bytes.
    pub retained_bytes: u64,
}

impl MetricPoint {
    /// Creates an empty sample at one virtual time.
    #[must_use]
    pub const fn zero(at_micros: u64) -> Self {
        Self {
            at_micros,
            arrivals: 0,
            backlog: 0,
            timers: 0,
            useful_completions: 0,
            transient_failures: 0,
            permanent_rejections: 0,
            timeouts: 0,
            latency_p50_micros: 0,
            latency_p90_micros: 0,
            latency_p99_micros: 0,
            latency_p999_micros: 0,
            permit_wait_p99_micros: 0,
            handler_elapsed_p99_micros: 0,
            settle_p99_micros: 0,
            queue_mean: 0.0_f64,
            queue_max: 0,
            requests_in_flight_p50: 0,
            requests_in_flight_p99: 0,
            handler_utilization_mean: 0.0_f64,
            handler_utilization_max: 0.0_f64,
            handler_utilization_cv: 0.0_f64,
            dependency_utilization: 0.0_f64,
            useful_throughput_per_second: 0.0_f64,
            resource_concurrency: 0.0_f64,
            capacity_low_per_second: 0.0_f64,
            capacity_median_per_second: 0.0_f64,
            capacity_high_per_second: 0.0_f64,
            saturation_probability: 0.0_f64,
            no_knee_probability: 0.0_f64,
            prediction_low: 0.0_f64,
            prediction_median: 0.0_f64,
            prediction_high: 0.0_f64,
            replicas: 0,
            target: 0,
            cap: 0,
            hold: false,
            miss_fraction: 0.0_f64,
            replica_seconds: 0.0_f64,
            snapshot_age_micros: 0,
            missing_reporters: 0,
            scale_actions: 0,
            recovery_micros: 0,
            expected_loss: 0.0_f64,
            lead_time_up_seconds: 0.0_f64,
            lead_time_down_seconds: 0.0_f64,
            lead_time_seconds: 0.0_f64,
            step_nanos: 0,
            step_allocations: 0,
            retained_bytes: 0,
        }
    }
}

/// Fixed-capacity structure-of-arrays metric trace.
pub struct MetricTrace {
    pub(crate) complete_metrics: bool,
    pub(crate) controller_metrics: bool,
    pub(crate) resource_metrics: bool,
    pub(crate) at_micros: Vec<u64>,
    pub(crate) arrivals: Vec<u64>,
    pub(crate) backlog: Vec<u64>,
    pub(crate) timers: Vec<u64>,
    pub(crate) useful_completions: Vec<u64>,
    pub(crate) transient_failures: Vec<u64>,
    pub(crate) permanent_rejections: Vec<u64>,
    pub(crate) timeouts: Vec<u64>,
    pub(crate) latency_p50_micros: Vec<u64>,
    pub(crate) latency_p90_micros: Vec<u64>,
    pub(crate) latency_p99_micros: Vec<u64>,
    pub(crate) latency_p999_micros: Vec<u64>,
    pub(crate) permit_wait_p99_micros: Vec<u64>,
    pub(crate) handler_elapsed_p99_micros: Vec<u64>,
    pub(crate) settle_p99_micros: Vec<u64>,
    pub(crate) queue_mean: Vec<f64>,
    pub(crate) queue_max: Vec<u64>,
    pub(crate) requests_in_flight_p50: Vec<u64>,
    pub(crate) requests_in_flight_p99: Vec<u64>,
    pub(crate) handler_utilization_mean: Vec<f64>,
    pub(crate) handler_utilization_max: Vec<f64>,
    pub(crate) handler_utilization_cv: Vec<f64>,
    pub(crate) dependency_utilization: Vec<f64>,
    pub(crate) useful_throughput_per_second: Vec<f64>,
    pub(crate) resource_concurrency: Vec<f64>,
    pub(crate) capacity_low_per_second: Vec<f64>,
    pub(crate) capacity_median_per_second: Vec<f64>,
    pub(crate) capacity_high_per_second: Vec<f64>,
    pub(crate) saturation_probability: Vec<f64>,
    pub(crate) no_knee_probability: Vec<f64>,
    pub(crate) prediction_low: Vec<f64>,
    pub(crate) prediction_median: Vec<f64>,
    pub(crate) prediction_high: Vec<f64>,
    pub(crate) replicas: Vec<u32>,
    pub(crate) target: Vec<u32>,
    pub(crate) cap: Vec<u32>,
    pub(crate) hold: Vec<bool>,
    pub(crate) miss_fraction: Vec<f64>,
    pub(crate) replica_seconds: Vec<f64>,
    pub(crate) snapshot_age_micros: Vec<u64>,
    pub(crate) missing_reporters: Vec<u32>,
    pub(crate) scale_actions: Vec<u32>,
    pub(crate) recovery_micros: Vec<u64>,
    pub(crate) expected_loss: Vec<f64>,
    pub(crate) lead_time_up_seconds: Vec<f64>,
    pub(crate) lead_time_down_seconds: Vec<f64>,
    pub(crate) lead_time_seconds: Vec<f64>,
    pub(crate) step_nanos: Vec<u64>,
    pub(crate) step_allocations: Vec<u64>,
    pub(crate) retained_bytes: Vec<u64>,
}

impl MetricTrace {
    /// Allocates every metric column at one fixed capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the capacity does not fit this platform.
    pub fn new(point_count_max: u32) -> Result<Self, PlantError> {
        let capacity = usize::try_from(point_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "metric_point_count_max",
            });
        }
        Ok(Self {
            complete_metrics: true,
            controller_metrics: true,
            resource_metrics: true,
            at_micros: Vec::with_capacity(capacity),
            arrivals: Vec::with_capacity(capacity),
            backlog: Vec::with_capacity(capacity),
            timers: Vec::with_capacity(capacity),
            useful_completions: Vec::with_capacity(capacity),
            transient_failures: Vec::with_capacity(capacity),
            permanent_rejections: Vec::with_capacity(capacity),
            timeouts: Vec::with_capacity(capacity),
            latency_p50_micros: Vec::with_capacity(capacity),
            latency_p90_micros: Vec::with_capacity(capacity),
            latency_p99_micros: Vec::with_capacity(capacity),
            latency_p999_micros: Vec::with_capacity(capacity),
            permit_wait_p99_micros: Vec::with_capacity(capacity),
            handler_elapsed_p99_micros: Vec::with_capacity(capacity),
            settle_p99_micros: Vec::with_capacity(capacity),
            queue_mean: Vec::with_capacity(capacity),
            queue_max: Vec::with_capacity(capacity),
            requests_in_flight_p50: Vec::with_capacity(capacity),
            requests_in_flight_p99: Vec::with_capacity(capacity),
            handler_utilization_mean: Vec::with_capacity(capacity),
            handler_utilization_max: Vec::with_capacity(capacity),
            handler_utilization_cv: Vec::with_capacity(capacity),
            dependency_utilization: Vec::with_capacity(capacity),
            useful_throughput_per_second: Vec::with_capacity(capacity),
            resource_concurrency: Vec::with_capacity(capacity),
            capacity_low_per_second: Vec::with_capacity(capacity),
            capacity_median_per_second: Vec::with_capacity(capacity),
            capacity_high_per_second: Vec::with_capacity(capacity),
            saturation_probability: Vec::with_capacity(capacity),
            no_knee_probability: Vec::with_capacity(capacity),
            prediction_low: Vec::with_capacity(capacity),
            prediction_median: Vec::with_capacity(capacity),
            prediction_high: Vec::with_capacity(capacity),
            replicas: Vec::with_capacity(capacity),
            target: Vec::with_capacity(capacity),
            cap: Vec::with_capacity(capacity),
            hold: Vec::with_capacity(capacity),
            miss_fraction: Vec::with_capacity(capacity),
            replica_seconds: Vec::with_capacity(capacity),
            snapshot_age_micros: Vec::with_capacity(capacity),
            missing_reporters: Vec::with_capacity(capacity),
            scale_actions: Vec::with_capacity(capacity),
            recovery_micros: Vec::with_capacity(capacity),
            expected_loss: Vec::with_capacity(capacity),
            lead_time_up_seconds: Vec::with_capacity(capacity),
            lead_time_down_seconds: Vec::with_capacity(capacity),
            lead_time_seconds: Vec::with_capacity(capacity),
            step_nanos: Vec::with_capacity(capacity),
            step_allocations: Vec::with_capacity(capacity),
            retained_bytes: Vec::with_capacity(capacity),
        })
    }

    /// Appends one complete point without growing any column.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixed trace is full.
    pub fn push(&mut self, point: MetricPoint) -> Result<(), PlantError> {
        if self.at_micros.len() == self.at_micros.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.at_micros.push(point.at_micros);
        self.arrivals.push(point.arrivals);
        self.backlog.push(point.backlog);
        self.timers.push(point.timers);
        self.useful_completions.push(point.useful_completions);
        self.transient_failures.push(point.transient_failures);
        self.permanent_rejections.push(point.permanent_rejections);
        self.timeouts.push(point.timeouts);
        self.latency_p50_micros.push(point.latency_p50_micros);
        self.latency_p90_micros.push(point.latency_p90_micros);
        self.latency_p99_micros.push(point.latency_p99_micros);
        self.latency_p999_micros.push(point.latency_p999_micros);
        self.permit_wait_p99_micros
            .push(point.permit_wait_p99_micros);
        self.handler_elapsed_p99_micros
            .push(point.handler_elapsed_p99_micros);
        self.settle_p99_micros.push(point.settle_p99_micros);
        self.queue_mean.push(point.queue_mean);
        self.queue_max.push(point.queue_max);
        self.requests_in_flight_p50
            .push(point.requests_in_flight_p50);
        self.requests_in_flight_p99
            .push(point.requests_in_flight_p99);
        self.handler_utilization_mean
            .push(point.handler_utilization_mean);
        self.handler_utilization_max
            .push(point.handler_utilization_max);
        self.handler_utilization_cv
            .push(point.handler_utilization_cv);
        self.dependency_utilization
            .push(point.dependency_utilization);
        self.useful_throughput_per_second
            .push(point.useful_throughput_per_second);
        self.resource_concurrency.push(point.resource_concurrency);
        self.capacity_low_per_second
            .push(point.capacity_low_per_second);
        self.capacity_median_per_second
            .push(point.capacity_median_per_second);
        self.capacity_high_per_second
            .push(point.capacity_high_per_second);
        self.saturation_probability
            .push(point.saturation_probability);
        self.no_knee_probability.push(point.no_knee_probability);
        self.prediction_low.push(point.prediction_low);
        self.prediction_median.push(point.prediction_median);
        self.prediction_high.push(point.prediction_high);
        self.replicas.push(point.replicas);
        self.target.push(point.target);
        self.cap.push(point.cap);
        self.hold.push(point.hold);
        self.miss_fraction.push(point.miss_fraction);
        self.replica_seconds.push(point.replica_seconds);
        self.snapshot_age_micros.push(point.snapshot_age_micros);
        self.missing_reporters.push(point.missing_reporters);
        self.scale_actions.push(point.scale_actions);
        self.recovery_micros.push(point.recovery_micros);
        self.expected_loss.push(point.expected_loss);
        self.lead_time_up_seconds.push(point.lead_time_up_seconds);
        self.lead_time_down_seconds
            .push(point.lead_time_down_seconds);
        self.lead_time_seconds.push(point.lead_time_seconds);
        self.step_nanos.push(point.step_nanos);
        self.step_allocations.push(point.step_allocations);
        self.retained_bytes.push(point.retained_bytes);
        Ok(())
    }

    /// Returns the number of complete points.
    #[must_use]
    pub fn len(&self) -> usize {
        self.at_micros.len()
    }

    /// Returns true when the trace has no points.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.at_micros.is_empty()
    }

    /// Reconstructs one metric point from the trace columns.
    #[must_use]
    pub fn point(&self, index: usize) -> Option<MetricPoint> {
        Some(MetricPoint {
            at_micros: *self.at_micros.get(index)?,
            arrivals: self.arrivals[index],
            backlog: self.backlog[index],
            timers: self.timers[index],
            useful_completions: self.useful_completions[index],
            transient_failures: self.transient_failures[index],
            permanent_rejections: self.permanent_rejections[index],
            timeouts: self.timeouts[index],
            latency_p50_micros: self.latency_p50_micros[index],
            latency_p90_micros: self.latency_p90_micros[index],
            latency_p99_micros: self.latency_p99_micros[index],
            latency_p999_micros: self.latency_p999_micros[index],
            permit_wait_p99_micros: self.permit_wait_p99_micros[index],
            handler_elapsed_p99_micros: self.handler_elapsed_p99_micros[index],
            settle_p99_micros: self.settle_p99_micros[index],
            queue_mean: self.queue_mean[index],
            queue_max: self.queue_max[index],
            requests_in_flight_p50: self.requests_in_flight_p50[index],
            requests_in_flight_p99: self.requests_in_flight_p99[index],
            handler_utilization_mean: self.handler_utilization_mean[index],
            handler_utilization_max: self.handler_utilization_max[index],
            handler_utilization_cv: self.handler_utilization_cv[index],
            dependency_utilization: self.dependency_utilization[index],
            useful_throughput_per_second: self.useful_throughput_per_second[index],
            resource_concurrency: self.resource_concurrency[index],
            capacity_low_per_second: self.capacity_low_per_second[index],
            capacity_median_per_second: self.capacity_median_per_second[index],
            capacity_high_per_second: self.capacity_high_per_second[index],
            saturation_probability: self.saturation_probability[index],
            no_knee_probability: self.no_knee_probability[index],
            prediction_low: self.prediction_low[index],
            prediction_median: self.prediction_median[index],
            prediction_high: self.prediction_high[index],
            replicas: self.replicas[index],
            target: self.target[index],
            cap: self.cap[index],
            hold: self.hold[index],
            miss_fraction: self.miss_fraction[index],
            replica_seconds: self.replica_seconds[index],
            snapshot_age_micros: self.snapshot_age_micros[index],
            missing_reporters: self.missing_reporters[index],
            scale_actions: self.scale_actions[index],
            recovery_micros: self.recovery_micros[index],
            expected_loss: self.expected_loss[index],
            lead_time_up_seconds: self.lead_time_up_seconds[index],
            lead_time_down_seconds: self.lead_time_down_seconds[index],
            lead_time_seconds: self.lead_time_seconds[index],
            step_nanos: self.step_nanos[index],
            step_allocations: self.step_allocations[index],
            retained_bytes: self.retained_bytes[index],
        })
    }

    pub(crate) const fn mark_plant_only(&mut self) {
        self.complete_metrics = false;
        self.controller_metrics = false;
        self.resource_metrics = false;
    }
}
