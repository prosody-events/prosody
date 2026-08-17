//! Independent virtual-time plant for predictive autoscaling tests.
//!
//! The plant uses fixed-capacity arrays and intrusive per-key queues. It does
//! not call controller equations.

use prosody_scale_core::{DemandClass, RandomStream};
use rayon::prelude::*;
use statrs::distribution::{BinomialError, PoissonError};
use std::collections::VecDeque;
use std::mem;
use std::num::NonZeroU8;
use thiserror::Error;

const PLOT_FONT_FAMILY: &str = "Charter";
const DEFAULT_CONCURRENCY_PER_REPLICA: u32 = 32;

mod batch;
mod batch_plot;
mod calibration;
mod calibration_plot;
mod controller;
mod harness;
mod input;
mod metadata;
mod metrics;
mod model;
mod plot_error;
mod posterior_plot;
mod regime;
mod report;
mod report_check;
mod result_metrics;
mod series;
mod snapshot;
mod story_plot;
mod visual;
mod w6_witness;

pub use batch::{
    BatchInputs, BatchSloError, BatchSloSummary, run_batch_regime, run_batch_slo,
    run_batch_slo_with_inputs,
};
pub use batch_plot::{write_batch_actuation_svg, write_batch_slo_svg};
pub use calibration::{
    CalibrationError, CapacityCalibration, CapacityCalibrationTrial,
    CapacitySensitivityCalibration, CapacitySensitivityTrial, DemandCalibration,
    DemandCalibrationTrial, LeadTimeCalibration, LeadTimeCalibrationTrial, PartitionCalibration,
    PartitionCalibrationTrial, predictive_coverage_levels, run_capacity_calibration,
    run_capacity_sensitivity, run_demand_calibration, run_lead_time_calibration,
    run_partition_calibration,
};
pub use calibration_plot::{
    write_capacity_calibration_figures, write_capacity_sensitivity_figures,
    write_demand_calibration_figures, write_lead_time_calibration_data,
    write_partition_calibration_data,
};
pub use controller::{
    ArrivalEvidenceSample, ArrivalWindowSample, CapacityEvidenceKind, CapacityEvidenceSample,
    CapacityTraceSample, CapacityWindowSample, ClosedLoop, ClosedLoopError, ControllerSample,
    ControllerTrace, LeadTimeEvidenceSample, ReliabilityEvidenceSample,
};
pub use harness::{
    CalendarForecastInput, EventContext, EventInputs, EventOutcomeRule, FailureBacklog,
    FailureBacklogView, NormalBacklog, NormalBacklogView, ReporterDirective, ScaleDirective,
    ScheduledReleasesInput, SimulationHarness, TickContext, TickGenerator, TickHistory,
    TickHistoryView, TickInputs,
};
pub use input::{ConcurrencyLatencyCurve, InputError, QuantileTable, StepSeries, WorkloadSeries};
pub use metadata::{GENERATOR_VERSION, PriorArtifactKind, PriorArtifactMetadata, ReportMetadata};
pub use metrics::{MetricPoint, MetricTrace};
pub use model::{
    AttemptContext, AttemptFrame, AttemptGenerator, AttemptHistory, AttemptHistoryView,
    AttemptModel, AttemptParameters, HistoricalAttemptModel, SeriesAttemptModel,
};
pub use plot_error::PlotError;
pub use posterior_plot::{write_model_belief_figures, write_model_belief_snapshot_figures};
pub use regime::{
    CapacitySensitivity, PrincipalRegime, PrincipalRun, PrincipalRunError, RegimeExperiment,
    RegimeValidationError, RunStop, RunStopReason, run_capacity_evidence_regime,
    run_capacity_evidence_regime_seeded, run_principal_regime, run_principal_regime_seeded,
    validate_principal_regime,
};
pub use report::{
    ExperimentReport, HistoricalComparisonRow, RegimeReport, ReportError, write_batch_report_pdf,
    write_capacity_calibration_report_pdf, write_demand_calibration_report_pdf,
    write_historical_comparison_pdf, write_lead_time_calibration_report_pdf,
    write_partition_calibration_report_pdf, write_regime_report_pdf,
};
pub use report_check::{
    DocumentManifest, ImageManifestEntry, PanelContent, ReportCheckError, ReportSection,
    check_document, check_images, label_inside_image,
};
pub use series::{
    OutputFunction, RecordedSeries, SeriesCell, SeriesContext, SeriesFunction, SeriesHistory,
    SeriesHistoryView, SeriesKey, SeriesValue,
};
pub use snapshot::{
    ArrivalInterval, FaultPattern, ReporterState, Snapshot, SnapshotChannel, SnapshotCursor,
    SnapshotTable,
};
pub use story_plot::{RegimeStory, write_regime_story_figures};
pub use w6_witness::{W6AblationArm, W6AblationWitness};

const NO_EVENT: u32 = u32::MAX;
const DEFAULT_FAILURE_WEIGHT: f64 = 0.3_f64;
const DEFAULT_DEFER_THRESHOLD: f64 = 0.9_f64;
const DEFAULT_FAILURE_WINDOW_MICROS: u64 = 300_000_000;
const DEFAULT_RETRY_BASE_MICROS: u64 = 20_000;
const DEFAULT_RETRY_MAX_MICROS: u64 = 300_000_000;
const DEFAULT_DEFER_BASE_MICROS: u64 = 1_000_000;
const DEFAULT_DEFER_MAX_MICROS: u64 = 86_400_000_000;
const MAX_RETRY_FAILURES: u8 = 2;

/// KIP-848 timing inputs for one moved partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Kip848Rebalance {
    notification_micros: QuantileTable,
    revocation_micros: QuantileTable,
    assignment_micros: QuantileTable,
    warmup_micros: QuantileTable,
    seed: u64,
}

impl Kip848Rebalance {
    /// Constructs independent empirical timing distributions.
    #[must_use]
    pub const fn new(
        notification_micros: QuantileTable,
        revocation_micros: QuantileTable,
        assignment_micros: QuantileTable,
        warmup_micros: QuantileTable,
        seed: u64,
    ) -> Self {
        Self {
            notification_micros,
            revocation_micros,
            assignment_micros,
            warmup_micros,
            seed,
        }
    }

    fn sample(&self, change: u32, partition: u32) -> ReconciliationTiming {
        let domain = u64::from(change) << 32_u32 | u64::from(partition);
        let mut random = RandomStream::new(self.seed).domain(domain);
        ReconciliationTiming {
            notification: self.notification_micros.sample(&mut random),
            revocation: self.revocation_micros.sample(&mut random),
            assignment: self.assignment_micros.sample(&mut random),
            warmup: self.warmup_micros.sample(&mut random),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReconciliationTiming {
    notification: u64,
    revocation: u64,
    assignment: u64,
    warmup: u64,
}

#[derive(Clone, Copy, Debug)]
struct RetryPolicy {
    failure_weight: f64,
    defer_threshold: f64,
    failure_window_micros: u64,
    inline_base_micros: u64,
    inline_max_micros: u64,
    deferred_base_micros: u64,
    deferred_max_micros: u64,
    seed: u64,
}

impl RetryPolicy {
    const fn prosody_default() -> Self {
        Self {
            failure_weight: DEFAULT_FAILURE_WEIGHT,
            defer_threshold: DEFAULT_DEFER_THRESHOLD,
            failure_window_micros: DEFAULT_FAILURE_WINDOW_MICROS,
            inline_base_micros: DEFAULT_RETRY_BASE_MICROS,
            inline_max_micros: DEFAULT_RETRY_MAX_MICROS,
            deferred_base_micros: DEFAULT_DEFER_BASE_MICROS,
            deferred_max_micros: DEFAULT_DEFER_MAX_MICROS,
            seed: 0,
        }
    }
}

/// Fixed plant bounds and service constants.
#[derive(Clone, Debug)]
pub struct PlantConfiguration {
    partition_count: u32,
    key_count: u32,
    event_count_max: u32,
    change_count_max: u32,
    metric_poll_interval_micros: u64,
    slots_per_replica: u32,
    dependency_slots: u32,
    dependency_operation_micros: StepSeries<u64>,
    dependency_latency_curve: ConcurrencyLatencyCurve,
    retry_policy: RetryPolicy,
    rebalance: Kip848Rebalance,
    handler_latency_curve: ConcurrencyLatencyCurve,
}

impl PlantConfiguration {
    /// Constructs validated fixed bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when a required bound is zero.
    pub fn new(
        partition_count: u32,
        key_count: u32,
        event_count_max: u32,
        change_count_max: u32,
        slots_per_replica: u32,
        dependency_slots: u32,
    ) -> Result<Self, PlantError> {
        validate_positive(partition_count, "partition_count")?;
        validate_positive(key_count, "key_count")?;
        validate_positive(event_count_max, "event_count_max")?;
        validate_positive(change_count_max, "change_count_max")?;
        validate_positive(slots_per_replica, "slots_per_replica")?;
        validate_positive(dependency_slots, "dependency_slots")?;
        Ok(Self {
            partition_count,
            key_count,
            event_count_max,
            change_count_max,
            metric_poll_interval_micros: 1_000_000,
            slots_per_replica,
            dependency_slots,
            dependency_operation_micros: StepSeries::constant(1_000),
            dependency_latency_curve: ConcurrencyLatencyCurve::zero(),
            retry_policy: RetryPolicy::prosody_default(),
            rebalance: Kip848Rebalance::new(
                QuantileTable::constant(0),
                QuantileTable::constant(100_000),
                QuantileTable::constant(0),
                QuantileTable::constant(100_000),
                0,
            ),
            handler_latency_curve: ConcurrencyLatencyCurve::zero(),
        })
    }

    /// Sets the interval between desired-replica metric polls.
    #[must_use]
    pub const fn with_metric_poll_interval_micros(mut self, micros: u64) -> Self {
        self.metric_poll_interval_micros = micros;
        self
    }

    const fn metric_poll_interval_micros(&self) -> u64 {
        self.metric_poll_interval_micros
    }

    /// Sets the duration of one dependency operation.
    #[must_use]
    pub fn with_dependency_operation_micros(mut self, micros: u64) -> Self {
        self.dependency_operation_micros = StepSeries::constant(micros);
        self
    }

    /// Sets base dependency latency as a virtual-time series.
    #[must_use]
    pub fn with_dependency_latency_series(mut self, series: StepSeries<u64>) -> Self {
        self.dependency_operation_micros = series;
        self
    }

    /// Sets added resource latency as an active-handler response curve.
    #[must_use]
    pub fn with_dependency_latency_curve(mut self, curve: ConcurrencyLatencyCurve) -> Self {
        self.dependency_latency_curve = curve;
        self
    }

    /// Sets retry backoff.
    #[must_use]
    pub const fn with_retry_backoff_micros(mut self, micros: u64) -> Self {
        self.retry_policy.inline_base_micros = micros;
        self.retry_policy.inline_max_micros = micros;
        self
    }

    /// Sets deterministic KIP-848 revocation and warm-up durations.
    #[must_use]
    pub fn with_rebalance(mut self, pause_micros: u64, warmup_micros: u64) -> Self {
        self.rebalance = Kip848Rebalance::new(
            QuantileTable::constant(0),
            QuantileTable::constant(pause_micros),
            QuantileTable::constant(0),
            QuantileTable::constant(warmup_micros),
            0,
        );
        self
    }

    /// Sets KIP-848 timing distributions for moved partitions.
    #[must_use]
    pub fn with_kip848_rebalance(mut self, rebalance: Kip848Rebalance) -> Self {
        self.rebalance = rebalance;
        self
    }

    /// Sets added handler time as a function of active concurrency.
    #[must_use]
    pub fn with_handler_latency_curve(mut self, curve: ConcurrencyLatencyCurve) -> Self {
        self.handler_latency_curve = curve;
        self
    }
}

/// One final attempt outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalOutcome {
    /// The handler completed successfully.
    Success,
    /// The handler rejected the event permanently.
    PermanentFailure,
}

/// One outcome that creates later Failure demand.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryOutcome {
    /// The attempt failed transiently.
    Transient,
    /// The attempt terminated the current client.
    Terminal,
}

/// A positive retry count within the simulator bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryCount(NonZeroU8);

impl RetryCount {
    /// Constructs one bounded retry count.
    ///
    /// # Errors
    ///
    /// Returns an error when the count is zero or exceeds the simulator bound.
    pub fn new(count: u8) -> Result<Self, RetryCountError> {
        let Some(count) = NonZeroU8::new(count) else {
            return Err(RetryCountError::Zero);
        };
        if count.get() > MAX_RETRY_FAILURES {
            return Err(RetryCountError::Bound {
                maximum: MAX_RETRY_FAILURES,
            });
        }
        Ok(Self(count))
    }

    const fn get(self) -> u8 {
        self.0.get()
    }

    fn after_one(self) -> Option<Self> {
        NonZeroU8::new(self.get() - 1).map(Self)
    }
}

/// Complete outcome plan for one logical event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventOutcome {
    /// The first attempt ends the event.
    Final(FinalOutcome),
    /// One or more outcomes create Failure demand before the final attempt.
    Retry {
        /// Outcome for each retry-producing attempt.
        outcome: RetryOutcome,
        /// Number of retry-producing attempts.
        count: RetryCount,
        /// Outcome that ends the event.
        final_outcome: FinalOutcome,
    },
}

/// Durable source that released one event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    /// Kafka released a message.
    Message,
    /// The timer store released a timer.
    Timer,
}

impl EventOutcome {
    /// Constructs a transient retry sequence or one final outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the retry count exceeds the simulator bound.
    pub fn from_transient_failures(
        count: u8,
        final_outcome: FinalOutcome,
    ) -> Result<Self, RetryCountError> {
        if count == 0 {
            return Ok(Self::Final(final_outcome));
        }
        Ok(Self::Retry {
            outcome: RetryOutcome::Transient,
            count: RetryCount::new(count)?,
            final_outcome,
        })
    }

    const fn final_outcome(self) -> FinalOutcome {
        match self {
            Self::Final(outcome)
            | Self::Retry {
                final_outcome: outcome,
                ..
            } => outcome,
        }
    }
}

/// One event offered to the plant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventSpec {
    /// Virtual arrival or timer release time.
    pub release_micros: u64,
    /// Kafka partition.
    pub partition: u32,
    /// Serialized key index.
    pub key: u32,
    /// Non-preemptive handler time for each attempt.
    pub handler_micros: u64,
    /// Dependency operations for each attempt.
    pub dependency_operations: u32,
    /// Complete sequence of attempt outcomes.
    pub outcome: EventOutcome,
    /// Durable source that released this event.
    pub source: EventSource,
}

/// One requested replica change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScaleChange {
    /// Command time in virtual microseconds.
    pub at_micros: u64,
    /// New replica count.
    pub replicas: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScaleUpCohort {
    count: u32,
    ready_micros: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScaleDown {
    target: u32,
    apply_micros: u64,
    generation: u32,
}

/// One mutually exclusive aggregate actuation mode.
enum PendingActuation {
    Converged(Vec<ScaleUpCohort>),
    Up(Vec<ScaleUpCohort>),
    Down {
        down: ScaleDown,
        inactive_up_storage: Vec<ScaleUpCohort>,
    },
}

fn cohort_replica_count(cohorts: &[ScaleUpCohort]) -> u32 {
    cohorts
        .iter()
        .fold(0_u32, |total, cohort| total.saturating_add(cohort.count))
}

fn cancel_excess_scale_up(cohorts: &mut [ScaleUpCohort], mut excess: u32) {
    while excess > 0 {
        let Some(index) = cohorts
            .iter()
            .enumerate()
            .filter(|(_, cohort)| cohort.count > 0)
            .max_by_key(|(_, cohort)| cohort.ready_micros)
            .map(|(index, _)| index)
        else {
            break;
        };
        let canceled = excess.min(cohorts[index].count);
        cohorts[index].count -= canceled;
        excess -= canceled;
    }
    assert_eq!(
        excess, 0,
        "the active cohorts must cover the canceled excess"
    );
}

/// One desired replica change before actuator delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScaleRequest {
    /// Decision time in virtual microseconds.
    pub at_micros: u64,
    /// Desired replica count.
    pub replicas: u32,
}

/// One scale request paired with its sampled readiness time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Actuation {
    /// Original desired replica change.
    pub request: ScaleRequest,
    /// Time when the desired replicas become actual replicas.
    pub ready_micros: u64,
}

/// Bounded plant state at one virtual-time controller tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlantSnapshot {
    /// Snapshot time.
    pub at_micros: u64,
    /// Actual ready replicas.
    pub replicas: u32,
    /// Events released by this time.
    pub released: u32,
    /// Events settled by this time.
    pub settled: u32,
    /// Released events that have not settled.
    pub backlog: u32,
    /// Handler attempts that currently hold slots.
    pub active_handlers: u32,
    /// Cumulative handler occupancy in handler-microseconds.
    pub handler_occupancy_micros: u64,
    /// Cumulative successful final completions.
    pub useful_completions: u32,
    /// Cumulative completed handler attempts for all outcomes.
    pub completed_attempts: u32,
    /// Cumulative handler attempts started for all outcomes.
    pub started_attempts: u32,
    /// Count of handler-slot transitions recorded by this snapshot.
    ///
    /// Two consecutive snapshots bracket one report window: the transition
    /// log between their counts is exactly the evidence between the samples,
    /// however boundary-time ties order against the sample.
    pub attempt_transition_count: usize,
    /// Cumulative time with at least one partition paused for reconciliation.
    pub rebalance_pause_micros: u64,
    /// Cumulative completed normal attempts.
    pub normal_attempts: u32,
    /// Cumulative successes from normal attempts.
    pub normal_successes: u32,
    /// Cumulative transient failures from normal attempts.
    pub normal_transient_failures: u32,
    /// Cumulative terminal failures from normal attempts.
    pub normal_terminal_failures: u32,
    /// Cumulative permanent failures from normal attempts.
    pub normal_permanent_failures: u32,
    /// Cumulative completed failure attempts.
    pub failure_attempts: u32,
    /// Cumulative successes from failure attempts.
    pub failure_successes: u32,
    /// Cumulative transient failures from failure attempts.
    pub failure_transient_failures: u32,
    /// Cumulative terminal failures from failure attempts.
    pub failure_terminal_failures: u32,
    /// Cumulative permanent failures from failure attempts.
    pub failure_permanent_failures: u32,
    /// Whether all assigned partitions are ready at this time.
    pub partitions_ready: bool,
    /// Count of partitions in an incomplete assignment change.
    pub reconciling_partitions: u32,
    /// Count of partitions that currently reject new dispatches.
    pub paused_partitions: u32,
    /// Exact time when the first moved partition stopped dispatches.
    pub reconciliation_started_micros: Option<u64>,
    /// Exact time when the last moved partition resumed dispatches.
    pub reconciliation_completed_micros: Option<u64>,
}

/// Final event outcome from the plant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Settlement {
    /// Event index in insertion order.
    pub event: u32,
    /// Original release time.
    pub release_micros: u64,
    /// Final settle time.
    pub settle_micros: u64,
    /// Number of attempts.
    pub attempts: u32,
    /// Time from release until the first handler slot starts work.
    pub permit_wait_micros: u64,
    /// Total dependency service time across all attempts.
    pub dependency_micros: u64,
    /// Total measured handler service time across all attempts.
    pub handler_micros: u64,
    /// Active requests when the first attempt started.
    pub in_flight_at_dispatch: u32,
    /// Queued events when the first attempt started.
    pub queue_at_dispatch: u32,
    /// Outcome that ended the event.
    pub final_outcome: FinalOutcome,
}

impl Settlement {
    /// Returns elapsed handler-slot time from dispatch through settlement.
    #[must_use]
    pub const fn handler_elapsed_micros(self) -> u64 {
        self.settle_micros
            .saturating_sub(self.release_micros)
            .saturating_sub(self.permit_wait_micros)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptState {
    Pending,
    Ready(DemandClass),
    Running(DemandClass),
    Backoff(RetryWait),
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryWait {
    Inline,
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryMode {
    Inline,
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AttemptOutcome {
    at_micros: u64,
    result: AttemptResult,
}

/// One exact handler-slot transition from the simulator event stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptTransition {
    /// Simulator clock time for the transition.
    pub at_micros: u64,
    /// Direction of the busy-slot change.
    pub kind: AttemptTransitionKind,
}

/// Direction of one exact handler-slot transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptTransitionKind {
    /// One handler slot started work.
    Start,
    /// One handler slot completed work.
    Completion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptResult {
    Success,
    Failure,
}

/// Bounded virtual-time plant.
pub struct Plant<M = SeriesAttemptModel> {
    configuration: PlantConfiguration,
    attempt_model: M,
    replicas: u32,
    events: Vec<EventSpec>,
    pending_actuation: PendingActuation,
    scale_down_generation: u32,
    scale_schedule_count: usize,
    applied_changes: Vec<ScaleChange>,
    desired_replicas: u32,
    heap: Vec<Scheduled>,
    next_by_event: Vec<u32>,
    attempts_by_event: Vec<u32>,
    attempt_state: Vec<AttemptState>,
    attempt_started_micros: Vec<u64>,
    retry_ready_micros: Vec<u64>,
    deferred_retry_count: Vec<u32>,
    retry_mode_by_event: Vec<RetryMode>,
    settled_by_event: Vec<bool>,
    first_dispatch_micros: Vec<u64>,
    dependency_micros: Vec<u64>,
    handler_micros: Vec<u64>,
    in_flight_at_dispatch: Vec<u32>,
    queue_at_dispatch: Vec<u32>,
    key_head: Vec<u32>,
    key_tail: Vec<u32>,
    key_active: Vec<bool>,
    owner_at_dispatch: Vec<u32>,
    partition_owner: Vec<u32>,
    partition_target_owner: Vec<u32>,
    partition_epoch: Vec<u32>,
    partition_reconciliation: Vec<PartitionReconciliation>,
    partition_active_handlers: Vec<u32>,
    active_handlers_by_owner: Vec<u32>,
    assignment_counts: Vec<u32>,
    reconciliation_started_micros: Option<u64>,
    reconciliation_completed_micros: Option<u64>,
    settlements: Vec<Settlement>,
    active_handlers: u32,
    handler_occupancy_micros: u64,
    active_dependency_operations: u32,
    normal_service_micros: u64,
    failure_service_micros: u64,
    attempt_outcomes: VecDeque<AttemptOutcome>,
    attempt_transitions: Vec<AttemptTransition>,
    useful_completions: u32,
    completed_attempts: u32,
    started_attempts: u32,
    rebalance_pause_micros: u64,
    normal_attempts: u32,
    normal_successes: u32,
    normal_transient_failures: u32,
    normal_terminal_failures: u32,
    normal_permanent_failures: u32,
    failure_attempts: u32,
    failure_successes: u32,
    failure_transient_failures: u32,
    failure_terminal_failures: u32,
    failure_permanent_failures: u32,
    queued_events: u32,
    initial_replicas: u32,
    now_micros: u64,
    started: bool,
}

impl Plant {
    /// Allocates all bounded plant memory.
    ///
    /// # Errors
    ///
    /// Returns an error when a bound does not fit this platform.
    pub fn new(
        configuration: PlantConfiguration,
        initial_replicas: u32,
    ) -> Result<Self, PlantError> {
        let attempt_model = SeriesAttemptModel::new(
            configuration.dependency_operation_micros.clone(),
            configuration.dependency_latency_curve.clone(),
            configuration.handler_latency_curve.clone(),
            configuration.event_count_max,
        )?;
        Self::with_attempt_model(configuration, initial_replicas, attempt_model)
    }
}

impl<M: AttemptModel> Plant<M> {
    /// Allocates all bounded plant memory with one regime calculation model.
    ///
    /// # Errors
    ///
    /// Returns an error when a bound does not fit this platform.
    pub fn with_attempt_model(
        configuration: PlantConfiguration,
        initial_replicas: u32,
        attempt_model: M,
    ) -> Result<Self, PlantError> {
        validate_positive(initial_replicas, "initial_replicas")?;
        let event_count_max = to_usize(configuration.event_count_max)?;
        let change_count_max = to_usize(configuration.change_count_max)?;
        let key_count = to_usize(configuration.key_count)?;
        let partition_count = to_usize(configuration.partition_count)?;
        let reconciliation_capacity = change_count_max
            .checked_mul(partition_count)
            .and_then(|count| count.checked_mul(2))
            .ok_or(PlantError::PlatformLimit)?;
        let heap_capacity = event_count_max
            .checked_mul(4)
            .and_then(|capacity| capacity.checked_add(change_count_max))
            .and_then(|capacity| capacity.checked_add(reconciliation_capacity))
            .ok_or(PlantError::PlatformLimit)?;
        Ok(Self {
            configuration,
            attempt_model,
            replicas: initial_replicas,
            events: Vec::with_capacity(event_count_max),
            pending_actuation: PendingActuation::Converged(Vec::with_capacity(change_count_max)),
            scale_down_generation: 0,
            scale_schedule_count: 0,
            applied_changes: Vec::with_capacity(change_count_max),
            desired_replicas: initial_replicas,
            heap: Vec::with_capacity(heap_capacity),
            next_by_event: vec![NO_EVENT; event_count_max],
            attempts_by_event: vec![0; event_count_max],
            attempt_state: vec![AttemptState::Pending; event_count_max],
            attempt_started_micros: vec![0; event_count_max],
            retry_ready_micros: vec![0; event_count_max],
            deferred_retry_count: vec![0; event_count_max],
            retry_mode_by_event: vec![RetryMode::Inline; event_count_max],
            settled_by_event: vec![false; event_count_max],
            first_dispatch_micros: vec![u64::MAX; event_count_max],
            dependency_micros: vec![0; event_count_max],
            handler_micros: vec![0; event_count_max],
            in_flight_at_dispatch: vec![0; event_count_max],
            queue_at_dispatch: vec![0; event_count_max],
            key_head: vec![NO_EVENT; key_count],
            key_tail: vec![NO_EVENT; key_count],
            key_active: vec![false; key_count],
            owner_at_dispatch: vec![0; event_count_max],
            partition_owner: initial_assignment(partition_count, initial_replicas),
            partition_target_owner: vec![0; partition_count],
            partition_epoch: vec![0; partition_count],
            partition_reconciliation: vec![PartitionReconciliation::Serving; partition_count],
            partition_active_handlers: vec![0; partition_count],
            active_handlers_by_owner: vec![0; partition_count],
            assignment_counts: vec![0; partition_count],
            reconciliation_started_micros: None,
            reconciliation_completed_micros: None,
            settlements: Vec::with_capacity(event_count_max),
            active_handlers: 0,
            handler_occupancy_micros: 0,
            active_dependency_operations: 0,
            normal_service_micros: 0,
            failure_service_micros: 0,
            attempt_outcomes: VecDeque::with_capacity(
                event_count_max.saturating_mul(usize::from(MAX_RETRY_FAILURES) + 1),
            ),
            attempt_transitions: Vec::with_capacity(
                event_count_max
                    .saturating_mul(usize::from(MAX_RETRY_FAILURES) + 1)
                    .saturating_mul(2),
            ),
            useful_completions: 0,
            completed_attempts: 0,
            started_attempts: 0,
            rebalance_pause_micros: 0,
            normal_attempts: 0,
            normal_successes: 0,
            normal_transient_failures: 0,
            normal_terminal_failures: 0,
            normal_permanent_failures: 0,
            failure_attempts: 0,
            failure_successes: 0,
            failure_transient_failures: 0,
            failure_terminal_failures: 0,
            failure_permanent_failures: 0,
            queued_events: 0,
            initial_replicas,
            now_micros: 0,
            started: false,
        })
    }

    /// Adds one event without growing retained memory.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid index or full event buffer.
    pub fn add_event(&mut self, event: EventSpec) -> Result<(), PlantError> {
        if self.started && event.release_micros < self.now_micros {
            return Err(PlantError::EventTimeRegressed);
        }
        if event.partition >= self.configuration.partition_count {
            return Err(PlantError::PartitionIndex);
        }
        if event.key >= self.configuration.key_count {
            return Err(PlantError::KeyIndex);
        }
        if self.events.len() == self.events.capacity() {
            return Err(PlantError::EventCapacity);
        }
        let event_index = self.events.len() as u32;
        self.events.push(event);
        if self.started {
            heap_push(
                &mut self.heap,
                Scheduled {
                    at_micros: event.release_micros,
                    ordinal: event_index,
                    kind: ScheduledKind::Arrival(event_index),
                },
            );
        }
        Ok(())
    }

    /// Adds one already-timed aggregate replica target.
    ///
    /// # Errors
    ///
    /// Returns an error for zero replicas or a full actuation buffer.
    pub fn add_scale_change(&mut self, change: ScaleChange) -> Result<(), PlantError> {
        self.replace_scale_target(change)
    }

    /// Publishes one polled aggregate target with its sampled apply time.
    ///
    /// Ready replicas only decrease when a pending down applies. Ready replicas
    /// plus active up cohorts never exceed the latest polled target. With no
    /// pending actuation, ready replicas equal that target.
    ///
    /// # Errors
    ///
    /// Returns an error for zero replicas or a full change buffer.
    pub(crate) fn replace_scale_target(&mut self, change: ScaleChange) -> Result<(), PlantError> {
        validate_positive(change.replicas, "replicas")?;
        if self.desired_replicas == change.replicas {
            return Ok(());
        }
        self.desired_replicas = change.replicas;
        if change.replicas < self.replicas {
            self.schedule_scale_down(change)?;
        } else {
            self.scale_down_generation = self.scale_down_generation.wrapping_add(1);
            let mut cohorts = self.take_cohort_storage();
            let in_flight = cohort_replica_count(&cohorts);
            let planned = self.replicas.saturating_add(in_flight);
            if change.replicas < planned {
                cancel_excess_scale_up(&mut cohorts, planned - change.replicas);
            } else if change.replicas > planned {
                self.schedule_scale_up(&mut cohorts, change.replicas - planned, change.at_micros)?;
            }
            self.pending_actuation = if cohort_replica_count(&cohorts) == 0 {
                PendingActuation::Converged(cohorts)
            } else {
                PendingActuation::Up(cohorts)
            };
        }
        self.assert_scale_invariants();
        Ok(())
    }

    pub(crate) fn in_flight_replicas(&self) -> u32 {
        match &self.pending_actuation {
            PendingActuation::Up(cohorts) => cohort_replica_count(cohorts),
            PendingActuation::Converged(_) | PendingActuation::Down { .. } => 0,
        }
    }

    fn take_cohort_storage(&mut self) -> Vec<ScaleUpCohort> {
        let mode = mem::replace(
            &mut self.pending_actuation,
            PendingActuation::Converged(Vec::new()),
        );
        let was_up = matches!(&mode, PendingActuation::Up(_));
        let mut cohorts = match mode {
            PendingActuation::Converged(cohorts) | PendingActuation::Up(cohorts) => cohorts,
            PendingActuation::Down {
                inactive_up_storage,
                ..
            } => inactive_up_storage,
        };
        if !was_up {
            for cohort in &mut cohorts {
                cohort.count = 0;
            }
        }
        cohorts
    }

    fn reserve_scale_schedule(&mut self) -> Result<u32, PlantError> {
        let schedule_capacity = usize::try_from(self.configuration.change_count_max)
            .map_err(|_| PlantError::PlatformLimit)?;
        if self.scale_schedule_count == schedule_capacity {
            return Err(PlantError::ChangeCapacity);
        }
        let ordinal =
            u32::try_from(self.scale_schedule_count).map_err(|_| PlantError::PlatformLimit)?;
        self.scale_schedule_count += 1;
        Ok(ordinal)
    }

    fn schedule_scale_up(
        &mut self,
        cohorts: &mut Vec<ScaleUpCohort>,
        count: u32,
        ready_micros: u64,
    ) -> Result<(), PlantError> {
        let ordinal = self.reserve_scale_schedule()?;
        let index = u32::try_from(cohorts.len()).map_err(|_| PlantError::PlatformLimit)?;
        cohorts.push(ScaleUpCohort {
            count,
            ready_micros,
        });
        heap_push(
            &mut self.heap,
            Scheduled {
                at_micros: ready_micros,
                ordinal,
                kind: ScheduledKind::ScaleUp(index),
            },
        );
        Ok(())
    }

    fn schedule_scale_down(&mut self, change: ScaleChange) -> Result<(), PlantError> {
        let ordinal = self.reserve_scale_schedule()?;
        self.scale_down_generation = self.scale_down_generation.wrapping_add(1);
        let generation = self.scale_down_generation;
        let mut cohorts = self.take_cohort_storage();
        for cohort in &mut cohorts {
            cohort.count = 0;
        }
        let down = ScaleDown {
            target: change.replicas,
            apply_micros: change.at_micros,
            generation,
        };
        self.pending_actuation = PendingActuation::Down {
            down,
            inactive_up_storage: cohorts,
        };
        heap_push(
            &mut self.heap,
            Scheduled {
                at_micros: change.at_micros,
                ordinal,
                kind: ScheduledKind::ScaleDown(generation),
            },
        );
        Ok(())
    }

    fn apply_scale_up(&mut self, cohort: u32, now_micros: u64) {
        let cohort = cohort as usize;
        let PendingActuation::Up(cohorts) = &mut self.pending_actuation else {
            return;
        };
        let count = cohorts[cohort].count;
        if count == 0 {
            return;
        }
        cohorts[cohort].count = 0;
        let converged = cohort_replica_count(cohorts) == 0;
        let previous = self.replicas;
        let replicas = previous.saturating_add(count);
        assert!(
            replicas >= previous,
            "an up cohort must not decrease ready replicas"
        );
        self.apply_scale(replicas, cohort as u32, now_micros);
        if converged {
            let cohorts = self.take_cohort_storage();
            self.pending_actuation = PendingActuation::Converged(cohorts);
        }
        self.assert_scale_invariants();
    }

    fn apply_scale_down(&mut self, generation: u32, now_micros: u64) {
        let PendingActuation::Down { down, .. } = &self.pending_actuation else {
            return;
        };
        let pending = *down;
        if pending.generation != generation || pending.apply_micros != now_micros {
            return;
        }
        let cohorts = self.take_cohort_storage();
        self.pending_actuation = PendingActuation::Converged(cohorts);
        assert!(
            pending.target < self.replicas,
            "a down must lower ready replicas"
        );
        self.apply_scale(pending.target, generation, now_micros);
        self.assert_scale_invariants();
    }

    fn assert_scale_invariants(&self) {
        match &self.pending_actuation {
            PendingActuation::Converged(cohorts) => {
                assert_eq!(
                    cohort_replica_count(cohorts),
                    0,
                    "converged actuation must not contain an active cohort"
                );
                assert_eq!(
                    self.replicas, self.desired_replicas,
                    "converged actuation must reach the published count"
                );
            }
            PendingActuation::Up(cohorts) => {
                assert!(
                    self.desired_replicas >= self.replicas,
                    "up actuation must not publish below the ready count"
                );
                assert!(
                    self.replicas.saturating_add(cohort_replica_count(cohorts))
                        <= self.desired_replicas,
                    "ready and in-flight replicas must not exceed the published count"
                );
            }
            PendingActuation::Down {
                down,
                inactive_up_storage,
            } => {
                assert_eq!(
                    cohort_replica_count(inactive_up_storage),
                    0,
                    "down actuation must not contain an active up cohort"
                );
                assert_eq!(
                    self.desired_replicas, down.target,
                    "down actuation must apply the published count"
                );
                assert!(
                    down.target < self.replicas,
                    "down actuation must terminate ready replicas"
                );
            }
        }
    }

    /// Samples actuator delay and schedules one desired replica change.
    ///
    /// # Errors
    ///
    /// Returns an error for zero replicas or a full change buffer.
    pub fn add_scale_request(
        &mut self,
        request: ScaleRequest,
        delays_micros: &QuantileTable,
        random: &mut RandomStream,
    ) -> Result<Actuation, PlantError> {
        let ready_micros = request
            .at_micros
            .saturating_add(delays_micros.sample(random));
        self.replace_scale_target(ScaleChange {
            at_micros: ready_micros,
            replicas: request.replicas,
        })?;
        Ok(Actuation {
            request,
            ready_micros,
        })
    }

    /// Advances scheduled work through one inclusive virtual-time boundary.
    #[must_use]
    pub fn advance_until(&mut self, until_micros: u64) -> PlantSnapshot {
        if !self.started {
            self.seed_heap();
            self.started = true;
        }
        while self
            .heap
            .first()
            .is_some_and(|scheduled| scheduled.at_micros <= until_micros)
        {
            let Some(scheduled) = heap_pop(&mut self.heap) else {
                break;
            };
            self.advance_clock(scheduled.at_micros);
            match scheduled.kind {
                ScheduledKind::Arrival(event) => self.enqueue(event),
                ScheduledKind::AttemptDone(event) => {
                    self.finish_attempt(event, scheduled.at_micros);
                }
                ScheduledKind::RetryReady(event) => {
                    if let AttemptState::Backoff(wait) = self.attempt_state[event as usize] {
                        if wait == RetryWait::Inline {
                            let key = self.events[event as usize].key as usize;
                            self.key_active[key] = true;
                        }
                        self.attempt_state[event as usize] =
                            AttemptState::Ready(DemandClass::Failure);
                    }
                }
                ScheduledKind::DependencyDone => {
                    self.active_dependency_operations =
                        self.active_dependency_operations.saturating_sub(1);
                }
                ScheduledKind::ScaleUp(cohort) => {
                    self.apply_scale_up(cohort, scheduled.at_micros);
                }
                ScheduledKind::ScaleDown(generation) => {
                    self.apply_scale_down(generation, scheduled.at_micros);
                }
                ScheduledKind::ReconcileStart { partition, epoch } => {
                    self.start_reconciliation(partition, epoch);
                }
                ScheduledKind::ReconcileReady { partition, epoch } => {
                    self.finish_reconciliation(partition, epoch, scheduled.at_micros);
                }
            }
            self.dispatch(scheduled.at_micros);
        }
        self.advance_clock(until_micros);
        self.snapshot(until_micros)
    }

    /// Runs all events to settlement with virtual time.
    #[must_use]
    pub fn run(mut self) -> SimulationResult {
        let _ = self.advance_until(u64::MAX);
        SimulationResult {
            events: self.events,
            settlements: self.settlements,
            changes: self.applied_changes,
            initial_replicas: self.initial_replicas,
            slots_per_replica: self.configuration.slots_per_replica,
            dependency_slots: self.configuration.dependency_slots,
        }
    }

    /// Writes observable Normal backlog counts by partition.
    ///
    /// A deferred event leaves this backlog after its retry timer persists.
    ///
    /// # Errors
    ///
    /// Returns an error when the output does not match the partition count.
    pub fn write_partition_normal_backlog(
        &self,
        at_micros: u64,
        output: &mut [u32],
    ) -> Result<(), PlantError> {
        if output.len() != self.partition_reconciliation.len() {
            return Err(PlantError::PartitionCount);
        }
        output.fill(0);
        for (event_index, event) in self.events.iter().enumerate() {
            if event.release_micros <= at_micros
                && !self.settled_by_event[event_index]
                && self.retry_mode_by_event[event_index] == RetryMode::Inline
            {
                let partition = event.partition as usize;
                output[partition] = output[partition].saturating_add(1);
            }
        }
        Ok(())
    }

    /// Writes the oldest observable Normal release by partition.
    ///
    /// A zero value means that the partition has no released backlog.
    ///
    /// # Errors
    ///
    /// Returns an error when the output does not match the partition count.
    pub fn write_partition_normal_oldest_release(
        &self,
        at_micros: u64,
        output: &mut [u64],
    ) -> Result<(), PlantError> {
        if output.len() != self.partition_reconciliation.len() {
            return Err(PlantError::PartitionCount);
        }
        output.fill(u64::MAX);
        for (event_index, event) in self.events.iter().enumerate() {
            if event.release_micros <= at_micros
                && !self.settled_by_event[event_index]
                && self.retry_mode_by_event[event_index] == RetryMode::Inline
            {
                let oldest = &mut output[event.partition as usize];
                *oldest = (*oldest).min(event.release_micros);
            }
        }
        for oldest in output {
            if *oldest == u64::MAX {
                *oldest = 0;
            }
        }
        Ok(())
    }

    /// Writes known deferred Failure backlog counts by partition.
    ///
    /// Running attempts do not enter this backlog.
    ///
    /// # Errors
    ///
    /// Returns an error when the output does not match the partition count.
    pub fn write_partition_failure_backlog(&self, output: &mut [u32]) -> Result<(), PlantError> {
        if output.len() != self.partition_reconciliation.len() {
            return Err(PlantError::PartitionCount);
        }
        output.fill(0);
        for (event_index, event) in self.events.iter().enumerate() {
            let queued = matches!(
                self.attempt_state[event_index],
                AttemptState::Backoff(RetryWait::Deferred)
                    | AttemptState::Ready(DemandClass::Failure)
            );
            if self.retry_mode_by_event[event_index] == RetryMode::Deferred && queued {
                let partition = event.partition as usize;
                output[partition] = output[partition].saturating_add(1);
            }
        }
        Ok(())
    }

    /// Writes the earliest known deferred Failure release by partition.
    ///
    /// A zero value means that the partition has no known Failure backlog.
    ///
    /// # Errors
    ///
    /// Returns an error when the output does not match the partition count.
    pub fn write_partition_failure_release(&self, output: &mut [u64]) -> Result<(), PlantError> {
        if output.len() != self.partition_reconciliation.len() {
            return Err(PlantError::PartitionCount);
        }
        output.fill(u64::MAX);
        for (event_index, event) in self.events.iter().enumerate() {
            let queued = matches!(
                self.attempt_state[event_index],
                AttemptState::Backoff(RetryWait::Deferred)
                    | AttemptState::Ready(DemandClass::Failure)
            );
            if self.retry_mode_by_event[event_index] == RetryMode::Deferred && queued {
                let release = &mut output[event.partition as usize];
                *release = (*release).min(self.retry_ready_micros[event_index]);
            }
        }
        for release in output {
            if *release == u64::MAX {
                *release = 0;
            }
        }
        Ok(())
    }

    /// Returns settlements completed through the current virtual time.
    #[must_use]
    pub fn completed_settlements(&self) -> &[Settlement] {
        &self.settlements
    }

    /// Returns exact handler-slot transitions through the current time.
    #[must_use]
    pub fn attempt_transitions(&self) -> &[AttemptTransition] {
        &self.attempt_transitions
    }

    fn seed_heap(&mut self) {
        for event in 0..self.events.len() {
            let scheduled = Scheduled {
                at_micros: self.events[event].release_micros,
                ordinal: event as u32,
                kind: ScheduledKind::Arrival(event as u32),
            };
            heap_push(&mut self.heap, scheduled);
        }
    }

    fn enqueue(&mut self, event: u32) {
        let key = self.events[event as usize].key as usize;
        let tail = self.key_tail[key];
        if tail == NO_EVENT {
            self.key_head[key] = event;
        } else {
            self.next_by_event[tail as usize] = event;
        }
        self.key_tail[key] = event;
        self.attempt_state[event as usize] = AttemptState::Ready(DemandClass::Normal);
        self.queued_events += 1;
    }

    fn dispatch(&mut self, now_micros: u64) {
        while let Some((event, class)) = self.next_dispatch_candidate() {
            self.start_attempt(event, class, now_micros);
        }
    }

    fn next_dispatch_candidate(&self) -> Option<(u32, DemandClass)> {
        let mut normal = None;
        let mut failure = None;
        for key in 0..self.key_head.len() {
            let event = self.key_head[key];
            if event == NO_EVENT {
                continue;
            }
            let AttemptState::Ready(class) = self.attempt_state[event as usize] else {
                continue;
            };
            if class == DemandClass::Normal && self.key_active[key] {
                continue;
            }
            let partition = self.events[event as usize].partition as usize;
            if matches!(
                self.partition_reconciliation[partition],
                PartitionReconciliation::Paused { .. }
            ) {
                continue;
            }
            let owner = self.partition_owner[partition] as usize;
            if self.active_handlers_by_owner[owner] >= self.configuration.slots_per_replica {
                continue;
            }
            match class {
                DemandClass::Normal if normal.is_none() => normal = Some(event),
                DemandClass::Failure if failure.is_none() => failure = Some(event),
                _ => {}
            }
        }
        match (normal, failure) {
            (Some(normal), Some(_)) if self.prefer_normal() => Some((normal, DemandClass::Normal)),
            (Some(_) | None, Some(failure)) => Some((failure, DemandClass::Failure)),
            (Some(normal), None) => Some((normal, DemandClass::Normal)),
            (None, None) => None,
        }
    }

    fn prefer_normal(&self) -> bool {
        let failure_weight = self.configuration.retry_policy.failure_weight;
        let normal_weight = 1.0_f64 - failure_weight;
        if failure_weight == 0.0_f64 {
            return true;
        }
        if normal_weight == 0.0_f64 {
            return false;
        }
        u64_to_f64(self.normal_service_micros) / normal_weight
            <= u64_to_f64(self.failure_service_micros) / failure_weight
    }

    fn start_attempt(&mut self, event: u32, class: DemandClass, now_micros: u64) {
        let event_index = event as usize;
        let spec = self.events[event_index];
        let key = spec.key as usize;
        let partition = spec.partition as usize;
        let owner = self.partition_owner[partition] as usize;
        self.key_active[key] = true;
        self.active_handlers += 1;
        self.started_attempts = self.started_attempts.saturating_add(1);
        self.attempt_transitions.push(AttemptTransition {
            at_micros: now_micros,
            kind: AttemptTransitionKind::Start,
        });
        self.active_handlers_by_owner[owner] += 1;
        self.partition_active_handlers[partition] += 1;
        self.owner_at_dispatch[event_index] = owner as u32;
        if self.first_dispatch_micros[event_index] == u64::MAX {
            self.first_dispatch_micros[event_index] = now_micros;
            self.in_flight_at_dispatch[event_index] = self.active_handlers;
            self.queue_at_dispatch[event_index] = self.queued_events;
        }
        if self.attempts_by_event[event_index] == 0 {
            self.attempts_by_event[event_index] = 1;
        }
        self.attempt_state[event_index] = AttemptState::Running(class);
        self.attempt_started_micros[event_index] = now_micros;
        let finish = self.attempt_finish(event, now_micros);
        heap_push(
            &mut self.heap,
            Scheduled {
                at_micros: finish,
                ordinal: event,
                kind: ScheduledKind::AttemptDone(event),
            },
        );
    }

    fn attempt_finish(&mut self, event: u32, now_micros: u64) -> u64 {
        let event_index = event as usize;
        let spec = self.events[event_index];
        self.active_dependency_operations = self
            .active_dependency_operations
            .saturating_add(u32::from(spec.dependency_operations > 0));
        let attempt = self.attempt_model.calculate(AttemptFrame {
            now_micros,
            event_index: event,
            attempt: self.attempts_by_event[event_index],
            replicas: self.replicas,
            active_handlers: self.active_handlers,
            dependency_concurrency: self.active_dependency_operations,
            queued_events: self.queued_events,
        });
        let dependency_operation_micros = attempt.dependency_operation_micros;
        let dependency_micros =
            dependency_operation_micros.saturating_mul(u64::from(spec.dependency_operations));
        let dependency_finish = now_micros.saturating_add(dependency_micros);
        if spec.dependency_operations > 0 {
            heap_push(
                &mut self.heap,
                Scheduled {
                    at_micros: dependency_finish,
                    ordinal: event,
                    kind: ScheduledKind::DependencyDone,
                },
            );
        }
        self.dependency_micros[event_index] =
            self.dependency_micros[event_index].saturating_add(dependency_micros);
        let handler_micros = spec
            .handler_micros
            .saturating_add(attempt.handler_added_micros);
        self.handler_micros[event_index] =
            self.handler_micros[event_index].saturating_add(handler_micros);
        dependency_finish.saturating_add(handler_micros)
    }

    fn finish_attempt(&mut self, event: u32, now_micros: u64) {
        let event_index = event as usize;
        let spec = self.events[event_index];
        let AttemptState::Running(class) = self.attempt_state[event_index] else {
            return;
        };
        self.finish_running_attempt(event, class, now_micros);
        self.completed_attempts = self.completed_attempts.saturating_add(1);
        self.attempt_transitions.push(AttemptTransition {
            at_micros: now_micros,
            kind: AttemptTransitionKind::Completion,
        });
        let retry = match spec.outcome {
            EventOutcome::Final(_) => None,
            EventOutcome::Retry {
                outcome,
                count,
                final_outcome,
            } => Some((outcome, count, final_outcome)),
        };
        match class {
            DemandClass::Normal => {
                self.normal_attempts = self.normal_attempts.saturating_add(1);
            }
            DemandClass::Failure => {
                self.failure_attempts = self.failure_attempts.saturating_add(1);
            }
        }
        if let Some((outcome, count, final_outcome)) = retry {
            self.finish_retry(event, class, now_micros, outcome, count, final_outcome);
            return;
        }
        self.settle_final(event, class, now_micros, spec);
    }

    fn finish_retry(
        &mut self,
        event: u32,
        class: DemandClass,
        now_micros: u64,
        outcome: RetryOutcome,
        count: RetryCount,
        final_outcome: FinalOutcome,
    ) {
        let event_index = event as usize;
        let spec = self.events[event_index];
        match (class, outcome) {
            (DemandClass::Normal, RetryOutcome::Transient) => {
                self.normal_transient_failures = self.normal_transient_failures.saturating_add(1);
            }
            (DemandClass::Normal, RetryOutcome::Terminal) => {
                self.normal_terminal_failures = self.normal_terminal_failures.saturating_add(1);
            }
            (DemandClass::Failure, RetryOutcome::Transient) => {
                self.failure_transient_failures = self.failure_transient_failures.saturating_add(1);
            }
            (DemandClass::Failure, RetryOutcome::Terminal) => {
                self.failure_terminal_failures = self.failure_terminal_failures.saturating_add(1);
            }
        }
        let defer = self.retry_mode_by_event[event_index] == RetryMode::Deferred
            || self.should_defer(now_micros);
        self.record_attempt_outcome(now_micros, AttemptResult::Failure);
        self.events[event_index].outcome =
            count
                .after_one()
                .map_or(EventOutcome::Final(final_outcome), |count| {
                    EventOutcome::Retry {
                        outcome,
                        count,
                        final_outcome,
                    }
                });
        self.attempts_by_event[event_index] += 1;
        let delay = if defer {
            let retry_count = if self.retry_mode_by_event[event_index] == RetryMode::Deferred {
                self.deferred_retry_count[event_index].saturating_add(1)
            } else {
                0
            };
            self.retry_mode_by_event[event_index] = RetryMode::Deferred;
            self.deferred_retry_count[event_index] = retry_count;
            self.key_active[spec.key as usize] = false;
            self.deferred_retry_delay(event, retry_count)
        } else {
            self.inline_retry_delay(event, self.attempts_by_event[event_index].saturating_sub(1))
        };
        let wait = if defer {
            RetryWait::Deferred
        } else {
            RetryWait::Inline
        };
        self.attempt_state[event_index] = AttemptState::Backoff(wait);
        self.retry_ready_micros[event_index] = now_micros.saturating_add(delay);
        heap_push(
            &mut self.heap,
            Scheduled {
                at_micros: self.retry_ready_micros[event_index],
                ordinal: event,
                kind: ScheduledKind::RetryReady(event),
            },
        );
    }

    fn settle_final(&mut self, event: u32, class: DemandClass, now_micros: u64, spec: EventSpec) {
        let event_index = event as usize;
        let final_outcome = spec.outcome.final_outcome();
        self.record_attempt_outcome(
            now_micros,
            match final_outcome {
                FinalOutcome::Success => AttemptResult::Success,
                FinalOutcome::PermanentFailure => AttemptResult::Failure,
            },
        );
        if matches!(final_outcome, FinalOutcome::PermanentFailure) {
            match class {
                DemandClass::Normal => {
                    self.normal_permanent_failures =
                        self.normal_permanent_failures.saturating_add(1);
                }
                DemandClass::Failure => {
                    self.failure_permanent_failures =
                        self.failure_permanent_failures.saturating_add(1);
                }
            }
        } else {
            match class {
                DemandClass::Normal => {
                    self.normal_successes = self.normal_successes.saturating_add(1);
                }
                DemandClass::Failure => {
                    self.failure_successes = self.failure_successes.saturating_add(1);
                }
            }
        }
        self.attempt_state[event_index] = AttemptState::Settled;
        let key = spec.key as usize;
        self.settlements.push(Settlement {
            event,
            release_micros: spec.release_micros,
            settle_micros: now_micros,
            attempts: self.attempts_by_event[event_index],
            permit_wait_micros: self.first_dispatch_micros[event_index]
                .saturating_sub(spec.release_micros),
            dependency_micros: self.dependency_micros[event_index],
            handler_micros: self.handler_micros[event_index],
            in_flight_at_dispatch: self.in_flight_at_dispatch[event_index],
            queue_at_dispatch: self.queue_at_dispatch[event_index],
            final_outcome,
        });
        self.useful_completions = self
            .useful_completions
            .saturating_add(u32::from(matches!(final_outcome, FinalOutcome::Success)));
        self.settled_by_event[event_index] = true;
        self.key_active[key] = false;
        self.remove_head(key);
    }

    fn finish_running_attempt(&mut self, event: u32, class: DemandClass, now_micros: u64) {
        let event_index = event as usize;
        let elapsed = now_micros.saturating_sub(self.attempt_started_micros[event_index]);
        match class {
            DemandClass::Normal => {
                self.normal_service_micros = self.normal_service_micros.saturating_add(elapsed);
            }
            DemandClass::Failure => {
                self.failure_service_micros = self.failure_service_micros.saturating_add(elapsed);
            }
        }
        self.active_handlers = self.active_handlers.saturating_sub(1);
        let spec = self.events[event_index];
        let partition = spec.partition as usize;
        let owner = self.owner_at_dispatch[event_index] as usize;
        self.active_handlers_by_owner[owner] =
            self.active_handlers_by_owner[owner].saturating_sub(1);
        self.partition_active_handlers[partition] =
            self.partition_active_handlers[partition].saturating_sub(1);
        self.complete_reconciliation_if_ready(partition, now_micros);
    }

    fn should_defer(&mut self, now_micros: u64) -> bool {
        let cutoff =
            now_micros.saturating_sub(self.configuration.retry_policy.failure_window_micros);
        while self
            .attempt_outcomes
            .front()
            .is_some_and(|outcome| outcome.at_micros < cutoff)
        {
            self.attempt_outcomes.pop_front();
        }
        let failures = self
            .attempt_outcomes
            .iter()
            .filter(|outcome| outcome.result == AttemptResult::Failure)
            .count();
        let rate = if self.attempt_outcomes.is_empty() {
            0.0_f64
        } else {
            let failures = u32::try_from(failures).map_or(u32::MAX, |value| value);
            let attempts =
                u32::try_from(self.attempt_outcomes.len()).map_or(u32::MAX, |value| value);
            f64::from(failures) / f64::from(attempts)
        };
        rate < self.configuration.retry_policy.defer_threshold
    }

    fn record_attempt_outcome(&mut self, at_micros: u64, result: AttemptResult) {
        self.attempt_outcomes
            .push_back(AttemptOutcome { at_micros, result });
    }

    fn inline_retry_delay(&self, event: u32, attempt: u32) -> u64 {
        let policy = self.configuration.retry_policy;
        let upper = policy
            .inline_base_micros
            .saturating_mul(2_u64.saturating_pow(attempt))
            .min(policy.inline_max_micros);
        full_jitter(policy.seed, event, attempt, 0, upper)
    }

    fn deferred_retry_delay(&self, event: u32, retry_count: u32) -> u64 {
        if retry_count == 0 {
            return 0;
        }
        let policy = self.configuration.retry_policy;
        let upper_micros = policy
            .deferred_base_micros
            .saturating_mul(2_u64.saturating_pow(retry_count - 1))
            .min(policy.deferred_max_micros)
            .max(1_000_000);
        let upper_seconds = upper_micros / 1_000_000;
        bounded_random(policy.seed, event, retry_count, 1, upper_seconds)
            .saturating_add(1)
            .saturating_mul(1_000_000)
    }

    fn remove_head(&mut self, key: usize) {
        let event = self.key_head[key];
        if event == NO_EVENT {
            return;
        }
        self.key_head[key] = self.next_by_event[event as usize];
        self.queued_events -= 1;
        self.next_by_event[event as usize] = NO_EVENT;
        if self.key_head[key] == NO_EVENT {
            self.key_tail[key] = NO_EVENT;
        }
    }

    fn apply_scale(&mut self, replicas: u32, ordinal: u32, now_micros: u64) {
        let applied = ScaleChange {
            at_micros: now_micros,
            replicas,
        };
        self.replicas = replicas;
        self.applied_changes.push(applied);
        sticky_assignment(
            &self.partition_owner,
            applied.replicas,
            &mut self.partition_target_owner,
            &mut self.assignment_counts,
        );
        self.reconciliation_started_micros = None;
        self.reconciliation_completed_micros = None;
        let mut moved = 0_u32;
        for partition in 0..self.partition_owner.len() {
            self.partition_epoch[partition] = self.partition_epoch[partition].wrapping_add(1);
            let epoch = self.partition_epoch[partition];
            if self.partition_owner[partition] == self.partition_target_owner[partition] {
                self.partition_reconciliation[partition] = PartitionReconciliation::Serving;
                continue;
            }
            moved = moved.saturating_add(1);
            let timing = self
                .configuration
                .rebalance
                .sample(ordinal, partition as u32);
            let pause_micros = now_micros.saturating_add(timing.notification);
            let ready_micros = pause_micros
                .saturating_add(timing.revocation)
                .saturating_add(timing.assignment)
                .saturating_add(timing.warmup);
            let reconciliation = PartitionReconciliation::Scheduled {
                target_owner: self.partition_target_owner[partition],
                ready_micros,
            };
            self.partition_reconciliation[partition] = if pause_micros == now_micros {
                reconciliation.pause()
            } else {
                reconciliation
            };
            heap_push(
                &mut self.heap,
                Scheduled {
                    at_micros: pause_micros,
                    ordinal: partition as u32,
                    kind: ScheduledKind::ReconcileStart {
                        partition: partition as u32,
                        epoch,
                    },
                },
            );
            heap_push(
                &mut self.heap,
                Scheduled {
                    at_micros: ready_micros,
                    ordinal: partition as u32,
                    kind: ScheduledKind::ReconcileReady {
                        partition: partition as u32,
                        epoch,
                    },
                },
            );
        }
        if moved == 0 {
            self.reconciliation_completed_micros = Some(now_micros);
        }
    }

    fn start_reconciliation(&mut self, partition: u32, epoch: u32) {
        let partition = partition as usize;
        if self.partition_epoch[partition] == epoch {
            self.partition_reconciliation[partition] =
                self.partition_reconciliation[partition].pause();
            self.reconciliation_started_micros = Some(
                self.reconciliation_started_micros
                    .map_or(self.now_micros, |started| started.min(self.now_micros)),
            );
        }
    }

    fn complete_reconciliation_if_ready(&mut self, partition: usize, now_micros: u64) {
        let PartitionReconciliation::Paused {
            target_owner,
            ready_micros,
            ..
        } = self.partition_reconciliation[partition]
        else {
            return;
        };
        if self.partition_active_handlers[partition] == 0 && ready_micros <= now_micros {
            self.partition_owner[partition] = target_owner;
            self.partition_reconciliation[partition] = PartitionReconciliation::Serving;
            if self
                .partition_reconciliation
                .iter()
                .all(|state| matches!(state, PartitionReconciliation::Serving))
            {
                self.reconciliation_completed_micros = Some(now_micros);
            }
        }
    }

    fn finish_reconciliation(&mut self, partition: u32, epoch: u32, now_micros: u64) {
        let partition = partition as usize;
        if self.partition_epoch[partition] == epoch {
            self.complete_reconciliation_if_ready(partition, now_micros);
        }
    }

    fn advance_clock(&mut self, now_micros: u64) {
        let elapsed = now_micros.saturating_sub(self.now_micros);
        self.handler_occupancy_micros = self
            .handler_occupancy_micros
            .saturating_add(elapsed.saturating_mul(u64::from(self.active_handlers)));
        if self
            .partition_reconciliation
            .iter()
            .any(|state| matches!(state, PartitionReconciliation::Paused { .. }))
        {
            self.rebalance_pause_micros = self.rebalance_pause_micros.saturating_add(elapsed);
        }
        self.now_micros = self.now_micros.max(now_micros);
    }

    fn snapshot(&self, at_micros: u64) -> PlantSnapshot {
        let mut released = 0_u32;
        let mut settled = 0_u32;
        for (event_index, event) in self.events.iter().enumerate() {
            released = released.saturating_add(u32::from(event.release_micros <= at_micros));
            settled = settled.saturating_add(u32::from(
                event.release_micros <= at_micros && self.settled_by_event[event_index],
            ));
        }
        let reconciling_partitions = self
            .partition_reconciliation
            .iter()
            .filter(|state| !matches!(state, PartitionReconciliation::Serving))
            .count() as u32;
        let paused_partitions = self
            .partition_reconciliation
            .iter()
            .filter(|state| matches!(state, PartitionReconciliation::Paused { .. }))
            .count() as u32;
        PlantSnapshot {
            at_micros,
            replicas: self.replicas,
            released,
            settled,
            backlog: released.saturating_sub(settled),
            active_handlers: self.active_handlers,
            handler_occupancy_micros: self.handler_occupancy_micros,
            useful_completions: self.useful_completions,
            completed_attempts: self.completed_attempts,
            started_attempts: self.started_attempts,
            attempt_transition_count: self.attempt_transitions.len(),
            rebalance_pause_micros: self.rebalance_pause_micros,
            normal_attempts: self.normal_attempts,
            normal_successes: self.normal_successes,
            normal_transient_failures: self.normal_transient_failures,
            normal_terminal_failures: self.normal_terminal_failures,
            normal_permanent_failures: self.normal_permanent_failures,
            failure_attempts: self.failure_attempts,
            failure_successes: self.failure_successes,
            failure_transient_failures: self.failure_transient_failures,
            failure_terminal_failures: self.failure_terminal_failures,
            failure_permanent_failures: self.failure_permanent_failures,
            partitions_ready: self
                .partition_reconciliation
                .iter()
                .all(|state| matches!(state, PartitionReconciliation::Serving)),
            reconciling_partitions,
            paused_partitions,
            reconciliation_started_micros: self.reconciliation_started_micros,
            reconciliation_completed_micros: self.reconciliation_completed_micros,
        }
    }
}

/// Runs independent plants in parallel and preserves their input order.
#[must_use]
pub fn run_parallel(plants: Vec<Plant>) -> Vec<SimulationResult> {
    plants.into_par_iter().map(Plant::run).collect()
}

/// Completed deterministic trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationResult {
    events: Vec<EventSpec>,
    settlements: Vec<Settlement>,
    changes: Vec<ScaleChange>,
    initial_replicas: u32,
    slots_per_replica: u32,
    dependency_slots: u32,
}

/// Service state for one partition during KIP-848 reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionReconciliation {
    Serving,
    Scheduled {
        target_owner: u32,
        ready_micros: u64,
    },
    Paused {
        target_owner: u32,
        ready_micros: u64,
    },
}

impl PartitionReconciliation {
    const fn pause(self) -> Self {
        match self {
            Self::Scheduled {
                target_owner,
                ready_micros,
            } => Self::Paused {
                target_owner,
                ready_micros,
            },
            state => state,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Scheduled {
    at_micros: u64,
    ordinal: u32,
    kind: ScheduledKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScheduledKind {
    Arrival(u32),
    AttemptDone(u32),
    RetryReady(u32),
    DependencyDone,
    ScaleUp(u32),
    ScaleDown(u32),
    ReconcileStart { partition: u32, epoch: u32 },
    ReconcileReady { partition: u32, epoch: u32 },
}

fn full_jitter(seed: u64, event: u32, attempt: u32, mode: u64, upper: u64) -> u64 {
    if upper == 0 {
        return 0;
    }
    bounded_random(seed, event, attempt, mode, upper)
}

fn bounded_random(seed: u64, event: u32, attempt: u32, mode: u64, bound: u64) -> u64 {
    if bound == 0 {
        return 0;
    }
    let domain = (u64::from(event) << 32_u32) ^ u64::from(attempt) ^ mode.rotate_left(17);
    let mut random = RandomStream::new(seed).domain(domain);
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let product = u128::from(random.next_u64()) * u128::from(bound);
        if product as u64 >= threshold {
            return (product >> 64) as u64;
        }
    }
}

fn heap_push(heap: &mut Vec<Scheduled>, value: Scheduled) {
    assert!(
        heap.len() < heap.capacity(),
        "the event heap exceeded its fixed bound"
    );
    heap.push(value);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        if schedule_key(heap[parent]) <= schedule_key(heap[child]) {
            break;
        }
        heap.swap(parent, child);
        child = parent;
    }
}

fn heap_pop(heap: &mut Vec<Scheduled>) -> Option<Scheduled> {
    if heap.is_empty() {
        return None;
    }
    let root = heap[0];
    let final_index = heap.len() - 1;
    let final_value = heap[final_index];
    heap.truncate(final_index);
    if !heap.is_empty() {
        heap[0] = final_value;
        heap_sift_down(heap);
    }
    Some(root)
}

fn heap_sift_down(heap: &mut [Scheduled]) {
    let mut parent = 0_usize;
    loop {
        let left = parent * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let child = if right < heap.len() && schedule_key(heap[right]) < schedule_key(heap[left]) {
            right
        } else {
            left
        };
        if schedule_key(heap[parent]) <= schedule_key(heap[child]) {
            break;
        }
        heap.swap(parent, child);
        parent = child;
    }
}

fn schedule_key(value: Scheduled) -> (u64, u8, u32) {
    let phase = match value.kind {
        ScheduledKind::DependencyDone => 0,
        ScheduledKind::ScaleUp(_)
        | ScheduledKind::ScaleDown(_)
        | ScheduledKind::ReconcileStart { .. } => 1,
        ScheduledKind::AttemptDone(_)
        | ScheduledKind::RetryReady(_)
        | ScheduledKind::ReconcileReady { .. } => 2,
        ScheduledKind::Arrival(_) => 3,
    };
    (value.at_micros, phase, value.ordinal)
}

fn initial_assignment(partition_count: usize, replicas: u32) -> Vec<u32> {
    let owner_count = replicas.min(partition_count as u32);
    (0..partition_count)
        .map(|partition| partition as u32 % owner_count)
        .collect()
}

/// Keeps valid owners and moves only partitions needed for a balanced target.
fn sticky_assignment(current: &[u32], replicas: u32, target: &mut [u32], counts: &mut [u32]) {
    let owner_count = replicas.min(current.len() as u32);
    counts.fill(0);
    for (partition, &owner) in current.iter().enumerate() {
        if owner < owner_count {
            target[partition] = owner;
            counts[owner as usize] += 1;
        } else {
            target[partition] = NO_EVENT;
        }
    }

    let base = current.len() as u32 / owner_count;
    let remainder = current.len() as u32 % owner_count;
    for owner in 0..owner_count {
        let desired = base + u32::from(owner < remainder);
        let mut excess = counts[owner as usize].saturating_sub(desired);
        if excess == 0 {
            continue;
        }
        for target_owner in target.iter_mut().rev() {
            if excess == 0 {
                break;
            }
            if *target_owner == owner {
                *target_owner = NO_EVENT;
                counts[owner as usize] -= 1;
                excess -= 1;
            }
        }
    }

    let mut owner = 0_u32;
    for target_owner in target {
        if *target_owner != NO_EVENT {
            continue;
        }
        while owner < owner_count {
            let desired = base + u32::from(owner < remainder);
            if counts[owner as usize] < desired {
                break;
            }
            owner += 1;
        }
        assert!(owner < owner_count, "a balanced owner must exist");
        *target_owner = owner;
        counts[owner as usize] += 1;
    }
}

fn validate_positive(value: u32, name: &'static str) -> Result<(), PlantError> {
    if value == 0 {
        return Err(PlantError::ZeroBound { name });
    }
    Ok(())
}

fn to_usize(value: u32) -> Result<usize, PlantError> {
    usize::try_from(value).map_err(|_| PlantError::PlatformLimit)
}

/// Converts an integer with the same rounding as the primitive `u64` to `f64`
/// conversion.
fn u64_to_f64(value: u64) -> f64 {
    let bytes = value.to_le_bytes();
    let low = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let high = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

/// Invalid plant input or capacity.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum PlantError {
    /// A simulator input table is invalid.
    #[error(transparent)]
    Input(#[from] InputError),
    /// An event retry count is invalid.
    #[error(transparent)]
    RetryCount(#[from] RetryCountError),
    /// A generated controller observation is invalid.
    #[error(transparent)]
    ControllerObservation(#[from] prosody_scale_core::ObservationError),
    /// A reconstructed controller state is invalid.
    #[error(transparent)]
    ControllerConfiguration(#[from] prosody_scale_core::ConfigurationError),
    /// Generated capacity evidence is invalid.
    #[error(transparent)]
    ResourceWindow(#[from] prosody_scale_core::ResourceWindowError),
    /// A posterior output buffer has an invalid length.
    #[error(transparent)]
    Posterior(#[from] prosody_scale_core::PosteriorError),
    /// A decision curve output buffer is invalid.
    #[error(transparent)]
    DecisionCurve(#[from] prosody_scale_core::DecisionCurveError),
    /// A predictive distribution parameter is invalid.
    #[error(transparent)]
    PredictiveDistribution(#[from] PoissonError),
    /// A paired predictive distribution parameter is invalid.
    #[error(transparent)]
    PairedPredictiveDistribution(#[from] BinomialError),
    /// An exact arrival-count prediction failed.
    #[error(transparent)]
    ArrivalPredictive(#[from] prosody_scale_core::ArrivalPredictiveError),
    /// Generated launch or rebalance evidence is invalid.
    #[error(transparent)]
    LaunchEvidence(#[from] prosody_scale_core::LaunchEvidenceError),
    /// A predictive lead-time quantile failed.
    #[error(transparent)]
    PredictiveQuantile(#[from] prosody_scale_core::PredictiveQuantileError),
    /// A fixed bound is zero.
    #[error("{name} must be positive")]
    ZeroBound {
        /// Name of the invalid bound.
        name: &'static str,
    },
    /// A count does not fit this platform.
    #[error("a plant count exceeds this platform's address space")]
    PlatformLimit,
    /// The event buffer is full.
    #[error("the event buffer is full")]
    EventCapacity,
    /// The scale-change buffer is full.
    #[error("the scale-change buffer is full")]
    ChangeCapacity,
    /// The calendar forecast exceeds its simulator bound.
    #[error("the calendar forecast buffer is full")]
    CalendarCapacity,
    /// The scheduled release input exceeds its simulator bound.
    #[error("the scheduled release buffer is full")]
    ScheduledReleaseCapacity,
    /// The live-demand window is outside the simulation window.
    #[error("the workload start must be between the simulation start and workload end")]
    WorkloadWindow,
    /// A new event precedes the plant's current virtual time.
    #[error("an event release cannot precede current plant time")]
    EventTimeRegressed,
    /// An event names an unknown partition.
    #[error("the event partition is outside the configured range")]
    PartitionIndex,
    /// A partition output does not match the plant partition count.
    #[error("a partition output must match the plant partition count")]
    PartitionCount,
    /// An event names an unknown key.
    #[error("the event key is outside the configured range")]
    KeyIndex,
    /// The pending snapshot delivery buffer is full.
    #[error("the pending snapshot delivery buffer is full")]
    DeliveryCapacity,
    /// The metric trace is full.
    #[error("the metric trace is full")]
    MetricCapacity,
}

/// Invalid retry count.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum RetryCountError {
    /// A retry sequence has no retry-producing outcome.
    #[error("a retry count must be positive")]
    Zero,
    /// A retry sequence exceeds the fixed simulator bound.
    #[error("a retry count exceeds the simulator maximum of {maximum}")]
    Bound {
        /// Largest accepted retry count.
        maximum: u8,
    },
}

#[cfg(test)]
mod tests;
