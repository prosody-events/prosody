use std::ops::Deref;
use std::time::{Duration, Instant};

use prosody_scale_core::{
    CalendarArtifactId, CalendarRateSegment, CapacityGrid, CapacityPrior, Configuration,
    DecisionRejection, LaunchPrior, RandomStream, RebalancePrior, ReliabilityPrior,
    ScheduledRelease, ServiceObjective,
};

use crate::harness::TickDrivenAttemptModel;
use crate::series::{
    OutputFunction, RecordedSeries, SeriesContext, SeriesFunction, SeriesHistory, series_graph,
};
use crate::{
    AttemptFrame, AttemptModel, AttemptParameters, CalendarForecastInput, ClosedLoop,
    ClosedLoopError, ConcurrencyLatencyCurve, ControllerSample, ControllerTrace,
    DEFAULT_CONCURRENCY_PER_REPLICA, DEFAULT_FAILURE_WEIGHT, EventContext, EventInputs,
    FaultPattern, MetricTrace, PlantConfiguration, PlantError, PlantSnapshot, ReporterDirective,
    ScaleDirective, ScheduledReleasesInput, SeriesCell, SimulationHarness, SimulationResult,
    TickContext, TickGenerator, TickInputs,
};

const CAPACITY_COLLAPSE_GRID: &[f64] = &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64];
const REPLICA_SECOND_DELAY_RATE: f64 = 3.0_f64;

const EVENT_COUNT: u32 = 2_000;
const HOT_KEY_EVENT_COUNT: u32 = 6_000;
const HOT_PARTITION_EVENT_COUNT: u32 = 60_000;
const SEASONAL_EVENT_COUNT: u32 = 3_000;
const TRANSIENT_EVENT_COUNT: u32 = 36_000;
const REBALANCE_EVENT_COUNT: u32 = 32_500;
const REPLICA_CEILING_EVENT_COUNT: u32 = 230_400;
const CAPACITY_EVENT_COUNT_MAX: u32 = 300_000;
const CAPACITY_RESPONSE_EVENT_COUNT: u32 = 231_000;
const LINEAR_RESPONSE_EVENT_COUNT: u32 = 462_000;
const LINEAR_STEP_MICROS: u64 = 180_000_000;
const LINEAR_RATE_INCREMENT: u32 = 100;
const HISTORY_EVENT_COUNT_MAX: u32 = 240_000;
const CALENDAR_HISTORY_EXPOSURE_SECONDS: u32 = 900;
const IDLE_COST_START_MICROS: u64 = 91_000_000;
const IDLE_DURATION_MICROS: u64 = 240_000_000;
const SHORT_BURST_RELEASE_MICROS: u64 = 120_000_000;
const CALENDAR_PRIOR_SHAPE: f64 = 4.0_f64;
const CALENDAR_PRIOR_RATE_SECONDS: f64 = 0.01_f64;
const CALENDAR_MODEL_PRIOR_PROBABILITY: f64 = 0.5_f64;
const HISTORY_START_MICROS: u64 = 300_000_000;
const HISTORY_END_MICROS: u64 = 420_000_000;
const HISTORY_RUN_END_MICROS: u64 = 480_000_000;
const TIMER_WAVE_RELEASE_MICROS: u64 = 420_000_000;
const TIMER_WAVE_RUN_END_MICROS: u64 = 480_000_000;
const HISTORICAL_MAXIMUM_LEAD_SECONDS: f64 = 90.0_f64;
const HISTORICAL_STEP_DURATION_SECONDS: f64 = 120.0_f64;
const REGIME_PRIOR_TRUST_SECONDS: f64 = 5.0_f64;
const PRIOR_RELEASE_MARGIN_MICROS: u64 =
    (1.5_f64 * 16.0_f64 * REGIME_PRIOR_TRUST_SECONDS * 1_000_000.0_f64) as u64;
/// Claim clock for capacity beliefs, as [`PRIOR_RELEASE_MARGIN_MICROS`] is
/// for arrival beliefs. A cold capacity grid tightens only through accepted
/// windows, and windows probe one operating point at a time, so the
/// pessimistic capacity quantile approaches the truth at window cadence.
/// The transient-failures trace tightens in about 51 s at a one-second
/// report interval; 90 s gives 1.75x headroom. Claims that charge capacity
/// cost or bound capacity-driven targets start their clock here.
const CAPACITY_WARMUP_MICROS: u64 = 90_000_000;

const HISTORICAL_SCHEDULE: &[ScheduleSegment] = &[
    ScheduleSegment::new(0, HISTORY_START_MICROS, 0),
    ScheduleSegment::new(HISTORY_START_MICROS, HISTORY_END_MICROS, 1_000),
    ScheduleSegment::new(HISTORY_END_MICROS, HISTORY_RUN_END_MICROS, 0),
];
const SEASONAL_SCHEDULE: &[ScheduleSegment] = &[
    ScheduleSegment::new(0, 120_000_000, 0),
    ScheduleSegment::new(120_000_000, 121_000_000, 1_000),
    ScheduleSegment::new(121_000_000, 240_000_000, 0),
    ScheduleSegment::new(240_000_000, 241_000_000, 1_000),
    ScheduleSegment::new(241_000_000, 360_000_000, 0),
    ScheduleSegment::new(360_000_000, 361_000_000, 1_000),
    ScheduleSegment::new(361_000_000, 420_000_000, 0),
];

/// A principal deterministic plant regime for plot review.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalRegime {
    /// No work arrives.
    Idle,
    /// Load stays below available handler and dependency capacity.
    ApplicationLimited,
    /// Useful throughput grows with ready handler capacity.
    LinearThroughput,
    /// Dependency throughput stays flat after its concurrency knee.
    FlatPostKnee,
    /// Dependency throughput declines after its concurrency knee.
    DecliningPostKnee,
    /// One burst finishes before new replicas can become ready.
    ShortBurst,
    /// Message demand recurs in regular waves.
    SeasonalWaves,
    /// One partition receives all events.
    HotPartition,
    /// Many timers release at one virtual instant.
    TimerWave,
    /// One serialized key receives all events.
    HotSerializedKey,
    /// Some events fail twice before settlement.
    TransientFailures,
    /// Some events settle as permanent rejections.
    PermanentRejections,
    /// Replica changes repeatedly pause partition ownership.
    RebalanceStorm,
    /// Active requests increase handler service time through contention.
    HandlerContention,
    /// One large backlog has a loose latency objective.
    LooseBudgetBacklog,
    /// Snapshot transport loses, duplicates, delays, and reorders reports.
    SnapshotFaults,
    /// The reporter stops permanently after initial evidence.
    MissingReporter,
    /// A new aggregator starts from the proper prior.
    AggregatorReplacement,
    /// Demand requires more replicas than the configured ceiling.
    ReplicaCeiling,
    /// Current demand matches the historical reference.
    HistoricalMatch,
    /// Current demand exceeds the historical reference.
    HistoricalExceeded,
    /// Current demand stays below the historical reference.
    HistoricalUnder,
    /// No historical reference is available.
    HistoricalMissing,
}

impl PrincipalRegime {
    /// Returns all principal regimes in stable plot order.
    pub const ALL: [Self; 23] = [
        Self::Idle,
        Self::ApplicationLimited,
        Self::LinearThroughput,
        Self::FlatPostKnee,
        Self::DecliningPostKnee,
        Self::ShortBurst,
        Self::SeasonalWaves,
        Self::HotPartition,
        Self::TimerWave,
        Self::HotSerializedKey,
        Self::TransientFailures,
        Self::PermanentRejections,
        Self::RebalanceStorm,
        Self::HandlerContention,
        Self::LooseBudgetBacklog,
        Self::SnapshotFaults,
        Self::MissingReporter,
        Self::AggregatorReplacement,
        Self::ReplicaCeiling,
        Self::HistoricalMatch,
        Self::HistoricalExceeded,
        Self::HistoricalUnder,
        Self::HistoricalMissing,
    ];

    /// Returns the stable artifact name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::ApplicationLimited => "application-limited",
            Self::LinearThroughput => "linear-throughput",
            Self::FlatPostKnee => "flat-post-knee",
            Self::DecliningPostKnee => "declining-post-knee",
            Self::ShortBurst => "short-burst",
            Self::SeasonalWaves => "seasonal-waves",
            Self::HotPartition => "hot-partition",
            Self::TimerWave => "timer-wave",
            Self::HotSerializedKey => "hot-serialized-key",
            Self::TransientFailures => "transient-failures",
            Self::PermanentRejections => "permanent-rejections",
            Self::RebalanceStorm => "rebalance-storm",
            Self::HandlerContention => "handler-contention",
            Self::LooseBudgetBacklog => "loose-budget-backlog",
            Self::SnapshotFaults => "snapshot-faults",
            Self::MissingReporter => "missing-reporter",
            Self::AggregatorReplacement => "aggregator-replacement",
            Self::ReplicaCeiling => "replica-ceiling",
            Self::HistoricalMatch => "historical-match",
            Self::HistoricalExceeded => "historical-exceeded",
            Self::HistoricalUnder => "historical-under",
            Self::HistoricalMissing => "historical-missing",
        }
    }

    /// Returns the declared latency budget in microseconds.
    #[must_use]
    pub const fn budget_micros(self) -> u64 {
        if matches!(self, Self::LooseBudgetBacklog) {
            60_000_000
        } else {
            1_000_000
        }
    }
}

/// Runs one deterministic principal regime through the shared harness.
///
/// # Errors
///
/// Returns an error when a fixed plant bound is invalid or full.
pub fn run_principal_regime(regime: PrincipalRegime) -> Result<PrincipalRun, PrincipalRunError> {
    let definition = PrincipalDefinition::for_regime(regime);
    run_principal_definition(regime, definition, None)
}

/// Runs one seeded principal regime through the shared harness.
///
/// The seed changes stochastic input equations. It does not change controller
/// inference or actuation logic.
///
/// # Errors
///
/// Returns an error when a fixed plant bound is invalid or full.
pub fn run_principal_regime_seeded(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<PrincipalRun, PrincipalRunError> {
    let definition = PrincipalDefinition::for_regime(regime).seeded(seed);
    run_principal_definition(regime, definition, None)
}

/// Runs an extended capacity experiment under controller actuation.
///
/// # Errors
///
/// Returns an error when a fixed plant bound is invalid or full.
pub fn run_capacity_evidence_regime(
    regime: PrincipalRegime,
) -> Result<PrincipalRun, PrincipalRunError> {
    let definition = PrincipalDefinition::capacity_evidence(regime);
    run_principal_definition(regime, definition, None)
}

/// Runs one seeded capacity experiment under controller actuation.
///
/// # Errors
///
/// Returns an error when a fixed plant bound is invalid or full.
pub fn run_capacity_evidence_regime_seeded(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<PrincipalRun, PrincipalRunError> {
    let definition = PrincipalDefinition::capacity_evidence(regime).seeded(seed);
    run_principal_definition(regime, definition, None)
}

pub(crate) fn run_capacity_evidence_regime_seeded_with_sensitivity(
    regime: PrincipalRegime,
    seed: u64,
    sensitivity: CapacitySensitivity,
) -> Result<PrincipalRun, PrincipalRunError> {
    let definition = PrincipalDefinition::capacity_evidence(regime).seeded(seed);
    run_principal_definition(regime, definition, Some(sensitivity))
}

/// Identifies which experiment produced a principal run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegimeExperiment {
    /// The model controls the plant.
    ClosedLoop,
    /// An extended closed-loop run supplies capacity evidence.
    CapacityEvidence,
}

/// Fixed capacity-prior or grid variant for sensitivity experiments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacitySensitivity {
    /// Log-normal prior with a factor-two logarithmic standard deviation.
    NarrowPrior,
    /// Log-normal prior with a factor-four logarithmic standard deviation.
    ReferencePrior,
    /// Log-normal prior with a factor-eight logarithmic standard deviation.
    WidePrior,
    /// Reference prior with a 640 operations-per-second grid ceiling.
    LowerGridCeiling,
    /// Reference prior with a 2,560 operations-per-second grid ceiling.
    HigherGridCeiling,
}

impl CapacitySensitivity {
    /// Returns every sensitivity variant once.
    pub const ALL: [Self; 5] = [
        Self::NarrowPrior,
        Self::ReferencePrior,
        Self::WidePrior,
        Self::LowerGridCeiling,
        Self::HigherGridCeiling,
    ];

    /// Returns the stable artifact name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::NarrowPrior => "narrow-prior",
            Self::ReferencePrior => "reference-prior",
            Self::WidePrior => "wide-prior",
            Self::LowerGridCeiling => "lower-grid-ceiling",
            Self::HigherGridCeiling => "higher-grid-ceiling",
        }
    }
}

/// Verifies the declared physical behavior before report generation.
///
/// # Errors
///
/// Returns an error when the run contradicts its regime definition.
pub fn validate_principal_regime(
    regime: PrincipalRegime,
    experiment: RegimeExperiment,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    let definition = match experiment {
        RegimeExperiment::ClosedLoop => PrincipalDefinition::for_regime(regime),
        RegimeExperiment::CapacityEvidence => PrincipalDefinition::capacity_evidence(regime),
    };
    validate_run_envelope(regime, experiment, definition, run)?;
    match experiment {
        RegimeExperiment::ClosedLoop => {
            validate_closed_loop_stimulus(regime, run)?;
            validate_closed_loop_claim(regime, run)?;
        }
        RegimeExperiment::CapacityEvidence => validate_capacity_evidence(regime, run)?,
    }
    // The coverage bar runs after the claims: it grades the belief's
    // calibration, and a red bar must not hide a regime's claim status.
    validate_capacity_coverage(regime, experiment, run)
}

fn validate_capacity_coverage(
    regime: PrincipalRegime,
    experiment: RegimeExperiment,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    let (capacity_window_count, capacity_covered_count) = capacity_coverage(run);
    if capacity_window_count >= 10 {
        let coverage = count_as_f64(capacity_covered_count) / count_as_f64(capacity_window_count);
        // The bar is one-sided. Under-coverage (ranks in the tails) marks a
        // belief inconsistent with observation — the false-knee collapse
        // scored 0.14 here. Over-coverage is not a defect: the plant
        // completes work deterministically, so a calibrated Poisson
        // predictive centers every rank and approaches full coverage. A
        // two-sided bar would test the plant's dispersion, not the belief.
        require_regime(
            coverage >= 0.65_f64,
            regime,
            experiment,
            "capacity predictive coverage fell below its stated probability",
        )?;
    }
    Ok(())
}

fn validate_closed_loop_stimulus(
    regime: PrincipalRegime,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    let experiment = RegimeExperiment::ClosedLoop;
    let settled_all = run.settlements().len() == run.events().len();
    let condition = match regime {
        PrincipalRegime::Idle => run.events().is_empty(),
        PrincipalRegime::ApplicationLimited => !run.events().is_empty() && settled_all,
        PrincipalRegime::LinearThroughput
        | PrincipalRegime::FlatPostKnee
        | PrincipalRegime::DecliningPostKnee => input_sum(run.inputs(), "message_count") > 100_000,
        PrincipalRegime::ShortBurst => validate_short_burst_stimulus(run),
        PrincipalRegime::SeasonalWaves => validate_seasonal_stimulus(run),
        PrincipalRegime::HotPartition => run.events().iter().all(|event| event.partition == 0),
        PrincipalRegime::TimerWave => {
            run.events().len() == EVENT_COUNT as usize
                && settled_all
                && run.events().iter().all(|event| {
                    event.source == crate::EventSource::Timer
                        && event.release_micros == TIMER_WAVE_RELEASE_MICROS
                })
        }
        PrincipalRegime::HotSerializedKey => {
            run.events().len() == HOT_KEY_EVENT_COUNT as usize
                && run
                    .events()
                    .iter()
                    .all(|event| event.key == 0 && event.partition == 0)
                && run
                    .settlements()
                    .iter()
                    .all(|settlement| settlement.in_flight_at_dispatch <= 1)
        }
        PrincipalRegime::TransientFailures => {
            let settlement_count = run.settlements().len();
            let retry_count = run
                .settlements()
                .iter()
                .filter(|settlement| settlement.attempts > 1)
                .count();
            run.events().len() == TRANSIENT_EVENT_COUNT as usize
                && retry_count.saturating_mul(10) >= settlement_count
        }
        PrincipalRegime::PermanentRejections => run.settlements().iter().any(|settlement| {
            matches!(
                settlement.final_outcome,
                crate::FinalOutcome::PermanentFailure
            )
        }),
        PrincipalRegime::RebalanceStorm => {
            input_distinct_positive_count(run.inputs(), "external_target") >= 2
                && input_sum(run.inputs(), "message_count") == u64::from(REBALANCE_EVENT_COUNT)
        }
        PrincipalRegime::HandlerContention => run
            .settlements()
            .iter()
            .any(|settlement| settlement.handler_micros > 2_000),
        PrincipalRegime::LooseBudgetBacklog | PrincipalRegime::SnapshotFaults => {
            run.events().len() == EVENT_COUNT as usize && settled_all
        }
        PrincipalRegime::MissingReporter => {
            run.events().len() == EVENT_COUNT as usize
                && settled_all
                && controller_samples(run)
                    .any(|sample| sample.reporter == ReporterDirective::Missing)
        }
        PrincipalRegime::AggregatorReplacement => {
            run.events().len() == EVENT_COUNT as usize
                && settled_all
                && controller_samples(run)
                    .any(|sample| sample.reporter == ReporterDirective::ReplaceAggregator)
        }
        PrincipalRegime::ReplicaCeiling => {
            run.events().len() == REPLICA_CEILING_EVENT_COUNT as usize
        }
        PrincipalRegime::HistoricalMatch => {
            let current = input_sum(run.inputs(), "message_count");
            let historical = input_sum(run.inputs(), "historical_message_count");
            current.abs_diff(historical) <= historical.saturating_div(10).max(1)
        }
        PrincipalRegime::HistoricalExceeded => {
            let current = input_sum(run.inputs(), "message_count");
            let historical = input_sum(run.inputs(), "historical_message_count");
            current > historical.saturating_add(historical.saturating_div(2))
        }
        PrincipalRegime::HistoricalUnder => {
            let current = input_sum(run.inputs(), "message_count");
            let historical = input_sum(run.inputs(), "historical_message_count");
            current.saturating_mul(4) < historical.saturating_mul(3)
        }
        PrincipalRegime::HistoricalMissing => {
            input_sum(run.inputs(), "historical_message_count") == 0
        }
    };
    require_regime(
        condition,
        regime,
        experiment,
        "the generated stimulus does not match the regime",
    )
}

fn validate_short_burst_stimulus(run: &PrincipalRun) -> bool {
    run.events().len() == EVENT_COUNT as usize
        && run
            .events()
            .iter()
            .all(|event| event.release_micros == SHORT_BURST_RELEASE_MICROS)
}

fn validate_seasonal_stimulus(run: &PrincipalRun) -> bool {
    let mut release_times = run
        .events()
        .iter()
        .map(|event| event.release_micros)
        .collect::<Vec<_>>();
    release_times.dedup();
    release_times == [120_000_000, 240_000_000, 360_000_000]
        && input_sum(run.inputs(), "historical_message_count") > 0
}

fn validate_closed_loop_claim(
    regime: PrincipalRegime,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    match regime {
        PrincipalRegime::Idle => validate_idle_claim(run),
        PrincipalRegime::ApplicationLimited => validate_application_limited_claim(run),
        PrincipalRegime::LinearThroughput => validate_linear_claim(run),
        PrincipalRegime::FlatPostKnee | PrincipalRegime::DecliningPostKnee => {
            validate_capacity_closed_loop_claim(regime, run)
        }
        PrincipalRegime::ShortBurst => validate_short_burst_claim(run),
        PrincipalRegime::SeasonalWaves => validate_seasonal_claim(run),
        PrincipalRegime::HotPartition => validate_single_worker_claim(
            regime,
            run,
            "the decision did not expose the binding partition-placement loss",
        ),
        PrincipalRegime::TimerWave => validate_timer_wave_claim(run),
        PrincipalRegime::HotSerializedKey => validate_single_worker_claim(
            regime,
            run,
            "the decision did not expose the binding serialized-key loss",
        ),
        PrincipalRegime::TransientFailures => validate_transient_failure_claim(run),
        PrincipalRegime::PermanentRejections => validate_permanent_rejection_claim(run),
        PrincipalRegime::RebalanceStorm => validate_rebalance_storm_claim(run),
        PrincipalRegime::LooseBudgetBacklog => validate_loose_budget_claim(run),
        PrincipalRegime::ReplicaCeiling => validate_replica_ceiling_claim(run),
        PrincipalRegime::HistoricalMatch => validate_historical_match_claim(run),
        PrincipalRegime::HistoricalExceeded => validate_historical_exceeded_claim(run),
        PrincipalRegime::HistoricalUnder => validate_historical_under_claim(run),
        PrincipalRegime::HistoricalMissing => validate_historical_missing_claim(run),
        _ => Ok(()),
    }
}

fn validate_idle_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let cost_duration_seconds =
        Duration::from_micros(run.stop.at_micros.saturating_sub(IDLE_COST_START_MICROS))
            .as_secs_f64();
    require_closed_loop(
        replica_seconds_between(run, IDLE_COST_START_MICROS, run.stop.at_micros)
            <= 1.5_f64 * cost_duration_seconds,
        PrincipalRegime::Idle,
        "idle capacity exceeded its controllable cost budget",
    )?;
    require_closed_loop(
        final_target(run) == Some(1),
        PrincipalRegime::Idle,
        "the idle controller did not finish at one replica",
    )
}

fn validate_application_limited_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    require_closed_loop(
        minimum_cap(run) >= 8,
        PrincipalRegime::ApplicationLimited,
        "uninformative evidence created a false saturation cap",
    )?;
    require_closed_loop(
        final_no_knee_probability(run) >= 0.25_f64,
        PrincipalRegime::ApplicationLimited,
        "uninformative evidence removed the no-knee hypothesis",
    )
}

fn validate_linear_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let accounting = linear_miss_accounting(run);
    require_closed_loop(
        run.settlements().len() == run.events().len(),
        PrincipalRegime::LinearThroughput,
        "the controller did not complete the linear workload",
    )?;
    require_closed_loop(
        accounting.reaction_window_misses <= accounting.reaction_window_allowance,
        PrincipalRegime::LinearThroughput,
        "linear misses exceeded the physical reaction-window allowance",
    )?;
    require_closed_loop(
        accounting.outside_misses.saturating_mul(100) <= accounting.outside_settlements,
        PrincipalRegime::LinearThroughput,
        "linear misses outside reaction windows exceeded the SLO allowance",
    )
}

/// Separates unavoidable step reactions from controller misses.
///
/// A level belief cannot act before an authored rate step. Each reaction
/// window starts at that step. It ends when the run first records enough ready
/// replicas after one report interval. Permit wait plus handler service gives
/// the physical miss allowance in each window. Settlements outside the windows
/// retain the SLO epsilon bound. All values come from the completed trace.
pub(crate) fn linear_miss_accounting(run: &PrincipalRun) -> LinearMissAccounting {
    let report_micros = run
        .controller
        .sample(1)
        .zip(run.controller.sample(0))
        .map_or(0, |(next, first)| {
            next.at_micros.saturating_sub(first.at_micros)
        });
    let mut windows = Vec::new();
    let events = run.events();
    let last_release_micros = events
        .iter()
        .map(|event| event.release_micros)
        .max()
        .unwrap_or(0);
    let mut step = 1_u32;
    loop {
        let release_micros = u64::from(step).saturating_mul(LINEAR_STEP_MICROS);
        if release_micros > last_release_micros {
            break;
        }
        if let Some(event) = events
            .iter()
            .find(|event| event.release_micros >= release_micros)
        {
            let rate = step.saturating_add(1).saturating_mul(LINEAR_RATE_INCREMENT);
            let handler_micros = event.handler_micros;
            let work_micros = u64::from(rate).saturating_mul(handler_micros);
            let replica_micros = u64::from(run.simulation.slots_per_replica) * 1_000_000;
            let required =
                work_micros.saturating_add(replica_micros.saturating_sub(1)) / replica_micros;
            let required = u32::try_from(required)
                .map_or(u32::MAX, |value| value)
                .max(1);
            let earliest_ready = release_micros.saturating_add(report_micros);
            if let Some(change) =
                run.simulation.changes.iter().find(|change| {
                    change.at_micros >= earliest_ready && change.replicas >= required
                })
            {
                windows.push((release_micros, change.at_micros));
            }
        }
        step = step.saturating_add(1);
    }
    let budget_micros = PrincipalRegime::LinearThroughput.budget_micros();
    let mut accounting = LinearMissAccounting::default();
    for settlement in run.settlements() {
        let in_reaction_window = windows
            .iter()
            .any(|&(start, end)| (start..end).contains(&settlement.release_micros));
        let missed = settlement
            .settle_micros
            .saturating_sub(settlement.release_micros)
            > budget_micros;
        if in_reaction_window {
            let physical_miss = settlement
                .permit_wait_micros
                .saturating_add(settlement.handler_micros)
                > budget_micros;
            accounting.reaction_window_allowance += usize::from(physical_miss);
            accounting.reaction_window_misses += usize::from(missed);
        } else {
            accounting.outside_settlements += 1;
            accounting.outside_misses += usize::from(missed);
        }
    }
    accounting
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LinearMissAccounting {
    pub(crate) reaction_window_allowance: usize,
    pub(crate) reaction_window_misses: usize,
    pub(crate) outside_settlements: usize,
    pub(crate) outside_misses: usize,
}

fn validate_capacity_closed_loop_claim(
    regime: PrincipalRegime,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    // The controller reacts within one second after demand exceeds capacity.
    // The first accepted window above the knee starts the evidence clock.
    // Identification then completes during the bounded clamp traversal.
    const DEMAND_STEP_MICROS: u64 = 270_000_000;
    const REACTION_LIMIT_MICROS: u64 = 5_000_000;
    const ENGAGEMENT_LIMIT_MICROS: u64 = 20_000_000;
    const KNEE_CONCURRENCY: u32 = 64;
    let pre_step_target = controller_samples(run)
        .take_while(|sample| sample.at_micros <= DEMAND_STEP_MICROS)
        .last()
        .map(|sample| sample.target);
    let reacted = pre_step_target.is_some_and(|pre_step| {
        controller_samples(run).any(|sample| {
            sample.at_micros > DEMAND_STEP_MICROS
                && sample.at_micros <= DEMAND_STEP_MICROS + REACTION_LIMIT_MICROS
                && sample.target > pre_step
        })
    });
    require_closed_loop(
        reacted,
        regime,
        "the capacity target did not react to the demand step",
    )?;
    let evidence_at = controller_samples(run)
        .find(|sample| {
            matches!(
                sample.capacity_evidence,
                crate::CapacityEvidenceSample::Window(window)
                    if window.concurrency > f64::from(KNEE_CONCURRENCY)
                        && window.completed_attempts > 0
            )
        })
        .map(|sample| sample.at_micros);
    require_closed_loop(
        evidence_at.is_some(),
        regime,
        "the plant produced no resource evidence above the knee",
    )?;
    let engaged = evidence_at.is_some_and(|evidence_at| {
        controller_samples(run).any(|sample| {
            sample.at_micros >= evidence_at
                && sample.at_micros <= evidence_at + ENGAGEMENT_LIMIT_MICROS
                && sample.cap <= 4
        })
    });
    require_closed_loop(
        engaged,
        regime,
        "the capacity cap did not engage after evidence became available",
    )?;
    if regime == PrincipalRegime::FlatPostKnee {
        return require_closed_loop(
            final_target(run).is_some_and(|target| (2..=3).contains(&target)),
            regime,
            "the capacity controller did not finish at its settled target",
        );
    }
    require_closed_loop(
        final_target(run).is_some_and(|target| target <= 3),
        regime,
        "the declining capacity controller finished above its target bound",
    )
}

/// A change point restarts the release clock. After the burst drains,
/// the honest posterior holds one insurance replica while the learned
/// hazard decays, and δ paces the release. The claim bounds the
/// insurance level at that δ-quantile (target ≤ 2) and requires the
/// release to complete within the derived margin.
fn validate_short_burst_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    const DRAIN_EXEMPTION_END_MICROS: u64 = SHORT_BURST_RELEASE_MICROS + 5_000_000;
    const RELEASE_CEILING_MICROS: u64 = SHORT_BURST_RELEASE_MICROS + PRIOR_RELEASE_MARGIN_MICROS;
    let bounded_insurance = controller_samples(run).all(|sample| {
        sample.at_micros < PRIOR_RELEASE_MARGIN_MICROS
            || (SHORT_BURST_RELEASE_MICROS..DRAIN_EXEMPTION_END_MICROS).contains(&sample.at_micros)
            || sample.target <= 2
    });
    let released = controller_samples(run)
        .filter(|sample| sample.at_micros >= RELEASE_CEILING_MICROS)
        .all(|sample| sample.target == 1);
    require_closed_loop(
        bounded_insurance,
        PrincipalRegime::ShortBurst,
        "the controller held more than one insurance replica after the burst drained",
    )?;
    require_closed_loop(
        released,
        PrincipalRegime::ShortBurst,
        "the controller held insurance past the derived release margin",
    )?;
    require_closed_loop(
        final_target(run) == Some(1),
        PrincipalRegime::ShortBurst,
        "the short-burst controller did not finish at one replica",
    )?;
    require_closed_loop(
        run.settlements().iter().all(|settlement| {
            settlement
                .settle_micros
                .saturating_sub(settlement.release_micros)
                < 30_000_000
        }),
        PrincipalRegime::ShortBurst,
        "a short-burst settlement exceeded the reactive drain bound",
    )
}

fn validate_seasonal_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let minimum_between_waves = controller_samples(run)
        .filter(|sample| (120_000_000..240_000_000).contains(&sample.at_micros))
        .map(|sample| sample.target)
        .min();
    require_closed_loop(
        minimum_between_waves.is_some_and(|target| target <= 2),
        PrincipalRegime::SeasonalWaves,
        "the controller did not return to idle capacity between seasonal waves",
    )?;
    require_closed_loop(
        release_window_miss_fraction(
            run,
            240_000_000,
            u64::MAX,
            PrincipalRegime::SeasonalWaves.budget_micros(),
        ) <= 0.01_f64,
        PrincipalRegime::SeasonalWaves,
        "the controller missed the SLO for the forecast seasonal waves",
    )
}

fn validate_single_worker_claim(
    regime: PrincipalRegime,
    run: &PrincipalRun,
    invariant: &'static str,
) -> Result<(), RegimeValidationError> {
    require_closed_loop(single_worker_constraint_binds(run), regime, invariant)
}

fn validate_timer_wave_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let minimum_before_wave = controller_samples(run)
        .filter(|sample| (IDLE_DURATION_MICROS..270_000_000).contains(&sample.at_micros))
        .map(|sample| sample.target)
        .min();
    require_closed_loop(
        minimum_before_wave.is_some_and(|target| target <= 2),
        PrincipalRegime::TimerWave,
        "the controller did not establish an idle baseline before the timer wave",
    )?;
    require_closed_loop(
        slo_miss_fraction(run, PrincipalRegime::TimerWave.budget_micros()) <= 0.01_f64,
        PrincipalRegime::TimerWave,
        "the controller missed the SLO for the known timer wave",
    )
}

fn validate_transient_failure_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    // The peak bound starts at the capacity warm-up: before the belief
    // tightens, the cost can provision against the pessimistic prior capacity
    // quantile. A standing army still fails at the first seasoned sample.
    let seasoned_peak = controller_samples(run)
        .filter(|sample| sample.at_micros >= CAPACITY_WARMUP_MICROS)
        .map(|sample| sample.target)
        .max();
    require_closed_loop(
        seasoned_peak.is_some_and(|target| target <= 12),
        PrincipalRegime::TransientFailures,
        "retry demand caused excessive replica growth",
    )?;
    let attempts = run
        .settlements()
        .iter()
        .map(|settlement| settlement.attempts)
        .sum::<u32>();
    require_closed_loop(
        attempts == 43_200,
        PrincipalRegime::TransientFailures,
        "the retry schedule did not produce its declared attempt count",
    )
}

fn validate_permanent_rejection_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let rejections = run
        .settlements()
        .iter()
        .filter(|settlement| {
            matches!(
                settlement.final_outcome,
                crate::FinalOutcome::PermanentFailure
            )
        })
        .count();
    require_closed_loop(
        rejections == 200,
        PrincipalRegime::PermanentRejections,
        "the rejection schedule did not produce its declared permanent count",
    )
}

fn validate_rebalance_storm_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    const CALM_START_MICROS: u64 = 5_000_000;
    require_closed_loop(
        target_change_count(run, CALM_START_MICROS) <= 2,
        PrincipalRegime::RebalanceStorm,
        "the controller churned during the calm rebalance tail",
    )?;
    // The cap claim measures the calm tail, where the cap can bind a
    // decision. During the storm the first ambiguous onset windows let the
    // knee ridge briefly outweigh the no-knee cells. This is an honest
    // posterior transient at one operating point. External actions drive
    // every transition. A pause-taught standing false knee still fails
    // because it persists into the calm tail. The rank clamp bounds the
    // transient by ln(1 / p) per window, and recovery follows at that rate.
    let calm_minimum_cap = controller_samples(run)
        .filter(|sample| sample.at_micros >= CALM_START_MICROS)
        .map(|sample| sample.cap)
        .min();
    require_closed_loop(
        calm_minimum_cap.is_some_and(|cap| cap >= 2),
        PrincipalRegime::RebalanceStorm,
        "rebalance evidence created a false saturation cap",
    )?;
    // The run stays at one operating point after onset. The data cannot
    // separate no-knee cells from knees above this point. Thus, the honest
    // posterior recovers to an even split. This bound rejects a standing
    // false knee without requiring certainty that the data cannot provide.
    require_closed_loop(
        final_no_knee_probability(run) >= 0.45_f64,
        PrincipalRegime::RebalanceStorm,
        "rebalance evidence left a standing false knee",
    )
}

/// The run starts at eight replicas, so the descent to one replica pays
/// the plant's launch delay before it can land. The cost claim charges
/// the controller only for what it controls: the descent must be
/// requested within the drain window, it must land, and capacity after
/// the landing stays within the clear-cost slack.
fn validate_loose_budget_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    // Drain window: 2,000 events at eight replicas of 32 slots each,
    // plus five report intervals of filter settling.
    const DRAIN_END_MICROS: u64 = 1_000_000 + (EVENT_COUNT as u64) * 1_000_000 / (8 * 32);
    const REQUEST_DEADLINE_MICROS: u64 = DRAIN_END_MICROS + 5_000_000;
    let clear_seconds = f64::from(EVENT_COUNT) / 32.0_f64;
    require_closed_loop(
        run.settlements().len() == run.events().len()
            && slo_miss_count(run, PrincipalRegime::LooseBudgetBacklog.budget_micros()) == 0,
        PrincipalRegime::LooseBudgetBacklog,
        "the controller did not clear the backlog within the loose SLO",
    )?;
    require_closed_loop(
        controller_samples(run)
            .any(|sample| sample.at_micros <= REQUEST_DEADLINE_MICROS && sample.target == 1),
        PrincipalRegime::LooseBudgetBacklog,
        "the controller did not request the descent within the drain window",
    )?;
    let descent_landing_micros = run
        .simulation
        .changes
        .iter()
        .find(|change| change.replicas < run.simulation.initial_replicas)
        .map(|change| change.at_micros);
    let Some(landing_micros) = descent_landing_micros else {
        return Err(RegimeValidationError::Failed {
            regime: PrincipalRegime::LooseBudgetBacklog,
            experiment: RegimeExperiment::ClosedLoop,
            invariant: "the requested descent never landed",
        });
    };
    let landed_window_seconds =
        Duration::from_micros(run.stop.at_micros.saturating_sub(landing_micros)).as_secs_f64();
    require_closed_loop(
        replica_seconds_between(run, landing_micros, run.stop.at_micros)
            <= landed_window_seconds + 3.0_f64 * clear_seconds,
        PrincipalRegime::LooseBudgetBacklog,
        "the controller used excessive capacity after the descent landed",
    )?;
    require_closed_loop(
        minimum_cap(run) >= maximum_target(run),
        PrincipalRegime::LooseBudgetBacklog,
        "the saturation cap fell below a selected target",
    )
}

fn validate_replica_ceiling_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let targets_valid = controller_samples(run)
        .filter(|sample| !sample.hold)
        .all(|sample| sample.target <= 8);
    require_closed_loop(
        targets_valid,
        PrincipalRegime::ReplicaCeiling,
        "the controller exceeded the configured replica ceiling",
    )?;
    require_closed_loop(
        controller_samples(run).any(|sample| !sample.hold && sample.target == 8),
        PrincipalRegime::ReplicaCeiling,
        "the controller did not bind at the configured replica ceiling",
    )?;
    // Sustained overload keeps a FIFO backlog, so almost every queued
    // event misses the one-second budget at any policy — a loss-system
    // miss oracle cannot bind here. The testable ceiling invariant is
    // pace: the run settles every event no slower than the ceiling
    // serves (eight replicas at 320 events per second). The 20 %
    // tolerance covers release overlap and partition serialization; a
    // seven-replica run still exceeds it.
    let ceiling_drain_seconds = f64::from(REPLICA_CEILING_EVENT_COUNT) / (8.0_f64 * 320.0_f64);
    let final_settle_seconds = run
        .settlements()
        .last()
        .map_or(f64::INFINITY, |settlement| {
            Duration::from_micros(settlement.settle_micros).as_secs_f64()
        });
    require_closed_loop(
        run.settlements().len() == run.events().len()
            && final_settle_seconds <= 1.2_f64 * ceiling_drain_seconds,
        PrincipalRegime::ReplicaCeiling,
        "the run did not settle the workload at the ceiling's service pace",
    )
}

fn validate_historical_match_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    require_closed_loop(
        slo_miss_fraction(run, PrincipalRegime::HistoricalMatch.budget_micros()) <= 0.01_f64,
        PrincipalRegime::HistoricalMatch,
        "matching history did not keep the workload inside its SLO",
    )
}

fn validate_historical_exceeded_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    let excess_fraction = (2_000.0_f64 - 1_000.0_f64) / 2_000.0_f64;
    let reactive_bound = excess_fraction
        * (HISTORICAL_MAXIMUM_LEAD_SECONDS / HISTORICAL_STEP_DURATION_SECONDS)
        + 0.01_f64;
    require_closed_loop(
        slo_miss_fraction(run, PrincipalRegime::HistoricalExceeded.budget_micros())
            <= reactive_bound,
        PrincipalRegime::HistoricalExceeded,
        "live demand exceeded the reactive historical SLO bound",
    )
}

fn validate_historical_under_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    require_closed_loop(
        slo_miss_fraction(run, PrincipalRegime::HistoricalUnder.budget_micros()) <= 0.01_f64,
        PrincipalRegime::HistoricalUnder,
        "lower live demand did not stay inside its SLO",
    )?;
    let history_cost =
        8.0_f64 * Duration::from_micros(HISTORY_END_MICROS - HISTORY_START_MICROS).as_secs_f64();
    require_closed_loop(
        replica_seconds_between(run, HISTORY_START_MICROS, HISTORY_END_MICROS)
            < 0.8_f64 * history_cost,
        PrincipalRegime::HistoricalUnder,
        "lower live demand did not reduce historical replica cost",
    )
}

fn validate_historical_missing_claim(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    // Down-transition dents and likely repairs make a one-step excursion the
    // priced cost minimum. The target returns to its old bound within tens of
    // seconds.
    let mut seasoned = controller_samples(run).filter(|sample| {
        sample.at_micros >= HISTORY_START_MICROS.saturating_add(CAPACITY_WARMUP_MICROS)
    });
    let first = seasoned.next();
    let (seasoned_peak, final_seasoned_target) = seasoned.fold(
        first.map_or((0, None), |sample| (sample.target, Some(sample.target))),
        |(peak, _), sample| (peak.max(sample.target), Some(sample.target)),
    );
    require_closed_loop(
        final_seasoned_target.is_some() && seasoned_peak <= 6,
        PrincipalRegime::HistoricalMissing,
        "missing history held an excessive seasoned target",
    )?;
    require_closed_loop(
        final_seasoned_target.is_some_and(|target| target <= 5),
        PrincipalRegime::HistoricalMissing,
        "missing history finished above its seasoned target bound",
    )
}

fn slo_miss_count(run: &PrincipalRun, budget_micros: u64) -> usize {
    run.settlements()
        .iter()
        .filter(|settlement| {
            settlement
                .settle_micros
                .saturating_sub(settlement.release_micros)
                > budget_micros
        })
        .count()
}

fn slo_miss_fraction(run: &PrincipalRun, budget_micros: u64) -> f64 {
    if run.events().is_empty() {
        return 0.0_f64;
    }
    let unsettled = run.events().len().saturating_sub(run.settlements().len());
    count_as_f64(
        u64::try_from(slo_miss_count(run, budget_micros).saturating_add(unsettled))
            .map_or(u64::MAX, |count| count),
    ) / count_as_f64(u64::try_from(run.events().len()).map_or(u64::MAX, |count| count))
}

fn release_window_miss_fraction(
    run: &PrincipalRun,
    start_micros: u64,
    end_micros: u64,
    budget_micros: u64,
) -> f64 {
    let event_count = run
        .events()
        .iter()
        .filter(|event| (start_micros..end_micros).contains(&event.release_micros))
        .count();
    let mut settled_count = 0_usize;
    let mut miss_count = 0_usize;
    for settlement in run
        .settlements()
        .iter()
        .filter(|settlement| (start_micros..end_micros).contains(&settlement.release_micros))
    {
        settled_count = settled_count.saturating_add(1);
        miss_count = miss_count.saturating_add(usize::from(
            settlement
                .settle_micros
                .saturating_sub(settlement.release_micros)
                > budget_micros,
        ));
    }
    if event_count == 0 {
        1.0_f64
    } else {
        let unsettled_count = event_count.saturating_sub(settled_count);
        count_as_f64(
            u64::try_from(miss_count.saturating_add(unsettled_count))
                .map_or(u64::MAX, |count| count),
        ) / count_as_f64(u64::try_from(event_count).map_or(u64::MAX, |count| count))
    }
}

fn replica_seconds_between(run: &PrincipalRun, start_micros: u64, end_micros: u64) -> f64 {
    let mut replicas = run.simulation.initial_replicas;
    let mut cursor = 0_u64;
    let mut area = 0.0_f64;
    for change in &run.simulation.changes {
        if change.at_micros >= end_micros {
            break;
        }
        if change.at_micros > start_micros {
            let interval_start = cursor.max(start_micros);
            area += f64::from(replicas)
                * Duration::from_micros(change.at_micros - interval_start).as_secs_f64();
        }
        cursor = change.at_micros;
        replicas = change.replicas;
    }
    let interval_start = cursor.max(start_micros);
    area + f64::from(replicas)
        * Duration::from_micros(end_micros.saturating_sub(interval_start)).as_secs_f64()
}

fn controller_samples(run: &PrincipalRun) -> impl Iterator<Item = ControllerSample> + '_ {
    (0..run.controller.len()).filter_map(|index| run.controller.sample(index))
}

fn final_target(run: &PrincipalRun) -> Option<u32> {
    controller_samples(run).last().map(|sample| sample.target)
}

fn minimum_cap(run: &PrincipalRun) -> u32 {
    controller_samples(run)
        .map(|sample| sample.cap)
        .min()
        .map_or(0, |cap| cap)
}

fn maximum_target(run: &PrincipalRun) -> u32 {
    controller_samples(run)
        .map(|sample| sample.target)
        .max()
        .map_or(0, |target| target)
}

fn final_no_knee_probability(run: &PrincipalRun) -> f64 {
    controller_samples(run)
        .last()
        .map_or(f64::NAN, |sample| sample.no_knee_probability)
}

fn target_change_count(run: &PrincipalRun, start_micros: u64) -> usize {
    let mut prior = None;
    let mut changes = 0_usize;
    for sample in controller_samples(run).filter(|sample| sample.at_micros >= start_micros) {
        if prior.is_some_and(|target| target != sample.target) {
            changes = changes.saturating_add(1);
        }
        prior = Some(sample.target);
    }
    changes
}

fn require_closed_loop(
    condition: bool,
    regime: PrincipalRegime,
    invariant: &'static str,
) -> Result<(), RegimeValidationError> {
    require_regime(condition, regime, RegimeExperiment::ClosedLoop, invariant)
}

fn capacity_coverage(run: &PrincipalRun) -> (u64, u64) {
    let mut windows = 0_u64;
    let mut covered = 0_u64;
    for sample in controller_samples(run) {
        if !matches!(
            sample.capacity_evidence,
            crate::CapacityEvidenceSample::Window(_)
        ) || !sample.capacity_predictive_rank.is_finite()
        {
            continue;
        }
        windows = windows.saturating_add(1);
        covered = covered.saturating_add(u64::from(
            (0.1_f64..=0.9_f64).contains(&sample.capacity_predictive_rank),
        ));
    }
    (windows, covered)
}

fn single_worker_constraint_binds(run: &PrincipalRun) -> bool {
    (0..run.controller.len()).any(|index| {
        let Some(sample) = run.controller.sample(index) else {
            return false;
        };
        let Some(losses) = run.controller.decision_expected_costs(index) else {
            return false;
        };
        let Some(&one_replica) = losses.first() else {
            return false;
        };
        !sample.hold
            && sample.target == 1
            && one_replica > 0.0_f64
            && losses.iter().skip(1).all(|loss| *loss == f64::INFINITY)
    })
}

fn input_sum(history: &SeriesHistory, name: &str) -> u64 {
    (0..history.len())
        .filter_map(|row| match history.cell(name, row) {
            Some(SeriesCell::Unsigned32(value)) => Some(u64::from(value)),
            _ => None,
        })
        .sum()
}

fn input_distinct_positive_count(history: &SeriesHistory, name: &str) -> usize {
    let mut previous = None;
    let mut count = 0_usize;
    for row in 0..history.len() {
        let Some(SeriesCell::Unsigned32(value)) = history.cell(name, row) else {
            continue;
        };
        if value > 0 && previous != Some(value) {
            count += 1;
        }
        previous = Some(value);
    }
    count
}

fn validate_run_envelope(
    regime: PrincipalRegime,
    experiment: RegimeExperiment,
    definition: PrincipalDefinition,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    require_regime(
        run.stop.at_micros <= definition.schedule.maximum_micros,
        regime,
        experiment,
        "the run exceeded its declared maximum duration",
    )?;
    require_regime(
        run.inputs.at_micros(0) == Some(definition.schedule.start_micros),
        regime,
        experiment,
        "the input history does not start with the run",
    )?;
    require_regime(
        run.inputs.at_micros(run.inputs.len().saturating_sub(1)) == Some(run.stop.at_micros),
        regime,
        experiment,
        "the input history does not cover the complete run",
    )?;
    require_regime(
        run.inputs.len() == run.controller.len(),
        regime,
        experiment,
        "the input and controller histories have different lengths",
    )?;
    for index in 0..run.controller.len() {
        let Some(sample) = run.controller.sample(index) else {
            return Err(RegimeValidationError::Failed {
                regime,
                experiment,
                invariant: "a controller sample is missing",
            });
        };
        require_regime(
            sample.target > 0 && sample.cap > 0 && sample.target <= sample.cap,
            regime,
            experiment,
            "a controller sample has an invalid target or cap",
        )?;
    }
    let has_external_target = (0..run.inputs.len()).any(|row| {
        matches!(
            run.inputs.cell("external_target", row),
            Some(SeriesCell::Unsigned32(target)) if target > 0
        )
    });
    let expects_external_target = regime == PrincipalRegime::RebalanceStorm;
    require_regime(
        has_external_target == expects_external_target,
        regime,
        experiment,
        "external interventions do not match the experiment",
    )
}

fn validate_capacity_evidence(
    regime: PrincipalRegime,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    let experiment = RegimeExperiment::CapacityEvidence;
    let window_count = run
        .controller
        .capacity_evidence_count(crate::CapacityEvidenceKind::Window);
    require_regime(
        window_count >= 30,
        regime,
        experiment,
        "the controller produced fewer than 30 passive resource windows",
    )?;
    let mut concurrency_min = f64::INFINITY;
    let mut concurrency_max = 0.0_f64;
    let mut throughput_min = f64::INFINITY;
    let mut throughput_max = 0.0_f64;
    for index in 0..run.controller.len() {
        let Some(sample) = run.controller.sample(index) else {
            continue;
        };
        let crate::CapacityEvidenceSample::Window(window) = sample.capacity_evidence else {
            continue;
        };
        let rate = window.throughput_per_second();
        concurrency_min = concurrency_min.min(window.concurrency);
        concurrency_max = concurrency_max.max(window.concurrency);
        throughput_min = throughput_min.min(rate);
        throughput_max = throughput_max.max(rate);
    }
    require_regime(
        concurrency_min.is_finite()
            && concurrency_max > concurrency_min
            && throughput_min.is_finite()
            && throughput_max > throughput_min,
        regime,
        experiment,
        "passive windows did not cover concurrency and throughput ranges",
    )?;
    if matches!(
        regime,
        PrincipalRegime::FlatPostKnee | PrincipalRegime::DecliningPostKnee
    ) {
        let knee = 64.0_f64;
        require_regime(
            concurrency_min < knee && concurrency_max > knee,
            regime,
            experiment,
            "passive windows did not cross the physical concurrency knee",
        )?;
    }
    match regime {
        PrincipalRegime::LinearThroughput => require_regime(
            final_no_knee_probability(run) >= 0.95_f64,
            regime,
            experiment,
            "the linear capacity belief lost its no-knee mass",
        )?,
        PrincipalRegime::FlatPostKnee => {
            require_regime(
                final_no_knee_probability(run) <= 0.01_f64,
                regime,
                experiment,
                "the flat capacity belief retained excess no-knee mass",
            )?;
            require_regime(
                controller_samples(run)
                    .last()
                    .is_some_and(|sample| sample.cap == 2),
                regime,
                experiment,
                "the flat capacity run did not finish with a cap of two",
            )?;
        }
        PrincipalRegime::DecliningPostKnee => {
            require_regime(
                capacity_evidence_has_no_gap(run, 5),
                regime,
                experiment,
                "the declining capacity evidence has a gap longer than five ticks",
            )?;
            validate_declining_capacity_evidence(run)?;
        }
        _ => {}
    }
    Ok(())
}

fn capacity_evidence_has_no_gap(run: &PrincipalRun, maximum_gap: usize) -> bool {
    let mut first_window = None;
    let mut previous_window = None;
    for (index, sample) in controller_samples(run).enumerate() {
        if !matches!(
            sample.capacity_evidence,
            crate::CapacityEvidenceSample::Window(_)
        ) {
            continue;
        }
        first_window.get_or_insert(index);
        if previous_window.is_some_and(|previous| index.saturating_sub(previous) > maximum_gap) {
            return false;
        }
        previous_window = Some(index);
    }
    first_window.is_some()
        && previous_window.is_some_and(|previous| {
            run.controller
                .len()
                .saturating_sub(1)
                .saturating_sub(previous)
                <= maximum_gap
        })
}

fn validate_declining_capacity_evidence(run: &PrincipalRun) -> Result<(), RegimeValidationError> {
    const KNEE_CONCURRENCY: f64 = 64.0_f64;
    const BELIEF_DEADLINE_MICROS: u64 = 10_000_000;
    const CAP_DEADLINE_MICROS: u64 = 60_000_000;
    let regime = PrincipalRegime::DecliningPostKnee;
    let experiment = RegimeExperiment::CapacityEvidence;
    let first_collapse = controller_samples(run)
        .enumerate()
        .find_map(|(index, sample)| {
            matches!(
                sample.capacity_evidence,
                crate::CapacityEvidenceSample::Window(window)
                    if window.concurrency > KNEE_CONCURRENCY
            )
            .then_some((index, sample.at_micros))
        });
    // Below the knee, covering cells hold their prior odds. The posterior must
    // retain no-knee mass until the plant supplies separating evidence.
    require_regime(
        first_collapse.is_some_and(|(index, _)| {
            controller_samples(run)
                .take(index)
                .all(|sample| sample.no_knee_probability >= 0.25_f64)
        }),
        regime,
        experiment,
        "the declining no-knee probability lost mass before the knee crossing",
    )?;
    let first_below = first_collapse.and_then(|(index, collapse_micros)| {
        controller_samples(run)
            .enumerate()
            .skip(index)
            .find(|(_, sample)| sample.no_knee_probability < 0.40_f64)
            .map(|(fall_index, sample)| (fall_index, collapse_micros, sample.at_micros))
    });
    // Above the knee, the declining curve separates from the no-knee family.
    // The posterior must respond within the fixed evidence deadline.
    require_regime(
        first_below.is_some_and(|(_, collapse_micros, fall_micros)| {
            fall_micros <= collapse_micros.saturating_add(BELIEF_DEADLINE_MICROS)
        }),
        regime,
        experiment,
        "the declining no-knee probability did not fall after the knee crossing",
    )?;
    // Separating evidence must keep the no-knee probability below its bound.
    require_regime(
        first_below.is_some_and(|(index, ..)| {
            controller_samples(run)
                .skip(index)
                .all(|sample| sample.no_knee_probability < 0.40_f64)
        }),
        regime,
        experiment,
        "the declining no-knee probability recovered after its post-knee fall",
    )?;
    let cap_engagement = controller_samples(run).position(|sample| sample.cap < 128);
    require_regime(
        first_collapse
            .map(|(_, collapse_micros)| collapse_micros)
            .zip(cap_engagement)
            .is_some_and(|(collapse_micros, cap_index)| {
                run.controller.sample(cap_index).is_some_and(|sample| {
                    sample.at_micros <= collapse_micros.saturating_add(CAP_DEADLINE_MICROS)
                })
            }),
        regime,
        experiment,
        "the declining capacity cap did not engage within 60 seconds",
    )
}

fn require_regime(
    condition: bool,
    regime: PrincipalRegime,
    experiment: RegimeExperiment,
    invariant: &'static str,
) -> Result<(), RegimeValidationError> {
    if condition {
        Ok(())
    } else {
        Err(RegimeValidationError::Failed {
            regime,
            experiment,
            invariant,
        })
    }
}

fn run_principal_definition(
    regime: PrincipalRegime,
    definition: PrincipalDefinition,
    sensitivity: Option<CapacitySensitivity>,
) -> Result<PrincipalRun, PrincipalRunError> {
    let seed = definition.inputs.seed;
    let capacity_regime = is_capacity_regime(regime);
    let slots_per_replica = DEFAULT_CONCURRENCY_PER_REPLICA;
    let plant_configuration = principal_plant_configuration(
        definition.event_count_max,
        slots_per_replica,
        definition.inputs.shared_resource.parallelism,
    )?;
    let graph = principal_graph(
        regime,
        capacity_regime,
        definition,
        slots_per_replica,
        sensitivity,
    )?;
    let attempt_model = PrincipalAttemptModel::new(
        definition.inputs.shared_resource,
        principal_handler_curve(regime)?,
        definition.event_count_max,
    )?;
    let mut harness = SimulationHarness::with_attempt_model(
        plant_configuration,
        definition.initial_replicas,
        64,
        graph,
        attempt_model,
    )?;
    let stop = run_schedule(&mut harness, regime, definition.schedule)?;
    let (simulation, graph) = harness.finish_with_graph();
    let (controller, graph) = graph.into_parts();
    Ok(PrincipalRun {
        simulation,
        controller,
        inputs: graph.inputs.into_series_history(),
        stop,
        seed,
        metric_window_micros: definition.schedule.workload_interval_micros,
    })
}

fn is_capacity_regime(regime: PrincipalRegime) -> bool {
    matches!(
        regime,
        PrincipalRegime::LinearThroughput
            | PrincipalRegime::FlatPostKnee
            | PrincipalRegime::DecliningPostKnee
    )
}

fn principal_plant_configuration(
    event_count_max: u32,
    slots_per_replica: u32,
    shared_resource_parallelism: u32,
) -> Result<PlantConfiguration, PlantError> {
    Ok(PlantConfiguration::new(
        64,
        1_024,
        event_count_max,
        event_count_max,
        slots_per_replica,
        shared_resource_parallelism,
    )?
    .with_metric_poll_interval_micros(1_000_000))
}

fn principal_handler_curve(regime: PrincipalRegime) -> Result<ConcurrencyLatencyCurve, PlantError> {
    if regime == PrincipalRegime::HandlerContention {
        return Ok(ConcurrencyLatencyCurve::new(
            &[0, 32, 64, 128, 256],
            &[0, 5_000, 20_000, 80_000, 250_000],
        )?);
    }
    Ok(ConcurrencyLatencyCurve::new(&[0], &[0])?)
}

fn principal_graph(
    regime: PrincipalRegime,
    capacity_regime: bool,
    definition: PrincipalDefinition,
    slots_per_replica: u32,
    sensitivity: Option<CapacitySensitivity>,
) -> Result<ClosedLoop<PrincipalGraph>, PrincipalRunError> {
    let replica_count_max = if regime == PrincipalRegime::ReplicaCeiling {
        8
    } else {
        128
    };
    let controller_configuration = Configuration {
        cohort_count_max: 64,
        calendar_segment_count_max: 64,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 64,
        replica_count_max,
        slots_per_replica,
        posterior_sample_count: 4_096,
        report_interval_micros: definition
            .schedule
            .workload_interval_micros
            .min(definition.schedule.followup_interval_micros),
        // The plant can retry every event, so one report window can carry
        // every attempt the run can produce. The certified bound comes from
        // that plant contract, never from an authored guess.
        resource_window_attempt_count_max: definition
            .event_count_max
            .saturating_mul(u32::from(crate::MAX_RETRY_FAILURES) + 1),
        failure_service_weight: DEFAULT_FAILURE_WEIGHT,
        arrival_prior: prosody_scale_core::ArrivalPrior::new(
            4.0_f64,
            0.01_f64,
            1.0_f64 / 90.0_f64,
        )?,
        // A collapsing plant changes as work crosses its knee. The kernel
        // revives cells that current data cover. This rate matches the
        // workload change cadence.
        capacity_change_rate_per_second: 1.0_f64 / 300.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(regime.budget_micros(), 0.01, REPLICA_SECOND_DELAY_RATE)?,
    };
    let capacity_grid = capacity_grid(regime, capacity_regime, sensitivity)?;
    let graph = ClosedLoop::new(
        PrincipalGraph::new(definition)?,
        &controller_configuration,
        capacity_grid,
        definition.schedule.controller_sample_count_max()?,
    )?
    .with_diagnostic_seed(definition.inputs.seed);
    match regime {
        PrincipalRegime::SnapshotFaults => Ok(graph.with_snapshot_transport(
            512,
            FaultPattern {
                drop_every: 4,
                duplicate_every: 3,
                delay_micros: 250_000,
                odd_sequence_delay_micros: 500_000,
            },
        )?),
        PrincipalRegime::MissingReporter | PrincipalRegime::AggregatorReplacement => {
            Ok(graph.with_snapshot_transport(256, FaultPattern::default())?)
        }
        _ => Ok(graph),
    }
}

fn capacity_grid(
    regime: PrincipalRegime,
    capacity_regime: bool,
    sensitivity: Option<CapacitySensitivity>,
) -> Result<CapacityGrid, prosody_scale_core::CapacityGridError> {
    let capacity_count = match sensitivity {
        Some(CapacitySensitivity::LowerGridCeiling) => 32,
        Some(CapacitySensitivity::HigherGridCeiling) => 128,
        _ => 64,
    };
    let capacities_per_second = if capacity_regime {
        (1_u32..=capacity_count)
            .map(|value| f64::from(value) * 20.0_f64)
            .collect::<Vec<_>>()
    } else {
        (1_u32..=64)
            .map(|value| f64::from(value) * 2_000.0_f64)
            .collect::<Vec<_>>()
    };
    let historical_regime = matches!(
        regime,
        PrincipalRegime::HistoricalMatch
            | PrincipalRegime::HistoricalExceeded
            | PrincipalRegime::HistoricalUnder
            | PrincipalRegime::HistoricalMissing
    );
    let service_times_seconds: &[f64] = if capacity_regime || historical_regime {
        &[0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64]
    } else {
        // Every step stays within the capacity model's two-octave cell
        // bound. The nine original anchors remain grid points.
        &[
            0.000_5_f64,
            0.001_f64,
            0.002_f64,
            0.005_f64,
            0.01_f64,
            0.03_f64,
            0.1_f64,
            0.3_f64,
            1.0_f64,
            3.0_f64,
            10.0_f64,
            25.0_f64,
            60.0_f64,
            240.0_f64,
            600.0_f64,
        ]
    };
    let prior = if historical_regime {
        CapacityPrior::LogNormal {
            service_time_median_seconds: 0.1_f64,
            capacity_median_per_second: 1_280.0_f64,
            log_standard_deviation: 2.0_f64.ln(),
        }
    } else if capacity_regime {
        sensitivity.map_or(CapacityPrior::LogUniform, |variant| {
            let factor: f64 = match variant {
                CapacitySensitivity::NarrowPrior => 2.0_f64,
                CapacitySensitivity::WidePrior => 8.0_f64,
                CapacitySensitivity::ReferencePrior
                | CapacitySensitivity::LowerGridCeiling
                | CapacitySensitivity::HigherGridCeiling => 4.0_f64,
            };
            CapacityPrior::LogNormal {
                service_time_median_seconds: 0.1_f64,
                capacity_median_per_second: 320.0_f64,
                log_standard_deviation: factor.ln(),
            }
        })
    } else {
        CapacityPrior::LogNormal {
            service_time_median_seconds: 0.002_f64,
            capacity_median_per_second: 64_000.0_f64,
            log_standard_deviation: 100.0_f64.ln(),
        }
    };
    CapacityGrid::new_with_prior(
        service_times_seconds,
        &capacities_per_second,
        CAPACITY_COLLAPSE_GRID,
        prior,
    )
}

struct RunProgress {
    started: Instant,
    progress_started: Instant,
    tick_count: u32,
    progress_tick_count: u32,
    prior_target: Option<u32>,
}

impl RunProgress {
    fn new() -> Self {
        let started = Instant::now();
        Self {
            started,
            progress_started: started,
            tick_count: 0,
            progress_tick_count: 0,
            prior_target: None,
        }
    }

    fn record(
        &mut self,
        harness: &SimulationHarness<ClosedLoop<PrincipalGraph>, PrincipalAttemptModel>,
        snapshot: &PlantSnapshot,
        regime: PrincipalRegime,
        schedule: RunSchedule,
        at_micros: u64,
        tick_count_max: u32,
    ) {
        const INTERVAL: u32 = 25;

        self.tick_count = self.tick_count.saturating_add(1);
        let controller = harness.graph().trace();
        let controller_index = controller.len().saturating_sub(1);
        let controller_sample = controller.sample(controller_index);
        let target_changed =
            controller_sample.is_some_and(|sample| self.prior_target != Some(sample.target));
        if self.tick_count == 1 || self.tick_count.is_multiple_of(INTERVAL) || target_changed {
            let now = Instant::now();
            let elapsed = now.duration_since(self.started).as_secs_f64();
            let progress_ticks = self.tick_count.saturating_sub(self.progress_tick_count);
            let recent_millis = now.duration_since(self.progress_started).as_secs_f64()
                * 1_000.0_f64
                / f64::from(progress_ticks);
            let average_millis = elapsed * 1_000.0_f64 / f64::from(self.tick_count);
            let remaining_ticks = tick_count_max.saturating_sub(self.tick_count);
            let eta_seconds = average_millis * f64::from(remaining_ticks) / 1_000.0_f64;
            let phase = if at_micros < schedule.workload_end_micros {
                "workload"
            } else {
                "followup"
            };
            let ready_index = snapshot.replicas.saturating_sub(1) as usize;
            let next_index = ready_index.saturating_add(1);
            let costs = controller.decision_expected_costs(controller_index);
            let satisfactions =
                controller.decision_deadline_satisfaction_probabilities(controller_index);
            let selected = controller_sample.map_or(0, |sample| sample.target);
            let selected_index = selected.saturating_sub(1) as usize;
            let rejection = |reason| {
                controller
                    .decision_rejection_probabilities(reason, controller_index)
                    .and_then(|values| values.get(selected_index))
                    .copied()
                    .unwrap_or(f64::NAN)
            };
            let arrival_rate =
                controller_sample.map_or(f64::NAN, |sample| sample.arrival_rate_per_second);
            let selected_cost = controller_sample.map_or(f64::NAN, |sample| sample.expected_cost);
            let selected_miss_delay_fraction =
                controller_sample.map_or(f64::NAN, |sample| sample.miss_delay_fraction);
            let scenario_count = controller_sample.map_or(0, |sample| sample.scenario_count);
            tracing::info!(
                regime = regime.name(),
                completed_ticks = self.tick_count,
                maximum_ticks = tick_count_max,
                virtual_seconds = Duration::from_micros(at_micros).as_secs_f64(),
                maximum_virtual_seconds =
                    Duration::from_micros(schedule.maximum_micros).as_secs_f64(),
                wall_elapsed_seconds = elapsed,
                recent_step_millis = recent_millis,
                average_step_millis = average_millis,
                eta_seconds,
                phase,
                actual_replicas = snapshot.replicas,
                desired_replicas = harness.desired_replicas(),
                selected_target = selected,
                backlog = snapshot.backlog,
                inferred_arrival_rate_per_second = arrival_rate,
                selected_expected_cost = selected_cost,
                selected_miss_delay_fraction,
                scenario_count,
                selected_deadline_rejection_probability = rejection(DecisionRejection::Deadline),
                selected_placement_rejection_probability =
                    rejection(DecisionRejection::PartitionPlacement),
                ready_target_deadline_satisfaction_probability = satisfactions
                    .and_then(|values| values.get(ready_index))
                    .copied()
                    .unwrap_or(f64::NAN),
                next_target_deadline_satisfaction_probability = satisfactions
                    .and_then(|values| values.get(next_index))
                    .copied()
                    .unwrap_or(f64::NAN),
                ready_target_expected_cost = costs
                    .and_then(|values| values.get(ready_index))
                    .copied()
                    .unwrap_or(f64::NAN),
                next_target_expected_cost = costs
                    .and_then(|values| values.get(next_index))
                    .copied()
                    .unwrap_or(f64::NAN),
                target_changed,
                "regime progress"
            );
            self.progress_started = now;
            self.progress_tick_count = self.tick_count;
        }
        if let Some(sample) = controller_sample {
            self.prior_target = Some(sample.target);
        }
    }
}

fn run_schedule(
    harness: &mut SimulationHarness<ClosedLoop<PrincipalGraph>, PrincipalAttemptModel>,
    regime: PrincipalRegime,
    schedule: RunSchedule,
) -> Result<RunStop, PrincipalRunError> {
    let mut at_micros = schedule.start_micros;
    let mut stable_count = 0_u8;
    let tick_count_max = schedule.controller_sample_count_max()?;
    let mut progress = RunProgress::new();
    loop {
        let snapshot = harness.tick(at_micros)?;
        progress.record(
            harness,
            &snapshot,
            regime,
            schedule,
            at_micros,
            tick_count_max,
        );
        match schedule.stop {
            StopCondition::IdleStable { sample_count } => {
                let stable = at_micros >= schedule.workload_end_micros
                    && snapshot.backlog == 0
                    && snapshot.replicas == harness.desired_replicas();
                stable_count = if stable {
                    stable_count.saturating_add(1)
                } else {
                    0
                };
                if stable_count >= sample_count {
                    return Ok(RunStop {
                        at_micros,
                        reason: RunStopReason::IdleStable,
                    });
                }
            }
            StopCondition::FixedDuration { reason } if at_micros >= schedule.maximum_micros => {
                return Ok(RunStop { at_micros, reason });
            }
            StopCondition::FixedDuration { .. } => {}
        }
        if at_micros >= schedule.maximum_micros {
            return Err(PrincipalRunError::RunDurationExceeded {
                maximum_micros: schedule.maximum_micros,
            });
        }
        let interval = if at_micros < schedule.workload_end_micros {
            schedule.workload_interval_micros
        } else {
            schedule.followup_interval_micros
        };
        at_micros = at_micros
            .saturating_add(interval)
            .min(schedule.maximum_micros);
    }
}

/// One principal plant result with its controller trace.
pub struct PrincipalRun {
    simulation: SimulationResult,
    controller: ControllerTrace,
    inputs: SeriesHistory,
    stop: RunStop,
    seed: u64,
    metric_window_micros: u64,
}

impl PrincipalRun {
    #[cfg(test)]
    pub(crate) fn applied_changes(&self) -> &[crate::ScaleChange] {
        &self.simulation.changes
    }

    /// Returns the regime interval for metric aggregation.
    #[must_use]
    pub const fn metric_window_micros(&self) -> u64 {
        self.metric_window_micros
    }

    /// Combines plant and controller values into one metric trace.
    ///
    /// # Errors
    ///
    /// Returns an error when the metric capacity is invalid.
    pub fn metric_trace(
        &self,
        window_micros: u64,
        budget_micros: u64,
    ) -> Result<MetricTrace, PlantError> {
        let mut trace = self.simulation.metric_trace_until(
            window_micros,
            budget_micros,
            self.controller.final_micros(),
        )?;
        self.controller.apply_to(&mut trace);
        Ok(trace)
    }

    /// Returns the controller decision trace.
    #[must_use]
    pub const fn controller(&self) -> &ControllerTrace {
        &self.controller
    }

    /// Returns every input series calculated by the regime graph.
    #[must_use]
    pub const fn inputs(&self) -> &SeriesHistory {
        &self.inputs
    }

    /// Returns the exact run duration and stop reason.
    #[must_use]
    pub const fn stop(&self) -> RunStop {
        self.stop
    }

    /// Returns reproducible metadata for this experiment.
    #[must_use]
    pub fn report_metadata(&self) -> crate::ReportMetadata {
        crate::ReportMetadata::new(
            self.controller.artifacts()[0].identity(),
            self.seed,
            self.stop.at_micros,
        )
    }
}

impl Deref for PrincipalRun {
    type Target = SimulationResult;

    fn deref(&self) -> &Self::Target {
        &self.simulation
    }
}

/// Failure while running one principal closed-loop regime.
#[derive(Debug, thiserror::Error)]
pub enum PrincipalRunError {
    /// The capacity grid is invalid.
    #[error(transparent)]
    CapacityGrid(#[from] prosody_scale_core::CapacityGridError),
    /// A transition population prior is invalid.
    #[error(transparent)]
    LeadTimePrior(#[from] prosody_scale_core::LeadTimePriorError),
    /// The arrival population prior is invalid.
    #[error(transparent)]
    ArrivalPrior(#[from] prosody_scale_core::ArrivalPriorError),
    /// The closed-loop controller is invalid.
    #[error(transparent)]
    ClosedLoop(#[from] ClosedLoopError),
    /// The controller configuration is invalid.
    #[error(transparent)]
    Configuration(#[from] prosody_scale_core::ConfigurationError),
    /// The plant input is invalid.
    #[error(transparent)]
    Plant(#[from] PlantError),
    /// The run reached its duration bound before its stop condition.
    #[error("the regime stop condition was not met by {maximum_micros} microseconds")]
    RunDurationExceeded {
        /// Maximum virtual time.
        maximum_micros: u64,
    },
}

/// A principal run contradicted its declared regime.
#[derive(Debug, thiserror::Error)]
pub enum RegimeValidationError {
    /// One declared invariant did not hold.
    #[error("{regime:?} {experiment:?} failed: {invariant}")]
    Failed {
        /// Regime under test.
        regime: PrincipalRegime,
        /// Experiment under test.
        experiment: RegimeExperiment,
        /// Failed invariant.
        invariant: &'static str,
    },
}

/// Reason that ended one regime run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStopReason {
    /// Work drained and replica actuation became stable.
    IdleStable,
    /// The declared closed-loop duration completed.
    DurationComplete,
}

/// Exact duration and reason for one completed regime run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunStop {
    /// Final virtual time.
    pub at_micros: u64,
    /// Condition that ended the run.
    pub reason: RunStopReason,
}

struct PrincipalAttemptModel {
    graph: PrincipalAttemptGraph,
}

impl PrincipalAttemptModel {
    fn new(
        resource: SharedResourcePolicy,
        handler_curve: ConcurrencyLatencyCurve,
        history_count_max: u32,
    ) -> Result<Self, PlantError> {
        Ok(Self {
            graph: PrincipalAttemptGraph::new(resource, handler_curve, history_count_max)?,
        })
    }
}

impl AttemptModel for PrincipalAttemptModel {
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters {
        self.graph.evaluate(frame.now_micros, frame)
    }
}

impl TickDrivenAttemptModel for PrincipalAttemptModel {
    fn update(&mut self, _: TickInputs) {}
}

series_graph! {
    struct PrincipalAttemptGraph(AttemptFrame) with (
        resource: SharedResourcePolicy,
        handler_curve: ConcurrencyLatencyCurve,
    ) {
        series resource_capacity: u32 ["shared resource capacity", Count, Input] =
            AttemptResourceCapacity(resource) => ();
        series resource_base_micros: u64 ["shared resource base time", Microseconds, State] =
            AttemptResourceBaseTime(resource.parallelism) => (resource_capacity);
        series resource_load: u32 ["shared resource offered concurrency", Count, Input] =
            AttemptResourceLoad {} => ();
        series dependency_operation_micros: u64 ["shared resource operation time", Microseconds, State] =
            AttemptResourceLatency(resource.parallelism, resource.collapse) =>
            (resource_base_micros, resource_load);
        series handler_added_micros: u64 ["handler contention", Microseconds, Input] =
            AttemptHandlerContention(handler_curve) => ();
        output output: AttemptParameters = AttemptOutput {} =>
            (dependency_operation_micros, handler_added_micros);
    }
}

struct AttemptResourceCapacity(SharedResourcePolicy);

impl SeriesFunction<AttemptFrame, ()> for AttemptResourceCapacity {
    type Output = u32;

    fn calculate(&self, _: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.capacity_per_second
    }
}

struct AttemptResourceBaseTime(u32);

impl SeriesFunction<AttemptFrame, (u32,)> for AttemptResourceBaseTime {
    type Output = u64;

    fn calculate(
        &self,
        _: SeriesContext<'_, AttemptFrame>,
        (capacity_per_second,): (u32,),
    ) -> Self::Output {
        u64::from(self.0)
            .saturating_mul(1_000_000)
            .div_ceil(u64::from(capacity_per_second.max(1)))
    }
}

struct AttemptResourceLoad;

impl SeriesFunction<AttemptFrame, ()> for AttemptResourceLoad {
    type Output = u32;

    fn calculate(&self, context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        context.frame.dependency_concurrency
    }
}

struct AttemptResourceLatency(u32, u32);

impl SeriesFunction<AttemptFrame, (u64, u32)> for AttemptResourceLatency {
    type Output = u64;

    fn calculate(
        &self,
        _: SeriesContext<'_, AttemptFrame>,
        (base_micros, offered_concurrency): (u64, u32),
    ) -> Self::Output {
        overloaded_operation_micros(base_micros, self.0, offered_concurrency, self.1)
    }
}

/// Converts one declared throughput curve into wall-clock operation duration.
fn overloaded_operation_micros(
    base_micros: u64,
    knee_concurrency: u32,
    offered_concurrency: u32,
    collapse: u32,
) -> u64 {
    if offered_concurrency <= knee_concurrency {
        return base_micros;
    }
    let knee = u128::from(knee_concurrency).max(1);
    let excess = u128::from(offered_concurrency.saturating_sub(knee_concurrency));
    let knee_squared = knee.saturating_pow(2);
    let collapse_factor =
        knee_squared.saturating_add(u128::from(collapse).saturating_mul(excess.saturating_pow(2)));
    let numerator = u128::from(offered_concurrency).saturating_mul(collapse_factor);
    let denominator = knee.saturating_mul(knee_squared);
    let scaled = u128::from(base_micros)
        .saturating_mul(numerator)
        .div_ceil(denominator);
    u64::try_from(scaled).map_or(u64::MAX, |value| value)
}

struct AttemptHandlerContention(ConcurrencyLatencyCurve);

impl SeriesFunction<AttemptFrame, ()> for AttemptHandlerContention {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.added_micros(context.frame.active_handlers)
    }
}

struct AttemptOutput;

impl OutputFunction<AttemptFrame, (u64, u64)> for AttemptOutput {
    type Output = AttemptParameters;

    fn calculate(
        &self,
        _: SeriesContext<'_, AttemptFrame>,
        (dependency_operation_micros, handler_added_micros): (u64, u64),
    ) -> Self::Output {
        AttemptParameters {
            dependency_operation_micros,
            handler_added_micros,
        }
    }
}

struct PrincipalGraph {
    messages: ArrivalSchedule,
    timers: ArrivalSchedule,
    historical_messages: ArrivalSchedule,
    events: EventPolicy,
    reporter: ReporterPolicy,
    calendar: HistoricalSeries,
    inputs: PrincipalInputGraph,
}

impl PrincipalGraph {
    fn new(definition: PrincipalDefinition) -> Result<Self, PlantError> {
        if definition.schedule.start_micros > definition.schedule.workload_start_micros
            || definition.schedule.workload_start_micros > definition.schedule.workload_end_micros
        {
            return Err(PlantError::WorkloadWindow);
        }
        let history_count_max = definition.schedule.controller_sample_count_max()?;
        let inputs = definition.inputs;
        Ok(Self {
            messages: ArrivalSchedule::new(
                inputs.messages,
                definition.schedule.workload_start_micros,
                definition.schedule.workload_end_micros,
                inputs.seed,
                0x6d65_7373_6167_6573,
                inputs.stochastic_arrivals,
            )?,
            timers: ArrivalSchedule::new(
                inputs.timers,
                definition.schedule.workload_start_micros,
                definition.schedule.workload_end_micros,
                inputs.seed,
                0x7469_6d65_7273,
                inputs.stochastic_arrivals,
            )?,
            historical_messages: ArrivalSchedule::from_segments(inputs.history.segments)?,
            events: definition.events,
            reporter: definition.reporter,
            calendar: inputs.history,
            inputs: PrincipalInputGraph::new(inputs, history_count_max)?,
        })
    }
}

impl TickGenerator for PrincipalGraph {
    fn scheduled_release_count_max(&self) -> u32 {
        64
    }

    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        let frame = PrincipalFrame {
            tick: context,
            message_count: self.messages.release_until(context.now_micros)?,
            timer_count: self.timers.release_until(context.now_micros)?,
            historical_message_count: self.historical_messages.release_until(context.now_micros)?,
        };
        Ok(self.inputs.evaluate(context.now_micros, frame))
    }

    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        let event_index = context.event_index;
        let arrivals = match context.source {
            crate::EventSource::Message => &self.messages,
            crate::EventSource::Timer => &self.timers,
        };
        let final_outcome = if self.events.permanent_rejections.matches(event_index) {
            crate::FinalOutcome::PermanentFailure
        } else {
            crate::FinalOutcome::Success
        };
        Ok(EventInputs {
            release_micros: arrivals.release_at(context.event_offset)?,
            partition: self.events.partitions.index(event_index, 64),
            key: self.events.keys.index(event_index, 1_024),
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            outcome: crate::EventOutcome::from_transient_failures(
                self.events.transient_failures.at(event_index),
                final_outcome,
            )?,
        })
    }

    fn calendar_forecast(
        &self,
        _: TickContext<'_>,
    ) -> Result<Option<CalendarForecastInput>, PlantError> {
        self.calendar.forecast()
    }

    fn scheduled_releases(&self, _: TickContext<'_>) -> Result<ScheduledReleasesInput, PlantError> {
        self.timers.pending_releases()
    }

    fn reporter(&self, context: TickContext<'_>) -> ReporterDirective {
        match self.reporter {
            ReporterPolicy::MissingAfter { at_micros } if context.now_micros >= at_micros => {
                ReporterDirective::Missing
            }
            ReporterPolicy::ReplaceAt { at_micros }
                if context.now_micros >= at_micros
                    && context.history.now_micros(0).unwrap_or(0) < at_micros =>
            {
                ReporterDirective::ReplaceAggregator
            }
            _ => ReporterDirective::Send,
        }
    }
}

#[derive(Clone, Copy)]
struct PrincipalDefinition {
    inputs: InputPolicies,
    events: EventPolicy,
    reporter: ReporterPolicy,
    schedule: RunSchedule,
    event_count_max: u32,
    initial_replicas: u32,
}

impl PrincipalDefinition {
    fn for_regime(regime: PrincipalRegime) -> Self {
        let standard = Self::standard();
        match regime {
            PrincipalRegime::Idle => standard
                .messages(ArrivalSeries::None)
                .schedule(RunSchedule::idle()),
            PrincipalRegime::ApplicationLimited | PrincipalRegime::SnapshotFaults => standard,
            PrincipalRegime::LinearThroughput => Self::linear_closed_loop().handler(100_000),
            PrincipalRegime::FlatPostKnee => {
                Self::capacity_closed_loop(100, 100).shared_resource(64, 320, 0)
            }
            PrincipalRegime::DecliningPostKnee => {
                Self::capacity_closed_loop(100, 100).shared_resource(64, 320, 2)
            }
            PrincipalRegime::ShortBurst => short_burst_definition(standard),
            PrincipalRegime::SeasonalWaves => seasonal_definition(standard),
            PrincipalRegime::HotPartition => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 500,
                    count_max: HOT_PARTITION_EVENT_COUNT,
                })
                .partitions(IndexSeries::Single)
                .handler(100_000)
                .schedule(RunSchedule::hot_partition())
                .event_count_max(HOT_PARTITION_EVENT_COUNT),
            PrincipalRegime::TimerWave => standard
                .messages(ArrivalSeries::None)
                .timers(ArrivalSeries::PeriodicDelayed {
                    count: EVENT_COUNT,
                    first_micros: TIMER_WAVE_RELEASE_MICROS,
                    interval_micros: TIMER_WAVE_RELEASE_MICROS,
                    count_max: EVENT_COUNT,
                })
                .handler(100_000)
                .schedule(RunSchedule::timer_wave())
                .initial_replicas(1),
            PrincipalRegime::HotSerializedKey => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 50,
                    count_max: HOT_KEY_EVENT_COUNT,
                })
                .partitions(IndexSeries::Single)
                .keys(IndexSeries::Single)
                .handler(100_000)
                .schedule(RunSchedule::hot_partition())
                .event_count_max(HOT_KEY_EVENT_COUNT)
                .initial_replicas(1),
            PrincipalRegime::TransientFailures => transient_failures_definition(standard),
            PrincipalRegime::PermanentRejections => {
                standard.permanent_rejections(OccurrenceSeries::Every(10))
            }
            PrincipalRegime::RebalanceStorm => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 500,
                    count_max: REBALANCE_EVENT_COUNT,
                })
                .handler(100_000)
                .scale(ScaleSeries::RebalanceStorm)
                .launch_delay(LaunchDelaySeries::Immediate)
                .schedule(RunSchedule::rebalance_storm())
                .event_count_max(REBALANCE_EVENT_COUNT),
            PrincipalRegime::HandlerContention => standard
                .messages(ArrivalSeries::Periodic {
                    count: 400,
                    interval_micros: 500_000,
                    count_max: EVENT_COUNT,
                })
                .schedule(RunSchedule::handler_contention()),
            PrincipalRegime::LooseBudgetBacklog => standard
                .messages(ArrivalSeries::Once(EVENT_COUNT))
                .handler(1_000_000)
                .schedule(RunSchedule::one_shot()),
            PrincipalRegime::ReplicaCeiling => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 3_840,
                    count_max: REPLICA_CEILING_EVENT_COUNT,
                })
                .handler(100_000)
                .schedule(RunSchedule::replica_ceiling())
                .event_count_max(REPLICA_CEILING_EVENT_COUNT),
            PrincipalRegime::MissingReporter => {
                standard.reporter(ReporterPolicy::MissingAfter { at_micros: 500_000 })
            }
            PrincipalRegime::AggregatorReplacement => {
                standard.reporter(ReporterPolicy::ReplaceAt {
                    at_micros: 1_000_000,
                })
            }
            PrincipalRegime::HistoricalMatch => historical_match_definition(standard),
            PrincipalRegime::HistoricalExceeded => {
                historical_rate_definition(standard, 2_000, HistoricalSeries::standard())
            }
            PrincipalRegime::HistoricalUnder => {
                historical_rate_definition(standard, 500, HistoricalSeries::standard())
            }
            PrincipalRegime::HistoricalMissing => {
                historical_rate_definition(standard, 1_000, HistoricalSeries::missing())
            }
        }
    }

    fn capacity_evidence(regime: PrincipalRegime) -> Self {
        Self::for_regime(regime)
            .messages(ArrivalSeries::StaircaseRate {
                initial_per_second: 100,
                increment_per_second: 400,
                step_interval_micros: 30_000_000,
                count_max: CAPACITY_EVENT_COUNT_MAX,
            })
            .launch_delay(LaunchDelaySeries::Immediate)
            .schedule(RunSchedule::extended_capacity_evidence())
    }

    const fn standard() -> Self {
        Self {
            inputs: InputPolicies {
                messages: ArrivalSeries::Rate {
                    per_second: 1_000,
                    count_max: EVENT_COUNT,
                },
                timers: ArrivalSeries::None,
                handler_micros: 2_000,
                shared_resource: SharedResourcePolicy::new(128, 128_000, 0),
                scale: ScaleSeries::None,
                launch_delay: LaunchDelaySeries::Uniform {
                    minimum_micros: 30_000_000,
                    maximum_micros: 90_000_000,
                },
                history: HistoricalSeries::missing(),
                seed: 0,
                stochastic_arrivals: false,
            },
            events: EventPolicy::standard(),
            reporter: ReporterPolicy::Always,
            schedule: RunSchedule::standard(),
            event_count_max: EVENT_COUNT,
            initial_replicas: 8,
        }
    }

    const fn capacity() -> Self {
        Self {
            inputs: InputPolicies {
                messages: ArrivalSeries::Rate {
                    per_second: 3_000,
                    count_max: CAPACITY_EVENT_COUNT_MAX,
                },
                timers: ArrivalSeries::None,
                handler_micros: 2_000,
                shared_resource: SharedResourcePolicy::new(128, 128_000, 0),
                scale: ScaleSeries::None,
                launch_delay: LaunchDelaySeries::Uniform {
                    minimum_micros: 30_000_000,
                    maximum_micros: 90_000_000,
                },
                history: HistoricalSeries::missing(),
                seed: 0,
                stochastic_arrivals: false,
            },
            events: EventPolicy::standard(),
            reporter: ReporterPolicy::Always,
            schedule: RunSchedule::capacity_evidence(),
            event_count_max: CAPACITY_EVENT_COUNT_MAX,
            initial_replicas: 1,
        }
    }

    const fn capacity_closed_loop(initial_demand_per_second: u32, demand_step: u32) -> Self {
        Self::capacity()
            .messages(ArrivalSeries::StaircaseRate {
                initial_per_second: initial_demand_per_second,
                increment_per_second: demand_step,
                step_interval_micros: 90_000_000,
                count_max: CAPACITY_RESPONSE_EVENT_COUNT,
            })
            .launch_delay(LaunchDelaySeries::Uniform {
                minimum_micros: 30_000_000,
                maximum_micros: 90_000_000,
            })
            .schedule(RunSchedule::capacity_response())
    }

    const fn linear_closed_loop() -> Self {
        Self::capacity()
            .messages(ArrivalSeries::StaircaseRate {
                initial_per_second: LINEAR_RATE_INCREMENT,
                increment_per_second: LINEAR_RATE_INCREMENT,
                step_interval_micros: LINEAR_STEP_MICROS,
                count_max: LINEAR_RESPONSE_EVENT_COUNT,
            })
            .launch_delay(LaunchDelaySeries::Uniform {
                minimum_micros: 30_000_000,
                maximum_micros: 90_000_000,
            })
            .schedule(RunSchedule::linear_response())
            .event_count_max(LINEAR_RESPONSE_EVENT_COUNT)
    }

    const fn messages(mut self, series: ArrivalSeries) -> Self {
        self.inputs.messages = series;
        self
    }

    const fn timers(mut self, series: ArrivalSeries) -> Self {
        self.inputs.timers = series;
        self
    }

    const fn handler(mut self, micros: u64) -> Self {
        self.inputs.handler_micros = micros;
        self
    }

    const fn shared_resource(
        mut self,
        parallelism: u32,
        capacity_per_second: u32,
        collapse: u32,
    ) -> Self {
        self.inputs.shared_resource =
            SharedResourcePolicy::new(parallelism, capacity_per_second, collapse);
        self
    }

    const fn scale(mut self, series: ScaleSeries) -> Self {
        self.inputs.scale = series;
        self
    }

    const fn launch_delay(mut self, series: LaunchDelaySeries) -> Self {
        self.inputs.launch_delay = series;
        self
    }

    const fn event_count_max(mut self, event_count_max: u32) -> Self {
        self.event_count_max = event_count_max;
        self
    }

    const fn initial_replicas(mut self, initial_replicas: u32) -> Self {
        self.initial_replicas = initial_replicas;
        self
    }

    const fn partitions(mut self, series: IndexSeries) -> Self {
        self.events.partitions = series;
        self
    }

    const fn keys(mut self, series: IndexSeries) -> Self {
        self.events.keys = series;
        self
    }

    const fn transient_failures(mut self, series: FailureSeries) -> Self {
        self.events.transient_failures = series;
        self
    }

    const fn permanent_rejections(mut self, series: OccurrenceSeries) -> Self {
        self.events.permanent_rejections = series;
        self
    }

    const fn reporter(mut self, policy: ReporterPolicy) -> Self {
        self.reporter = policy;
        self
    }

    const fn schedule(mut self, schedule: RunSchedule) -> Self {
        self.schedule = schedule;
        self
    }

    const fn history(mut self, history: HistoricalSeries) -> Self {
        self.inputs.history = history;
        self
    }

    const fn seeded(mut self, seed: u64) -> Self {
        self.inputs.seed = seed;
        self.inputs.stochastic_arrivals = true;
        self
    }
}

fn short_burst_definition(standard: PrincipalDefinition) -> PrincipalDefinition {
    standard
        .messages(ArrivalSeries::PeriodicDelayed {
            count: EVENT_COUNT,
            first_micros: SHORT_BURST_RELEASE_MICROS,
            interval_micros: SHORT_BURST_RELEASE_MICROS,
            count_max: EVENT_COUNT,
        })
        .handler(100_000)
        .schedule(RunSchedule::short_burst())
        .initial_replicas(1)
}

fn seasonal_definition(standard: PrincipalDefinition) -> PrincipalDefinition {
    standard
        .messages(ArrivalSeries::PeriodicDelayed {
            count: 1_000,
            first_micros: 120_000_000,
            interval_micros: 120_000_000,
            count_max: SEASONAL_EVENT_COUNT,
        })
        .handler(100_000)
        .history(HistoricalSeries::seasonal())
        .launch_delay(LaunchDelaySeries::Immediate)
        .schedule(RunSchedule::seasonal())
        .event_count_max(SEASONAL_EVENT_COUNT)
        .initial_replicas(1)
}

fn transient_failures_definition(standard: PrincipalDefinition) -> PrincipalDefinition {
    standard
        .messages(ArrivalSeries::Rate {
            per_second: 300,
            count_max: TRANSIENT_EVENT_COUNT,
        })
        .handler(100_000)
        .transient_failures(FailureSeries::Every {
            interval: 10,
            transient_count: 2,
        })
        .schedule(RunSchedule::hot_partition())
        .event_count_max(TRANSIENT_EVENT_COUNT)
        .initial_replicas(1)
}

fn historical_match_definition(standard: PrincipalDefinition) -> PrincipalDefinition {
    let history = HistoricalSeries::standard();
    let messages = history.live_demand(HISTORY_EVENT_COUNT_MAX);
    historical_definition(standard, messages, history)
}

fn historical_rate_definition(
    standard: PrincipalDefinition,
    per_second: u32,
    history: HistoricalSeries,
) -> PrincipalDefinition {
    historical_definition(
        standard,
        ArrivalSeries::Rate {
            per_second,
            count_max: HISTORY_EVENT_COUNT_MAX,
        },
        history,
    )
}

fn historical_definition(
    standard: PrincipalDefinition,
    messages: ArrivalSeries,
    history: HistoricalSeries,
) -> PrincipalDefinition {
    standard
        .messages(messages)
        .history(history)
        .handler(100_000)
        .schedule(RunSchedule::history())
        .event_count_max(HISTORY_EVENT_COUNT_MAX)
        .initial_replicas(1)
}

#[derive(Clone, Copy)]
struct InputPolicies {
    messages: ArrivalSeries,
    timers: ArrivalSeries,
    handler_micros: u64,
    shared_resource: SharedResourcePolicy,
    scale: ScaleSeries,
    launch_delay: LaunchDelaySeries,
    history: HistoricalSeries,
    seed: u64,
    stochastic_arrivals: bool,
}

#[derive(Clone, Copy)]
struct HistoricalSeries {
    segments: &'static [ScheduleSegment],
    replicas: u32,
}

impl HistoricalSeries {
    const fn standard() -> Self {
        Self {
            segments: HISTORICAL_SCHEDULE,
            replicas: 8,
        }
    }

    const fn seasonal() -> Self {
        Self {
            segments: SEASONAL_SCHEDULE,
            replicas: 4,
        }
    }

    const fn missing() -> Self {
        Self {
            segments: &[],
            replicas: 0,
        }
    }

    const fn live_demand(self, count_max: u32) -> ArrivalSeries {
        ArrivalSeries::Rate {
            per_second: self.segments[1].rate_per_second,
            count_max,
        }
    }

    fn forecast(self) -> Result<Option<CalendarForecastInput>, PlantError> {
        let Some(first) = self.segments.first() else {
            return Ok(None);
        };
        if self.segments.len() > 8 {
            return Err(PlantError::CalendarCapacity);
        }
        let span_micros = self
            .segments
            .last()
            .map_or(0, |segment| segment.end_micros)
            .saturating_sub(first.start_micros);
        let span_seconds = Duration::from_micros(span_micros).as_secs_f64();
        if span_seconds <= 0.0_f64 {
            return Err(PlantError::ZeroBound {
                name: "historical_schedule_span",
            });
        }
        let exposure_scale = f64::from(CALENDAR_HISTORY_EXPOSURE_SECONDS) / span_seconds;
        let mut segments = [CalendarRateSegment::new(
            0,
            first.start_micros,
            first.end_micros,
            CALENDAR_PRIOR_SHAPE,
            CALENDAR_PRIOR_RATE_SECONDS,
        )?; 8];
        for (position, source) in self.segments.iter().enumerate() {
            let exposure_seconds =
                Duration::from_micros(source.end_micros.saturating_sub(source.start_micros))
                    .as_secs_f64()
                    * exposure_scale;
            let historical_count = f64::from(source.rate_per_second) * exposure_seconds;
            segments[position] = CalendarRateSegment::new(
                position as u32,
                source.start_micros,
                source.end_micros,
                CALENDAR_PRIOR_SHAPE + historical_count,
                CALENDAR_PRIOR_RATE_SECONDS + exposure_seconds,
            )?;
        }
        Ok(Some(CalendarForecastInput::new(
            CalendarArtifactId(1),
            CALENDAR_MODEL_PRIOR_PROBABILITY,
            &segments[..self.segments.len()],
        )?))
    }
}

#[derive(Clone, Copy)]
struct ScheduleSegment {
    start_micros: u64,
    end_micros: u64,
    rate_per_second: u32,
}

impl ScheduleSegment {
    const fn new(start_micros: u64, end_micros: u64, rate_per_second: u32) -> Self {
        Self {
            start_micros,
            end_micros,
            rate_per_second,
        }
    }
}

#[derive(Clone, Copy)]
enum ArrivalSeries {
    None,
    Once(u32),
    Rate {
        per_second: u32,
        count_max: u32,
    },
    Periodic {
        count: u32,
        interval_micros: u64,
        count_max: u32,
    },
    PeriodicDelayed {
        count: u32,
        first_micros: u64,
        interval_micros: u64,
        count_max: u32,
    },
    StaircaseRate {
        initial_per_second: u32,
        increment_per_second: u32,
        step_interval_micros: u64,
        count_max: u32,
    },
}

/// One bounded demand process with fixed virtual release times.
///
/// The controller cadence cannot change the ordered release column.
struct ArrivalSchedule {
    release_micros: Vec<u64>,
    cursor: usize,
    interval_start: usize,
}

impl ArrivalSchedule {
    fn from_segments(segments: &[ScheduleSegment]) -> Result<Self, PlantError> {
        let count = segments.iter().try_fold(0_usize, |sum, segment| {
            let duration_micros = segment.end_micros.saturating_sub(segment.start_micros);
            let count =
                u64::from(segment.rate_per_second).saturating_mul(duration_micros) / 1_000_000;
            let count = usize::try_from(count).map_err(|_| PlantError::PlatformLimit)?;
            sum.checked_add(count).ok_or(PlantError::PlatformLimit)
        })?;
        let mut release_micros = Vec::with_capacity(count);
        for segment in segments {
            let segment_count = u64::from(segment.rate_per_second)
                .saturating_mul(segment.end_micros.saturating_sub(segment.start_micros))
                / 1_000_000;
            for ordinal in 1..=segment_count {
                let offset =
                    ordinal.saturating_mul(1_000_000) / u64::from(segment.rate_per_second.max(1));
                release_micros.push(segment.start_micros.saturating_add(offset));
            }
        }
        Ok(Self {
            release_micros,
            cursor: 0,
            interval_start: 0,
        })
    }

    fn new(
        series: ArrivalSeries,
        start_micros: u64,
        end_micros: u64,
        seed: u64,
        domain: u64,
        stochastic: bool,
    ) -> Result<Self, PlantError> {
        let deterministic_count = usize::try_from(series.count_between(start_micros, end_micros))
            .map_err(|_| PlantError::PlatformLimit)?;
        let stochastic_rate = stochastic
            && matches!(
                series,
                ArrivalSeries::Rate { .. } | ArrivalSeries::StaircaseRate { .. }
            );
        let count = if stochastic_rate {
            usize::try_from(series.count_max()).map_err(|_| PlantError::PlatformLimit)?
        } else {
            deterministic_count
        };
        let mut release_micros = Vec::with_capacity(count);
        if stochastic_rate {
            series.push_stochastic_releases(
                start_micros,
                end_micros,
                seed,
                domain,
                count,
                &mut release_micros,
            );
        } else {
            series.push_deterministic_releases(
                start_micros,
                end_micros,
                count,
                &mut release_micros,
            );
        }
        Ok(Self {
            release_micros,
            cursor: 0,
            interval_start: 0,
        })
    }

    fn release_until(&mut self, now_micros: u64) -> Result<u32, PlantError> {
        self.interval_start = self.cursor;
        self.cursor +=
            self.release_micros[self.cursor..].partition_point(|release| *release <= now_micros);
        u32::try_from(self.cursor - self.interval_start).map_err(|_| PlantError::PlatformLimit)
    }

    fn release_at(&self, event_offset: u32) -> Result<u64, PlantError> {
        let offset = usize::try_from(event_offset).map_err(|_| PlantError::PlatformLimit)?;
        self.release_micros
            .get(self.interval_start.saturating_add(offset))
            .copied()
            .ok_or(PlantError::PlatformLimit)
    }

    fn pending_releases(&self) -> Result<ScheduledReleasesInput, PlantError> {
        let mut releases = Vec::with_capacity(64);
        let mut input = &self.release_micros[self.cursor..];
        let mut count = 0_usize;
        while !input.is_empty() && count < releases.capacity() {
            let release_micros = input[0];
            let group_count = input.partition_point(|release| *release == release_micros);
            releases.push(ScheduledRelease {
                release_micros,
                count: u32::try_from(group_count).map_err(|_| PlantError::PlatformLimit)?,
            });
            count += 1;
            input = &input[group_count..];
        }
        ScheduledReleasesInput::new(releases, 64)
    }
}

impl ArrivalSeries {
    fn push_deterministic_releases(
        self,
        start_micros: u64,
        end_micros: u64,
        count: usize,
        output: &mut Vec<u64>,
    ) {
        for index in 0..count {
            let ordinal = index as u64 + 1;
            let release = match self {
                Self::None => continue,
                Self::Once(_) => start_micros,
                Self::Rate { .. } | Self::StaircaseRate { .. } => {
                    self.release_for_ordinal(start_micros, end_micros, ordinal)
                }
                Self::Periodic {
                    count,
                    interval_micros,
                    ..
                } => (ordinal - 1)
                    .checked_div(u64::from(count))
                    .map_or(0, |period| period.saturating_mul(interval_micros)),
                Self::PeriodicDelayed {
                    count,
                    first_micros,
                    interval_micros,
                    ..
                } => first_micros.saturating_add(
                    (ordinal - 1)
                        .checked_div(u64::from(count))
                        .map_or(0, |period| period.saturating_mul(interval_micros)),
                ),
            };
            output.push(release);
        }
    }

    fn push_stochastic_releases(
        self,
        start_micros: u64,
        end_micros: u64,
        seed: u64,
        domain: u64,
        count_max: usize,
        output: &mut Vec<u64>,
    ) {
        let mut cell_start = start_micros;
        while cell_start < end_micros && output.len() < count_max {
            let cell_end = cell_start.saturating_add(1_000_000).min(end_micros);
            let start_hazard = self.cumulative_event_micros(cell_start);
            let end_hazard = self.cumulative_event_micros(cell_end);
            let hazard_delta = end_hazard.saturating_sub(start_hazard);
            let hazard = count_as_f64(hazard_delta) / 1_000_000.0_f64;
            let mut random = RandomStream::new(seed).domain(domain ^ cell_start);
            let cell_count = usize::try_from(sample_poisson(hazard, &mut random))
                .map_or(count_max - output.len(), |value| {
                    value.min(count_max - output.len())
                });
            let first = output.len();
            for _ in 0..cell_count {
                let target = count_as_f64(start_hazard)
                    + random.open_unit_f64() * count_as_f64(hazard_delta);
                output.push(self.release_for_hazard(cell_start, cell_end, target));
            }
            output[first..].sort_unstable();
            cell_start = cell_end;
        }
    }

    fn release_for_hazard(self, start_micros: u64, end_micros: u64, target: f64) -> u64 {
        let mut lower = start_micros.saturating_add(1).min(end_micros);
        let mut upper = end_micros;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if count_as_f64(self.cumulative_event_micros(middle)) >= target {
                upper = middle;
            } else {
                lower = middle.saturating_add(1);
            }
        }
        lower
    }

    fn release_for_ordinal(self, start_micros: u64, end_micros: u64, ordinal: u64) -> u64 {
        let target = self
            .cumulative_event_micros(start_micros)
            .saturating_add(ordinal.saturating_mul(1_000_000));
        let mut lower = start_micros.saturating_add(1).min(end_micros);
        let mut upper = end_micros;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.cumulative_event_micros(middle) >= target {
                upper = middle;
            } else {
                lower = middle.saturating_add(1);
            }
        }
        lower
    }

    const fn count_max(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Once(count)
            | Self::Rate {
                count_max: count, ..
            }
            | Self::Periodic {
                count_max: count, ..
            }
            | Self::PeriodicDelayed {
                count_max: count, ..
            }
            | Self::StaircaseRate {
                count_max: count, ..
            } => count,
        }
    }

    fn cumulative_event_micros(self, now_micros: u64) -> u64 {
        match self {
            Self::Rate { per_second, .. } => u64::from(per_second).saturating_mul(now_micros),
            Self::StaircaseRate {
                initial_per_second,
                increment_per_second,
                step_interval_micros,
                ..
            } => staircase_event_micros(
                now_micros,
                initial_per_second,
                increment_per_second,
                step_interval_micros,
            ),
            Self::None | Self::Once(_) | Self::Periodic { .. } | Self::PeriodicDelayed { .. } => 0,
        }
    }

    fn count_between(self, start_micros: u64, end_micros: u64) -> u32 {
        match self {
            Self::Rate { count_max, .. } | Self::StaircaseRate { count_max, .. } => {
                let event_micros = self
                    .cumulative_event_micros(end_micros)
                    .saturating_sub(self.cumulative_event_micros(start_micros));
                bounded_count(event_micros / 1_000_000, count_max)
            }
            _ => self.at(end_micros, 0),
        }
    }

    fn at(self, now_micros: u64, emitted: u32) -> u32 {
        let cumulative = match self {
            Self::None => 0,
            Self::Once(count) => count,
            Self::Rate {
                per_second,
                count_max,
            } => bounded_count(
                u64::from(per_second).saturating_mul(now_micros) / 1_000_000,
                count_max,
            ),
            Self::Periodic {
                count,
                interval_micros,
                count_max,
            } => {
                let release_count = now_micros / interval_micros + 1;
                let count = u64::from(count).saturating_mul(release_count);
                bounded_count(count, count_max)
            }
            Self::PeriodicDelayed {
                count,
                first_micros,
                interval_micros,
                count_max,
            } => {
                let release_count = if now_micros < first_micros {
                    0
                } else {
                    (now_micros - first_micros) / interval_micros + 1
                };
                bounded_count(u64::from(count).saturating_mul(release_count), count_max)
            }
            Self::StaircaseRate {
                initial_per_second,
                increment_per_second,
                step_interval_micros,
                count_max,
            } => bounded_count(
                staircase_event_micros(
                    now_micros,
                    initial_per_second,
                    increment_per_second,
                    step_interval_micros,
                ) / 1_000_000,
                count_max,
            ),
        };
        cumulative.saturating_sub(emitted)
    }
}

fn sample_poisson(mean: f64, random: &mut RandomStream) -> u32 {
    let mut remaining = mean;
    let mut total = 0_u32;
    while remaining > f64::EPSILON {
        let lambda = remaining.min(20.0_f64);
        let limit = (-lambda).exp();
        let mut product = 1.0_f64;
        let mut count = 0_u32;
        loop {
            product *= random.open_unit_f64();
            if product <= limit {
                break;
            }
            count = count.saturating_add(1);
        }
        total = total.saturating_add(count);
        remaining -= lambda;
    }
    total
}

fn staircase_event_micros(
    now_micros: u64,
    initial_per_second: u32,
    increment_per_second: u32,
    step_interval_micros: u64,
) -> u64 {
    let complete_steps = now_micros / step_interval_micros;
    let partial_step_micros = now_micros % step_interval_micros;
    let step_sum = complete_steps.saturating_mul(complete_steps.saturating_sub(1)) / 2;
    u64::from(initial_per_second)
        .saturating_mul(now_micros)
        .saturating_add(
            u64::from(increment_per_second)
                .saturating_mul(step_interval_micros)
                .saturating_mul(step_sum),
        )
        .saturating_add(
            u64::from(increment_per_second)
                .saturating_mul(complete_steps)
                .saturating_mul(partial_step_micros),
        )
}

fn bounded_count(count: u64, count_max: u32) -> u32 {
    match u32::try_from(count.min(u64::from(count_max))) {
        Ok(count) => count,
        Err(_) => count_max,
    }
}

fn count_as_f64(value: u64) -> f64 {
    Duration::from_micros(value).as_secs_f64() * 1_000_000.0_f64
}

impl ScaleSeries {
    fn at(self, now_micros: u64, previous: Option<u32>) -> u32 {
        match self {
            Self::None => 0,
            Self::RebalanceStorm if now_micros < 400_000 => previous.unwrap_or(0),
            Self::RebalanceStorm => {
                let change = (now_micros - 400_000) / 300_000;
                if change < 6 {
                    6 + change as u32 % 3
                } else {
                    previous.unwrap_or(0)
                }
            }
        }
    }
}

impl LaunchDelaySeries {
    fn at(self, now_micros: u64, seed: u64) -> u64 {
        match self {
            Self::Immediate => 0,
            Self::Uniform {
                minimum_micros,
                maximum_micros,
            } => uniform_delay(seed, now_micros, minimum_micros, maximum_micros),
        }
    }
}

fn uniform_delay(seed: u64, now_micros: u64, minimum_micros: u64, maximum_micros: u64) -> u64 {
    let span = maximum_micros.saturating_sub(minimum_micros) + 1;
    let mut random = RandomStream::new(seed).domain(0x6c61_756e_6368 ^ now_micros);
    minimum_micros.saturating_add(random.next_u64() % span)
}

#[derive(Clone, Copy)]
struct SharedResourcePolicy {
    parallelism: u32,
    capacity_per_second: u32,
    collapse: u32,
}

impl SharedResourcePolicy {
    const fn new(parallelism: u32, capacity_per_second: u32, collapse: u32) -> Self {
        Self {
            parallelism,
            capacity_per_second,
            collapse,
        }
    }
}

#[derive(Clone, Copy)]
enum ScaleSeries {
    None,
    RebalanceStorm,
}

#[derive(Clone, Copy)]
enum LaunchDelaySeries {
    Immediate,
    Uniform {
        minimum_micros: u64,
        maximum_micros: u64,
    },
}

#[derive(Clone, Copy)]
struct EventPolicy {
    partitions: IndexSeries,
    keys: IndexSeries,
    transient_failures: FailureSeries,
    permanent_rejections: OccurrenceSeries,
}

impl EventPolicy {
    const fn standard() -> Self {
        Self {
            partitions: IndexSeries::Striped,
            keys: IndexSeries::Striped,
            transient_failures: FailureSeries::None,
            permanent_rejections: OccurrenceSeries::Never,
        }
    }
}

#[derive(Clone, Copy)]
enum IndexSeries {
    Striped,
    Single,
}

impl IndexSeries {
    fn index(self, event_index: u32, count: u32) -> u32 {
        match self {
            Self::Striped => event_index % count,
            Self::Single => 0,
        }
    }
}

#[derive(Clone, Copy)]
enum FailureSeries {
    None,
    Every { interval: u32, transient_count: u8 },
}

impl FailureSeries {
    fn at(self, event_index: u32) -> u8 {
        match self {
            Self::Every {
                interval,
                transient_count,
            } if event_index.is_multiple_of(interval) => transient_count,
            _ => 0,
        }
    }
}

#[derive(Clone, Copy)]
enum OccurrenceSeries {
    Never,
    Every(u32),
}

impl OccurrenceSeries {
    fn matches(self, event_index: u32) -> bool {
        match self {
            Self::Never => false,
            Self::Every(interval) => event_index.is_multiple_of(interval),
        }
    }
}

#[derive(Clone, Copy)]
enum ReporterPolicy {
    Always,
    MissingAfter { at_micros: u64 },
    ReplaceAt { at_micros: u64 },
}

#[derive(Clone, Copy)]
/// One simulation clock and its live-demand window.
///
/// The workload window must start no earlier than the simulation. It must
/// start no later than its end.
struct RunSchedule {
    start_micros: u64,
    workload_start_micros: u64,
    workload_end_micros: u64,
    workload_interval_micros: u64,
    followup_interval_micros: u64,
    maximum_micros: u64,
    stop: StopCondition,
}

impl RunSchedule {
    fn controller_sample_count_max(self) -> Result<u32, PlantError> {
        let interval = self
            .workload_interval_micros
            .min(self.followup_interval_micros);
        if interval == 0 {
            return Err(PlantError::ZeroBound {
                name: "controller_tick_interval",
            });
        }
        let span = self.maximum_micros.saturating_sub(self.start_micros);
        u32::try_from(span / interval + 2).map_err(|_| PlantError::PlatformLimit)
    }

    const fn standard() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 2_000_000,
            workload_interval_micros: 100_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 300_000_000,
            stop: StopCondition::IdleStable { sample_count: 3 },
        }
    }

    const fn idle() -> Self {
        Self {
            maximum_micros: IDLE_DURATION_MICROS,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
            ..Self::standard()
        }
    }

    const fn capacity_evidence() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 50_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 50_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn extended_capacity_evidence() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 180_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 180_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn capacity_response() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 600_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 690_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn linear_response() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 1_200_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 1_290_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn one_shot() -> Self {
        Self {
            start_micros: 1_000_000,
            workload_start_micros: 1_000_000,
            workload_end_micros: 1_000_000,
            ..Self::standard()
        }
    }

    const fn short_burst() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: SHORT_BURST_RELEASE_MICROS,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 300_000_000,
            stop: StopCondition::IdleStable { sample_count: 3 },
        }
    }

    const fn replica_ceiling() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 60_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 150_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn rebalance_storm() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: 0,
            workload_end_micros: 65_000_000,
            workload_interval_micros: 100_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 65_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn handler_contention() -> Self {
        Self {
            workload_end_micros: 2_000_000,
            workload_interval_micros: 500_000,
            ..Self::standard()
        }
    }

    const fn seasonal() -> Self {
        Self {
            workload_end_micros: 420_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 420_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
            ..Self::standard()
        }
    }

    const fn timer_wave() -> Self {
        Self {
            workload_end_micros: TIMER_WAVE_RUN_END_MICROS,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: TIMER_WAVE_RUN_END_MICROS,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
            ..Self::standard()
        }
    }

    const fn hot_partition() -> Self {
        Self {
            workload_end_micros: 120_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 120_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
            ..Self::standard()
        }
    }

    const fn history() -> Self {
        Self {
            start_micros: 0,
            workload_start_micros: HISTORY_START_MICROS,
            workload_end_micros: HISTORY_END_MICROS,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: HISTORY_RUN_END_MICROS,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }
}

#[derive(Clone, Copy)]
enum StopCondition {
    IdleStable { sample_count: u8 },
    FixedDuration { reason: RunStopReason },
}

#[derive(Clone, Copy)]
struct PrincipalFrame<'a> {
    tick: TickContext<'a>,
    message_count: u32,
    timer_count: u32,
    historical_message_count: u32,
}

series_graph! {
    struct PrincipalInputGraph(PrincipalFrame<'_>) with (policies: InputPolicies) {
        series message_count: u32 ["message arrivals", Count, Input] =
            ScheduledMessages {} => ();
        series timer_count: u32 ["timer arrivals", Count, Input] =
            ScheduledTimers {} => ();
        series historical_message_count: u32 ["historical demand", Count, Input] =
            ScheduledHistoricalMessages {} => ();
        series historical_replicas: u32 ["historical replicas", Replicas, Input] =
            HistoricalReplicas(policies.history.replicas) => ();
        series external_target: u32 ["external target", Replicas, Action] =
            ScaleInput(policies.scale) => () previous (external_target);
        series scale_changed: bool ["external scale change", Boolean, Action] =
            ScaleChanged {} => (external_target) previous (external_target);
        series handler_micros: u64 ["base handler time", Microseconds, Input] =
            HandlerDuration(policies.handler_micros) => ();
        series shared_resource_capacity_per_second: u32 ["shared resource capacity", Count, Input] =
            SharedResourceCapacity(policies.shared_resource.capacity_per_second) => ();
        series dependency_operation_micros: u64 ["shared resource service time", Microseconds, State] =
            SharedResourceServiceTime(policies.shared_resource.parallelism) =>
            (shared_resource_capacity_per_second);
        series launch_delay_micros: u64 ["launch delay", Microseconds, Input] =
            LaunchDelay(policies.launch_delay, policies.seed) => ();
        output output: TickInputs = PrincipalInputs {} => (
            message_count,
            timer_count,
            external_target,
            scale_changed,
            handler_micros,
            dependency_operation_micros,
            launch_delay_micros,
        );
    }
}

struct ScheduledMessages;

impl SeriesFunction<PrincipalFrame<'_>, ()> for ScheduledMessages {
    type Output = u32;

    fn calculate(&self, context: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        context.frame.message_count
    }
}

struct ScheduledTimers;

impl SeriesFunction<PrincipalFrame<'_>, ()> for ScheduledTimers {
    type Output = u32;

    fn calculate(&self, context: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        context.frame.timer_count
    }
}

struct ScheduledHistoricalMessages;

impl SeriesFunction<PrincipalFrame<'_>, ()> for ScheduledHistoricalMessages {
    type Output = u32;

    fn calculate(&self, context: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        context.frame.historical_message_count
    }
}

struct HistoricalReplicas(u32);

impl SeriesFunction<PrincipalFrame<'_>, ()> for HistoricalReplicas {
    type Output = u32;

    fn calculate(&self, _: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct ScaleInput(ScaleSeries);

impl SeriesFunction<PrincipalFrame<'_>, (Option<u32>,)> for ScaleInput {
    type Output = u32;

    fn calculate(
        &self,
        context: SeriesContext<'_, PrincipalFrame<'_>>,
        (previous,): (Option<u32>,),
    ) -> Self::Output {
        self.0.at(context.frame.tick.now_micros, previous)
    }
}

struct ScaleChanged;

impl SeriesFunction<PrincipalFrame<'_>, (u32, Option<u32>)> for ScaleChanged {
    type Output = bool;

    fn calculate(
        &self,
        _: SeriesContext<'_, PrincipalFrame<'_>>,
        (target, previous): (u32, Option<u32>),
    ) -> Self::Output {
        target > 0 && Some(target) != previous
    }
}

struct HandlerDuration(u64);

impl SeriesFunction<PrincipalFrame<'_>, ()> for HandlerDuration {
    type Output = u64;

    fn calculate(&self, _: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct SharedResourceCapacity(u32);

impl SeriesFunction<PrincipalFrame<'_>, ()> for SharedResourceCapacity {
    type Output = u32;

    fn calculate(&self, _: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct SharedResourceServiceTime(u32);

impl SeriesFunction<PrincipalFrame<'_>, (u32,)> for SharedResourceServiceTime {
    type Output = u64;

    fn calculate(
        &self,
        _: SeriesContext<'_, PrincipalFrame<'_>>,
        (capacity_per_second,): (u32,),
    ) -> Self::Output {
        u64::from(self.0)
            .saturating_mul(1_000_000)
            .div_ceil(u64::from(capacity_per_second.max(1)))
    }
}

struct LaunchDelay(LaunchDelaySeries, u64);

impl SeriesFunction<PrincipalFrame<'_>, ()> for LaunchDelay {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, PrincipalFrame<'_>>, (): ()) -> Self::Output {
        self.0.at(context.frame.tick.now_micros, self.1)
    }
}

struct PrincipalInputs;

impl OutputFunction<PrincipalFrame<'_>, (u32, u32, u32, bool, u64, u64, u64)> for PrincipalInputs {
    type Output = TickInputs;

    fn calculate(
        &self,
        _: SeriesContext<'_, PrincipalFrame<'_>>,
        values: (u32, u32, u32, bool, u64, u64, u64),
    ) -> Self::Output {
        TickInputs {
            message_count: values.0,
            timer_count: values.1,
            handler_micros: values.4,
            dependency_operations: 1,
            dependency_operation_micros: values.5,
            handler_added_micros: 0,
            outcome: crate::EventOutcomeRule::Success,
            launch_delay_micros: values.6,
            scale: if values.2 == 0 {
                ScaleDirective::Hold
            } else if values.3 {
                ScaleDirective::Request { replicas: values.2 }
            } else {
                ScaleDirective::ExternalHold
            },
        }
    }
}

#[cfg(test)]
#[path = "regime_tests.rs"]
mod tests;
