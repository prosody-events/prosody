use std::ops::Deref;

use prosody_scale_core::{
    CapacityGrid, CapacityPrior, Configuration, RandomStream, ServiceObjective,
};

use crate::harness::TickDrivenAttemptModel;
use crate::series::{
    OutputFunction, RecordedSeries, SeriesContext, SeriesFunction, SeriesHistory, series_graph,
};
use crate::{
    AttemptFrame, AttemptModel, AttemptParameters, ClosedLoop, ClosedLoopError,
    ConcurrencyLatencyCurve, ControllerTrace, DEFAULT_CONCURRENCY_PER_REPLICA, EventContext,
    EventInputs, FaultPattern, MetricTrace, PlantConfiguration, PlantError, ReporterDirective,
    ScaleDirective, SeriesCell, SimulationHarness, SimulationResult, TickContext, TickGenerator,
    TickInputs,
};

const CAPACITY_COLLAPSE_GRID: &[f64] = &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64];

const EVENT_COUNT: u32 = 2_000;
const HOT_PARTITION_EVENT_COUNT: u32 = 60_000;
const SEASONAL_EVENT_COUNT: u32 = 3_000;
const CAPACITY_EVENT_COUNT_MAX: u32 = 300_000;
const HISTORY_EVENT_COUNT_MAX: u32 = 64_000;

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
        PrincipalRegime::ShortBurst => {
            run.events().len() == EVENT_COUNT as usize
                && run
                    .events()
                    .iter()
                    .all(|event| event.release_micros == 1_000_000)
        }
        PrincipalRegime::SeasonalWaves => {
            let first = run.events().first().map(|event| event.release_micros);
            run.events()
                .iter()
                .any(|event| Some(event.release_micros) != first)
                && input_sum(run.inputs(), "historical_message_count") > 0
        }
        PrincipalRegime::HotPartition => run.events().iter().all(|event| event.partition == 0),
        PrincipalRegime::TimerWave => {
            run.events().len() == EVENT_COUNT as usize
                && run
                    .events()
                    .iter()
                    .all(|event| event.timer && event.release_micros == 120_000_000)
        }
        PrincipalRegime::HotSerializedKey => {
            run.events().iter().all(|event| event.key == 0)
                && run
                    .settlements()
                    .iter()
                    .all(|settlement| settlement.in_flight_at_dispatch <= 1)
        }
        PrincipalRegime::TransientFailures => run
            .settlements()
            .iter()
            .any(|settlement| settlement.attempts > 1),
        PrincipalRegime::PermanentRejections => run
            .settlements()
            .iter()
            .any(|settlement| settlement.permanent_rejection),
        PrincipalRegime::RebalanceStorm => {
            input_distinct_positive_count(run.inputs(), "external_target") >= 2
        }
        PrincipalRegime::HandlerContention => run
            .settlements()
            .iter()
            .any(|settlement| settlement.handler_micros > 2_000),
        PrincipalRegime::LooseBudgetBacklog => {
            run.events().len() == EVENT_COUNT as usize && settled_all
        }
        PrincipalRegime::SnapshotFaults => settled_all,
        PrincipalRegime::MissingReporter => (0..run.controller().len())
            .filter_map(|index| run.controller().sample(index))
            .any(|sample| sample.reporter == ReporterDirective::Missing),
        PrincipalRegime::AggregatorReplacement => (0..run.controller().len())
            .filter_map(|index| run.controller().sample(index))
            .any(|sample| sample.reporter == ReporterDirective::ReplaceAggregator),
        PrincipalRegime::ReplicaCeiling => (0..run.controller().len())
            .filter_map(|index| run.controller().sample(index))
            .filter(|sample| !sample.hold)
            .all(|sample| sample.target <= 8),
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

fn validate_closed_loop_claim(
    regime: PrincipalRegime,
    run: &PrincipalRun,
) -> Result<(), RegimeValidationError> {
    let invariant = match regime {
        PrincipalRegime::ShortBurst => (
            (0..run.controller.len()).all(|index| {
                run.controller
                    .sample(index)
                    .is_some_and(|sample| sample.target == 1)
            }),
            "the controller requested capacity that cannot arrive before the burst deadlines",
        ),
        PrincipalRegime::HotPartition => (
            placement_constraint_binds(run),
            "the decision did not expose the binding partition-placement loss",
        ),
        PrincipalRegime::SeasonalWaves => (
            (0..run.controller.len()).any(|index| {
                run.controller
                    .sample(index)
                    .is_some_and(|sample| sample.at_micros < 120_000_000 && sample.target > 1)
            }),
            "the controller did not request forecast capacity before the first seasonal wave",
        ),
        PrincipalRegime::TimerWave => (
            (0..run.controller.len()).any(|index| {
                run.controller
                    .sample(index)
                    .is_some_and(|sample| sample.at_micros < 120_000_000 && sample.target > 1)
            }),
            "the controller did not request capacity before the known timer wave",
        ),
        _ => return Ok(()),
    };
    require_regime(
        invariant.0,
        regime,
        RegimeExperiment::ClosedLoop,
        invariant.1,
    )
}

fn placement_constraint_binds(run: &PrincipalRun) -> bool {
    (0..run.controller.len()).any(|index| {
        let Some(sample) = run.controller.sample(index) else {
            return false;
        };
        let Some(losses) = run.controller.decision_expected_losses(index) else {
            return false;
        };
        let Some((&one_replica, &maximum_replicas)) = losses.first().zip(losses.last()) else {
            return false;
        };
        !sample.hold
            && sample.target == 1
            && one_replica > 0.0_f64
            && (one_replica - maximum_replicas).abs() <= 1.0e-9_f64
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
        window_count > 0,
        regime,
        experiment,
        "the controller produced no passive resource window",
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
    Ok(())
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
    let capacity_regime = is_capacity_regime(regime);
    let slots_per_replica = DEFAULT_CONCURRENCY_PER_REPLICA;
    let plant_configuration = principal_plant_configuration(
        capacity_regime,
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
    let stop = run_schedule(&mut harness, definition.schedule)?;
    let (simulation, graph) = harness.finish_with_graph();
    let (controller, graph) = graph.into_parts();
    Ok(PrincipalRun {
        simulation,
        controller,
        inputs: graph.inputs.into_series_history(),
        stop,
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
    capacity_regime: bool,
    event_count_max: u32,
    slots_per_replica: u32,
    shared_resource_parallelism: u32,
) -> Result<PlantConfiguration, PlantError> {
    let configuration = PlantConfiguration::new(
        64,
        1_024,
        event_count_max,
        event_count_max,
        slots_per_replica,
        shared_resource_parallelism,
    )?;
    Ok(if capacity_regime {
        configuration.with_rebalance(0, 0)
    } else {
        configuration
    })
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
        partition_count: 64,
        replica_count_max,
        slots_per_replica,
        posterior_sample_count: 1_024,
        objective: ServiceObjective::new(regime.budget_micros(), 0.01)?,
    };
    let capacity_grid = capacity_grid(capacity_regime, sensitivity)?;
    let graph = ClosedLoop::new(
        PrincipalGraph::new(definition)?,
        &controller_configuration,
        capacity_grid,
        definition.schedule.controller_sample_count_max()?,
    )?
    .with_diagnostic_seed(definition.inputs.seed);
    let graph = if capacity_regime {
        graph.with_controller_actuation_schedule(0, 15_000_000)?
    } else {
        graph
    };
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
    let service_times_seconds: &[f64] = if capacity_regime {
        &[0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64]
    } else {
        &[
            0.000_5_f64,
            0.001_f64,
            0.002_f64,
            0.01_f64,
            0.1_f64,
            1.0_f64,
            10.0_f64,
            60.0_f64,
            600.0_f64,
        ]
    };
    let prior = if capacity_regime {
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

fn run_schedule(
    harness: &mut SimulationHarness<ClosedLoop<PrincipalGraph>, PrincipalAttemptModel>,
    schedule: RunSchedule,
) -> Result<RunStop, PrincipalRunError> {
    let mut at_micros = schedule.start_micros;
    let mut stable_count = 0_u8;
    loop {
        let snapshot = harness.tick(at_micros)?;
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
            StopCondition::FixedDuration { reason }
                if at_micros >= schedule.workload_end_micros =>
            {
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
    metric_window_micros: u64,
}

impl PrincipalRun {
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
    fn update(&mut self, _inputs: TickInputs) {}
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

    fn calculate(&self, _context: SeriesContext<'_, AttemptFrame>, (): ()) -> Self::Output {
        self.0.capacity_per_second
    }
}

struct AttemptResourceBaseTime(u32);

impl SeriesFunction<AttemptFrame, (u32,)> for AttemptResourceBaseTime {
    type Output = u64;

    fn calculate(
        &self,
        _context: SeriesContext<'_, AttemptFrame>,
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
        _context: SeriesContext<'_, AttemptFrame>,
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
        _context: SeriesContext<'_, AttemptFrame>,
        (dependency_operation_micros, handler_added_micros): (u64, u64),
    ) -> Self::Output {
        AttemptParameters {
            dependency_operation_micros,
            handler_added_micros,
        }
    }
}

struct PrincipalGraph {
    events: EventPolicy,
    reporter: ReporterPolicy,
    inputs: PrincipalInputGraph,
}

impl PrincipalGraph {
    fn new(definition: PrincipalDefinition) -> Result<Self, PlantError> {
        let history_count_max = definition.schedule.controller_sample_count_max()?;
        Ok(Self {
            events: definition.events,
            reporter: definition.reporter,
            inputs: PrincipalInputGraph::new(definition.inputs, history_count_max)?,
        })
    }
}

impl TickGenerator for PrincipalGraph {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(self.inputs.evaluate(context.now_micros, context))
    }

    fn event(&self, context: EventContext<'_>) -> EventInputs {
        let event_index = context.event_index;
        EventInputs {
            partition: self.events.partitions.index(event_index, 64),
            key: self.events.keys.index(event_index, 1_024),
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            transient_failures: self.events.transient_failures.at(event_index),
            permanent_rejection: self.events.permanent_rejections.matches(event_index),
        }
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
            PrincipalRegime::Idle => standard.messages(ArrivalSeries::None),
            PrincipalRegime::ApplicationLimited | PrincipalRegime::SnapshotFaults => standard,
            PrincipalRegime::LinearThroughput => {
                Self::capacity_closed_loop(100, 100).handler(100_000)
            }
            PrincipalRegime::FlatPostKnee => {
                Self::capacity_closed_loop(100, 100).shared_resource(64, 320, 0)
            }
            PrincipalRegime::DecliningPostKnee => {
                Self::capacity_closed_loop(100, 100).shared_resource(64, 320, 2)
            }
            PrincipalRegime::ShortBurst => standard
                .messages(ArrivalSeries::Once(EVENT_COUNT))
                .handler(100_000)
                .schedule(RunSchedule::one_shot())
                .initial_replicas(1),
            PrincipalRegime::SeasonalWaves => {
                let demand = ArrivalSeries::PeriodicDelayed {
                    count: 1_000,
                    first_micros: 120_000_000,
                    interval_micros: 120_000_000,
                    count_max: SEASONAL_EVENT_COUNT,
                };
                standard
                    .messages(demand)
                    .handler(100_000)
                    .history(HistoricalSeries {
                        demand,
                        replicas: 4,
                    })
                    .schedule(RunSchedule::seasonal())
                    .event_count_max(SEASONAL_EVENT_COUNT)
                    .initial_replicas(1)
            }
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
                    first_micros: 120_000_000,
                    interval_micros: 120_000_000,
                    count_max: EVENT_COUNT,
                })
                .handler(100_000)
                .schedule(RunSchedule::timer_wave())
                .initial_replicas(1),
            PrincipalRegime::HotSerializedKey => standard.keys(IndexSeries::Single),
            PrincipalRegime::TransientFailures => {
                standard.transient_failures(FailureSeries::Every {
                    interval: 10,
                    transient_count: 2,
                })
            }
            PrincipalRegime::PermanentRejections => {
                standard.permanent_rejections(OccurrenceSeries::Every(10))
            }
            PrincipalRegime::RebalanceStorm => standard
                .scale(ScaleSeries::RebalanceStorm)
                .launch_delay(LaunchDelaySeries::Immediate),
            PrincipalRegime::HandlerContention => standard
                .messages(ArrivalSeries::Periodic {
                    count: 400,
                    interval_micros: 500_000,
                    count_max: EVENT_COUNT,
                })
                .schedule(RunSchedule::handler_contention()),
            PrincipalRegime::LooseBudgetBacklog | PrincipalRegime::ReplicaCeiling => standard
                .messages(ArrivalSeries::Once(EVENT_COUNT))
                .handler(1_000_000)
                .schedule(RunSchedule::one_shot()),
            PrincipalRegime::MissingReporter => {
                standard.reporter(ReporterPolicy::MissingAfter { at_micros: 500_000 })
            }
            PrincipalRegime::AggregatorReplacement => {
                standard.reporter(ReporterPolicy::ReplaceAt {
                    at_micros: 1_000_000,
                })
            }
            PrincipalRegime::HistoricalMatch => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 1_000,
                    count_max: HISTORY_EVENT_COUNT_MAX,
                })
                .history(HistoricalSeries::standard())
                .schedule(RunSchedule::history())
                .event_count_max(HISTORY_EVENT_COUNT_MAX),
            PrincipalRegime::HistoricalExceeded => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 2_000,
                    count_max: HISTORY_EVENT_COUNT_MAX,
                })
                .history(HistoricalSeries::standard())
                .schedule(RunSchedule::history())
                .event_count_max(HISTORY_EVENT_COUNT_MAX),
            PrincipalRegime::HistoricalUnder => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 500,
                    count_max: HISTORY_EVENT_COUNT_MAX,
                })
                .history(HistoricalSeries::standard())
                .schedule(RunSchedule::history())
                .event_count_max(HISTORY_EVENT_COUNT_MAX),
            PrincipalRegime::HistoricalMissing => standard
                .messages(ArrivalSeries::Rate {
                    per_second: 1_000,
                    count_max: HISTORY_EVENT_COUNT_MAX,
                })
                .history(HistoricalSeries::missing())
                .schedule(RunSchedule::history())
                .event_count_max(HISTORY_EVENT_COUNT_MAX),
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
                count_max: CAPACITY_EVENT_COUNT_MAX,
            })
            .launch_delay(LaunchDelaySeries::Uniform {
                minimum_micros: 30_000_000,
                maximum_micros: 90_000_000,
            })
            .schedule(RunSchedule::capacity_response())
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
    demand: ArrivalSeries,
    replicas: u32,
}

impl HistoricalSeries {
    const fn standard() -> Self {
        Self {
            demand: ArrivalSeries::Rate {
                per_second: 1_000,
                count_max: HISTORY_EVENT_COUNT_MAX,
            },
            replicas: 8,
        }
    }

    const fn missing() -> Self {
        Self {
            demand: ArrivalSeries::None,
            replicas: 0,
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

impl ArrivalSeries {
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

    fn at_seeded(
        self,
        now_micros: u64,
        previous_micros: u64,
        emitted: u32,
        seed: u64,
        domain: u64,
        stochastic: bool,
    ) -> u32 {
        if !stochastic {
            return self.at(now_micros, emitted);
        }
        let Some((mean, count_max)) = self.interval_mean(previous_micros, now_micros) else {
            return self.at(now_micros, emitted);
        };
        let remaining = count_max.saturating_sub(emitted);
        let mut random = RandomStream::new(seed).domain(domain ^ now_micros);
        sample_poisson(mean, &mut random).min(remaining)
    }

    fn interval_mean(self, start_micros: u64, end_micros: u64) -> Option<(f64, u32)> {
        match self {
            Self::Rate {
                per_second,
                count_max,
            } => Some((
                f64::from(per_second)
                    * std::time::Duration::from_micros(end_micros.saturating_sub(start_micros))
                        .as_secs_f64(),
                count_max,
            )),
            Self::StaircaseRate {
                initial_per_second,
                increment_per_second,
                step_interval_micros,
                count_max,
            } => {
                let start = staircase_event_micros(
                    start_micros,
                    initial_per_second,
                    increment_per_second,
                    step_interval_micros,
                );
                let end = staircase_event_micros(
                    end_micros,
                    initial_per_second,
                    increment_per_second,
                    step_interval_micros,
                );
                Some((
                    count_as_f64(end.saturating_sub(start)) / 1_000_000.0_f64,
                    count_max,
                ))
            }
            Self::None | Self::Once(_) | Self::Periodic { .. } | Self::PeriodicDelayed { .. } => {
                None
            }
        }
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

fn count_as_f64(value: u64) -> f64 {
    let high = (value >> 32_u32) as u32;
    let low = value as u32;
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn bounded_count(count: u64, count_max: u32) -> u32 {
    match u32::try_from(count.min(u64::from(count_max))) {
        Ok(count) => count,
        Err(_) => count_max,
    }
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
struct RunSchedule {
    start_micros: u64,
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
            workload_end_micros: 2_000_000,
            workload_interval_micros: 100_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 300_000_000,
            stop: StopCondition::IdleStable { sample_count: 3 },
        }
    }

    const fn capacity_evidence() -> Self {
        Self {
            start_micros: 0,
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
            workload_end_micros: 600_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 630_000_000,
            stop: StopCondition::FixedDuration {
                reason: RunStopReason::DurationComplete,
            },
        }
    }

    const fn one_shot() -> Self {
        Self {
            start_micros: 1_000_000,
            workload_end_micros: 1_000_000,
            ..Self::standard()
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
            workload_end_micros: 180_000_000,
            workload_interval_micros: 1_000_000,
            followup_interval_micros: 1_000_000,
            maximum_micros: 180_000_000,
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
            workload_end_micros: 30_000_000,
            workload_interval_micros: 100_000,
            followup_interval_micros: 100_000,
            maximum_micros: 30_000_000,
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

series_graph! {
    struct PrincipalInputGraph(TickContext<'_>) with (policies: InputPolicies) {
        series message_count: u32 ["message arrivals", Count, Input] =
            MessageDemand(policies.messages, policies.seed, policies.stochastic_arrivals) =>
            () previous (message_emitted);
        series timer_count: u32 ["timer arrivals", Count, Input] =
            TimerDemand(policies.timers, policies.seed, policies.stochastic_arrivals) =>
            () previous (timer_emitted);
        series message_emitted: u32 ["messages emitted", Count, State] =
            EmittedEvents {} => (message_count) previous (message_emitted);
        series timer_emitted: u32 ["timers emitted", Count, State] =
            EmittedEvents {} => (timer_count) previous (timer_emitted);
        series historical_message_count: u32 ["historical demand", Count, Input] =
            HistoricalDemand(
                policies.history.demand,
                policies.seed,
                policies.stochastic_arrivals,
            ) => () previous (historical_emitted);
        series historical_emitted: u32 ["historical demand emitted", Count, State] =
            EmittedEvents {} => (historical_message_count) previous (historical_emitted);
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

fn previous_tick_micros(context: &TickContext<'_>) -> u64 {
    context
        .history
        .now_micros(0)
        .map_or(context.now_micros, |previous| previous)
}

struct MessageDemand(ArrivalSeries, u64, bool);

impl SeriesFunction<TickContext<'_>, (Option<u32>,)> for MessageDemand {
    type Output = u32;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (emitted_events,): (Option<u32>,),
    ) -> Self::Output {
        self.0.at_seeded(
            context.frame.now_micros,
            previous_tick_micros(&context.frame),
            emitted_events.unwrap_or(0),
            self.1,
            0x6d65_7373_6167_6573,
            self.2,
        )
    }
}

struct TimerDemand(ArrivalSeries, u64, bool);

impl SeriesFunction<TickContext<'_>, (Option<u32>,)> for TimerDemand {
    type Output = u32;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (emitted_events,): (Option<u32>,),
    ) -> Self::Output {
        self.0.at_seeded(
            context.frame.now_micros,
            previous_tick_micros(&context.frame),
            emitted_events.unwrap_or(0),
            self.1,
            0x7469_6d65_7273,
            self.2,
        )
    }
}

struct HistoricalDemand(ArrivalSeries, u64, bool);

impl SeriesFunction<TickContext<'_>, (Option<u32>,)> for HistoricalDemand {
    type Output = u32;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (emitted_events,): (Option<u32>,),
    ) -> Self::Output {
        self.0.at_seeded(
            context.frame.now_micros,
            previous_tick_micros(&context.frame),
            emitted_events.unwrap_or(0),
            self.1,
            0x0068_6973_746f_7279,
            self.2,
        )
    }
}

struct HistoricalReplicas(u32);

impl SeriesFunction<TickContext<'_>, ()> for HistoricalReplicas {
    type Output = u32;

    fn calculate(&self, _context: SeriesContext<'_, TickContext<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct EmittedEvents;

impl SeriesFunction<TickContext<'_>, (u32, Option<u32>)> for EmittedEvents {
    type Output = u32;

    fn calculate(
        &self,
        _context: SeriesContext<'_, TickContext<'_>>,
        (count, emitted): (u32, Option<u32>),
    ) -> Self::Output {
        emitted.unwrap_or(0).saturating_add(count)
    }
}

struct ScaleInput(ScaleSeries);

impl SeriesFunction<TickContext<'_>, (Option<u32>,)> for ScaleInput {
    type Output = u32;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (previous,): (Option<u32>,),
    ) -> Self::Output {
        self.0.at(context.frame.now_micros, previous)
    }
}

struct ScaleChanged;

impl SeriesFunction<TickContext<'_>, (u32, Option<u32>)> for ScaleChanged {
    type Output = bool;

    fn calculate(
        &self,
        _context: SeriesContext<'_, TickContext<'_>>,
        (target, previous): (u32, Option<u32>),
    ) -> Self::Output {
        target > 0 && Some(target) != previous
    }
}

struct HandlerDuration(u64);

impl SeriesFunction<TickContext<'_>, ()> for HandlerDuration {
    type Output = u64;

    fn calculate(&self, _context: SeriesContext<'_, TickContext<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct SharedResourceCapacity(u32);

impl SeriesFunction<TickContext<'_>, ()> for SharedResourceCapacity {
    type Output = u32;

    fn calculate(&self, _context: SeriesContext<'_, TickContext<'_>>, (): ()) -> Self::Output {
        self.0
    }
}

struct SharedResourceServiceTime(u32);

impl SeriesFunction<TickContext<'_>, (u32,)> for SharedResourceServiceTime {
    type Output = u64;

    fn calculate(
        &self,
        _context: SeriesContext<'_, TickContext<'_>>,
        (capacity_per_second,): (u32,),
    ) -> Self::Output {
        u64::from(self.0)
            .saturating_mul(1_000_000)
            .div_ceil(u64::from(capacity_per_second.max(1)))
    }
}

struct LaunchDelay(LaunchDelaySeries, u64);

impl SeriesFunction<TickContext<'_>, ()> for LaunchDelay {
    type Output = u64;

    fn calculate(&self, context: SeriesContext<'_, TickContext<'_>>, (): ()) -> Self::Output {
        self.0.at(context.frame.now_micros, self.1)
    }
}

struct PrincipalInputs;

impl OutputFunction<TickContext<'_>, (u32, u32, u32, bool, u64, u64, u64)> for PrincipalInputs {
    type Output = TickInputs;

    fn calculate(
        &self,
        _context: SeriesContext<'_, TickContext<'_>>,
        values: (u32, u32, u32, bool, u64, u64, u64),
    ) -> Self::Output {
        TickInputs {
            message_count: values.0,
            timer_count: values.1,
            handler_micros: values.4,
            dependency_operations: 1,
            dependency_operation_micros: values.5,
            handler_added_micros: 0,
            transient_failures: 0,
            permanent_rejection_every: 0,
            launch_delay_micros: values.6,
            scale: if values.2 == 0 {
                ScaleDirective::Hold
            } else if values.3 {
                ScaleDirective::Request {
                    replicas: values.2,
                    delay_micros: 0,
                }
            } else {
                ScaleDirective::ExternalHold
            },
        }
    }
}

#[cfg(test)]
#[path = "regime_tests.rs"]
mod tests;
