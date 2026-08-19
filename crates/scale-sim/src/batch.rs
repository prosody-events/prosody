use std::num::TryFromIntError;
use std::time::Duration;

use prosody_scale_core::{
    ArrivalPrior, ArrivalPriorError, CapacityGrid, CapacityGridError, Cohort, Configuration,
    ConfigurationError, DemandClass, LaunchPrior, ModelTime, ObservationBuffer, ObservationError,
    RandomStream, RebalancePrior, ReliabilityPrior, ScaleDecision, ScaleState, ServiceObjective,
    step,
};
use thiserror::Error;

use crate::{
    DEFAULT_CONCURRENCY_PER_REPLICA, EventContext, EventInputs, InputError, PlantConfiguration,
    PlantError, QuantileTable, ScaleDirective, SimulationHarness, SimulationResult, TickContext,
    TickGenerator, TickInputs,
};

const PARTITION_COUNT: u32 = 64;
const EVENT_COUNT: u32 = 50_000;
const REPLICA_COUNT_MAX: u32 = 64;
const INITIAL_REPLICAS: u32 = 1;

/// Replayable external inputs for the batch objective sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchInputs {
    pod_readiness_micros: QuantileTable,
    seed: u64,
}

impl BatchInputs {
    /// Constructs batch inputs from a pod readiness distribution.
    #[must_use]
    pub const fn new(pod_readiness_micros: QuantileTable, seed: u64) -> Self {
        Self {
            pod_readiness_micros,
            seed,
        }
    }
}

/// Result of one batch objective evaluated by the controller and plant.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BatchSloSummary {
    /// Reproducible source and artifact identity.
    pub metadata: crate::ReportMetadata,
    /// Configured latency budget.
    pub budget_micros: u64,
    /// Configured tolerated miss fraction.
    pub epsilon: f64,
    /// Replica target selected before the batch starts.
    pub target: u32,
    /// Actual replica count when the controller makes its decision.
    pub initial_replicas: u32,
    /// Time when launch, rebalance, and warm-up make the request ready.
    pub actuation_micros: u64,
    /// Saturation cap selected before the batch starts.
    pub cap: u32,
    /// Fraction of jobs that exceeded the latency budget.
    pub miss_fraction: f64,
    /// Time when the final job settled.
    pub completion_micros: u64,
    /// Replica-seconds used through final settlement.
    pub replica_seconds: f64,
}

/// Runs the 50,000-job batch under one latency objective.
///
/// The controller sees the complete Kafka backlog before it selects the
/// initial replica count. The independent plant then executes that decision.
///
/// # Errors
///
/// Returns an error for an invalid objective, model input, or plant bound.
pub fn run_batch_slo(budget_micros: u64, epsilon: f64) -> Result<BatchSloSummary, BatchSloError> {
    let pod_readiness_micros =
        QuantileTable::new(&[30_000_000, 45_000_000, 60_000_000, 75_000_000, 90_000_000])?;
    run_batch_slo_with_inputs(
        budget_micros,
        epsilon,
        &BatchInputs::new(pod_readiness_micros, 0x0062_6174_6368),
    )
}

/// Runs the 50,000-job batch without a replica transition.
///
/// # Errors
///
/// Returns an error when a fixed plant bound is invalid.
pub fn run_batch_regime(initial_replicas: u32) -> Result<SimulationResult, PlantError> {
    run_batch_plant(initial_replicas, initial_replicas, 0)
}

/// Runs the batch under caller-supplied actuator inputs.
///
/// # Errors
///
/// Returns an error for an invalid objective, model input, or plant bound.
pub fn run_batch_slo_with_inputs(
    budget_micros: u64,
    epsilon: f64,
    inputs: &BatchInputs,
) -> Result<BatchSloSummary, BatchSloError> {
    let objective = ServiceObjective::new(budget_micros, epsilon, 3.0_f64)?;
    let configuration = Configuration {
        cohort_count_max: PARTITION_COUNT,
        calendar_segment_count_max: PARTITION_COUNT,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: PARTITION_COUNT,
        replica_count_max: REPLICA_COUNT_MAX,
        slots_per_replica: DEFAULT_CONCURRENCY_PER_REPLICA,
        posterior_sample_count: 4_096,
        report_interval_micros: budget_micros,
        resource_window_attempt_count_max: EVENT_COUNT
            .saturating_mul(u32::from(crate::MAX_RETRY_FAILURES) + 1),
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        // The plant releases no events after the backlog, so the authored
        // prior expects about one spurious arrival per day. A one-per-second
        // mean forecasts a phantom stream across the whole budget window and
        // erases the budget response the batch regime exists to measure.
        arrival_prior: ArrivalPrior::new(1.0_f64, 86_400.0_f64, 1.0_f64 / 86_400.0_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective,
    };
    let grid = CapacityGrid::new(
        &[60.0_f64, 120.0_f64, 240.0_f64, 480.0_f64, 600.0_f64],
        &[1_000.0_f64],
        &[0.0_f64],
    )?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let artifact_identity = state.capacity_artifact().identity();
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.advance_model_time(ModelTime::from_micros(1))?;
    let mut partition_events = [0_u32; PARTITION_COUNT as usize];
    for event_index in 0..EVENT_COUNT {
        let event = batch_event(event_index);
        let partition = event.partition as usize;
        partition_events[partition] += 1;
    }
    for partition in 0..PARTITION_COUNT {
        observation.push_cohort(Cohort {
            release_micros: 0,
            deadline_micros: budget_micros,
            offered_events: f64::from(partition_events[partition as usize]),
            partition,
            demand_class: DemandClass::Normal,
        })?;
    }
    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(BatchSloError::ControllerHold);
    };

    let mut random = RandomStream::new(inputs.seed).domain(budget_micros ^ u64::from(apply.target));
    let actuation_micros = if apply.target == INITIAL_REPLICAS {
        0
    } else {
        inputs.pod_readiness_micros.sample(&mut random)
    };
    let result = run_batch_plant(INITIAL_REPLICAS, apply.target, actuation_micros)?;
    let completion_micros = result
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros);
    let missed = result
        .settlements()
        .iter()
        .filter(|settlement| settlement.settle_micros > budget_micros)
        .count();
    let completed = u32::try_from(result.settlements().len())?;
    let missed = u32::try_from(missed)?;
    let miss_fraction = f64::from(missed) / f64::from(completed);
    let initial_seconds =
        Duration::from_micros(actuation_micros.min(completion_micros)).as_secs_f64();
    let scaled_seconds =
        Duration::from_micros(completion_micros.saturating_sub(actuation_micros)).as_secs_f64();
    let replica_seconds =
        f64::from(INITIAL_REPLICAS) * initial_seconds + f64::from(apply.target) * scaled_seconds;
    Ok(BatchSloSummary {
        metadata: crate::ReportMetadata::new(artifact_identity, inputs.seed, completion_micros),
        budget_micros,
        epsilon,
        target: apply.target,
        initial_replicas: INITIAL_REPLICAS,
        actuation_micros,
        cap: apply.cap,
        miss_fraction,
        completion_micros,
        replica_seconds,
    })
}

fn run_batch_plant(
    initial_replicas: u32,
    target: u32,
    delay_micros: u64,
) -> Result<SimulationResult, PlantError> {
    let configuration = PlantConfiguration::new(
        PARTITION_COUNT,
        1_024,
        EVENT_COUNT,
        1,
        DEFAULT_CONCURRENCY_PER_REPLICA,
        1_024,
    )?
    .with_rebalance(0, 0);
    let graph = BatchGraph {
        target,
        delay_micros,
    };
    let mut harness = SimulationHarness::new(configuration, initial_replicas, 1, graph)?;
    harness.tick(0)?;
    Ok(harness.finish())
}

const fn batch_event(event_index: u32) -> crate::EventSpec {
    let duration_seconds = 60_u64 + (event_index.wrapping_mul(137) % 541) as u64;
    crate::EventSpec {
        release_micros: 0,
        partition: event_index % PARTITION_COUNT,
        key: event_index % 1_024,
        handler_micros: duration_seconds * 1_000_000,
        dependency_operations: 0,
        outcome: crate::EventOutcome::Final(crate::FinalOutcome::Success),
        source: crate::EventSource::Message,
    }
}

struct BatchGraph {
    target: u32,
    delay_micros: u64,
}

impl TickGenerator for BatchGraph {
    fn calculate(&mut self, _: TickContext<'_>) -> Result<TickInputs, PlantError> {
        let scale = if self.target == INITIAL_REPLICAS {
            ScaleDirective::Hold
        } else {
            ScaleDirective::Request {
                replicas: self.target,
            }
        };
        Ok(TickInputs {
            message_count: EVENT_COUNT,
            timer_count: 0,
            handler_micros: 0,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: crate::EventOutcomeRule::Success,
            launch_delay_micros: self.delay_micros,
            scale,
        })
    }

    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        let event = batch_event(context.event_index);
        Ok(EventInputs {
            release_micros: event.release_micros,
            partition: event.partition,
            key: event.key,
            handler_micros: event.handler_micros,
            dependency_operations: event.dependency_operations,
            outcome: event.outcome,
        })
    }
}

/// Failure while evaluating one batch objective.
#[derive(Debug, Error)]
pub enum BatchSloError {
    /// A lead-time prior artifact is invalid.
    #[error(transparent)]
    LeadTimePrior(#[from] prosody_scale_core::LeadTimePriorError),
    /// The controller refused to apply a target.
    #[error("the controller held the batch decision")]
    ControllerHold,
    /// A count exceeded its fixed representation.
    #[error("a batch count exceeds u32")]
    Count(#[from] TryFromIntError),
    /// The resource grid is invalid.
    #[error(transparent)]
    CapacityGrid(#[from] CapacityGridError),
    /// The arrival prior is invalid.
    #[error(transparent)]
    ArrivalPrior(#[from] ArrivalPriorError),
    /// The controller configuration is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// The observation is invalid.
    #[error(transparent)]
    Observation(#[from] ObservationError),
    /// A caller-supplied input table is invalid.
    #[error(transparent)]
    Input(#[from] InputError),
    /// The plant configuration is invalid.
    #[error(transparent)]
    Plant(#[from] PlantError),
}
