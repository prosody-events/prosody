use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};

use crate::TransitionDirection;
use crate::arrival::ArrivalFactor;
use crate::capacity::{CapacityFactor, ThroughputPosteriorCell};
use crate::edf::{
    ArrivalPath, EdfOutcome, EdfScratch, EvaluationWindow, StepCandidates, SupplyStep,
    SupplyTrajectory, evaluate_empty_steps_simd, evaluate_prepared_step,
    evaluate_prepared_trajectory, prepare, required_capacity_prepared,
};
use crate::lead_time::{LeadTimeFactor, sample_index};
use crate::partition::PartitionFactor;
use crate::planning::{
    PredictiveObservations, group_particles, replica_seconds, root_event, select_root_action,
};
use crate::reliability::{RELIABILITY_BIN_COUNT, ReliabilityFactor};
use crate::types::{CalendarForecast, WorkCohorts};
use crate::{
    ApplyDecision, ArrivalPosterior, CapacityCurve, CapacityGrid, Configuration,
    ConfigurationError, DecisionDiagnostics, GroupObservation, HoldDecision, HoldReason, ModelTime,
    PosteriorError, PosteriorQuery, RandomStream, ScaleDecision,
};
use thiserror::Error;

const DECISION_SCENARIO_SEED: u64 = 0x7363_616c_652d_636f;
const DECISION_BOOTSTRAP_COUNT: u32 = 64;
const DECISION_SAMPLE_COUNT_MIN: u32 = 1_024;
const TRANSITION_SAMPLE_COUNT: usize = 8;
const STRATIFICATION_SHIFT: u64 = 0x9e37_79b9_7f4a_7c15;

/// One calculated reason that a posterior scenario rejects an action.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionRejection {
    /// Work misses a deadline during the root stage.
    RootDeadline = 1,
    /// Partition placement cannot serve the located work.
    PartitionPlacement = 2,
    /// Work misses a deadline during the recourse stage.
    RecourseDeadline = 4,
    /// Terminal capacity cannot drain the remaining work within one budget.
    TerminalBacklog = 8,
    /// Terminal capacity is below the sampled future arrival rate.
    FutureArrival = 16,
}

impl DecisionRejection {
    const fn bit(self) -> u8 {
        self as u8
    }
}

#[repr(u64)]
#[derive(Clone, Copy)]
pub(crate) enum DecisionRandomDomain {
    Reliability = 0x7265_6c69_6162_6c65,
    LeadTime = 0x6c65_6164_7469_6d65,
    Rebalance = 0x7265_6261_6c61_6e63,
    Placement = 0x706c_6163_656d_656e,
    Commitment = 0x636f_6d6d_6974_6d65,
    Arrival = 0x6172_7269_7661_6c73,
    Bootstrap = 0x626f_6f74_7374_7261,
}

/// All posterior and transition state that survives a controller tick.
pub struct ScaleState {
    simd_level: Level,
    configuration: Configuration,
    model_time: ModelTime,
    arrivals: ArrivalFactor,
    capacity: CapacityFactor,
    reliability: ReliabilityFactor,
    partition_placement: PartitionFactor,
    lead_time: LeadTimeFactor,
    rebalance_time: LeadTimeFactor,
    current_replicas: u32,
}

impl ScaleState {
    /// Allocates bounded posterior state from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when a configuration bound is invalid.
    pub fn new(
        configuration: Configuration,
        capacity_grid: CapacityGrid,
    ) -> Result<Self, ConfigurationError> {
        configuration.validate()?;
        let partition_placement = PartitionFactor::new(configuration.partition_count)?;
        let reliability = ReliabilityFactor::new(configuration.reliability_prior);
        let lead_time = LeadTimeFactor::new(&configuration.launch_time_prior);
        let rebalance_time = LeadTimeFactor::new(&configuration.rebalance_time_prior);
        let arrivals = ArrivalFactor::new(configuration.arrival_prior);
        let capacity =
            CapacityFactor::new(capacity_grid, configuration.capacity_change_rate_per_second);
        Ok(Self {
            simd_level: Level::new(),
            configuration,
            model_time: ModelTime::from_micros(0),
            arrivals,
            capacity,
            reliability,
            partition_placement,
            lead_time,
            rebalance_time,
            current_replicas: 1,
        })
    }

    /// Returns the fixed configuration.
    #[must_use]
    pub const fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    /// Returns the fixed number of marginal capacity values.
    #[must_use]
    pub const fn capacity_posterior_value_count(&self) -> u32 {
        self.capacity.posterior_value_count()
    }

    /// Returns the fixed number of joint capacity curves.
    #[must_use]
    pub fn throughput_posterior_value_count(&self) -> u32 {
        self.capacity.curve_posterior_value_count()
    }

    /// Writes the joint posterior throughput at one concurrency.
    ///
    /// # Errors
    ///
    /// Returns an error when the buffer has the wrong fixed length.
    pub fn write_throughput_posterior(
        &self,
        concurrency: f64,
        cells: &mut [ThroughputPosteriorCell],
    ) -> Result<(), PosteriorError> {
        self.capacity.write_throughput_posterior(concurrency, cells)
    }

    /// Writes the marginal capacity posterior into caller-owned buffers.
    ///
    /// # Errors
    ///
    /// Returns an error when either buffer has the wrong fixed length.
    pub fn write_capacity_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        self.capacity
            .write_capacity_posterior(values, probabilities)
    }

    /// Returns exact Gamma parameters for the arrival-rate posterior.
    #[must_use]
    pub fn arrival_posterior(&self) -> ArrivalPosterior {
        self.arrivals.posterior(self.model_time.as_micros())
    }

    /// Returns the posterior predictive CDF for one actuation duration.
    #[must_use]
    pub fn lead_time_predictive_cdf(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        elapsed_seconds: f64,
    ) -> f64 {
        self.lead_time
            .predictive_cdf(direction, replica_delta, elapsed_seconds)
    }

    /// Returns one posterior predictive actuation-duration quantile.
    #[must_use]
    pub fn lead_time_predictive_quantile(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        probability: f64,
    ) -> f64 {
        self.lead_time
            .predictive_quantile(direction, replica_delta, probability)
    }

    /// Returns the fixed value count for one discrete posterior view.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero lead-time replica change.
    pub fn posterior_value_count(&self, query: PosteriorQuery) -> Result<u32, PosteriorError> {
        match query {
            PosteriorQuery::Capacity => Ok(self.capacity.posterior_value_count()),
            PosteriorQuery::ServiceTime => Ok(self.capacity.service_time_posterior_value_count()),
            PosteriorQuery::Collapse => Ok(self.capacity.collapse_posterior_value_count()),
            PosteriorQuery::Knee => Ok(self.capacity.knee_posterior_value_count()),
            PosteriorQuery::SaturationState => Ok(2),
            PosteriorQuery::NormalRetryProbability | PosteriorQuery::FailureRetryProbability => {
                Ok(RELIABILITY_BIN_COUNT)
            }
            PosteriorQuery::PartitionShare => Ok(self.partition_placement.value_count()),
            PosteriorQuery::LeadTime {
                replica_delta: 0, ..
            }
            | PosteriorQuery::RebalanceTime {
                replica_delta: 0, ..
            } => Err(PosteriorError::ZeroReplicaDelta),
            PosteriorQuery::LeadTime { .. } | PosteriorQuery::RebalanceTime { .. } => {
                Ok(LeadTimeFactor::posterior_value_count())
            }
        }
    }

    /// Writes one discrete posterior into caller-owned buffers.
    ///
    /// # Errors
    ///
    /// Returns an error when a buffer length or query is invalid.
    pub fn write_posterior(
        &self,
        query: PosteriorQuery,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let expected = self.posterior_value_count(query)?;
        if values.len() != expected as usize || probabilities.len() != expected as usize {
            return Err(PosteriorError::BufferLength { expected });
        }
        match query {
            PosteriorQuery::Capacity => self
                .capacity
                .write_capacity_posterior(values, probabilities),
            PosteriorQuery::ServiceTime => self
                .capacity
                .write_service_time_posterior(values, probabilities),
            PosteriorQuery::Collapse => self
                .capacity
                .write_collapse_posterior(values, probabilities),
            PosteriorQuery::Knee => self.capacity.write_knee_posterior(values, probabilities),
            PosteriorQuery::SaturationState => {
                values.copy_from_slice(&[0.0_f64, 1.0_f64]);
                probabilities[0] = 1.0_f64 - self.capacity.no_knee_probability();
                probabilities[1] = self.capacity.no_knee_probability();
                Ok(())
            }
            PosteriorQuery::NormalRetryProbability => self
                .reliability
                .write_normal_posterior(values, probabilities),
            PosteriorQuery::FailureRetryProbability => self
                .reliability
                .write_failure_posterior(values, probabilities),
            PosteriorQuery::PartitionShare => {
                for (partition, value) in values.iter_mut().enumerate() {
                    *value = f64::from(partition as u32);
                }
                if self
                    .partition_placement
                    .write_expected_shares(probabilities)
                {
                    Ok(())
                } else {
                    Err(PosteriorError::BufferLength { expected })
                }
            }
            PosteriorQuery::LeadTime {
                direction,
                replica_delta,
            } => {
                if self
                    .lead_time
                    .write_posterior(direction, replica_delta, values, probabilities)
                {
                    Ok(())
                } else {
                    Err(PosteriorError::BufferLength { expected })
                }
            }
            PosteriorQuery::RebalanceTime {
                direction,
                replica_delta,
            } => {
                if self.rebalance_time.write_posterior(
                    direction,
                    replica_delta,
                    values,
                    probabilities,
                ) {
                    Ok(())
                } else {
                    Err(PosteriorError::BufferLength { expected })
                }
            }
        }
    }
}

/// Reusable memory for one controller transition.
pub struct ScaleScratch {
    placement_edf: EdfScratch,
    handler_cohorts: WorkCohorts,
    resource_cohorts: WorkCohorts,
    placement_cohorts: WorkCohorts,
    partition_offsets: Vec<u32>,
    partition_write_offsets: Vec<u32>,
    partition_cohort_indexes: Vec<u32>,
    partition_work_slot_seconds: Vec<f64>,
    partition_order: Vec<u32>,
    partition_share_draws: Vec<f64>,
    moved_partition_share: Vec<f64>,
    moved_partition_counts: Vec<u32>,
    transition_sample_indexes: Vec<u8>,
    active_partition_count: u32,
    placement_interval_seconds: f64,
    partition_shortfall: f64,
    posterior_pass_counts: Vec<f64>,
    posterior_loss_sums: Vec<f64>,
    posterior_replica_seconds_sums: Vec<f64>,
    bootstrap_pass_counts: Vec<f64>,
    bootstrap_loss_sums: Vec<f64>,
    bootstrap_replica_seconds_sums: Vec<f64>,
    bootstrap_target_counts: Vec<u32>,
    mpc_pass_counts: Vec<f64>,
    mpc_loss_sums: Vec<f64>,
    mpc_replica_seconds_sums: Vec<f64>,
    candidate_concurrency: Vec<f64>,
    posterior_resource_supply: Vec<f64>,
    scenario_shortfall: Vec<f64>,
    scenario_delay_area: Vec<f64>,
    scenario_terminal_work: Vec<f64>,
    scenario_replica_seconds: Vec<f64>,
    scenario_root_pass: Vec<u8>,
    scenario_rejection: Vec<u8>,
    scenario_pass: Vec<u8>,
    scenario_supply: Vec<f64>,
    scenario_lead_seconds: Vec<f64>,
    scenario_rebalance_seconds: Vec<f64>,
    scenario_moved_partition_share: Vec<f64>,
    scenario_arrival_path_length: Vec<u32>,
    scenario_arrival_path_end_seconds: Vec<f64>,
    scenario_arrival_path_rates: Vec<f64>,
    scenario_horizon_micros: Vec<u64>,
    scenario_planning_horizon_seconds: Vec<f64>,
    scenario_future_rate: Vec<f64>,
    scenario_event_count: Vec<f64>,
    scenario_partition_delay_area: Vec<f64>,
    predictive_elapsed_micros: Vec<u64>,
    predictive_arrivals: Vec<u32>,
    predictive_completions: Vec<u32>,
    predictive_backlog: Vec<u32>,
    predictive_warm_replicas: Vec<u32>,
    predictive_transition_complete: Vec<u8>,
    particle_order: Vec<u32>,
    node_offsets: Vec<u32>,
    recourse_loss: Vec<f64>,
    recourse_replica_seconds: Vec<f64>,
    recourse_rejection: Vec<u8>,
    recourse_pass: Vec<u8>,
    recourse_workspaces: Vec<RecourseWorkspace>,
    deterministic_loss: Vec<f64>,
    trajectory_offsets: Vec<u32>,
    trajectory: TrajectoryColumns,
    commitment_pause_seconds: Vec<f64>,
    scenario_commitment_pause_seconds: Vec<f64>,
    resource_debt_events: f64,
    active_scenario_count: usize,
    decision_curve_sample_count: u32,
}

struct RecourseWorkspace {
    edf: EdfScratch,
    cohorts: WorkCohorts,
    pause_seconds: Vec<f64>,
    ready_seconds: Vec<f64>,
    during_supply: Vec<f64>,
    delay_area: Vec<f64>,
    terminal_work: Vec<f64>,
    deadline_shortfall: Vec<f64>,
    predictive_elapsed_micros: Vec<u64>,
    predictive_arrivals: Vec<u32>,
    predictive_completions: Vec<u32>,
    predictive_backlog: Vec<u32>,
    predictive_warm_replicas: Vec<u32>,
    predictive_transition_complete: Vec<u8>,
}

struct TrajectoryColumns {
    targets: Vec<u32>,
    pause_seconds: Vec<f64>,
    ready_seconds: Vec<f64>,
    during_supply: Vec<f64>,
    after_supply: Vec<f64>,
}

struct ScratchBounds {
    work_cohort_count_max: usize,
    work_cohort_count_max_u32: u32,
    partition_count: usize,
    partition_offset_count: usize,
    replica_count_max: usize,
    trajectory_event_count_max: usize,
    posterior_sample_count: usize,
    scenario_cell_count: usize,
    transition_cell_count: usize,
    moved_share_cell_count: usize,
    arrival_path_cell_count: usize,
    scenario_commitment_cell_count: usize,
}

#[derive(Clone, Copy)]
struct RecourseParticle {
    root_target_index: usize,
    particle: usize,
}

#[derive(Clone, Copy)]
struct RootBoundary {
    supply: f64,
    warm_replicas: u32,
    transition_complete: bool,
}

struct RootForecast<'a> {
    scenario: usize,
    normal_events: f64,
    failure_events: f64,
    current_supply: f64,
    event_supply_factor: f64,
    curve: CapacityCurve,
    lead_seconds: &'a [f64; TRANSITION_SAMPLE_COUNT],
    rebalance_seconds: &'a [f64; TRANSITION_SAMPLE_COUNT],
    calendar: Option<CalendarForecast<'a>>,
}

struct RootCandidateShared<'a> {
    model_time_micros: u64,
    deadline_budget_micros: u64,
    current_supply: f64,
    resource_debt_events: f64,
    resource_cohorts: &'a WorkCohorts,
    trajectory_offsets: &'a [u32],
    trajectory_pause_seconds: &'a [f64],
    trajectory_ready_seconds: &'a [f64],
    trajectory_during_supply: &'a [f64],
    trajectory_after_supply: &'a [f64],
    horizon_micros: &'a [u64],
    deterministic_loss: &'a [f64],
    arrival_path: ArrivalPath<'a>,
}

struct EmptyRecourse<'a> {
    after_supply: &'a [f64],
    deterministic_loss: &'a [f64],
    delay_area: &'a [f64],
    terminal_work: &'a mut [f64],
    deadline_shortfall: &'a [f64],
    root_delay_area: f64,
    partition_delay_area: f64,
    future_rate: f64,
    budget_seconds: f64,
    event_seconds: f64,
}

struct RecourseShared<'a> {
    model_time_micros: u64,
    current_replicas: u32,
    candidate_count: usize,
    action_count: usize,
    path_segment_count: usize,
    moved_share_count: usize,
    budget_seconds: f64,
    resource_debt_events: f64,
    resource_cohorts: &'a WorkCohorts,
    scenario_horizon_micros: &'a [u64],
    scenario_terminal_work: &'a [f64],
    scenario_arrival_path_length: &'a [u32],
    scenario_arrival_path_end_seconds: &'a [f64],
    scenario_arrival_path_rates: &'a [f64],
    scenario_supply: &'a [f64],
    scenario_lead_seconds: &'a [f64],
    scenario_rebalance_seconds: &'a [f64],
    scenario_moved_partition_share: &'a [f64],
    scenario_planning_horizon_seconds: &'a [f64],
    scenario_future_rate: &'a [f64],
    scenario_event_count: &'a [f64],
    scenario_delay_area: &'a [f64],
    scenario_partition_delay_area: &'a [f64],
    moved_partition_counts: &'a [u32],
    transition_sample_indexes: &'a [u8],
    actuation_commitments: &'a crate::types::ActuationCommitments,
    scenario_commitment_pause_seconds: &'a [f64],
    deterministic_loss: &'a [f64],
}

impl ScratchBounds {
    fn new(configuration: &Configuration) -> Result<Self, ConfigurationError> {
        let cohort_count_max = usize::try_from(configuration.cohort_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_count = usize::try_from(configuration.partition_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let replica_count_max = usize::try_from(configuration.replica_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let work_cohort_count_max = partition_count
            .checked_mul(crate::DemandClass::COUNT_USIZE)
            .and_then(|count| cohort_count_max.checked_add(count))
            .ok_or(ConfigurationError::PlatformLimit)?;
        let partition_offset_count = partition_count
            .checked_add(1)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let posterior_sample_count = usize::try_from(configuration.posterior_sample_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let scenario_cell_count = posterior_sample_count
            .checked_mul(replica_count_max)
            .ok_or(ConfigurationError::PlatformLimit)?;
        Ok(Self {
            work_cohort_count_max,
            work_cohort_count_max_u32: u32::try_from(work_cohort_count_max)
                .map_err(|_| ConfigurationError::PlatformLimit)?,
            partition_count,
            partition_offset_count,
            replica_count_max,
            trajectory_event_count_max: replica_count_max
                .checked_mul(
                    replica_count_max
                        .checked_add(1)
                        .ok_or(ConfigurationError::PlatformLimit)?,
                )
                .ok_or(ConfigurationError::PlatformLimit)?,
            posterior_sample_count,
            scenario_cell_count,
            transition_cell_count: posterior_sample_count
                .checked_mul(TRANSITION_SAMPLE_COUNT)
                .ok_or(ConfigurationError::PlatformLimit)?,
            moved_share_cell_count: posterior_sample_count
                .checked_mul(partition_offset_count)
                .ok_or(ConfigurationError::PlatformLimit)?,
            arrival_path_cell_count: posterior_sample_count
                .checked_mul(configuration.arrival_prior.path_segment_count_max())
                .ok_or(ConfigurationError::PlatformLimit)?,
            scenario_commitment_cell_count: scenario_cell_count,
        })
    }
}

impl TrajectoryColumns {
    fn new(capacity: usize) -> Self {
        Self {
            targets: Vec::with_capacity(capacity),
            pause_seconds: Vec::with_capacity(capacity),
            ready_seconds: Vec::with_capacity(capacity),
            during_supply: Vec::with_capacity(capacity),
            after_supply: Vec::with_capacity(capacity),
        }
    }
}

impl RecourseWorkspace {
    fn new(
        cohort_count_max: u32,
        cohort_capacity: usize,
        candidate_count: usize,
        particle_capacity: usize,
    ) -> Result<Self, ConfigurationError> {
        Ok(Self {
            edf: EdfScratch::new(cohort_count_max)?,
            cohorts: WorkCohorts::new(cohort_capacity),
            pause_seconds: vec![0.0_f64; candidate_count],
            ready_seconds: vec![0.0_f64; candidate_count],
            during_supply: vec![0.0_f64; candidate_count],
            delay_area: vec![0.0_f64; candidate_count],
            terminal_work: vec![0.0_f64; candidate_count],
            deadline_shortfall: vec![0.0_f64; candidate_count],
            predictive_elapsed_micros: vec![0; particle_capacity],
            predictive_arrivals: vec![0; particle_capacity],
            predictive_completions: vec![0; particle_capacity],
            predictive_backlog: vec![0; particle_capacity],
            predictive_warm_replicas: vec![0; particle_capacity],
            predictive_transition_complete: vec![0; particle_capacity],
        })
    }
}

impl ScaleScratch {
    /// Allocates every temporary buffer at its validated maximum size.
    ///
    /// # Errors
    ///
    /// Returns an error when a configuration bound is invalid for this
    /// platform.
    pub fn new(configuration: &Configuration) -> Result<Self, ConfigurationError> {
        configuration.validate()?;
        let ScratchBounds {
            work_cohort_count_max,
            work_cohort_count_max_u32,
            partition_count,
            partition_offset_count,
            replica_count_max,
            trajectory_event_count_max,
            posterior_sample_count,
            scenario_cell_count,
            transition_cell_count,
            moved_share_cell_count,
            arrival_path_cell_count,
            scenario_commitment_cell_count,
        } = ScratchBounds::new(configuration)?;
        let candidate_concurrency = (1..=configuration.replica_count_max)
            .map(|replicas| f64::from(replicas) * f64::from(configuration.slots_per_replica))
            .collect::<Vec<_>>();
        let moved_partition_counts = moved_partition_count_matrix(
            configuration.partition_count,
            configuration.replica_count_max,
        )?;
        let transition_sample_indexes =
            transition_sample_index_matrix(configuration.replica_count_max)?;
        let worker_count = rayon::current_num_threads()
            .min(posterior_sample_count)
            .max(1);
        let worker_particle_count = posterior_sample_count.div_ceil(worker_count);
        let mut recourse_workspaces = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let particle_capacity = if worker == 0 {
                posterior_sample_count
            } else {
                worker_particle_count
            };
            recourse_workspaces.push(RecourseWorkspace::new(
                work_cohort_count_max_u32,
                work_cohort_count_max,
                replica_count_max,
                particle_capacity,
            )?);
        }
        Ok(Self {
            placement_edf: EdfScratch::new(work_cohort_count_max_u32)?,
            handler_cohorts: WorkCohorts::new(work_cohort_count_max),
            resource_cohorts: WorkCohorts::new(work_cohort_count_max),
            placement_cohorts: WorkCohorts::new(work_cohort_count_max),
            partition_offsets: vec![0; partition_offset_count],
            partition_write_offsets: vec![0; partition_count],
            partition_cohort_indexes: vec![0; work_cohort_count_max],
            partition_work_slot_seconds: vec![0.0_f64; partition_count],
            partition_order: vec![0; partition_count],
            partition_share_draws: vec![0.0_f64; partition_count],
            moved_partition_share: vec![0.0_f64; partition_offset_count],
            moved_partition_counts,
            transition_sample_indexes,
            active_partition_count: 0,
            placement_interval_seconds: 0.0_f64,
            partition_shortfall: 0.0_f64,
            posterior_pass_counts: vec![0.0_f64; replica_count_max],
            posterior_loss_sums: vec![0.0_f64; replica_count_max],
            posterior_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            bootstrap_pass_counts: vec![0.0_f64; replica_count_max],
            bootstrap_loss_sums: vec![0.0_f64; replica_count_max],
            bootstrap_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            bootstrap_target_counts: vec![0; replica_count_max],
            mpc_pass_counts: vec![0.0_f64; replica_count_max],
            mpc_loss_sums: vec![0.0_f64; replica_count_max],
            mpc_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            candidate_concurrency,
            posterior_resource_supply: vec![0.0_f64; replica_count_max],
            scenario_shortfall: vec![0.0_f64; scenario_cell_count],
            scenario_delay_area: vec![0.0_f64; scenario_cell_count],
            scenario_terminal_work: vec![0.0_f64; scenario_cell_count],
            scenario_replica_seconds: vec![0.0_f64; scenario_cell_count],
            scenario_root_pass: vec![0; scenario_cell_count],
            scenario_rejection: vec![0; scenario_cell_count],
            scenario_pass: vec![0; scenario_cell_count],
            scenario_supply: vec![0.0_f64; scenario_cell_count],
            scenario_lead_seconds: vec![0.0_f64; transition_cell_count],
            scenario_rebalance_seconds: vec![0.0_f64; transition_cell_count],
            scenario_moved_partition_share: vec![0.0_f64; moved_share_cell_count],
            scenario_arrival_path_length: vec![0; posterior_sample_count],
            scenario_arrival_path_end_seconds: vec![0.0_f64; arrival_path_cell_count],
            scenario_arrival_path_rates: vec![0.0_f64; arrival_path_cell_count],
            scenario_horizon_micros: vec![0; scenario_cell_count],
            scenario_planning_horizon_seconds: vec![0.0_f64; posterior_sample_count],
            scenario_future_rate: vec![0.0_f64; posterior_sample_count],
            scenario_event_count: vec![0.0_f64; posterior_sample_count],
            scenario_partition_delay_area: vec![0.0_f64; posterior_sample_count],
            predictive_elapsed_micros: vec![0; posterior_sample_count],
            predictive_arrivals: vec![0; posterior_sample_count],
            predictive_completions: vec![0; posterior_sample_count],
            predictive_backlog: vec![0; posterior_sample_count],
            predictive_warm_replicas: vec![0; posterior_sample_count],
            predictive_transition_complete: vec![0; posterior_sample_count],
            particle_order: vec![0; posterior_sample_count],
            node_offsets: Vec::with_capacity(
                posterior_sample_count
                    .checked_add(1)
                    .ok_or(ConfigurationError::PlatformLimit)?,
            ),
            recourse_loss: vec![0.0_f64; scenario_cell_count],
            recourse_replica_seconds: vec![0.0_f64; scenario_cell_count],
            recourse_rejection: vec![0; scenario_cell_count],
            recourse_pass: vec![0; scenario_cell_count],
            recourse_workspaces,
            deterministic_loss: vec![0.0_f64; replica_count_max],
            trajectory_offsets: vec![0; replica_count_max + 1],
            trajectory: TrajectoryColumns::new(trajectory_event_count_max),
            commitment_pause_seconds: vec![0.0_f64; replica_count_max],
            scenario_commitment_pause_seconds: vec![f64::INFINITY; scenario_commitment_cell_count],
            resource_debt_events: 0.0_f64,
            active_scenario_count: posterior_sample_count,
            decision_curve_sample_count: 0,
        })
    }

    /// Returns the fixed number of replica candidates.
    #[must_use]
    pub fn decision_candidate_count(&self) -> usize {
        self.posterior_loss_sums.len()
    }

    /// Writes the expected loss and SLO pass probability for each candidate.
    ///
    /// Candidate index zero represents one replica. The last index represents
    /// the configured replica limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid buffer or an unavailable decision.
    pub fn write_decision_curve(
        &self,
        expected_losses: &mut [f64],
        pass_probabilities: &mut [f64],
    ) -> Result<(), DecisionCurveError> {
        if expected_losses.len() != self.posterior_loss_sums.len()
            || pass_probabilities.len() != self.posterior_loss_sums.len()
        {
            return Err(DecisionCurveError::BufferLength {
                expected: self.posterior_loss_sums.len(),
            });
        }
        if self.decision_curve_sample_count == 0 {
            return Err(DecisionCurveError::Unavailable);
        }
        let sample_count = f64::from(self.decision_curve_sample_count);
        for index in 0..self.posterior_loss_sums.len() {
            expected_losses[index] = self.posterior_loss_sums[index] / sample_count;
            pass_probabilities[index] = self.posterior_pass_counts[index] / sample_count;
        }
        Ok(())
    }

    /// Writes the root-stage SLO pass probability for each candidate.
    ///
    /// The root stage ends at the first report or completed transition. The
    /// final pass curve also includes the selected recourse action.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid buffer or an unavailable decision.
    pub fn write_root_pass_curve(
        &self,
        pass_probabilities: &mut [f64],
    ) -> Result<(), DecisionCurveError> {
        if pass_probabilities.len() != self.posterior_loss_sums.len() {
            return Err(DecisionCurveError::BufferLength {
                expected: self.posterior_loss_sums.len(),
            });
        }
        if self.decision_curve_sample_count == 0 {
            return Err(DecisionCurveError::Unavailable);
        }
        pass_probabilities.fill(0.0_f64);
        let candidate_count = self.posterior_loss_sums.len();
        let action_count = decision_action_count(self);
        for scenario in 0..self.active_scenario_count {
            let first = scenario * candidate_count;
            for target in 0..action_count {
                pass_probabilities[target] += f64::from(self.scenario_root_pass[first + target]);
            }
        }
        let sample_count = f64::from(self.decision_curve_sample_count);
        for probability in &mut pass_probabilities[..action_count] {
            *probability /= sample_count;
        }
        Ok(())
    }

    /// Writes the probability of one rejection reason for each candidate.
    ///
    /// A scenario can contain more than one reason. These probabilities do
    /// not form a categorical distribution.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid buffer or an unavailable decision.
    pub fn write_rejection_curve(
        &self,
        reason: DecisionRejection,
        probabilities: &mut [f64],
    ) -> Result<(), DecisionCurveError> {
        if probabilities.len() != self.posterior_loss_sums.len() {
            return Err(DecisionCurveError::BufferLength {
                expected: self.posterior_loss_sums.len(),
            });
        }
        if self.decision_curve_sample_count == 0 {
            return Err(DecisionCurveError::Unavailable);
        }
        probabilities.fill(0.0_f64);
        let candidate_count = self.posterior_loss_sums.len();
        let action_count = decision_action_count(self);
        for scenario in 0..self.active_scenario_count {
            let first = scenario * candidate_count;
            for target in 0..action_count {
                let rejects = self.scenario_rejection[first + target] & reason.bit() != 0;
                probabilities[target] += f64::from(u8::from(rejects));
            }
        }
        let sample_count = f64::from(self.decision_curve_sample_count);
        for probability in &mut probabilities[..action_count] {
            *probability /= sample_count;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn trajectory_targets(&self, candidate: u32) -> Option<&[u32]> {
        let index = usize::try_from(candidate.checked_sub(1)?).ok()?;
        let first = *self.trajectory_offsets.get(index)? as usize;
        let last = *self.trajectory_offsets.get(index + 1)? as usize;
        self.trajectory.targets.get(first..last)
    }
}

/// Advances the complete controller by one observation.
#[must_use]
pub fn step(
    state: &mut ScaleState,
    scratch: &mut ScaleScratch,
    observation: GroupObservation<'_>,
    now: ModelTime,
) -> ScaleDecision {
    scratch.decision_curve_sample_count = 0;
    if now < state.model_time {
        return hold(state, HoldReason::ModelTimeRegressed, 0.0);
    }
    let GroupObservation {
        cohorts,
        backlog,
        arrivals,
        calendar,
        partition_arrivals,
        resource_window,
        attempt_outcomes,
        transition,
        current_replicas,
        actuation_commitments,
    } = observation;
    let elapsed =
        Duration::from_micros(now.as_micros().saturating_sub(state.model_time.as_micros()));
    state.model_time = now;
    state.capacity.transition(elapsed);
    state.lead_time.transition(elapsed);
    state.rebalance_time.transition(elapsed);
    state.arrivals.prepare_calendar(calendar, now.as_micros());
    if let Some(window) = resource_window {
        state.capacity.update(state.simd_level, &window);
    }
    if let Some(evidence) = attempt_outcomes {
        state.reliability.update(evidence);
    }
    if let Some(evidence) = arrivals {
        state.arrivals.update(evidence, calendar, now.as_micros());
    }
    if let Some(evidence) = partition_arrivals {
        state.partition_placement.update(evidence.consume());
    }
    if let Some(evidence) = transition {
        let (pre_pause, pause) = evidence.consume();
        state.lead_time.update(state.simd_level, pre_pause);
        if let Some(pause) = pause {
            state.rebalance_time.update(state.simd_level, pause);
        }
    }
    if let Some(replicas) = current_replicas {
        state.current_replicas = replicas;
    }

    select_target(
        state,
        scratch,
        cohorts,
        backlog,
        calendar,
        actuation_commitments,
    )
}

fn select_target(
    state: &mut ScaleState,
    scratch: &mut ScaleScratch,
    cohorts: &crate::types::CohortColumns,
    backlog: &crate::types::BacklogColumns,
    calendar: Option<CalendarForecast<'_>>,
    actuation_commitments: &crate::types::ActuationCommitments,
) -> ScaleDecision {
    let (normal_events, failure_events) = demand_class_totals(cohorts, backlog);
    prepare_work_cohorts(state, scratch, cohorts, backlog);
    prepare_partition_work(state, scratch);
    if scratch.active_partition_count == 0
        && state.arrivals.expected_rate(state.model_time.as_micros()) > f64::EPSILON
    {
        scratch.active_partition_count = state.configuration.partition_count;
    }
    prepare_candidate_concurrency(state, scratch);
    scratch.posterior_pass_counts.fill(0.0_f64);
    scratch.posterior_loss_sums.fill(0.0_f64);
    for candidate_index in 0..scratch.deterministic_loss.len() {
        let candidate = candidate_index as u32 + 1;
        scratch.deterministic_loss[candidate_index] =
            placement_shortfall(state, scratch, candidate);
    }
    let scenario_count_max = state.configuration.posterior_sample_count;
    let scenario_count = scenario_count_max.min(DECISION_SAMPLE_COUNT_MIN);
    evaluate_scenarios(
        state,
        scratch,
        normal_events,
        failure_events,
        calendar,
        actuation_commitments,
        scenario_count,
    );
    if scenario_count < scenario_count_max && !decision_is_resolved(state, scratch) {
        evaluate_scenarios(
            state,
            scratch,
            normal_events,
            failure_events,
            calendar,
            actuation_commitments,
            scenario_count_max,
        );
        return finish_decision(state, scratch, f64::from(scenario_count_max));
    }
    finish_decision(state, scratch, f64::from(scenario_count))
}

fn evaluate_scenarios(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    normal_events: f64,
    failure_events: f64,
    calendar: Option<CalendarForecast<'_>>,
    actuation_commitments: &crate::types::ActuationCommitments,
    scenario_count: u32,
) {
    scratch.active_scenario_count = scenario_count as usize;
    for workspace in &mut scratch.recourse_workspaces {
        prepare(&scratch.resource_cohorts, &mut workspace.edf);
    }
    let current_index = state.current_replicas as usize - 1;
    let current_concurrency = scratch.candidate_concurrency[current_index];
    for sample in 0..scenario_count {
        let scenario = sample as usize;
        let candidate_count = scratch.posterior_resource_supply.len();
        let scenario_first = scenario * candidate_count;
        let scenario_last = scenario_first + candidate_count;
        let transition_first = scenario * TRANSITION_SAMPLE_COUNT;
        let transition_last = transition_first + TRANSITION_SAMPLE_COUNT;
        let moved_share_count = scratch.moved_partition_share.len();
        let moved_share_first = scenario * moved_share_count;
        let moved_share_last = moved_share_first + moved_share_count;
        let probability = stratified_probability(sample);
        let curve = state.capacity.curve_at_probability(probability);
        CapacityFactor::fill_throughput(
            state.simd_level,
            curve,
            &scratch.candidate_concurrency,
            &mut scratch.posterior_resource_supply,
        );
        let mut reliability_random = decision_random(sample, DecisionRandomDomain::Reliability);
        let (normal_retry, failure_retry) = state
            .reliability
            .sample_retry_probabilities(&mut reliability_random);
        let event_supply_factor = mixed_event_supply(
            1.0_f64,
            normal_retry,
            failure_retry,
            state.configuration.failure_service_weight,
            normal_events,
            failure_events,
        );
        dispatch!(state.simd_level, simd => scale_and_store_supply(
            simd,
            event_supply_factor,
            &mut scratch.posterior_resource_supply,
            &mut scratch.scenario_supply[scenario_first..scenario_last],
        ));
        let current_supply = curve.throughput(current_concurrency) * event_supply_factor;
        let mut lead_random = decision_random(sample, DecisionRandomDomain::LeadTime);
        let lead_seconds = state.lead_time.sample_bucket_seconds(&mut lead_random);
        let mut rebalance_random = decision_random(sample, DecisionRandomDomain::Rebalance);
        let pause_seconds = state
            .rebalance_time
            .sample_bucket_seconds(&mut rebalance_random);
        scratch.scenario_lead_seconds[transition_first..transition_last]
            .copy_from_slice(&lead_seconds);
        scratch.scenario_rebalance_seconds[transition_first..transition_last]
            .copy_from_slice(&pause_seconds);
        let mut placement_random = decision_random(sample, DecisionRandomDomain::Placement);
        state.partition_placement.sample_moved_prefix(
            &mut placement_random,
            &mut scratch.partition_order,
            &mut scratch.partition_share_draws,
            &mut scratch.moved_partition_share,
        );
        scratch.scenario_moved_partition_share[moved_share_first..moved_share_last]
            .copy_from_slice(&scratch.moved_partition_share);
        prepare_supply_trajectories(
            state,
            scratch,
            current_supply,
            &lead_seconds,
            &pause_seconds,
            actuation_commitments,
            decision_random(sample, DecisionRandomDomain::Commitment),
        );
        let commitment_first = scenario * candidate_count;
        let commitment_last = commitment_first + candidate_count;
        scratch.scenario_commitment_pause_seconds[commitment_first..commitment_last]
            .fill(f64::INFINITY);
        let commitment_count = actuation_commitments.len();
        scratch.scenario_commitment_pause_seconds
            [commitment_first..commitment_first + commitment_count]
            .copy_from_slice(&scratch.commitment_pause_seconds[..commitment_count]);
        let mut arrival_random = decision_random(sample, DecisionRandomDomain::Arrival);
        evaluate_root_forecast(
            state,
            scratch,
            &RootForecast {
                scenario,
                normal_events,
                failure_events,
                current_supply,
                event_supply_factor,
                curve,
                lead_seconds: &lead_seconds,
                rebalance_seconds: &pause_seconds,
                calendar,
            },
            &mut arrival_random,
        );
    }
    apply_recourse_value(state, scratch, actuation_commitments);
    scratch.decision_curve_sample_count = scenario_count;
}

fn decision_is_resolved(state: &ScaleState, scratch: &mut ScaleScratch) -> bool {
    let candidate_count = decision_action_count(scratch);
    let candidate_stride = scratch.posterior_loss_sums.len();
    let scenario_count = scratch.active_scenario_count;
    let required_probability = 1.0_f64 - state.configuration.objective.epsilon();
    let nominal_target = select_root_action(
        &scratch.posterior_pass_counts[..candidate_count],
        &scratch.posterior_loss_sums[..candidate_count],
        &scratch.posterior_replica_seconds_sums[..candidate_count],
        f64::from(scratch.decision_curve_sample_count),
        required_probability,
    );
    scratch.bootstrap_target_counts.fill(0);
    for bootstrap in 0..DECISION_BOOTSTRAP_COUNT {
        scratch.bootstrap_pass_counts.fill(0.0_f64);
        scratch.bootstrap_loss_sums.fill(0.0_f64);
        scratch.bootstrap_replica_seconds_sums.fill(0.0_f64);
        let mut weight_sum = 0.0_f64;
        for scenario in 0..scenario_count {
            let mut random = decision_random(scenario as u32, DecisionRandomDomain::Bootstrap)
                .domain(u64::from(bootstrap));
            let weight = -random.open_unit_f64().ln();
            weight_sum += weight;
            let first = scenario * candidate_stride;
            for target in 0..candidate_count {
                scratch.bootstrap_pass_counts[target] +=
                    weight * f64::from(scratch.scenario_pass[first + target]);
                scratch.bootstrap_loss_sums[target] +=
                    weight * scratch.scenario_shortfall[first + target];
                scratch.bootstrap_replica_seconds_sums[target] +=
                    weight * scratch.scenario_replica_seconds[first + target];
            }
        }
        let target = select_root_action(
            &scratch.bootstrap_pass_counts[..candidate_count],
            &scratch.bootstrap_loss_sums[..candidate_count],
            &scratch.bootstrap_replica_seconds_sums[..candidate_count],
            weight_sum,
            required_probability,
        );
        scratch.bootstrap_target_counts[target] += 1;
    }
    f64::from(scratch.bootstrap_target_counts[nominal_target]) / f64::from(DECISION_BOOTSTRAP_COUNT)
        >= required_probability
}

fn evaluate_root_forecast(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    forecast: &RootForecast<'_>,
    random: &mut RandomStream,
) {
    let candidate_count = scratch.posterior_resource_supply.len();
    let action_count = decision_action_count(scratch);
    let scenario_first = forecast.scenario * candidate_count;
    let budget_seconds =
        Duration::from_micros(state.configuration.objective.budget_micros()).as_secs_f64();
    let start_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    let report_horizon_micros = state
        .model_time
        .as_micros()
        .saturating_add(state.configuration.report_interval_micros);
    let mut horizon_max_micros = report_horizon_micros;
    for candidate_index in 0..action_count {
        let first = scratch.trajectory_offsets[candidate_index] as usize;
        let last = scratch.trajectory_offsets[candidate_index + 1] as usize;
        let horizon_micros = root_event(
            report_horizon_micros,
            &scratch.trajectory.ready_seconds[first..last],
        )
        .at_micros();
        scratch.scenario_horizon_micros[scenario_first + candidate_index] = horizon_micros;
        horizon_max_micros = horizon_max_micros.max(horizon_micros);
    }
    let transition_max_seconds = forecast
        .lead_seconds
        .iter()
        .copied()
        .fold(0.0_f64, f64::max)
        + forecast
            .rebalance_seconds
            .iter()
            .copied()
            .fold(0.0_f64, f64::max);
    let planning_horizon_seconds = Duration::from_micros(horizon_max_micros).as_secs_f64()
        + transition_max_seconds
        + budget_seconds;
    scratch.scenario_planning_horizon_seconds[forecast.scenario] = planning_horizon_seconds;
    let path_segment_count = state.configuration.arrival_prior.path_segment_count_max();
    let path_first = forecast.scenario * path_segment_count;
    let path_last = path_first + path_segment_count;
    let path_length = state.arrivals.sample_rate_path(
        planning_horizon_seconds - start_seconds + budget_seconds,
        random,
        &mut scratch.scenario_arrival_path_end_seconds[path_first..path_last],
        &mut scratch.scenario_arrival_path_rates[path_first..path_last],
        forecast.calendar,
        state.model_time.as_micros(),
    );
    scratch.scenario_arrival_path_length[forecast.scenario] = path_length as u32;
    scratch.scenario_partition_delay_area[forecast.scenario] = partition_delay_area(
        state,
        scratch,
        forecast.curve.service_time_seconds(),
        forecast.event_supply_factor,
        planning_horizon_seconds,
    );
    let arrival_path = ArrivalPath {
        start_seconds,
        end_seconds: &scratch.scenario_arrival_path_end_seconds
            [path_first..path_first + path_length],
        rates: &scratch.scenario_arrival_path_rates[path_first..path_first + path_length],
    };
    let path_end = arrival_path
        .end_seconds
        .last()
        .map_or(planning_horizon_seconds, |end| start_seconds + end);
    scratch.scenario_future_rate[forecast.scenario] = arrival_path.maximum_window_rate(
        planning_horizon_seconds,
        path_end.min(planning_horizon_seconds + budget_seconds),
        budget_seconds,
    );
    scratch.scenario_event_count[forecast.scenario] = forecast.normal_events
        + forecast.failure_events
        + arrival_path.integrated_count(start_seconds, planning_horizon_seconds);
    let worker_count = if scratch.recourse_workspaces[0].edf.has_common_interval() {
        1
    } else {
        scratch.recourse_workspaces.len().min(action_count)
    };
    let candidate_chunk = action_count.div_ceil(worker_count);
    let active_workers = action_count.div_ceil(candidate_chunk);
    evaluate_root_candidate_workers(
        &RootCandidateShared {
            model_time_micros: state.model_time.as_micros(),
            deadline_budget_micros: state.configuration.objective.budget_micros(),
            current_supply: forecast.current_supply,
            resource_debt_events: scratch.resource_debt_events,
            resource_cohorts: &scratch.resource_cohorts,
            trajectory_offsets: &scratch.trajectory_offsets,
            trajectory_pause_seconds: &scratch.trajectory.pause_seconds,
            trajectory_ready_seconds: &scratch.trajectory.ready_seconds,
            trajectory_during_supply: &scratch.trajectory.during_supply,
            trajectory_after_supply: &scratch.trajectory.after_supply,
            horizon_micros: &scratch.scenario_horizon_micros
                [scenario_first..scenario_first + action_count],
            deterministic_loss: &scratch.deterministic_loss[..action_count],
            arrival_path,
        },
        &mut scratch.recourse_workspaces[..active_workers],
        &mut scratch.scenario_shortfall[scenario_first..scenario_first + action_count],
        &mut scratch.scenario_delay_area[scenario_first..scenario_first + action_count],
        &mut scratch.scenario_terminal_work[scenario_first..scenario_first + action_count],
        &mut scratch.scenario_pass[scenario_first..scenario_first + action_count],
        candidate_chunk,
        0,
    );
    scratch.scenario_root_pass[scenario_first..scenario_first + action_count]
        .copy_from_slice(&scratch.scenario_pass[scenario_first..scenario_first + action_count]);
    for candidate in 0..action_count {
        let cell = scenario_first + candidate;
        let mut rejection = 0_u8;
        if scratch.scenario_shortfall[cell] > f64::EPSILON {
            rejection |= DecisionRejection::RootDeadline.bit();
        }
        if scratch.deterministic_loss[candidate] > f64::EPSILON {
            rejection |= DecisionRejection::PartitionPlacement.bit();
        }
        scratch.scenario_rejection[cell] = rejection;
    }
    for candidate_index in 0..action_count {
        let first = scratch.trajectory_offsets[candidate_index] as usize;
        let last = scratch.trajectory_offsets[candidate_index + 1] as usize;
        let horizon_seconds = Duration::from_micros(
            scratch.scenario_horizon_micros[scenario_first + candidate_index],
        )
        .as_secs_f64();
        scratch.scenario_replica_seconds[scenario_first + candidate_index] = replica_seconds(
            start_seconds,
            horizon_seconds,
            state.current_replicas,
            &scratch.trajectory.targets[first..last],
            &scratch.trajectory.ready_seconds[first..last],
        );
    }
}

fn evaluate_root_candidate_workers(
    shared: &RootCandidateShared<'_>,
    workspaces: &mut [RecourseWorkspace],
    shortfall: &mut [f64],
    delay_area: &mut [f64],
    terminal_work: &mut [f64],
    pass: &mut [u8],
    candidate_chunk: usize,
    first_worker: usize,
) {
    if workspaces.len() == 1 {
        let first_candidate = first_worker * candidate_chunk;
        for local_candidate in 0..shortfall.len() {
            let candidate = first_candidate + local_candidate;
            let first = shared.trajectory_offsets[candidate] as usize;
            let last = shared.trajectory_offsets[candidate + 1] as usize;
            let outcome = evaluate_prepared_trajectory(
                shared.resource_cohorts,
                &SupplyTrajectory {
                    initial: shared.current_supply,
                    pause_seconds: &shared.trajectory_pause_seconds[first..last],
                    ready_seconds: &shared.trajectory_ready_seconds[first..last],
                    during: &shared.trajectory_during_supply[first..last],
                    after: &shared.trajectory_after_supply[first..last],
                },
                shared.model_time_micros,
                shared.horizon_micros[candidate],
                shared.resource_debt_events,
                shared.deadline_budget_micros,
                &shared.arrival_path,
                &mut workspaces[0].edf,
            );
            shortfall[local_candidate] = outcome.shortfall;
            delay_area[local_candidate] = outcome.delay_area;
            terminal_work[local_candidate] = outcome.terminal_work;
            pass[local_candidate] = u8::from(
                outcome.shortfall <= f64::EPSILON
                    && shared.deterministic_loss[candidate] <= f64::EPSILON,
            );
        }
        return;
    }

    let middle = workspaces.len() / 2;
    let candidate_middle = (middle * candidate_chunk).min(shortfall.len());
    let (left_workspaces, right_workspaces) = workspaces.split_at_mut(middle);
    let (left_shortfall, right_shortfall) = shortfall.split_at_mut(candidate_middle);
    let (left_delay, right_delay) = delay_area.split_at_mut(candidate_middle);
    let (left_terminal, right_terminal) = terminal_work.split_at_mut(candidate_middle);
    let (left_pass, right_pass) = pass.split_at_mut(candidate_middle);
    rayon::join(
        || {
            evaluate_root_candidate_workers(
                shared,
                left_workspaces,
                left_shortfall,
                left_delay,
                left_terminal,
                left_pass,
                candidate_chunk,
                first_worker,
            );
        },
        || {
            evaluate_root_candidate_workers(
                shared,
                right_workspaces,
                right_shortfall,
                right_delay,
                right_terminal,
                right_pass,
                candidate_chunk,
                first_worker + middle,
            );
        },
    );
}

fn finish_decision(state: &ScaleState, scratch: &ScaleScratch, sample_count: f64) -> ScaleDecision {
    let action_count = decision_action_count(scratch);
    let required_probability = 1.0_f64 - state.configuration.objective.epsilon();
    let target_index = select_root_action(
        &scratch.posterior_pass_counts[..action_count],
        &scratch.posterior_loss_sums[..action_count],
        &scratch.posterior_replica_seconds_sums[..action_count],
        sample_count,
        required_probability,
    );
    let target = target_index as u32 + 1;
    let cap = state.capacity.cap(
        state.configuration.slots_per_replica,
        state.configuration.replica_count_max,
        state.configuration.objective.epsilon(),
    );
    let target = target.min(cap);
    let selected = target as usize - 1;
    let expected_loss = scratch.posterior_loss_sums[selected] / sample_count;
    let saturation_probability = state
        .capacity
        .saturation_probability(state.simd_level, scratch.candidate_concurrency[selected]);
    ScaleDecision::Apply(ApplyDecision {
        target,
        cap,
        diagnostics: diagnostics(
            state,
            expected_loss,
            saturation_probability,
            Some(target),
            scratch.decision_curve_sample_count,
        ),
    })
}

fn apply_recourse_value(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    actuation_commitments: &crate::types::ActuationCommitments,
) {
    let candidate_count = scratch.posterior_resource_supply.len();
    let action_count = decision_action_count(scratch);
    let particle_count = scratch.active_scenario_count;
    let required_probability = 1.0_f64 - state.configuration.objective.epsilon();
    let budget_seconds =
        Duration::from_micros(state.configuration.objective.budget_micros()).as_secs_f64();
    let path_segment_count = state.configuration.arrival_prior.path_segment_count_max();
    let moved_share_count = scratch.moved_partition_share.len();
    scratch.mpc_pass_counts.fill(0.0_f64);
    scratch.mpc_loss_sums.fill(f64::INFINITY);
    scratch.mpc_replica_seconds_sums.fill(f64::INFINITY);

    for root_target_index in 0..action_count {
        scratch.mpc_loss_sums[root_target_index] = 0.0_f64;
        scratch.mpc_replica_seconds_sums[root_target_index] = 0.0_f64;
        evaluate_recourse_root(
            state,
            scratch,
            root_target_index,
            budget_seconds,
            path_segment_count,
            moved_share_count,
            actuation_commitments,
        );

        group_particles(
            &PredictiveObservations {
                elapsed_micros: &scratch.predictive_elapsed_micros[..particle_count],
                arrivals: &scratch.predictive_arrivals[..particle_count],
                completions: &scratch.predictive_completions[..particle_count],
                backlog: &scratch.predictive_backlog[..particle_count],
                warm_replicas: &scratch.predictive_warm_replicas[..particle_count],
                transition_complete: &scratch.predictive_transition_complete[..particle_count],
            },
            &mut scratch.particle_order[..particle_count],
            &mut scratch.node_offsets,
        );
        dispatch!(state.simd_level, simd => apply_recourse_policy(
            simd,
            scratch,
            root_target_index,
            required_probability,
        ));
        for particle in 0..particle_count {
            let root_cell = particle * candidate_count + root_target_index;
            scratch.mpc_pass_counts[root_target_index] +=
                f64::from(scratch.scenario_pass[root_cell]);
            scratch.mpc_loss_sums[root_target_index] += scratch.scenario_shortfall[root_cell];
            scratch.mpc_replica_seconds_sums[root_target_index] +=
                scratch.scenario_replica_seconds[root_cell];
        }
    }
    scratch
        .posterior_pass_counts
        .copy_from_slice(&scratch.mpc_pass_counts);
    scratch
        .posterior_loss_sums
        .copy_from_slice(&scratch.mpc_loss_sums);
    scratch
        .posterior_replica_seconds_sums
        .copy_from_slice(&scratch.mpc_replica_seconds_sums);
}

fn apply_recourse_policy<S: Simd>(
    simd: S,
    scratch: &mut ScaleScratch,
    root_target_index: usize,
    required_probability: f64,
) {
    let candidate_count = scratch.posterior_resource_supply.len();
    let action_count = decision_action_count(scratch);
    for node_index in 1..scratch.node_offsets.len() {
        let first = scratch.node_offsets[node_index - 1] as usize;
        let last = scratch.node_offsets[node_index] as usize;
        scratch.posterior_pass_counts.fill(0.0_f64);
        scratch.posterior_loss_sums.fill(0.0_f64);
        scratch.posterior_replica_seconds_sums.fill(0.0_f64);
        for &particle in &scratch.particle_order[first..last] {
            let row = particle as usize * candidate_count;
            for target in 0..action_count {
                scratch.posterior_pass_counts[target] +=
                    f64::from(scratch.recourse_pass[row + target]);
            }
            add_loss_row(
                simd,
                &mut scratch.posterior_loss_sums[..action_count],
                &scratch.recourse_loss[row..row + action_count],
            );
            add_loss_row(
                simd,
                &mut scratch.posterior_replica_seconds_sums[..action_count],
                &scratch.recourse_replica_seconds[row..row + action_count],
            );
        }
        let node_sample_count = (last - first) as f64;
        let recourse_target = select_root_action(
            &scratch.posterior_pass_counts[..action_count],
            &scratch.posterior_loss_sums[..action_count],
            &scratch.posterior_replica_seconds_sums[..action_count],
            node_sample_count,
            required_probability,
        );
        for &particle in &scratch.particle_order[first..last] {
            let particle = particle as usize;
            let root_cell = particle * candidate_count + root_target_index;
            let recourse_cell = particle * candidate_count + recourse_target;
            let passes =
                scratch.scenario_pass[root_cell] != 0 && scratch.recourse_pass[recourse_cell] != 0;
            scratch.scenario_pass[root_cell] = u8::from(passes);
            scratch.scenario_shortfall[root_cell] = scratch.recourse_loss[recourse_cell];
            scratch.scenario_replica_seconds[root_cell] +=
                scratch.recourse_replica_seconds[recourse_cell];
            scratch.scenario_rejection[root_cell] |= scratch.recourse_rejection[recourse_cell];
        }
    }
}

fn add_loss_row<S: Simd>(simd: S, sums: &mut [f64], row: &[f64]) {
    let lane_count = S::f64s::N;
    let vector_count = sums.len() / lane_count;
    for vector in 0..vector_count {
        let first = vector * lane_count;
        let last = first + lane_count;
        let sum = S::f64s::from_slice(simd, &sums[first..last]);
        let value = S::f64s::from_slice(simd, &row[first..last]);
        (sum + value).store_slice(&mut sums[first..last]);
    }
    for target in vector_count * lane_count..sums.len() {
        sums[target] += row[target];
    }
}

fn evaluate_recourse_root(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    root_target_index: usize,
    budget_seconds: f64,
    path_segment_count: usize,
    moved_share_count: usize,
    actuation_commitments: &crate::types::ActuationCommitments,
) {
    let candidate_count = scratch.posterior_resource_supply.len();
    let particle_count = scratch.active_scenario_count;
    let worker_count = scratch.recourse_workspaces.len();
    let particle_chunk = particle_count.div_ceil(worker_count);
    let shared = RecourseShared {
        model_time_micros: state.model_time.as_micros(),
        current_replicas: state.current_replicas,
        candidate_count,
        action_count: decision_action_count(scratch),
        path_segment_count,
        moved_share_count,
        budget_seconds,
        resource_debt_events: scratch.resource_debt_events,
        resource_cohorts: &scratch.resource_cohorts,
        scenario_horizon_micros: &scratch.scenario_horizon_micros,
        scenario_terminal_work: &scratch.scenario_terminal_work,
        scenario_arrival_path_length: &scratch.scenario_arrival_path_length,
        scenario_arrival_path_end_seconds: &scratch.scenario_arrival_path_end_seconds,
        scenario_arrival_path_rates: &scratch.scenario_arrival_path_rates,
        scenario_supply: &scratch.scenario_supply,
        scenario_lead_seconds: &scratch.scenario_lead_seconds,
        scenario_rebalance_seconds: &scratch.scenario_rebalance_seconds,
        scenario_moved_partition_share: &scratch.scenario_moved_partition_share,
        scenario_planning_horizon_seconds: &scratch.scenario_planning_horizon_seconds,
        scenario_future_rate: &scratch.scenario_future_rate,
        scenario_event_count: &scratch.scenario_event_count,
        scenario_delay_area: &scratch.scenario_delay_area,
        scenario_partition_delay_area: &scratch.scenario_partition_delay_area,
        moved_partition_counts: &scratch.moved_partition_counts,
        transition_sample_indexes: &scratch.transition_sample_indexes,
        actuation_commitments,
        scenario_commitment_pause_seconds: &scratch.scenario_commitment_pause_seconds,
        deterministic_loss: &scratch.deterministic_loss,
    };
    let active_workers = particle_count.div_ceil(particle_chunk);
    dispatch!(state.simd_level, simd => evaluate_recourse_workers(
        simd,
        &shared,
        root_target_index,
        &mut scratch.recourse_workspaces[..active_workers],
        &mut scratch.recourse_loss[..particle_count * candidate_count],
        &mut scratch.recourse_replica_seconds[..particle_count * candidate_count],
        &mut scratch.recourse_rejection[..particle_count * candidate_count],
        &mut scratch.recourse_pass[..particle_count * candidate_count],
        particle_chunk,
        0,
    ));
    for (worker, workspace) in scratch
        .recourse_workspaces
        .iter()
        .take(active_workers)
        .enumerate()
    {
        let first = worker * particle_chunk;
        let last = (first + particle_chunk).min(particle_count);
        let count = last - first;
        scratch.predictive_elapsed_micros[first..last]
            .copy_from_slice(&workspace.predictive_elapsed_micros[..count]);
        scratch.predictive_arrivals[first..last]
            .copy_from_slice(&workspace.predictive_arrivals[..count]);
        scratch.predictive_completions[first..last]
            .copy_from_slice(&workspace.predictive_completions[..count]);
        scratch.predictive_backlog[first..last]
            .copy_from_slice(&workspace.predictive_backlog[..count]);
        scratch.predictive_warm_replicas[first..last]
            .copy_from_slice(&workspace.predictive_warm_replicas[..count]);
        scratch.predictive_transition_complete[first..last]
            .copy_from_slice(&workspace.predictive_transition_complete[..count]);
    }
}

fn evaluate_recourse_workers<S: Simd + Sync>(
    simd: S,
    shared: &RecourseShared<'_>,
    root_target_index: usize,
    workspaces: &mut [RecourseWorkspace],
    loss: &mut [f64],
    replica_seconds_values: &mut [f64],
    rejection: &mut [u8],
    pass: &mut [u8],
    particle_chunk: usize,
    first_worker: usize,
) {
    if workspaces.len() == 1 {
        let workspace = &mut workspaces[0];
        let first_particle = first_worker * particle_chunk;
        let chunk_particles = loss.len() / shared.candidate_count;
        for local_particle in 0..chunk_particles {
            let particle = first_particle + local_particle;
            let first = local_particle * shared.candidate_count;
            let last = first + shared.candidate_count;
            evaluate_recourse_particle(
                simd,
                shared,
                workspace,
                RecourseParticle {
                    root_target_index,
                    particle,
                },
                local_particle,
                &mut loss[first..last],
                &mut replica_seconds_values[first..last],
                &mut rejection[first..last],
                &mut pass[first..last],
            );
        }
        return;
    }
    let middle = workspaces.len() / 2;
    let cell_chunk = particle_chunk * shared.candidate_count;
    let cell_middle = (middle * cell_chunk).min(loss.len());
    let (left_workspaces, right_workspaces) = workspaces.split_at_mut(middle);
    let (left_loss, right_loss) = loss.split_at_mut(cell_middle);
    let (left_replica_seconds, right_replica_seconds) =
        replica_seconds_values.split_at_mut(cell_middle);
    let (left_rejection, right_rejection) = rejection.split_at_mut(cell_middle);
    let (left_pass, right_pass) = pass.split_at_mut(cell_middle);
    rayon::join(
        || {
            evaluate_recourse_workers(
                simd,
                shared,
                root_target_index,
                left_workspaces,
                left_loss,
                left_replica_seconds,
                left_rejection,
                left_pass,
                particle_chunk,
                first_worker,
            );
        },
        || {
            evaluate_recourse_workers(
                simd,
                shared,
                root_target_index,
                right_workspaces,
                right_loss,
                right_replica_seconds,
                right_rejection,
                right_pass,
                particle_chunk,
                first_worker + middle,
            );
        },
    );
}

fn evaluate_recourse_particle<S: Simd>(
    simd: S,
    shared: &RecourseShared<'_>,
    workspace: &mut RecourseWorkspace,
    particle: RecourseParticle,
    local_particle: usize,
    loss: &mut [f64],
    replica_seconds_values: &mut [f64],
    rejection: &mut [u8],
    pass: &mut [u8],
) {
    let candidate_count = shared.candidate_count;
    let action_count = shared.action_count;
    let path_first = particle.particle * shared.path_segment_count;
    let path_length = shared.scenario_arrival_path_length[particle.particle] as usize;
    let root_cell = particle.particle * candidate_count + particle.root_target_index;
    let root_horizon_micros = shared.scenario_horizon_micros[root_cell];
    let root_boundary = prepare_recourse_steps(shared, workspace, particle, root_horizon_micros);
    let arrival_path = ArrivalPath {
        start_seconds: Duration::from_micros(shared.model_time_micros).as_secs_f64(),
        end_seconds: &shared.scenario_arrival_path_end_seconds
            [path_first..path_first + path_length],
        rates: &shared.scenario_arrival_path_rates[path_first..path_first + path_length],
    };
    let root_horizon = Duration::from_micros(root_horizon_micros).as_secs_f64();
    let arrivals = arrival_path.integrated_count(arrival_path.start_seconds, root_horizon);
    let backlog = shared.scenario_terminal_work[root_cell];
    let mut released_events = shared.resource_debt_events;
    workspace.cohorts.clear();
    if shared.resource_cohorts.release_max_micros() <= root_horizon_micros {
        released_events += shared.resource_cohorts.work_slot_seconds_sum();
    } else {
        for cohort in 0..shared.resource_cohorts.len() {
            let release_micros = shared.resource_cohorts.release_micros(cohort);
            let work_slot_seconds = shared.resource_cohorts.work_slot_seconds(cohort);
            if release_micros <= root_horizon_micros {
                released_events += work_slot_seconds;
            } else {
                workspace.cohorts.push_values(
                    release_micros,
                    shared.resource_cohorts.deadline_micros(cohort),
                    work_slot_seconds,
                    shared.resource_cohorts.partition(cohort),
                );
            }
        }
    }
    let completions = (released_events + arrivals - backlog).max(0.0_f64);
    workspace.predictive_elapsed_micros[local_particle] =
        seconds_to_micros(root_horizon - arrival_path.start_seconds);
    workspace.predictive_arrivals[local_particle] = bounded_count(arrivals.round());
    workspace.predictive_completions[local_particle] = bounded_count(completions.round());
    workspace.predictive_backlog[local_particle] = bounded_count(backlog.ceil());
    workspace.predictive_warm_replicas[local_particle] = root_boundary.warm_replicas;
    workspace.predictive_transition_complete[local_particle] =
        u8::from(root_boundary.transition_complete);
    prepare(&workspace.cohorts, &mut workspace.edf);
    let future_rate = shared.scenario_future_rate[particle.particle];
    let planning_horizon = shared.scenario_planning_horizon_seconds[particle.particle];
    for target_index in 0..action_count {
        replica_seconds_values[target_index] = replica_seconds(
            root_horizon,
            planning_horizon,
            root_boundary.warm_replicas,
            &[target_index as u32 + 1],
            &[workspace.ready_seconds[target_index]],
        );
    }
    if workspace.cohorts.is_empty() {
        let root_supply = root_boundary.supply;
        let scenario_first = particle.particle * candidate_count;
        evaluate_empty_steps_simd(
            simd,
            &StepCandidates {
                before: root_supply,
                during: &workspace.during_supply[..action_count],
                after: &shared.scenario_supply[scenario_first..scenario_first + action_count],
                pause_seconds: &workspace.pause_seconds[..action_count],
                ready_seconds: &workspace.ready_seconds[..action_count],
            },
            Duration::from_micros(root_horizon_micros).as_secs_f64(),
            planning_horizon,
            shared.scenario_terminal_work[root_cell],
            &arrival_path,
            seconds_to_micros(shared.budget_seconds),
            &mut workspace.delay_area[..action_count],
            &mut workspace.terminal_work[..action_count],
            &mut workspace.deadline_shortfall[..action_count],
        );
        let event_seconds = shared.scenario_event_count[particle.particle] * shared.budget_seconds;
        record_empty_recourse_outcomes(
            simd,
            &mut EmptyRecourse {
                after_supply: &shared.scenario_supply
                    [scenario_first..scenario_first + action_count],
                deterministic_loss: &shared.deterministic_loss[..action_count],
                delay_area: &workspace.delay_area[..action_count],
                terminal_work: &mut workspace.terminal_work[..action_count],
                deadline_shortfall: &workspace.deadline_shortfall[..action_count],
                root_delay_area: shared.scenario_delay_area[root_cell],
                partition_delay_area: shared.scenario_partition_delay_area[particle.particle],
                future_rate,
                budget_seconds: shared.budget_seconds,
                event_seconds,
            },
            &mut loss[..action_count],
            &mut rejection[..action_count],
            &mut pass[..action_count],
        );
        return;
    }

    for target_index in 0..action_count {
        evaluate_recourse_action(
            shared,
            workspace,
            particle,
            root_horizon_micros,
            target_index,
            future_rate,
            root_boundary.supply,
            loss,
            rejection,
            pass,
        );
    }
}

fn record_empty_recourse_outcomes<S: Simd>(
    simd: S,
    recourse: &mut EmptyRecourse<'_>,
    loss: &mut [f64],
    rejection: &mut [u8],
    pass: &mut [u8],
) {
    let lane_count = S::f64s::N;
    let vector_count = recourse.after_supply.len() / lane_count;
    let zero = S::f64s::splat(simd, 0.0_f64);
    let root_delay = S::f64s::splat(simd, recourse.root_delay_area);
    let partition_delay = S::f64s::splat(simd, recourse.partition_delay_area);
    let event_seconds = S::f64s::splat(simd, recourse.event_seconds.max(f64::MIN_POSITIVE));
    for vector in 0..vector_count {
        let first = vector * lane_count;
        let last = first + lane_count;
        let supply = S::f64s::from_slice(simd, &recourse.after_supply[first..last]);
        let terminal = S::f64s::from_slice(simd, &recourse.terminal_work[first..last]);
        let safe_supply = supply
            .simd_gt(S::f64s::splat(simd, f64::MIN_POSITIVE))
            .select(supply, S::f64s::splat(simd, f64::MIN_POSITIVE));
        let terminal_area = terminal * terminal / (safe_supply + safe_supply);
        let delay = root_delay
            + S::f64s::from_slice(simd, &recourse.delay_area[first..last])
            + terminal_area;
        let delay = delay
            .simd_gt(partition_delay)
            .select(delay, partition_delay);
        let normalized = if recourse.event_seconds > f64::EPSILON {
            delay / event_seconds
        } else {
            zero
        };
        normalized.store_slice(&mut loss[first..last]);
    }
    for target in vector_count * lane_count..recourse.after_supply.len() {
        let terminal_area = terminal_drain_area(
            recourse.terminal_work[target],
            recourse.after_supply[target],
        );
        loss[target] = if recourse.event_seconds > f64::EPSILON {
            (recourse.root_delay_area + recourse.delay_area[target] + terminal_area)
                .max(recourse.partition_delay_area)
                / recourse.event_seconds
        } else {
            0.0_f64
        };
    }
    for target in 0..pass.len() {
        let terminal = fractional_shortfall(
            recourse.terminal_work[target],
            recourse.after_supply[target] * recourse.budget_seconds,
        );
        let future = fractional_shortfall(recourse.future_rate, recourse.after_supply[target]);
        let mut reasons = 0_u8;
        if recourse.deadline_shortfall[target] > f64::EPSILON {
            reasons |= DecisionRejection::RecourseDeadline.bit();
        }
        if terminal > f64::EPSILON {
            reasons |= DecisionRejection::TerminalBacklog.bit();
        }
        if future > f64::EPSILON {
            reasons |= DecisionRejection::FutureArrival.bit();
        }
        if recourse.deterministic_loss[target] > f64::EPSILON {
            reasons |= DecisionRejection::PartitionPlacement.bit();
        }
        rejection[target] = reasons;
        pass[target] = u8::from(reasons == 0);
    }
}

fn prepare_recourse_steps(
    shared: &RecourseShared<'_>,
    workspace: &mut RecourseWorkspace,
    particle: RecourseParticle,
    root_horizon_micros: u64,
) -> RootBoundary {
    let candidate_count = shared.candidate_count;
    let action_count = shared.action_count;
    let root_target = particle.root_target_index as u32 + 1;
    let root_cell = particle.particle * candidate_count + particle.root_target_index;
    let root_horizon = Duration::from_micros(root_horizon_micros).as_secs_f64();
    let current_index = shared.current_replicas as usize - 1;
    let scenario_first = particle.particle * candidate_count;
    let current_supply = shared.scenario_supply[scenario_first + current_index];
    let target_supply = shared.scenario_supply[root_cell];
    let matrix_first = particle.root_target_index * candidate_count;
    let matrix_last = matrix_first + action_count;
    let transition_indexes = &shared.transition_sample_indexes[matrix_first..matrix_last];
    let moved_counts = &shared.moved_partition_counts[matrix_first..matrix_last];
    let transition_first = particle.particle * TRANSITION_SAMPLE_COUNT;
    let transition_last = transition_first + TRANSITION_SAMPLE_COUNT;
    let lead_seconds = &shared.scenario_lead_seconds[transition_first..transition_last];
    let rebalance_seconds = &shared.scenario_rebalance_seconds[transition_first..transition_last];
    let moved_share_first = particle.particle * shared.moved_share_count;
    let moved_share_last = moved_share_first + shared.moved_share_count;
    let moved_shares = &shared.scenario_moved_partition_share[moved_share_first..moved_share_last];
    let root_transition = if root_target == shared.current_replicas {
        0
    } else {
        sample_index(
            if root_target > shared.current_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            },
            root_target.abs_diff(shared.current_replicas),
        )
    };
    let mut root_pause = Duration::from_micros(shared.model_time_micros).as_secs_f64()
        + lead_seconds[root_transition];
    let commitment_first = particle.particle * candidate_count;
    let root_direction = if root_target > shared.current_replicas {
        TransitionDirection::Up
    } else {
        TransitionDirection::Down
    };
    for commitment in 0..shared.actuation_commitments.len() {
        let commitment_target = shared.actuation_commitments.target_replicas(commitment);
        let covers_target = match root_direction {
            TransitionDirection::Up => commitment_target >= root_target,
            TransitionDirection::Down => commitment_target <= root_target,
        };
        if shared.actuation_commitments.direction(commitment) == root_direction && covers_target {
            root_pause = root_pause
                .min(shared.scenario_commitment_pause_seconds[commitment_first + commitment]);
        }
    }
    let root_ready = root_pause + rebalance_seconds[root_transition];
    let root_moved =
        shared.moved_partition_counts[current_index * candidate_count + particle.root_target_index];
    let root_retained = 1.0_f64 - moved_shares[root_moved as usize];
    let root_during_supply = current_supply * root_retained;
    let root_complete = root_target == shared.current_replicas || root_horizon >= root_ready;
    let root_started = root_target != shared.current_replicas && root_horizon >= root_pause;
    let root_supply = if root_complete {
        target_supply
    } else if root_started {
        root_during_supply
    } else {
        current_supply
    };
    for target_index in 0..action_count {
        let target = target_index as u32 + 1;
        let transition = transition_indexes[target_index] as usize;
        if !root_complete && target == root_target {
            workspace.pause_seconds[target_index] = root_pause.max(root_horizon);
            workspace.ready_seconds[target_index] = root_ready.max(root_horizon);
            workspace.during_supply[target_index] = root_during_supply;
        } else if root_started && !root_complete {
            workspace.pause_seconds[target_index] = root_horizon;
            workspace.ready_seconds[target_index] = root_ready;
            workspace.during_supply[target_index] = root_during_supply;
        } else if target == shared.current_replicas && !root_complete {
            workspace.pause_seconds[target_index] = root_horizon;
            workspace.ready_seconds[target_index] = root_horizon;
            workspace.during_supply[target_index] = current_supply;
        } else {
            let pause = root_horizon + lead_seconds[transition];
            let ready = pause + rebalance_seconds[transition];
            let retained = 1.0_f64 - moved_shares[moved_counts[target_index] as usize];
            workspace.pause_seconds[target_index] = pause;
            workspace.ready_seconds[target_index] = ready;
            workspace.during_supply[target_index] = root_supply * retained;
        }
    }
    if root_complete {
        workspace.pause_seconds[particle.root_target_index] = root_horizon;
        workspace.ready_seconds[particle.root_target_index] = root_horizon;
        workspace.during_supply[particle.root_target_index] = root_supply;
    }
    RootBoundary {
        supply: root_supply,
        warm_replicas: if root_complete {
            root_target
        } else {
            shared.current_replicas
        },
        transition_complete: root_complete,
    }
}

fn evaluate_recourse_action(
    shared: &RecourseShared<'_>,
    workspace: &mut RecourseWorkspace,
    particle: RecourseParticle,
    root_horizon_micros: u64,
    target_index: usize,
    future_rate: f64,
    root_supply: f64,
    loss: &mut [f64],
    rejection: &mut [u8],
    pass: &mut [u8],
) {
    let candidate_count = shared.candidate_count;
    let root_cell = particle.particle * candidate_count + particle.root_target_index;
    let planning_horizon = shared.scenario_planning_horizon_seconds[particle.particle];
    let pause_seconds = workspace.pause_seconds[target_index];
    let ready_seconds = workspace.ready_seconds[target_index];
    let during_supply = workspace.during_supply[target_index];
    let after_supply = shared.scenario_supply[particle.particle * candidate_count + target_index];
    let path_first = particle.particle * shared.path_segment_count;
    let path_length = shared.scenario_arrival_path_length[particle.particle] as usize;
    let arrival_path = ArrivalPath {
        start_seconds: Duration::from_micros(shared.model_time_micros).as_secs_f64(),
        end_seconds: &shared.scenario_arrival_path_end_seconds
            [path_first..path_first + path_length],
        rates: &shared.scenario_arrival_path_rates[path_first..path_first + path_length],
    };
    let outcome = evaluate_prepared_step(
        &workspace.cohorts,
        SupplyStep {
            before: root_supply,
            during: during_supply,
            after: after_supply,
            pause_micros: seconds_to_micros(pause_seconds),
            ready_micros: seconds_to_micros(ready_seconds),
        },
        EvaluationWindow {
            start_micros: root_horizon_micros,
            horizon_micros: seconds_to_micros(planning_horizon),
            initial_debt_work: shared.scenario_terminal_work[root_cell],
            deadline_budget_micros: seconds_to_micros(shared.budget_seconds),
        },
        &arrival_path,
        &mut workspace.edf,
    );
    record_recourse_outcome(
        shared,
        particle,
        target_index,
        future_rate,
        outcome,
        loss,
        rejection,
        pass,
    );
}

fn record_recourse_outcome(
    shared: &RecourseShared<'_>,
    particle: RecourseParticle,
    target_index: usize,
    future_rate: f64,
    outcome: EdfOutcome,
    loss: &mut [f64],
    rejection: &mut [u8],
    pass: &mut [u8],
) {
    let candidate_count = shared.candidate_count;
    let root_cell = particle.particle * candidate_count + particle.root_target_index;
    let after_supply = shared.scenario_supply[particle.particle * candidate_count + target_index];
    let backlog_shortfall =
        fractional_shortfall(outcome.terminal_work, after_supply * shared.budget_seconds);
    let future_shortfall = fractional_shortfall(future_rate, after_supply);
    let denominator = shared.scenario_event_count[particle.particle] * shared.budget_seconds;
    let normalized_loss = if denominator > f64::EPSILON {
        (shared.scenario_delay_area[root_cell]
            + outcome.delay_area
            + terminal_drain_area(outcome.terminal_work, after_supply))
        .max(shared.scenario_partition_delay_area[particle.particle])
            / denominator
    } else {
        0.0_f64
    };
    loss[target_index] = normalized_loss;
    let mut reasons = 0_u8;
    if outcome.shortfall > f64::EPSILON {
        reasons |= DecisionRejection::RecourseDeadline.bit();
    }
    if backlog_shortfall > f64::EPSILON {
        reasons |= DecisionRejection::TerminalBacklog.bit();
    }
    if future_shortfall > f64::EPSILON {
        reasons |= DecisionRejection::FutureArrival.bit();
    }
    if shared.deterministic_loss[target_index] > f64::EPSILON {
        reasons |= DecisionRejection::PartitionPlacement.bit();
    }
    rejection[target_index] = reasons;
    pass[target_index] = u8::from(reasons == 0);
}

pub(crate) fn terminal_drain_area(backlog: f64, supply_per_second: f64) -> f64 {
    if backlog <= f64::EPSILON {
        0.0_f64
    } else {
        backlog * backlog / (2.0_f64 * supply_per_second.max(f64::MIN_POSITIVE))
    }
}

fn bounded_count(value: f64) -> u32 {
    if !value.is_finite() || value >= f64::from(u32::MAX) {
        u32::MAX
    } else if value <= 0.0_f64 {
        0
    } else {
        value as u32
    }
}

fn prepare_supply_trajectories(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    current_supply: f64,
    lead_seconds: &[f64; 8],
    rebalance_seconds: &[f64; 8],
    actuation_commitments: &crate::types::ActuationCommitments,
    commitment_random: RandomStream,
) {
    let candidate_count = scratch.posterior_resource_supply.len();
    let now_seconds =
        sample_commitment_pauses(state, scratch, actuation_commitments, commitment_random);
    scratch.trajectory.targets.clear();
    scratch.trajectory.pause_seconds.clear();
    scratch.trajectory.ready_seconds.clear();
    scratch.trajectory.during_supply.clear();
    scratch.trajectory.after_supply.clear();
    scratch.trajectory_offsets[0] = 0;
    for candidate_index in 0..scratch.posterior_resource_supply.len() {
        let candidate = candidate_index as u32 + 1;
        let first = scratch.trajectory.targets.len();
        if candidate != state.current_replicas {
            let candidate_direction = if candidate > state.current_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            for commitment_index in 0..actuation_commitments.len() {
                if actuation_commitments.direction(commitment_index) != candidate_direction
                    || !scratch.commitment_pause_seconds[commitment_index].is_finite()
                {
                    continue;
                }
                let target = match candidate_direction {
                    TransitionDirection::Up => actuation_commitments
                        .target_replicas(commitment_index)
                        .min(candidate),
                    TransitionDirection::Down => actuation_commitments
                        .target_replicas(commitment_index)
                        .max(candidate),
                };
                if target == state.current_replicas
                    || scratch.trajectory.targets[first..].contains(&target)
                {
                    continue;
                }
                push_trajectory_event(
                    scratch,
                    target,
                    scratch.commitment_pause_seconds[commitment_index],
                );
            }
        }
        if candidate != state.current_replicas
            && !scratch.trajectory.targets[first..].contains(&candidate)
        {
            let direction = if candidate > state.current_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            let sample = sample_index(direction, candidate.abs_diff(state.current_replicas));
            push_trajectory_event(scratch, candidate, now_seconds + lead_seconds[sample]);
        }
        sort_trajectory_events(scratch, first);
        let mut write = first;
        let mut replicas = state.current_replicas;
        let mut ready_floor = now_seconds;
        for read in first..scratch.trajectory.targets.len() {
            let target = scratch.trajectory.targets[read];
            if (candidate > state.current_replicas && target <= replicas)
                || (candidate < state.current_replicas && target >= replicas)
            {
                continue;
            }
            let pause = scratch.trajectory.pause_seconds[read].max(ready_floor);
            let direction = if target > replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            let sample = sample_index(direction, target.abs_diff(replicas));
            let ready = pause + rebalance_seconds[sample];
            let before_supply = if replicas == state.current_replicas {
                current_supply
            } else {
                scratch.posterior_resource_supply[replicas as usize - 1]
            };
            let moved = scratch.moved_partition_counts
                [(replicas as usize - 1) * candidate_count + target as usize - 1];
            let retained = 1.0_f64 - scratch.moved_partition_share[moved as usize];
            scratch.trajectory.targets[write] = target;
            scratch.trajectory.pause_seconds[write] = pause;
            scratch.trajectory.ready_seconds[write] = ready;
            scratch.trajectory.during_supply[write] = before_supply * retained;
            scratch.trajectory.after_supply[write] =
                scratch.posterior_resource_supply[target as usize - 1];
            write += 1;
            replicas = target;
            ready_floor = ready;
        }
        scratch.trajectory.targets.truncate(write);
        scratch.trajectory.pause_seconds.truncate(write);
        scratch.trajectory.ready_seconds.truncate(write);
        scratch.trajectory.during_supply.truncate(write);
        scratch.trajectory.after_supply.truncate(write);
        scratch.trajectory_offsets[candidate_index + 1] = write as u32;
    }
}

fn sample_commitment_pauses(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    commitments: &crate::types::ActuationCommitments,
    random: RandomStream,
) -> f64 {
    let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    for index in 0..commitments.len() {
        if commitments.requested_at(index) > state.model_time {
            scratch.commitment_pause_seconds[index] = f64::INFINITY;
            continue;
        }
        let elapsed_seconds = Duration::from_micros(
            state
                .model_time
                .as_micros()
                .saturating_sub(commitments.requested_at(index).as_micros()),
        )
        .as_secs_f64();
        let domain = commitments.requested_at(index).as_micros()
            ^ u64::from(commitments.target_replicas(index)).rotate_left(21)
            ^ u64::from(commitments.replica_delta(index)).rotate_left(42);
        let mut commitment_random = random.clone().domain(domain);
        let remaining_seconds = state.lead_time.sample_remaining_seconds(
            commitments.direction(index),
            commitments.replica_delta(index),
            elapsed_seconds,
            &mut commitment_random,
        );
        scratch.commitment_pause_seconds[index] = now_seconds + remaining_seconds;
    }
    now_seconds
}

pub(crate) fn decision_random(scenario: u32, domain: DecisionRandomDomain) -> RandomStream {
    RandomStream::new(DECISION_SCENARIO_SEED)
        .domain(u64::from(scenario))
        .domain(domain as u64)
}

fn stratified_probability(scenario: u32) -> f64 {
    let mantissa = (u64::from(scenario).reverse_bits() ^ STRATIFICATION_SHIFT) >> 11_u32;
    let high = (mantissa >> 27_u32) as u32;
    let low = (mantissa & 0x07ff_ffff) as u32;
    let exact = f64::from(high) * 134_217_728.0_f64 + f64::from(low);
    (exact + 0.5_f64) * 2.0_f64.powi(-53)
}

fn push_trajectory_event(scratch: &mut ScaleScratch, target: u32, pause_seconds: f64) {
    scratch.trajectory.targets.push(target);
    scratch.trajectory.pause_seconds.push(pause_seconds);
    scratch.trajectory.ready_seconds.push(0.0_f64);
    scratch.trajectory.during_supply.push(0.0_f64);
    scratch.trajectory.after_supply.push(0.0_f64);
}

fn sort_trajectory_events(scratch: &mut ScaleScratch, first: usize) {
    for mut event in first + 1..scratch.trajectory.targets.len() {
        while event > first
            && scratch.trajectory.pause_seconds[event] < scratch.trajectory.pause_seconds[event - 1]
        {
            scratch.trajectory.targets.swap(event, event - 1);
            scratch.trajectory.pause_seconds.swap(event, event - 1);
            scratch.trajectory.ready_seconds.swap(event, event - 1);
            event -= 1;
        }
    }
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}

pub(crate) fn minimal_moved_partitions(partitions: u32, current: u32, target: u32) -> u32 {
    assert!(partitions > 0 && current > 0 && target > 0);
    let current = current.min(partitions);
    let target = target.min(partitions);
    let common = current.min(target);
    let current_base = partitions / current;
    let target_base = partitions / target;
    let current_extra = partitions % current;
    let target_extra = partitions % target;
    let overlap = match current_base.cmp(&target_base) {
        std::cmp::Ordering::Less => common * current_base + common.min(current_extra),
        std::cmp::Ordering::Greater => common * target_base + common.min(target_extra),
        std::cmp::Ordering::Equal => {
            common * current_base + common.min(current_extra).min(target_extra)
        }
    };
    partitions.saturating_sub(overlap)
}

fn moved_partition_count_matrix(
    partitions: u32,
    replica_count_max: u32,
) -> Result<Vec<u32>, ConfigurationError> {
    let replica_count =
        usize::try_from(replica_count_max).map_err(|_| ConfigurationError::PlatformLimit)?;
    let cell_count = replica_count
        .checked_mul(replica_count)
        .ok_or(ConfigurationError::PlatformLimit)?;
    let mut counts = Vec::with_capacity(cell_count);
    for current in 1..=replica_count_max {
        for target in 1..=replica_count_max {
            counts.push(minimal_moved_partitions(partitions, current, target));
        }
    }
    Ok(counts)
}

fn transition_sample_index_matrix(replica_count_max: u32) -> Result<Vec<u8>, ConfigurationError> {
    let replica_count =
        usize::try_from(replica_count_max).map_err(|_| ConfigurationError::PlatformLimit)?;
    let cell_count = replica_count
        .checked_mul(replica_count)
        .ok_or(ConfigurationError::PlatformLimit)?;
    let mut indexes = Vec::with_capacity(cell_count);
    for current in 1..=replica_count_max {
        for target in 1..=replica_count_max {
            if target == current {
                indexes.push(0);
                continue;
            }
            let direction = if target >= current {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            indexes.push(sample_index(direction, target.abs_diff(current)) as u8);
        }
    }
    Ok(indexes)
}

fn prepare_work_cohorts(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    cohorts: &crate::types::CohortColumns,
    backlog: &crate::types::BacklogColumns,
) {
    let handler_seconds = state.capacity.expected_service_time(state.simd_level);
    scratch.handler_cohorts.clear();
    scratch.resource_cohorts.clear();
    scratch.resource_debt_events = 0.0_f64;
    for cohort in 0..cohorts.len() {
        let release_micros = cohorts.release_micros(cohort);
        let deadline_micros = cohorts.deadline_micros(cohort);
        let offered_events = cohorts.offered_events(cohort);
        let partition = cohorts.partition(cohort);
        scratch.handler_cohorts.push_values(
            release_micros,
            deadline_micros,
            offered_events * handler_seconds,
            partition,
        );
        scratch.resource_cohorts.push_values(
            release_micros,
            deadline_micros,
            offered_events,
            partition,
        );
    }
    for backlog_index in 0..backlog.len() {
        if !backlog.is_present(backlog_index) {
            continue;
        }
        let release_micros = state
            .model_time
            .as_micros()
            .max(backlog.observed_at_micros(backlog_index));
        let deadline_micros = backlog
            .oldest_arrival_micros(backlog_index)
            .saturating_add(state.configuration.objective.budget_micros());
        if deadline_micros <= release_micros {
            scratch.resource_debt_events += f64::from(backlog.event_count(backlog_index));
            continue;
        }
        let offered_events = f64::from(backlog.event_count(backlog_index));
        let partition = backlog_index as u32 / crate::DemandClass::COUNT;
        scratch.handler_cohorts.push_values(
            release_micros,
            deadline_micros,
            offered_events * handler_seconds,
            partition,
        );
        scratch.resource_cohorts.push_values(
            release_micros,
            deadline_micros,
            offered_events,
            partition,
        );
    }
}

fn prepare_partition_work(state: &ScaleState, scratch: &mut ScaleScratch) {
    let cohorts = &scratch.handler_cohorts;
    scratch.partition_work_slot_seconds.fill(0.0_f64);
    scratch.active_partition_count = 0;
    scratch.partition_offsets.fill(0);
    let mut release_min = u64::MAX;
    let mut deadline_max = 0_u64;
    for cohort in 0..cohorts.len() {
        let partition = cohorts.partition(cohort) as usize;
        scratch.partition_work_slot_seconds[partition] += cohorts.work_slot_seconds(cohort);
        scratch.partition_offsets[partition + 1] += 1;
        release_min = release_min.min(cohorts.release_micros(cohort));
        deadline_max = deadline_max.max(cohorts.deadline_micros(cohort));
    }
    scratch.active_partition_count = scratch
        .partition_work_slot_seconds
        .iter()
        .filter(|&&work| work > f64::EPSILON)
        .fold(0_u32, |count, _work| count.saturating_add(1));
    for partition in 0..scratch.partition_write_offsets.len() {
        scratch.partition_offsets[partition + 1] += scratch.partition_offsets[partition];
        scratch.partition_write_offsets[partition] = scratch.partition_offsets[partition];
    }
    for cohort_index in 0..cohorts.len() {
        let partition = cohorts.partition(cohort_index) as usize;
        let write_offset = scratch.partition_write_offsets[partition] as usize;
        scratch.partition_cohort_indexes[write_offset] = cohort_index as u32;
        scratch.partition_write_offsets[partition] += 1;
    }
    scratch.partition_shortfall = 0.0_f64;
    for partition in 0..scratch.partition_write_offsets.len() {
        let start = scratch.partition_offsets[partition] as usize;
        let end = scratch.partition_offsets[partition + 1] as usize;
        if start == end {
            continue;
        }
        scratch.placement_cohorts.clear();
        for &cohort_index in &scratch.partition_cohort_indexes[start..end] {
            let cohort = cohort_index as usize;
            scratch.placement_cohorts.push_values(
                cohorts.release_micros(cohort),
                cohorts.deadline_micros(cohort),
                cohorts.work_slot_seconds(cohort),
                cohorts.partition(cohort),
            );
        }
        prepare(&scratch.placement_cohorts, &mut scratch.placement_edf);
        let required =
            required_capacity_prepared(&scratch.placement_cohorts, &mut scratch.placement_edf);
        let shortfall =
            fractional_shortfall(required, f64::from(state.configuration.slots_per_replica));
        scratch.partition_shortfall = scratch.partition_shortfall.max(shortfall);
    }
    scratch
        .partition_work_slot_seconds
        .sort_unstable_by(|left, right| right.total_cmp(left));
    scratch.placement_interval_seconds = if cohorts.is_empty() {
        0.0_f64
    } else {
        Duration::from_micros(deadline_max.saturating_sub(release_min)).as_secs_f64()
    };
}

fn partition_delay_area(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    service_time_seconds: f64,
    event_supply_factor: f64,
    horizon_seconds: f64,
) -> f64 {
    let no_arrivals = ArrivalPath {
        start_seconds: Duration::from_micros(state.model_time.as_micros()).as_secs_f64(),
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let capacity = f64::from(state.configuration.slots_per_replica) * event_supply_factor;
    let mut delay_area = 0.0_f64;
    for partition in 0..scratch.partition_write_offsets.len() {
        let first = scratch.partition_offsets[partition] as usize;
        let last = scratch.partition_offsets[partition + 1] as usize;
        if first == last {
            continue;
        }
        scratch.placement_cohorts.clear();
        for &cohort_index in &scratch.partition_cohort_indexes[first..last] {
            let cohort = cohort_index as usize;
            scratch.placement_cohorts.push_values(
                scratch.resource_cohorts.release_micros(cohort),
                scratch.resource_cohorts.deadline_micros(cohort),
                scratch.resource_cohorts.work_slot_seconds(cohort) * service_time_seconds,
                scratch.resource_cohorts.partition(cohort),
            );
        }
        prepare(&scratch.placement_cohorts, &mut scratch.placement_edf);
        let outcome = evaluate_prepared_step(
            &scratch.placement_cohorts,
            SupplyStep {
                before: capacity,
                during: capacity,
                after: capacity,
                pause_micros: state.model_time.as_micros(),
                ready_micros: state.model_time.as_micros(),
            },
            EvaluationWindow {
                start_micros: state.model_time.as_micros(),
                horizon_micros: seconds_to_micros(horizon_seconds),
                initial_debt_work: 0.0_f64,
                deadline_budget_micros: state.configuration.objective.budget_micros(),
            },
            &no_arrivals,
            &mut scratch.placement_edf,
        );
        delay_area += outcome.delay_area / service_time_seconds;
    }
    delay_area
}

fn prepare_candidate_concurrency(state: &ScaleState, scratch: &mut ScaleScratch) {
    for (candidate_index, concurrency) in scratch.candidate_concurrency.iter_mut().enumerate() {
        let candidate = candidate_index as u32 + 1;
        let active_replicas = candidate.min(scratch.active_partition_count);
        *concurrency =
            f64::from(active_replicas.saturating_mul(state.configuration.slots_per_replica));
    }
}

/// Returns the replica actions that can own demanded partitions.
///
/// Located work defines partition support. Aggregate future demand without a
/// location activates all configured partitions before this function runs.
fn decision_action_count(scratch: &ScaleScratch) -> usize {
    usize::try_from(scratch.active_partition_count.max(1))
        .map_or(scratch.posterior_loss_sums.len(), |count| {
            count.min(scratch.posterior_loss_sums.len())
        })
}

fn demand_class_totals(
    cohorts: &crate::types::CohortColumns,
    backlog: &crate::types::BacklogColumns,
) -> (f64, f64) {
    let totals = cohorts.demand_totals();
    let backlog_totals = backlog.demand_totals();
    (totals.0 + backlog_totals.0, totals.1 + backlog_totals.1)
}

fn scale_and_store_supply<S: Simd>(
    simd: S,
    factor: f64,
    supply: &mut [f64],
    scenario_supply: &mut [f64],
) {
    let lane_count = S::f64s::N;
    let vector_count = supply.len() / lane_count;
    let scalar_factor = factor;
    let factor = S::f64s::splat(simd, scalar_factor);
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let scaled = S::f64s::from_slice(simd, &supply[start..end]) * factor;
        scaled.store_slice(&mut supply[start..end]);
        scaled.store_slice(&mut scenario_supply[start..end]);
    }
    let tail = vector_count * lane_count;
    for value in &mut supply[tail..] {
        *value *= scalar_factor;
    }
    scenario_supply[tail..].copy_from_slice(&supply[tail..]);
}

pub(crate) fn mixed_event_supply(
    attempt_supply: f64,
    normal_retry: f64,
    failure_retry: f64,
    failure_service_weight: f64,
    normal_events: f64,
    failure_events: f64,
) -> f64 {
    let events = normal_events + failure_events;
    if events <= f64::EPSILON {
        return attempt_supply;
    }
    let failure_sequence_attempts = (1.0_f64 - failure_retry).recip();
    let normal_failure_attempts = normal_retry * failure_sequence_attempts;
    let attempt_demand = normal_events * (1.0_f64 + normal_failure_attempts)
        + failure_events * failure_sequence_attempts;
    let failure_demand =
        normal_events * normal_failure_attempts + failure_events * failure_sequence_attempts;
    let aggregate = attempt_supply * events / attempt_demand;
    if normal_events <= f64::EPSILON || failure_demand <= f64::EPSILON {
        return aggregate;
    }
    aggregate.min(attempt_supply * failure_service_weight * events / failure_demand)
}

fn fractional_shortfall(demand: f64, supply: f64) -> f64 {
    if demand <= f64::EPSILON || supply >= demand {
        0.0_f64
    } else {
        (demand - supply) / demand
    }
}

fn placement_shortfall(state: &ScaleState, scratch: &ScaleScratch, candidate: u32) -> f64 {
    if scratch.placement_interval_seconds <= 0.0_f64 {
        return scratch.partition_shortfall;
    }
    let partitions_on_one_replica = state.configuration.partition_count.div_ceil(candidate);
    let work = scratch.partition_work_slot_seconds[..partitions_on_one_replica as usize]
        .iter()
        .sum::<f64>();
    let capacity =
        f64::from(state.configuration.slots_per_replica) * scratch.placement_interval_seconds;
    if work <= capacity || work <= f64::EPSILON {
        scratch.partition_shortfall
    } else {
        scratch.partition_shortfall.max((work - capacity) / work)
    }
}

fn hold(state: &ScaleState, reason: HoldReason, shortfall: f64) -> ScaleDecision {
    ScaleDecision::Hold(HoldDecision {
        reason,
        diagnostics: diagnostics(state, shortfall, 0.0_f64, None, 0),
    })
}

fn diagnostics(
    state: &ScaleState,
    shortfall: f64,
    saturation_probability: f64,
    selected_target: Option<u32>,
    scenario_count: u32,
) -> DecisionDiagnostics {
    let selected_lead_time = selected_target.map_or_else(
        || state.lead_time.expected_last_seconds() + state.rebalance_time.expected_last_seconds(),
        |target| {
            if target == state.current_replicas {
                return 0.0_f64;
            }
            let (direction, delta) = if target > state.current_replicas {
                (TransitionDirection::Up, target - state.current_replicas)
            } else {
                (TransitionDirection::Down, state.current_replicas - target)
            };
            state.lead_time.expected_seconds(direction, delta)
                + state.rebalance_time.expected_seconds(direction, delta)
        },
    );
    DecisionDiagnostics {
        scenario_count,
        arrival_rate_per_second: arrival_rate(state),
        capacity_per_second: state.capacity.expected_capacity(state.simd_level),
        capacity_low_per_second: state.capacity.capacity_quantile(0.1_f64),
        capacity_median_per_second: state.capacity.capacity_quantile(0.5_f64),
        capacity_high_per_second: state.capacity.capacity_quantile(0.9_f64),
        saturation_probability,
        no_knee_probability: state.capacity.no_knee_probability(),
        lead_time_up_seconds: state.lead_time.expected_seconds(TransitionDirection::Up, 1)
            + state
                .rebalance_time
                .expected_seconds(TransitionDirection::Up, 1),
        lead_time_down_seconds: state
            .lead_time
            .expected_seconds(TransitionDirection::Down, 1)
            + state
                .rebalance_time
                .expected_seconds(TransitionDirection::Down, 1),
        lead_time_seconds: selected_lead_time,
        handler_seconds: state.capacity.expected_service_time(state.simd_level),
        maximum_partition_share: state.partition_placement.maximum_expected_share(),
        shortfall,
        expected_loss: shortfall,
    }
}

fn arrival_rate(state: &ScaleState) -> f64 {
    state.arrivals.expected_rate(state.model_time.as_micros())
}

/// Error from a caller-owned decision curve buffer.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecisionCurveError {
    /// The output buffer has the wrong fixed length.
    #[error("the decision curve buffer must contain {expected} values")]
    BufferLength {
        /// Required value count.
        expected: usize,
    },
    /// The last controller step did not calculate a decision curve.
    #[error("the last controller step did not calculate a decision curve")]
    Unavailable,
}
