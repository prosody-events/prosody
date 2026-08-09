use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};

use crate::TransitionDirection;
use crate::arrival::ArrivalFactor;
use crate::capacity::{CapacityFactor, ThroughputPosteriorCell};
use crate::edf::{
    ArrivalPath, EdfScratch, EvaluationWindow, SupplyStep, SupplyTrajectory,
    evaluate_prepared_step, evaluate_prepared_trajectory, prepare, required_capacity_prepared,
};
use crate::lead_time::{LeadTimeFactor, sample_index};
use crate::partition::PartitionFactor;
use crate::planning::{compare_actions, complete_horizon_micros, replica_seconds, select_action};
use crate::reliability::{RELIABILITY_BIN_COUNT, ReliabilityFactor};
use crate::types::{CalendarForecast, WorkCohorts};
use crate::{
    ApplyDecision, ArrivalPosterior, CapacityCurve, CapacityGrid, Configuration,
    ConfigurationError, DecisionDiagnostics, GroupObservation, HoldDecision, HoldReason, ModelTime,
    PosteriorError, PosteriorQuery, RandomStream, ScaleDecision,
};
use thiserror::Error;

const DECISION_SCENARIO_SEED: u64 = 0x7363_616c_652d_636f;
const DECISION_BOOTSTRAP_COUNT: u32 = 128;
const DECISION_SAMPLE_COUNT_MIN: u32 = 1_024;
const STRATIFICATION_SHIFT: u64 = 0x9e37_79b9_7f4a_7c15;

/// One calculated reason that a posterior scenario rejects an action.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionRejection {
    /// Work misses the posterior planning deadline.
    Deadline = 1,
    /// Partition placement cannot serve the located work.
    PartitionPlacement = 2,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericalDecision {
    Resolved { target_index: usize },
    Unresolved { smallest_credible_index: usize },
}

impl NumericalDecision {
    const fn target_index(self) -> usize {
        match self {
            Self::Resolved { target_index } => target_index,
            Self::Unresolved {
                smallest_credible_index,
            } => smallest_credible_index,
        }
    }

    const fn is_resolved(self) -> bool {
        matches!(self, Self::Resolved { .. })
    }
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
    active_partition_count: u32,
    placement_interval_seconds: f64,
    partition_shortfall: f64,
    posterior_missed_work_sums: Vec<f64>,
    posterior_loss_sums: Vec<f64>,
    posterior_replica_seconds_sums: Vec<f64>,
    bootstrap_missed_work_sums: Vec<f64>,
    bootstrap_loss_sums: Vec<f64>,
    bootstrap_replica_seconds_sums: Vec<f64>,
    bootstrap_worse_counts: Vec<u32>,
    candidate_concurrency: Vec<f64>,
    posterior_resource_supply: Vec<f64>,
    scenario_shortfall: Vec<f64>,
    scenario_terminal_late_work: Vec<f64>,
    scenario_replica_seconds: Vec<f64>,
    scenario_rejection: Vec<u8>,
    scenario_missed_work: Vec<f64>,
    scenario_supply: Vec<f64>,
    scenario_arrival_path_end_seconds: Vec<f64>,
    scenario_arrival_path_rates: Vec<f64>,
    scenario_event_count: Vec<f64>,
    scenario_partition_missed_work: Vec<f64>,
    scenario_partition_late_area: Vec<f64>,
    candidate_workspaces: Vec<CandidateWorkspace>,
    deterministic_loss: Vec<f64>,
    trajectory_offsets: Vec<u32>,
    trajectory: TrajectoryColumns,
    commitment_pause_seconds: Vec<f64>,
    rebalancing_ready_seconds: f64,
    resource_debt_events: f64,
    active_scenario_count: usize,
    decision_curve_sample_count: u32,
}

struct CandidateWorkspace {
    edf: EdfScratch,
}

struct TrajectoryColumns {
    targets: Vec<u32>,
    pause_seconds: Vec<f64>,
    ready_overrides: Vec<f64>,
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
    arrival_path_cell_count: usize,
}

struct ForecastScenario<'a> {
    scenario: usize,
    normal_events: f64,
    failure_events: f64,
    current_supply: f64,
    event_supply_factor: f64,
    curve: CapacityCurve,
    calendar: Option<CalendarForecast<'a>>,
}

#[derive(Clone, Copy)]
struct PartitionDeadlineOutcome {
    missed_work: f64,
    late_area: f64,
}

struct CandidateEvaluation<'a> {
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
    horizon_micros: u64,
    arrival_path: ArrivalPath<'a>,
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
            arrival_path_cell_count: posterior_sample_count
                .checked_mul(configuration.arrival_prior.path_segment_count_max())
                .ok_or(ConfigurationError::PlatformLimit)?,
        })
    }
}

impl TrajectoryColumns {
    fn new(capacity: usize) -> Self {
        Self {
            targets: Vec::with_capacity(capacity),
            pause_seconds: Vec::with_capacity(capacity),
            ready_overrides: Vec::with_capacity(capacity),
            ready_seconds: Vec::with_capacity(capacity),
            during_supply: Vec::with_capacity(capacity),
            after_supply: Vec::with_capacity(capacity),
        }
    }
}

impl CandidateWorkspace {
    fn new(cohort_count_max: u32) -> Result<Self, ConfigurationError> {
        Ok(Self {
            edf: EdfScratch::new(cohort_count_max)?,
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
            arrival_path_cell_count,
        } = ScratchBounds::new(configuration)?;
        let candidate_concurrency = (1..=configuration.replica_count_max)
            .map(|replicas| f64::from(replicas) * f64::from(configuration.slots_per_replica))
            .collect::<Vec<_>>();
        let moved_partition_counts = moved_partition_count_matrix(
            configuration.partition_count,
            configuration.replica_count_max,
        )?;
        let worker_count = rayon::current_num_threads().min(replica_count_max).max(1);
        let mut candidate_workspaces = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            candidate_workspaces.push(CandidateWorkspace::new(work_cohort_count_max_u32)?);
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
            active_partition_count: 0,
            placement_interval_seconds: 0.0_f64,
            partition_shortfall: 0.0_f64,
            posterior_missed_work_sums: vec![0.0_f64; replica_count_max],
            posterior_loss_sums: vec![0.0_f64; replica_count_max],
            posterior_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            bootstrap_missed_work_sums: vec![0.0_f64; replica_count_max],
            bootstrap_loss_sums: vec![0.0_f64; replica_count_max],
            bootstrap_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            bootstrap_worse_counts: vec![0; replica_count_max],
            candidate_concurrency,
            posterior_resource_supply: vec![0.0_f64; replica_count_max],
            scenario_shortfall: vec![0.0_f64; scenario_cell_count],
            scenario_terminal_late_work: vec![0.0_f64; scenario_cell_count],
            scenario_replica_seconds: vec![0.0_f64; scenario_cell_count],
            scenario_rejection: vec![0; scenario_cell_count],
            scenario_missed_work: vec![0.0_f64; scenario_cell_count],
            scenario_supply: vec![0.0_f64; scenario_cell_count],
            scenario_arrival_path_end_seconds: vec![0.0_f64; arrival_path_cell_count],
            scenario_arrival_path_rates: vec![0.0_f64; arrival_path_cell_count],
            scenario_event_count: vec![0.0_f64; posterior_sample_count],
            scenario_partition_missed_work: vec![0.0_f64; posterior_sample_count],
            scenario_partition_late_area: vec![0.0_f64; posterior_sample_count],
            candidate_workspaces,
            deterministic_loss: vec![0.0_f64; replica_count_max],
            trajectory_offsets: vec![0; replica_count_max + 1],
            trajectory: TrajectoryColumns::new(trajectory_event_count_max),
            commitment_pause_seconds: vec![0.0_f64; replica_count_max],
            rebalancing_ready_seconds: f64::INFINITY,
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
        let event_count_sum: f64 = self.scenario_event_count[..self.active_scenario_count]
            .iter()
            .sum();
        let action_count = decision_action_count(self);
        for index in 0..self.posterior_loss_sums.len() {
            if index < action_count {
                expected_losses[index] = self.posterior_loss_sums[index] / sample_count;
                pass_probabilities[index] = 1.0_f64
                    - self.posterior_missed_work_sums[index]
                        / event_count_sum.max(f64::MIN_POSITIVE);
            } else {
                expected_losses[index] = f64::INFINITY;
                pass_probabilities[index] = 0.0_f64;
            }
        }
        Ok(())
    }

    /// # Errors
    ///
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

    #[cfg(test)]
    pub(crate) fn trajectory_pause_seconds(&self, candidate: u32) -> Option<&[f64]> {
        let index = usize::try_from(candidate.checked_sub(1)?).ok()?;
        let first = *self.trajectory_offsets.get(index)? as usize;
        let last = *self.trajectory_offsets.get(index + 1)? as usize;
        self.trajectory.pause_seconds.get(first..last)
    }

    #[cfg(test)]
    pub(crate) fn trajectory_ready_seconds(&self, candidate: u32) -> Option<&[f64]> {
        let index = usize::try_from(candidate.checked_sub(1)?).ok()?;
        let first = *self.trajectory_offsets.get(index)? as usize;
        let last = *self.trajectory_offsets.get(index + 1)? as usize;
        self.trajectory.ready_seconds.get(first..last)
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
    scratch.posterior_missed_work_sums.fill(0.0_f64);
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
    let pilot_decision = numerical_decision(state, scratch);
    if scenario_count < scenario_count_max && !pilot_decision.is_resolved() {
        evaluate_scenarios(
            state,
            scratch,
            normal_events,
            failure_events,
            calendar,
            actuation_commitments,
            scenario_count_max,
        );
        let target_index = numerical_decision(state, scratch).target_index();
        return finish_decision(state, scratch, f64::from(scenario_count_max), target_index);
    }
    finish_decision(
        state,
        scratch,
        f64::from(scenario_count),
        pilot_decision.target_index(),
    )
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
    for workspace in &mut scratch.candidate_workspaces {
        prepare(&scratch.resource_cohorts, &mut workspace.edf);
    }
    let current_index = state.current_replicas as usize - 1;
    let current_concurrency = scratch.candidate_concurrency[current_index];
    for sample in 0..scenario_count {
        let scenario = sample as usize;
        let candidate_count = scratch.posterior_resource_supply.len();
        let scenario_first = scenario * candidate_count;
        let scenario_last = scenario_first + candidate_count;
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
        let mut placement_random = decision_random(sample, DecisionRandomDomain::Placement);
        state.partition_placement.sample_moved_prefix(
            &mut placement_random,
            &mut scratch.partition_order,
            &mut scratch.partition_share_draws,
            &mut scratch.moved_partition_share,
        );
        prepare_supply_trajectories(
            state,
            scratch,
            current_supply,
            &lead_seconds,
            &pause_seconds,
            actuation_commitments,
            decision_random(sample, DecisionRandomDomain::Commitment),
        );
        let mut arrival_random = decision_random(sample, DecisionRandomDomain::Arrival);
        evaluate_forecast_scenario(
            state,
            scratch,
            &ForecastScenario {
                scenario,
                normal_events,
                failure_events,
                current_supply,
                event_supply_factor,
                curve,
                calendar,
            },
            &mut arrival_random,
        );
    }
    dispatch!(state.simd_level, simd => aggregate_scenario_values(simd, scratch));
    scratch.decision_curve_sample_count = scenario_count;
}

fn aggregate_scenario_values<S: Simd>(simd: S, scratch: &mut ScaleScratch) {
    let candidate_count = decision_action_count(scratch);
    let candidate_stride = scratch.posterior_resource_supply.len();
    scratch.posterior_missed_work_sums.fill(0.0_f64);
    scratch.posterior_loss_sums.fill(0.0_f64);
    scratch.posterior_replica_seconds_sums.fill(0.0_f64);
    for scenario in 0..scratch.active_scenario_count {
        let first = scenario * candidate_stride;
        let vector_count = candidate_count / S::f64s::N;
        for vector in 0..vector_count {
            let target = vector * S::f64s::N;
            let last = target + S::f64s::N;
            let cell = first + target;
            let missed =
                S::f64s::from_slice(simd, &scratch.posterior_missed_work_sums[target..last])
                    + S::f64s::from_slice(
                        simd,
                        &scratch.scenario_missed_work[cell..cell + S::f64s::N],
                    );
            let loss = S::f64s::from_slice(simd, &scratch.posterior_loss_sums[target..last])
                + S::f64s::from_slice(simd, &scratch.scenario_shortfall[cell..cell + S::f64s::N]);
            let replica_seconds =
                S::f64s::from_slice(simd, &scratch.posterior_replica_seconds_sums[target..last])
                    + S::f64s::from_slice(
                        simd,
                        &scratch.scenario_replica_seconds[cell..cell + S::f64s::N],
                    );
            missed.store_slice(&mut scratch.posterior_missed_work_sums[target..last]);
            loss.store_slice(&mut scratch.posterior_loss_sums[target..last]);
            replica_seconds.store_slice(&mut scratch.posterior_replica_seconds_sums[target..last]);
        }
        for target in vector_count * S::f64s::N..candidate_count {
            let cell = first + target;
            scratch.posterior_missed_work_sums[target] += scratch.scenario_missed_work[cell];
            scratch.posterior_loss_sums[target] += scratch.scenario_shortfall[cell];
            scratch.posterior_replica_seconds_sums[target] +=
                scratch.scenario_replica_seconds[cell];
        }
    }
}

/// Returns the smallest action in the bootstrap credible set.
///
/// The credible set excludes an action only when at least the configured SLO
/// confidence of paired bootstrap posteriors ranks it below the nominal
/// optimum. A singleton set makes the numerical integral resolved. Multiple
/// actions preserve the smallest action because the integral cannot
/// distinguish them at that confidence.
fn numerical_decision(state: &ScaleState, scratch: &mut ScaleScratch) -> NumericalDecision {
    dispatch!(state.simd_level, simd => numerical_decision_simd(simd, state, scratch))
}

fn numerical_decision_simd<S: Simd>(
    simd: S,
    state: &ScaleState,
    scratch: &mut ScaleScratch,
) -> NumericalDecision {
    let candidate_count = decision_action_count(scratch);
    let candidate_stride = scratch.posterior_loss_sums.len();
    let scenario_count = scratch.active_scenario_count;
    let epsilon = state.configuration.objective.epsilon();
    let event_count_sum: f64 = scratch.scenario_event_count[..scenario_count].iter().sum();
    let nominal_target = select_action(
        &scratch.posterior_missed_work_sums[..candidate_count],
        &scratch.posterior_loss_sums[..candidate_count],
        &scratch.posterior_replica_seconds_sums[..candidate_count],
        event_count_sum,
        epsilon,
    );
    scratch.bootstrap_worse_counts.fill(0);
    for bootstrap in 0..DECISION_BOOTSTRAP_COUNT {
        scratch.bootstrap_missed_work_sums.fill(0.0_f64);
        scratch.bootstrap_loss_sums.fill(0.0_f64);
        scratch.bootstrap_replica_seconds_sums.fill(0.0_f64);
        let mut event_count_sum = 0.0_f64;
        for scenario in 0..scenario_count {
            let mut random = decision_random(scenario as u32, DecisionRandomDomain::Bootstrap)
                .domain(u64::from(bootstrap));
            let weight = -random.open_unit_f64().ln();
            event_count_sum += weight * scratch.scenario_event_count[scenario];
            let first = scenario * candidate_stride;
            let vector_count = candidate_count / S::f64s::N;
            for vector in 0..vector_count {
                let target = vector * S::f64s::N;
                let last = target + S::f64s::N;
                let missed =
                    S::f64s::from_slice(simd, &scratch.bootstrap_missed_work_sums[target..last])
                        + S::f64s::from_slice(
                            simd,
                            &scratch.scenario_missed_work[first + target..first + last],
                        ) * S::f64s::splat(simd, weight);
                let loss = S::f64s::from_slice(simd, &scratch.bootstrap_loss_sums[target..last])
                    + S::f64s::from_slice(
                        simd,
                        &scratch.scenario_shortfall[first + target..first + last],
                    ) * S::f64s::splat(simd, weight);
                let replica_seconds = S::f64s::from_slice(
                    simd,
                    &scratch.bootstrap_replica_seconds_sums[target..last],
                ) + S::f64s::from_slice(
                    simd,
                    &scratch.scenario_replica_seconds[first + target..first + last],
                ) * S::f64s::splat(simd, weight);
                missed.store_slice(&mut scratch.bootstrap_missed_work_sums[target..last]);
                loss.store_slice(&mut scratch.bootstrap_loss_sums[target..last]);
                replica_seconds
                    .store_slice(&mut scratch.bootstrap_replica_seconds_sums[target..last]);
            }
            for target in vector_count * S::f64s::N..candidate_count {
                scratch.bootstrap_missed_work_sums[target] +=
                    weight * scratch.scenario_missed_work[first + target];
                scratch.bootstrap_loss_sums[target] +=
                    weight * scratch.scenario_shortfall[first + target];
                scratch.bootstrap_replica_seconds_sums[target] +=
                    weight * scratch.scenario_replica_seconds[first + target];
            }
        }
        for target in 0..candidate_count {
            if compare_actions(
                nominal_target,
                target,
                &scratch.bootstrap_missed_work_sums[..candidate_count],
                &scratch.bootstrap_loss_sums[..candidate_count],
                &scratch.bootstrap_replica_seconds_sums[..candidate_count],
                event_count_sum,
                epsilon,
            )
            .is_lt()
            {
                scratch.bootstrap_worse_counts[target] += 1;
            }
        }
    }
    classify_paired_bootstrap(
        &scratch.bootstrap_worse_counts[..candidate_count],
        DECISION_BOOTSTRAP_COUNT,
        epsilon,
    )
}

pub(crate) fn classify_paired_bootstrap(
    worse_counts: &[u32],
    bootstrap_count: u32,
    epsilon: f64,
) -> NumericalDecision {
    let credible = |target: usize| {
        f64::from(worse_counts[target]) / f64::from(bootstrap_count) < 1.0_f64 - epsilon
    };
    let smallest_credible_index = (0..worse_counts.len())
        .find(|&target| credible(target))
        .map_or(0, |target| target);
    if (smallest_credible_index + 1..worse_counts.len()).any(credible) {
        NumericalDecision::Unresolved {
            smallest_credible_index,
        }
    } else {
        NumericalDecision::Resolved {
            target_index: smallest_credible_index,
        }
    }
}

fn evaluate_forecast_scenario(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    forecast: &ForecastScenario<'_>,
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
        let ready_micros = scratch.trajectory.ready_seconds[first..last]
            .last()
            .map_or(state.model_time.as_micros(), |ready| {
                seconds_to_micros(*ready)
            });
        horizon_max_micros = horizon_max_micros.max(ready_micros);
    }
    let planning_horizon_micros = complete_horizon_micros(
        report_horizon_micros,
        horizon_max_micros,
        scratch.resource_cohorts.deadline_max_micros(),
        state.configuration.objective.budget_micros(),
    );
    let disturbance_horizon_micros =
        planning_horizon_micros.saturating_sub(state.configuration.objective.budget_micros());
    let planning_horizon_seconds = Duration::from_micros(planning_horizon_micros).as_secs_f64();
    let disturbance_horizon_seconds =
        Duration::from_micros(disturbance_horizon_micros).as_secs_f64();
    let path_segment_count = state.configuration.arrival_prior.path_segment_count_max();
    let path_first = forecast.scenario * path_segment_count;
    let path_last = path_first + path_segment_count;
    let path_length = state.arrivals.sample_rate_path(
        disturbance_horizon_seconds - start_seconds,
        random,
        &mut scratch.scenario_arrival_path_end_seconds[path_first..path_last],
        &mut scratch.scenario_arrival_path_rates[path_first..path_last],
        forecast.calendar,
        state.model_time.as_micros(),
    );
    let partition_outcome = partition_deadline_outcome(
        state,
        scratch,
        forecast.curve.service_time_seconds(),
        forecast.event_supply_factor,
        planning_horizon_seconds,
    );
    scratch.scenario_partition_missed_work[forecast.scenario] = partition_outcome.missed_work;
    scratch.scenario_partition_late_area[forecast.scenario] = partition_outcome.late_area;
    let arrival_path = ArrivalPath {
        start_seconds,
        end_seconds: &scratch.scenario_arrival_path_end_seconds
            [path_first..path_first + path_length],
        rates: &scratch.scenario_arrival_path_rates[path_first..path_first + path_length],
    };
    scratch.scenario_event_count[forecast.scenario] = forecast.normal_events
        + forecast.failure_events
        + arrival_path.integrated_count(start_seconds, disturbance_horizon_seconds);
    let worker_count = if scratch.candidate_workspaces[0].edf.has_common_interval() {
        1
    } else {
        scratch.candidate_workspaces.len().min(action_count)
    };
    let candidate_chunk = action_count.div_ceil(worker_count);
    let active_workers = action_count.div_ceil(candidate_chunk);
    evaluate_candidate_workers(
        &CandidateEvaluation {
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
            horizon_micros: planning_horizon_micros,
            arrival_path,
        },
        &mut scratch.candidate_workspaces[..active_workers],
        &mut scratch.scenario_shortfall[scenario_first..scenario_first + action_count],
        &mut scratch.scenario_terminal_late_work[scenario_first..scenario_first + action_count],
        &mut scratch.scenario_missed_work[scenario_first..scenario_first + action_count],
        candidate_chunk,
        0,
    );
    for candidate in 0..action_count {
        let cell = scenario_first + candidate;
        let late_area = scratch.scenario_shortfall[cell];
        let terminal_late_work = scratch.scenario_terminal_late_work[cell];
        let denominator = scratch.scenario_event_count[forecast.scenario] * budget_seconds;
        scratch.scenario_shortfall[cell] = if denominator > f64::EPSILON {
            (late_area + terminal_drain_area(terminal_late_work, scratch.scenario_supply[cell]))
                .max(scratch.scenario_partition_late_area[forecast.scenario])
                / denominator
        } else {
            0.0_f64
        };
        let missed_work = scratch.scenario_missed_work[cell]
            .max(scratch.scenario_partition_missed_work[forecast.scenario]);
        scratch.scenario_missed_work[cell] = missed_work;
        let mut rejection = 0_u8;
        let miss_fraction =
            missed_work / scratch.scenario_event_count[forecast.scenario].max(f64::MIN_POSITIVE);
        if miss_fraction > state.configuration.objective.epsilon() {
            rejection |= DecisionRejection::Deadline.bit();
        }
        if scratch.deterministic_loss[candidate] > f64::EPSILON {
            rejection |= DecisionRejection::PartitionPlacement.bit();
        }
        scratch.scenario_rejection[cell] = rejection;
    }
    for candidate_index in 0..action_count {
        let cell = scenario_first + candidate_index;
        let first = scratch.trajectory_offsets[candidate_index] as usize;
        let last = scratch.trajectory_offsets[candidate_index + 1] as usize;
        let horizon_seconds = planning_horizon_seconds;
        scratch.scenario_replica_seconds[cell] = replica_seconds(
            start_seconds,
            horizon_seconds,
            state.current_replicas,
            &scratch.trajectory.targets[first..last],
            &scratch.trajectory.pause_seconds[first..last],
        );
    }
}

fn evaluate_candidate_workers(
    shared: &CandidateEvaluation<'_>,
    workspaces: &mut [CandidateWorkspace],
    shortfall: &mut [f64],
    terminal_late_work: &mut [f64],
    missed_work: &mut [f64],
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
                shared.horizon_micros,
                shared.resource_debt_events,
                shared.deadline_budget_micros,
                &shared.arrival_path,
                &mut workspaces[0].edf,
            );
            shortfall[local_candidate] = outcome.late_area;
            terminal_late_work[local_candidate] = outcome.terminal_late_work;
            missed_work[local_candidate] = outcome.missed_work;
        }
        return;
    }

    let middle = workspaces.len() / 2;
    let candidate_middle = (middle * candidate_chunk).min(shortfall.len());
    let (left_workspaces, right_workspaces) = workspaces.split_at_mut(middle);
    let (left_shortfall, right_shortfall) = shortfall.split_at_mut(candidate_middle);
    let (left_terminal, right_terminal) = terminal_late_work.split_at_mut(candidate_middle);
    let (left_missed, right_missed) = missed_work.split_at_mut(candidate_middle);
    rayon::join(
        || {
            evaluate_candidate_workers(
                shared,
                left_workspaces,
                left_shortfall,
                left_terminal,
                left_missed,
                candidate_chunk,
                first_worker,
            );
        },
        || {
            evaluate_candidate_workers(
                shared,
                right_workspaces,
                right_shortfall,
                right_terminal,
                right_missed,
                candidate_chunk,
                first_worker + middle,
            );
        },
    );
}

fn finish_decision(
    state: &ScaleState,
    scratch: &ScaleScratch,
    sample_count: f64,
    target_index: usize,
) -> ScaleDecision {
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

pub(crate) fn terminal_drain_area(backlog: f64, supply_per_second: f64) -> f64 {
    if backlog <= f64::EPSILON {
        0.0_f64
    } else {
        backlog * backlog / (2.0_f64 * supply_per_second.max(f64::MIN_POSITIVE))
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
    scratch.trajectory.ready_overrides.clear();
    scratch.trajectory.ready_seconds.clear();
    scratch.trajectory.during_supply.clear();
    scratch.trajectory.after_supply.clear();
    scratch.trajectory_offsets[0] = 0;
    for candidate_index in 0..scratch.posterior_resource_supply.len() {
        let candidate = candidate_index as u32 + 1;
        let first = scratch.trajectory.targets.len();
        let rebalancing = actuation_commitments.rebalancing();
        let committed_replicas = rebalancing.map_or(state.current_replicas, |commitment| {
            push_trajectory_event(
                scratch,
                commitment.target_replicas,
                now_seconds,
                scratch.rebalancing_ready_seconds,
            );
            commitment.target_replicas
        });
        let fixed_event_count = scratch.trajectory.targets.len() - first;
        if candidate != committed_replicas {
            let candidate_direction = if candidate > committed_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            for commitment_index in 0..actuation_commitments.launching_len() {
                if actuation_commitments.launching_direction(commitment_index)
                    != candidate_direction
                    || !scratch.commitment_pause_seconds[commitment_index].is_finite()
                {
                    continue;
                }
                let target = match candidate_direction {
                    TransitionDirection::Up => actuation_commitments
                        .launching_target_replicas(commitment_index)
                        .min(candidate),
                    TransitionDirection::Down => actuation_commitments
                        .launching_target_replicas(commitment_index)
                        .max(candidate),
                };
                if target == committed_replicas
                    || scratch.trajectory.targets[first..].contains(&target)
                {
                    continue;
                }
                push_trajectory_event(
                    scratch,
                    target,
                    scratch.commitment_pause_seconds[commitment_index],
                    f64::NAN,
                );
            }
        }
        if candidate != committed_replicas
            && !scratch.trajectory.targets[first..].contains(&candidate)
        {
            let direction = if candidate > committed_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            let sample = sample_index(direction, candidate.abs_diff(committed_replicas));
            push_trajectory_event(
                scratch,
                candidate,
                now_seconds + lead_seconds[sample],
                f64::NAN,
            );
        }
        sort_trajectory_events(scratch, first + fixed_event_count);
        let mut write = first;
        let mut replicas = state.current_replicas;
        let mut active_ready = now_seconds;
        let mut active_during_supply = current_supply;
        let mut active_after_supply = current_supply;
        let mut active_event = None;
        for read in first..scratch.trajectory.targets.len() {
            let target = scratch.trajectory.targets[read];
            if read >= first + fixed_event_count
                && ((candidate > committed_replicas && target <= replicas)
                    || (candidate < committed_replicas && target >= replicas))
            {
                continue;
            }
            let pause = scratch.trajectory.pause_seconds[read].max(now_seconds);
            let before_supply = if pause < active_ready {
                if let Some(event) = active_event {
                    scratch.trajectory.ready_seconds[event] = pause;
                }
                active_during_supply
            } else {
                active_after_supply
            };
            let direction = if target > replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            let sample = sample_index(direction, target.abs_diff(replicas));
            let ready_override = scratch.trajectory.ready_overrides[read];
            let ready = if ready_override.is_finite() {
                ready_override.max(pause)
            } else {
                pause + rebalance_seconds[sample]
            };
            let moved = scratch.moved_partition_counts
                [(replicas as usize - 1) * candidate_count + target as usize - 1];
            let retained = 1.0_f64 - scratch.moved_partition_share[moved as usize];
            scratch.trajectory.targets[write] = target;
            scratch.trajectory.pause_seconds[write] = pause;
            scratch.trajectory.ready_overrides[write] = ready_override;
            scratch.trajectory.ready_seconds[write] = ready;
            scratch.trajectory.during_supply[write] = before_supply * retained;
            scratch.trajectory.after_supply[write] =
                scratch.posterior_resource_supply[target as usize - 1];
            active_ready = ready;
            active_during_supply = scratch.trajectory.during_supply[write];
            active_after_supply = scratch.trajectory.after_supply[write];
            active_event = Some(write);
            write += 1;
            replicas = target;
        }
        scratch.trajectory.targets.truncate(write);
        scratch.trajectory.pause_seconds.truncate(write);
        scratch.trajectory.ready_overrides.truncate(write);
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
    for index in 0..commitments.launching_len() {
        if commitments.launching_requested_at(index) > state.model_time {
            scratch.commitment_pause_seconds[index] = f64::INFINITY;
            continue;
        }
        let elapsed_seconds = Duration::from_micros(
            state
                .model_time
                .as_micros()
                .saturating_sub(commitments.launching_requested_at(index).as_micros()),
        )
        .as_secs_f64();
        let domain = commitments.launching_requested_at(index).as_micros()
            ^ u64::from(commitments.launching_target_replicas(index)).rotate_left(21)
            ^ u64::from(commitments.launching_replica_delta(index)).rotate_left(42);
        let mut commitment_random = random.clone().domain(domain);
        let remaining_seconds = state.lead_time.sample_remaining_seconds(
            commitments.launching_direction(index),
            commitments.launching_replica_delta(index),
            elapsed_seconds,
            &mut commitment_random,
        );
        scratch.commitment_pause_seconds[index] = now_seconds + remaining_seconds;
    }
    scratch.rebalancing_ready_seconds =
        commitments
            .rebalancing()
            .map_or(f64::INFINITY, |commitment| {
                if commitment.started_at > state.model_time {
                    return f64::INFINITY;
                }
                let elapsed_seconds = Duration::from_micros(
                    state
                        .model_time
                        .as_micros()
                        .saturating_sub(commitment.started_at.as_micros()),
                )
                .as_secs_f64();
                let direction = if commitment.target_replicas > commitment.from_replicas {
                    TransitionDirection::Up
                } else {
                    TransitionDirection::Down
                };
                let delta = commitment
                    .target_replicas
                    .abs_diff(commitment.from_replicas);
                let domain = commitment.requested_at.as_micros()
                    ^ commitment.started_at.as_micros().rotate_left(17)
                    ^ u64::from(commitment.target_replicas).rotate_left(34);
                let mut rebalance_random = random.clone().domain(domain);
                now_seconds
                    + state.rebalance_time.sample_remaining_seconds(
                        direction,
                        delta,
                        elapsed_seconds,
                        &mut rebalance_random,
                    )
            });
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

fn push_trajectory_event(
    scratch: &mut ScaleScratch,
    target: u32,
    pause_seconds: f64,
    ready_override: f64,
) {
    scratch.trajectory.targets.push(target);
    scratch.trajectory.pause_seconds.push(pause_seconds);
    scratch.trajectory.ready_overrides.push(ready_override);
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
            scratch.trajectory.ready_overrides.swap(event, event - 1);
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

fn partition_deadline_outcome(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    service_time_seconds: f64,
    event_supply_factor: f64,
    horizon_seconds: f64,
) -> PartitionDeadlineOutcome {
    let no_arrivals = ArrivalPath {
        start_seconds: Duration::from_micros(state.model_time.as_micros()).as_secs_f64(),
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let capacity = f64::from(state.configuration.slots_per_replica) * event_supply_factor;
    let mut missed_work = 0.0_f64;
    let mut late_area = 0.0_f64;
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
        missed_work += outcome.missed_work / service_time_seconds;
        late_area += outcome.late_area / service_time_seconds;
    }
    PartitionDeadlineOutcome {
        missed_work,
        late_area,
    }
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
