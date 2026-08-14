use std::cmp::Ordering;
use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};

use crate::TransitionDirection;
use crate::arrival::{ArrivalFactor, ArrivalPrior};
use crate::capacity::{
    CapacityClockCheck, CapacityFactor, CompletionPosteriorCell, ThroughputPosteriorCell,
};
use crate::edf::{
    ArrivalPath, EdfScratch, EvaluationWindow, SupplyStep, SupplyTrajectory,
    evaluate_prepared_step, evaluate_prepared_trajectory, prepare,
};
use crate::lead_time::{LaunchComponentSummary, LaunchTimeFactor, RebalanceTimeFactor};
use crate::partition::PartitionFactor;
use crate::planning::{
    ActionColumns, compare_actions, complete_horizon_micros, replica_seconds, select_action,
    terminal_replica_seconds,
};
use crate::reliability::{RELIABILITY_BIN_COUNT, ReliabilityFactor};
use crate::types::{
    ActuationCommitments, BacklogColumns, CalendarForecast, CohortColumns,
    POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN, ScheduledRelease, WorkCohorts,
};
use crate::{
    ApplyDecision, ArrivalCountPredictive, ArrivalPredictiveError, CapacityGrid, Configuration,
    ConfigurationError, DecisionDiagnostics, DemandClass, GroupObservation, HoldDecision,
    HoldReason, ModelTime, PosteriorError, PosteriorQuery, PredictiveQuantileError, PriorArtifact,
    RandomStream, ResourceWindow, ScaleDecision,
};
use thiserror::Error;

const DECISION_SCENARIO_SEED: u64 = 0x7363_616c_652d_636f;
/// Schedule visibility covers the worst launch delay, rebalance, report,
/// objective budget, and slack. A posterior lead-time quantile can replace it.
/// Scheduled work has no known partition. Resource feasibility prices it.
/// Partition placement excludes this sentinel before it indexes columns.
const SCHEDULED_PARTITION: u32 = u32::MAX;

/// One calculated reason that a posterior scenario rejects an action.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionRejection {
    /// The scenario's missed events exceed the objective epsilon.
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
}

/// Fixed curve classes and their capacity-cell members.
///
/// Members in one class have equal supply at each candidate concurrency and
/// equal service time. The member ranges use one construction-time CSR layout.
struct CapacityClasses {
    representatives: Vec<usize>,
    member_offsets: Vec<usize>,
    members: Vec<usize>,
}

impl CapacityClasses {
    fn new(
        configuration: &Configuration,
        capacity: &CapacityFactor,
    ) -> Result<Self, ConfigurationError> {
        let cell_count = usize::try_from(capacity.curve_posterior_value_count())
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let candidate_concurrency = candidate_concurrency_ladder(configuration);
        let mut representatives = Vec::with_capacity(cell_count);
        let mut class_indexes = Vec::with_capacity(cell_count);
        for cell in 0..cell_count {
            let (curve, _) = capacity.curve_and_probability(cell);
            let existing = representatives.iter().position(|&representative| {
                let (candidate, _) = capacity.curve_and_probability(representative);
                curves_are_equivalent(curve, candidate, &candidate_concurrency)
            });
            let class = if let Some(class) = existing {
                class
            } else {
                representatives.push(cell);
                representatives.len() - 1
            };
            class_indexes.push(class);
        }
        let mut member_offsets = vec![0; representatives.len() + 1];
        for &class in &class_indexes {
            member_offsets[class + 1] += 1;
        }
        for class in 0..representatives.len() {
            member_offsets[class + 1] += member_offsets[class];
        }
        let mut members = vec![0; cell_count];
        let mut write_offsets = member_offsets[..representatives.len()].to_vec();
        for (cell, &class) in class_indexes.iter().enumerate() {
            members[write_offsets[class]] = cell;
            write_offsets[class] += 1;
        }
        Ok(Self {
            representatives,
            member_offsets,
            members,
        })
    }

    const fn len(&self) -> usize {
        self.representatives.len()
    }

    fn representative(&self, class: usize) -> usize {
        self.representatives[class]
    }

    fn members(&self, class: usize) -> &[usize] {
        &self.members[self.member_offsets[class]..self.member_offsets[class + 1]]
    }
}

fn candidate_concurrency_ladder(configuration: &Configuration) -> Vec<f64> {
    (1..=configuration.replica_count_max)
        .map(|replicas| {
            f64::from(replicas.min(configuration.partition_count))
                * f64::from(configuration.slots_per_replica)
        })
        .collect()
}

fn curves_are_equivalent(
    left: crate::CapacityCurve,
    right: crate::CapacityCurve,
    candidate_concurrency: &[f64],
) -> bool {
    left.service_time_seconds()
        .total_cmp(&right.service_time_seconds())
        .is_eq()
        && candidate_concurrency.iter().all(|&concurrency| {
            left.sustainable_throughput(concurrency)
                .total_cmp(&right.sustainable_throughput(concurrency))
                .is_eq()
        })
}

/// All posterior and transition state that survives a controller tick.
pub struct ScaleState {
    simd_level: Level,
    configuration: Configuration,
    model_time: ModelTime,
    arrivals: ArrivalFactor,
    capacity: CapacityFactor,
    capacity_artifact: PriorArtifact,
    capacity_classes: CapacityClasses,
    reliability: ReliabilityFactor,
    partition_placement: PartitionFactor,
    lead_time: LaunchTimeFactor,
    rebalance_time: RebalanceTimeFactor,
    current_replicas: u32,
    standing_target: u32,
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
        let capacity = CapacityFactor::new_with_prior(
            capacity_grid,
            configuration.capacity_change_rate_per_second,
            &configuration.arrival_prior,
            configuration.capacity_concurrency_max()?,
            configuration.resource_exposure_min_seconds(),
            configuration.resource_window_attempt_count_max,
        )?;
        let capacity_artifact = capacity.artifact(configuration.capacity_change_rate_per_second)?;
        let capacity_classes = CapacityClasses::new(&configuration, &capacity)?;
        let class_count =
            u32::try_from(capacity_classes.len()).map_err(|_| ConfigurationError::PlatformLimit)?;
        let minimum = class_count
            .checked_mul(POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN)
            .ok_or(ConfigurationError::PlatformLimit)?;
        if configuration.posterior_sample_count < minimum {
            return Err(ConfigurationError::InsufficientPosteriorSamples {
                sample_count: configuration.posterior_sample_count,
                minimum,
            });
        }
        let partition_placement = PartitionFactor::new(configuration.partition_count)?;
        let reliability = ReliabilityFactor::new(configuration.reliability_prior);
        let lead_time = LaunchTimeFactor::new(&configuration.launch_time_prior);
        let rebalance_time = RebalanceTimeFactor::new(&configuration.rebalance_time_prior);
        let arrivals = ArrivalFactor::new(&configuration.arrival_prior);
        Ok(Self {
            simd_level: Level::new(),
            configuration,
            model_time: ModelTime::from_micros(0),
            arrivals,
            capacity,
            capacity_artifact,
            capacity_classes,
            reliability,
            partition_placement,
            lead_time,
            rebalance_time,
            current_replicas: 1,
            standing_target: 0,
        })
    }

    /// Allocates scratch memory for this state's validated capacity grid.
    ///
    /// # Errors
    ///
    /// Returns an error when a validated bound exceeds the platform limit.
    pub fn new_scratch(&self) -> Result<ScaleScratch, ConfigurationError> {
        ScaleScratch::new(&self.configuration, self.capacity_classes.len())
    }

    /// Returns the fixed configuration.
    #[must_use]
    pub const fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    /// Returns true when time-rescaled residuals reject the Markov clock.
    #[must_use]
    pub const fn capacity_clock_rejected(&self) -> bool {
        self.capacity.markov_clock_rejected()
    }

    /// Returns the time-rescaled completion-clock check.
    #[must_use]
    pub fn capacity_clock_check(&self) -> CapacityClockCheck {
        self.capacity.clock_check()
    }

    /// Returns the capacity model's complete prior artifact contract.
    #[must_use]
    pub const fn capacity_artifact(&self) -> &PriorArtifact {
        &self.capacity_artifact
    }

    /// Returns the number of capacity classes used for stratified sampling.
    #[must_use]
    pub fn capacity_class_count(&self) -> u32 {
        self.capacity_classes.len() as u32
    }

    /// Returns the minimum posterior draws required for each capacity class.
    #[must_use]
    pub const fn posterior_samples_per_capacity_class_min(&self) -> u32 {
        POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN
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

    /// Writes the completion predictive with the likelihood's shared mean.
    ///
    /// The predictive uses the current posterior before the window update.
    ///
    /// # Errors
    ///
    /// Returns an error when the buffer has the wrong fixed length.
    pub fn write_completion_posterior(
        &mut self,
        window: &ResourceWindow,
        cells: &mut [CompletionPosteriorCell],
    ) -> Result<(), PosteriorError> {
        self.capacity.write_completion_posterior(window, cells)
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

    /// Returns the exact count prediction for one observation interval.
    ///
    /// # Errors
    ///
    /// Returns an error when exposure is outside the validated domain.
    pub fn arrival_count_predictive(
        &self,
        observed_count: u32,
        exposure_seconds: f64,
    ) -> Result<ArrivalCountPredictive, ArrivalPredictiveError> {
        self.arrivals.count_predictive(
            self.model_time.as_micros(),
            observed_count,
            exposure_seconds,
        )
    }

    /// Returns the fixed value count for the finite arrival-rate posterior.
    #[must_use]
    pub const fn arrival_posterior_value_count(&self) -> u32 {
        ArrivalPrior::POSTERIOR_VALUE_COUNT
    }

    /// Writes the finite arrival-rate posterior into caller-owned buffers.
    ///
    /// # Errors
    ///
    /// Returns an error when either buffer has the wrong fixed length.
    pub fn write_arrival_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let expected = self.arrival_posterior_value_count();
        if self
            .arrivals
            .write_posterior(self.model_time.as_micros(), values, probabilities)
        {
            Ok(())
        } else {
            Err(PosteriorError::BufferLength { expected })
        }
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
    ///
    /// # Errors
    ///
    /// Returns a structured error when finite arithmetic cannot invert the CDF.
    pub fn lead_time_predictive_quantile(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        probability: f64,
    ) -> Result<f64, PredictiveQuantileError> {
        self.lead_time
            .predictive_quantile(direction, replica_delta, probability)
    }

    /// Returns the fast and slow launch components for one replica delta.
    #[must_use]
    pub fn launch_component_summary(&self, replica_delta: u32) -> LaunchComponentSummary {
        self.lead_time.component_summary(replica_delta)
    }

    /// Returns launch components for the latest accepted replica delta.
    #[must_use]
    pub fn latest_launch_component_summary(&self) -> LaunchComponentSummary {
        self.lead_time.last_component_summary()
    }

    /// Returns the posterior predictive CDF for one rebalance pause.
    #[must_use]
    pub fn rebalance_time_predictive_cdf(&self, elapsed_seconds: f64) -> f64 {
        self.rebalance_time.predictive_cdf(elapsed_seconds)
    }

    /// Returns one posterior predictive rebalance-pause quantile.
    ///
    /// # Errors
    ///
    /// Returns a structured error when finite arithmetic cannot invert the CDF.
    pub fn rebalance_time_predictive_quantile(
        &self,
        probability: f64,
    ) -> Result<f64, PredictiveQuantileError> {
        self.rebalance_time.predictive_quantile(probability)
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
            PosteriorQuery::CapacityContaminationProbability => {
                Ok(self.capacity.contamination_posterior_value_count())
            }
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
            PosteriorQuery::LeadTime { .. } => Ok(self.lead_time.posterior_value_count()),
            PosteriorQuery::RebalanceTime { .. } => Ok(self.rebalance_time.posterior_value_count()),
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
            PosteriorQuery::CapacityContaminationProbability => self
                .capacity
                .write_contamination_posterior(values, probabilities),
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
            PosteriorQuery::RebalanceTime { .. } => {
                if self.rebalance_time.write_posterior(values, probabilities) {
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
    resource_cohorts: WorkCohorts,
    partition_offsets: Vec<u32>,
    partition_write_offsets: Vec<u32>,
    partition_cohort_indexes: Vec<u32>,
    moved_partition_counts: Vec<u32>,
    posterior_missed_work_sums: Vec<f64>,
    posterior_loss_sums: Vec<f64>,
    posterior_late_area_sums: Vec<f64>,
    posterior_replica_seconds_sums: Vec<f64>,
    posterior_supply_sums: Vec<f64>,
    class_masses: Vec<f64>,
    candidate_concurrency: Vec<f64>,
    scenario_shortfall: Vec<f64>,
    scenario_late_area: Vec<f64>,
    scenario_drain_seconds: Vec<f64>,
    scenario_replica_seconds: Vec<f64>,
    scenario_rejection: Vec<u8>,
    scenario_missed_work: Vec<f64>,
    scenario_supply: Vec<f64>,
    scenario_arrival_path_end_seconds: Vec<f64>,
    scenario_arrival_path_rates: Vec<f64>,
    scenario_event_count: Vec<f64>,
    scenario_partition_missed_work: Vec<f64>,
    scenario_partition_late_area: Vec<f64>,
    scenario_workspaces: Vec<ScenarioWorkspace>,
    active_scenario_count: usize,
    active_inner_count: usize,
    decision_curve_sample_count: u32,
    decision_rate: f64,
}

/// Retained scalar columns for one replica action.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecisionActionColumns {
    /// Zero-based replica action index.
    pub action_index: u32,
    /// Posterior late-area sum in event-delay-seconds.
    pub late_area_mean: f64,
    /// Posterior mean replica-seconds.
    pub replica_seconds_mean: f64,
    /// Posterior expected cost.
    pub cost: f64,
}

/// Bounded scalar summary for one controller decision.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecisionColumnSummary {
    /// Columns for the selected action.
    pub selected: DecisionActionColumns,
    /// Columns for the next action under the same ordering.
    pub runner_up: Option<DecisionActionColumns>,
    /// Paired standard error of the runner-up cost minus selected cost.
    pub paired_standard_error: Option<f64>,
    /// Smallest action index that covers known demand.
    pub demand_floor: u32,
}

/// One worker's private buffers for evaluating whole scenarios.
///
/// Scenario evaluation parallelizes over scenarios, not candidates:
/// each worker owns one workspace and a disjoint range of scenario
/// cells, so workers share no mutable state. Scenario randomness is
/// index-derived ([`decision_random`]) and the aggregation pass reads
/// the cells in a fixed order, so the parallel evaluation is
/// bit-identical to a serial one.
struct ScenarioWorkspace {
    edf: EdfScratch,
    placement_edf: EdfScratch,
    placement_cohorts: WorkCohorts,
    posterior_resource_supply: Vec<f64>,
    partition_order: Vec<u32>,
    partition_share_draws: Vec<f64>,
    moved_partition_share: Vec<f64>,
    commitment_pause_seconds: Vec<f64>,
    rebalancing_ready_seconds: f64,
    trajectory_offsets: Vec<u32>,
    trajectory: TrajectoryColumns,
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

#[derive(Clone, Copy)]
struct PartitionDeadlineOutcome {
    missed_work: f64,
    late_area: f64,
}

struct CandidateEvaluation<'a> {
    model_time_micros: u64,
    deadline_budget_micros: u64,
    current_supply: f64,
    resource_cohorts: &'a WorkCohorts,
    trajectory_offsets: &'a [u32],
    trajectory_pause_seconds: &'a [f64],
    trajectory_ready_seconds: &'a [f64],
    trajectory_during_supply: &'a [f64],
    trajectory_after_supply: &'a [f64],
    horizon_micros: u64,
    arrival_path: ArrivalPath<'a>,
}

/// The decision-invariant inputs every scenario reads.
struct ScenarioShared<'a> {
    resource_cohorts: &'a WorkCohorts,
    moved_partition_counts: &'a [u32],
    partition_offsets: &'a [u32],
    partition_cohort_indexes: &'a [u32],
    partition_count: usize,
    candidate_concurrency: &'a [f64],
    action_count: usize,
    current_index: usize,
    normal_events: f64,
    failure_events: f64,
    calendar: Option<CalendarForecast<'a>>,
    actuation_commitments: &'a ActuationCommitments,
    inner_count: usize,
}

/// One worker's disjoint chunk of the scenario-indexed output cells.
struct ScenarioColumns<'a> {
    first_scenario: usize,
    scenario_count: usize,
    candidate_stride: usize,
    path_stride: usize,
    supply: &'a mut [f64],
    shortfall: &'a mut [f64],
    late_area: &'a mut [f64],
    drain_seconds: &'a mut [f64],
    missed_work: &'a mut [f64],
    replica_seconds: &'a mut [f64],
    rejection: &'a mut [u8],
    event_count: &'a mut [f64],
    partition_missed_work: &'a mut [f64],
    partition_late_area: &'a mut [f64],
    arrival_path_end_seconds: &'a mut [f64],
    arrival_path_rates: &'a mut [f64],
}

/// One scenario's output cells.
struct ScenarioCells<'a> {
    supply: &'a mut [f64],
    shortfall: &'a mut [f64],
    late_area: &'a mut [f64],
    drain_seconds: &'a mut [f64],
    missed_work: &'a mut [f64],
    replica_seconds: &'a mut [f64],
    rejection: &'a mut [u8],
    event_count: &'a mut f64,
    partition_missed_work: &'a mut f64,
    partition_late_area: &'a mut f64,
    arrival_path_end_seconds: &'a mut [f64],
    arrival_path_rates: &'a mut [f64],
}

impl ScenarioColumns<'_> {
    /// Splits this chunk into disjoint halves at one scenario index.
    fn split_at(self, scenario: usize) -> (Self, Self) {
        let cell = scenario * self.candidate_stride;
        let path_cell = scenario * self.path_stride;
        let (left_supply, right_supply) = self.supply.split_at_mut(cell);
        let (left_shortfall, right_shortfall) = self.shortfall.split_at_mut(cell);
        let (left_late_area, right_late_area) = self.late_area.split_at_mut(cell);
        let (left_drain, right_drain) = self.drain_seconds.split_at_mut(cell);
        let (left_missed, right_missed) = self.missed_work.split_at_mut(cell);
        let (left_replica, right_replica) = self.replica_seconds.split_at_mut(cell);
        let (left_rejection, right_rejection) = self.rejection.split_at_mut(cell);
        let (left_events, right_events) = self.event_count.split_at_mut(scenario);
        let (left_partition_missed, right_partition_missed) =
            self.partition_missed_work.split_at_mut(scenario);
        let (left_partition_late, right_partition_late) =
            self.partition_late_area.split_at_mut(scenario);
        let (left_path_ends, right_path_ends) =
            self.arrival_path_end_seconds.split_at_mut(path_cell);
        let (left_path_rates, right_path_rates) = self.arrival_path_rates.split_at_mut(path_cell);
        (
            ScenarioColumns {
                first_scenario: self.first_scenario,
                scenario_count: scenario,
                candidate_stride: self.candidate_stride,
                path_stride: self.path_stride,
                supply: left_supply,
                shortfall: left_shortfall,
                late_area: left_late_area,
                drain_seconds: left_drain,
                missed_work: left_missed,
                replica_seconds: left_replica,
                rejection: left_rejection,
                event_count: left_events,
                partition_missed_work: left_partition_missed,
                partition_late_area: left_partition_late,
                arrival_path_end_seconds: left_path_ends,
                arrival_path_rates: left_path_rates,
            },
            ScenarioColumns {
                first_scenario: self.first_scenario + scenario,
                scenario_count: self.scenario_count - scenario,
                candidate_stride: self.candidate_stride,
                path_stride: self.path_stride,
                supply: right_supply,
                shortfall: right_shortfall,
                late_area: right_late_area,
                drain_seconds: right_drain,
                missed_work: right_missed,
                replica_seconds: right_replica,
                rejection: right_rejection,
                event_count: right_events,
                partition_missed_work: right_partition_missed,
                partition_late_area: right_partition_late,
                arrival_path_end_seconds: right_path_ends,
                arrival_path_rates: right_path_rates,
            },
        )
    }

    /// Borrows one local scenario's cells.
    fn cells(&mut self, local: usize) -> ScenarioCells<'_> {
        let first = local * self.candidate_stride;
        let last = first + self.candidate_stride;
        let path_first = local * self.path_stride;
        let path_last = path_first + self.path_stride;
        ScenarioCells {
            supply: &mut self.supply[first..last],
            shortfall: &mut self.shortfall[first..last],
            late_area: &mut self.late_area[first..last],
            drain_seconds: &mut self.drain_seconds[first..last],
            missed_work: &mut self.missed_work[first..last],
            replica_seconds: &mut self.replica_seconds[first..last],
            rejection: &mut self.rejection[first..last],
            event_count: &mut self.event_count[local],
            partition_missed_work: &mut self.partition_missed_work[local],
            partition_late_area: &mut self.partition_late_area[local],
            arrival_path_end_seconds: &mut self.arrival_path_end_seconds[path_first..path_last],
            arrival_path_rates: &mut self.arrival_path_rates[path_first..path_last],
        }
    }
}

impl ScratchBounds {
    fn new(configuration: &Configuration) -> Result<Self, ConfigurationError> {
        let cohort_count_max = usize::try_from(configuration.cohort_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_count = usize::try_from(configuration.partition_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let replica_count_max = usize::try_from(configuration.replica_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let scheduled_release_count_max =
            usize::try_from(configuration.scheduled_release_count_max)
                .map_err(|_| ConfigurationError::PlatformLimit)?;
        let work_cohort_count_max = partition_count
            .checked_mul(DemandClass::COUNT_USIZE)
            .and_then(|count| cohort_count_max.checked_add(count))
            .and_then(|count| count.checked_add(scheduled_release_count_max))
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
            // Each candidate holds its own transition events plus at most
            // one strictly-increasing repair for every replica target.
            trajectory_event_count_max: replica_count_max
                .checked_mul(
                    replica_count_max
                        .checked_mul(2)
                        .and_then(|events| events.checked_add(1))
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

impl ScenarioWorkspace {
    fn new(bounds: &ScratchBounds) -> Result<Self, ConfigurationError> {
        Ok(Self {
            edf: EdfScratch::new(bounds.work_cohort_count_max_u32)?,
            placement_edf: EdfScratch::new(bounds.work_cohort_count_max_u32)?,
            placement_cohorts: WorkCohorts::new(bounds.work_cohort_count_max),
            posterior_resource_supply: vec![0.0_f64; bounds.replica_count_max],
            partition_order: vec![0; bounds.partition_count],
            partition_share_draws: vec![0.0_f64; bounds.partition_count],
            moved_partition_share: vec![0.0_f64; bounds.partition_offset_count],
            commitment_pause_seconds: vec![0.0_f64; bounds.replica_count_max],
            rebalancing_ready_seconds: f64::INFINITY,
            trajectory_offsets: vec![0; bounds.replica_count_max + 1],
            trajectory: TrajectoryColumns::new(bounds.trajectory_event_count_max),
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
    fn new(
        configuration: &Configuration,
        capacity_class_count: usize,
    ) -> Result<Self, ConfigurationError> {
        configuration.validate()?;
        let bounds = ScratchBounds::new(configuration)?;
        let &ScratchBounds {
            work_cohort_count_max,
            partition_count,
            partition_offset_count,
            replica_count_max,
            posterior_sample_count,
            scenario_cell_count,
            arrival_path_cell_count,
            ..
        } = &bounds;
        let candidate_concurrency = (1..=configuration.replica_count_max)
            .map(|replicas| f64::from(replicas) * f64::from(configuration.slots_per_replica))
            .collect::<Vec<_>>();
        let moved_partition_counts = moved_partition_count_matrix(
            configuration.partition_count,
            configuration.replica_count_max,
        )?;
        let worker_count = rayon::current_num_threads()
            .min(posterior_sample_count)
            .max(1);
        let mut scenario_workspaces = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            scenario_workspaces.push(ScenarioWorkspace::new(&bounds)?);
        }
        Ok(Self {
            resource_cohorts: WorkCohorts::new(work_cohort_count_max),
            partition_offsets: vec![0; partition_offset_count],
            partition_write_offsets: vec![0; partition_count],
            partition_cohort_indexes: vec![0; work_cohort_count_max],
            moved_partition_counts,
            posterior_missed_work_sums: vec![0.0_f64; replica_count_max],
            posterior_loss_sums: vec![0.0_f64; replica_count_max],
            posterior_late_area_sums: vec![0.0_f64; replica_count_max],
            posterior_replica_seconds_sums: vec![0.0_f64; replica_count_max],
            posterior_supply_sums: vec![0.0_f64; replica_count_max],
            class_masses: vec![0.0_f64; capacity_class_count],
            candidate_concurrency,
            scenario_shortfall: vec![0.0_f64; scenario_cell_count],
            scenario_late_area: vec![0.0_f64; scenario_cell_count],
            scenario_drain_seconds: vec![0.0_f64; scenario_cell_count],
            scenario_replica_seconds: vec![0.0_f64; scenario_cell_count],
            scenario_rejection: vec![0; scenario_cell_count],
            scenario_missed_work: vec![0.0_f64; scenario_cell_count],
            scenario_supply: vec![0.0_f64; scenario_cell_count],
            scenario_arrival_path_end_seconds: vec![0.0_f64; arrival_path_cell_count],
            scenario_arrival_path_rates: vec![0.0_f64; arrival_path_cell_count],
            scenario_event_count: vec![0.0_f64; posterior_sample_count],
            scenario_partition_missed_work: vec![0.0_f64; posterior_sample_count],
            scenario_partition_late_area: vec![0.0_f64; posterior_sample_count],
            scenario_workspaces,
            active_scenario_count: posterior_sample_count,
            active_inner_count: 0,
            decision_curve_sample_count: 0,
            decision_rate: 0.0_f64,
        })
    }

    /// Returns the fixed number of replica candidates.
    #[must_use]
    pub fn decision_candidate_count(&self) -> usize {
        self.posterior_loss_sums.len()
    }

    /// Returns bounded scalar columns for the selected and runner-up actions.
    #[must_use]
    pub fn decision_column_summary(&self, selected: usize) -> Option<DecisionColumnSummary> {
        let action_count = decision_action_count(self);
        if self.decision_curve_sample_count == 0 || selected >= action_count {
            return None;
        }
        let columns = ActionColumns {
            late_area_sums: &self.posterior_late_area_sums[..action_count],
            replica_seconds_sums: &self.posterior_replica_seconds_sums[..action_count],
            rate: self.decision_rate,
        };
        let runner_up_index = (0..action_count)
            .filter(|index| *index != selected)
            .min_by(|left, right| compare_actions(*left, *right, &columns));
        Some(DecisionColumnSummary {
            selected: self.action_columns(selected, columns.rate),
            runner_up: runner_up_index.map(|index| self.action_columns(index, columns.rate)),
            paired_standard_error: runner_up_index
                .map(|runner_up| self.paired_standard_error(selected, runner_up, columns.rate)),
            demand_floor: 0,
        })
    }

    fn paired_standard_error(&self, selected: usize, runner_up: usize, rate: f64) -> f64 {
        if self.active_inner_count < 2 {
            return f64::NAN;
        }
        let stride = self.posterior_loss_sums.len();
        let count = u32::try_from(self.active_inner_count).map_or(f64::INFINITY, f64::from);
        let mut variance = 0.0_f64;
        for class in 0..self.class_masses.len() {
            let first = class * self.active_inner_count;
            let mean = (first..first + self.active_inner_count)
                .map(|scenario| {
                    let cell = scenario * stride;
                    self.scenario_late_area[cell + runner_up]
                        - self.scenario_late_area[cell + selected]
                        + rate
                            * (self.scenario_replica_seconds[cell + runner_up]
                                - self.scenario_replica_seconds[cell + selected])
                })
                .sum::<f64>()
                / count;
            let sum = (first..first + self.active_inner_count)
                .map(|scenario| {
                    let cell = scenario * stride;
                    let difference = self.scenario_late_area[cell + runner_up]
                        - self.scenario_late_area[cell + selected]
                        + rate
                            * (self.scenario_replica_seconds[cell + runner_up]
                                - self.scenario_replica_seconds[cell + selected]);
                    (difference - mean).powi(2)
                })
                .sum::<f64>();
            variance += self.class_masses[class].powi(2) * sum / (count * (count - 1.0_f64));
        }
        variance.sqrt()
    }

    fn action_columns(&self, index: usize, rate: f64) -> DecisionActionColumns {
        DecisionActionColumns {
            action_index: u32::try_from(index).map_or(u32::MAX, |value| value),
            late_area_mean: self.posterior_late_area_sums[index],
            replica_seconds_mean: self.posterior_replica_seconds_sums[index],
            cost: self.posterior_late_area_sums[index]
                + rate * self.posterior_replica_seconds_sums[index],
        }
    }

    /// Writes the expected normalized loss for each candidate.
    ///
    /// Candidate index zero represents one replica. The last index represents
    /// the configured replica limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid buffer or an unavailable decision.
    pub fn write_decision_expected_losses(
        &self,
        expected_losses: &mut [f64],
    ) -> Result<(), DecisionCurveError> {
        if expected_losses.len() != self.posterior_loss_sums.len() {
            return Err(DecisionCurveError::BufferLength {
                expected: self.posterior_loss_sums.len(),
            });
        }
        if self.decision_curve_sample_count == 0 {
            return Err(DecisionCurveError::Unavailable);
        }
        let action_count = decision_action_count(self);
        for (index, expected_loss) in expected_losses.iter_mut().enumerate() {
            if index < action_count {
                *expected_loss = self.posterior_loss_sums[index];
            } else {
                *expected_loss = f64::INFINITY;
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
            let inner_count =
                u32::try_from(self.active_inner_count).map_or(u32::MAX, |value| value);
            let mass =
                self.class_masses[scenario / self.active_inner_count] / f64::from(inner_count);
            let first = scenario * candidate_count;
            for (target, probability) in probabilities.iter_mut().enumerate().take(action_count) {
                let rejects = self.scenario_rejection[first + target] & reason.bit() != 0;
                *probability += mass * f64::from(u8::from(rejects));
            }
        }
        Ok(())
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
        scheduled_releases,
        partition_arrivals,
        resource,
        attempt_outcomes,
        launch,
        rebalance,
        current_replicas,
        actuation_commitments,
    } = observation;
    let elapsed =
        Duration::from_micros(now.as_micros().saturating_sub(state.model_time.as_micros()));
    state.model_time = now;
    state.capacity.transition(elapsed);
    state.lead_time.transition(elapsed);
    state.rebalance_time.transition(elapsed);
    if let Some(evidence) = resource {
        state.capacity.update(evidence);
    } else {
        state.capacity.omit_observation();
    }
    if let Some(evidence) = attempt_outcomes {
        state.reliability.update(evidence);
    }
    if let Some(evidence) = arrivals {
        state.arrivals.update(evidence, calendar, now.as_micros());
    } else {
        state.arrivals.prepare_calendar(calendar, now.as_micros());
    }
    if let Some(evidence) = partition_arrivals {
        state.partition_placement.update(evidence.consume());
    }
    if let Some(evidence) = launch {
        state.lead_time.update(state.simd_level, evidence);
    }
    if let Some(evidence) = rebalance {
        state.rebalance_time.update(state.simd_level, evidence);
    }
    if let Some(replicas) = current_replicas {
        state.current_replicas = replicas;
    }

    let decision = select_target(
        state,
        scratch,
        cohorts,
        backlog,
        scheduled_releases,
        calendar,
        actuation_commitments,
    );
    if let ScaleDecision::Apply(apply) = &decision {
        state.standing_target = apply.target;
    }
    decision
}

fn select_target(
    state: &mut ScaleState,
    scratch: &mut ScaleScratch,
    cohorts: &CohortColumns,
    backlog: &BacklogColumns,
    scheduled_releases: &[ScheduledRelease],
    calendar: Option<CalendarForecast<'_>>,
    actuation_commitments: &ActuationCommitments,
) -> ScaleDecision {
    let (normal_events, failure_events) = demand_class_totals(cohorts, backlog);
    prepare_work_cohorts(state, scratch, cohorts, backlog, scheduled_releases);
    prepare_partition_work(state, scratch);
    prepare_candidate_concurrency(state, scratch);
    scratch.posterior_missed_work_sums.fill(0.0_f64);
    scratch.posterior_loss_sums.fill(0.0_f64);
    let scenario_count = state.configuration.posterior_sample_count;
    let class_count = state.capacity_classes.len() as u32;
    let inner_count = scenario_count / class_count;
    evaluate_scenarios(
        state,
        scratch,
        normal_events,
        failure_events,
        calendar,
        actuation_commitments,
        inner_count,
    );
    let target_index = numerical_decision(state, scratch);
    finish_decision(state, scratch, target_index)
}

/// Evaluates every posterior scenario, scenarios fanned across workers.
///
/// Each worker owns one [`ScenarioWorkspace`] and a disjoint, contiguous
/// range of scenario cells, so workers share no mutable state. Scenario
/// randomness is index-derived ([`decision_random`]) and
/// [`finalize_scenario_columns`] reads the cells in a fixed order
/// afterwards, so the parallel evaluation is bit-identical to a serial
/// loop.
fn evaluate_scenarios(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    normal_events: f64,
    failure_events: f64,
    calendar: Option<CalendarForecast<'_>>,
    actuation_commitments: &ActuationCommitments,
    inner_count: u32,
) {
    let inner_count = inner_count as usize;
    let scenario_total = state.capacity_classes.len() * inner_count;
    scratch.active_scenario_count = scenario_total;
    scratch.active_inner_count = inner_count;
    for class in 0..state.capacity_classes.len() {
        scratch.class_masses[class] = state
            .capacity_classes
            .members(class)
            .iter()
            .map(|&cell| state.capacity.curve_and_probability(cell).1)
            .sum();
    }
    let candidate_stride = scratch.posterior_loss_sums.len();
    let path_stride = state.configuration.arrival_prior.path_segment_count_max();
    let action_count = decision_action_count(scratch);
    let worker_count = scratch.scenario_workspaces.len().min(scenario_total).max(1);
    let scenario_chunk = scenario_total.div_ceil(worker_count);
    let active_workers = scenario_total.div_ceil(scenario_chunk);
    for workspace in &mut scratch.scenario_workspaces[..active_workers] {
        prepare(&scratch.resource_cohorts, &mut workspace.edf);
    }
    let columns = ScenarioColumns {
        first_scenario: 0,
        scenario_count: scenario_total,
        candidate_stride,
        path_stride,
        supply: &mut scratch.scenario_supply[..scenario_total * candidate_stride],
        shortfall: &mut scratch.scenario_shortfall[..scenario_total * candidate_stride],
        late_area: &mut scratch.scenario_late_area[..scenario_total * candidate_stride],
        drain_seconds: &mut scratch.scenario_drain_seconds[..scenario_total * candidate_stride],
        missed_work: &mut scratch.scenario_missed_work[..scenario_total * candidate_stride],
        replica_seconds: &mut scratch.scenario_replica_seconds[..scenario_total * candidate_stride],
        rejection: &mut scratch.scenario_rejection[..scenario_total * candidate_stride],
        event_count: &mut scratch.scenario_event_count[..scenario_total],
        partition_missed_work: &mut scratch.scenario_partition_missed_work[..scenario_total],
        partition_late_area: &mut scratch.scenario_partition_late_area[..scenario_total],
        arrival_path_end_seconds: &mut scratch.scenario_arrival_path_end_seconds
            [..scenario_total * path_stride],
        arrival_path_rates: &mut scratch.scenario_arrival_path_rates
            [..scenario_total * path_stride],
    };
    let shared = ScenarioShared {
        resource_cohorts: &scratch.resource_cohorts,
        moved_partition_counts: &scratch.moved_partition_counts,
        partition_offsets: &scratch.partition_offsets,
        partition_cohort_indexes: &scratch.partition_cohort_indexes,
        partition_count: scratch.partition_write_offsets.len(),
        candidate_concurrency: &scratch.candidate_concurrency,
        action_count,
        current_index: state.current_replicas as usize - 1,
        normal_events,
        failure_events,
        calendar,
        actuation_commitments,
        inner_count,
    };
    evaluate_scenario_workers(
        state,
        &shared,
        &mut scratch.scenario_workspaces[..active_workers],
        columns,
        scenario_chunk,
    );
    finalize_scenario_columns(state, scratch);
}

/// Splits the workspaces and their scenario chunks across rayon workers.
fn evaluate_scenario_workers(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspaces: &mut [ScenarioWorkspace],
    mut columns: ScenarioColumns<'_>,
    scenario_chunk: usize,
) {
    if let [workspace] = workspaces {
        for local in 0..columns.scenario_count {
            let scenario = columns.first_scenario + local;
            evaluate_one_scenario(state, shared, workspace, columns.cells(local), scenario);
        }
        return;
    }
    let middle = workspaces.len() / 2;
    let scenario_middle = (middle * scenario_chunk).min(columns.scenario_count);
    let (left_workspaces, right_workspaces) = workspaces.split_at_mut(middle);
    let (left_columns, right_columns) = columns.split_at(scenario_middle);
    rayon::join(
        || evaluate_scenario_workers(state, shared, left_workspaces, left_columns, scenario_chunk),
        || {
            evaluate_scenario_workers(
                state,
                shared,
                right_workspaces,
                right_columns,
                scenario_chunk,
            );
        },
    );
}

/// Samples one scenario's draws and writes its decision cells.
fn evaluate_one_scenario(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspace: &mut ScenarioWorkspace,
    cells: ScenarioCells<'_>,
    scenario: usize,
) {
    let capacity_class = scenario / shared.inner_count;
    let sample = scenario as u32;
    let representative = state.capacity_classes.representative(capacity_class);
    let (curve, _) = state.capacity.curve_and_probability(representative);
    CapacityFactor::fill_throughput(
        state.simd_level,
        curve,
        shared.candidate_concurrency,
        &mut workspace.posterior_resource_supply,
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
        shared.normal_events,
        shared.failure_events,
    );
    dispatch!(state.simd_level, simd => scale_and_store_supply(
        simd,
        event_supply_factor,
        &mut workspace.posterior_resource_supply,
        &mut *cells.supply,
    ));
    let current_concurrency = shared.candidate_concurrency[shared.current_index];
    let current_supply = curve.sustainable_throughput(current_concurrency) * event_supply_factor;
    let lead_random = decision_random(sample, DecisionRandomDomain::LeadTime);
    let rebalance_random = decision_random(sample, DecisionRandomDomain::Rebalance);
    let mut placement_random = decision_random(sample, DecisionRandomDomain::Placement);
    state.partition_placement.sample_moved_prefix(
        &mut placement_random,
        &mut workspace.partition_order,
        &mut workspace.partition_share_draws,
        &mut workspace.moved_partition_share,
    );
    let (planning_horizon_micros, disturbance_horizon_micros) =
        scenario_horizons(state, shared.resource_cohorts);
    let path_length = sample_scenario_path(
        state,
        shared.calendar,
        sample,
        disturbance_horizon_micros,
        cells.arrival_path_end_seconds,
        cells.arrival_path_rates,
    );
    prepare_supply_trajectories(
        state,
        shared,
        workspace,
        &ScenarioDraws {
            current_supply,
            lead_random,
            rebalance_random,
            commitment_random: decision_random(sample, DecisionRandomDomain::Commitment),
            arrival_path_end_seconds: &cells.arrival_path_end_seconds[..path_length],
            arrival_path_rates: &cells.arrival_path_rates[..path_length],
        },
    );
    evaluate_scenario_outcome(
        state,
        shared,
        workspace,
        cells,
        &ScenarioForecast {
            current_supply,
            event_supply_factor,
            service_time_seconds: curve.service_time_seconds(),
            path_length,
            planning_horizon_micros,
            disturbance_horizon_micros,
        },
    );
}

/// Evaluates one scenario's prepared trajectories into decision cells.
fn evaluate_scenario_outcome(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspace: &mut ScenarioWorkspace,
    mut cells: ScenarioCells<'_>,
    forecast: &ScenarioForecast,
) {
    let start_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    let planning_horizon_seconds =
        Duration::from_micros(forecast.planning_horizon_micros).as_secs_f64();
    let disturbance_horizon_seconds =
        Duration::from_micros(forecast.disturbance_horizon_micros).as_secs_f64();
    let partition_outcome = partition_deadline_outcome(
        state,
        shared,
        workspace,
        forecast.service_time_seconds,
        forecast.event_supply_factor,
        planning_horizon_seconds,
    );
    *cells.partition_missed_work = partition_outcome.missed_work;
    *cells.partition_late_area = partition_outcome.late_area;
    let arrival_path = ArrivalPath {
        start_seconds,
        end_seconds: &cells.arrival_path_end_seconds[..forecast.path_length],
        rates: &cells.arrival_path_rates[..forecast.path_length],
    };
    *cells.event_count = scenario_event_count(
        shared.normal_events,
        shared.failure_events,
        &arrival_path,
        start_seconds,
        disturbance_horizon_seconds,
        shared.resource_cohorts,
        forecast.disturbance_horizon_micros,
    );
    evaluate_candidates(
        &CandidateEvaluation {
            model_time_micros: state.model_time.as_micros(),
            deadline_budget_micros: state.configuration.objective.budget_micros(),
            current_supply: forecast.current_supply,
            resource_cohorts: shared.resource_cohorts,
            trajectory_offsets: &workspace.trajectory_offsets,
            trajectory_pause_seconds: &workspace.trajectory.pause_seconds,
            trajectory_ready_seconds: &workspace.trajectory.ready_seconds,
            trajectory_during_supply: &workspace.trajectory.during_supply,
            trajectory_after_supply: &workspace.trajectory.after_supply,
            horizon_micros: forecast.planning_horizon_micros,
            arrival_path,
        },
        &mut workspace.edf,
        &mut cells.shortfall[..shared.action_count],
        &mut cells.drain_seconds[..shared.action_count],
        &mut cells.missed_work[..shared.action_count],
    );
    normalize_scenario_outcomes(state, shared, &mut cells);
    for candidate_index in 0..shared.action_count {
        let first = workspace.trajectory_offsets[candidate_index] as usize;
        let last = workspace.trajectory_offsets[candidate_index + 1] as usize;
        cells.replica_seconds[candidate_index] = replica_seconds(
            start_seconds,
            planning_horizon_seconds,
            state.current_replicas,
            &workspace.trajectory.targets[first..last],
            &workspace.trajectory.ready_seconds[first..last],
        ) + terminal_replica_seconds(
            forecast.planning_horizon_micros,
            cells.drain_seconds[candidate_index],
            state.configuration.report_interval_micros,
            workspace.trajectory.targets[first..last]
                .last()
                .copied()
                .unwrap_or(state.current_replicas),
        );
    }
}

/// Folds scenario cells into decision columns.
fn finalize_scenario_columns(state: &ScaleState, scratch: &mut ScaleScratch) {
    dispatch!(state.simd_level, simd => aggregate_scenario_values(simd, scratch));
    scratch.decision_curve_sample_count =
        u32::try_from(scratch.active_scenario_count).map_or(u32::MAX, |count| count);
}

/// Returns the planning and disturbance horizons for one scenario.
///
/// The horizon covers the candidate transition and one reactive repair,
/// every known deadline, and one budget past the last boundary. It does
/// not depend on the candidate, so every action is judged over the same
/// future.
fn scenario_horizons(state: &ScaleState, cohorts: &WorkCohorts) -> (u64, u64) {
    let transition_span_seconds = state
        .configuration
        .launch_time_prior
        .coverage_support_seconds()
        .1
        + state
            .configuration
            .rebalance_time_prior
            .coverage_support_seconds()
            .1;
    let report_horizon_micros = state
        .model_time
        .as_micros()
        .saturating_add(state.configuration.report_interval_micros);
    let response_horizon_micros =
        report_horizon_micros.saturating_add(seconds_to_micros(2.0_f64 * transition_span_seconds));
    let planning_horizon_micros = complete_horizon_micros(
        report_horizon_micros,
        response_horizon_micros,
        cohorts.deadline_max_micros(),
        state.configuration.objective.budget_micros(),
    );
    let disturbance_horizon_micros =
        planning_horizon_micros.saturating_sub(state.configuration.objective.budget_micros());
    (planning_horizon_micros, disturbance_horizon_micros)
}

fn sample_scenario_path(
    state: &ScaleState,
    calendar: Option<CalendarForecast<'_>>,
    sample: u32,
    disturbance_horizon_micros: u64,
    end_seconds: &mut [f64],
    rates: &mut [f64],
) -> usize {
    let start_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    let disturbance_horizon_seconds =
        Duration::from_micros(disturbance_horizon_micros).as_secs_f64();
    let mut arrival_random = decision_random(sample, DecisionRandomDomain::Arrival);
    state.arrivals.sample_rate_path(
        disturbance_horizon_seconds - start_seconds,
        &mut arrival_random,
        end_seconds,
        rates,
        calendar,
        state.model_time.as_micros(),
    )
}

fn aggregate_scenario_values<S: Simd>(simd: S, scratch: &mut ScaleScratch) {
    let candidate_count = decision_action_count(scratch);
    let candidate_stride = scratch.posterior_loss_sums.len();
    scratch.posterior_missed_work_sums.fill(0.0_f64);
    scratch.posterior_loss_sums.fill(0.0_f64);
    scratch.posterior_late_area_sums.fill(0.0_f64);
    scratch.posterior_replica_seconds_sums.fill(0.0_f64);
    scratch.posterior_supply_sums.fill(0.0_f64);
    for scenario in 0..scratch.active_scenario_count {
        let inner_count = u32::try_from(scratch.active_inner_count).map_or(u32::MAX, |value| value);
        let cell_weight =
            scratch.class_masses[scenario / scratch.active_inner_count] / f64::from(inner_count);
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
                    ) * S::f64s::splat(simd, cell_weight);
            let loss = S::f64s::from_slice(simd, &scratch.posterior_loss_sums[target..last])
                + S::f64s::from_slice(simd, &scratch.scenario_shortfall[cell..cell + S::f64s::N])
                    * S::f64s::splat(simd, cell_weight);
            let late_area =
                S::f64s::from_slice(simd, &scratch.posterior_late_area_sums[target..last])
                    + S::f64s::from_slice(
                        simd,
                        &scratch.scenario_late_area[cell..cell + S::f64s::N],
                    ) * S::f64s::splat(simd, cell_weight);
            let replica_seconds =
                S::f64s::from_slice(simd, &scratch.posterior_replica_seconds_sums[target..last])
                    + S::f64s::from_slice(
                        simd,
                        &scratch.scenario_replica_seconds[cell..cell + S::f64s::N],
                    ) * S::f64s::splat(simd, cell_weight);
            let supply = S::f64s::from_slice(simd, &scratch.posterior_supply_sums[target..last])
                + S::f64s::from_slice(simd, &scratch.scenario_supply[cell..cell + S::f64s::N])
                    * S::f64s::splat(simd, cell_weight);
            missed.store_slice(&mut scratch.posterior_missed_work_sums[target..last]);
            loss.store_slice(&mut scratch.posterior_loss_sums[target..last]);
            late_area.store_slice(&mut scratch.posterior_late_area_sums[target..last]);
            replica_seconds.store_slice(&mut scratch.posterior_replica_seconds_sums[target..last]);
            supply.store_slice(&mut scratch.posterior_supply_sums[target..last]);
        }
        for target in vector_count * S::f64s::N..candidate_count {
            let cell = first + target;
            scratch.posterior_missed_work_sums[target] +=
                cell_weight * scratch.scenario_missed_work[cell];
            scratch.posterior_loss_sums[target] += cell_weight * scratch.scenario_shortfall[cell];
            scratch.posterior_late_area_sums[target] +=
                cell_weight * scratch.scenario_late_area[cell];
            scratch.posterior_replica_seconds_sums[target] +=
                cell_weight * scratch.scenario_replica_seconds[cell];
            scratch.posterior_supply_sums[target] += cell_weight * scratch.scenario_supply[cell];
        }
    }
}

/// Selects the paired-sample minimum expected cost.
///
/// All actions use the same configured posterior scenarios. A smaller target
/// wins only when represented means are exactly equal.
fn numerical_decision(state: &ScaleState, scratch: &mut ScaleScratch) -> usize {
    let candidate_count = decision_action_count(scratch);
    let rate = state.configuration.objective.replica_second_delay_rate();
    scratch.decision_rate = rate;
    select_action(&ActionColumns {
        late_area_sums: &scratch.posterior_late_area_sums[..candidate_count],
        replica_seconds_sums: &scratch.posterior_replica_seconds_sums[..candidate_count],
        rate,
    })
}

fn scheduled_event_count(cohorts: &WorkCohorts, horizon_micros: u64) -> f64 {
    (0..cohorts.len())
        .filter(|&cohort| {
            cohorts.partition(cohort) == SCHEDULED_PARTITION
                && cohorts.release_micros(cohort) <= horizon_micros
        })
        .map(|cohort| cohorts.work_slot_seconds(cohort))
        .sum()
}

fn scenario_event_count(
    normal_events: f64,
    failure_events: f64,
    arrival_path: &ArrivalPath<'_>,
    start_seconds: f64,
    disturbance_horizon_seconds: f64,
    cohorts: &WorkCohorts,
    disturbance_horizon_micros: u64,
) -> f64 {
    normal_events
        + failure_events
        + arrival_path.integrated_count(start_seconds, disturbance_horizon_seconds)
        + scheduled_event_count(cohorts, disturbance_horizon_micros)
}

/// Converts one scenario's raw outcomes into normalized decision cells.
///
/// The shortfall cell becomes excess delay for each served event budget.
/// The missed cell keeps event units and adds the deterministic placement
/// floor, so a candidate that cannot own the located work carries its
/// unservable events in every scenario.
fn normalize_scenario_outcomes(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    cells: &mut ScenarioCells<'_>,
) {
    let budget_seconds =
        Duration::from_micros(state.configuration.objective.budget_micros()).as_secs_f64();
    // Retry demand remains message-evidence-only. This is conservative for
    // future scheduled work, whose outcomes do not exist yet.
    for candidate in 0..shared.action_count {
        let late_area = cells.shortfall[candidate];
        let denominator = *cells.event_count * budget_seconds;
        cells.late_area[candidate] = late_area.max(*cells.partition_late_area);
        cells.shortfall[candidate] = if denominator > f64::EPSILON {
            cells.late_area[candidate] / denominator
        } else {
            0.0_f64
        };
        let missed_work = cells.missed_work[candidate].max(*cells.partition_missed_work);
        cells.missed_work[candidate] = missed_work;
        let mut rejection = 0_u8;
        let miss_fraction = missed_work / cells.event_count.max(f64::MIN_POSITIVE);
        // This SLO constraint is epsilon's only consumer.
        if miss_fraction > state.configuration.objective.epsilon() {
            rejection |= DecisionRejection::Deadline.bit();
        }
        if *cells.partition_missed_work > f64::EPSILON {
            rejection |= DecisionRejection::PartitionPlacement.bit();
        }
        cells.rejection[candidate] = rejection;
    }
}

fn evaluate_candidates(
    shared: &CandidateEvaluation<'_>,
    edf: &mut EdfScratch,
    shortfall: &mut [f64],
    drain_seconds: &mut [f64],
    missed_work: &mut [f64],
) {
    for candidate in 0..shortfall.len() {
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
            EvaluationWindow {
                start_micros: shared.model_time_micros,
                horizon_micros: shared.horizon_micros,
                initial_debt_work: 0.0_f64,
                deadline_budget_micros: shared.deadline_budget_micros,
            },
            &shared.arrival_path,
            edf,
        );
        shortfall[candidate] = outcome.late_area + outcome.terminal_late_area;
        drain_seconds[candidate] = outcome.drain_seconds;
        missed_work[candidate] = outcome.missed_work;
    }
}

fn finish_decision(
    state: &ScaleState,
    scratch: &ScaleScratch,
    target_index: usize,
) -> ScaleDecision {
    let target = target_index as u32 + 1;
    let selected = target_index;
    let expected_loss = scratch.posterior_loss_sums[selected];
    let saturation_probability = state
        .capacity
        .saturation_probability(state.simd_level, scratch.candidate_concurrency[selected]);
    ScaleDecision::Apply(ApplyDecision {
        target,
        cap: state
            .configuration
            .replica_count_max
            .min(state.configuration.partition_count),
        diagnostics: diagnostics(
            state,
            expected_loss,
            saturation_probability,
            Some(target),
            scratch.decision_curve_sample_count,
        ),
    })
}

/// One scenario's realized draws that the outcome evaluation reads.
struct ScenarioForecast {
    current_supply: f64,
    event_supply_factor: f64,
    service_time_seconds: f64,
    path_length: usize,
    planning_horizon_micros: u64,
    disturbance_horizon_micros: u64,
}

/// One scenario's sampled transition and disturbance context.
struct ScenarioDraws<'a> {
    current_supply: f64,
    lead_random: RandomStream,
    rebalance_random: RandomStream,
    commitment_random: RandomStream,
    arrival_path_end_seconds: &'a [f64],
    arrival_path_rates: &'a [f64],
}

fn prepare_supply_trajectories(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspace: &mut ScenarioWorkspace,
    draws: &ScenarioDraws<'_>,
) {
    let current_supply = draws.current_supply;
    let candidate_count = workspace.posterior_resource_supply.len();
    let now_seconds = sample_commitment_pauses(
        state,
        workspace,
        shared.actuation_commitments,
        &draws.commitment_random,
    );
    workspace.trajectory.targets.clear();
    workspace.trajectory.pause_seconds.clear();
    workspace.trajectory.ready_overrides.clear();
    workspace.trajectory.ready_seconds.clear();
    workspace.trajectory.during_supply.clear();
    workspace.trajectory.after_supply.clear();
    workspace.trajectory_offsets[0] = 0;
    for candidate_index in 0..candidate_count {
        let candidate = candidate_index as u32 + 1;
        let (first, fixed_event_count, committed_replicas) = push_candidate_events(
            state,
            workspace,
            draws,
            shared.actuation_commitments,
            candidate,
            now_seconds,
        );
        sort_trajectory_events(&mut workspace.trajectory, first + fixed_event_count);
        let mut write = first;
        let mut replicas = state.current_replicas;
        let mut active_ready = now_seconds;
        let mut active_during_supply = current_supply;
        let mut active_after_supply = current_supply;
        let mut active_event = None;
        for read in first..workspace.trajectory.targets.len() {
            let target = workspace.trajectory.targets[read];
            if read >= first + fixed_event_count
                && ((candidate > committed_replicas && target <= replicas)
                    || (candidate < committed_replicas && target >= replicas))
            {
                continue;
            }
            let pause = workspace.trajectory.pause_seconds[read].max(now_seconds);
            let before_supply = if pause < active_ready {
                if let Some(event) = active_event {
                    workspace.trajectory.ready_seconds[event] = pause;
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
            let replica_delta = target.abs_diff(replicas);
            let ready_override = workspace.trajectory.ready_overrides[read];
            let sampled_ready = sampled_membership_ready(
                state,
                draws,
                direction,
                replica_delta,
                pause,
                ready_override,
            );
            let moved = shared.moved_partition_counts
                [(replicas as usize - 1) * candidate_count + target as usize - 1];
            let ready = membership_ready(direction, moved, pause, sampled_ready);
            let retained = 1.0_f64 - workspace.moved_partition_share[moved as usize];
            workspace.trajectory.targets[write] = target;
            workspace.trajectory.pause_seconds[write] = pause;
            workspace.trajectory.ready_overrides[write] = ready_override;
            workspace.trajectory.ready_seconds[write] = ready;
            workspace.trajectory.during_supply[write] = before_supply * retained;
            workspace.trajectory.after_supply[write] =
                workspace.posterior_resource_supply[target as usize - 1];
            active_ready = ready;
            active_during_supply = workspace.trajectory.during_supply[write];
            active_after_supply = workspace.trajectory.after_supply[write];
            active_event = Some(write);
            write += 1;
            replicas = target;
        }
        workspace.trajectory.targets.truncate(write);
        workspace.trajectory.pause_seconds.truncate(write);
        workspace.trajectory.ready_overrides.truncate(write);
        workspace.trajectory.ready_seconds.truncate(write);
        workspace.trajectory.during_supply.truncate(write);
        workspace.trajectory.after_supply.truncate(write);
        append_reactive_repairs(
            state,
            shared,
            workspace,
            draws,
            replicas,
            active_after_supply,
            active_ready,
        );
        workspace.trajectory_offsets[candidate_index + 1] =
            workspace.trajectory.targets.len() as u32;
    }
}

const fn membership_ready(
    direction: TransitionDirection,
    moved: u32,
    pause: f64,
    sampled_ready: f64,
) -> f64 {
    if matches!(direction, TransitionDirection::Down) && moved == 0 {
        pause
    } else {
        sampled_ready
    }
}

/// Pushes one candidate's committed and requested transition events.
///
/// Returns the candidate's first event index, its fixed-event count, and
/// the replica count the started rebalance commits.
fn push_candidate_events(
    state: &ScaleState,
    workspace: &mut ScenarioWorkspace,
    draws: &ScenarioDraws<'_>,
    actuation_commitments: &ActuationCommitments,
    candidate: u32,
    now_seconds: f64,
) -> (usize, usize, u32) {
    let first = workspace.trajectory.targets.len();
    let rebalancing = actuation_commitments.rebalancing();
    let committed_replicas = rebalancing.map_or(state.current_replicas, |commitment| {
        push_trajectory_event(
            &mut workspace.trajectory,
            commitment.target_replicas,
            now_seconds,
            workspace.rebalancing_ready_seconds,
        );
        commitment.target_replicas
    });
    let fixed_event_count = workspace.trajectory.targets.len() - first;
    if candidate != committed_replicas {
        let candidate_direction = if candidate > committed_replicas {
            TransitionDirection::Up
        } else {
            TransitionDirection::Down
        };
        for commitment_index in 0..actuation_commitments.launching_len() {
            if actuation_commitments.launching_direction(commitment_index) != candidate_direction
                || !workspace.commitment_pause_seconds[commitment_index].is_finite()
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
                || workspace.trajectory.targets[first..].contains(&target)
            {
                continue;
            }
            push_trajectory_event(
                &mut workspace.trajectory,
                target,
                workspace.commitment_pause_seconds[commitment_index],
                f64::NAN,
            );
        }
    }
    if candidate != committed_replicas
        && !workspace.trajectory.targets[first..].contains(&candidate)
    {
        let direction = if candidate > committed_replicas {
            TransitionDirection::Up
        } else {
            TransitionDirection::Down
        };
        let replica_delta = candidate.abs_diff(committed_replicas);
        push_trajectory_event(
            &mut workspace.trajectory,
            candidate,
            now_seconds
                + scenario_launch_seconds(state, &draws.lead_random, direction, replica_delta),
            f64::NAN,
        );
    }
    (first, fixed_event_count, committed_replicas)
}

/// Appends the reactive corrections one deterministic successor makes.
///
/// The successor requests the smallest target that covers the realized rate.
///
/// This rule is an optimistic approximation. It observes the sampled plant
/// truth, but the real controller observes a posterior. The approximation can
/// underprice low and high initial targets. Paired reports measure this bias.
fn append_reactive_repairs(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspace: &mut ScenarioWorkspace,
    draws: &ScenarioDraws<'_>,
    mut replicas: u32,
    mut supply: f64,
    mut ready: f64,
) {
    let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    let action_count = shared.action_count;
    let candidate_count = workspace.posterior_resource_supply.len();
    let report_seconds =
        Duration::from_micros(state.configuration.report_interval_micros).as_secs_f64();
    let mut segment_start = 0.0_f64;
    for segment in 0..draws.arrival_path_end_seconds.len() {
        let segment_end = draws.arrival_path_end_seconds[segment];
        let begin_seconds = now_seconds + segment_start;
        let end_seconds = now_seconds + segment_end;
        segment_start = segment_end;
        let rate = draws.arrival_path_rates[segment];
        // One transition runs at a time: the successor acts once the
        // active transition completes and the report shows the shortage.
        let observed = begin_seconds.max(ready);
        if observed >= end_seconds {
            continue;
        }
        let intervals = ((observed - now_seconds) / report_seconds)
            .ceil()
            .max(1.0_f64);
        let requested = now_seconds + intervals * report_seconds;
        let target = repair_target(&workspace.posterior_resource_supply[..action_count], rate);
        if target == replicas {
            continue;
        }
        let direction = if target > replicas {
            TransitionDirection::Up
        } else {
            TransitionDirection::Down
        };
        let replica_delta = target.abs_diff(replicas);
        let pause = requested.max(ready)
            + scenario_launch_seconds(state, &draws.lead_random, direction, replica_delta);
        let moved = shared.moved_partition_counts
            [(replicas as usize - 1) * candidate_count + target as usize - 1];
        let repair_ready = if direction == TransitionDirection::Down && moved == 0 {
            pause
        } else {
            pause
                + scenario_rebalance_seconds(
                    state,
                    &draws.rebalance_random,
                    direction,
                    replica_delta,
                )
        };
        let retained = 1.0_f64 - workspace.moved_partition_share[moved as usize];
        let after = workspace.posterior_resource_supply[target as usize - 1];
        workspace.trajectory.targets.push(target);
        workspace.trajectory.pause_seconds.push(pause);
        workspace.trajectory.ready_overrides.push(f64::NAN);
        workspace.trajectory.ready_seconds.push(repair_ready);
        workspace.trajectory.during_supply.push(supply * retained);
        workspace.trajectory.after_supply.push(after);
        replicas = target;
        supply = after;
        ready = repair_ready;
    }
}

/// Returns the smallest replica target whose supply covers one rate.
///
/// The supply column is non-decreasing. When no target covers the rate,
/// the largest target is the best repair.
///
/// This target is the reactive-policy fixed point. The live policy uses the
/// posterior-mean rate from its next report. The controller keeps this demand
/// floor by design. Sampled future rates affect cost, but they do not restrict
/// the initial action set.
fn repair_target(supply: &[f64], rate: f64) -> u32 {
    let index = supply.partition_point(|value| *value < rate);
    index.min(supply.len() - 1) as u32 + 1
}

fn sample_commitment_pauses(
    state: &ScaleState,
    workspace: &mut ScenarioWorkspace,
    commitments: &ActuationCommitments,
    random: &RandomStream,
) -> f64 {
    let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    for index in 0..commitments.launching_len() {
        if commitments.launching_requested_at(index) > state.model_time {
            workspace.commitment_pause_seconds[index] = f64::INFINITY;
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
        workspace.commitment_pause_seconds[index] = now_seconds + remaining_seconds;
    }
    workspace.rebalancing_ready_seconds =
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
                let domain = commitment.requested_at.as_micros()
                    ^ commitment.started_at.as_micros().rotate_left(17)
                    ^ u64::from(commitment.target_replicas).rotate_left(34);
                let mut rebalance_random = random.clone().domain(domain);
                now_seconds
                    + state
                        .rebalance_time
                        .sample_remaining_seconds(elapsed_seconds, &mut rebalance_random)
            });
    now_seconds
}

pub(crate) fn decision_random(scenario: u32, domain: DecisionRandomDomain) -> RandomStream {
    RandomStream::new(DECISION_SCENARIO_SEED)
        .domain(u64::from(scenario))
        .domain(domain as u64)
}

fn scenario_launch_seconds(
    state: &ScaleState,
    random: &RandomStream,
    direction: TransitionDirection,
    replica_delta: u32,
) -> f64 {
    let direction_domain = match direction {
        TransitionDirection::Up => 0_u64,
        TransitionDirection::Down => 1_u64 << 32_u32,
    };
    let mut random = random
        .clone()
        .domain(direction_domain | u64::from(replica_delta));
    state
        .lead_time
        .sample_seconds(direction, replica_delta, &mut random)
}

fn scenario_rebalance_seconds(
    state: &ScaleState,
    random: &RandomStream,
    direction: TransitionDirection,
    replica_delta: u32,
) -> f64 {
    let direction_domain = match direction {
        TransitionDirection::Up => 0_u64,
        TransitionDirection::Down => 1_u64 << 32_u32,
    };
    let mut random = random
        .clone()
        .domain(direction_domain | u64::from(replica_delta));
    state.rebalance_time.sample_seconds(&mut random)
}

fn push_trajectory_event(
    trajectory: &mut TrajectoryColumns,
    target: u32,
    pause_seconds: f64,
    ready_override: f64,
) {
    trajectory.targets.push(target);
    trajectory.pause_seconds.push(pause_seconds);
    trajectory.ready_overrides.push(ready_override);
    trajectory.ready_seconds.push(0.0_f64);
    trajectory.during_supply.push(0.0_f64);
    trajectory.after_supply.push(0.0_f64);
}

fn sampled_membership_ready(
    state: &ScaleState,
    draws: &ScenarioDraws<'_>,
    direction: TransitionDirection,
    replica_delta: u32,
    pause: f64,
    ready_override: f64,
) -> f64 {
    if ready_override.is_finite() {
        ready_override.max(pause)
    } else {
        pause + scenario_rebalance_seconds(state, &draws.rebalance_random, direction, replica_delta)
    }
}

fn sort_trajectory_events(trajectory: &mut TrajectoryColumns, first: usize) {
    for mut event in first + 1..trajectory.targets.len() {
        while event > first && trajectory.pause_seconds[event] < trajectory.pause_seconds[event - 1]
        {
            trajectory.targets.swap(event, event - 1);
            trajectory.pause_seconds.swap(event, event - 1);
            trajectory.ready_overrides.swap(event, event - 1);
            trajectory.ready_seconds.swap(event, event - 1);
            event -= 1;
        }
    }
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}

pub(crate) fn minimal_moved_partitions(partitions: u32, current: u32, target: u32) -> u32 {
    assert!(
        partitions > 0 && current > 0 && target > 0,
        "partition and replica counts are one-based"
    );
    let current = current.min(partitions);
    let target = target.min(partitions);
    let common = current.min(target);
    let current_base = partitions / current;
    let target_base = partitions / target;
    let current_extra = partitions % current;
    let target_extra = partitions % target;
    let overlap = match current_base.cmp(&target_base) {
        Ordering::Less => common * current_base + common.min(current_extra),
        Ordering::Greater => common * target_base + common.min(target_extra),
        Ordering::Equal => common * current_base + common.min(current_extra).min(target_extra),
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
    cohorts: &CohortColumns,
    backlog: &BacklogColumns,
    scheduled_releases: &[ScheduledRelease],
) {
    scratch.resource_cohorts.clear();
    for cohort in 0..cohorts.len() {
        let release_micros = cohorts.release_micros(cohort);
        let deadline_micros = cohorts.deadline_micros(cohort);
        let offered_events = cohorts.offered_events(cohort);
        let partition = cohorts.partition(cohort);
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
        let offered_events = f64::from(backlog.event_count(backlog_index));
        let partition = backlog_index as u32 / DemandClass::COUNT;
        scratch.resource_cohorts.push_values(
            release_micros,
            deadline_micros,
            offered_events,
            partition,
        );
    }
    for release in scheduled_releases
        .iter()
        .filter(|release| release.release_micros > state.model_time.as_micros())
    {
        let deadline_micros = release
            .release_micros
            .saturating_add(state.configuration.objective.budget_micros());
        let offered_events = f64::from(release.count);
        scratch.resource_cohorts.push_values(
            release.release_micros,
            deadline_micros,
            offered_events,
            SCHEDULED_PARTITION,
        );
    }
}

fn prepare_partition_work(_state: &ScaleState, scratch: &mut ScaleScratch) {
    let cohorts = &scratch.resource_cohorts;
    scratch.partition_offsets.fill(0);
    for cohort in 0..cohorts.len() {
        if cohorts.partition(cohort) == SCHEDULED_PARTITION {
            continue;
        }
        let partition = cohorts.partition(cohort) as usize;
        scratch.partition_offsets[partition + 1] += 1;
    }
    for partition in 0..scratch.partition_write_offsets.len() {
        scratch.partition_offsets[partition + 1] += scratch.partition_offsets[partition];
        scratch.partition_write_offsets[partition] = scratch.partition_offsets[partition];
    }
    for cohort_index in 0..cohorts.len() {
        if cohorts.partition(cohort_index) == SCHEDULED_PARTITION {
            continue;
        }
        let partition = cohorts.partition(cohort_index) as usize;
        let write_offset = scratch.partition_write_offsets[partition] as usize;
        scratch.partition_cohort_indexes[write_offset] = cohort_index as u32;
        scratch.partition_write_offsets[partition] += 1;
    }
}

fn partition_deadline_outcome(
    state: &ScaleState,
    shared: &ScenarioShared<'_>,
    workspace: &mut ScenarioWorkspace,
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
    for partition in 0..shared.partition_count {
        let first = shared.partition_offsets[partition] as usize;
        let last = shared.partition_offsets[partition + 1] as usize;
        if first == last {
            continue;
        }
        workspace.placement_cohorts.clear();
        for &cohort_index in &shared.partition_cohort_indexes[first..last] {
            let cohort = cohort_index as usize;
            if shared.resource_cohorts.partition(cohort) == SCHEDULED_PARTITION {
                continue;
            }
            workspace.placement_cohorts.push_values(
                shared.resource_cohorts.release_micros(cohort),
                shared.resource_cohorts.deadline_micros(cohort),
                shared.resource_cohorts.work_slot_seconds(cohort) * service_time_seconds,
                shared.resource_cohorts.partition(cohort),
            );
        }
        prepare(&workspace.placement_cohorts, &mut workspace.placement_edf);
        let outcome = evaluate_prepared_step(
            &workspace.placement_cohorts,
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
            &mut workspace.placement_edf,
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
        let active_replicas = candidate.min(state.configuration.partition_count);
        *concurrency =
            f64::from(active_replicas.saturating_mul(state.configuration.slots_per_replica));
    }
}

/// Returns the complete physical action domain.
///
/// Kafka assigns one partition to at most one consumer. A larger target adds
/// no service. Prior construction must reject material endpoint mass.
fn decision_action_count(scratch: &ScaleScratch) -> usize {
    scratch
        .partition_write_offsets
        .len()
        .min(scratch.posterior_loss_sums.len())
}

fn demand_class_totals(cohorts: &CohortColumns, backlog: &BacklogColumns) -> (f64, f64) {
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
        || state.lead_time.expected_last_seconds() + state.rebalance_time.expected_seconds(),
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
                + state.rebalance_time.expected_seconds()
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
            + state.rebalance_time.expected_seconds(),
        lead_time_down_seconds: state
            .lead_time
            .expected_seconds(TransitionDirection::Down, 1)
            + state.rebalance_time.expected_seconds(),
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

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;
