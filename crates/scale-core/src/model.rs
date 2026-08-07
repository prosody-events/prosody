use std::time::Duration;

use fearless_simd::Level;

use crate::TransitionDirection;
use crate::arrival::ArrivalFactor;
use crate::capacity::{CapacityFactor, ThroughputPosteriorCell};
use crate::edf::{
    ArrivalPath, CandidateLoss, CandidateSupply, EdfScratch, SupplyTrajectories, SupplyTrajectory,
    evaluate_prepared_step, evaluate_prepared_trajectory, has_common_release, prepare,
    required_capacity_prepared, shortfall_prepared_common_release_candidates,
    shortfall_prepared_common_release_trajectories,
};
use crate::lead_time::{LeadTimeFactor, sample_index};
use crate::partition::PartitionFactor;
use crate::reliability::{RELIABILITY_BIN_COUNT, ReliabilityFactor};
use crate::types::{CalendarForecast, WorkCohort};
use crate::{
    ActuationCommitment, ApplyDecision, ArrivalPosterior, CapacityGrid, Configuration,
    ConfigurationError, DecisionDiagnostics, GroupObservation, HoldDecision, HoldReason, ModelTime,
    PosteriorError, PosteriorQuery, RandomStream, ScaleDecision,
};
use thiserror::Error;

const DECISION_SCENARIO_SEED: u64 = 0x7363_616c_652d_636f;

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
    resource_edf: EdfScratch,
    placement_edf: EdfScratch,
    handler_cohorts: Vec<WorkCohort>,
    resource_cohorts: Vec<WorkCohort>,
    placement_cohorts: Vec<WorkCohort>,
    partition_offsets: Vec<u32>,
    partition_write_offsets: Vec<u32>,
    partition_cohort_indexes: Vec<u32>,
    partition_work_slot_seconds: Vec<f64>,
    partition_order: Vec<u32>,
    partition_share_draws: Vec<f64>,
    moved_partition_share: Vec<f64>,
    active_partition_count: u32,
    placement_interval_seconds: f64,
    partition_shortfall: f64,
    posterior_pass_counts: Vec<f64>,
    posterior_loss_sums: Vec<f64>,
    candidate_concurrency: Vec<f64>,
    posterior_resource_supply: Vec<f64>,
    posterior_rebalance_supply: Vec<f64>,
    posterior_pause_seconds: Vec<f64>,
    posterior_ready_seconds: Vec<f64>,
    posterior_future_rate: Vec<f64>,
    posterior_service_credit: Vec<f64>,
    posterior_interval_supply: Vec<f64>,
    posterior_sample_loss: Vec<f64>,
    posterior_delay_area: Vec<f64>,
    deterministic_loss: Vec<f64>,
    trajectory_offsets: Vec<u32>,
    trajectory_targets: Vec<u32>,
    trajectory_pause_seconds: Vec<f64>,
    trajectory_ready_seconds: Vec<f64>,
    trajectory_during_supply: Vec<f64>,
    trajectory_after_supply: Vec<f64>,
    commitment_pause_seconds: Vec<f64>,
    arrival_path_end_seconds: Vec<f64>,
    arrival_path_rates: Vec<f64>,
    resource_debt_events: f64,
    decision_curve_sample_count: u32,
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
        let cohort_count_max = usize::try_from(configuration.cohort_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_count = usize::try_from(configuration.partition_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let replica_count_max = usize::try_from(configuration.replica_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let backlog_count_max = partition_count
            .checked_mul(crate::DemandClass::COUNT_USIZE)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let work_cohort_count_max = cohort_count_max
            .checked_add(backlog_count_max)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let work_cohort_count_max_u32 =
            u32::try_from(work_cohort_count_max).map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_offset_count = partition_count
            .checked_add(1)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let trajectory_event_count_max = replica_count_max
            .checked_mul(
                replica_count_max
                    .checked_add(1)
                    .ok_or(ConfigurationError::PlatformLimit)?,
            )
            .ok_or(ConfigurationError::PlatformLimit)?;
        let candidate_concurrency = (1..=configuration.replica_count_max)
            .map(|replicas| f64::from(replicas) * f64::from(configuration.slots_per_replica))
            .collect::<Vec<_>>();
        Ok(Self {
            resource_edf: EdfScratch::new(work_cohort_count_max_u32)?,
            placement_edf: EdfScratch::new(work_cohort_count_max_u32)?,
            handler_cohorts: Vec::with_capacity(work_cohort_count_max),
            resource_cohorts: Vec::with_capacity(work_cohort_count_max),
            placement_cohorts: Vec::with_capacity(work_cohort_count_max),
            partition_offsets: vec![0; partition_offset_count],
            partition_write_offsets: vec![0; partition_count],
            partition_cohort_indexes: vec![0; work_cohort_count_max],
            partition_work_slot_seconds: vec![0.0_f64; partition_count],
            partition_order: vec![0; partition_count],
            partition_share_draws: vec![0.0_f64; partition_count],
            moved_partition_share: vec![0.0_f64; partition_offset_count],
            active_partition_count: 0,
            placement_interval_seconds: 0.0_f64,
            partition_shortfall: 0.0_f64,
            posterior_pass_counts: vec![0.0_f64; replica_count_max],
            posterior_loss_sums: vec![0.0_f64; replica_count_max],
            candidate_concurrency,
            posterior_resource_supply: vec![0.0_f64; replica_count_max],
            posterior_rebalance_supply: vec![0.0_f64; replica_count_max],
            posterior_pause_seconds: vec![0.0_f64; replica_count_max],
            posterior_ready_seconds: vec![0.0_f64; replica_count_max],
            posterior_future_rate: vec![0.0_f64; replica_count_max],
            posterior_service_credit: vec![0.0_f64; replica_count_max],
            posterior_interval_supply: vec![0.0_f64; replica_count_max],
            posterior_sample_loss: vec![0.0_f64; replica_count_max],
            posterior_delay_area: vec![0.0_f64; replica_count_max],
            deterministic_loss: vec![0.0_f64; replica_count_max],
            trajectory_offsets: vec![0; replica_count_max + 1],
            trajectory_targets: Vec::with_capacity(trajectory_event_count_max),
            trajectory_pause_seconds: Vec::with_capacity(trajectory_event_count_max),
            trajectory_ready_seconds: Vec::with_capacity(trajectory_event_count_max),
            trajectory_during_supply: Vec::with_capacity(trajectory_event_count_max),
            trajectory_after_supply: Vec::with_capacity(trajectory_event_count_max),
            commitment_pause_seconds: vec![0.0_f64; replica_count_max],
            arrival_path_end_seconds: vec![
                0.0_f64;
                configuration.arrival_prior.path_segment_count_max()
            ],
            arrival_path_rates: vec![0.0_f64; configuration.arrival_prior.path_segment_count_max()],
            resource_debt_events: 0.0_f64,
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

    #[cfg(test)]
    pub(crate) fn trajectory_targets(&self, candidate: u32) -> Option<&[u32]> {
        let index = usize::try_from(candidate.checked_sub(1)?).ok()?;
        let first = *self.trajectory_offsets.get(index)? as usize;
        let last = *self.trajectory_offsets.get(index + 1)? as usize;
        self.trajectory_targets.get(first..last)
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
    let elapsed_seconds =
        Duration::from_micros(now.as_micros().saturating_sub(state.model_time.as_micros()))
            .as_secs_f64();
    state.model_time = now;
    state.capacity.transition(elapsed_seconds);
    state.lead_time.transition(elapsed_seconds);
    state.rebalance_time.transition(elapsed_seconds);
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
    cohorts: &[crate::Cohort],
    backlog: &[Option<crate::BacklogCohort>],
    calendar: Option<CalendarForecast<'_>>,
    actuation_commitments: &[ActuationCommitment],
) -> ScaleDecision {
    let (normal_events, failure_events) = demand_class_totals(cohorts, backlog);
    prepare_work_cohorts(state, scratch, cohorts, backlog);
    prepare_partition_work(state, scratch);
    if state.arrivals.expected_rate(state.model_time.as_micros()) > f64::EPSILON {
        scratch.active_partition_count = scratch
            .active_partition_count
            .max(state.configuration.partition_count);
    }
    prepare_candidate_concurrency(state, scratch);
    prepare(&scratch.resource_cohorts, &mut scratch.resource_edf);
    scratch.posterior_pass_counts.fill(0.0_f64);
    scratch.posterior_loss_sums.fill(0.0_f64);
    for candidate_index in 0..scratch.deterministic_loss.len() {
        let candidate = candidate_index as u32 + 1;
        scratch.deterministic_loss[candidate_index] =
            placement_shortfall(state, scratch, candidate);
    }
    let sample_count = f64::from(state.configuration.posterior_sample_count);
    let mut random = RandomStream::new(DECISION_SCENARIO_SEED);
    let sample_offset = random.open_unit_f64();
    let current_index = state.current_replicas as usize - 1;
    let current_concurrency = scratch.candidate_concurrency[current_index];
    let common_release = has_common_release(&scratch.resource_cohorts);
    for sample in 0..state.configuration.posterior_sample_count {
        let probability = (f64::from(sample) + sample_offset) / sample_count;
        let curve = state.capacity.curve_at_probability(probability);
        CapacityFactor::fill_throughput(
            state.simd_level,
            curve,
            &scratch.candidate_concurrency,
            &mut scratch.posterior_resource_supply,
        );
        let (normal_retry, failure_retry) =
            state.reliability.sample_retry_probabilities(&mut random);
        for supply in &mut scratch.posterior_resource_supply {
            *supply = mixed_event_supply(
                *supply,
                normal_retry,
                failure_retry,
                state.configuration.failure_service_weight,
                normal_events,
                failure_events,
            );
        }
        let current_supply = mixed_event_supply(
            curve.throughput(current_concurrency),
            normal_retry,
            failure_retry,
            state.configuration.failure_service_weight,
            normal_events,
            failure_events,
        );
        let lead_seconds = state.lead_time.sample_bucket_seconds(&mut random);
        let pause_seconds = state.rebalance_time.sample_bucket_seconds(&mut random);
        state.partition_placement.sample_moved_prefix(
            &mut random,
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
            &mut random,
        );
        let single_phase = prepare_single_phase_columns(scratch, current_supply, state.model_time);
        let deadline_max_micros = scratch
            .resource_cohorts
            .iter()
            .map(|cohort| cohort.deadline_micros)
            .max()
            .unwrap_or(state.model_time.as_micros());
        let ready_max_seconds = scratch.posterior_ready_seconds.iter().copied().fold(
            Duration::from_micros(deadline_max_micros).as_secs_f64(),
            f64::max,
        );
        let budget_seconds =
            Duration::from_micros(state.configuration.objective.budget_micros()).as_secs_f64();
        let horizon_seconds = ready_max_seconds + budget_seconds;
        let start_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
        let path_length = state.arrivals.sample_rate_path(
            horizon_seconds - start_seconds,
            &mut random,
            &mut scratch.arrival_path_end_seconds,
            &mut scratch.arrival_path_rates,
            calendar,
            state.model_time.as_micros(),
        );
        let arrival_path = ArrivalPath {
            start_seconds,
            end_seconds: &scratch.arrival_path_end_seconds[..path_length],
            rates: &scratch.arrival_path_rates[..path_length],
        };
        for candidate_index in 0..scratch.posterior_future_rate.len() {
            let ready = scratch.posterior_ready_seconds[candidate_index];
            scratch.posterior_future_rate[candidate_index] =
                arrival_path.integrated_count(ready, ready + budget_seconds) / budget_seconds;
        }
        let horizon_micros = seconds_to_micros(horizon_seconds);
        if common_release && single_phase {
            let supply = CandidateSupply {
                before: current_supply,
                during: &scratch.posterior_rebalance_supply,
                after: &scratch.posterior_resource_supply,
                pause_seconds: &scratch.posterior_pause_seconds,
                ready_seconds: &scratch.posterior_ready_seconds,
            };
            let mut results = CandidateLoss {
                service_balance: &mut scratch.posterior_service_credit,
                shortfall: &mut scratch.posterior_sample_loss,
                delay_area: &mut scratch.posterior_delay_area,
            };
            shortfall_prepared_common_release_candidates(
                state.simd_level,
                &scratch.resource_cohorts,
                &supply,
                scratch.resource_debt_events,
                &arrival_path,
                start_seconds,
                horizon_seconds,
                &mut results,
                &scratch.resource_edf,
            );
        } else if common_release {
            let trajectories = SupplyTrajectories {
                initial: current_supply,
                offsets: &scratch.trajectory_offsets,
                pause_seconds: &scratch.trajectory_pause_seconds,
                ready_seconds: &scratch.trajectory_ready_seconds,
                during: &scratch.trajectory_during_supply,
                after: &scratch.trajectory_after_supply,
            };
            let mut results = CandidateLoss {
                service_balance: &mut scratch.posterior_service_credit,
                shortfall: &mut scratch.posterior_sample_loss,
                delay_area: &mut scratch.posterior_delay_area,
            };
            shortfall_prepared_common_release_trajectories(
                state.simd_level,
                &scratch.resource_cohorts,
                &trajectories,
                scratch.resource_debt_events,
                &arrival_path,
                start_seconds,
                horizon_seconds,
                &mut results,
                &mut scratch.posterior_interval_supply,
                &scratch.resource_edf,
            );
        } else if single_phase {
            for candidate_index in 0..scratch.posterior_resource_supply.len() {
                let outcome = evaluate_prepared_step(
                    &scratch.resource_cohorts,
                    current_supply,
                    scratch.posterior_rebalance_supply[candidate_index],
                    scratch.posterior_resource_supply[candidate_index],
                    seconds_to_micros(scratch.posterior_pause_seconds[candidate_index]),
                    seconds_to_micros(scratch.posterior_ready_seconds[candidate_index]),
                    state.model_time.as_micros(),
                    horizon_micros,
                    scratch.resource_debt_events,
                    &arrival_path,
                    &mut scratch.resource_edf,
                );
                scratch.posterior_sample_loss[candidate_index] = outcome.shortfall;
                scratch.posterior_delay_area[candidate_index] = outcome.delay_area;
            }
        } else {
            for candidate_index in 0..scratch.posterior_resource_supply.len() {
                let first = scratch.trajectory_offsets[candidate_index] as usize;
                let last = scratch.trajectory_offsets[candidate_index + 1] as usize;
                let trajectory = SupplyTrajectory {
                    initial: current_supply,
                    pause_seconds: &scratch.trajectory_pause_seconds[first..last],
                    ready_seconds: &scratch.trajectory_ready_seconds[first..last],
                    during: &scratch.trajectory_during_supply[first..last],
                    after: &scratch.trajectory_after_supply[first..last],
                };
                let outcome = evaluate_prepared_trajectory(
                    &scratch.resource_cohorts,
                    &trajectory,
                    state.model_time.as_micros(),
                    horizon_micros,
                    scratch.resource_debt_events,
                    &arrival_path,
                    &mut scratch.resource_edf,
                );
                scratch.posterior_sample_loss[candidate_index] = outcome.shortfall;
                scratch.posterior_delay_area[candidate_index] = outcome.delay_area;
            }
        }
        for candidate_index in 0..scratch.posterior_resource_supply.len() {
            let future = fractional_shortfall(
                scratch.posterior_future_rate[candidate_index],
                scratch.posterior_resource_supply[candidate_index],
            );
            let resource = scratch.posterior_sample_loss[candidate_index].max(future);
            let event_count = normal_events
                + failure_events
                + arrival_path.integrated_count(start_seconds, horizon_seconds);
            let delay_denominator = event_count * budget_seconds;
            let delay = if delay_denominator > f64::EPSILON {
                scratch.posterior_delay_area[candidate_index] / delay_denominator
            } else {
                0.0_f64
            };
            let loss = delay;
            scratch.posterior_loss_sums[candidate_index] += loss;
            scratch.posterior_pass_counts[candidate_index] += f64::from(u8::from(
                resource <= f64::EPSILON
                    && scratch.deterministic_loss[candidate_index] <= f64::EPSILON,
            ));
        }
    }
    scratch.decision_curve_sample_count = state.configuration.posterior_sample_count;
    finish_decision(state, scratch, sample_count)
}

fn finish_decision(state: &ScaleState, scratch: &ScaleScratch, sample_count: f64) -> ScaleDecision {
    let required_probability = 1.0_f64 - state.configuration.objective.epsilon();
    let mut target_index = scratch
        .posterior_pass_counts
        .iter()
        .position(|&passes| passes / sample_count >= required_probability);
    if target_index.is_none() {
        target_index = scratch
            .posterior_loss_sums
            .iter()
            .enumerate()
            .min_by(|left, right| left.1.total_cmp(right.1))
            .map(|(index, _loss)| index);
    }
    let target = target_index.map_or(1, |index| index as u32 + 1);
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
        diagnostics: diagnostics(state, expected_loss, saturation_probability, Some(target)),
    })
}

fn prepare_supply_trajectories(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    current_supply: f64,
    lead_seconds: &[f64; 8],
    rebalance_seconds: &[f64; 8],
    actuation_commitments: &[ActuationCommitment],
    random: &mut RandomStream,
) {
    let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    scratch.trajectory_targets.clear();
    scratch.trajectory_pause_seconds.clear();
    scratch.trajectory_ready_seconds.clear();
    scratch.trajectory_during_supply.clear();
    scratch.trajectory_after_supply.clear();
    scratch.trajectory_offsets[0] = 0;
    for (commitment_index, commitment) in actuation_commitments.iter().enumerate() {
        if commitment.requested_at() > state.model_time {
            scratch.commitment_pause_seconds[commitment_index] = f64::INFINITY;
            continue;
        }
        let elapsed_seconds = Duration::from_micros(
            state
                .model_time
                .as_micros()
                .saturating_sub(commitment.requested_at().as_micros()),
        )
        .as_secs_f64();
        let remaining_seconds = state.lead_time.sample_remaining_seconds(
            commitment.direction(),
            commitment.replica_delta(),
            elapsed_seconds,
            random,
        );
        scratch.commitment_pause_seconds[commitment_index] = now_seconds + remaining_seconds;
    }
    for candidate_index in 0..scratch.posterior_resource_supply.len() {
        let candidate = candidate_index as u32 + 1;
        let first = scratch.trajectory_targets.len();
        if candidate != state.current_replicas {
            let candidate_direction = if candidate > state.current_replicas {
                TransitionDirection::Up
            } else {
                TransitionDirection::Down
            };
            for (commitment_index, commitment) in actuation_commitments.iter().enumerate() {
                if commitment.direction() != candidate_direction
                    || !scratch.commitment_pause_seconds[commitment_index].is_finite()
                {
                    continue;
                }
                let target = match candidate_direction {
                    TransitionDirection::Up => commitment.target_replicas().min(candidate),
                    TransitionDirection::Down => commitment.target_replicas().max(candidate),
                };
                if target == state.current_replicas
                    || scratch.trajectory_targets[first..].contains(&target)
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
            && !scratch.trajectory_targets[first..].contains(&candidate)
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
        for read in first..scratch.trajectory_targets.len() {
            let target = scratch.trajectory_targets[read];
            if (candidate > state.current_replicas && target <= replicas)
                || (candidate < state.current_replicas && target >= replicas)
            {
                continue;
            }
            let pause = scratch.trajectory_pause_seconds[read].max(ready_floor);
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
            let moved =
                minimal_moved_partitions(state.configuration.partition_count, replicas, target);
            let retained = 1.0_f64 - scratch.moved_partition_share[moved as usize];
            scratch.trajectory_targets[write] = target;
            scratch.trajectory_pause_seconds[write] = pause;
            scratch.trajectory_ready_seconds[write] = ready;
            scratch.trajectory_during_supply[write] = before_supply * retained;
            scratch.trajectory_after_supply[write] =
                scratch.posterior_resource_supply[target as usize - 1];
            write += 1;
            replicas = target;
            ready_floor = ready;
        }
        scratch.trajectory_targets.truncate(write);
        scratch.trajectory_pause_seconds.truncate(write);
        scratch.trajectory_ready_seconds.truncate(write);
        scratch.trajectory_during_supply.truncate(write);
        scratch.trajectory_after_supply.truncate(write);
        scratch.trajectory_offsets[candidate_index + 1] = write as u32;
        scratch.posterior_ready_seconds[candidate_index] = ready_floor;
    }
}

fn prepare_single_phase_columns(
    scratch: &mut ScaleScratch,
    current_supply: f64,
    now: ModelTime,
) -> bool {
    if scratch
        .trajectory_offsets
        .windows(2)
        .any(|offsets| offsets[1] - offsets[0] > 1)
    {
        return false;
    }
    for candidate in 0..scratch.posterior_resource_supply.len() {
        let first = scratch.trajectory_offsets[candidate] as usize;
        let last = scratch.trajectory_offsets[candidate + 1] as usize;
        if first == last {
            let now_seconds = Duration::from_micros(now.as_micros()).as_secs_f64();
            scratch.posterior_pause_seconds[candidate] = now_seconds;
            scratch.posterior_ready_seconds[candidate] = now_seconds;
            scratch.posterior_rebalance_supply[candidate] = current_supply;
            scratch.posterior_resource_supply[candidate] = current_supply;
            continue;
        }
        scratch.posterior_pause_seconds[candidate] = scratch.trajectory_pause_seconds[first];
        scratch.posterior_ready_seconds[candidate] = scratch.trajectory_ready_seconds[first];
        scratch.posterior_rebalance_supply[candidate] = scratch.trajectory_during_supply[first];
        scratch.posterior_resource_supply[candidate] = scratch.trajectory_after_supply[first];
    }
    true
}

fn push_trajectory_event(scratch: &mut ScaleScratch, target: u32, pause_seconds: f64) {
    scratch.trajectory_targets.push(target);
    scratch.trajectory_pause_seconds.push(pause_seconds);
    scratch.trajectory_ready_seconds.push(0.0_f64);
    scratch.trajectory_during_supply.push(0.0_f64);
    scratch.trajectory_after_supply.push(0.0_f64);
}

fn sort_trajectory_events(scratch: &mut ScaleScratch, first: usize) {
    for mut event in first + 1..scratch.trajectory_targets.len() {
        while event > first
            && scratch.trajectory_pause_seconds[event] < scratch.trajectory_pause_seconds[event - 1]
        {
            scratch.trajectory_targets.swap(event, event - 1);
            scratch.trajectory_pause_seconds.swap(event, event - 1);
            scratch.trajectory_ready_seconds.swap(event, event - 1);
            event -= 1;
        }
    }
}

fn seconds_to_micros(seconds: f64) -> u64 {
    (seconds * 1_000_000.0_f64) as u64
}

fn minimal_moved_partitions(partitions: u32, current: u32, target: u32) -> u32 {
    let current = current.min(partitions);
    let target = target.min(partitions);
    let common = current.min(target);
    let overlap = (0..common).fold(0_u32, |sum, owner| {
        sum.saturating_add(
            balanced_partition_count(partitions, current, owner)
                .min(balanced_partition_count(partitions, target, owner)),
        )
    });
    partitions.saturating_sub(overlap)
}

fn balanced_partition_count(partitions: u32, owners: u32, owner: u32) -> u32 {
    partitions / owners + u32::from(owner < partitions % owners)
}

fn prepare_work_cohorts(
    state: &ScaleState,
    scratch: &mut ScaleScratch,
    cohorts: &[crate::Cohort],
    backlog: &[Option<crate::BacklogCohort>],
) {
    let handler_seconds = state.capacity.expected_service_time(state.simd_level);
    scratch.handler_cohorts.clear();
    scratch.resource_cohorts.clear();
    scratch.resource_debt_events = 0.0_f64;
    for cohort in cohorts {
        scratch.handler_cohorts.push(WorkCohort::new(
            *cohort,
            cohort.offered_events * handler_seconds,
        ));
        scratch
            .resource_cohorts
            .push(WorkCohort::new(*cohort, cohort.offered_events));
    }
    for backlog in backlog.iter().flatten() {
        let release_micros = state
            .model_time
            .as_micros()
            .max(backlog.observed_at_micros());
        let deadline_micros = backlog
            .oldest_arrival_micros()
            .saturating_add(state.configuration.objective.budget_micros());
        if deadline_micros <= release_micros {
            scratch.resource_debt_events += f64::from(backlog.event_count());
            continue;
        }
        let cohort = crate::Cohort {
            release_micros,
            deadline_micros,
            offered_events: f64::from(backlog.event_count()),
            partition: backlog.partition(),
            demand_class: backlog.demand_class(),
        };
        scratch.handler_cohorts.push(WorkCohort::new(
            cohort,
            cohort.offered_events * handler_seconds,
        ));
        scratch
            .resource_cohorts
            .push(WorkCohort::new(cohort, cohort.offered_events));
    }
}

fn prepare_partition_work(state: &ScaleState, scratch: &mut ScaleScratch) {
    let cohorts = &scratch.handler_cohorts;
    scratch.partition_work_slot_seconds.fill(0.0_f64);
    scratch.active_partition_count = 0;
    scratch.partition_offsets.fill(0);
    let mut release_min = u64::MAX;
    let mut deadline_max = 0_u64;
    for cohort in cohorts {
        let partition = cohort.partition as usize;
        scratch.partition_work_slot_seconds[partition] += cohort.work_slot_seconds;
        scratch.partition_offsets[partition + 1] += 1;
        release_min = release_min.min(cohort.release_micros);
        deadline_max = deadline_max.max(cohort.deadline_micros);
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
    for (cohort_index, cohort) in cohorts.iter().enumerate() {
        let partition = cohort.partition as usize;
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
            scratch
                .placement_cohorts
                .push(cohorts[cohort_index as usize]);
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

fn prepare_candidate_concurrency(state: &ScaleState, scratch: &mut ScaleScratch) {
    for (candidate_index, concurrency) in scratch.candidate_concurrency.iter_mut().enumerate() {
        let candidate = candidate_index as u32 + 1;
        let active_replicas = candidate.min(scratch.active_partition_count);
        *concurrency =
            f64::from(active_replicas.saturating_mul(state.configuration.slots_per_replica));
    }
}

fn demand_class_totals(
    cohorts: &[crate::Cohort],
    backlog: &[Option<crate::BacklogCohort>],
) -> (f64, f64) {
    let totals = cohorts
        .iter()
        .fold(
            (0.0_f64, 0.0_f64),
            |(normal, failure), cohort| match cohort.demand_class {
                crate::DemandClass::Normal => (normal + cohort.offered_events, failure),
                crate::DemandClass::Failure => (normal, failure + cohort.offered_events),
            },
        );
    backlog.iter().flatten().fold(totals, |totals, cohort| {
        let count = f64::from(cohort.event_count());
        match cohort.demand_class() {
            crate::DemandClass::Normal => (totals.0 + count, totals.1),
            crate::DemandClass::Failure => (totals.0, totals.1 + count),
        }
    })
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
        diagnostics: diagnostics(state, shortfall, 0.0_f64, None),
    })
}

fn diagnostics(
    state: &ScaleState,
    shortfall: f64,
    saturation_probability: f64,
    selected_target: Option<u32>,
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
