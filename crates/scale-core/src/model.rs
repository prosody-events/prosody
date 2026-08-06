use std::time::Duration;

use fearless_simd::Level;

use crate::TransitionDirection;
use crate::arrival::ArrivalFactor;
use crate::capacity::{CapacityFactor, ThroughputPosteriorCell};
use crate::edf::{
    CandidateLoss, CandidateSupply, EdfScratch, has_common_release, prepare,
    required_capacity_prepared, shortfall_prepared_common_release_candidates,
    shortfall_prepared_step,
};
use crate::lead_time::{LeadTimeFactor, sample_index};
use crate::partition::PartitionFactor;
use crate::types::WorkCohort;
use crate::{
    ApplyDecision, ArrivalPosterior, CapacityGrid, Configuration, ConfigurationError,
    DecisionDiagnostics, GroupObservation, HoldDecision, HoldReason, ModelTime, PosteriorError,
    PosteriorQuery, RandomStream, ScaleDecision,
};
use thiserror::Error;

/// All posterior and transition state that survives a controller tick.
pub struct ScaleState {
    simd_level: Level,
    configuration: Configuration,
    model_time: ModelTime,
    arrivals: ArrivalFactor,
    capacity: CapacityFactor,
    partition_placement: PartitionFactor,
    lead_time: LeadTimeFactor,
    rebalance_time: LeadTimeFactor,
    current_replicas: u32,
    random: RandomStream,
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
        Ok(Self {
            simd_level: Level::new(),
            configuration,
            model_time: ModelTime::from_micros(0),
            arrivals: ArrivalFactor::new(),
            capacity: CapacityFactor::new(capacity_grid),
            partition_placement,
            lead_time: LeadTimeFactor::new(),
            rebalance_time: LeadTimeFactor::new(),
            current_replicas: 1,
            random: RandomStream::new(0x7363_616c_652d_636f),
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
        self.arrivals.posterior()
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
    posterior_service_credit: Vec<f64>,
    posterior_sample_loss: Vec<f64>,
    deterministic_loss: Vec<f64>,
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
        let partition_offset_count = partition_count
            .checked_add(1)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let candidate_concurrency = (1..=configuration.replica_count_max)
            .map(|replicas| f64::from(replicas) * f64::from(configuration.slots_per_replica))
            .collect::<Vec<_>>();
        Ok(Self {
            resource_edf: EdfScratch::new(configuration.cohort_count_max)?,
            placement_edf: EdfScratch::new(configuration.cohort_count_max)?,
            handler_cohorts: Vec::with_capacity(cohort_count_max),
            resource_cohorts: Vec::with_capacity(cohort_count_max),
            placement_cohorts: Vec::with_capacity(cohort_count_max),
            partition_offsets: vec![0; partition_offset_count],
            partition_write_offsets: vec![0; partition_count],
            partition_cohort_indexes: vec![0; cohort_count_max],
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
            posterior_service_credit: vec![0.0_f64; replica_count_max],
            posterior_sample_loss: vec![0.0_f64; replica_count_max],
            deterministic_loss: vec![0.0_f64; replica_count_max],
            decision_curve_sample_count: 0,
        })
    }

    /// Returns the fixed number of replica candidates.
    #[must_use]
    pub fn decision_candidate_count(&self) -> usize {
        self.posterior_loss_sums.len()
    }

    /// Writes the expected loss for each replica candidate.
    ///
    /// Candidate index zero represents one replica. The last index represents
    /// the configured replica limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid buffer or an unavailable decision.
    pub fn write_expected_loss_curve(
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
        let sample_count = f64::from(self.decision_curve_sample_count);
        for (output, loss_sum) in expected_losses.iter_mut().zip(&self.posterior_loss_sums) {
            *output = loss_sum / sample_count;
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
        arrivals,
        partition_arrivals,
        resource_window,
        transition,
        current_replicas,
    } = observation;
    state.model_time = now;
    state.capacity.transition();
    state.lead_time.transition();
    state.rebalance_time.transition();
    if let Some(window) = resource_window {
        state.capacity.update(state.simd_level, &window);
    }
    if let Some(evidence) = arrivals {
        state.arrivals.update(evidence);
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

    select_target(state, scratch, cohorts)
}

fn select_target(
    state: &mut ScaleState,
    scratch: &mut ScaleScratch,
    cohorts: &[crate::Cohort],
) -> ScaleDecision {
    prepare_work_cohorts(state, scratch, cohorts);
    prepare_partition_work(state, scratch);
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
    let sample_offset = state.random.open_unit_f64();
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
        let current_supply = curve.throughput(current_concurrency);
        let lead_seconds = state.lead_time.sample_bucket_seconds(&mut state.random);
        let pause_seconds = state
            .rebalance_time
            .sample_bucket_seconds(&mut state.random);
        state.partition_placement.sample_moved_prefix(
            &mut state.random,
            &mut scratch.partition_order,
            &mut scratch.partition_share_draws,
            &mut scratch.moved_partition_share,
        );
        for candidate_index in 0..scratch.posterior_resource_supply.len() {
            let candidate = candidate_index as u32 + 1;
            let (pause_seconds, ready_seconds, retained_fraction) = transition_trajectory(
                state,
                candidate,
                &lead_seconds,
                &pause_seconds,
                &scratch.moved_partition_share,
            );
            scratch.posterior_pause_seconds[candidate_index] = pause_seconds;
            scratch.posterior_ready_seconds[candidate_index] = ready_seconds;
            scratch.posterior_rebalance_supply[candidate_index] =
                current_supply * retained_fraction;
        }
        if common_release {
            let supply = CandidateSupply {
                before: current_supply,
                during: &scratch.posterior_rebalance_supply,
                after: &scratch.posterior_resource_supply,
                pause_seconds: &scratch.posterior_pause_seconds,
                ready_seconds: &scratch.posterior_ready_seconds,
            };
            let mut results = CandidateLoss {
                service_credit: &mut scratch.posterior_service_credit,
                shortfall: &mut scratch.posterior_sample_loss,
            };
            shortfall_prepared_common_release_candidates(
                state.simd_level,
                &scratch.resource_cohorts,
                &supply,
                &mut results,
                &scratch.resource_edf,
            );
        } else {
            for candidate_index in 0..scratch.posterior_resource_supply.len() {
                scratch.posterior_sample_loss[candidate_index] = shortfall_prepared_step(
                    &scratch.resource_cohorts,
                    current_supply,
                    scratch.posterior_rebalance_supply[candidate_index],
                    scratch.posterior_resource_supply[candidate_index],
                    seconds_to_micros(scratch.posterior_pause_seconds[candidate_index]),
                    seconds_to_micros(scratch.posterior_ready_seconds[candidate_index]),
                    &mut scratch.resource_edf,
                );
            }
        }
        for candidate_index in 0..scratch.posterior_resource_supply.len() {
            let resource = scratch.posterior_sample_loss[candidate_index];
            let loss = resource.max(scratch.deterministic_loss[candidate_index]);
            scratch.posterior_loss_sums[candidate_index] += loss;
            scratch.posterior_pass_counts[candidate_index] +=
                f64::from(u8::from(loss <= f64::EPSILON));
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

fn transition_trajectory(
    state: &ScaleState,
    candidate: u32,
    lead_seconds: &[f64; 8],
    pause_seconds: &[f64; 8],
    moved_partition_share: &[f64],
) -> (f64, f64, f64) {
    if candidate == state.current_replicas {
        let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
        return (now_seconds, now_seconds, 1.0_f64);
    }
    let (direction, replica_delta) = if candidate > state.current_replicas {
        (TransitionDirection::Up, candidate - state.current_replicas)
    } else {
        (
            TransitionDirection::Down,
            state.current_replicas - candidate,
        )
    };
    let sample = sample_index(direction, replica_delta);
    let now_seconds = Duration::from_micros(state.model_time.as_micros()).as_secs_f64();
    let pause_at_seconds = now_seconds + lead_seconds[sample];
    let ready_at_seconds = pause_at_seconds + pause_seconds[sample];
    let moved = minimal_moved_partitions(
        state.configuration.partition_count,
        state.current_replicas,
        candidate,
    );
    let retained_fraction = 1.0_f64 - moved_partition_share[moved as usize];
    (pause_at_seconds, ready_at_seconds, retained_fraction)
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

fn prepare_work_cohorts(state: &ScaleState, scratch: &mut ScaleScratch, cohorts: &[crate::Cohort]) {
    let handler_seconds = state.capacity.expected_service_time(state.simd_level);
    scratch.handler_cohorts.clear();
    scratch.resource_cohorts.clear();
    for cohort in cohorts {
        scratch.handler_cohorts.push(WorkCohort::new(
            *cohort,
            cohort.offered_events * handler_seconds,
        ));
        scratch
            .resource_cohorts
            .push(WorkCohort::new(*cohort, cohort.offered_events));
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
    state.arrivals.expected_rate()
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
