use std::collections::VecDeque;
use std::slice;
use std::time::Duration;

use prosody_scale_core::{
    ActuationCommitment, AttemptOutcomeCounts, AttemptOutcomeEvidence, BacklogCohort, CapacityGrid,
    Cohort, CompletionPosteriorCell, Configuration, ConfigurationError, DecisionRejection,
    DemandClass, HoldReason, ModelTime, ObservationBuffer, OccupancyTransition, PosteriorQuery,
    RandomStream, ReadinessGroupId, ReadinessLump, ReadinessObservation, RebalanceEvidence,
    ResourceWindow, ScaleDecision, ScaleScratch, ScaleState, TransitionDirection, step,
};
use statrs::distribution::{DiscreteCDF, Poisson};
use thiserror::Error;

#[cfg(test)]
use prosody_scale_core::ThroughputPosteriorCell;

use crate::{
    CalendarForecastInput, EventContext, EventInputs, FaultPattern, MetricTrace, PlantError,
    ReporterDirective, ScaleDirective, ScheduledReleasesInput, Snapshot, SnapshotChannel,
    SnapshotCursor, SnapshotTable, TickContext, TickGenerator, TickInputs,
};

const HANDLER_COVERAGE_LEVELS: [f64; 4] = [0.5_f64, 0.8_f64, 0.9_f64, 0.95_f64];
const HANDLER_RANK_BIN_COUNT: usize = 10;
#[cfg(test)]
const GAUSS_LEGENDRE_NODES: [f64; 4] = [
    0.183_434_642_495_649_8_f64,
    0.525_532_409_916_329_f64,
    0.796_666_477_413_626_7_f64,
    0.960_289_856_497_536_3_f64,
];
#[cfg(test)]
const GAUSS_LEGENDRE_WEIGHTS: [f64; 4] = [
    0.362_683_783_378_362_f64,
    0.313_706_645_877_887_3_f64,
    0.222_381_034_453_374_5_f64,
    0.101_228_536_290_376_3_f64,
];

/// One controller result retained by the simulator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ControllerSample {
    /// Virtual decision time.
    pub at_micros: u64,
    /// Posterior scenarios used for this decision.
    pub scenario_count: u32,
    /// Current requested replica target.
    pub target: u32,
    /// Last valid saturation cap.
    pub cap: u32,
    /// Whether the controller returned Hold.
    pub hold: bool,
    /// Typed Hold reason. Apply decisions have no reason.
    pub hold_reason: Option<HoldReason>,
    /// Largest fractional resource shortfall.
    pub shortfall: f64,
    /// Posterior expected fractional loss.
    pub expected_loss: f64,
    /// Selected action late-area sum.
    pub selected_late_area_mean: f64,
    /// Selected action replica-seconds sum.
    pub selected_replica_seconds_mean: f64,
    /// Selected action expected-cost sum.
    pub selected_cost: f64,
    /// Zero-based runner-up action index.
    pub runner_up_action_index: u32,
    /// Runner-up action late-area sum.
    pub runner_up_late_area_mean: f64,
    /// Runner-up action replica-seconds sum.
    pub runner_up_replica_seconds_mean: f64,
    /// Runner-up action expected-cost sum.
    pub runner_up_cost: f64,
    /// Zero-based demand-floor action index.
    pub demand_floor: u32,
    /// Posterior expected arrival rate.
    pub arrival_rate_per_second: f64,
    /// Arrival evidence accepted at this controller tick.
    pub arrival_evidence: ArrivalEvidenceSample,
    /// Lower prequential arrival-count quantile.
    pub arrival_predictive_low_count: f64,
    /// Prequential median arrival count.
    pub arrival_predictive_median_count: f64,
    /// Upper prequential arrival-count quantile.
    pub arrival_predictive_high_count: f64,
    /// Randomized prequential rank for the accepted arrival count.
    pub arrival_predictive_rank: f64,
    /// Partition assignments accepted at this controller tick.
    pub partition_evidence_count: u32,
    /// Assignments inside each prequential highest-density credible set.
    pub partition_predictive_covered_counts: [u32; 4],
    /// Randomized prequential partition-rank counts by decile.
    pub partition_predictive_rank_counts: [u32; HANDLER_RANK_BIN_COUNT],
    /// Sum of prequential negative log probabilities.
    pub partition_log_loss_sum: f64,
    /// Sum of prequential predictive entropy values.
    pub partition_entropy_sum: f64,
    /// Lead-time evidence accepted at this controller tick.
    pub lead_time_evidence: LeadTimeEvidenceSample,
    /// Lower prequential lead-time quantile in seconds.
    pub lead_time_predictive_low_seconds: f64,
    /// Prequential median lead time in seconds.
    pub lead_time_predictive_median_seconds: f64,
    /// Upper prequential lead-time quantile in seconds.
    pub lead_time_predictive_high_seconds: f64,
    /// Prequential CDF rank for one completed transition.
    pub lead_time_predictive_rank: f64,
    /// Posterior expected resource capacity.
    pub capacity_per_second: f64,
    /// Lower posterior capacity quantile.
    pub capacity_low_per_second: f64,
    /// Median posterior capacity quantile.
    pub capacity_median_per_second: f64,
    /// Upper posterior capacity quantile.
    pub capacity_high_per_second: f64,
    /// Posterior probability that the resource knee binds.
    pub saturation_probability: f64,
    /// Posterior probability that no knee exists in the supported range.
    pub no_knee_probability: f64,
    /// Posterior expected lead time for a one-replica scale-up.
    pub lead_time_up_seconds: f64,
    /// Posterior expected lead time for a one-replica scale-down.
    pub lead_time_down_seconds: f64,
    /// Posterior expected lead time for the selected or last replica change.
    pub lead_time_seconds: f64,
    /// Mean live handler concurrency for the latest eligible window.
    pub resource_concurrency: f64,
    /// Completed attempt rate for the latest eligible window.
    pub attempt_throughput_per_second: f64,
    /// Capacity evidence emitted at this controller tick.
    pub capacity_evidence: CapacityEvidenceSample,
    /// Lower prequential throughput quantile at the accepted concurrency.
    pub capacity_predictive_low_per_second: f64,
    /// Prequential median throughput at the accepted concurrency.
    pub capacity_predictive_median_per_second: f64,
    /// Upper prequential throughput quantile at the accepted concurrency.
    pub capacity_predictive_high_per_second: f64,
    /// Randomized rank for the model's own completion predictive.
    ///
    /// Shared code gives this predictive and the likelihood the same mean.
    pub capacity_predictive_rank: f64,
    /// Reporter action applied at this controller tick.
    pub reporter: ReporterDirective,
}

/// One arrival count and its accepted exposure.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArrivalWindowSample {
    /// Accepted event count.
    pub count: u32,
    /// Accepted exposure duration.
    pub exposure_seconds: f64,
}

/// Arrival evidence accepted at one controller tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArrivalEvidenceSample {
    /// No arrival evidence was eligible.
    None,
    /// One count and exposure were accepted.
    Accepted(ArrivalWindowSample),
}

/// Lead-time evidence accepted at one controller tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LeadTimeEvidenceSample {
    /// No transition evidence was eligible.
    None,
    /// One transition completed.
    Completed {
        /// Transition direction.
        direction: TransitionDirection,
        /// Absolute requested replica change.
        replica_delta: u32,
        /// Observed duration in seconds.
        elapsed_seconds: f64,
    },
    /// One transition was superseded before completion.
    Censored {
        /// Transition direction.
        direction: TransitionDirection,
        /// Absolute requested replica change.
        replica_delta: u32,
        /// Observed lower bound in seconds.
        exposure_seconds: f64,
    },
}

/// Capacity evidence emitted by the simulator adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityEvidenceKind {
    /// No capacity evidence was eligible.
    None,
    /// One passive resource window was emitted.
    Window,
}

/// One measured capacity window retained for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CapacityWindowSample {
    /// Mean live concurrency.
    pub concurrency: f64,
    /// Eligible exposure duration.
    pub exposure_seconds: f64,
    /// Handler attempts completed during the exposure.
    pub completed_attempts: u32,
}

impl CapacityWindowSample {
    /// Returns useful completions per second.
    #[must_use]
    pub fn throughput_per_second(self) -> f64 {
        f64::from(self.completed_attempts) / self.exposure_seconds
    }
}

/// Capacity evidence accepted at one controller tick.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CapacityEvidenceSample {
    /// No capacity evidence was eligible.
    None,
    /// One passive resource window was accepted.
    Window(CapacityWindowSample),
}

impl CapacityEvidenceSample {
    /// Returns the evidence variant without its measurements.
    #[must_use]
    pub const fn kind(self) -> CapacityEvidenceKind {
        match self {
            Self::None => CapacityEvidenceKind::None,
            Self::Window(_) => CapacityEvidenceKind::Window,
        }
    }
}

/// Fixed-capacity structure-of-arrays controller trace.
pub struct ControllerTrace {
    at_micros: Vec<u64>,
    scenario_count: Vec<u32>,
    target: Vec<u32>,
    cap: Vec<u32>,
    hold: Vec<bool>,
    hold_reason: Vec<Option<HoldReason>>,
    shortfall: Vec<f64>,
    expected_loss: Vec<f64>,
    selected_late_area_mean: Vec<f64>,
    selected_replica_seconds_mean: Vec<f64>,
    selected_cost: Vec<f64>,
    runner_up_action_index: Vec<u32>,
    runner_up_late_area_mean: Vec<f64>,
    runner_up_replica_seconds_mean: Vec<f64>,
    runner_up_cost: Vec<f64>,
    demand_floor: Vec<u32>,
    arrival_rate_per_second: Vec<f64>,
    arrival_evidence: Vec<bool>,
    arrival_evidence_count: Vec<u32>,
    arrival_evidence_exposure_seconds: Vec<f64>,
    arrival_predictive_low_count: Vec<f64>,
    arrival_predictive_median_count: Vec<f64>,
    arrival_predictive_high_count: Vec<f64>,
    arrival_predictive_rank: Vec<f64>,
    partition_evidence_count: Vec<u32>,
    partition_predictive_covered_counts: Vec<[u32; 4]>,
    partition_predictive_rank_counts: Vec<[u32; HANDLER_RANK_BIN_COUNT]>,
    partition_log_loss_sum: Vec<f64>,
    partition_entropy_sum: Vec<f64>,
    lead_time_evidence: Vec<LeadTimeEvidenceSample>,
    lead_time_predictive_low_seconds: Vec<f64>,
    lead_time_predictive_median_seconds: Vec<f64>,
    lead_time_predictive_high_seconds: Vec<f64>,
    lead_time_predictive_rank: Vec<f64>,
    capacity_per_second: Vec<f64>,
    capacity_low_per_second: Vec<f64>,
    capacity_median_per_second: Vec<f64>,
    capacity_high_per_second: Vec<f64>,
    saturation_probability: Vec<f64>,
    no_knee_probability: Vec<f64>,
    lead_time_up_seconds: Vec<f64>,
    lead_time_down_seconds: Vec<f64>,
    lead_time_seconds: Vec<f64>,
    resource_concurrency: Vec<f64>,
    attempt_throughput_per_second: Vec<f64>,
    capacity_evidence: Vec<CapacityEvidenceKind>,
    capacity_after_concurrency: Vec<f64>,
    capacity_after_exposure_seconds: Vec<f64>,
    capacity_after_completed_attempts: Vec<u32>,
    reporter: Vec<ReporterDirective>,
    capacity_predictive_low_per_second: Vec<f64>,
    capacity_predictive_median_per_second: Vec<f64>,
    capacity_predictive_high_per_second: Vec<f64>,
    capacity_predictive_rank: Vec<f64>,
    capacity_posterior_values: Vec<f64>,
    capacity_prior_probabilities: Vec<f64>,
    capacity_posterior_probabilities: Vec<f64>,
    service_time_posterior: DiscretePosteriorTrace,
    collapse_posterior: DiscretePosteriorTrace,
    knee_posterior: DiscretePosteriorTrace,
    saturation_state_posterior: DiscretePosteriorTrace,
    normal_retry_posterior: DiscretePosteriorTrace,
    failure_retry_posterior: DiscretePosteriorTrace,
    partition_share_posterior: DiscretePosteriorTrace,
    lead_time_up_posterior: DiscretePosteriorTrace,
    lead_time_down_posterior: DiscretePosteriorTrace,
    rebalance_time_up_posterior: DiscretePosteriorTrace,
    rebalance_time_down_posterior: DiscretePosteriorTrace,
    arrival_posterior_values: Vec<f64>,
    arrival_prior_probabilities: Vec<f64>,
    arrival_posterior_probabilities: Vec<f64>,
    decision_candidate_count: usize,
    decision_expected_losses: Vec<f64>,
    decision_deadline_satisfaction_probabilities: Vec<f64>,
    decision_deadline_rejections: Vec<f64>,
    decision_placement_rejections: Vec<f64>,
}

struct DiscretePosteriorTrace {
    query: PosteriorQuery,
    values: Vec<f64>,
    prior: Vec<f64>,
    probabilities: Vec<f64>,
}

struct PosteriorTraces {
    service_time: DiscretePosteriorTrace,
    collapse: DiscretePosteriorTrace,
    knee: DiscretePosteriorTrace,
    saturation_state: DiscretePosteriorTrace,
    normal_retry: DiscretePosteriorTrace,
    failure_retry: DiscretePosteriorTrace,
    partition_share: DiscretePosteriorTrace,
    lead_time_up: DiscretePosteriorTrace,
    lead_time_down: DiscretePosteriorTrace,
    rebalance_time_up: DiscretePosteriorTrace,
    rebalance_time_down: DiscretePosteriorTrace,
}

impl PosteriorTraces {
    fn new(state: &ScaleState, capacity: usize) -> Result<Self, PlantError> {
        let trace = |query| DiscretePosteriorTrace::new(state, query, capacity);
        Ok(Self {
            service_time: trace(PosteriorQuery::ServiceTime)?,
            collapse: trace(PosteriorQuery::Collapse)?,
            knee: trace(PosteriorQuery::Knee)?,
            saturation_state: trace(PosteriorQuery::SaturationState)?,
            normal_retry: trace(PosteriorQuery::NormalRetryProbability)?,
            failure_retry: trace(PosteriorQuery::FailureRetryProbability)?,
            partition_share: trace(PosteriorQuery::PartitionShare)?,
            lead_time_up: trace(PosteriorQuery::LeadTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            })?,
            lead_time_down: trace(PosteriorQuery::LeadTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            })?,
            rebalance_time_up: trace(PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            })?,
            rebalance_time_down: trace(PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            })?,
        })
    }
}

fn initial_capacity_posterior(
    state: &ScaleState,
    value_count: usize,
) -> Result<(Vec<f64>, Vec<f64>), PlantError> {
    let mut values = vec![0.0_f64; value_count];
    let mut probabilities = vec![0.0_f64; value_count];
    state.write_capacity_posterior(&mut values, &mut probabilities)?;
    Ok((values, probabilities))
}

fn cell_counts(capacity: usize, value_count: u32) -> Result<(usize, usize), PlantError> {
    let count = usize::try_from(value_count).map_err(|_| PlantError::PlatformLimit)?;
    let cells = capacity
        .checked_mul(count)
        .ok_or(PlantError::PlatformLimit)?;
    Ok((count, cells))
}

fn initial_arrival_posterior(state: &ScaleState) -> Result<(Vec<f64>, Vec<f64>), PlantError> {
    let value_count = usize::try_from(state.arrival_posterior_value_count())
        .map_err(|_| PlantError::PlatformLimit)?;
    let mut values = vec![0.0_f64; value_count];
    let mut probabilities = vec![0.0_f64; value_count];
    state.write_arrival_posterior(&mut values, &mut probabilities)?;
    Ok((values, probabilities))
}

impl ControllerTrace {
    fn new(sample_count_max: u32, state: &ScaleState) -> Result<Self, PlantError> {
        let capacity = usize::try_from(sample_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "controller_trace_count_max",
            });
        }
        let (posterior_value_count, posterior_cell_count) =
            cell_counts(capacity, state.capacity_posterior_value_count())?;
        let (decision_candidate_count, decision_cell_count) =
            cell_counts(capacity, state.configuration().replica_count_max)?;
        let (capacity_posterior_values, capacity_prior_probabilities) =
            initial_capacity_posterior(state, posterior_value_count)?;
        let posteriors = PosteriorTraces::new(state, capacity)?;
        let (arrival_posterior_values, arrival_prior_probabilities) =
            initial_arrival_posterior(state)?;
        let arrival_cell_count = capacity
            .checked_mul(arrival_posterior_values.len())
            .ok_or(PlantError::PlatformLimit)?;
        Ok(Self {
            at_micros: Vec::with_capacity(capacity),
            scenario_count: Vec::with_capacity(capacity),
            target: Vec::with_capacity(capacity),
            cap: Vec::with_capacity(capacity),
            hold: Vec::with_capacity(capacity),
            hold_reason: Vec::with_capacity(capacity),
            shortfall: Vec::with_capacity(capacity),
            expected_loss: Vec::with_capacity(capacity),
            selected_late_area_mean: Vec::with_capacity(capacity),
            selected_replica_seconds_mean: Vec::with_capacity(capacity),
            selected_cost: Vec::with_capacity(capacity),
            runner_up_action_index: Vec::with_capacity(capacity),
            runner_up_late_area_mean: Vec::with_capacity(capacity),
            runner_up_replica_seconds_mean: Vec::with_capacity(capacity),
            runner_up_cost: Vec::with_capacity(capacity),
            demand_floor: Vec::with_capacity(capacity),
            arrival_rate_per_second: Vec::with_capacity(capacity),
            arrival_evidence: Vec::with_capacity(capacity),
            arrival_evidence_count: Vec::with_capacity(capacity),
            arrival_evidence_exposure_seconds: Vec::with_capacity(capacity),
            arrival_predictive_low_count: Vec::with_capacity(capacity),
            arrival_predictive_median_count: Vec::with_capacity(capacity),
            arrival_predictive_high_count: Vec::with_capacity(capacity),
            arrival_predictive_rank: Vec::with_capacity(capacity),
            partition_evidence_count: Vec::with_capacity(capacity),
            partition_predictive_covered_counts: Vec::with_capacity(capacity),
            partition_predictive_rank_counts: Vec::with_capacity(capacity),
            partition_log_loss_sum: Vec::with_capacity(capacity),
            partition_entropy_sum: Vec::with_capacity(capacity),
            lead_time_evidence: Vec::with_capacity(capacity),
            lead_time_predictive_low_seconds: Vec::with_capacity(capacity),
            lead_time_predictive_median_seconds: Vec::with_capacity(capacity),
            lead_time_predictive_high_seconds: Vec::with_capacity(capacity),
            lead_time_predictive_rank: Vec::with_capacity(capacity),
            capacity_per_second: Vec::with_capacity(capacity),
            capacity_low_per_second: Vec::with_capacity(capacity),
            capacity_median_per_second: Vec::with_capacity(capacity),
            capacity_high_per_second: Vec::with_capacity(capacity),
            saturation_probability: Vec::with_capacity(capacity),
            no_knee_probability: Vec::with_capacity(capacity),
            lead_time_up_seconds: Vec::with_capacity(capacity),
            lead_time_down_seconds: Vec::with_capacity(capacity),
            lead_time_seconds: Vec::with_capacity(capacity),
            resource_concurrency: Vec::with_capacity(capacity),
            attempt_throughput_per_second: Vec::with_capacity(capacity),
            capacity_evidence: Vec::with_capacity(capacity),
            capacity_after_concurrency: Vec::with_capacity(capacity),
            capacity_after_exposure_seconds: Vec::with_capacity(capacity),
            capacity_after_completed_attempts: Vec::with_capacity(capacity),
            reporter: Vec::with_capacity(capacity),
            capacity_predictive_low_per_second: Vec::with_capacity(capacity),
            capacity_predictive_median_per_second: Vec::with_capacity(capacity),
            capacity_predictive_high_per_second: Vec::with_capacity(capacity),
            capacity_predictive_rank: Vec::with_capacity(capacity),
            capacity_posterior_values,
            capacity_prior_probabilities,
            capacity_posterior_probabilities: Vec::with_capacity(posterior_cell_count),
            service_time_posterior: posteriors.service_time,
            collapse_posterior: posteriors.collapse,
            knee_posterior: posteriors.knee,
            saturation_state_posterior: posteriors.saturation_state,
            normal_retry_posterior: posteriors.normal_retry,
            failure_retry_posterior: posteriors.failure_retry,
            partition_share_posterior: posteriors.partition_share,
            lead_time_up_posterior: posteriors.lead_time_up,
            lead_time_down_posterior: posteriors.lead_time_down,
            rebalance_time_up_posterior: posteriors.rebalance_time_up,
            rebalance_time_down_posterior: posteriors.rebalance_time_down,
            arrival_posterior_values,
            arrival_prior_probabilities,
            arrival_posterior_probabilities: Vec::with_capacity(arrival_cell_count),
            decision_candidate_count,
            decision_expected_losses: Vec::with_capacity(decision_cell_count),
            decision_deadline_satisfaction_probabilities: Vec::with_capacity(decision_cell_count),
            decision_deadline_rejections: Vec::with_capacity(decision_cell_count),
            decision_placement_rejections: Vec::with_capacity(decision_cell_count),
        })
    }

    /// Returns the retained sample count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.at_micros.len()
    }

    /// Returns true when the trace has no sample.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.at_micros.is_empty()
    }

    pub(crate) fn final_micros(&self) -> u64 {
        self.at_micros.last().copied().unwrap_or(0)
    }

    /// Reconstructs one controller sample from its columns.
    #[must_use]
    pub fn sample(&self, index: usize) -> Option<ControllerSample> {
        Some(ControllerSample {
            at_micros: *self.at_micros.get(index)?,
            scenario_count: self.scenario_count[index],
            target: self.target[index],
            cap: self.cap[index],
            hold: self.hold[index],
            hold_reason: self.hold_reason[index],
            shortfall: self.shortfall[index],
            expected_loss: self.expected_loss[index],
            selected_late_area_mean: self.selected_late_area_mean[index],
            selected_replica_seconds_mean: self.selected_replica_seconds_mean[index],
            selected_cost: self.selected_cost[index],
            runner_up_action_index: self.runner_up_action_index[index],
            runner_up_late_area_mean: self.runner_up_late_area_mean[index],
            runner_up_replica_seconds_mean: self.runner_up_replica_seconds_mean[index],
            runner_up_cost: self.runner_up_cost[index],
            demand_floor: self.demand_floor[index],
            arrival_rate_per_second: self.arrival_rate_per_second[index],
            arrival_evidence: self.arrival_evidence_sample(index),
            arrival_predictive_low_count: self.arrival_predictive_low_count[index],
            arrival_predictive_median_count: self.arrival_predictive_median_count[index],
            arrival_predictive_high_count: self.arrival_predictive_high_count[index],
            arrival_predictive_rank: self.arrival_predictive_rank[index],
            partition_evidence_count: self.partition_evidence_count[index],
            partition_predictive_covered_counts: self.partition_predictive_covered_counts[index],
            partition_predictive_rank_counts: self.partition_predictive_rank_counts[index],
            partition_log_loss_sum: self.partition_log_loss_sum[index],
            partition_entropy_sum: self.partition_entropy_sum[index],
            lead_time_evidence: self.lead_time_evidence[index],
            lead_time_predictive_low_seconds: self.lead_time_predictive_low_seconds[index],
            lead_time_predictive_median_seconds: self.lead_time_predictive_median_seconds[index],
            lead_time_predictive_high_seconds: self.lead_time_predictive_high_seconds[index],
            lead_time_predictive_rank: self.lead_time_predictive_rank[index],
            capacity_per_second: self.capacity_per_second[index],
            capacity_low_per_second: self.capacity_low_per_second[index],
            capacity_median_per_second: self.capacity_median_per_second[index],
            capacity_high_per_second: self.capacity_high_per_second[index],
            saturation_probability: self.saturation_probability[index],
            no_knee_probability: self.no_knee_probability[index],
            lead_time_up_seconds: self.lead_time_up_seconds[index],
            lead_time_down_seconds: self.lead_time_down_seconds[index],
            lead_time_seconds: self.lead_time_seconds[index],
            resource_concurrency: self.resource_concurrency[index],
            attempt_throughput_per_second: self.attempt_throughput_per_second[index],
            capacity_evidence: self.evidence_sample(index),
            capacity_predictive_low_per_second: self.capacity_predictive_low_per_second[index],
            capacity_predictive_median_per_second: self.capacity_predictive_median_per_second
                [index],
            capacity_predictive_high_per_second: self.capacity_predictive_high_per_second[index],
            capacity_predictive_rank: self.capacity_predictive_rank[index],
            reporter: self.reporter[index],
        })
    }

    /// Counts samples that contain the selected capacity evidence.
    #[must_use]
    pub fn capacity_evidence_count(&self, kind: CapacityEvidenceKind) -> usize {
        self.capacity_evidence
            .iter()
            .filter(|evidence| **evidence == kind)
            .count()
    }

    /// Returns the ordered values on the marginal capacity axis.
    #[must_use]
    pub fn capacity_posterior_values(&self) -> &[f64] {
        &self.capacity_posterior_values
    }

    /// Returns the marginal capacity prior before the first observation.
    #[must_use]
    pub fn capacity_prior(&self) -> &[f64] {
        &self.capacity_prior_probabilities
    }

    /// Returns one marginal capacity posterior at the selected time.
    #[must_use]
    pub fn capacity_posterior(&self, index: usize) -> Option<&[f64]> {
        let width = self.capacity_posterior_values.len();
        let start = index.checked_mul(width)?;
        let end = start.checked_add(width)?;
        self.capacity_posterior_probabilities.get(start..end)
    }

    /// Returns the ordered axis for one retained posterior.
    #[must_use]
    pub fn posterior_values(&self, query: PosteriorQuery) -> Option<&[f64]> {
        if query == PosteriorQuery::Capacity {
            return Some(&self.capacity_posterior_values);
        }
        self.discrete_posterior(query)
            .map(|posterior| posterior.values.as_slice())
    }

    /// Returns one discrete prior before the first observation.
    #[must_use]
    pub fn posterior_prior(&self, query: PosteriorQuery) -> Option<&[f64]> {
        if query == PosteriorQuery::Capacity {
            return Some(&self.capacity_prior_probabilities);
        }
        self.discrete_posterior(query)
            .map(|posterior| posterior.prior.as_slice())
    }

    /// Returns one normalized discrete posterior at the selected time.
    #[must_use]
    pub fn posterior(&self, query: PosteriorQuery, index: usize) -> Option<&[f64]> {
        if query == PosteriorQuery::Capacity {
            return self.capacity_posterior(index);
        }
        self.discrete_posterior(query)?.posterior(index)
    }

    /// Returns the expected loss for each replica candidate at one decision.
    #[must_use]
    pub fn decision_expected_losses(&self, index: usize) -> Option<&[f64]> {
        let start = index.checked_mul(self.decision_candidate_count)?;
        let end = start.checked_add(self.decision_candidate_count)?;
        self.decision_expected_losses.get(start..end)
    }

    /// Returns the absolute deadline-satisfaction probability for each
    /// candidate.
    #[must_use]
    pub fn decision_deadline_satisfaction_probabilities(&self, index: usize) -> Option<&[f64]> {
        let start = index.checked_mul(self.decision_candidate_count)?;
        let end = start.checked_add(self.decision_candidate_count)?;
        self.decision_deadline_satisfaction_probabilities
            .get(start..end)
    }

    /// Returns one rejection-reason probability for each replica candidate.
    #[must_use]
    pub fn decision_rejection_probabilities(
        &self,
        reason: DecisionRejection,
        index: usize,
    ) -> Option<&[f64]> {
        let start = index.checked_mul(self.decision_candidate_count)?;
        let end = start.checked_add(self.decision_candidate_count)?;
        let values = match reason {
            DecisionRejection::Deadline => &self.decision_deadline_rejections,
            DecisionRejection::PartitionPlacement => &self.decision_placement_rejections,
        };
        values.get(start..end)
    }

    /// Returns the ordered finite arrival-rate axis.
    #[must_use]
    pub fn arrival_posterior_values(&self) -> &[f64] {
        &self.arrival_posterior_values
    }

    /// Returns the finite arrival-rate prior before the first observation.
    #[must_use]
    pub fn arrival_prior(&self) -> &[f64] {
        &self.arrival_prior_probabilities
    }

    /// Returns one exact finite arrival-rate posterior.
    #[must_use]
    pub fn arrival_posterior(&self, index: usize) -> Option<&[f64]> {
        let width = self.arrival_posterior_values.len();
        let start = index.checked_mul(width)?;
        let end = start.checked_add(width)?;
        self.arrival_posterior_probabilities.get(start..end)
    }

    fn arrival_evidence_sample(&self, index: usize) -> ArrivalEvidenceSample {
        if self.arrival_evidence[index] {
            ArrivalEvidenceSample::Accepted(ArrivalWindowSample {
                count: self.arrival_evidence_count[index],
                exposure_seconds: self.arrival_evidence_exposure_seconds[index],
            })
        } else {
            ArrivalEvidenceSample::None
        }
    }

    fn discrete_posterior(&self, query: PosteriorQuery) -> Option<&DiscretePosteriorTrace> {
        match query {
            PosteriorQuery::ServiceTime => Some(&self.service_time_posterior),
            PosteriorQuery::Collapse => Some(&self.collapse_posterior),
            PosteriorQuery::Knee => Some(&self.knee_posterior),
            PosteriorQuery::SaturationState => Some(&self.saturation_state_posterior),
            PosteriorQuery::NormalRetryProbability => Some(&self.normal_retry_posterior),
            PosteriorQuery::FailureRetryProbability => Some(&self.failure_retry_posterior),
            PosteriorQuery::PartitionShare => Some(&self.partition_share_posterior),
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            } => Some(&self.lead_time_up_posterior),
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            } => Some(&self.lead_time_down_posterior),
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            } => Some(&self.rebalance_time_up_posterior),
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            } => Some(&self.rebalance_time_down_posterior),
            PosteriorQuery::Capacity
            | PosteriorQuery::LeadTime { .. }
            | PosteriorQuery::RebalanceTime { .. } => None,
        }
    }

    fn evidence_sample(&self, index: usize) -> CapacityEvidenceSample {
        let after = CapacityWindowSample {
            concurrency: self.capacity_after_concurrency[index],
            exposure_seconds: self.capacity_after_exposure_seconds[index],
            completed_attempts: self.capacity_after_completed_attempts[index],
        };
        match self.capacity_evidence[index] {
            CapacityEvidenceKind::None => CapacityEvidenceSample::None,
            CapacityEvidenceKind::Window => CapacityEvidenceSample::Window(after),
        }
    }

    /// Adds the latest controller values to each plant metric sample.
    pub fn apply_to(&self, trace: &mut MetricTrace) {
        trace.controller_metrics = true;
        trace.resource_metrics = true;
        let mut controller_index = 0_usize;
        let mut reporter_index = None;
        let mut last_report_micros = None;
        for metric_index in 0..trace.at_micros.len() {
            while controller_index + 1 < self.len()
                && self.at_micros[controller_index + 1] <= trace.at_micros[metric_index]
            {
                controller_index += 1;
            }
            if self.is_empty() || self.at_micros[controller_index] > trace.at_micros[metric_index] {
                continue;
            }
            let reporter_start = reporter_index.map_or(0, |index| index + 1);
            for index in reporter_start..=controller_index {
                if self.reporter[index] != ReporterDirective::Missing {
                    last_report_micros = Some(self.at_micros[index]);
                }
            }
            reporter_index = Some(controller_index);
            trace.target[metric_index] = self.target[controller_index];
            trace.cap[metric_index] = self.cap[controller_index];
            trace.hold[metric_index] = self.hold[controller_index];
            trace.expected_loss[metric_index] = self.expected_loss[controller_index];
            trace.prediction_median[metric_index] = f64::NAN;
            trace.resource_concurrency[metric_index] = self.resource_concurrency[controller_index];
            trace.attempt_throughput_per_second[metric_index] =
                self.attempt_throughput_per_second[controller_index];
            trace.capacity_low_per_second[metric_index] =
                self.capacity_low_per_second[controller_index];
            trace.capacity_median_per_second[metric_index] =
                self.capacity_median_per_second[controller_index];
            trace.capacity_high_per_second[metric_index] =
                self.capacity_high_per_second[controller_index];
            trace.saturation_probability[metric_index] =
                self.saturation_probability[controller_index];
            trace.no_knee_probability[metric_index] = self.no_knee_probability[controller_index];
            trace.lead_time_up_seconds[metric_index] = self.lead_time_up_seconds[controller_index];
            trace.lead_time_down_seconds[metric_index] =
                self.lead_time_down_seconds[controller_index];
            trace.lead_time_seconds[metric_index] = self.lead_time_seconds[controller_index];
            trace.missing_reporters[metric_index] =
                u32::from(self.reporter[controller_index] == ReporterDirective::Missing);
            trace.snapshot_age_micros[metric_index] = last_report_micros
                .map_or(0, |last| trace.at_micros[metric_index].saturating_sub(last));
        }
    }

    fn push(
        &mut self,
        sample: &ControllerSample,
        state: &ScaleState,
        scratch: &ScaleScratch,
    ) -> Result<(), PlantError> {
        if self.at_micros.len() == self.at_micros.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.push_sample_columns(sample);
        self.push_decision_columns(sample.target, scratch);
        self.push_decision_curves(scratch)?;
        self.push_posteriors(state)?;
        Ok(())
    }

    fn push_sample_columns(&mut self, sample: &ControllerSample) {
        self.at_micros.push(sample.at_micros);
        self.scenario_count.push(sample.scenario_count);
        self.target.push(sample.target);
        self.cap.push(sample.cap);
        self.hold.push(sample.hold);
        self.hold_reason.push(sample.hold_reason);
        self.shortfall.push(sample.shortfall);
        self.expected_loss.push(sample.expected_loss);
        self.arrival_rate_per_second
            .push(sample.arrival_rate_per_second);
        match sample.arrival_evidence {
            ArrivalEvidenceSample::None => {
                self.arrival_evidence.push(false);
                self.arrival_evidence_count.push(0);
                self.arrival_evidence_exposure_seconds.push(f64::NAN);
            }
            ArrivalEvidenceSample::Accepted(window) => {
                self.arrival_evidence.push(true);
                self.arrival_evidence_count.push(window.count);
                self.arrival_evidence_exposure_seconds
                    .push(window.exposure_seconds);
            }
        }
        self.arrival_predictive_low_count
            .push(sample.arrival_predictive_low_count);
        self.arrival_predictive_median_count
            .push(sample.arrival_predictive_median_count);
        self.arrival_predictive_high_count
            .push(sample.arrival_predictive_high_count);
        self.arrival_predictive_rank
            .push(sample.arrival_predictive_rank);
        self.partition_evidence_count
            .push(sample.partition_evidence_count);
        self.partition_predictive_covered_counts
            .push(sample.partition_predictive_covered_counts);
        self.partition_predictive_rank_counts
            .push(sample.partition_predictive_rank_counts);
        self.partition_log_loss_sum
            .push(sample.partition_log_loss_sum);
        self.partition_entropy_sum
            .push(sample.partition_entropy_sum);
        self.lead_time_evidence.push(sample.lead_time_evidence);
        self.lead_time_predictive_low_seconds
            .push(sample.lead_time_predictive_low_seconds);
        self.lead_time_predictive_median_seconds
            .push(sample.lead_time_predictive_median_seconds);
        self.lead_time_predictive_high_seconds
            .push(sample.lead_time_predictive_high_seconds);
        self.lead_time_predictive_rank
            .push(sample.lead_time_predictive_rank);
        self.capacity_per_second.push(sample.capacity_per_second);
        self.capacity_low_per_second
            .push(sample.capacity_low_per_second);
        self.capacity_median_per_second
            .push(sample.capacity_median_per_second);
        self.capacity_high_per_second
            .push(sample.capacity_high_per_second);
        self.saturation_probability
            .push(sample.saturation_probability);
        self.no_knee_probability.push(sample.no_knee_probability);
        self.lead_time_up_seconds.push(sample.lead_time_up_seconds);
        self.lead_time_down_seconds
            .push(sample.lead_time_down_seconds);
        self.lead_time_seconds.push(sample.lead_time_seconds);
        self.resource_concurrency.push(sample.resource_concurrency);
        self.attempt_throughput_per_second
            .push(sample.attempt_throughput_per_second);
        let (kind, after) = evidence_columns(sample.capacity_evidence);
        self.capacity_evidence.push(kind);
        self.capacity_after_concurrency.push(after.concurrency);
        self.capacity_after_exposure_seconds
            .push(after.exposure_seconds);
        self.capacity_after_completed_attempts
            .push(after.completed_attempts);
        self.reporter.push(sample.reporter);
        self.capacity_predictive_low_per_second
            .push(sample.capacity_predictive_low_per_second);
        self.capacity_predictive_median_per_second
            .push(sample.capacity_predictive_median_per_second);
        self.capacity_predictive_high_per_second
            .push(sample.capacity_predictive_high_per_second);
        self.capacity_predictive_rank
            .push(sample.capacity_predictive_rank);
    }

    fn push_decision_columns(&mut self, target: u32, scratch: &ScaleScratch) {
        let summary = usize::try_from(target)
            .map_or(None, |target| target.checked_sub(1))
            .and_then(|selected| scratch.decision_column_summary(selected));
        let selected = summary.map(|summary| summary.selected);
        let runner_up = summary.and_then(|summary| summary.runner_up);
        self.selected_late_area_mean
            .push(selected.map_or(f64::NAN, |action| action.late_area_mean));
        self.selected_replica_seconds_mean
            .push(selected.map_or(f64::NAN, |action| action.replica_seconds_mean));
        self.selected_cost
            .push(selected.map_or(f64::NAN, |action| action.cost));
        self.runner_up_action_index
            .push(runner_up.map_or(u32::MAX, |action| action.action_index));
        self.runner_up_late_area_mean
            .push(runner_up.map_or(f64::NAN, |action| action.late_area_mean));
        self.runner_up_replica_seconds_mean
            .push(runner_up.map_or(f64::NAN, |action| action.replica_seconds_mean));
        self.runner_up_cost
            .push(runner_up.map_or(f64::NAN, |action| action.cost));
        self.demand_floor
            .push(summary.map_or(u32::MAX, |summary| summary.demand_floor));
    }

    fn push_decision_curves(&mut self, scratch: &ScaleScratch) -> Result<(), PlantError> {
        let decision_start = self.decision_expected_losses.len();
        let decision_end = decision_start
            .checked_add(self.decision_candidate_count)
            .ok_or(PlantError::PlatformLimit)?;
        if decision_end > self.decision_expected_losses.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.decision_expected_losses.resize(decision_end, f64::NAN);
        if decision_end > self.decision_deadline_satisfaction_probabilities.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.decision_deadline_satisfaction_probabilities
            .resize(decision_end, f64::NAN);
        match scratch.write_decision_expected_losses(
            &mut self.decision_expected_losses[decision_start..decision_end],
        ) {
            Ok(()) | Err(prosody_scale_core::DecisionCurveError::Unavailable) => {}
            Err(error) => return Err(error.into()),
        }
        write_rejection_curve(
            scratch,
            DecisionRejection::Deadline,
            &mut self.decision_deadline_rejections,
            decision_start,
            decision_end,
        )?;
        for (satisfaction, rejection) in self.decision_deadline_satisfaction_probabilities
            [decision_start..decision_end]
            .iter_mut()
            .zip(&self.decision_deadline_rejections[decision_start..decision_end])
        {
            *satisfaction = 1.0_f64 - *rejection;
        }
        write_rejection_curve(
            scratch,
            DecisionRejection::PartitionPlacement,
            &mut self.decision_placement_rejections,
            decision_start,
            decision_end,
        )?;
        Ok(())
    }

    fn push_posteriors(&mut self, state: &ScaleState) -> Result<(), PlantError> {
        let posterior_start = self.capacity_posterior_probabilities.len();
        let posterior_end = posterior_start
            .checked_add(self.capacity_posterior_values.len())
            .ok_or(PlantError::PlatformLimit)?;
        if posterior_end > self.capacity_posterior_probabilities.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.capacity_posterior_probabilities
            .resize(posterior_end, 0.0_f64);
        state.write_capacity_posterior(
            &mut self.capacity_posterior_values,
            &mut self.capacity_posterior_probabilities[posterior_start..posterior_end],
        )?;
        self.service_time_posterior.push(state)?;
        self.collapse_posterior.push(state)?;
        self.knee_posterior.push(state)?;
        self.saturation_state_posterior.push(state)?;
        self.normal_retry_posterior.push(state)?;
        self.failure_retry_posterior.push(state)?;
        self.partition_share_posterior.push(state)?;
        self.lead_time_up_posterior.push(state)?;
        self.lead_time_down_posterior.push(state)?;
        self.rebalance_time_up_posterior.push(state)?;
        self.rebalance_time_down_posterior.push(state)?;
        let arrival_start = self.arrival_posterior_probabilities.len();
        let arrival_end = arrival_start
            .checked_add(self.arrival_posterior_values.len())
            .ok_or(PlantError::PlatformLimit)?;
        if arrival_end > self.arrival_posterior_probabilities.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.arrival_posterior_probabilities
            .resize(arrival_end, 0.0_f64);
        state.write_arrival_posterior(
            &mut self.arrival_posterior_values,
            &mut self.arrival_posterior_probabilities[arrival_start..arrival_end],
        )?;
        Ok(())
    }
}

impl DiscretePosteriorTrace {
    fn new(
        state: &ScaleState,
        query: PosteriorQuery,
        sample_count_max: usize,
    ) -> Result<Self, PlantError> {
        let width = usize::try_from(state.posterior_value_count(query)?)
            .map_err(|_| PlantError::PlatformLimit)?;
        let cell_count = sample_count_max
            .checked_mul(width)
            .ok_or(PlantError::PlatformLimit)?;
        let mut values = vec![0.0_f64; width];
        let mut prior = vec![0.0_f64; width];
        state.write_posterior(query, &mut values, &mut prior)?;
        Ok(Self {
            query,
            values,
            prior,
            probabilities: Vec::with_capacity(cell_count),
        })
    }

    fn posterior(&self, index: usize) -> Option<&[f64]> {
        let start = index.checked_mul(self.values.len())?;
        let end = start.checked_add(self.values.len())?;
        self.probabilities.get(start..end)
    }

    fn push(&mut self, state: &ScaleState) -> Result<(), PlantError> {
        let start = self.probabilities.len();
        let end = start
            .checked_add(self.values.len())
            .ok_or(PlantError::PlatformLimit)?;
        if end > self.probabilities.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.probabilities.resize(end, 0.0_f64);
        state.write_posterior(
            self.query,
            &mut self.values,
            &mut self.probabilities[start..end],
        )?;
        Ok(())
    }
}

fn evidence_columns(
    evidence: CapacityEvidenceSample,
) -> (CapacityEvidenceKind, CapacityWindowSample) {
    const EMPTY: CapacityWindowSample = CapacityWindowSample {
        concurrency: f64::NAN,
        exposure_seconds: f64::NAN,
        completed_attempts: 0,
    };
    match evidence {
        CapacityEvidenceSample::None => (CapacityEvidenceKind::None, EMPTY),
        CapacityEvidenceSample::Window(after) => (CapacityEvidenceKind::Window, after),
    }
}

/// A workload graph composed with the production controller transition.
pub struct ClosedLoop<Workload> {
    workload: Workload,
    configuration: ClosedLoopConfiguration,
    capacity_grid: CapacityGrid,
    state: ScaleState,
    scratch: ScaleScratch,
    observation: ObservationBuffer,
    arrival_counts: Vec<u32>,
    generated_counts: Vec<u32>,
    arrival_evidence_sample: ArrivalEvidenceSample,
    partition_evidence_accepted: bool,
    partition_posterior_values: Vec<f64>,
    partition_posterior_probabilities: Vec<f64>,
    partition_order_scratch: Vec<usize>,
    snapshot_pipeline: Option<SnapshotPipeline>,
    event_count: u32,
    budget_micros: u64,
    latest_capacity_window: Option<CapacityWindow>,
    capacity_evidence_sample: CapacityEvidenceSample,
    completion_posterior_scratch: Vec<CompletionPosteriorCell>,
    capacity_transition_scratch: Vec<OccupancyTransition>,
    inflight_transitions: Vec<PendingTransition>,
    ready_transitions: Vec<PendingTransition>,
    pending_transition_observations: VecDeque<PendingTransitionObservation>,
    lead_time_evidence_sample: LeadTimeEvidenceSample,
    trace: ControllerTrace,
    diagnostic_seed: u64,
}

/// Controller configuration with capacity for every partition and demand class.
///
/// The cohort bound always covers one aggregate cohort per class and partition.
#[derive(Clone)]
struct ClosedLoopConfiguration(Configuration);

impl ClosedLoopConfiguration {
    fn new(mut configuration: Configuration) -> Result<Self, ConfigurationError> {
        let required = configuration
            .partition_count
            .checked_mul(DemandClass::COUNT)
            .ok_or(ConfigurationError::PlatformLimit)?;
        configuration.cohort_count_max = configuration.cohort_count_max.max(required);
        configuration.validate()?;
        Ok(Self(configuration))
    }

    const fn core(&self) -> &Configuration {
        &self.0
    }
}

#[derive(Clone, Copy)]
struct CapacityWindow {
    concurrency: f64,
    exposure_seconds: f64,
    completed_attempts: u32,
    started_attempts: u32,
}

/// One aggregate-readiness observation segment.
///
/// The controller observes only published and ready replica counts. A segment
/// measures when its target first became ready after its anchor was demanded.
/// It never attributes readiness to a specific cohort.
#[derive(Clone, Copy)]
struct PendingTransition {
    from_replicas: u32,
    target_replicas: u32,
    requested_at_micros: u64,
}

struct PendingTransitionObservation {
    launch: Option<PendingLaunchObservation>,
    rebalance: Option<RebalanceEvidence>,
    sample: LeadTimeEvidenceSample,
}

#[derive(Clone, Copy)]
struct PendingLaunchObservation {
    requested_at: ModelTime,
    requested_delta: u32,
    observed_at: ModelTime,
    lump: ReadinessLump,
}

#[derive(Clone, Copy)]
struct CapacityPrediction {
    quantiles: [f64; 3],
    rank: f64,
}

#[derive(Clone, Copy)]
struct ArrivalPrediction {
    quantiles: [f64; 3],
    rank: f64,
}

#[derive(Clone, Copy)]
struct PartitionPrediction {
    evidence_count: u32,
    covered_counts: [u32; 4],
    rank_counts: [u32; HANDLER_RANK_BIN_COUNT],
    log_loss_sum: f64,
    entropy_sum: f64,
}

#[derive(Clone, Copy)]
struct LeadTimePrediction {
    quantiles: [f64; 3],
    rank: f64,
}

impl LeadTimePrediction {
    const fn missing() -> Self {
        Self {
            quantiles: [f64::NAN; 3],
            rank: f64::NAN,
        }
    }
}

impl PartitionPrediction {
    const fn missing() -> Self {
        Self {
            evidence_count: 0,
            covered_counts: [0; 4],
            rank_counts: [0; HANDLER_RANK_BIN_COUNT],
            log_loss_sum: 0.0_f64,
            entropy_sum: 0.0_f64,
        }
    }
}

struct SnapshotPipeline {
    channel: SnapshotChannel,
    table: SnapshotTable,
    cursor: SnapshotCursor,
    sequence: u64,
    incarnation: u64,
    cumulative_arrivals: u64,
}

impl CapacityWindow {
    fn evidence(self) -> Result<ResourceWindow, PlantError> {
        Ok(ResourceWindow::new_with_starts(
            self.concurrency,
            self.exposure_seconds,
            self.completed_attempts,
            self.started_attempts,
        )?)
    }

    const fn sample(self) -> CapacityWindowSample {
        CapacityWindowSample {
            concurrency: self.concurrency,
            exposure_seconds: self.exposure_seconds,
            completed_attempts: self.completed_attempts,
        }
    }
}

impl CapacityPrediction {
    const fn missing() -> Self {
        Self {
            quantiles: [f64::NAN; 3],
            rank: f64::NAN,
        }
    }
}

impl ArrivalPrediction {
    const fn missing() -> Self {
        Self {
            quantiles: [f64::NAN; 3],
            rank: f64::NAN,
        }
    }
}

impl<Workload: TickGenerator> ClosedLoop<Workload> {
    /// Allocates the controller adapter and all bounded scratch columns.
    ///
    /// # Errors
    ///
    /// Returns an error when a controller or simulator bound is invalid.
    pub fn new(
        workload: Workload,
        configuration: &Configuration,
        capacity_grid: CapacityGrid,
        trace_count_max: u32,
    ) -> Result<Self, ClosedLoopError> {
        let producer_count_max = workload.scheduled_release_count_max();
        if producer_count_max == 0 || producer_count_max > configuration.scheduled_release_count_max
        {
            return Err(ClosedLoopError::ScheduledReleaseCertification {
                producer_count_max,
                configured_count_max: configuration.scheduled_release_count_max,
            });
        }
        let configuration = ClosedLoopConfiguration::new(configuration.clone())?;
        let core_configuration = configuration.core();
        let partition_count = usize::try_from(core_configuration.partition_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let budget_micros = core_configuration.objective.budget_micros();
        let state = ScaleState::new(core_configuration.clone(), capacity_grid.clone())?;
        let trace = ControllerTrace::new(trace_count_max, &state)?;
        let throughput_posterior_count = usize::try_from(state.throughput_posterior_value_count())
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_posterior_count = trace.partition_share_posterior.values.len();
        let transition_capacity =
            usize::try_from(trace_count_max).map_err(|_| ConfigurationError::PlatformLimit)?;
        let capacity_transition_count = usize::try_from(
            core_configuration
                .resource_window_attempt_count_max
                .checked_mul(2)
                .and_then(|count| count.checked_add(1))
                .ok_or(ConfigurationError::PlatformLimit)?,
        )
        .map_err(|_| ConfigurationError::PlatformLimit)?;
        let scratch = state.new_scratch()?;
        let observation = ObservationBuffer::new(core_configuration)?;
        Ok(Self {
            workload,
            configuration,
            capacity_grid,
            state,
            scratch,
            observation,
            arrival_counts: vec![0; partition_count],
            generated_counts: vec![0; partition_count],
            arrival_evidence_sample: ArrivalEvidenceSample::None,
            partition_evidence_accepted: false,
            partition_posterior_values: vec![0.0_f64; partition_posterior_count],
            partition_posterior_probabilities: vec![0.0_f64; partition_posterior_count],
            partition_order_scratch: (0..partition_posterior_count).collect(),
            snapshot_pipeline: None,
            event_count: 0,
            budget_micros,
            latest_capacity_window: None,
            capacity_evidence_sample: CapacityEvidenceSample::None,
            completion_posterior_scratch: vec![
                CompletionPosteriorCell::default();
                throughput_posterior_count
            ],
            capacity_transition_scratch: Vec::with_capacity(capacity_transition_count),
            inflight_transitions: Vec::with_capacity(transition_capacity),
            ready_transitions: Vec::with_capacity(transition_capacity),
            pending_transition_observations: VecDeque::with_capacity(transition_capacity),
            lead_time_evidence_sample: LeadTimeEvidenceSample::None,
            trace,
            diagnostic_seed: 0,
        })
    }

    /// Sets the deterministic seed for randomized diagnostic ranks.
    #[must_use]
    pub(crate) const fn with_diagnostic_seed(mut self, seed: u64) -> Self {
        self.diagnostic_seed = seed;
        self
    }

    /// Routes arrival evidence through a bounded snapshot transport.
    ///
    /// # Errors
    ///
    /// Returns an error when the delivery bound is zero or unsupported.
    pub fn with_snapshot_transport(
        mut self,
        delivery_count_max: u32,
        fault: FaultPattern,
    ) -> Result<Self, PlantError> {
        self.snapshot_pipeline = Some(SnapshotPipeline {
            channel: SnapshotChannel::new(delivery_count_max, fault)?,
            table: SnapshotTable::new(1)?,
            cursor: SnapshotCursor::new(1)?,
            sequence: 0,
            incarnation: 1,
            cumulative_arrivals: 0,
        });
        Ok(self)
    }

    /// Returns every retained controller decision.
    #[must_use]
    pub const fn trace(&self) -> &ControllerTrace {
        &self.trace
    }

    /// Consumes the adapter and returns its controller trace.
    #[must_use]
    pub fn into_trace(self) -> ControllerTrace {
        self.trace
    }

    pub(crate) fn into_parts(self) -> (ControllerTrace, Workload) {
        (self.trace, self.workload)
    }

    fn prepare_observation(
        &mut self,
        context: &TickContext<'_>,
        inputs: TickInputs,
        reporter: ReporterDirective,
        calendar: Option<&CalendarForecastInput>,
        scheduled_releases: &ScheduledReleasesInput,
    ) -> Result<(), PlantError>
    where
        Workload: TickGenerator,
    {
        self.observation.clear();
        self.observation
            .advance_model_time(ModelTime::from_micros(context.now_micros))?;
        let active_transition = if context.plant.partitions_ready {
            None
        } else {
            self.inflight_transitions
                .iter()
                .rposition(|pending| pending.target_replicas == context.plant.replicas)
        };
        let ready_replicas = active_transition.map_or(context.plant.replicas, |index| {
            self.inflight_transitions[index].from_replicas
        });
        self.observation.set_current_replicas(ready_replicas)?;
        if let Some(calendar) = calendar {
            self.observation.set_calendar_forecast(
                calendar.artifact(),
                calendar.prior_probability(),
                calendar.segments(),
            )?;
        }
        self.observation
            .set_scheduled_releases(scheduled_releases.releases())?;
        for (index, pending) in self.inflight_transitions.iter().enumerate() {
            if pending.reached(context.plant.replicas) && Some(index) != active_transition {
                continue;
            }
            let requested_at = ModelTime::from_micros(pending.requested_at_micros);
            let commitment = active_transition
                .filter(|active| *active == index)
                .and(context.plant.reconciliation_started_micros)
                .map_or_else(
                    || {
                        ActuationCommitment::launching(
                            pending.from_replicas,
                            pending.target_replicas,
                            requested_at,
                        )
                    },
                    |started_at| {
                        ActuationCommitment::rebalancing(
                            pending.from_replicas,
                            pending.target_replicas,
                            requested_at,
                            ModelTime::from_micros(started_at),
                        )
                    },
                )?;
            self.observation.push_actuation_commitment(commitment)?;
        }
        self.latest_capacity_window = None;
        self.capacity_evidence_sample = CapacityEvidenceSample::None;
        self.arrival_counts.fill(0);
        self.generated_counts.fill(0);
        self.arrival_evidence_sample = ArrivalEvidenceSample::None;
        self.partition_evidence_accepted = false;
        self.lead_time_evidence_sample = LeadTimeEvidenceSample::None;
        self.count_generated(
            context,
            inputs,
            crate::EventSource::Message,
            inputs.message_count,
        )?;
        self.count_generated(
            context,
            inputs,
            crate::EventSource::Timer,
            inputs.timer_count,
        )?;
        self.prepare_arrival_evidence(context, reporter)?;
        self.push_backlog_cohorts(context)?;
        self.prepare_attempt_outcomes(context)?;
        self.prepare_capacity_evidence(context)?;
        self.prepare_transition_evidence(context)?;
        Ok(())
    }

    fn prepare_arrival_evidence(
        &mut self,
        context: &TickContext<'_>,
        reporter: ReporterDirective,
    ) -> Result<(), PlantError> {
        if reporter == ReporterDirective::ReplaceAggregator {
            self.replace_aggregator()?;
        }
        let Some(pipeline) = &mut self.snapshot_pipeline else {
            let exposure_micros = context
                .history
                .now_micros(0)
                .map_or(0, |previous| context.now_micros.saturating_sub(previous));
            if exposure_micros > 0 {
                self.observation
                    .set_partition_arrivals(&self.arrival_counts, exposure_micros)?;
                self.partition_evidence_accepted = true;
                let count = self.arrival_counts.iter().try_fold(0_u32, |total, count| {
                    total.checked_add(*count).ok_or(PlantError::PlatformLimit)
                })?;
                self.arrival_evidence_sample =
                    ArrivalEvidenceSample::Accepted(ArrivalWindowSample {
                        count,
                        exposure_seconds: Duration::from_micros(exposure_micros).as_secs_f64(),
                    });
            }
            return Ok(());
        };
        let arrivals = self.arrival_counts.iter().fold(0_u64, |total, count| {
            total.saturating_add(u64::from(*count))
        });
        pipeline.cumulative_arrivals = pipeline.cumulative_arrivals.saturating_add(arrivals);
        match reporter {
            ReporterDirective::Send => {}
            ReporterDirective::Missing => {
                pipeline
                    .channel
                    .deliver(context.now_micros, &mut pipeline.table);
                return Ok(());
            }
            ReporterDirective::Restart => {
                pipeline.incarnation = pipeline.incarnation.saturating_add(1);
                pipeline.sequence = 0;
                pipeline.cumulative_arrivals = arrivals;
            }
            ReporterDirective::ReplaceAggregator => {
                pipeline.table.clear();
                pipeline.cursor.clear();
            }
        }
        pipeline.sequence = pipeline.sequence.saturating_add(1);
        pipeline.channel.send(Snapshot {
            sender: 0,
            incarnation: pipeline.incarnation,
            sequence: pipeline.sequence,
            observed_at_micros: context.now_micros,
            arrival_count: pipeline.cumulative_arrivals,
        })?;
        pipeline
            .channel
            .deliver(context.now_micros, &mut pipeline.table);
        if let Some(interval) = pipeline.cursor.next(0, &pipeline.table) {
            let count = u32::try_from(interval.count).map_err(|_| PlantError::PlatformLimit)?;
            self.observation
                .set_arrivals(count, interval.exposure_micros)?;
            self.arrival_evidence_sample = ArrivalEvidenceSample::Accepted(ArrivalWindowSample {
                count,
                exposure_seconds: Duration::from_micros(interval.exposure_micros).as_secs_f64(),
            });
        }
        Ok(())
    }

    fn replace_aggregator(&mut self) -> Result<(), PlantError> {
        self.state = ScaleState::new(
            self.configuration.core().clone(),
            self.capacity_grid.clone(),
        )?;
        self.latest_capacity_window = None;
        self.capacity_evidence_sample = CapacityEvidenceSample::None;
        self.inflight_transitions.clear();
        self.ready_transitions.clear();
        self.pending_transition_observations.clear();
        self.lead_time_evidence_sample = LeadTimeEvidenceSample::None;
        Ok(())
    }

    fn prepare_transition_evidence(&mut self, context: &TickContext<'_>) -> Result<(), PlantError> {
        let mut index = 0_usize;
        while index < self.inflight_transitions.len() {
            if !self.inflight_transitions[index].reached(context.plant.replicas) {
                index += 1;
                continue;
            }
            if self.ready_transitions.len() == self.ready_transitions.capacity() {
                return Err(PlantError::ChangeCapacity);
            }
            self.ready_transitions
                .push(self.inflight_transitions.remove(index));
        }
        if let Some(pending) = self.inflight_transitions.iter_mut().find(|pending| {
            pending.direction() == TransitionDirection::Up
                && pending.from_replicas < context.plant.replicas
                && pending.target_replicas > context.plant.replicas
        }) {
            pending.from_replicas = context.plant.replicas;
        }
        if context.plant.partitions_ready {
            let completed_micros = context
                .plant
                .reconciliation_completed_micros
                .map_or(context.now_micros, |completed| completed);
            while !self.ready_transitions.is_empty() {
                let transition = self.ready_transitions.remove(0);
                let elapsed_micros =
                    completed_micros.saturating_sub(transition.requested_at_micros);
                if elapsed_micros == 0 {
                    continue;
                }
                let rebalance_started = context
                    .plant
                    .reconciliation_started_micros
                    .filter(|started| *started > transition.requested_at_micros)
                    .filter(|started| completed_micros > *started);
                let launch_completed_micros =
                    rebalance_started.map_or(completed_micros, |started| started);
                let launch = if transition.direction() == TransitionDirection::Up {
                    let observation = ReadinessObservation::ready(
                        ModelTime::from_micros(transition.requested_at_micros),
                        ModelTime::from_micros(launch_completed_micros),
                    )?;
                    Some(PendingLaunchObservation {
                        requested_at: ModelTime::from_micros(transition.requested_at_micros),
                        requested_delta: transition.replica_delta(),
                        observed_at: ModelTime::from_micros(launch_completed_micros),
                        lump: ReadinessLump::new(
                            ReadinessGroupId(
                                transition.requested_at_micros
                                    ^ u64::from(transition.target_replicas).rotate_left(32),
                            ),
                            transition.replica_delta(),
                            observation,
                        )?,
                    })
                } else {
                    None
                };
                let rebalance = rebalance_started
                    .map(|started| {
                        RebalanceEvidence::completed(
                            ModelTime::from_micros(started),
                            ModelTime::from_micros(completed_micros),
                        )
                    })
                    .transpose()?;
                self.push_transition_observation(PendingTransitionObservation {
                    launch,
                    rebalance,
                    sample: LeadTimeEvidenceSample::Completed {
                        direction: transition.direction(),
                        replica_delta: transition.replica_delta(),
                        elapsed_seconds: Duration::from_micros(
                            launch_completed_micros.saturating_sub(transition.requested_at_micros),
                        )
                        .as_secs_f64(),
                    },
                })?;
            }
        }
        if let Some(pending) = self.pending_transition_observations.pop_front() {
            if let Some(launch) = pending.launch {
                self.observation.set_launch_evidence(
                    launch.requested_at,
                    launch.requested_delta,
                    launch.observed_at,
                    slice::from_ref(&launch.lump),
                )?;
            }
            if let Some(rebalance) = pending.rebalance {
                self.observation.set_rebalance_evidence(rebalance)?;
            }
            self.lead_time_evidence_sample = pending.sample;
        }
        Ok(())
    }

    fn prepare_attempt_outcomes(&mut self, context: &TickContext<'_>) -> Result<(), PlantError> {
        let Some(previous_normal_successes) = context.history.normal_successes(0) else {
            return Ok(());
        };
        let Some(previous_normal_transient) = context.history.normal_transient_failures(0) else {
            return Ok(());
        };
        let Some(previous_normal_terminal) = context.history.normal_terminal_failures(0) else {
            return Ok(());
        };
        let Some(previous_normal_permanent) = context.history.normal_permanent_failures(0) else {
            return Ok(());
        };
        let Some(previous_failure_successes) = context.history.failure_successes(0) else {
            return Ok(());
        };
        let Some(previous_failure_transient) = context.history.failure_transient_failures(0) else {
            return Ok(());
        };
        let Some(previous_failure_terminal) = context.history.failure_terminal_failures(0) else {
            return Ok(());
        };
        let Some(previous_failure_permanent) = context.history.failure_permanent_failures(0) else {
            return Ok(());
        };
        let normal_success = context
            .plant
            .normal_successes
            .saturating_sub(previous_normal_successes);
        let normal_transient = context
            .plant
            .normal_transient_failures
            .saturating_sub(previous_normal_transient);
        let normal_terminal = context
            .plant
            .normal_terminal_failures
            .saturating_sub(previous_normal_terminal);
        let normal_permanent = context
            .plant
            .normal_permanent_failures
            .saturating_sub(previous_normal_permanent);
        let failure_success = context
            .plant
            .failure_successes
            .saturating_sub(previous_failure_successes);
        let failure_transient = context
            .plant
            .failure_transient_failures
            .saturating_sub(previous_failure_transient);
        let failure_terminal = context
            .plant
            .failure_terminal_failures
            .saturating_sub(previous_failure_terminal);
        let failure_permanent = context
            .plant
            .failure_permanent_failures
            .saturating_sub(previous_failure_permanent);
        let evidence = AttemptOutcomeEvidence::new(
            AttemptOutcomeCounts::new(
                normal_success,
                normal_permanent,
                normal_transient,
                normal_terminal,
            ),
            AttemptOutcomeCounts::new(
                failure_success,
                failure_permanent,
                failure_transient,
                failure_terminal,
            ),
        );
        self.observation.set_attempt_outcomes(evidence)?;
        Ok(())
    }

    /// Adds a measured resource window between two plant states.
    ///
    /// Two consecutive plant snapshots bracket the window: the transition
    /// log between their recorded counts is exactly the evidence between the
    /// handler samples, however boundary-time ties order against a sample.
    /// A tie clamps to offset zero or to the full exposure. Only a window
    /// that spans exactly one report interval is a certified report; a tick
    /// at any other spacing omits the observation.
    fn prepare_capacity_evidence(&mut self, context: &TickContext<'_>) -> Result<(), PlantError> {
        let Some(previous_micros) = context.history.now_micros(0) else {
            return Ok(());
        };
        let exposure_micros = context.now_micros.saturating_sub(previous_micros);
        if exposure_micros != self.configuration.core().report_interval_micros {
            return Ok(());
        }
        let Some(initial_busy_slots) = context.history.active_handlers(0) else {
            return Ok(());
        };
        let Some(previous_count) = context.history.attempt_transition_count(0) else {
            return Ok(());
        };
        let window_transitions = context
            .attempt_transitions
            .get(previous_count..context.plant.attempt_transition_count)
            .unwrap_or(&[]);
        self.capacity_transition_scratch.clear();
        for transition in window_transitions.iter().copied() {
            let offset_micros = transition
                .at_micros
                .saturating_sub(previous_micros)
                .min(exposure_micros);
            if let Some(group) = self.capacity_transition_scratch.last_mut()
                && group.offset_micros() == offset_micros
            {
                let completed = group
                    .completed_attempts()
                    .saturating_add(u32::from(matches!(
                        transition.kind,
                        crate::AttemptTransitionKind::Completion
                    )));
                let started = group.started_attempts().saturating_add(u32::from(matches!(
                    transition.kind,
                    crate::AttemptTransitionKind::Start
                )));
                *group = OccupancyTransition::new(offset_micros, completed, started);
            } else {
                if self.capacity_transition_scratch.len()
                    == self.capacity_transition_scratch.capacity()
                {
                    return Err(PlantError::MetricCapacity);
                }
                self.capacity_transition_scratch
                    .push(OccupancyTransition::new(
                        offset_micros,
                        u32::from(matches!(
                            transition.kind,
                            crate::AttemptTransitionKind::Completion
                        )),
                        u32::from(matches!(
                            transition.kind,
                            crate::AttemptTransitionKind::Start
                        )),
                    ));
            }
        }
        let completed_attempts = self
            .capacity_transition_scratch
            .iter()
            .map(|group| group.completed_attempts())
            .fold(0_u32, u32::saturating_add);
        let started_attempts = self
            .capacity_transition_scratch
            .iter()
            .map(|group| group.started_attempts())
            .fold(0_u32, u32::saturating_add);
        let mut final_busy_slots = initial_busy_slots;
        for group in &self.capacity_transition_scratch {
            final_busy_slots = final_busy_slots
                .checked_sub(group.completed_attempts())
                .and_then(|state| state.checked_add(group.started_attempts()))
                .ok_or(PlantError::MetricCapacity)?;
        }
        let previous_occupancy = context.history.handler_occupancy_micros(0).unwrap_or(0);
        let occupancy = context
            .plant
            .handler_occupancy_micros
            .saturating_sub(previous_occupancy);
        let exposure_seconds = Duration::from_micros(exposure_micros).as_secs_f64();
        let current = CapacityWindow {
            concurrency: Duration::from_micros(occupancy).as_secs_f64() / exposure_seconds,
            exposure_seconds,
            completed_attempts,
            started_attempts,
        };
        self.latest_capacity_window = Some(current);
        self.observation.set_resource_observation(
            current.evidence()?,
            initial_busy_slots,
            final_busy_slots,
            &self.capacity_transition_scratch,
        )?;
        self.capacity_evidence_sample = CapacityEvidenceSample::Window(current.sample());
        Ok(())
    }

    fn count_generated(
        &mut self,
        context: &TickContext<'_>,
        inputs: TickInputs,
        source: crate::EventSource,
        count: u32,
    ) -> Result<(), PlantError>
    where
        Workload: TickGenerator,
    {
        for event_offset in 0..count {
            let event_index = self.event_count.saturating_add(event_offset);
            let event = self.workload.event(EventContext {
                tick: *context,
                inputs,
                event_offset,
                event_index,
                partition_count: self.arrival_counts.len() as u32,
                key_count: 1,
                source,
            })?;
            let partition =
                usize::try_from(event.partition).map_err(|_| PlantError::PlatformLimit)?;
            let Some(generated) = self.generated_counts.get_mut(partition) else {
                return Err(PlantError::PartitionIndex);
            };
            *generated = generated.saturating_add(1);
            if source == crate::EventSource::Message {
                self.arrival_counts[partition] = self.arrival_counts[partition].saturating_add(1);
            }
        }
        self.event_count = self.event_count.saturating_add(count);
        Ok(())
    }

    fn push_backlog_cohorts(&mut self, context: &TickContext<'_>) -> Result<(), PlantError> {
        for partition in 0..self.generated_counts.len() {
            let Some(normal) = context.normal_backlog.get(partition) else {
                return Err(PlantError::PartitionIndex);
            };
            if normal.count > 0 {
                self.observation.set_backlog(BacklogCohort::new(
                    context.now_micros,
                    normal.oldest_release_micros,
                    normal.count,
                    partition as u32,
                    DemandClass::Normal,
                )?)?;
            }
            let generated = self.generated_counts[partition];
            if generated > 0 {
                self.observation.push_cohort(Cohort {
                    release_micros: context.now_micros,
                    deadline_micros: context.now_micros.saturating_add(self.budget_micros),
                    offered_events: f64::from(generated),
                    partition: partition as u32,
                    demand_class: DemandClass::Normal,
                })?;
            }

            let Some(failure) = context.failure_backlog.get(partition) else {
                return Err(PlantError::PartitionIndex);
            };
            if failure.count == 0 {
                continue;
            }
            let scheduled_release = failure.release_micros;
            let release_micros = scheduled_release.max(context.now_micros);
            let deadline_micros = scheduled_release
                .saturating_add(self.budget_micros)
                .max(release_micros.saturating_add(1));
            self.observation.push_cohort(Cohort {
                release_micros,
                deadline_micros,
                offered_events: f64::from(failure.count),
                partition: partition as u32,
                demand_class: DemandClass::Failure,
            })?;
        }
        Ok(())
    }

    fn apply_decision(
        &mut self,
        context: &TickContext<'_>,
        mut inputs: TickInputs,
        reporter: ReporterDirective,
    ) -> Result<TickInputs, PlantError> {
        let arrival_predictive = self.arrival_prediction(context.now_micros)?;
        let partition_predictive = self.partition_prediction(context.now_micros)?;
        let lead_time_predictive = self.lead_time_prediction()?;
        let capacity_predictive = self.capacity_prediction(context.now_micros)?;
        let decision = step(
            &mut self.state,
            &mut self.scratch,
            self.observation.observation(),
            ModelTime::from_micros(context.now_micros),
        );
        let desired = context
            .history
            .desired_replicas(0)
            .unwrap_or(context.plant.replicas);
        let (held_target, held_cap) = self.held_target_and_cap(context.plant.replicas);
        let external_scale = matches!(
            inputs.scale,
            ScaleDirective::ExternalHold | ScaleDirective::Request { .. }
        );
        let (resource_concurrency, attempt_throughput_per_second) = self.latest_capacity_rates();
        let (diagnostics, target, cap, hold, hold_reason) = match decision {
            ScaleDecision::Apply(apply) => {
                if !external_scale {
                    inputs.scale = if apply.target == desired {
                        ScaleDirective::Hold
                    } else {
                        ScaleDirective::Request {
                            replicas: apply.target,
                        }
                    };
                }
                (apply.diagnostics, apply.target, apply.cap, false, None)
            }
            ScaleDecision::Hold(held) => {
                if !external_scale {
                    inputs.scale = ScaleDirective::Hold;
                }
                (
                    held.diagnostics,
                    held_target,
                    held_cap,
                    true,
                    Some(held.reason),
                )
            }
        };
        let sample = ControllerSample {
            at_micros: context.now_micros,
            scenario_count: diagnostics.scenario_count,
            target,
            cap,
            hold,
            hold_reason,
            shortfall: diagnostics.shortfall,
            expected_loss: diagnostics.expected_loss,
            selected_late_area_mean: f64::NAN,
            selected_replica_seconds_mean: f64::NAN,
            selected_cost: f64::NAN,
            runner_up_action_index: u32::MAX,
            runner_up_late_area_mean: f64::NAN,
            runner_up_replica_seconds_mean: f64::NAN,
            runner_up_cost: f64::NAN,
            demand_floor: u32::MAX,
            arrival_rate_per_second: diagnostics.arrival_rate_per_second,
            arrival_evidence: self.arrival_evidence_sample,
            arrival_predictive_low_count: arrival_predictive.quantiles[0],
            arrival_predictive_median_count: arrival_predictive.quantiles[1],
            arrival_predictive_high_count: arrival_predictive.quantiles[2],
            arrival_predictive_rank: arrival_predictive.rank,
            partition_evidence_count: partition_predictive.evidence_count,
            partition_predictive_covered_counts: partition_predictive.covered_counts,
            partition_predictive_rank_counts: partition_predictive.rank_counts,
            partition_log_loss_sum: partition_predictive.log_loss_sum,
            partition_entropy_sum: partition_predictive.entropy_sum,
            lead_time_evidence: self.lead_time_evidence_sample,
            lead_time_predictive_low_seconds: lead_time_predictive.quantiles[0],
            lead_time_predictive_median_seconds: lead_time_predictive.quantiles[1],
            lead_time_predictive_high_seconds: lead_time_predictive.quantiles[2],
            lead_time_predictive_rank: lead_time_predictive.rank,
            capacity_per_second: diagnostics.capacity_per_second,
            capacity_low_per_second: diagnostics.capacity_low_per_second,
            capacity_median_per_second: diagnostics.capacity_median_per_second,
            capacity_high_per_second: diagnostics.capacity_high_per_second,
            saturation_probability: diagnostics.saturation_probability,
            no_knee_probability: diagnostics.no_knee_probability,
            lead_time_up_seconds: diagnostics.lead_time_up_seconds,
            lead_time_down_seconds: diagnostics.lead_time_down_seconds,
            lead_time_seconds: diagnostics.lead_time_seconds,
            resource_concurrency,
            attempt_throughput_per_second,
            capacity_evidence: self.capacity_evidence_sample,
            capacity_predictive_low_per_second: capacity_predictive.quantiles[0],
            capacity_predictive_median_per_second: capacity_predictive.quantiles[1],
            capacity_predictive_high_per_second: capacity_predictive.quantiles[2],
            capacity_predictive_rank: capacity_predictive.rank,
            reporter,
        };
        self.trace.push(&sample, &self.state, &self.scratch)?;
        Ok(inputs)
    }

    fn held_target_and_cap(&self, current_replicas: u32) -> (u32, u32) {
        let held_target = (0..self.trace.len())
            .rev()
            .find_map(|index| self.trace.sample(index).filter(|sample| !sample.hold))
            .map_or(current_replicas, |sample| sample.target);
        let held_cap = (0..self.trace.len())
            .rev()
            .find_map(|index| self.trace.sample(index).map(|sample| sample.cap))
            .filter(|cap| *cap > 0)
            .map_or(self.configuration.core().replica_count_max, |cap| cap);
        (held_target, held_cap)
    }

    fn latest_capacity_rates(&self) -> (f64, f64) {
        self.latest_capacity_window
            .map_or((f64::NAN, f64::NAN), |window| {
                (
                    window.concurrency,
                    f64::from(window.completed_attempts) / window.exposure_seconds,
                )
            })
    }

    fn arrival_prediction(&self, now_micros: u64) -> Result<ArrivalPrediction, PlantError> {
        let ArrivalEvidenceSample::Accepted(window) = self.arrival_evidence_sample else {
            return Ok(ArrivalPrediction::missing());
        };
        let predictive = self
            .state
            .arrival_count_predictive(window.count, window.exposure_seconds)?;
        let quantiles = predictive.quantiles.map(count_f64);
        let rank_offset = arrival_predictive_rank_offset(self.diagnostic_seed, now_micros);
        Ok(ArrivalPrediction {
            quantiles,
            rank: predictive.lower_cdf
                + rank_offset * (predictive.upper_cdf - predictive.lower_cdf),
        })
    }

    fn partition_prediction(&mut self, now_micros: u64) -> Result<PartitionPrediction, PlantError> {
        if !self.partition_evidence_accepted {
            return Ok(PartitionPrediction::missing());
        }
        let evidence_count = self
            .arrival_counts
            .iter()
            .copied()
            .fold(0_u32, u32::saturating_add);
        if evidence_count == 0 {
            return Ok(PartitionPrediction::missing());
        }
        self.state.write_posterior(
            PosteriorQuery::PartitionShare,
            &mut self.partition_posterior_values,
            &mut self.partition_posterior_probabilities,
        )?;
        for (index, order) in self.partition_order_scratch.iter_mut().enumerate() {
            *order = index;
        }
        self.partition_order_scratch
            .sort_unstable_by(|left, right| {
                self.partition_posterior_probabilities[*right]
                    .total_cmp(&self.partition_posterior_probabilities[*left])
            });
        let mut covered_counts = [0_u32; 4];
        for (level_index, level) in HANDLER_COVERAGE_LEVELS.into_iter().enumerate() {
            let mut mass = 0.0_f64;
            for &partition in &self.partition_order_scratch {
                covered_counts[level_index] =
                    covered_counts[level_index].saturating_add(self.arrival_counts[partition]);
                mass += self.partition_posterior_probabilities[partition];
                if mass >= level {
                    break;
                }
            }
        }
        let mut rank_counts = [0_u32; HANDLER_RANK_BIN_COUNT];
        let mut cumulative = 0.0_f64;
        let mut log_loss_sum = 0.0_f64;
        for (partition, (&probability, &count)) in self
            .partition_posterior_probabilities
            .iter()
            .zip(&self.arrival_counts)
            .enumerate()
        {
            log_loss_sum -= f64::from(count) * probability.ln();
            let mut random = RandomStream::new(self.diagnostic_seed).domain(
                0x7061_7274_6974_696f_u64
                    ^ now_micros
                    ^ u64::try_from(partition).map_or(u64::MAX, |value| value),
            );
            for _ in 0..count {
                let rank = cumulative + random.open_unit_f64() * probability;
                let bin = predictive_rank_bin(rank);
                rank_counts[bin] = rank_counts[bin].saturating_add(1);
            }
            cumulative += probability;
        }
        let entropy = self
            .partition_posterior_probabilities
            .iter()
            .filter(|probability| **probability > 0.0_f64)
            .map(|probability| -probability * probability.ln())
            .sum::<f64>();
        Ok(PartitionPrediction {
            evidence_count,
            covered_counts,
            rank_counts,
            log_loss_sum,
            entropy_sum: entropy * f64::from(evidence_count),
        })
    }

    fn lead_time_prediction(&self) -> Result<LeadTimePrediction, PlantError> {
        let (direction, replica_delta, elapsed_seconds) = match self.lead_time_evidence_sample {
            LeadTimeEvidenceSample::None => return Ok(LeadTimePrediction::missing()),
            LeadTimeEvidenceSample::Completed {
                direction,
                replica_delta,
                elapsed_seconds,
            } => (direction, replica_delta, Some(elapsed_seconds)),
            LeadTimeEvidenceSample::Censored {
                direction,
                replica_delta,
                ..
            } => (direction, replica_delta, None),
        };
        Ok(LeadTimePrediction {
            quantiles: [
                self.state
                    .lead_time_predictive_quantile(direction, replica_delta, 0.1_f64)?,
                self.state
                    .lead_time_predictive_quantile(direction, replica_delta, 0.5_f64)?,
                self.state
                    .lead_time_predictive_quantile(direction, replica_delta, 0.9_f64)?,
            ],
            rank: elapsed_seconds.map_or(f64::NAN, |elapsed| {
                self.state
                    .lead_time_predictive_cdf(direction, replica_delta, elapsed)
            }),
        })
    }

    fn capacity_prediction(&mut self, now_micros: u64) -> Result<CapacityPrediction, PlantError> {
        let rank_offset = predictive_rank_offset(self.diagnostic_seed, now_micros);
        match self.capacity_evidence_sample {
            CapacityEvidenceSample::None => Ok(CapacityPrediction::missing()),
            CapacityEvidenceSample::Window(window) => {
                let evidence = self
                    .latest_capacity_window
                    .ok_or(PlantError::MetricCapacity)?
                    .evidence()?;
                self.state.write_completion_posterior(
                    &evidence,
                    &mut self.completion_posterior_scratch,
                )?;
                let quantiles = posterior_predictive_completion_quantiles(
                    &self.completion_posterior_scratch,
                    window.exposure_seconds,
                )?;
                let observed = u64::from(window.completed_attempts);
                let upper =
                    predictive_completion_cdf(&self.completion_posterior_scratch, observed)?;
                let lower = if observed == 0 {
                    0.0_f64
                } else {
                    predictive_completion_cdf(&self.completion_posterior_scratch, observed - 1)?
                };
                Ok(CapacityPrediction {
                    quantiles,
                    rank: lower + rank_offset * (upper - lower),
                })
            }
        }
    }

    fn track_scale_request(
        &mut self,
        context: &TickContext<'_>,
        replicas: u32,
        plant_in_flight: u32,
    ) -> Result<(), PlantError> {
        let ready = context.plant.replicas;
        if replicas <= ready {
            while let Some(pending) = self.inflight_transitions.pop() {
                self.record_censored_transition(context, pending)?;
            }
            if replicas < ready {
                self.push_pending_transition(PendingTransition {
                    from_replicas: ready,
                    target_replicas: replicas,
                    requested_at_micros: context.now_micros,
                })?;
            }
            self.assert_pending_up_segments(ready, replicas, plant_in_flight);
            return Ok(());
        }

        let mut index = 0_usize;
        while index < self.inflight_transitions.len() {
            if self.inflight_transitions[index].direction() == TransitionDirection::Down {
                let pending = self.inflight_transitions.remove(index);
                self.record_censored_transition(context, pending)?;
            } else {
                index += 1;
            }
        }
        while self.inflight_transitions.last().is_some_and(|pending| {
            pending.direction() == TransitionDirection::Up && pending.from_replicas >= replicas
        }) {
            let Some(pending) = self.inflight_transitions.pop() else {
                break;
            };
            self.record_censored_transition(context, pending)?;
        }
        if let Some(pending) = self.inflight_transitions.last_mut()
            && pending.direction() == TransitionDirection::Up
            && pending.target_replicas > replicas
        {
            pending.target_replicas = replicas;
        }

        let frontier = self
            .inflight_transitions
            .iter()
            .filter(|pending| pending.direction() == TransitionDirection::Up)
            .map(|pending| pending.target_replicas)
            .max()
            .map_or(ready, |target| target.max(ready));
        if replicas > frontier {
            self.push_pending_transition(PendingTransition {
                from_replicas: frontier,
                target_replicas: replicas,
                requested_at_micros: context.now_micros,
            })?;
        }
        self.assert_pending_up_segments(ready, replicas, plant_in_flight);
        Ok(())
    }

    fn push_pending_transition(&mut self, transition: PendingTransition) -> Result<(), PlantError> {
        if self.inflight_transitions.len() == self.inflight_transitions.capacity() {
            return Err(PlantError::ChangeCapacity);
        }
        self.inflight_transitions.push(transition);
        Ok(())
    }

    fn assert_pending_up_segments(&self, ready: u32, published: u32, plant_in_flight: u32) {
        let mut frontier = ready;
        let mut delta = 0_u32;
        for pending in self
            .inflight_transitions
            .iter()
            .filter(|pending| pending.direction() == TransitionDirection::Up)
        {
            assert_eq!(
                pending.from_replicas, frontier,
                "pending up segments must be disjoint and contiguous"
            );
            frontier = pending.target_replicas;
            delta = delta.saturating_add(pending.replica_delta());
        }
        if published >= ready {
            assert_eq!(
                frontier, published,
                "pending up segments must reach the published target"
            );
        }
        assert_eq!(
            delta, plant_in_flight,
            "pending up segment deltas must equal the plant in-flight count"
        );
    }

    fn record_censored_transition(
        &mut self,
        context: &TickContext<'_>,
        transition: PendingTransition,
    ) -> Result<(), PlantError> {
        let exposure_micros = context
            .now_micros
            .saturating_sub(transition.requested_at_micros);
        if exposure_micros == 0 {
            return Ok(());
        }
        let launch = if transition.direction() == TransitionDirection::Up {
            let requested_at = ModelTime::from_micros(transition.requested_at_micros);
            let observed_at = ModelTime::from_micros(context.now_micros);
            Some(PendingLaunchObservation {
                requested_at,
                requested_delta: transition.replica_delta(),
                observed_at,
                lump: ReadinessLump::new(
                    ReadinessGroupId(
                        transition.requested_at_micros
                            ^ u64::from(transition.target_replicas).rotate_left(32),
                    ),
                    transition.replica_delta(),
                    ReadinessObservation::pending(requested_at, observed_at)?,
                )?,
            })
        } else {
            None
        };
        self.push_transition_observation(PendingTransitionObservation {
            launch,
            rebalance: None,
            sample: LeadTimeEvidenceSample::Censored {
                direction: transition.direction(),
                replica_delta: transition.replica_delta(),
                exposure_seconds: Duration::from_micros(exposure_micros).as_secs_f64(),
            },
        })
    }

    fn push_transition_observation(
        &mut self,
        observation: PendingTransitionObservation,
    ) -> Result<(), PlantError> {
        if self.pending_transition_observations.len()
            == self.pending_transition_observations.capacity()
        {
            return Err(PlantError::ChangeCapacity);
        }
        self.pending_transition_observations.push_back(observation);
        Ok(())
    }
}

fn predictive_rank_bin(rank: f64) -> usize {
    (1..HANDLER_RANK_BIN_COUNT)
        .position(|boundary| {
            let boundary = u32::try_from(boundary).map_or(u32::MAX, |value| value);
            rank < f64::from(boundary) / 10.0_f64
        })
        .map_or(HANDLER_RANK_BIN_COUNT - 1, |bin| bin)
}

fn predictive_rank_offset(seed: u64, now_micros: u64) -> f64 {
    let mut random = RandomStream::new(0x6361_7061_6369_7479 ^ seed).domain(now_micros);
    random.open_unit_f64()
}

fn arrival_predictive_rank_offset(seed: u64, now_micros: u64) -> f64 {
    let mut random = RandomStream::new(0x6172_7269_7661_6c73 ^ seed).domain(now_micros);
    random.open_unit_f64()
}

fn count_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).map_or(0, |part| part);
    let low = u32::try_from(value & u64::from(u32::MAX)).map_or(0, |part| part);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

fn posterior_predictive_completion_quantiles(
    cells: &[CompletionPosteriorCell],
    exposure_seconds: f64,
) -> Result<[f64; 3], PlantError> {
    let maximum_mean = cells.iter().map(|cell| cell.mean).fold(0.0_f64, f64::max);
    let upper = (maximum_mean + 12.0_f64 * maximum_mean.sqrt() + 64.0_f64)
        .ceil()
        .clamp(1.0_f64, f64::from(u32::MAX)) as u64;
    let mut quantiles = [0.0_f64; 3];
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    for (index, threshold) in thresholds.into_iter().enumerate() {
        let mut low = 0_u64;
        let mut high = upper;
        while low < high {
            let middle = low + (high - low) / 2;
            if predictive_completion_cdf(cells, middle)? >= threshold {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        let count = u32::try_from(low).map_err(|_| PlantError::PlatformLimit)?;
        quantiles[index] = f64::from(count) / exposure_seconds;
    }
    Ok(quantiles)
}

fn predictive_completion_cdf(
    cells: &[CompletionPosteriorCell],
    completed_attempts: u64,
) -> Result<f64, PlantError> {
    let mut cumulative = 0.0_f64;
    for cell in cells {
        cumulative += cell.probability * poisson_cdf(cell.mean, completed_attempts)?;
    }
    Ok(cumulative)
}

#[cfg(test)]
fn posterior_predictive_throughput_quantiles(
    cells: &[ThroughputPosteriorCell],
    exposure_seconds: f64,
) -> Result<[f64; 3], PlantError> {
    let maximum_mean = cells
        .iter()
        .map(|cell| cell.throughput_per_second * exposure_seconds)
        .fold(0.0_f64, f64::max);
    let upper = (maximum_mean + 12.0_f64 * maximum_mean.sqrt() + 64.0_f64)
        .ceil()
        .clamp(1.0_f64, f64::from(u32::MAX)) as u64;
    let mut quantiles = [0.0_f64; 3];
    for (index, threshold) in [0.1_f64, 0.5_f64, 0.9_f64].into_iter().enumerate() {
        let mut low = 0_u64;
        let mut high = upper;
        while low < high {
            let middle = low + (high - low) / 2;
            if point_predictive_throughput_cdf(cells, exposure_seconds, middle)? >= threshold {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        quantiles[index] = f64::from(u32::try_from(low).map_err(|_| PlantError::PlatformLimit)?)
            / exposure_seconds;
    }
    Ok(quantiles)
}

#[cfg(test)]
fn predictive_throughput_cdf(
    cells: &[ThroughputPosteriorCell],
    exposure_seconds: f64,
    completed_attempts: u64,
) -> Result<f64, PlantError> {
    let mut cumulative = 0.0_f64;
    for cell in cells {
        let low = cell.throughput_low_per_second;
        let high = cell.throughput_high_per_second;
        let probability = if high > low && low > 0.0_f64 {
            log_uniform_predictive_throughput_cdf(low, high, exposure_seconds, completed_attempts)?
        } else {
            poisson_cdf(
                cell.throughput_per_second * exposure_seconds,
                completed_attempts,
            )?
        };
        cumulative += cell.probability * probability;
    }
    Ok(cumulative)
}

#[cfg(test)]
fn log_uniform_predictive_throughput_cdf(
    low: f64,
    high: f64,
    exposure_seconds: f64,
    completed_attempts: u64,
) -> Result<f64, PlantError> {
    let log_low = low.ln();
    let midpoint = log_low.midpoint(high.ln());
    let half_width = (high.ln() - log_low) / 2.0_f64;
    let mut integral = 0.0_f64;
    for (&node, &weight) in GAUSS_LEGENDRE_NODES.iter().zip(&GAUSS_LEGENDRE_WEIGHTS) {
        let lower_mean = (midpoint - half_width * node).exp() * exposure_seconds;
        let upper_mean = (midpoint + half_width * node).exp() * exposure_seconds;
        integral += weight
            * (poisson_cdf(lower_mean, completed_attempts)?
                + poisson_cdf(upper_mean, completed_attempts)?);
    }
    Ok(integral / 2.0_f64)
}

#[cfg(test)]
fn point_predictive_throughput_cdf(
    cells: &[ThroughputPosteriorCell],
    exposure_seconds: f64,
    completed_attempts: u64,
) -> Result<f64, PlantError> {
    let mut cumulative = 0.0_f64;
    for cell in cells {
        cumulative += cell.probability
            * poisson_cdf(
                cell.throughput_per_second * exposure_seconds,
                completed_attempts,
            )?;
    }
    Ok(cumulative)
}

fn poisson_cdf(mean: f64, completed_attempts: u64) -> Result<f64, PlantError> {
    if mean <= f64::EPSILON {
        return Ok(1.0_f64);
    }
    Ok(Poisson::new(mean)?.cdf(completed_attempts))
}

fn write_rejection_curve(
    scratch: &ScaleScratch,
    reason: DecisionRejection,
    values: &mut Vec<f64>,
    start: usize,
    end: usize,
) -> Result<(), PlantError> {
    if end > values.capacity() {
        return Err(PlantError::MetricCapacity);
    }
    values.resize(end, f64::NAN);
    match scratch.write_rejection_curve(reason, &mut values[start..end]) {
        Ok(()) | Err(prosody_scale_core::DecisionCurveError::Unavailable) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

impl PendingTransition {
    const fn reached(self, replicas: u32) -> bool {
        match self.direction() {
            TransitionDirection::Up => replicas >= self.target_replicas,
            TransitionDirection::Down => replicas <= self.target_replicas,
        }
    }

    const fn direction(self) -> TransitionDirection {
        if self.target_replicas > self.from_replicas {
            TransitionDirection::Up
        } else {
            TransitionDirection::Down
        }
    }

    const fn replica_delta(self) -> u32 {
        self.target_replicas.abs_diff(self.from_replicas)
    }
}

impl<Workload: TickGenerator> TickGenerator for ClosedLoop<Workload> {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        self.workload.calculate(context)
    }

    fn observe(
        &mut self,
        context: TickContext<'_>,
        inputs: TickInputs,
    ) -> Result<TickInputs, PlantError> {
        let reporter = self.workload.reporter(context);
        let calendar = self.workload.calendar_forecast(context)?;
        let scheduled_releases = self.workload.scheduled_releases(context)?;
        self.prepare_observation(
            &context,
            inputs,
            reporter,
            calendar.as_ref(),
            &scheduled_releases,
        )?;
        self.apply_decision(&context, inputs, reporter)
    }

    fn metric_polled(
        &mut self,
        context: TickContext<'_>,
        replicas: u32,
        plant_in_flight: u32,
    ) -> Result<(), PlantError> {
        self.track_scale_request(&context, replicas, plant_in_flight)
    }

    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        self.workload.event(context)
    }

    fn calendar_forecast(
        &self,
        context: TickContext<'_>,
    ) -> Result<Option<CalendarForecastInput>, PlantError> {
        self.workload.calendar_forecast(context)
    }

    fn scheduled_releases(
        &self,
        context: TickContext<'_>,
    ) -> Result<ScheduledReleasesInput, PlantError> {
        self.workload.scheduled_releases(context)
    }

    fn scheduled_release_count_max(&self) -> u32 {
        self.workload.scheduled_release_count_max()
    }
}

/// Failure while constructing one closed-loop simulator graph.
#[derive(Debug, Error)]
pub enum ClosedLoopError {
    /// The producer does not certify the configured future-release bound.
    #[error(
        "producer release bound {producer_count_max} must be positive and not exceed configured \
         bound {configured_count_max}"
    )]
    ScheduledReleaseCertification {
        /// Producer-certified maximum.
        producer_count_max: u32,
        /// Controller storage maximum.
        configured_count_max: u32,
    },
    /// The controller configuration is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// The simulator configuration is invalid.
    #[error(transparent)]
    Plant(#[from] PlantError),
}

#[cfg(test)]
mod tests;
