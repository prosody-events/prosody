use std::time::Duration;

use prosody_scale_core::{
    ActuationCommitment, ArrivalPosterior, AttemptOutcomeCounts, AttemptOutcomeEvidence,
    BacklogCohort, CapacityGrid, Cohort, Configuration, ConfigurationError, DecisionRejection,
    DemandClass, HoldReason, ModelTime, ObservationBuffer, PosteriorQuery, RandomStream,
    ResourceWindow, ScaleDecision, ScaleScratch, ScaleState, ThroughputPosteriorCell,
    TransitionDirection, TransitionEvidence, step,
};
use statrs::distribution::{DiscreteCDF, NegativeBinomial, Poisson};
use thiserror::Error;

use crate::{
    CalendarForecastInput, EventContext, EventInputs, FaultPattern, MetricTrace, PlantError,
    ReporterDirective, ScaleDirective, ScheduledReleasesInput, Snapshot, SnapshotChannel,
    SnapshotCursor, SnapshotTable, TickContext, TickGenerator, TickInputs,
};

const HANDLER_COVERAGE_LEVELS: [f64; 4] = [0.5_f64, 0.8_f64, 0.9_f64, 0.95_f64];
const HANDLER_RANK_BIN_COUNT: usize = 10;

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
    /// Posterior expected lead time for the selected or last transition bucket.
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
    /// Randomized prequential rank for the accepted throughput observation.
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
    arrival_prior: ArrivalPosterior,
    arrival_shape: Vec<f64>,
    arrival_rate: Vec<f64>,
    decision_candidate_count: usize,
    decision_expected_losses: Vec<f64>,
    decision_pass_probabilities: Vec<f64>,
    decision_deadline_rejections: Vec<f64>,
    decision_placement_rejections: Vec<f64>,
}

struct DiscretePosteriorTrace {
    query: PosteriorQuery,
    values: Vec<f64>,
    prior: Vec<f64>,
    probabilities: Vec<f64>,
}

impl ControllerTrace {
    fn new(sample_count_max: u32, state: &ScaleState) -> Result<Self, PlantError> {
        let capacity = usize::try_from(sample_count_max).map_err(|_| PlantError::PlatformLimit)?;
        if capacity == 0 {
            return Err(PlantError::ZeroBound {
                name: "controller_trace_count_max",
            });
        }
        let posterior_value_count = usize::try_from(state.capacity_posterior_value_count())
            .map_err(|_| PlantError::PlatformLimit)?;
        let posterior_cell_count = capacity
            .checked_mul(posterior_value_count)
            .ok_or(PlantError::PlatformLimit)?;
        let decision_candidate_count = usize::try_from(state.configuration().replica_count_max)
            .map_err(|_| PlantError::PlatformLimit)?;
        let decision_cell_count = capacity
            .checked_mul(decision_candidate_count)
            .ok_or(PlantError::PlatformLimit)?;
        let mut capacity_posterior_values = vec![0.0_f64; posterior_value_count];
        let mut capacity_prior_probabilities = vec![0.0_f64; posterior_value_count];
        state.write_capacity_posterior(
            &mut capacity_posterior_values,
            &mut capacity_prior_probabilities,
        )?;
        let service_time_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::ServiceTime, capacity)?;
        let collapse_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::Collapse, capacity)?;
        let knee_posterior = DiscretePosteriorTrace::new(state, PosteriorQuery::Knee, capacity)?;
        let saturation_state_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::SaturationState, capacity)?;
        let normal_retry_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::NormalRetryProbability, capacity)?;
        let failure_retry_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::FailureRetryProbability, capacity)?;
        let partition_share_posterior =
            DiscretePosteriorTrace::new(state, PosteriorQuery::PartitionShare, capacity)?;
        let lead_time_up_posterior = DiscretePosteriorTrace::new(
            state,
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            },
            capacity,
        )?;
        let lead_time_down_posterior = DiscretePosteriorTrace::new(
            state,
            PosteriorQuery::LeadTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            },
            capacity,
        )?;
        let rebalance_time_up_posterior = DiscretePosteriorTrace::new(
            state,
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Up,
                replica_delta: 1,
            },
            capacity,
        )?;
        let rebalance_time_down_posterior = DiscretePosteriorTrace::new(
            state,
            PosteriorQuery::RebalanceTime {
                direction: TransitionDirection::Down,
                replica_delta: 1,
            },
            capacity,
        )?;
        Ok(Self {
            at_micros: Vec::with_capacity(capacity),
            scenario_count: Vec::with_capacity(capacity),
            target: Vec::with_capacity(capacity),
            cap: Vec::with_capacity(capacity),
            hold: Vec::with_capacity(capacity),
            hold_reason: Vec::with_capacity(capacity),
            shortfall: Vec::with_capacity(capacity),
            expected_loss: Vec::with_capacity(capacity),
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
            service_time_posterior,
            collapse_posterior,
            knee_posterior,
            saturation_state_posterior,
            normal_retry_posterior,
            failure_retry_posterior,
            partition_share_posterior,
            lead_time_up_posterior,
            lead_time_down_posterior,
            rebalance_time_up_posterior,
            rebalance_time_down_posterior,
            arrival_prior: state.arrival_posterior(),
            arrival_shape: Vec::with_capacity(capacity),
            arrival_rate: Vec::with_capacity(capacity),
            decision_candidate_count,
            decision_expected_losses: Vec::with_capacity(decision_cell_count),
            decision_pass_probabilities: Vec::with_capacity(decision_cell_count),
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

    /// Returns the SLO pass probability for each replica candidate.
    #[must_use]
    pub fn decision_pass_probabilities(&self, index: usize) -> Option<&[f64]> {
        let start = index.checked_mul(self.decision_candidate_count)?;
        let end = start.checked_add(self.decision_candidate_count)?;
        self.decision_pass_probabilities.get(start..end)
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

    /// Returns the arrival-rate prior before the first observation.
    #[must_use]
    pub const fn arrival_prior(&self) -> ArrivalPosterior {
        self.arrival_prior
    }

    /// Returns one arrival-rate posterior at the selected time.
    #[must_use]
    pub fn arrival_posterior(&self, index: usize) -> Option<ArrivalPosterior> {
        Some(ArrivalPosterior {
            shape: *self.arrival_shape.get(index)?,
            rate: self.arrival_rate[index],
        })
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
        sample: ControllerSample,
        state: &ScaleState,
        scratch: &ScaleScratch,
    ) -> Result<(), PlantError> {
        if self.at_micros.len() == self.at_micros.capacity() {
            return Err(PlantError::MetricCapacity);
        }
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
        let decision_start = self.decision_expected_losses.len();
        let decision_end = decision_start
            .checked_add(self.decision_candidate_count)
            .ok_or(PlantError::PlatformLimit)?;
        if decision_end > self.decision_expected_losses.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.decision_expected_losses.resize(decision_end, f64::NAN);
        if decision_end > self.decision_pass_probabilities.capacity() {
            return Err(PlantError::MetricCapacity);
        }
        self.decision_pass_probabilities
            .resize(decision_end, f64::NAN);
        match scratch.write_decision_curve(
            &mut self.decision_expected_losses[decision_start..decision_end],
            &mut self.decision_pass_probabilities[decision_start..decision_end],
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
        write_rejection_curve(
            scratch,
            DecisionRejection::PartitionPlacement,
            &mut self.decision_placement_rejections,
            decision_start,
            decision_end,
        )?;
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
        let arrival = state.arrival_posterior();
        self.arrival_shape.push(arrival.shape);
        self.arrival_rate.push(arrival.rate);
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
    throughput_posterior_scratch: Vec<ThroughputPosteriorCell>,
    last_observed_replicas: Option<u32>,
    inflight_transitions: Vec<PendingTransition>,
    pending_transition_observation: Option<PendingTransitionObservation>,
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
}

#[derive(Clone, Copy)]
struct PendingTransition {
    from_replicas: u32,
    target_replicas: u32,
    requested_at_micros: u64,
}

struct PendingTransitionObservation {
    evidence: TransitionEvidence,
    sample: LeadTimeEvidenceSample,
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
        Ok(ResourceWindow::new(
            self.concurrency,
            self.exposure_seconds,
            self.completed_attempts,
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

impl<Workload> ClosedLoop<Workload> {
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
        let scratch = ScaleScratch::new(core_configuration)?;
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
            throughput_posterior_scratch: vec![
                ThroughputPosteriorCell::default();
                throughput_posterior_count
            ],
            last_observed_replicas: None,
            inflight_transitions: Vec::with_capacity(
                usize::try_from(trace_count_max).map_err(|_| ConfigurationError::PlatformLimit)?,
            ),
            pending_transition_observation: None,
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
        context: TickContext<'_>,
        inputs: TickInputs,
        reporter: ReporterDirective,
        calendar: Option<CalendarForecastInput>,
        scheduled_releases: &ScheduledReleasesInput,
    ) -> Result<(), PlantError>
    where
        Workload: TickGenerator,
    {
        self.observation.clear();
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
                .and_then(|_| context.plant.reconciliation_started_micros)
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
        context: TickContext<'_>,
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
        self.last_observed_replicas = None;
        self.inflight_transitions.clear();
        self.pending_transition_observation = None;
        self.lead_time_evidence_sample = LeadTimeEvidenceSample::None;
        Ok(())
    }

    fn prepare_transition_evidence(&mut self, context: TickContext<'_>) -> Result<(), PlantError> {
        if let Some(pending) = self.pending_transition_observation.take() {
            self.observation.set_transition(pending.evidence)?;
            self.lead_time_evidence_sample = pending.sample;
            return Ok(());
        }
        let Some((index, transition)) = self
            .inflight_transitions
            .iter()
            .copied()
            .enumerate()
            .find(|(_index, transition)| transition.reached(context.plant.replicas))
        else {
            return Ok(());
        };
        if !context.plant.partitions_ready {
            return Ok(());
        }
        let completed_micros = context
            .plant
            .reconciliation_completed_micros
            .unwrap_or(context.now_micros);
        let elapsed_micros = completed_micros.saturating_sub(transition.requested_at_micros);
        if elapsed_micros == 0 {
            self.inflight_transitions.remove(index);
            return Ok(());
        }
        let evidence = context
            .plant
            .reconciliation_started_micros
            .filter(|started| *started > transition.requested_at_micros)
            .filter(|started| completed_micros > *started)
            .map_or_else(
                || {
                    TransitionEvidence::completed(
                        transition.direction(),
                        transition.replica_delta(),
                        elapsed_micros,
                    )
                },
                |started| {
                    TransitionEvidence::completed_rebalance(
                        transition.direction(),
                        transition.replica_delta(),
                        started.saturating_sub(transition.requested_at_micros),
                        completed_micros.saturating_sub(started),
                    )
                },
            )?;
        self.observation.set_transition(evidence)?;
        self.lead_time_evidence_sample = LeadTimeEvidenceSample::Completed {
            direction: transition.direction(),
            replica_delta: transition.replica_delta(),
            elapsed_seconds: Duration::from_micros(elapsed_micros).as_secs_f64(),
        };
        self.inflight_transitions.remove(index);
        Ok(())
    }

    fn prepare_attempt_outcomes(&mut self, context: TickContext<'_>) -> Result<(), PlantError> {
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

    fn prepare_capacity_evidence(&mut self, context: TickContext<'_>) -> Result<(), PlantError> {
        let Some(previous_micros) = context.history.now_micros(0) else {
            return Ok(());
        };
        let previous_replicas = self
            .last_observed_replicas
            .replace(context.plant.replicas)
            .unwrap_or(context.plant.replicas);
        let replicas_changed = previous_replicas != context.plant.replicas;
        if replicas_changed {
            return Ok(());
        }
        let ready =
            context.history.partitions_ready(0).unwrap_or(false) && context.plant.partitions_ready;
        if !ready {
            return Ok(());
        }
        let previous_pause_micros = context.history.rebalance_pause_micros(0).unwrap_or(0);
        if previous_pause_micros != context.plant.rebalance_pause_micros {
            return Ok(());
        }
        let exposure_micros = context.now_micros.saturating_sub(previous_micros);
        if exposure_micros == 0 {
            return Ok(());
        }
        let previous_occupancy = context.history.handler_occupancy_micros(0).unwrap_or(0);
        let occupancy = context
            .plant
            .handler_occupancy_micros
            .saturating_sub(previous_occupancy);
        if occupancy == 0 {
            return Ok(());
        }
        let previous_attempts = context.history.completed_attempts(0).unwrap_or(0);
        let completed_attempts = context
            .plant
            .completed_attempts
            .saturating_sub(previous_attempts);
        // A zero-completion window cannot distinguish ramp-up from collapse.
        // The capacity model does not represent this censored observation.
        if completed_attempts == 0 {
            return Ok(());
        }
        let exposure_seconds = Duration::from_micros(exposure_micros).as_secs_f64();
        let current = CapacityWindow {
            concurrency: Duration::from_micros(occupancy).as_secs_f64() / exposure_seconds,
            exposure_seconds,
            completed_attempts,
        };
        self.latest_capacity_window = Some(current);
        self.observation.set_resource_window(current.evidence()?)?;
        self.capacity_evidence_sample = CapacityEvidenceSample::Window(current.sample());
        Ok(())
    }

    fn count_generated(
        &mut self,
        context: TickContext<'_>,
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
                tick: context,
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

    fn push_backlog_cohorts(&mut self, context: TickContext<'_>) -> Result<(), PlantError> {
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
        context: TickContext<'_>,
        mut inputs: TickInputs,
        reporter: ReporterDirective,
    ) -> Result<TickInputs, PlantError> {
        let arrival_predictive = self.arrival_prediction(context.now_micros)?;
        let partition_predictive = self.partition_prediction(context.now_micros)?;
        let lead_time_predictive = self.lead_time_prediction();
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
        let held_target = (0..self.trace.len())
            .rev()
            .find_map(|index| self.trace.sample(index).filter(|sample| !sample.hold))
            .map_or(context.plant.replicas, |sample| sample.target);
        let last_cap = (0..self.trace.len()).rev().find_map(|index| {
            self.trace
                .sample(index)
                .map(|sample| sample.cap)
                .filter(|cap| *cap > 0)
        });
        let held_cap = last_cap.map_or(self.configuration.core().replica_count_max, |cap| cap);
        let external_scale = matches!(
            inputs.scale,
            ScaleDirective::ExternalHold | ScaleDirective::Request { .. }
        );
        let (resource_concurrency, attempt_throughput_per_second) = self
            .latest_capacity_window
            .map_or((f64::NAN, f64::NAN), |window| {
                (
                    window.concurrency,
                    f64::from(window.completed_attempts) / window.exposure_seconds,
                )
            });
        let sample = match decision {
            ScaleDecision::Apply(apply) => {
                if !external_scale {
                    inputs.scale = if apply.target == desired {
                        ScaleDirective::Hold
                    } else {
                        ScaleDirective::Request {
                            replicas: apply.target,
                            delay_micros: inputs.launch_delay_micros,
                        }
                    };
                }
                ControllerSample {
                    at_micros: context.now_micros,
                    scenario_count: apply.diagnostics.scenario_count,
                    target: apply.target,
                    cap: apply.cap,
                    hold: false,
                    hold_reason: None,
                    shortfall: apply.diagnostics.shortfall,
                    expected_loss: apply.diagnostics.expected_loss,
                    arrival_rate_per_second: apply.diagnostics.arrival_rate_per_second,
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
                    capacity_per_second: apply.diagnostics.capacity_per_second,
                    capacity_low_per_second: apply.diagnostics.capacity_low_per_second,
                    capacity_median_per_second: apply.diagnostics.capacity_median_per_second,
                    capacity_high_per_second: apply.diagnostics.capacity_high_per_second,
                    saturation_probability: apply.diagnostics.saturation_probability,
                    no_knee_probability: apply.diagnostics.no_knee_probability,
                    lead_time_up_seconds: apply.diagnostics.lead_time_up_seconds,
                    lead_time_down_seconds: apply.diagnostics.lead_time_down_seconds,
                    lead_time_seconds: apply.diagnostics.lead_time_seconds,
                    resource_concurrency,
                    attempt_throughput_per_second,
                    capacity_evidence: self.capacity_evidence_sample,
                    capacity_predictive_low_per_second: capacity_predictive.quantiles[0],
                    capacity_predictive_median_per_second: capacity_predictive.quantiles[1],
                    capacity_predictive_high_per_second: capacity_predictive.quantiles[2],
                    capacity_predictive_rank: capacity_predictive.rank,
                    reporter,
                }
            }
            ScaleDecision::Hold(hold) => {
                if !external_scale {
                    inputs.scale = ScaleDirective::Hold;
                }
                ControllerSample {
                    at_micros: context.now_micros,
                    scenario_count: hold.diagnostics.scenario_count,
                    target: held_target,
                    cap: held_cap,
                    hold: true,
                    hold_reason: Some(hold.reason),
                    shortfall: hold.diagnostics.shortfall,
                    expected_loss: hold.diagnostics.expected_loss,
                    arrival_rate_per_second: hold.diagnostics.arrival_rate_per_second,
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
                    capacity_per_second: hold.diagnostics.capacity_per_second,
                    capacity_low_per_second: hold.diagnostics.capacity_low_per_second,
                    capacity_median_per_second: hold.diagnostics.capacity_median_per_second,
                    capacity_high_per_second: hold.diagnostics.capacity_high_per_second,
                    saturation_probability: hold.diagnostics.saturation_probability,
                    no_knee_probability: hold.diagnostics.no_knee_probability,
                    lead_time_up_seconds: hold.diagnostics.lead_time_up_seconds,
                    lead_time_down_seconds: hold.diagnostics.lead_time_down_seconds,
                    lead_time_seconds: hold.diagnostics.lead_time_seconds,
                    resource_concurrency,
                    attempt_throughput_per_second,
                    capacity_evidence: self.capacity_evidence_sample,
                    capacity_predictive_low_per_second: capacity_predictive.quantiles[0],
                    capacity_predictive_median_per_second: capacity_predictive.quantiles[1],
                    capacity_predictive_high_per_second: capacity_predictive.quantiles[2],
                    capacity_predictive_rank: capacity_predictive.rank,
                    reporter,
                }
            }
        };
        self.track_scale_request(context, inputs.scale)?;
        self.trace.push(sample, &self.state, &self.scratch)?;
        Ok(inputs)
    }

    fn arrival_prediction(&self, now_micros: u64) -> Result<ArrivalPrediction, PlantError> {
        let ArrivalEvidenceSample::Accepted(window) = self.arrival_evidence_sample else {
            return Ok(ArrivalPrediction::missing());
        };
        let posterior = self.state.arrival_posterior();
        let success = posterior.rate / (posterior.rate + window.exposure_seconds);
        let distribution = NegativeBinomial::new(posterior.shape, success)?;
        let quantiles = negative_binomial_quantiles(&distribution);
        let observed = u64::from(window.count);
        let upper = distribution.cdf(observed);
        let lower = if observed == 0 {
            0.0_f64
        } else {
            distribution.cdf(observed - 1)
        };
        let rank_offset = arrival_predictive_rank_offset(self.diagnostic_seed, now_micros);
        Ok(ArrivalPrediction {
            quantiles,
            rank: lower + rank_offset * (upper - lower),
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

    fn lead_time_prediction(&self) -> LeadTimePrediction {
        let (direction, replica_delta, elapsed_seconds) = match self.lead_time_evidence_sample {
            LeadTimeEvidenceSample::None => return LeadTimePrediction::missing(),
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
        LeadTimePrediction {
            quantiles: [0.1_f64, 0.5_f64, 0.9_f64].map(|probability| {
                self.state
                    .lead_time_predictive_quantile(direction, replica_delta, probability)
            }),
            rank: elapsed_seconds.map_or(f64::NAN, |elapsed| {
                self.state
                    .lead_time_predictive_cdf(direction, replica_delta, elapsed)
            }),
        }
    }

    fn capacity_prediction(&mut self, now_micros: u64) -> Result<CapacityPrediction, PlantError> {
        let rank_offset = predictive_rank_offset(self.diagnostic_seed, now_micros);
        match self.capacity_evidence_sample {
            CapacityEvidenceSample::None => Ok(CapacityPrediction::missing()),
            CapacityEvidenceSample::Window(window) => {
                self.state.write_throughput_posterior(
                    window.concurrency,
                    &mut self.throughput_posterior_scratch,
                )?;
                let quantiles = posterior_predictive_throughput_quantiles(
                    &self.throughput_posterior_scratch,
                    window.exposure_seconds,
                )?;
                let observed = u64::from(window.completed_attempts);
                let upper = predictive_throughput_cdf(
                    &self.throughput_posterior_scratch,
                    window.exposure_seconds,
                    observed,
                )?;
                let lower = if observed == 0 {
                    0.0_f64
                } else {
                    predictive_throughput_cdf(
                        &self.throughput_posterior_scratch,
                        window.exposure_seconds,
                        observed - 1,
                    )?
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
        context: TickContext<'_>,
        directive: ScaleDirective,
    ) -> Result<(), PlantError> {
        let ScaleDirective::Request { replicas, .. } = directive else {
            return Ok(());
        };
        let requested = PendingTransition {
            from_replicas: context.plant.replicas,
            target_replicas: replicas,
            requested_at_micros: context.now_micros,
        };
        if replicas == context.plant.replicas
            && !context.plant.partitions_ready
            && self
                .inflight_transitions
                .iter()
                .any(|pending| pending.target_replicas == replicas)
        {
            return Ok(());
        }
        let mut index = 0_usize;
        let mut exact = false;
        while index < self.inflight_transitions.len() {
            let mut pending = self.inflight_transitions[index];
            if replicas == context.plant.replicas || pending.direction() != requested.direction() {
                self.record_censored_transition(context, pending)?;
                self.inflight_transitions.remove(index);
                continue;
            }
            match requested.direction() {
                TransitionDirection::Up if pending.target_replicas > replicas => {
                    pending.target_replicas = replicas;
                }
                TransitionDirection::Down if pending.target_replicas < replicas => {
                    pending.target_replicas = replicas;
                }
                TransitionDirection::Up | TransitionDirection::Down => {}
            }
            // A clamp can move the pending target back to its origin. That
            // pending transition is no longer observable: censor it like
            // any other superseded transition.
            if pending.target_replicas == pending.from_replicas {
                self.record_censored_transition(context, self.inflight_transitions[index])?;
                self.inflight_transitions.remove(index);
                continue;
            }
            if pending.target_replicas == replicas {
                if exact {
                    self.inflight_transitions.remove(index);
                    continue;
                }
                exact = true;
            }
            self.inflight_transitions[index] = pending;
            index += 1;
        }
        if replicas == context.plant.replicas || exact {
            return Ok(());
        }
        if self.inflight_transitions.len() == self.inflight_transitions.capacity() {
            return Err(PlantError::ChangeCapacity);
        }
        self.inflight_transitions.push(requested);
        Ok(())
    }

    fn record_censored_transition(
        &mut self,
        context: TickContext<'_>,
        transition: PendingTransition,
    ) -> Result<(), PlantError> {
        if self.pending_transition_observation.is_some() {
            return Ok(());
        }
        let exposure_micros = context
            .now_micros
            .saturating_sub(transition.requested_at_micros);
        if exposure_micros == 0 {
            return Ok(());
        }
        let evidence = TransitionEvidence::censored(
            transition.direction(),
            transition.replica_delta(),
            exposure_micros,
        )?;
        self.pending_transition_observation = Some(PendingTransitionObservation {
            evidence,
            sample: LeadTimeEvidenceSample::Censored {
                direction: transition.direction(),
                replica_delta: transition.replica_delta(),
                exposure_seconds: Duration::from_micros(exposure_micros).as_secs_f64(),
            },
        });
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

fn negative_binomial_quantiles(distribution: &NegativeBinomial) -> [f64; 3] {
    let mut quantiles = [0.0_f64; 3];
    for (index, threshold) in [0.1_f64, 0.5_f64, 0.9_f64].into_iter().enumerate() {
        let mut high = 1_u64;
        while distribution.cdf(high) < threshold && high < u64::MAX {
            high = high.saturating_mul(2);
        }
        let mut low = 0_u64;
        while low < high {
            let middle = low.midpoint(high);
            if distribution.cdf(middle) >= threshold {
                high = middle;
            } else {
                low = middle.saturating_add(1);
            }
        }
        quantiles[index] = count_f64(low);
    }
    quantiles
}

fn count_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32_u32).map_or(0, |part| part);
    let low = u32::try_from(value & u64::from(u32::MAX)).map_or(0, |part| part);
    f64::from(high) * 4_294_967_296.0_f64 + f64::from(low)
}

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
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    for (index, threshold) in thresholds.into_iter().enumerate() {
        let mut low = 0_u64;
        let mut high = upper;
        while low < high {
            let middle = low + (high - low) / 2;
            if predictive_throughput_cdf(cells, exposure_seconds, middle)? >= threshold {
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

fn predictive_throughput_cdf(
    cells: &[ThroughputPosteriorCell],
    exposure_seconds: f64,
    completed_attempts: u64,
) -> Result<f64, PlantError> {
    let mut cumulative = 0.0_f64;
    for cell in cells {
        let mean = cell.throughput_per_second * exposure_seconds;
        let probability = if mean <= f64::EPSILON {
            1.0_f64
        } else {
            Poisson::new(mean)?.cdf(completed_attempts)
        };
        cumulative += cell.probability * probability;
    }
    Ok(cumulative)
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
        self.prepare_observation(context, inputs, reporter, calendar, &scheduled_releases)?;
        self.apply_decision(context, inputs, reporter)
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
}

/// Failure while constructing one closed-loop simulator graph.
#[derive(Debug, Error)]
pub enum ClosedLoopError {
    /// The controller configuration is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// The simulator configuration is invalid.
    #[error(transparent)]
    Plant(#[from] PlantError),
}

#[cfg(test)]
mod tests;
