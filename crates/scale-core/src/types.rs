use std::{num::NonZeroU32, time::Duration};

use thiserror::Error;

use crate::{
    ArrivalEvidence, LaunchEvidence, LaunchEvidenceError, ReadinessLump, RebalanceEvidence,
    ResourceWindow, TransitionDirection,
};

pub(crate) const POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN: u32 = 2;

/// Monotonic model time in microseconds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ModelTime(u64);

impl ModelTime {
    /// Constructs a model time from elapsed microseconds.
    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    /// Returns elapsed microseconds.
    #[must_use]
    pub const fn as_micros(self) -> u64 {
        self.0
    }
}

/// One replica transition that has not reached warm membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActuationCommitment {
    phase: ActuationPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActuationPhase {
    Launching {
        from_replicas: u32,
        target_replicas: u32,
        requested_at: ModelTime,
    },
    Rebalancing {
        from_replicas: u32,
        target_replicas: u32,
        requested_at: ModelTime,
        started_at: ModelTime,
    },
}

impl ActuationCommitment {
    /// Constructs one transition that has not changed replica membership.
    ///
    /// # Errors
    ///
    /// Returns an error for zero or equal replica counts.
    pub fn launching(
        from_replicas: u32,
        target_replicas: u32,
        requested_at: ModelTime,
    ) -> Result<Self, ObservationError> {
        validate_actuation_replicas(from_replicas, target_replicas)?;
        Ok(Self {
            phase: ActuationPhase::Launching {
                from_replicas,
                target_replicas,
                requested_at,
            },
        })
    }

    /// Constructs one transition whose replica membership has changed.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid counts or an invalid phase boundary.
    pub fn rebalancing(
        from_replicas: u32,
        target_replicas: u32,
        requested_at: ModelTime,
        started_at: ModelTime,
    ) -> Result<Self, ObservationError> {
        validate_actuation_replicas(from_replicas, target_replicas)?;
        if started_at < requested_at {
            return Err(ObservationError::ActuationCommitment);
        }
        Ok(Self {
            phase: ActuationPhase::Rebalancing {
                from_replicas,
                target_replicas,
                requested_at,
                started_at,
            },
        })
    }
}

fn validate_actuation_replicas(
    from_replicas: u32,
    target_replicas: u32,
) -> Result<(), ObservationError> {
    if from_replicas == 0 || target_replicas == 0 || from_replicas == target_replicas {
        return Err(ObservationError::ActuationCommitment);
    }
    Ok(())
}

/// One discrete posterior view for diagnostics and calibration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PosteriorQuery {
    /// Peak completed-attempt throughput.
    Capacity,
    /// Uncongested operation time.
    ServiceTime,
    /// Post-knee throughput collapse.
    Collapse,
    /// Concurrency at peak throughput.
    Knee,
    /// Whether a finite knee exists in the supported range.
    SaturationState,
    /// Retry probability after a normal attempt.
    NormalRetryProbability,
    /// Retry probability after a failure attempt.
    FailureRetryProbability,
    /// Expected Kafka partition share.
    PartitionShare,
    /// Actuation duration for one transition class.
    LeadTime {
        /// Replica transition direction.
        direction: TransitionDirection,
        /// Absolute replica-count change.
        replica_delta: u32,
    },
    /// KIP-848 partition-pause duration for one transition class.
    RebalanceTime {
        /// Replica transition direction.
        direction: TransitionDirection,
        /// Absolute replica-count change.
        replica_delta: u32,
    },
}

/// Exact Gamma posterior parameters for the arrival rate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArrivalPosterior {
    /// Gamma shape parameter.
    pub shape: f64,
    /// Gamma rate parameter in seconds.
    pub rate: f64,
}

/// Stable identity for one frozen calendar artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CalendarArtifactId(pub u64);

/// Stable identity and random stream for one prior artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriorArtifactIdentity {
    source: u64,
    version: u32,
    random_stream: u64,
}

impl PriorArtifactIdentity {
    /// Constructs one versioned artifact identity.
    #[must_use]
    pub const fn new(source: u64, version: u32, random_stream: u64) -> Self {
        Self {
            source,
            version,
            random_stream,
        }
    }

    /// Returns the artifact source identity.
    #[must_use]
    pub const fn source(self) -> u64 {
        self.source
    }

    /// Returns the artifact version.
    #[must_use]
    pub const fn version(self) -> u32 {
        self.version
    }

    /// Returns the stream used for prior-predictive checks.
    #[must_use]
    pub const fn random_stream(self) -> u64 {
        self.random_stream
    }

    pub(crate) const fn is_valid(self) -> bool {
        self.source != 0 && self.version != 0 && self.random_stream != 0
    }
}

/// Fixed approximation limits for one prior artifact.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriorArtifactBudget {
    hypothesis_count_max: u32,
    storage_bytes_max: u64,
    update_operation_count_max: u64,
    boundary_probability_max: f64,
    path_time_error_seconds: f64,
    decision_cost_error_max: f64,
}

impl PriorArtifactBudget {
    /// Constructs one grid, boundary, and path budget.
    #[must_use]
    pub const fn new(
        hypothesis_count_max: u32,
        storage_bytes_max: u64,
        update_operation_count_max: u64,
        boundary_probability_max: f64,
        path_time_error_seconds: f64,
        decision_cost_error_max: f64,
    ) -> Self {
        Self {
            hypothesis_count_max,
            storage_bytes_max,
            update_operation_count_max,
            boundary_probability_max,
            path_time_error_seconds,
            decision_cost_error_max,
        }
    }

    pub(crate) fn is_valid(self) -> bool {
        self.hypothesis_count_max > 0
            && self.storage_bytes_max > 0
            && self.update_operation_count_max > 0
            && self.boundary_probability_max.is_finite()
            && (0.0_f64..1.0_f64).contains(&self.boundary_probability_max)
            && self.path_time_error_seconds.is_finite()
            && self.path_time_error_seconds >= 0.0_f64
            && self.decision_cost_error_max.is_finite()
            && self.decision_cost_error_max >= 0.0_f64
    }

    /// Returns the maximum product-grid cell count.
    #[must_use]
    pub const fn hypothesis_count_max(self) -> u32 {
        self.hypothesis_count_max
    }

    /// Returns the maximum posterior and work storage in bytes.
    #[must_use]
    pub const fn storage_bytes_max(self) -> u64 {
        self.storage_bytes_max
    }

    /// Returns the maximum cell and observation operations per update.
    #[must_use]
    pub const fn update_operation_count_max(self) -> u64 {
        self.update_operation_count_max
    }

    /// Returns the maximum predictive mass outside recorded support.
    #[must_use]
    pub const fn boundary_probability_max(self) -> f64 {
        self.boundary_probability_max
    }

    /// Returns the maximum lead-time path error in seconds.
    #[must_use]
    pub const fn path_time_error_seconds(self) -> f64 {
        self.path_time_error_seconds
    }

    /// Returns the maximum decision-cost approximation error.
    #[must_use]
    pub const fn decision_cost_error_max(self) -> f64 {
        self.decision_cost_error_max
    }
}

/// One prior-predictive support and omitted-tail record.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriorCoverageRecord {
    lower_endpoint: f64,
    upper_endpoint: f64,
    lower_tail_probability: f64,
    upper_tail_probability: f64,
    decision_cost_error: f64,
}

impl PriorCoverageRecord {
    /// Constructs one coverage record.
    #[must_use]
    pub const fn new(
        lower_endpoint: f64,
        upper_endpoint: f64,
        lower_tail_probability: f64,
        upper_tail_probability: f64,
        decision_cost_error: f64,
    ) -> Self {
        Self {
            lower_endpoint,
            upper_endpoint,
            lower_tail_probability,
            upper_tail_probability,
            decision_cost_error,
        }
    }

    pub(crate) fn is_valid(self) -> bool {
        self.lower_endpoint.is_finite()
            && self.lower_endpoint >= 0.0_f64
            && self.upper_endpoint.is_finite()
            && self.upper_endpoint > self.lower_endpoint
            && self.lower_tail_probability.is_finite()
            && self.lower_tail_probability >= 0.0_f64
            && self.upper_tail_probability.is_finite()
            && self.upper_tail_probability >= 0.0_f64
            && self.tail_probability() < 1.0_f64
            && self.decision_cost_error.is_finite()
            && self.decision_cost_error >= 0.0_f64
    }

    /// Returns the lower support endpoint in artifact units.
    #[must_use]
    pub const fn lower_endpoint(self) -> f64 {
        self.lower_endpoint
    }

    /// Returns the upper support endpoint in artifact units.
    #[must_use]
    pub const fn upper_endpoint(self) -> f64 {
        self.upper_endpoint
    }

    /// Returns the recorded probability outside both endpoints.
    #[must_use]
    pub const fn tail_probability(self) -> f64 {
        self.lower_tail_probability + self.upper_tail_probability
    }

    /// Returns the recorded decision-cost error from omitted mass.
    #[must_use]
    pub const fn decision_cost_error(self) -> f64 {
        self.decision_cost_error
    }
}

pub(crate) fn prior_artifact_contract_holds(
    identity: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: &[PriorCoverageRecord],
    hypothesis_count: usize,
    storage_bytes: usize,
    update_operation_count: u64,
) -> bool {
    identity.is_valid()
        && budget.is_valid()
        && !coverage.is_empty()
        && u32::try_from(hypothesis_count).is_ok_and(|count| count <= budget.hypothesis_count_max())
        && u64::try_from(storage_bytes).is_ok_and(|bytes| bytes <= budget.storage_bytes_max())
        && update_operation_count <= budget.update_operation_count_max()
        && coverage.iter().all(|record| {
            record.is_valid()
                && record.tail_probability() <= budget.boundary_probability_max()
                && record.decision_cost_error() <= budget.decision_cost_error_max()
        })
}

/// One Gamma rate posterior for a future calendar interval.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalendarRateSegment {
    pub(crate) position: u32,
    pub(crate) start_micros: u64,
    pub(crate) end_micros: u64,
    pub(crate) shape: f64,
    pub(crate) rate_seconds: f64,
}

/// One known future release from the timer store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledRelease {
    /// Virtual time when the work becomes available.
    pub release_micros: u64,
    /// Number of events released at that time.
    pub count: u32,
}

impl CalendarRateSegment {
    /// Constructs one frozen calendar posterior interval.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid interval or Gamma distribution.
    pub fn new(
        position: u32,
        start_micros: u64,
        end_micros: u64,
        shape: f64,
        rate_seconds: f64,
    ) -> Result<Self, ObservationError> {
        if start_micros >= end_micros {
            return Err(ObservationError::InvalidCalendarInterval);
        }
        if !shape.is_finite() || shape <= 0.0_f64 {
            return Err(ObservationError::InvalidCalendarShape);
        }
        if !rate_seconds.is_finite() || rate_seconds <= 0.0_f64 {
            return Err(ObservationError::InvalidCalendarRate);
        }
        Ok(Self {
            position,
            start_micros,
            end_micros,
            shape,
            rate_seconds,
        })
    }

    /// Returns this segment's calendar position.
    #[must_use]
    pub const fn position(self) -> u32 {
        self.position
    }

    /// Returns this segment's inclusive start time.
    #[must_use]
    pub const fn start_micros(self) -> u64 {
        self.start_micros
    }

    /// Returns this segment's exclusive end time.
    #[must_use]
    pub const fn end_micros(self) -> u64 {
        self.end_micros
    }

    /// Returns this segment's Gamma shape parameter.
    #[must_use]
    pub const fn shape(self) -> f64 {
        self.shape
    }

    /// Returns this segment's Gamma rate in seconds.
    #[must_use]
    pub const fn rate_seconds(self) -> f64 {
        self.rate_seconds
    }
}

#[derive(Debug)]
pub(crate) struct CalendarColumns {
    positions: Vec<u32>,
    start_micros: Vec<u64>,
    end_micros: Vec<u64>,
    shapes: Vec<f64>,
    rate_seconds: Vec<f64>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CalendarForecast<'a> {
    pub(crate) artifact: CalendarArtifactId,
    pub(crate) prior_probability: f64,
    pub(crate) segments: &'a CalendarColumns,
}

/// One latency objective supplied by a user.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServiceObjective {
    budget_micros: u64,
    epsilon: f64,
    replica_second_delay_rate: f64,
}

impl ServiceObjective {
    /// Constructs a validated objective.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero budget, an invalid miss fraction, or an
    /// invalid rate.
    pub fn new(
        budget_micros: u64,
        epsilon: f64,
        replica_second_delay_rate: f64,
    ) -> Result<Self, ConfigurationError> {
        if budget_micros == 0 {
            return Err(ConfigurationError::ZeroBudget);
        }
        if !(0.0_f64..1.0_f64).contains(&epsilon) {
            return Err(ConfigurationError::InvalidEpsilon { epsilon });
        }
        if !replica_second_delay_rate.is_finite() || replica_second_delay_rate <= 0.0_f64 {
            return Err(ConfigurationError::InvalidReplicaSecondDelayRate {
                rate: replica_second_delay_rate,
            });
        }
        Ok(Self {
            budget_micros,
            epsilon,
            replica_second_delay_rate,
        })
    }

    /// Returns the latency budget in microseconds.
    #[must_use]
    pub const fn budget_micros(self) -> u64 {
        self.budget_micros
    }

    /// Returns the SLO miss-fraction constraint.
    ///
    /// The controller rejects a candidate when its predicted miss fraction
    /// exceeds epsilon. It then minimizes expected cost among valid candidates.
    /// Pricing has no epsilon by design.
    #[must_use]
    pub const fn epsilon(self) -> f64 {
        self.epsilon
    }

    /// Returns event-delay-seconds priced per replica-second.
    #[must_use]
    pub const fn replica_second_delay_rate(self) -> f64 {
        self.replica_second_delay_rate
    }
}

/// Fixed bounds and model constants.
#[derive(Clone, Debug)]
pub struct Configuration {
    /// Maximum number of cohorts in one observation.
    pub cohort_count_max: u32,
    /// Maximum calendar intervals supplied in one observation.
    pub calendar_segment_count_max: u32,
    /// Certified maximum future releases in one complete observation.
    pub scheduled_release_count_max: u32,
    /// Maximum scheduling groups in one launch update.
    pub readiness_lump_count_max: u32,
    /// Configured Kafka partition count.
    pub partition_count: u32,
    /// Maximum allowed replica count.
    pub replica_count_max: u32,
    /// Warm handler slots on each replica.
    pub slots_per_replica: u32,
    /// Posterior sample budget for at least two draws per capacity class.
    pub posterior_sample_count: u32,
    /// Time between complete telemetry reports.
    pub report_interval_micros: u64,
    /// Maximum starts or completions in one resource report.
    pub resource_window_attempt_count_max: u32,
    /// Maximum failure-service fraction while normal work waits.
    pub failure_service_weight: f64,
    /// Prior for live arrival-rate segments.
    pub arrival_prior: crate::ArrivalPrior,
    /// Prior rate for physical capacity-curve changes.
    pub capacity_change_rate_per_second: f64,
    /// Population prior for class-specific retry probabilities.
    pub reliability_prior: crate::ReliabilityPrior,
    /// Population prior for replica launch time.
    pub launch_time_prior: crate::LaunchPrior,
    /// Population prior for KIP-848 pause time.
    pub rebalance_time_prior: crate::RebalancePrior,
    /// User latency objective.
    pub objective: ServiceObjective,
}

impl Configuration {
    /// Validates all construction bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when any fixed capacity is zero.
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.cohort_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "cohort_count_max",
            });
        }
        if self.calendar_segment_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "calendar_segment_count_max",
            });
        }
        if self.scheduled_release_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "scheduled_release_count_max",
            });
        }
        if self.readiness_lump_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "readiness_lump_count_max",
            });
        }
        self.launch_time_prior
            .validate_update_budget(self.readiness_lump_count_max)?;
        if self.calendar_segment_count_max > self.arrival_prior.path_segment_count_max() as u32 {
            return Err(ConfigurationError::CalendarPathCapacity);
        }
        if self.partition_count == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "partition_count",
            });
        }
        if self.replica_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "replica_count_max",
            });
        }
        if self.slots_per_replica == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "slots_per_replica",
            });
        }
        if self.posterior_sample_count < POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN {
            return Err(ConfigurationError::InsufficientPosteriorSamples {
                sample_count: self.posterior_sample_count,
                minimum: POSTERIOR_SAMPLES_PER_CAPACITY_CLASS_MIN,
            });
        }
        if self.report_interval_micros == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "report_interval_micros",
            });
        }
        if self.resource_window_attempt_count_max == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "resource_window_attempt_count_max",
            });
        }
        if !self.failure_service_weight.is_finite()
            || !(0.0_f64..=1.0_f64).contains(&self.failure_service_weight)
        {
            return Err(ConfigurationError::InvalidFailureServiceWeight {
                weight: self.failure_service_weight,
            });
        }
        if !self.capacity_change_rate_per_second.is_finite()
            || self.capacity_change_rate_per_second <= 0.0_f64
        {
            return Err(ConfigurationError::InvalidCapacityChangeRate);
        }
        Ok(())
    }

    pub(crate) fn capacity_concurrency_max(&self) -> Result<f64, ConfigurationError> {
        self.replica_count_max
            .checked_mul(self.slots_per_replica)
            .map(f64::from)
            .ok_or(ConfigurationError::PlatformLimit)
    }

    pub(crate) fn resource_exposure_min_seconds(&self) -> f64 {
        Duration::from_micros(self.report_interval_micros).as_secs_f64()
    }
}

/// Final outcome counts for one demand class and observation window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptOutcomeCounts {
    /// Attempts that completed successfully.
    pub success: u32,
    /// Attempts that ended with a permanent failure.
    pub permanent: u32,
    /// Attempts that ended with a transient failure.
    pub transient: u32,
    /// Attempts that ended with a terminal failure.
    pub terminal: u32,
}

impl AttemptOutcomeCounts {
    /// Constructs one complete outcome table row.
    #[must_use]
    pub const fn new(success: u32, permanent: u32, transient: u32, terminal: u32) -> Self {
        Self {
            success,
            permanent,
            transient,
            terminal,
        }
    }
}

/// Attempt outcomes for both demand classes in one observation window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptOutcomeEvidence {
    pub(crate) normal: AttemptOutcomeCounts,
    pub(crate) failure: AttemptOutcomeCounts,
}

impl AttemptOutcomeEvidence {
    /// Constructs one class-separated outcome window.
    #[must_use]
    pub const fn new(normal: AttemptOutcomeCounts, failure: AttemptOutcomeCounts) -> Self {
        Self { normal, failure }
    }
}

/// Scheduler demand class for one work cohort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DemandClass {
    /// A first attempt from Kafka or the timer store.
    Normal,
    /// A later attempt caused by a retry-producing outcome.
    Failure,
}

impl DemandClass {
    /// Number of scheduler demand classes.
    pub const COUNT: u32 = 2;
    pub(crate) const COUNT_USIZE: usize = 2;

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Normal => 0,
            Self::Failure => 1,
        }
    }
}

/// Observable queued work with unknown individual arrival times.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacklogCohort {
    observed_at_micros: u64,
    oldest_arrival_micros: u64,
    event_count: NonZeroU32,
    partition: u32,
    demand_class: DemandClass,
}

impl BacklogCohort {
    /// Constructs one observable backlog cohort.
    ///
    /// # Errors
    ///
    /// Returns an error for zero work or an arrival after the observation.
    pub fn new(
        observed_at_micros: u64,
        oldest_arrival_micros: u64,
        event_count: u32,
        partition: u32,
        demand_class: DemandClass,
    ) -> Result<Self, ObservationError> {
        let Some(event_count) = NonZeroU32::new(event_count) else {
            return Err(ObservationError::EmptyBacklog);
        };
        if oldest_arrival_micros > observed_at_micros {
            return Err(ObservationError::FutureBacklogArrival);
        }
        Ok(Self {
            observed_at_micros,
            oldest_arrival_micros,
            event_count,
            partition,
            demand_class,
        })
    }

    pub(crate) const fn observed_at_micros(self) -> u64 {
        self.observed_at_micros
    }

    pub(crate) const fn oldest_arrival_micros(self) -> u64 {
        self.oldest_arrival_micros
    }

    pub(crate) const fn event_count(self) -> u32 {
        self.event_count.get()
    }

    pub(crate) const fn partition(self) -> u32 {
        self.partition
    }

    pub(crate) const fn demand_class(self) -> DemandClass {
        self.demand_class
    }
}

/// Work released at one time and due at one deadline.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Cohort {
    /// Earliest service time in model microseconds.
    pub release_micros: u64,
    /// Required completion time in model microseconds.
    pub deadline_micros: u64,
    /// Offered events represented by this cohort.
    pub offered_events: f64,
    /// Partition that owns this work.
    pub partition: u32,
    /// Scheduler class that serves this work.
    pub demand_class: DemandClass,
}

impl Cohort {
    fn validate(self) -> Result<(), ObservationError> {
        if self.release_micros >= self.deadline_micros {
            return Err(ObservationError::InvalidCohortInterval);
        }
        if !self.offered_events.is_finite() || self.offered_events < 0.0_f64 {
            return Err(ObservationError::InvalidWork);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WorkCohorts {
    release_micros: Vec<u64>,
    deadline_micros: Vec<u64>,
    work_slot_seconds: Vec<f64>,
    partitions: Vec<u32>,
    deadline_max_micros: u64,
}

#[derive(Debug)]
pub(crate) struct CohortColumns {
    release_micros: Vec<u64>,
    deadline_micros: Vec<u64>,
    offered_events: Vec<f64>,
    partitions: Vec<u32>,
    demand_classes: Vec<DemandClass>,
    normal_events: f64,
    failure_events: f64,
}

#[derive(Debug)]
pub(crate) struct BacklogColumns {
    event_counts: Vec<u32>,
    oldest_arrival_micros: Vec<u64>,
    observed_at_micros: Vec<u64>,
    present: Vec<u8>,
    normal_events: f64,
    failure_events: f64,
}

#[derive(Debug)]
pub(crate) struct ActuationCommitments {
    capacity: usize,
    launching: LaunchingCommitments,
    rebalancing: Option<RebalancingCommitment>,
}

#[derive(Debug)]
struct LaunchingCommitments {
    from_replicas: Vec<u32>,
    target_replicas: Vec<u32>,
    requested_at: Vec<ModelTime>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RebalancingCommitment {
    pub(crate) target_replicas: u32,
    pub(crate) requested_at: ModelTime,
    pub(crate) started_at: ModelTime,
}

impl WorkCohorts {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            release_micros: Vec::with_capacity(capacity),
            deadline_micros: Vec::with_capacity(capacity),
            work_slot_seconds: Vec::with_capacity(capacity),
            partitions: Vec::with_capacity(capacity),
            deadline_max_micros: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.release_micros.clear();
        self.deadline_micros.clear();
        self.work_slot_seconds.clear();
        self.partitions.clear();
        self.deadline_max_micros = 0;
    }

    pub(crate) fn push_values(
        &mut self,
        release_micros: u64,
        deadline_micros: u64,
        work_slot_seconds: f64,
        partition: u32,
    ) {
        assert!(
            self.len() < self.release_micros.capacity(),
            "work cohorts must fit the configured capacity"
        );
        self.release_micros.push(release_micros);
        self.deadline_micros.push(deadline_micros);
        self.work_slot_seconds.push(work_slot_seconds);
        self.partitions.push(partition);
        self.deadline_max_micros = self.deadline_max_micros.max(deadline_micros);
    }

    pub(crate) const fn len(&self) -> usize {
        self.release_micros.len()
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.release_micros.is_empty()
    }

    pub(crate) fn release_micros(&self, index: usize) -> u64 {
        self.release_micros[index]
    }

    pub(crate) fn deadline_micros(&self, index: usize) -> u64 {
        self.deadline_micros[index]
    }

    pub(crate) fn work_slot_seconds(&self, index: usize) -> f64 {
        self.work_slot_seconds[index]
    }

    pub(crate) fn partition(&self, index: usize) -> u32 {
        self.partitions[index]
    }

    pub(crate) const fn deadline_max_micros(&self) -> u64 {
        self.deadline_max_micros
    }
}

impl CalendarColumns {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            positions: Vec::with_capacity(capacity),
            start_micros: Vec::with_capacity(capacity),
            end_micros: Vec::with_capacity(capacity),
            shapes: Vec::with_capacity(capacity),
            rate_seconds: Vec::with_capacity(capacity),
        }
    }

    fn clear(&mut self) {
        self.positions.clear();
        self.start_micros.clear();
        self.end_micros.clear();
        self.shapes.clear();
        self.rate_seconds.clear();
    }

    pub(crate) fn extend(&mut self, segments: &[CalendarRateSegment]) {
        for segment in segments {
            self.positions.push(segment.position);
            self.start_micros.push(segment.start_micros);
            self.end_micros.push(segment.end_micros);
            self.shapes.push(segment.shape);
            self.rate_seconds.push(segment.rate_seconds);
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.positions.len()
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.positions.capacity()
    }

    pub(crate) fn position(&self, index: usize) -> u32 {
        self.positions[index]
    }

    pub(crate) fn start_micros(&self, index: usize) -> u64 {
        self.start_micros[index]
    }

    pub(crate) fn end_micros(&self, index: usize) -> u64 {
        self.end_micros[index]
    }

    pub(crate) fn shape(&self, index: usize) -> f64 {
        self.shapes[index]
    }

    pub(crate) fn rate_seconds(&self, index: usize) -> f64 {
        self.rate_seconds[index]
    }
}

impl CohortColumns {
    fn new(capacity: usize) -> Self {
        Self {
            release_micros: Vec::with_capacity(capacity),
            deadline_micros: Vec::with_capacity(capacity),
            offered_events: Vec::with_capacity(capacity),
            partitions: Vec::with_capacity(capacity),
            demand_classes: Vec::with_capacity(capacity),
            normal_events: 0.0_f64,
            failure_events: 0.0_f64,
        }
    }

    fn clear(&mut self) {
        self.release_micros.clear();
        self.deadline_micros.clear();
        self.offered_events.clear();
        self.partitions.clear();
        self.demand_classes.clear();
        self.normal_events = 0.0_f64;
        self.failure_events = 0.0_f64;
    }

    fn push(&mut self, cohort: Cohort) {
        self.release_micros.push(cohort.release_micros);
        self.deadline_micros.push(cohort.deadline_micros);
        self.offered_events.push(cohort.offered_events);
        self.partitions.push(cohort.partition);
        self.demand_classes.push(cohort.demand_class);
        match cohort.demand_class {
            DemandClass::Normal => self.normal_events += cohort.offered_events,
            DemandClass::Failure => self.failure_events += cohort.offered_events,
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.release_micros.len()
    }

    const fn capacity(&self) -> usize {
        self.release_micros.capacity()
    }

    pub(crate) fn release_micros(&self, index: usize) -> u64 {
        self.release_micros[index]
    }

    pub(crate) fn deadline_micros(&self, index: usize) -> u64 {
        self.deadline_micros[index]
    }

    pub(crate) fn offered_events(&self, index: usize) -> f64 {
        self.offered_events[index]
    }

    pub(crate) fn partition(&self, index: usize) -> u32 {
        self.partitions[index]
    }

    pub(crate) const fn demand_totals(&self) -> (f64, f64) {
        (self.normal_events, self.failure_events)
    }
}

impl BacklogColumns {
    fn new(count: usize) -> Self {
        Self {
            event_counts: vec![0; count],
            oldest_arrival_micros: vec![0; count],
            observed_at_micros: vec![0; count],
            present: vec![0; count],
            normal_events: 0.0_f64,
            failure_events: 0.0_f64,
        }
    }

    fn clear(&mut self) {
        self.present.fill(0);
        self.normal_events = 0.0_f64;
        self.failure_events = 0.0_f64;
    }

    fn set(&mut self, index: usize, backlog: BacklogCohort) -> bool {
        if self.present[index] != 0 {
            return false;
        }
        self.event_counts[index] = backlog.event_count();
        self.oldest_arrival_micros[index] = backlog.oldest_arrival_micros();
        self.observed_at_micros[index] = backlog.observed_at_micros();
        self.present[index] = 1;
        match backlog.demand_class() {
            DemandClass::Normal => self.normal_events += f64::from(backlog.event_count()),
            DemandClass::Failure => self.failure_events += f64::from(backlog.event_count()),
        }
        true
    }

    pub(crate) const fn len(&self) -> usize {
        self.present.len()
    }

    pub(crate) fn is_present(&self, index: usize) -> bool {
        self.present[index] != 0
    }

    pub(crate) fn event_count(&self, index: usize) -> u32 {
        self.event_counts[index]
    }

    pub(crate) fn oldest_arrival_micros(&self, index: usize) -> u64 {
        self.oldest_arrival_micros[index]
    }

    pub(crate) fn observed_at_micros(&self, index: usize) -> u64 {
        self.observed_at_micros[index]
    }

    pub(crate) const fn demand_totals(&self) -> (f64, f64) {
        (self.normal_events, self.failure_events)
    }
}

impl ActuationCommitments {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            launching: LaunchingCommitments {
                from_replicas: Vec::with_capacity(capacity),
                target_replicas: Vec::with_capacity(capacity),
                requested_at: Vec::with_capacity(capacity),
            },
            rebalancing: None,
        }
    }

    fn clear(&mut self) {
        self.launching.from_replicas.clear();
        self.launching.target_replicas.clear();
        self.launching.requested_at.clear();
        self.rebalancing = None;
    }

    fn push(&mut self, commitment: ActuationCommitment) {
        match commitment.phase {
            ActuationPhase::Launching {
                from_replicas,
                target_replicas,
                requested_at,
            } => {
                self.launching.from_replicas.push(from_replicas);
                self.launching.target_replicas.push(target_replicas);
                self.launching.requested_at.push(requested_at);
            }
            ActuationPhase::Rebalancing {
                target_replicas,
                requested_at,
                started_at,
                ..
            } => {
                self.rebalancing = Some(RebalancingCommitment {
                    target_replicas,
                    requested_at,
                    started_at,
                });
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.launching.target_replicas.len() + usize::from(self.rebalancing.is_some())
    }

    const fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) const fn launching_len(&self) -> usize {
        self.launching.target_replicas.len()
    }

    pub(crate) fn launching_target_replicas(&self, index: usize) -> u32 {
        self.launching.target_replicas[index]
    }

    pub(crate) fn launching_requested_at(&self, index: usize) -> ModelTime {
        self.launching.requested_at[index]
    }

    pub(crate) fn launching_direction(&self, index: usize) -> TransitionDirection {
        if self.launching.target_replicas[index] > self.launching.from_replicas[index] {
            TransitionDirection::Up
        } else {
            TransitionDirection::Down
        }
    }

    pub(crate) fn launching_replica_delta(&self, index: usize) -> u32 {
        self.launching.target_replicas[index].abs_diff(self.launching.from_replicas[index])
    }

    pub(crate) const fn rebalancing(&self) -> Option<RebalancingCommitment> {
        self.rebalancing
    }
}

#[derive(Clone, Copy, Debug)]
struct LaunchEvidenceHeader {
    requested_at: ModelTime,
    requested_delta: u32,
    observed_at: ModelTime,
}

/// Borrowed typed input for one controller tick.
#[derive(Debug)]
pub struct GroupObservation<'a> {
    pub(crate) cohorts: &'a CohortColumns,
    pub(crate) backlog: &'a BacklogColumns,
    pub(crate) arrivals: Option<ArrivalEvidence>,
    pub(crate) calendar: Option<CalendarForecast<'a>>,
    pub(crate) scheduled_releases: &'a [ScheduledRelease],
    pub(crate) partition_arrivals: Option<PartitionArrivalEvidence<'a>>,
    pub(crate) resource: Option<OccupancyTraceEvidence<'a>>,
    pub(crate) attempt_outcomes: Option<AttemptOutcomeEvidence>,
    pub(crate) launch: Option<LaunchEvidence<'a>>,
    pub(crate) rebalance: Option<RebalanceEvidence>,
    pub(crate) current_replicas: Option<u32>,
    pub(crate) actuation_commitments: &'a ActuationCommitments,
}

/// One grouped busy-slot transition on the report clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OccupancyTransition {
    offset_micros: u64,
    completed_attempts: u32,
    started_attempts: u32,
}

impl OccupancyTransition {
    /// Constructs one ordered transition group.
    #[must_use]
    pub const fn new(offset_micros: u64, completed_attempts: u32, started_attempts: u32) -> Self {
        Self {
            offset_micros,
            completed_attempts,
            started_attempts,
        }
    }

    /// Returns the offset from the report start.
    #[must_use]
    pub const fn offset_micros(self) -> u64 {
        self.offset_micros
    }

    /// Returns completions at this clock tick.
    #[must_use]
    pub const fn completed_attempts(self) -> u32 {
        self.completed_attempts
    }

    /// Returns starts at this clock tick.
    #[must_use]
    pub const fn started_attempts(self) -> u32 {
        self.started_attempts
    }
}

/// A certified busy-slot path for one resource report.
///
/// The observation buffer constructs this view only after it proves the
/// report counts, state bounds, event order, final state, and occupancy sum.
#[derive(Clone, Copy, Debug)]
pub struct OccupancyTraceEvidence<'a> {
    window: ResourceWindow,
    initial_busy_slots: u32,
    final_busy_slots: u32,
    busy_slot_micros: u128,
    mean_concurrency: f64,
    offsets_micros: &'a [u64],
    completed_attempts: &'a [u32],
    started_attempts: &'a [u32],
}

impl OccupancyTraceEvidence<'_> {
    /// Returns the checked report summary.
    #[must_use]
    pub const fn window(&self) -> &ResourceWindow {
        &self.window
    }

    /// Returns the busy-slot count at the report start.
    #[must_use]
    pub const fn initial_busy_slots(&self) -> u32 {
        self.initial_busy_slots
    }

    /// Returns the busy-slot count at the report end.
    #[must_use]
    pub const fn final_busy_slots(&self) -> u32 {
        self.final_busy_slots
    }

    /// Returns the event-time busy-slot integral in microseconds.
    #[must_use]
    pub const fn busy_slot_micros(&self) -> u128 {
        self.busy_slot_micros
    }

    /// Returns mean busy slots over the complete report.
    #[must_use]
    pub const fn mean_concurrency(&self) -> f64 {
        self.mean_concurrency
    }

    /// Returns the number of grouped transitions.
    #[must_use]
    pub const fn transition_count(&self) -> usize {
        self.offsets_micros.len()
    }

    pub(crate) const fn offsets_micros(&self) -> &[u64] {
        self.offsets_micros
    }

    pub(crate) const fn completion_groups(&self) -> &[u32] {
        self.completed_attempts
    }

    pub(crate) const fn start_groups(&self) -> &[u32] {
        self.started_attempts
    }
}

#[cfg(test)]
pub(crate) const fn occupancy_trace_for_test<'a>(
    window: ResourceWindow,
    initial_busy_slots: u32,
    final_busy_slots: u32,
    busy_slot_micros: u128,
    offsets_micros: &'a [u64],
    completed_attempts: &'a [u32],
    started_attempts: &'a [u32],
) -> OccupancyTraceEvidence<'a> {
    OccupancyTraceEvidence {
        window,
        initial_busy_slots,
        final_busy_slots,
        busy_slot_micros,
        mean_concurrency: window.concurrency(),
        offsets_micros,
        completed_attempts,
        started_attempts,
    }
}

#[derive(Clone, Copy, Debug)]
struct OccupancyTraceHeader {
    window: ResourceWindow,
    initial_busy_slots: u32,
    final_busy_slots: u32,
    busy_slot_micros: u128,
    mean_concurrency: f64,
}

/// Reusable owner for one [`GroupObservation`] view.
#[derive(Debug)]
pub struct ObservationBuffer {
    partition_count: u32,
    replica_count_max: u32,
    resource_concurrency_max: f64,
    resource_exposure_micros: u64,
    resource_attempt_count_max: u32,
    cohorts: CohortColumns,
    backlog: BacklogColumns,
    arrivals: Option<ArrivalEvidence>,
    calendar_artifact: Option<CalendarArtifactId>,
    calendar_prior_probability: f64,
    calendar_segments: CalendarColumns,
    scheduled_release_count_max: usize,
    readiness_lump_count_max: usize,
    scheduled_releases: Vec<ScheduledRelease>,
    partition_arrival_counts: Vec<u32>,
    partition_arrival_token: Option<UpdateToken>,
    resource_trace: Option<OccupancyTraceHeader>,
    resource_transition_offsets_micros: Vec<u64>,
    resource_transition_completed_attempts: Vec<u32>,
    resource_transition_started_attempts: Vec<u32>,
    attempt_outcomes: Option<AttemptOutcomeEvidence>,
    launch_header: Option<LaunchEvidenceHeader>,
    readiness_lumps: Vec<ReadinessLump>,
    rebalance: Option<RebalanceEvidence>,
    current_replicas: Option<u32>,
    actuation_commitments: ActuationCommitments,
}

impl ObservationBuffer {
    /// Allocates one buffer at its validated maximum size.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is invalid for this platform.
    pub fn new(configuration: &Configuration) -> Result<Self, ConfigurationError> {
        configuration.validate()?;
        let cohort_count_max = usize::try_from(configuration.cohort_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let partition_count = usize::try_from(configuration.partition_count)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let calendar_segment_count = usize::try_from(configuration.calendar_segment_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let scheduled_release_count_max =
            usize::try_from(configuration.scheduled_release_count_max)
                .map_err(|_| ConfigurationError::PlatformLimit)?;
        let readiness_lump_count_max = usize::try_from(configuration.readiness_lump_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let replica_count_max = usize::try_from(configuration.replica_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let backlog_count = partition_count
            .checked_mul(DemandClass::COUNT_USIZE)
            .ok_or(ConfigurationError::PlatformLimit)?;
        let resource_transition_count_max = usize::try_from(
            configuration
                .resource_window_attempt_count_max
                .checked_mul(2)
                .and_then(|count| count.checked_add(1))
                .ok_or(ConfigurationError::PlatformLimit)?,
        )
        .map_err(|_| ConfigurationError::PlatformLimit)?;
        Ok(Self {
            partition_count: configuration.partition_count,
            replica_count_max: configuration.replica_count_max,
            resource_concurrency_max: configuration.capacity_concurrency_max()?,
            resource_exposure_micros: configuration.report_interval_micros,
            resource_attempt_count_max: configuration.resource_window_attempt_count_max,
            cohorts: CohortColumns::new(cohort_count_max),
            backlog: BacklogColumns::new(backlog_count),
            arrivals: None,
            calendar_artifact: None,
            calendar_prior_probability: 0.0_f64,
            calendar_segments: CalendarColumns::new(calendar_segment_count),
            scheduled_release_count_max,
            readiness_lump_count_max,
            scheduled_releases: Vec::with_capacity(scheduled_release_count_max),
            partition_arrival_counts: vec![0; partition_count],
            partition_arrival_token: None,
            resource_trace: None,
            resource_transition_offsets_micros: Vec::with_capacity(resource_transition_count_max),
            resource_transition_completed_attempts: Vec::with_capacity(
                resource_transition_count_max,
            ),
            resource_transition_started_attempts: Vec::with_capacity(resource_transition_count_max),
            attempt_outcomes: None,
            launch_header: None,
            readiness_lumps: Vec::with_capacity(readiness_lump_count_max),
            rebalance: None,
            current_replicas: None,
            actuation_commitments: ActuationCommitments::new(replica_count_max),
        })
    }

    /// Clears values without releasing capacity.
    pub fn clear(&mut self) {
        self.cohorts.clear();
        self.backlog.clear();
        self.arrivals = None;
        self.calendar_artifact = None;
        self.calendar_prior_probability = 0.0_f64;
        self.calendar_segments.clear();
        self.scheduled_releases.clear();
        self.partition_arrival_counts.fill(0);
        self.partition_arrival_token = None;
        self.resource_trace = None;
        self.resource_transition_offsets_micros.clear();
        self.resource_transition_completed_attempts.clear();
        self.resource_transition_started_attempts.clear();
        self.attempt_outcomes = None;
        self.launch_header = None;
        self.readiness_lumps.clear();
        self.rebalance = None;
        self.current_replicas = None;
        self.actuation_commitments.clear();
    }

    /// Adds one cohort without growing the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid work or a full cohort buffer.
    pub fn push_cohort(&mut self, cohort: Cohort) -> Result<(), ObservationError> {
        cohort.validate()?;
        if cohort.partition >= self.partition_count {
            return Err(ObservationError::PartitionIndex);
        }
        if self.cohorts.len() == self.cohorts.capacity() {
            return Err(ObservationError::CohortCapacity);
        }
        self.cohorts.push(cohort);
        Ok(())
    }

    /// Sets one partition and class backlog observation.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown partition or a duplicate observation.
    pub fn set_backlog(&mut self, backlog: BacklogCohort) -> Result<(), ObservationError> {
        if backlog.partition() >= self.partition_count {
            return Err(ObservationError::PartitionIndex);
        }
        let index = backlog.partition() as usize * DemandClass::COUNT_USIZE
            + backlog.demand_class().index();
        if !self.backlog.set(index, backlog) {
            return Err(ObservationError::BacklogPending);
        }
        Ok(())
    }

    /// Sets one complete arrival interval.
    ///
    /// # Errors
    ///
    /// Returns an error when exposure is zero.
    pub fn set_arrivals(
        &mut self,
        count: u32,
        exposure_micros: u64,
    ) -> Result<(), ObservationError> {
        if exposure_micros == 0 {
            return Err(ObservationError::ZeroExposure);
        }
        if self.arrivals.is_some() {
            return Err(ObservationError::ArrivalEvidencePending);
        }
        self.arrivals = Some(ArrivalEvidence::new(count, exposure_micros));
        Ok(())
    }

    /// Sets one frozen calendar forecast without growing the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid probability, order, overlap, or capacity.
    pub fn set_calendar_forecast(
        &mut self,
        artifact: CalendarArtifactId,
        prior_probability: f64,
        segments: &[CalendarRateSegment],
    ) -> Result<(), ObservationError> {
        if self.calendar_artifact.is_some() {
            return Err(ObservationError::CalendarForecastPending);
        }
        if !prior_probability.is_finite() || !(0.0_f64..1.0_f64).contains(&prior_probability) {
            return Err(ObservationError::InvalidCalendarProbability);
        }
        if segments.is_empty() || segments.len() > self.calendar_segments.capacity() {
            return Err(ObservationError::CalendarCapacity);
        }
        if segments
            .windows(2)
            .any(|pair| pair[0].end_micros != pair[1].start_micros)
        {
            return Err(ObservationError::CalendarContinuity);
        }
        self.calendar_segments.extend(segments);
        self.calendar_artifact = Some(artifact);
        self.calendar_prior_probability = prior_probability;
        Ok(())
    }

    /// Replaces the complete known future release schedule.
    ///
    /// # Errors
    ///
    /// Returns an error for excess entries, invalid counts, or decreasing
    /// times.
    pub fn set_scheduled_releases(
        &mut self,
        releases: &[ScheduledRelease],
    ) -> Result<(), ObservationError> {
        if releases.len() > self.scheduled_release_count_max {
            return Err(ObservationError::ScheduledReleaseCapacity);
        }
        if releases.iter().any(|release| release.count == 0) {
            return Err(ObservationError::ZeroScheduledReleaseCount);
        }
        if releases
            .windows(2)
            .any(|pair| pair[0].release_micros > pair[1].release_micros)
        {
            return Err(ObservationError::ScheduledReleaseOrder);
        }
        self.scheduled_releases.clear();
        for &release in releases {
            if let Some(previous) = self.scheduled_releases.last_mut()
                && previous.release_micros == release.release_micros
            {
                previous.count = previous
                    .count
                    .checked_add(release.count)
                    .ok_or(ObservationError::CountOverflow)?;
            } else {
                self.scheduled_releases.push(release);
            }
        }
        Ok(())
    }

    /// Sets one complete partition arrival vector and its exposure.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid vector, exposure, or pending update.
    pub fn set_partition_arrivals(
        &mut self,
        counts: &[u32],
        exposure_micros: u64,
    ) -> Result<(), ObservationError> {
        if counts.len() != self.partition_arrival_counts.len() {
            return Err(ObservationError::PartitionCount);
        }
        if exposure_micros == 0 {
            return Err(ObservationError::ZeroExposure);
        }
        if self.arrivals.is_some() || self.partition_arrival_token.is_some() {
            return Err(ObservationError::ArrivalEvidencePending);
        }
        let count = counts.iter().try_fold(0_u32, |sum, &value| {
            sum.checked_add(value)
                .ok_or(ObservationError::CountOverflow)
        })?;
        self.partition_arrival_counts.copy_from_slice(counts);
        self.partition_arrival_token = Some(UpdateToken);
        self.arrivals = Some(ArrivalEvidence::new(count, exposure_micros));
        Ok(())
    }

    /// Sets one resource summary and its complete busy-slot trace.
    ///
    /// # Errors
    ///
    /// Returns an error when the trace violates its certified contract.
    pub fn set_resource_observation(
        &mut self,
        window: ResourceWindow,
        initial_busy_slots: u32,
        final_busy_slots: u32,
        transitions: &[OccupancyTransition],
    ) -> Result<(), ObservationError> {
        if self.resource_trace.is_some() {
            return Err(ObservationError::ResourceWindowPending);
        }
        if window.concurrency() > self.resource_concurrency_max {
            return Err(ObservationError::ResourceConcurrency);
        }
        if window.exposure_micros() != self.resource_exposure_micros {
            return Err(ObservationError::ResourceExposure);
        }
        if window.completed_attempts() > self.resource_attempt_count_max
            || window
                .started_attempts()
                .is_some_and(|count| count > self.resource_attempt_count_max)
        {
            return Err(ObservationError::ResourceAttemptCount);
        }
        if initial_busy_slots > self.resource_concurrency_max as u32
            || final_busy_slots > self.resource_concurrency_max as u32
        {
            return Err(ObservationError::ResourceBusySlots);
        }
        if transitions.len() > self.resource_transition_offsets_micros.capacity() {
            return Err(ObservationError::ResourceTransitionCapacity);
        }
        let mut state = initial_busy_slots;
        let mut previous_offset = 0_u64;
        let mut busy_slot_micros = 0_u128;
        let mut completed_attempts = 0_u32;
        let mut started_attempts = 0_u32;
        for (index, transition) in transitions.iter().copied().enumerate() {
            if transition.offset_micros > window.exposure_micros() {
                return Err(ObservationError::ResourceTransitionTime);
            }
            if index > 0 && transition.offset_micros <= previous_offset {
                return Err(ObservationError::ResourceTransitionOrder);
            }
            let elapsed = transition.offset_micros - previous_offset;
            busy_slot_micros = busy_slot_micros
                .checked_add(u128::from(elapsed) * u128::from(state))
                .ok_or(ObservationError::CountOverflow)?;
            state = state
                .checked_sub(transition.completed_attempts)
                .ok_or(ObservationError::ResourceBusySlots)?;
            state = state
                .checked_add(transition.started_attempts)
                .filter(|value| *value <= self.resource_concurrency_max as u32)
                .ok_or(ObservationError::ResourceBusySlots)?;
            completed_attempts = completed_attempts
                .checked_add(transition.completed_attempts)
                .ok_or(ObservationError::CountOverflow)?;
            started_attempts = started_attempts
                .checked_add(transition.started_attempts)
                .ok_or(ObservationError::CountOverflow)?;
            previous_offset = transition.offset_micros;
        }
        busy_slot_micros = busy_slot_micros
            .checked_add(u128::from(window.exposure_micros() - previous_offset) * u128::from(state))
            .ok_or(ObservationError::CountOverflow)?;
        if state != final_busy_slots
            || completed_attempts != window.completed_attempts()
            || Some(started_attempts) != window.started_attempts()
        {
            return Err(ObservationError::ResourceTraceSummary);
        }
        let derived_concurrency = busy_slot_mean(busy_slot_micros, window.exposure_micros())?;
        let concurrency_error = 8.0_f64
            * f64::EPSILON
            * derived_concurrency
                .abs()
                .max(window.concurrency().abs())
                .max(1.0_f64);
        if (derived_concurrency - window.concurrency()).abs() > concurrency_error {
            return Err(ObservationError::ResourceTraceSummary);
        }
        self.resource_transition_offsets_micros.clear();
        self.resource_transition_completed_attempts.clear();
        self.resource_transition_started_attempts.clear();
        for transition in transitions {
            self.resource_transition_offsets_micros
                .push(transition.offset_micros);
            self.resource_transition_completed_attempts
                .push(transition.completed_attempts);
            self.resource_transition_started_attempts
                .push(transition.started_attempts);
        }
        self.resource_trace = Some(OccupancyTraceHeader {
            window,
            initial_busy_slots,
            final_busy_slots,
            busy_slot_micros,
            mean_concurrency: derived_concurrency,
        });
        Ok(())
    }

    /// Sets one complete attempt-outcome window.
    ///
    /// # Errors
    ///
    /// Returns an error when unconsumed outcome evidence is present.
    pub fn set_attempt_outcomes(
        &mut self,
        evidence: AttemptOutcomeEvidence,
    ) -> Result<(), ObservationError> {
        if self.attempt_outcomes.is_some() {
            return Err(ObservationError::AttemptOutcomePending);
        }
        self.attempt_outcomes = Some(evidence);
        Ok(())
    }

    /// Sets one consumable launch update without growing the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid groups, intervals, or capacity.
    pub fn set_launch_evidence(
        &mut self,
        requested_at: ModelTime,
        requested_delta: u32,
        observed_at: ModelTime,
        lumps: &[ReadinessLump],
    ) -> Result<(), ObservationError> {
        if self.launch_header.is_some() {
            return Err(ObservationError::LaunchEvidencePending);
        }
        if requested_delta == 0 {
            return Err(LaunchEvidenceError::ZeroReplicaDelta.into());
        }
        if observed_at < requested_at {
            return Err(LaunchEvidenceError::ObservationBeforeRequest.into());
        }
        if lumps.len() > self.readiness_lump_count_max {
            return Err(ObservationError::ReadinessLumpCapacity);
        }
        let mut pod_count = 0_u32;
        for (index, lump) in lumps.iter().copied().enumerate() {
            let (lower, upper) = lump.observation().bounds();
            if lower < requested_at || upper > observed_at {
                return Err(LaunchEvidenceError::IntervalOutsideObservation.into());
            }
            if lumps[..index]
                .iter()
                .any(|prior| prior.group() == lump.group())
            {
                return Err(LaunchEvidenceError::DuplicateGroup.into());
            }
            pod_count = pod_count
                .checked_add(lump.pod_count())
                .ok_or(ObservationError::CountOverflow)?;
        }
        if pod_count > requested_delta {
            return Err(LaunchEvidenceError::PodCountExceedsDelta.into());
        }
        self.readiness_lumps.clear();
        self.readiness_lumps.extend_from_slice(lumps);
        self.launch_header = Some(LaunchEvidenceHeader {
            requested_at,
            requested_delta,
            observed_at,
        });
        Ok(())
    }

    /// Sets one consumable rebalance-pause update.
    ///
    /// # Errors
    ///
    /// Returns an error when an unconsumed update is present.
    pub fn set_rebalance_evidence(
        &mut self,
        evidence: RebalanceEvidence,
    ) -> Result<(), ObservationError> {
        if self.rebalance.is_some() {
            return Err(ObservationError::RebalanceEvidencePending);
        }
        self.rebalance = Some(evidence);
        Ok(())
    }

    /// Sets the current warm replica count.
    ///
    /// # Errors
    ///
    /// Returns an error when the count is outside the configured range.
    pub fn set_current_replicas(&mut self, replicas: u32) -> Result<(), ObservationError> {
        if replicas == 0 || replicas > self.replica_count_max {
            return Err(ObservationError::ReplicaCount);
        }
        self.current_replicas = Some(replicas);
        Ok(())
    }

    /// Adds one observed incomplete replica transition.
    ///
    /// # Errors
    ///
    /// Returns an error when a replica count is outside the configured range.
    pub fn push_actuation_commitment(
        &mut self,
        commitment: ActuationCommitment,
    ) -> Result<(), ObservationError> {
        let (from_replicas, target_replicas, duplicate_rebalance) = match commitment.phase {
            ActuationPhase::Launching {
                from_replicas,
                target_replicas,
                ..
            } => (from_replicas, target_replicas, false),
            ActuationPhase::Rebalancing {
                from_replicas,
                target_replicas,
                ..
            } => (
                from_replicas,
                target_replicas,
                self.actuation_commitments.rebalancing().is_some(),
            ),
        };
        if from_replicas > self.replica_count_max
            || target_replicas > self.replica_count_max
            || duplicate_rebalance
            || self.actuation_commitments.len() == self.actuation_commitments.capacity()
        {
            return Err(ObservationError::ActuationCommitment);
        }
        self.actuation_commitments.push(commitment);
        Ok(())
    }

    /// Borrows the current values for one controller transition.
    #[must_use]
    pub fn observation(&mut self) -> GroupObservation<'_> {
        let partition_arrivals =
            self.partition_arrival_token
                .take()
                .map(|token| PartitionArrivalEvidence {
                    counts: &self.partition_arrival_counts,
                    token,
                });
        GroupObservation {
            cohorts: &self.cohorts,
            backlog: &self.backlog,
            arrivals: self.arrivals.take(),
            calendar: self.calendar_artifact.map(|artifact| CalendarForecast {
                artifact,
                prior_probability: self.calendar_prior_probability,
                segments: &self.calendar_segments,
            }),
            scheduled_releases: &self.scheduled_releases,
            partition_arrivals,
            resource: self
                .resource_trace
                .take()
                .map(|header| OccupancyTraceEvidence {
                    window: header.window,
                    initial_busy_slots: header.initial_busy_slots,
                    final_busy_slots: header.final_busy_slots,
                    busy_slot_micros: header.busy_slot_micros,
                    mean_concurrency: header.mean_concurrency,
                    offsets_micros: &self.resource_transition_offsets_micros,
                    completed_attempts: &self.resource_transition_completed_attempts,
                    started_attempts: &self.resource_transition_started_attempts,
                }),
            attempt_outcomes: self.attempt_outcomes.take(),
            launch: self.launch_header.take().map(|header| {
                LaunchEvidence::new(
                    header.requested_at,
                    header.requested_delta,
                    header.observed_at,
                    &self.readiness_lumps,
                )
            }),
            rebalance: self.rebalance.take(),
            current_replicas: self.current_replicas.take(),
            actuation_commitments: &self.actuation_commitments,
        }
    }
}

fn busy_slot_mean(busy_slot_micros: u128, exposure_micros: u64) -> Result<f64, ObservationError> {
    let exposure = u128::from(exposure_micros);
    let whole =
        u32::try_from(busy_slot_micros / exposure).map_err(|_| ObservationError::CountOverflow)?;
    let remainder =
        u64::try_from(busy_slot_micros % exposure).map_err(|_| ObservationError::CountOverflow)?;
    Ok(f64::from(whole)
        + Duration::from_micros(remainder).as_secs_f64()
            / Duration::from_micros(exposure_micros).as_secs_f64())
}

/// Bounded values exported for diagnosis.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecisionDiagnostics {
    /// Posterior scenarios used for this decision.
    pub scenario_count: u32,
    /// Posterior expected arrival rate in events per second.
    pub arrival_rate_per_second: f64,
    /// Posterior expected resource capacity in operations per second.
    pub capacity_per_second: f64,
    /// Lower posterior resource-capacity quantile.
    pub capacity_low_per_second: f64,
    /// Median posterior resource-capacity quantile.
    pub capacity_median_per_second: f64,
    /// Upper posterior resource-capacity quantile.
    pub capacity_high_per_second: f64,
    /// Posterior probability that live concurrency exceeds the resource knee.
    pub saturation_probability: f64,
    /// Posterior probability that no knee exists in the supported range.
    pub no_knee_probability: f64,
    /// Posterior expected scale-up lead time for one replica.
    pub lead_time_up_seconds: f64,
    /// Posterior expected scale-down lead time for one replica.
    pub lead_time_down_seconds: f64,
    /// Posterior expected lead time for the selected or last replica change.
    pub lead_time_seconds: f64,
    /// Posterior expected uncongested handler duration in seconds.
    pub handler_seconds: f64,
    /// Largest posterior expected partition share.
    pub maximum_partition_share: f64,
    /// Largest fractional shortfall at the selected target.
    pub shortfall: f64,
    /// Posterior expected fractional loss at the selected target.
    pub expected_loss: f64,
}

/// An actionable controller decision.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ApplyDecision {
    /// Desired replica count.
    pub target: u32,
    /// Largest safe replica count under saturation evidence.
    pub cap: u32,
    /// Bounded diagnostic values.
    pub diagnostics: DecisionDiagnostics,
}

/// A safe refusal to change replica count.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HoldDecision {
    /// Reason that prevents safe actuation.
    pub reason: HoldReason,
    /// Bounded diagnostic values.
    pub diagnostics: DecisionDiagnostics,
}

/// Reason that prevents a safe target decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldReason {
    /// Model time moved backward.
    ModelTimeRegressed,
    /// A prequential resource check rejected the capacity model.
    CapacityModelMismatch,
}

/// Result of one controller transition.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScaleDecision {
    /// Apply a target and its saturation cap.
    Apply(ApplyDecision),
    /// Keep the current replica count.
    Hold(HoldDecision),
}

/// Invalid construction input.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ConfigurationError {
    /// The capacity model exceeds its artifact or plant contract.
    #[error(transparent)]
    CapacityModel(#[from] crate::CapacityModelError),
    /// A lead-time artifact exceeds its declared budget.
    #[error(transparent)]
    LeadTimePrior(#[from] crate::LeadTimePriorError),
    /// The latency budget is zero.
    #[error("the latency budget must be positive")]
    ZeroBudget,
    /// Epsilon is outside the half-open unit interval.
    #[error("epsilon {epsilon} must be at least zero and less than one")]
    InvalidEpsilon {
        /// Invalid miss fraction.
        epsilon: f64,
    },
    /// The replica-second delay rate is not positive and finite.
    #[error("replica-second delay rate {rate} must be positive and finite")]
    InvalidReplicaSecondDelayRate {
        /// Invalid rate.
        rate: f64,
    },
    /// The failure-service weight is not in the closed unit interval.
    #[error("failure service weight {weight} must be between zero and one")]
    InvalidFailureServiceWeight {
        /// Invalid failure-service weight.
        weight: f64,
    },
    /// A reliability-prior shape is not positive and finite.
    #[error("reliability prior shapes must be positive and finite")]
    InvalidReliabilityPrior,
    /// The physical capacity change rate is not positive and finite.
    #[error("capacity change rate must be positive and finite")]
    InvalidCapacityChangeRate,
    /// A fixed capacity bound is zero.
    #[error("{name} must be positive")]
    ZeroBound {
        /// Name of the zero bound.
        name: &'static str,
    },
    /// The posterior budget cannot provide two draws for each capacity cell.
    #[error("posterior sample count {sample_count} must be at least {minimum}")]
    InsufficientPosteriorSamples {
        /// Configured posterior sample count.
        sample_count: u32,
        /// Minimum sample count for this capacity grid.
        minimum: u32,
    },
    /// A validated count does not fit this platform.
    #[error("a validated count exceeds this platform's address space")]
    PlatformLimit,
    /// Calendar input exceeds the bounded arrival-path representation.
    #[error("calendar segment capacity exceeds arrival path capacity")]
    CalendarPathCapacity,
}

/// Invalid observation input.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ObservationError {
    /// Resource concurrency exceeds the configured plant maximum.
    #[error("resource concurrency exceeds the configured maximum")]
    ResourceConcurrency,
    /// Resource exposure differs from the configured report interval.
    #[error("resource exposure differs from the configured report interval")]
    ResourceExposure,
    /// A resource attempt count exceeds the configured report maximum.
    #[error("resource attempt count exceeds the configured maximum")]
    ResourceAttemptCount,
    /// A busy-slot boundary or transition exceeds the plant range.
    #[error("a resource busy-slot state is outside the configured range")]
    ResourceBusySlots,
    /// A resource trace exceeds its fixed transition bound.
    #[error("the resource trace exceeds its fixed transition bound")]
    ResourceTransitionCapacity,
    /// A resource transition is outside the report interval.
    #[error("a resource transition is outside the report interval")]
    ResourceTransitionTime,
    /// Resource transitions are not strictly ordered and grouped.
    #[error("resource transitions must be strictly ordered and grouped")]
    ResourceTransitionOrder,
    /// A resource summary disagrees with its derived trace values.
    #[error("the resource summary disagrees with its trace")]
    ResourceTraceSummary,
    /// An incomplete actuation has invalid replica counts or exceeds its bound.
    #[error("an actuation commitment is invalid or exceeds its fixed bound")]
    ActuationCommitment,
    /// The buffer contains an unconsumed arrival update token.
    #[error("consume the pending arrival evidence before replacement")]
    ArrivalEvidencePending,
    /// The buffer contains an unconsumed calendar forecast.
    #[error("consume the pending calendar forecast before replacement")]
    CalendarForecastPending,
    /// A scheduled release list exceeds its fixed bound.
    #[error("the scheduled release list exceeds its fixed bound")]
    ScheduledReleaseCapacity,
    /// A scheduled release has no events.
    #[error("a scheduled release count must be positive")]
    ZeroScheduledReleaseCount,
    /// Scheduled release times decrease.
    #[error("scheduled releases must have nondecreasing times")]
    ScheduledReleaseOrder,
    /// A calendar forecast has no segment or exceeds its fixed bound.
    #[error("the calendar forecast is empty or exceeds its fixed bound")]
    CalendarCapacity,
    /// Calendar segments do not form one continuous ordered path.
    #[error("calendar forecast intervals must be continuous and ordered")]
    CalendarContinuity,
    /// A calendar interval is empty or reversed.
    #[error("a calendar interval start must precede its end")]
    InvalidCalendarInterval,
    /// A calendar Gamma shape is not positive and finite.
    #[error("calendar shape must be positive and finite")]
    InvalidCalendarShape,
    /// A calendar Gamma rate is not positive and finite.
    #[error("calendar rate must be positive and finite")]
    InvalidCalendarRate,
    /// Calendar prior probability is outside the open unit interval.
    #[error("calendar prior probability must be between zero and one")]
    InvalidCalendarProbability,
    /// The buffer contains an unconsumed resource window.
    #[error("consume the pending resource window before replacement")]
    ResourceWindowPending,
    /// Attempt outcome evidence was not consumed.
    #[error("consume the pending attempt outcomes before replacement")]
    AttemptOutcomePending,
    /// A partition and class backlog observation was not consumed.
    #[error("backlog evidence is already pending for this partition and class")]
    BacklogPending,
    /// A launch update was not consumed.
    #[error("launch evidence is already pending")]
    LaunchEvidencePending,
    /// A rebalance update was not consumed.
    #[error("rebalance evidence is already pending")]
    RebalanceEvidencePending,
    /// A launch update exceeds its configured scheduling-group bound.
    #[error("the launch evidence exceeds its scheduling-group bound")]
    ReadinessLumpCapacity,
    /// Launch evidence is invalid.
    #[error(transparent)]
    LaunchEvidence(#[from] LaunchEvidenceError),
    /// A warm replica count is outside the configured range.
    #[error("the current replica count is outside the configured range")]
    ReplicaCount,
    /// An evidence counter exceeded its fixed representation.
    #[error("an evidence count exceeds u32")]
    CountOverflow,
    /// The cohort buffer is full.
    #[error("the observation exceeds its cohort capacity")]
    CohortCapacity,
    /// A cohort names an unknown Kafka partition.
    #[error("a cohort partition is outside the configured range")]
    PartitionIndex,
    /// An arrival vector does not match the configured partition count.
    #[error("an arrival vector must match the configured partition count")]
    PartitionCount,
    /// A cohort has no positive service interval.
    #[error("a cohort release must precede its deadline")]
    InvalidCohortInterval,
    /// Cohort work is negative or non-finite.
    #[error("cohort work must be finite and nonnegative")]
    InvalidWork,
    /// A backlog observation contains no work.
    #[error("backlog work must be positive")]
    EmptyBacklog,
    /// The oldest backlog arrival is after the observation time.
    #[error("the oldest backlog arrival must not follow its observation")]
    FutureBacklogArrival,
    /// An arrival interval has no exposure.
    #[error("arrival exposure must be positive")]
    ZeroExposure,
}

/// One consumable partition arrival vector.
#[derive(Debug)]
pub(crate) struct PartitionArrivalEvidence<'a> {
    pub(crate) counts: &'a [u32],
    token: UpdateToken,
}

impl<'a> PartitionArrivalEvidence<'a> {
    pub(crate) fn consume(self) -> &'a [u32] {
        let Self { counts, token } = self;
        drop(token);
        counts
    }
}

#[derive(Debug)]
struct UpdateToken;

impl Drop for UpdateToken {
    fn drop(&mut self) {}
}
