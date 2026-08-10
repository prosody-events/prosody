use std::num::NonZeroU32;

use thiserror::Error;

use crate::{ArrivalEvidence, ResourceWindow, TransitionDirection, TransitionEvidence};

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

/// One Gamma rate posterior for a future calendar interval.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CalendarRateSegment {
    pub(crate) position: u32,
    pub(crate) start_micros: u64,
    pub(crate) end_micros: u64,
    pub(crate) shape: f64,
    pub(crate) rate_seconds: f64,
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
    slo_violation_probability: f64,
}

impl ServiceObjective {
    const DEFAULT_SLO_VIOLATION_PROBABILITY: f64 = 0.05_f64;

    /// Constructs a validated objective.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero budget or an invalid miss fraction.
    pub fn new(budget_micros: u64, epsilon: f64) -> Result<Self, ConfigurationError> {
        if budget_micros == 0 {
            return Err(ConfigurationError::ZeroBudget);
        }
        if !(0.0_f64..1.0_f64).contains(&epsilon) {
            return Err(ConfigurationError::InvalidEpsilon { epsilon });
        }
        Ok(Self {
            budget_micros,
            epsilon,
            slo_violation_probability: Self::DEFAULT_SLO_VIOLATION_PROBABILITY,
        })
    }

    /// Sets the posterior-future probability budget for an SLO violation.
    ///
    /// # Errors
    ///
    /// Returns an error when the probability is outside the open interval
    /// from zero to one half.
    pub fn with_slo_violation_probability(
        mut self,
        probability: f64,
    ) -> Result<Self, ConfigurationError> {
        if !probability.is_finite() || probability <= 0.0_f64 || probability >= 0.5_f64 {
            return Err(ConfigurationError::InvalidSloViolationProbability { probability });
        }
        self.slo_violation_probability = probability;
        Ok(self)
    }

    /// Returns the latency budget in microseconds.
    #[must_use]
    pub const fn budget_micros(self) -> u64 {
        self.budget_micros
    }

    /// Returns the tolerated miss fraction.
    #[must_use]
    pub const fn epsilon(self) -> f64 {
        self.epsilon
    }

    /// Returns the posterior-future probability budget for an SLO violation.
    #[must_use]
    pub const fn slo_violation_probability(self) -> f64 {
        self.slo_violation_probability
    }
}

/// Fixed bounds and model constants.
#[derive(Clone, Debug)]
pub struct Configuration {
    /// Maximum number of cohorts in one observation.
    pub cohort_count_max: u32,
    /// Maximum calendar intervals supplied in one observation.
    pub calendar_segment_count_max: u32,
    /// Configured Kafka partition count.
    pub partition_count: u32,
    /// Maximum allowed replica count.
    pub replica_count_max: u32,
    /// Warm handler slots on each replica.
    pub slots_per_replica: u32,
    /// Number of posterior samples per decision.
    pub posterior_sample_count: u32,
    /// Time between complete telemetry reports.
    pub report_interval_micros: u64,
    /// Maximum failure-service fraction while normal work waits.
    pub failure_service_weight: f64,
    /// Prior for live arrival-rate segments.
    pub arrival_prior: crate::ArrivalPrior,
    /// Prior rate for physical capacity-curve changes.
    pub capacity_change_rate_per_second: f64,
    /// Population prior for class-specific retry probabilities.
    pub reliability_prior: crate::ReliabilityPrior,
    /// Population prior for replica launch time.
    pub launch_time_prior: crate::TransitionPrior,
    /// Population prior for KIP-848 pause time.
    pub rebalance_time_prior: crate::TransitionPrior,
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
        if self.posterior_sample_count == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "posterior_sample_count",
            });
        }
        if self.report_interval_micros == 0 {
            return Err(ConfigurationError::ZeroBound {
                name: "report_interval_micros",
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
            || self.capacity_change_rate_per_second < 0.0_f64
        {
            return Err(ConfigurationError::InvalidCapacityChangeRate);
        }
        Ok(())
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
    pub(crate) from_replicas: u32,
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
                from_replicas,
                target_replicas,
                requested_at,
                started_at,
            } => {
                self.rebalancing = Some(RebalancingCommitment {
                    from_replicas,
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

/// Borrowed typed input for one controller tick.
#[derive(Debug)]
pub struct GroupObservation<'a> {
    pub(crate) cohorts: &'a CohortColumns,
    pub(crate) backlog: &'a BacklogColumns,
    pub(crate) arrivals: Option<ArrivalEvidence>,
    pub(crate) calendar: Option<CalendarForecast<'a>>,
    pub(crate) partition_arrivals: Option<PartitionArrivalEvidence<'a>>,
    pub(crate) resource_window: Option<ResourceWindow>,
    pub(crate) attempt_outcomes: Option<AttemptOutcomeEvidence>,
    pub(crate) transition: Option<TransitionEvidence>,
    pub(crate) current_replicas: Option<u32>,
    pub(crate) actuation_commitments: &'a ActuationCommitments,
}

/// Reusable owner for one [`GroupObservation`] view.
#[derive(Debug)]
pub struct ObservationBuffer {
    partition_count: u32,
    replica_count_max: u32,
    cohorts: CohortColumns,
    backlog: BacklogColumns,
    arrivals: Option<ArrivalEvidence>,
    calendar_artifact: Option<CalendarArtifactId>,
    calendar_prior_probability: f64,
    calendar_segments: CalendarColumns,
    partition_arrival_counts: Vec<u32>,
    partition_arrival_token: Option<UpdateToken>,
    resource_window: Option<ResourceWindow>,
    attempt_outcomes: Option<AttemptOutcomeEvidence>,
    transition: Option<TransitionEvidence>,
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
        let replica_count_max = usize::try_from(configuration.replica_count_max)
            .map_err(|_| ConfigurationError::PlatformLimit)?;
        let backlog_count = partition_count
            .checked_mul(DemandClass::COUNT_USIZE)
            .ok_or(ConfigurationError::PlatformLimit)?;
        Ok(Self {
            partition_count: configuration.partition_count,
            replica_count_max: configuration.replica_count_max,
            cohorts: CohortColumns::new(cohort_count_max),
            backlog: BacklogColumns::new(backlog_count),
            arrivals: None,
            calendar_artifact: None,
            calendar_prior_probability: 0.0_f64,
            calendar_segments: CalendarColumns::new(calendar_segment_count),
            partition_arrival_counts: vec![0; partition_count],
            partition_arrival_token: None,
            resource_window: None,
            attempt_outcomes: None,
            transition: None,
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
        self.partition_arrival_counts.fill(0);
        self.partition_arrival_token = None;
        self.resource_window = None;
        self.attempt_outcomes = None;
        self.transition = None;
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

    /// Sets one passive resource window.
    ///
    /// # Errors
    ///
    /// Returns an error when an unconsumed token is present.
    pub fn set_resource_window(&mut self, window: ResourceWindow) -> Result<(), ObservationError> {
        if self.resource_window.is_some() {
            return Err(ObservationError::ResourceWindowPending);
        }
        self.resource_window = Some(window);
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

    /// Sets one actuation lead-time update token.
    ///
    /// # Errors
    ///
    /// Returns an error when an unconsumed token is present.
    pub fn set_transition(&mut self, evidence: TransitionEvidence) -> Result<(), ObservationError> {
        if self.transition.is_some() {
            return Err(ObservationError::TransitionEvidencePending);
        }
        self.transition = Some(evidence);
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
            partition_arrivals,
            resource_window: self.resource_window.take(),
            attempt_outcomes: self.attempt_outcomes.take(),
            transition: self.transition.take(),
            current_replicas: self.current_replicas.take(),
            actuation_commitments: &self.actuation_commitments,
        }
    }
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
    /// Posterior expected lead time for the selected or last transition bucket.
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
    /// The latency budget is zero.
    #[error("the latency budget must be positive")]
    ZeroBudget,
    /// Epsilon is outside the half-open unit interval.
    #[error("epsilon {epsilon} must be at least zero and less than one")]
    InvalidEpsilon {
        /// Invalid miss fraction.
        epsilon: f64,
    },
    /// The posterior-future SLO violation probability is invalid.
    #[error(
        "SLO violation probability {probability} must be greater than zero and less than one half"
    )]
    InvalidSloViolationProbability {
        /// Invalid probability.
        probability: f64,
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
    /// The physical capacity change rate is negative or non-finite.
    #[error("capacity change rate must be finite and nonnegative")]
    InvalidCapacityChangeRate,
    /// A fixed capacity bound is zero.
    #[error("{name} must be positive")]
    ZeroBound {
        /// Name of the zero bound.
        name: &'static str,
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
    /// An incomplete actuation has invalid replica counts or exceeds its bound.
    #[error("an actuation commitment is invalid or exceeds its fixed bound")]
    ActuationCommitment,
    /// The buffer contains an unconsumed arrival update token.
    #[error("consume the pending arrival evidence before replacement")]
    ArrivalEvidencePending,
    /// The buffer contains an unconsumed calendar forecast.
    #[error("consume the pending calendar forecast before replacement")]
    CalendarForecastPending,
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
    /// An actuation lead-time update was not consumed.
    #[error("transition evidence is already pending")]
    TransitionEvidencePending,
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
