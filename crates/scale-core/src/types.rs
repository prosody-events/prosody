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

/// One discrete posterior view for diagnostics and calibration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PosteriorQuery {
    /// Peak useful throughput.
    Capacity,
    /// Uncongested operation time.
    ServiceTime,
    /// Post-knee throughput collapse.
    Collapse,
    /// Concurrency at peak throughput.
    Knee,
    /// Whether a finite knee exists in the supported range.
    SaturationState,
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

/// One latency objective supplied by a user.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServiceObjective {
    budget_micros: u64,
    epsilon: f64,
}

impl ServiceObjective {
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
        })
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
}

/// Fixed bounds and model constants.
#[derive(Clone, Debug)]
pub struct Configuration {
    /// Maximum number of cohorts in one observation.
    pub cohort_count_max: u32,
    /// Configured Kafka partition count.
    pub partition_count: u32,
    /// Maximum allowed replica count.
    pub replica_count_max: u32,
    /// Warm handler slots on each replica.
    pub slots_per_replica: u32,
    /// Number of posterior samples per decision.
    pub posterior_sample_count: u32,
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
        Ok(())
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WorkCohort {
    pub(crate) release_micros: u64,
    pub(crate) deadline_micros: u64,
    pub(crate) work_slot_seconds: f64,
    pub(crate) partition: u32,
}

impl WorkCohort {
    pub(crate) const fn new(cohort: Cohort, work_slot_seconds: f64) -> Self {
        Self {
            release_micros: cohort.release_micros,
            deadline_micros: cohort.deadline_micros,
            work_slot_seconds,
            partition: cohort.partition,
        }
    }
}

/// Borrowed typed input for one controller tick.
#[derive(Debug)]
pub struct GroupObservation<'a> {
    pub(crate) cohorts: &'a [Cohort],
    pub(crate) arrivals: Option<ArrivalEvidence>,
    pub(crate) partition_arrivals: Option<PartitionArrivalEvidence<'a>>,
    pub(crate) resource_window: Option<ResourceWindow>,
    pub(crate) transition: Option<TransitionEvidence>,
    pub(crate) current_replicas: Option<u32>,
}

/// Reusable owner for one [`GroupObservation`] view.
#[derive(Debug)]
pub struct ObservationBuffer {
    partition_count: u32,
    replica_count_max: u32,
    cohorts: Vec<Cohort>,
    arrivals: Option<ArrivalEvidence>,
    partition_arrival_counts: Vec<u32>,
    partition_arrival_token: Option<UpdateToken>,
    resource_window: Option<ResourceWindow>,
    transition: Option<TransitionEvidence>,
    current_replicas: Option<u32>,
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
        Ok(Self {
            partition_count: configuration.partition_count,
            replica_count_max: configuration.replica_count_max,
            cohorts: Vec::with_capacity(cohort_count_max),
            arrivals: None,
            partition_arrival_counts: vec![0; partition_count],
            partition_arrival_token: None,
            resource_window: None,
            transition: None,
            current_replicas: None,
        })
    }

    /// Clears values without releasing capacity.
    pub fn clear(&mut self) {
        self.cohorts.clear();
        self.arrivals = None;
        self.partition_arrival_counts.fill(0);
        self.partition_arrival_token = None;
        self.resource_window = None;
        self.transition = None;
        self.current_replicas = None;
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
            arrivals: self.arrivals.take(),
            partition_arrivals,
            resource_window: self.resource_window.take(),
            transition: self.transition.take(),
            current_replicas: self.current_replicas.take(),
        }
    }
}

/// Bounded values exported for diagnosis.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DecisionDiagnostics {
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
    /// A fixed capacity bound is zero.
    #[error("{name} must be positive")]
    ZeroBound {
        /// Name of the zero bound.
        name: &'static str,
    },
    /// A validated count does not fit this platform.
    #[error("a validated count exceeds this platform's address space")]
    PlatformLimit,
}

/// Invalid observation input.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ObservationError {
    /// The buffer contains an unconsumed arrival update token.
    #[error("consume the pending arrival evidence before replacement")]
    ArrivalEvidencePending,
    /// The buffer contains an unconsumed resource window.
    #[error("consume the pending resource window before replacement")]
    ResourceWindowPending,
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
