use std::f64::consts::{SQRT_2, TAU};
use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use thiserror::Error;

use crate::change_point::ChangePointKernel;
use crate::types::prior_artifact_contract_holds;
use crate::{
    ModelTime, PriorArtifactBudget, PriorArtifactIdentity, PriorCoverageRecord, RandomStream,
};

const DEFAULT_LAUNCH_ARTIFACT: PriorArtifactIdentity =
    PriorArtifactIdentity::new(0x4c41_554e_4348, 1, 0x4c41_554e_4348_0001);
const DEFAULT_REBALANCE_ARTIFACT: PriorArtifactIdentity =
    PriorArtifactIdentity::new(0x5245_4241_4c41, 1, 0x5245_4241_4c41_0001);

/// Direction of one replica transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionDirection {
    /// The requested replica count increased.
    Up,
    /// The requested replica count decreased.
    Down,
}

/// One stable scheduling-group identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReadinessGroupId(pub u64);

/// One readiness interval from a telemetry poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessObservation {
    /// The group became ready in this interval.
    Ready {
        /// Exclusive prior observation bound.
        after: ModelTime,
        /// Inclusive ready-time bound.
        at_or_before: ModelTime,
    },
    /// The group stayed pending through this interval.
    Pending {
        /// Exclusive prior observation bound.
        after: ModelTime,
        /// Inclusive current observation bound.
        through: ModelTime,
    },
}

impl ReadinessObservation {
    /// Constructs one completed readiness interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the interval is empty or reversed.
    pub fn ready(after: ModelTime, at_or_before: ModelTime) -> Result<Self, LaunchEvidenceError> {
        if at_or_before <= after {
            return Err(LaunchEvidenceError::InvalidInterval);
        }
        Ok(Self::Ready {
            after,
            at_or_before,
        })
    }

    /// Constructs one incremental pending interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the interval is empty or reversed.
    pub fn pending(after: ModelTime, through: ModelTime) -> Result<Self, LaunchEvidenceError> {
        if through <= after {
            return Err(LaunchEvidenceError::InvalidInterval);
        }
        Ok(Self::Pending { after, through })
    }

    pub(crate) const fn bounds(self) -> (ModelTime, ModelTime) {
        match self {
            Self::Ready {
                after,
                at_or_before,
            } => (after, at_or_before),
            Self::Pending { after, through } => (after, through),
        }
    }
}

/// One observed scheduling group and its shared readiness time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessLump {
    group: ReadinessGroupId,
    pod_count: u32,
    observation: ReadinessObservation,
}

impl ReadinessLump {
    /// Constructs one readiness-lump observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the group has no pod.
    pub fn new(
        group: ReadinessGroupId,
        pod_count: u32,
        observation: ReadinessObservation,
    ) -> Result<Self, LaunchEvidenceError> {
        if pod_count == 0 {
            return Err(LaunchEvidenceError::ZeroPodCount);
        }
        Ok(Self {
            group,
            pod_count,
            observation,
        })
    }

    /// Returns the stable group identity.
    #[must_use]
    pub const fn group(self) -> ReadinessGroupId {
        self.group
    }

    /// Returns the pods that share this readiness time.
    #[must_use]
    pub const fn pod_count(self) -> u32 {
        self.pod_count
    }

    /// Returns the readiness interval.
    #[must_use]
    pub const fn observation(self) -> ReadinessObservation {
        self.observation
    }
}

/// One consumable launch update over a fixed scheduling-group layout.
///
/// The model conditions on group identities and pod counts. Each pending
/// interval starts at the prior consumed bound. This rule prevents repeated
/// polls from adding the same survival evidence twice.
///
/// One update can contain only the groups observed since the prior update.
/// Its pod count can therefore be less than the requested replica change.
#[derive(Debug)]
pub struct LaunchEvidence<'a> {
    requested_at: ModelTime,
    requested_delta: u32,
    observed_at: ModelTime,
    lumps: &'a [ReadinessLump],
    token: EvidenceToken,
}

impl<'a> LaunchEvidence<'a> {
    pub(crate) fn new(
        requested_at: ModelTime,
        requested_delta: u32,
        observed_at: ModelTime,
        lumps: &'a [ReadinessLump],
    ) -> Self {
        Self {
            requested_at,
            requested_delta,
            observed_at,
            lumps,
            token: EvidenceToken,
        }
    }

    fn consume(self) -> LaunchEvidenceValues<'a> {
        let Self {
            requested_at,
            requested_delta,
            observed_at,
            lumps,
            token,
        } = self;
        drop(token);
        LaunchEvidenceValues {
            requested_at,
            requested_delta,
            observed_at,
            lumps,
        }
    }
}

/// One consumable rebalance-pause interval.
#[derive(Debug)]
pub struct RebalanceEvidence {
    observation: ReadinessObservation,
    token: EvidenceToken,
}

impl RebalanceEvidence {
    /// Constructs one completed pause interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the interval is empty or reversed.
    pub fn completed(
        after: ModelTime,
        at_or_before: ModelTime,
    ) -> Result<Self, LaunchEvidenceError> {
        Ok(Self {
            observation: ReadinessObservation::ready(after, at_or_before)?,
            token: EvidenceToken,
        })
    }

    /// Constructs one incremental pending pause interval.
    ///
    /// # Errors
    ///
    /// Returns an error when the interval is empty or reversed.
    pub fn pending(after: ModelTime, through: ModelTime) -> Result<Self, LaunchEvidenceError> {
        Ok(Self {
            observation: ReadinessObservation::pending(after, through)?,
            token: EvidenceToken,
        })
    }

    fn consume(self) -> ReadinessObservation {
        let Self { observation, token } = self;
        drop(token);
        observation
    }
}

/// One log-normal duration hypothesis.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DurationCell {
    mu_log_seconds: f64,
    sigma_log_seconds: f64,
}

impl DurationCell {
    /// Constructs one positive finite duration cell.
    ///
    /// # Errors
    ///
    /// Returns an error when the median or deviation is invalid.
    pub fn new(
        median_seconds: f64,
        log_standard_deviation: f64,
    ) -> Result<Self, LeadTimePriorError> {
        if !median_seconds.is_finite()
            || median_seconds <= 0.0_f64
            || !log_standard_deviation.is_finite()
            || log_standard_deviation <= 0.0_f64
        {
            return Err(LeadTimePriorError::InvalidDurationCell);
        }
        Ok(Self {
            mu_log_seconds: median_seconds.ln(),
            sigma_log_seconds: log_standard_deviation,
        })
    }

    /// Returns the duration median in seconds.
    #[must_use]
    pub fn median_seconds(self) -> f64 {
        self.mu_log_seconds.exp()
    }
}

/// One joint prior for launch readiness.
#[derive(Clone, Debug, PartialEq)]
pub struct LaunchPrior {
    artifact: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: Box<[PriorCoverageRecord]>,
    intercepts: Box<[f64]>,
    slopes: Box<[f64]>,
    fast_cells: Box<[DurationCell]>,
    slow_cells: Box<[DurationCell]>,
    probabilities: Box<[f64]>,
    change_kernel: ChangePointKernel,
}

/// Borrowed axes for one launch prior product grid.
#[derive(Clone, Copy, Debug)]
pub struct LaunchPriorGrid<'a> {
    intercepts: &'a [f64],
    slopes: &'a [f64],
    fast_cells: &'a [DurationCell],
    slow_cells: &'a [DurationCell],
}

/// Posterior launch mixture for one replica delta.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LaunchComponentSummary {
    /// Posterior probability of the slow launch mode.
    pub slow_probability: f64,
    /// Conditional posterior mean for the fast mode in seconds.
    pub fast_mean_seconds: f64,
    /// Conditional posterior mean for the slow mode in seconds.
    pub slow_mean_seconds: f64,
}

impl<'a> LaunchPriorGrid<'a> {
    /// Groups the four product-grid axes for prior construction.
    #[must_use]
    pub const fn new(
        intercepts: &'a [f64],
        slopes: &'a [f64],
        fast_cells: &'a [DurationCell],
        slow_cells: &'a [DurationCell],
    ) -> Self {
        Self {
            intercepts,
            slopes,
            fast_cells,
            slow_cells,
        }
    }
}

impl LaunchPrior {
    /// Returns this prior's artifact identity.
    #[must_use]
    pub const fn artifact(&self) -> PriorArtifactIdentity {
        self.artifact
    }

    /// Returns this prior's approximation budget.
    #[must_use]
    pub const fn budget(&self) -> PriorArtifactBudget {
        self.budget
    }

    /// Returns the recorded predictive coverage.
    #[must_use]
    pub fn coverage(&self) -> &[PriorCoverageRecord] {
        &self.coverage
    }

    pub(crate) fn coverage_support_seconds(&self) -> (f64, f64) {
        coverage_support(&self.coverage)
    }

    /// Constructs one normalized joint readiness prior.
    ///
    /// Probability order is intercept, slope, fast cell, and slow cell.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid axes, coverage, mass, or artifact budgets.
    pub fn new(
        artifact: PriorArtifactIdentity,
        budget: PriorArtifactBudget,
        coverage: &[PriorCoverageRecord],
        grid: LaunchPriorGrid<'_>,
        probabilities: &[f64],
        change_rate_per_second: f64,
    ) -> Result<Self, LeadTimePriorError> {
        let LaunchPriorGrid {
            intercepts,
            slopes,
            fast_cells,
            slow_cells,
        } = grid;
        if intercepts.is_empty()
            || slopes.is_empty()
            || fast_cells.is_empty()
            || slow_cells.is_empty()
            || !intercepts.iter().all(|value| value.is_finite())
            || !slopes
                .iter()
                .all(|value| value.is_finite() && *value >= 0.0_f64)
            || !change_rate_per_second.is_finite()
            || change_rate_per_second < 0.0_f64
        {
            return Err(LeadTimePriorError::InvalidAxis);
        }
        let hypothesis_count = checked_product(&[
            intercepts.len(),
            slopes.len(),
            fast_cells.len(),
            slow_cells.len(),
        ])?;
        validate_artifact(artifact, budget, coverage, hypothesis_count, probabilities)?;
        let mut probabilities = probabilities.to_vec();
        normalize(&mut probabilities)?;
        let [fast_coverage, slow_coverage, ..] = coverage else {
            return Err(LeadTimePriorError::CoverageCount);
        };
        validate_duration_support(fast_cells, *fast_coverage, budget)?;
        validate_duration_support(slow_cells, *slow_coverage, budget)?;
        Ok(Self {
            artifact,
            budget,
            coverage: coverage.into(),
            intercepts: intercepts.into(),
            slopes: slopes.into(),
            fast_cells: fast_cells.into(),
            slow_cells: slow_cells.into(),
            probabilities: probabilities.into(),
            change_kernel: ChangePointKernel::new(change_rate_per_second),
        })
    }

    /// Returns the supported Kubernetes launch artifact.
    ///
    /// The fast support includes cached starts and cold pulls. The slow
    /// support includes optimized nodes, cold images, and capacity retries.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded artifact fails its own contract.
    pub fn kubernetes() -> Result<Self, LeadTimePriorError> {
        let fast = [
            DurationCell::new(5.0_f64, 0.25_f64)?,
            DurationCell::new(15.0_f64, 0.25_f64)?,
            DurationCell::new(30.0_f64, 0.25_f64)?,
        ];
        let slow = [
            DurationCell::new(60.0_f64, 0.3_f64)?,
            DurationCell::new(150.0_f64, 0.3_f64)?,
            DurationCell::new(360.0_f64, 0.3_f64)?,
        ];
        let coverage = [
            PriorCoverageRecord::new(0.5_f64, 120.0_f64, 1.0e-18_f64, 1.5e-8_f64, 1.0e-5_f64),
            PriorCoverageRecord::new(15.0_f64, 1_800.0_f64, 2.0e-6_f64, 5.0e-7_f64, 1.0e-5_f64),
        ];
        let budget = PriorArtifactBudget::new(64, 2_048, 4_096, 1.0e-4_f64, 0.001_f64, 1.0e-4_f64);
        let intercepts = [
            -2.197_224_577_336_219_6_f64,
            0.0_f64,
            2.197_224_577_336_219_6_f64,
        ];
        let slopes = [0.0_f64, 0.5_f64];
        let mut probabilities = [0.0_f64; 54];
        let intercept_mass = [0.25_f64, 0.5_f64, 0.25_f64];
        let slope_mass = [0.7_f64, 0.3_f64];
        let duration_mass = [0.2_f64, 0.6_f64, 0.2_f64];
        let mut index = 0;
        for intercept in intercept_mass {
            for slope in slope_mass {
                for fast_mass in duration_mass {
                    for slow_mass in duration_mass {
                        probabilities[index] = intercept * slope * fast_mass * slow_mass;
                        index += 1;
                    }
                }
            }
        }
        Self::new(
            DEFAULT_LAUNCH_ARTIFACT,
            budget,
            &coverage,
            LaunchPriorGrid::new(&intercepts, &slopes, &fast, &slow),
            &probabilities,
            0.0_f64,
        )
    }

    pub(crate) fn validate_update_budget(
        &self,
        readiness_lump_count_max: u32,
    ) -> Result<(), LeadTimePriorError> {
        let operations = u64::try_from(self.probabilities.len())
            .map_err(|_| LeadTimePriorError::GridSize)?
            .checked_mul(u64::from(readiness_lump_count_max))
            .ok_or(LeadTimePriorError::GridSize)?;
        if operations > self.budget.update_operation_count_max() {
            return Err(LeadTimePriorError::UpdateBudget);
        }
        Ok(())
    }
}

/// One supported prior for a rebalance pause.
#[derive(Clone, Debug, PartialEq)]
pub struct RebalancePrior {
    artifact: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: Box<[PriorCoverageRecord]>,
    cells: Box<[DurationCell]>,
    probabilities: Box<[f64]>,
    change_kernel: ChangePointKernel,
}

impl RebalancePrior {
    /// Returns this prior's artifact identity.
    #[must_use]
    pub const fn artifact(&self) -> PriorArtifactIdentity {
        self.artifact
    }

    /// Returns this prior's approximation budget.
    #[must_use]
    pub const fn budget(&self) -> PriorArtifactBudget {
        self.budget
    }

    /// Returns the recorded predictive coverage.
    #[must_use]
    pub fn coverage(&self) -> &[PriorCoverageRecord] {
        &self.coverage
    }

    pub(crate) fn coverage_support_seconds(&self) -> (f64, f64) {
        coverage_support(&self.coverage)
    }

    /// Constructs one normalized rebalance-duration prior.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid coverage, mass, or artifact budgets.
    pub fn new(
        artifact: PriorArtifactIdentity,
        budget: PriorArtifactBudget,
        coverage: &[PriorCoverageRecord],
        cells: &[DurationCell],
        probabilities: &[f64],
        change_rate_per_second: f64,
    ) -> Result<Self, LeadTimePriorError> {
        if cells.is_empty()
            || !change_rate_per_second.is_finite()
            || change_rate_per_second < 0.0_f64
        {
            return Err(LeadTimePriorError::InvalidAxis);
        }
        validate_artifact(artifact, budget, coverage, cells.len(), probabilities)?;
        let mut probabilities = probabilities.to_vec();
        normalize(&mut probabilities)?;
        let Some(&duration_coverage) = coverage.first() else {
            return Err(LeadTimePriorError::CoverageCount);
        };
        validate_duration_support(cells, duration_coverage, budget)?;
        Ok(Self {
            artifact,
            budget,
            coverage: coverage.into(),
            cells: cells.into(),
            probabilities: probabilities.into(),
            change_kernel: ChangePointKernel::new(change_rate_per_second),
        })
    }

    /// Returns the supported KIP-848 pause artifact.
    ///
    /// The support includes normal handoffs and the 45-second crash path.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded artifact fails its own contract.
    pub fn kip848() -> Result<Self, LeadTimePriorError> {
        let cells = [
            DurationCell::new(0.05_f64, 0.3_f64)?,
            DurationCell::new(0.2_f64, 0.3_f64)?,
            DurationCell::new(1.0_f64, 0.3_f64)?,
            DurationCell::new(10.0_f64, 0.3_f64)?,
            DurationCell::new(45.0_f64, 0.3_f64)?,
            DurationCell::new(120.0_f64, 0.3_f64)?,
        ];
        let coverage = [PriorCoverageRecord::new(
            0.005_f64,
            600.0_f64,
            1.0e-14_f64,
            4.0e-8_f64,
            1.0e-5_f64,
        )];
        Self::new(
            DEFAULT_REBALANCE_ARTIFACT,
            PriorArtifactBudget::new(8, 512, 512, 1.0e-4_f64, 0.001_f64, 1.0e-4_f64),
            &coverage,
            &cells,
            &[0.25_f64, 0.25_f64, 0.2_f64, 0.1_f64, 0.15_f64, 0.05_f64],
            0.0_f64,
        )
    }
}

struct LaunchEvidenceValues<'a> {
    requested_at: ModelTime,
    requested_delta: u32,
    observed_at: ModelTime,
    lumps: &'a [ReadinessLump],
}

pub(crate) struct LaunchTimeFactor {
    prior: LaunchPrior,
    weights: Vec<f64>,
    likelihoods: Vec<f64>,
    last_replica_delta: u32,
}

impl LaunchTimeFactor {
    pub(crate) fn new(prior: &LaunchPrior) -> Self {
        Self {
            prior: prior.clone(),
            weights: prior.probabilities.to_vec(),
            likelihoods: vec![0.0_f64; prior.probabilities.len()],
            last_replica_delta: 1,
        }
    }

    pub(crate) fn transition(&mut self, elapsed: Duration) {
        let transition = self.prior.change_kernel.probabilities(elapsed);
        for (weight, prior) in self.weights.iter_mut().zip(&self.prior.probabilities) {
            *weight = transition.retained * *weight + transition.redrawn * prior;
        }
    }

    pub(crate) fn update(&mut self, simd_level: Level, evidence: LaunchEvidence<'_>) {
        let evidence = evidence.consume();
        self.last_replica_delta = evidence.requested_delta;
        self.likelihoods.fill(0.0_f64);
        for lump in evidence.lumps {
            let observation = lump.observation();
            for hypothesis in 0..self.weights.len() {
                let components = self.components(hypothesis, evidence.requested_delta);
                let contribution =
                    launch_observation_probability(components, observation, evidence.requested_at);
                self.likelihoods[hypothesis] += contribution.max(f64::MIN_POSITIVE).ln();
            }
        }
        debug_assert!(
            evidence.observed_at >= evidence.requested_at,
            "validated launch evidence must not precede its request"
        );
        apply_log_likelihood(simd_level, &mut self.weights, &mut self.likelihoods);
    }

    pub(crate) fn expected_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
    ) -> f64 {
        self.weights
            .iter()
            .enumerate()
            .map(|(hypothesis, weight)| {
                let components = self.components(hypothesis, replica_delta);
                let slow_probability = match direction {
                    TransitionDirection::Up => components.slow_probability,
                    TransitionDirection::Down => 0.0_f64,
                };
                weight
                    * ((1.0_f64 - slow_probability) * log_normal_mean(components.fast)
                        + slow_probability * log_normal_mean(components.slow))
            })
            .sum()
    }

    pub(crate) fn expected_last_seconds(&self) -> f64 {
        self.expected_seconds(TransitionDirection::Up, self.last_replica_delta)
    }

    pub(crate) fn component_summary(&self, replica_delta: u32) -> LaunchComponentSummary {
        let mut slow_probability = 0.0_f64;
        let mut fast_mean_seconds = 0.0_f64;
        let mut slow_mean_seconds = 0.0_f64;
        for (hypothesis, weight) in self.weights.iter().copied().enumerate() {
            let components = self.components(hypothesis, replica_delta);
            let slow_mass = weight * components.slow_probability;
            let fast_mass = weight - slow_mass;
            slow_probability += slow_mass;
            fast_mean_seconds += fast_mass * log_normal_mean(components.fast);
            slow_mean_seconds += slow_mass * log_normal_mean(components.slow);
        }
        let fast_probability = 1.0_f64 - slow_probability;
        LaunchComponentSummary {
            slow_probability,
            fast_mean_seconds: if fast_probability > 0.0_f64 {
                fast_mean_seconds / fast_probability
            } else {
                0.0_f64
            },
            slow_mean_seconds: if slow_probability > 0.0_f64 {
                slow_mean_seconds / slow_probability
            } else {
                0.0_f64
            },
        }
    }

    pub(crate) fn last_component_summary(&self) -> LaunchComponentSummary {
        self.component_summary(self.last_replica_delta)
    }

    pub(crate) fn sample_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        random: &mut RandomStream,
    ) -> f64 {
        let hypothesis = sample_weight_index(&self.weights, random);
        let components = self.components(hypothesis, replica_delta);
        let cell = if direction == TransitionDirection::Up
            && random.open_unit_f64() < components.slow_probability
        {
            components.slow
        } else {
            components.fast
        };
        sample_log_normal(cell, random)
    }

    pub(crate) fn predictive_cdf(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        elapsed_seconds: f64,
    ) -> f64 {
        if !elapsed_seconds.is_finite() || elapsed_seconds <= 0.0_f64 {
            return 0.0_f64;
        }
        self.weights
            .iter()
            .enumerate()
            .map(|(hypothesis, weight)| {
                let components = self.components(hypothesis, replica_delta);
                let slow_probability = match direction {
                    TransitionDirection::Up => components.slow_probability,
                    TransitionDirection::Down => 0.0_f64,
                };
                weight
                    * ((1.0_f64 - slow_probability)
                        * log_normal_cdf(components.fast, elapsed_seconds)
                        + slow_probability * log_normal_cdf(components.slow, elapsed_seconds))
            })
            .sum::<f64>()
            .clamp(0.0_f64, 1.0_f64)
    }

    pub(crate) fn predictive_quantile(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        probability: f64,
    ) -> Result<f64, PredictiveQuantileError> {
        let support = coverage_support(&self.prior.coverage);
        validate_boundary_mass(
            self.boundary_mass(direction, replica_delta)?,
            self.prior.budget.boundary_probability_max(),
        )?;
        invert_predictive(
            probability,
            self.prior.budget.path_time_error_seconds(),
            support,
            |seconds| self.predictive_cdf(direction, replica_delta, seconds),
        )
    }

    pub(crate) fn sample_remaining_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        elapsed_seconds: f64,
        random: &mut RandomStream,
    ) -> f64 {
        let mut total_survival = 0.0_f64;
        for (hypothesis, weight) in self.weights.iter().copied().enumerate() {
            let components = self.components(hypothesis, replica_delta);
            let slow_probability = match direction {
                TransitionDirection::Up => components.slow_probability,
                TransitionDirection::Down => 0.0_f64,
            };
            total_survival += weight
                * ((1.0_f64 - slow_probability)
                    * (1.0_f64 - log_normal_cdf(components.fast, elapsed_seconds))
                    + slow_probability
                        * (1.0_f64 - log_normal_cdf(components.slow, elapsed_seconds)));
        }
        let selector = random.open_unit_f64() * total_survival;
        let mut cumulative = 0.0_f64;
        let mut selected = self.prior.fast_cells[0];
        'hypotheses: for (hypothesis, weight) in self.weights.iter().copied().enumerate() {
            let components = self.components(hypothesis, replica_delta);
            let slow_probability = match direction {
                TransitionDirection::Up => components.slow_probability,
                TransitionDirection::Down => 0.0_f64,
            };
            for (cell, mode_probability) in [
                (components.fast, 1.0_f64 - slow_probability),
                (components.slow, slow_probability),
            ] {
                cumulative +=
                    weight * mode_probability * (1.0_f64 - log_normal_cdf(cell, elapsed_seconds));
                if cumulative >= selector {
                    selected = cell;
                    break 'hypotheses;
                }
            }
        }
        sample_log_normal_after(
            selected,
            elapsed_seconds,
            self.prior.budget.path_time_error_seconds(),
            random,
        ) - elapsed_seconds
    }

    pub(crate) fn posterior_value_count(&self) -> u32 {
        u32::try_from(self.prior.fast_cells.len() + self.prior.slow_cells.len())
            .map_or(u32::MAX, |count| count)
    }

    pub(crate) fn write_posterior(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> bool {
        let fast_count = self.prior.fast_cells.len();
        if values.len() != fast_count + self.prior.slow_cells.len()
            || probabilities.len() != values.len()
        {
            return false;
        }
        for (value, cell) in values[..fast_count].iter_mut().zip(&self.prior.fast_cells) {
            *value = cell.median_seconds();
        }
        for (value, cell) in values[fast_count..].iter_mut().zip(&self.prior.slow_cells) {
            *value = cell.median_seconds();
        }
        probabilities.fill(0.0_f64);
        for (hypothesis, weight) in self.weights.iter().copied().enumerate() {
            let indices = self.indices(hypothesis);
            let slow_probability = match direction {
                TransitionDirection::Up => self.slow_probability(indices, replica_delta),
                TransitionDirection::Down => 0.0_f64,
            };
            probabilities[indices.fast] += weight * (1.0_f64 - slow_probability);
            probabilities[fast_count + indices.slow] += weight * slow_probability;
        }
        true
    }

    fn components(&self, hypothesis: usize, replica_delta: u32) -> MixtureComponents {
        let indices = self.indices(hypothesis);
        MixtureComponents {
            slow_probability: self.slow_probability(indices, replica_delta),
            fast: self.prior.fast_cells[indices.fast],
            slow: self.prior.slow_cells[indices.slow],
        }
    }

    fn boundary_mass(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
    ) -> Result<f64, PredictiveQuantileError> {
        let [fast_coverage, slow_coverage, ..] = self.prior.coverage.as_ref() else {
            return Err(PredictiveQuantileError::CoverageInvariant);
        };
        Ok(self
            .weights
            .iter()
            .copied()
            .enumerate()
            .map(|(hypothesis, weight)| {
                let components = self.components(hypothesis, replica_delta);
                let slow_probability = match direction {
                    TransitionDirection::Up => components.slow_probability,
                    TransitionDirection::Down => 0.0_f64,
                };
                weight
                    * ((1.0_f64 - slow_probability)
                        * duration_boundary_mass(components.fast, *fast_coverage)
                        + slow_probability
                            * duration_boundary_mass(components.slow, *slow_coverage))
            })
            .sum())
    }

    fn slow_probability(&self, indices: HypothesisIndices, replica_delta: u32) -> f64 {
        let delta_log = f64::from(replica_delta.max(1)).ln();
        logistic(
            self.prior.intercepts[indices.intercept] + self.prior.slopes[indices.slope] * delta_log,
        )
    }

    fn indices(&self, hypothesis: usize) -> HypothesisIndices {
        let slow_count = self.prior.slow_cells.len();
        let fast_count = self.prior.fast_cells.len();
        let slope_count = self.prior.slopes.len();
        let slow = hypothesis % slow_count;
        let quotient = hypothesis / slow_count;
        let fast = quotient % fast_count;
        let quotient = quotient / fast_count;
        HypothesisIndices {
            intercept: quotient / slope_count,
            slope: quotient % slope_count,
            fast,
            slow,
        }
    }

    #[cfg(test)]
    pub(crate) fn posterior_weights(&self) -> &[f64] {
        &self.weights
    }
}

pub(crate) struct RebalanceTimeFactor {
    prior: RebalancePrior,
    weights: Vec<f64>,
    likelihoods: Vec<f64>,
}

impl RebalanceTimeFactor {
    pub(crate) fn new(prior: &RebalancePrior) -> Self {
        Self {
            prior: prior.clone(),
            weights: prior.probabilities.to_vec(),
            likelihoods: vec![0.0_f64; prior.probabilities.len()],
        }
    }

    pub(crate) fn transition(&mut self, elapsed: Duration) {
        let transition = self.prior.change_kernel.probabilities(elapsed);
        for (weight, prior) in self.weights.iter_mut().zip(&self.prior.probabilities) {
            *weight = transition.retained * *weight + transition.redrawn * prior;
        }
    }

    pub(crate) fn update(&mut self, simd_level: Level, evidence: RebalanceEvidence) {
        let observation = evidence.consume();
        for (likelihood, cell) in self.likelihoods.iter_mut().zip(&self.prior.cells) {
            *likelihood = observation_probability(*cell, observation, ModelTime::from_micros(0))
                .max(f64::MIN_POSITIVE)
                .ln();
        }
        apply_log_likelihood(simd_level, &mut self.weights, &mut self.likelihoods);
    }

    pub(crate) fn expected_seconds(&self) -> f64 {
        self.weights
            .iter()
            .zip(&self.prior.cells)
            .map(|(weight, cell)| weight * log_normal_mean(*cell))
            .sum()
    }

    pub(crate) fn sample_seconds(&self, random: &mut RandomStream) -> f64 {
        sample_log_normal(
            self.prior.cells[sample_weight_index(&self.weights, random)],
            random,
        )
    }

    pub(crate) fn predictive_cdf(&self, elapsed_seconds: f64) -> f64 {
        if !elapsed_seconds.is_finite() || elapsed_seconds <= 0.0_f64 {
            return 0.0_f64;
        }
        self.weights
            .iter()
            .zip(&self.prior.cells)
            .map(|(weight, cell)| weight * log_normal_cdf(*cell, elapsed_seconds))
            .sum::<f64>()
            .clamp(0.0_f64, 1.0_f64)
    }

    pub(crate) fn predictive_quantile(
        &self,
        probability: f64,
    ) -> Result<f64, PredictiveQuantileError> {
        let support = coverage_support(&self.prior.coverage);
        validate_boundary_mass(
            self.predictive_cdf(support.0) + 1.0_f64 - self.predictive_cdf(support.1),
            self.prior.budget.boundary_probability_max(),
        )?;
        invert_predictive(
            probability,
            self.prior.budget.path_time_error_seconds(),
            support,
            |seconds| self.predictive_cdf(seconds),
        )
    }

    pub(crate) fn sample_remaining_seconds(
        &self,
        elapsed_seconds: f64,
        random: &mut RandomStream,
    ) -> f64 {
        let total_survival = self
            .weights
            .iter()
            .zip(&self.prior.cells)
            .map(|(weight, cell)| weight * (1.0_f64 - log_normal_cdf(*cell, elapsed_seconds)))
            .sum::<f64>();
        let selector = random.open_unit_f64() * total_survival;
        let mut cumulative = 0.0_f64;
        let mut selected = self.prior.cells[0];
        for (weight, cell) in self.weights.iter().zip(&self.prior.cells) {
            cumulative += weight * (1.0_f64 - log_normal_cdf(*cell, elapsed_seconds));
            if cumulative >= selector {
                selected = *cell;
                break;
            }
        }
        sample_log_normal_after(
            selected,
            elapsed_seconds,
            self.prior.budget.path_time_error_seconds(),
            random,
        ) - elapsed_seconds
    }

    pub(crate) fn posterior_value_count(&self) -> u32 {
        u32::try_from(self.prior.cells.len()).map_or(u32::MAX, |count| count)
    }

    pub(crate) fn write_posterior(&self, values: &mut [f64], probabilities: &mut [f64]) -> bool {
        if values.len() != self.prior.cells.len() || probabilities.len() != values.len() {
            return false;
        }
        for (value, cell) in values.iter_mut().zip(&self.prior.cells) {
            *value = cell.median_seconds();
        }
        probabilities.copy_from_slice(&self.weights);
        true
    }
}

#[derive(Clone, Copy)]
struct HypothesisIndices {
    intercept: usize,
    slope: usize,
    fast: usize,
    slow: usize,
}

#[derive(Clone, Copy)]
struct MixtureComponents {
    slow_probability: f64,
    fast: DurationCell,
    slow: DurationCell,
}

#[derive(Debug)]
struct EvidenceToken;

impl Drop for EvidenceToken {
    fn drop(&mut self) {}
}

fn checked_product(lengths: &[usize]) -> Result<usize, LeadTimePriorError> {
    lengths.iter().try_fold(1_usize, |product, length| {
        product
            .checked_mul(*length)
            .ok_or(LeadTimePriorError::GridSize)
    })
}

fn validate_artifact(
    artifact: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: &[PriorCoverageRecord],
    hypothesis_count: usize,
    probabilities: &[f64],
) -> Result<(), LeadTimePriorError> {
    if probabilities.len() != hypothesis_count {
        return Err(LeadTimePriorError::ProbabilityCount);
    }
    let storage_bytes = hypothesis_count
        .checked_mul(size_of::<f64>() * 2)
        .ok_or(LeadTimePriorError::GridSize)?;
    let update_operation_count =
        u64::try_from(hypothesis_count).map_err(|_| LeadTimePriorError::GridSize)?;
    if !prior_artifact_contract_holds(
        artifact,
        budget,
        coverage,
        hypothesis_count,
        storage_bytes,
        update_operation_count,
    ) {
        return Err(LeadTimePriorError::GridBudget);
    }
    Ok(())
}

fn validate_duration_support(
    cells: &[DurationCell],
    coverage: PriorCoverageRecord,
    budget: PriorArtifactBudget,
) -> Result<(), LeadTimePriorError> {
    let boundary_mass = cells
        .iter()
        .map(|cell| duration_boundary_mass(*cell, coverage))
        .fold(0.0_f64, f64::max);
    if !boundary_mass.is_finite() || boundary_mass > budget.boundary_probability_max() {
        return Err(LeadTimePriorError::CoverageBudget);
    }
    Ok(())
}

fn duration_boundary_mass(cell: DurationCell, coverage: PriorCoverageRecord) -> f64 {
    log_normal_cdf(cell, coverage.lower_endpoint()) + 1.0_f64
        - log_normal_cdf(cell, coverage.upper_endpoint())
}

fn validate_boundary_mass(probability: f64, budget: f64) -> Result<(), PredictiveQuantileError> {
    if !probability.is_finite() {
        return Err(PredictiveQuantileError::NonFiniteCdf);
    }
    if probability > budget {
        return Err(PredictiveQuantileError::BoundaryMass {
            probability,
            budget,
        });
    }
    Ok(())
}

fn normalize(probabilities: &mut [f64]) -> Result<(), LeadTimePriorError> {
    if !probabilities
        .iter()
        .all(|probability| probability.is_finite() && *probability >= 0.0_f64)
    {
        return Err(LeadTimePriorError::InvalidProbability);
    }
    let total = probabilities.iter().sum::<f64>();
    if !total.is_finite() || total <= f64::EPSILON {
        return Err(LeadTimePriorError::EmptyMass);
    }
    for probability in probabilities {
        *probability /= total;
    }
    Ok(())
}

fn launch_observation_probability(
    components: MixtureComponents,
    observation: ReadinessObservation,
    requested_at: ModelTime,
) -> f64 {
    match observation {
        ReadinessObservation::Ready { .. } => {
            let fast = observation_probability(components.fast, observation, requested_at);
            let slow = observation_probability(components.slow, observation, requested_at);
            (1.0_f64 - components.slow_probability) * fast + components.slow_probability * slow
        }
        ReadinessObservation::Pending { after, through } => {
            let lower_seconds =
                Duration::from_micros(after.as_micros().saturating_sub(requested_at.as_micros()))
                    .as_secs_f64();
            let upper_seconds =
                Duration::from_micros(through.as_micros().saturating_sub(requested_at.as_micros()))
                    .as_secs_f64();
            let prior_survival = (1.0_f64 - components.slow_probability)
                * (1.0_f64 - log_normal_cdf(components.fast, lower_seconds))
                + components.slow_probability
                    * (1.0_f64 - log_normal_cdf(components.slow, lower_seconds));
            let current_survival = (1.0_f64 - components.slow_probability)
                * (1.0_f64 - log_normal_cdf(components.fast, upper_seconds))
                + components.slow_probability
                    * (1.0_f64 - log_normal_cdf(components.slow, upper_seconds));
            current_survival / prior_survival.max(f64::MIN_POSITIVE)
        }
    }
}

fn observation_probability(
    cell: DurationCell,
    observation: ReadinessObservation,
    origin: ModelTime,
) -> f64 {
    let (lower, upper) = observation.bounds();
    let lower_seconds =
        Duration::from_micros(lower.as_micros().saturating_sub(origin.as_micros())).as_secs_f64();
    let upper_seconds =
        Duration::from_micros(upper.as_micros().saturating_sub(origin.as_micros())).as_secs_f64();
    match observation {
        ReadinessObservation::Ready { .. } => {
            log_normal_cdf(cell, upper_seconds) - log_normal_cdf(cell, lower_seconds)
        }
        ReadinessObservation::Pending { .. } => {
            let prior_survival = 1.0_f64 - log_normal_cdf(cell, lower_seconds);
            let current_survival = 1.0_f64 - log_normal_cdf(cell, upper_seconds);
            current_survival / prior_survival.max(f64::MIN_POSITIVE)
        }
    }
}

fn logistic(value: f64) -> f64 {
    if value >= 0.0_f64 {
        1.0_f64 / (1.0_f64 + (-value).exp())
    } else {
        let exponential = value.exp();
        exponential / (1.0_f64 + exponential)
    }
}

fn log_normal_mean(cell: DurationCell) -> f64 {
    (cell.mu_log_seconds + 0.5_f64 * cell.sigma_log_seconds * cell.sigma_log_seconds).exp()
}

fn log_normal_cdf(cell: DurationCell, elapsed_seconds: f64) -> f64 {
    if elapsed_seconds <= 0.0_f64 {
        return 0.0_f64;
    }
    let standardized = (elapsed_seconds.ln() - cell.mu_log_seconds) / cell.sigma_log_seconds;
    1.0_f64 - normal_survival(standardized)
}

fn sample_log_normal(cell: DurationCell, random: &mut RandomStream) -> f64 {
    let radius = (-2.0_f64 * random.open_unit_f64().ln()).sqrt();
    let normal = radius * (TAU * random.open_unit_f64()).cos();
    (cell.mu_log_seconds + cell.sigma_log_seconds * normal).exp()
}

fn sample_log_normal_after(
    cell: DurationCell,
    elapsed_seconds: f64,
    time_error_seconds: f64,
    random: &mut RandomStream,
) -> f64 {
    let lower_cdf = log_normal_cdf(cell, elapsed_seconds);
    let probability = lower_cdf + random.open_unit_f64() * (1.0_f64 - lower_cdf);
    let mut low = elapsed_seconds.max(f64::MIN_POSITIVE);
    let mut high = cell.median_seconds().max(low * 2.0_f64);
    while log_normal_cdf(cell, high) < probability {
        high *= 2.0_f64;
    }
    while high - low > time_error_seconds {
        let middle = f64::midpoint(low, high);
        if middle.total_cmp(&low).is_eq() || middle.total_cmp(&high).is_eq() {
            return middle;
        }
        if log_normal_cdf(cell, middle) < probability {
            low = middle;
        } else {
            high = middle;
        }
    }
    f64::midpoint(low, high)
}

fn sample_weight_index(weights: &[f64], random: &mut RandomStream) -> usize {
    let selector = random.open_unit_f64();
    let mut cumulative = 0.0_f64;
    let mut selected = weights.len() - 1;
    for (index, weight) in weights.iter().copied().enumerate() {
        cumulative += weight;
        if cumulative >= selector {
            selected = index;
            break;
        }
    }
    selected
}

fn invert_predictive(
    probability: f64,
    time_error_seconds: f64,
    support: (f64, f64),
    cdf: impl Fn(f64) -> f64,
) -> Result<f64, PredictiveQuantileError> {
    if !probability.is_finite() || !(0.0_f64..1.0_f64).contains(&probability) {
        return Err(PredictiveQuantileError::InvalidProbability { probability });
    }
    let (support_low, support_high) = support;
    let mut low_log = support_low.ln();
    let mut high_log = support_high.ln();
    let mut width = high_log - low_log;
    loop {
        let low_cdf = cdf(low_log.exp());
        let high_cdf = cdf(high_log.exp());
        if !low_cdf.is_finite() || !high_cdf.is_finite() {
            return Err(PredictiveQuantileError::NonFiniteCdf);
        }
        if low_cdf <= probability && high_cdf >= probability {
            break;
        }
        if low_cdf > probability {
            high_log = low_log;
            low_log -= width;
        } else {
            low_log = high_log;
            high_log += width;
        }
        width *= 2.0_f64;
        if !low_log.is_finite() || !high_log.is_finite() || !width.is_finite() {
            return Err(PredictiveQuantileError::CannotEnclose { probability });
        }
    }
    loop {
        let low = low_log.exp();
        let high = high_log.exp();
        if high - low <= time_error_seconds {
            return Ok(f64::midpoint(low, high));
        }
        let middle_log = f64::midpoint(low_log, high_log);
        if middle_log.total_cmp(&low_log).is_eq() || middle_log.total_cmp(&high_log).is_eq() {
            return Err(PredictiveQuantileError::CannotRefine { probability });
        }
        let middle_cdf = cdf(middle_log.exp());
        if !middle_cdf.is_finite() {
            return Err(PredictiveQuantileError::NonFiniteCdf);
        }
        if middle_cdf < probability {
            low_log = middle_log;
        } else {
            high_log = middle_log;
        }
    }
}

fn coverage_support(coverage: &[PriorCoverageRecord]) -> (f64, f64) {
    coverage.iter().fold(
        (f64::INFINITY, f64::NEG_INFINITY),
        |(lower, upper), record| {
            (
                lower.min(record.lower_endpoint()),
                upper.max(record.upper_endpoint()),
            )
        },
    )
}

fn apply_log_likelihood(level: Level, weights: &mut [f64], likelihoods: &mut [f64]) {
    let maximum = likelihoods
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    for likelihood in likelihoods.iter_mut() {
        *likelihood = (*likelihood - maximum).exp();
    }
    dispatch!(level, simd => multiply(simd, weights, likelihoods));
    let total = weights.iter().sum::<f64>();
    if total > 0.0_f64 {
        for weight in weights {
            *weight /= total;
        }
    }
}

fn multiply<S: Simd>(simd: S, weights: &mut [f64], likelihoods: &[f64]) {
    let lane_count = S::f64s::N;
    let vector_count = weights.len() / lane_count;
    for vector in 0..vector_count {
        let start = vector * lane_count;
        let end = start + lane_count;
        let weight = S::f64s::from_slice(simd, &weights[start..end]);
        let likelihood = S::f64s::from_slice(simd, &likelihoods[start..end]);
        (weight * likelihood).store_slice(&mut weights[start..end]);
    }
    for cell in vector_count * lane_count..weights.len() {
        weights[cell] *= likelihoods[cell];
    }
}

fn normal_survival(standardized: f64) -> f64 {
    0.5_f64 * complementary_error_function(standardized / SQRT_2)
}

fn complementary_error_function(value: f64) -> f64 {
    let absolute = value.abs();
    let t = 1.0_f64 / (1.0_f64 + 0.5_f64 * absolute);
    let polynomial = t
        * (-absolute * absolute - 1.265_512_23_f64
            + t * (1.000_023_68_f64
                + t * (0.374_091_96_f64
                    + t * (0.096_784_18_f64
                        + t * (-0.186_288_06_f64
                            + t * (0.278_868_07_f64
                                + t * (-1.135_203_98_f64
                                    + t * (1.488_515_87_f64
                                        + t * (-0.822_152_23_f64 + t * 0.170_872_77_f64)))))))))
            .exp();
    if value >= 0.0_f64 {
        polynomial
    } else {
        2.0_f64 - polynomial
    }
}

/// Invalid launch evidence.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LaunchEvidenceError {
    /// A readiness group has no pod.
    #[error("a readiness group must contain at least one pod")]
    ZeroPodCount,
    /// A readiness interval is empty or reversed.
    #[error("a readiness interval must advance its prior bound")]
    InvalidInterval,
    /// A launch request has no replica.
    #[error("a launch replica delta must be positive")]
    ZeroReplicaDelta,
    /// The observation precedes the launch request.
    #[error("a launch observation must not precede its request")]
    ObservationBeforeRequest,
    /// A readiness interval falls outside the launch observation.
    #[error("a readiness interval must stay inside the launch observation")]
    IntervalOutsideObservation,
    /// Readiness groups exceed the requested replica delta.
    #[error("readiness group pod counts must not exceed the requested replica delta")]
    PodCountExceedsDelta,
    /// A launch update repeats a stable group identity.
    #[error("readiness group identities must be unique in one launch update")]
    DuplicateGroup,
}

/// Invalid launch or rebalance prior artifact.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LeadTimePriorError {
    /// The artifact budget is empty or non-finite.
    #[error("a lead-time artifact budget is invalid")]
    InvalidArtifactBudget,
    /// A duration cell is not positive and finite.
    #[error("a duration cell must have a positive finite median and deviation")]
    InvalidDurationCell,
    /// A prior axis is empty or contains an invalid value.
    #[error("a lead-time prior axis is invalid")]
    InvalidAxis,
    /// The probability count does not match the product grid.
    #[error("lead-time probabilities must match the product grid")]
    ProbabilityCount,
    /// A prior probability is negative or non-finite.
    #[error("a lead-time prior probability is invalid")]
    InvalidProbability,
    /// The prior has no positive probability mass.
    #[error("lead-time prior probability mass must be positive")]
    EmptyMass,
    /// The product grid size overflowed the platform representation.
    #[error("the lead-time product grid is too large")]
    GridSize,
    /// The product grid exceeds its declared storage budget.
    #[error("the lead-time product grid exceeds its artifact budget")]
    GridBudget,
    /// Predictive coverage exceeds its boundary or decision-cost budget.
    #[error("lead-time predictive coverage exceeds its artifact budget")]
    CoverageBudget,
    /// The launch artifact does not record both readiness modes.
    #[error("a lead-time artifact has too few coverage records")]
    CoverageCount,
    /// One bounded update exceeds the artifact operation budget.
    #[error("a lead-time update exceeds its artifact operation budget")]
    UpdateBudget,
}

/// Predictive quantile inversion failure.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum PredictiveQuantileError {
    /// A validated launch prior lost one readiness coverage record.
    #[error("the launch prior coverage invariant is invalid")]
    CoverageInvariant,
    /// Current predictive mass exceeds the artifact support budget.
    #[error("predictive boundary mass {probability} exceeds budget {budget}")]
    BoundaryMass {
        /// Current predictive mass outside the recorded support.
        probability: f64,
        /// Maximum permitted predictive boundary mass.
        budget: f64,
    },
    /// The requested probability is outside the open unit interval.
    #[error("predictive quantile probability {probability} must be between zero and one")]
    InvalidProbability {
        /// Invalid requested probability.
        probability: f64,
    },
    /// A predictive CDF result was not finite.
    #[error("the predictive CDF returned a non-finite value")]
    NonFiniteCdf,
    /// Finite arithmetic could not enclose the probability.
    #[error("finite arithmetic cannot enclose predictive probability {probability}")]
    CannotEnclose {
        /// Probability that could not be enclosed.
        probability: f64,
    },
    /// Finite arithmetic could not reduce the final bracket.
    #[error("finite arithmetic cannot refine predictive probability {probability}")]
    CannotRefine {
        /// Probability that could not be refined.
        probability: f64,
    },
}
