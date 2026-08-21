use std::mem::size_of;

use crate::AttemptOutcomeEvidence;
use crate::ConfigurationError;
use crate::PosteriorError;
use crate::PriorArtifactBudget;
use crate::PriorArtifactIdentity;
use crate::PriorCoverageRecord;
use crate::RandomStream;
use crate::random::sample_gamma;
use crate::types::prior_artifact_contract_holds;
use statrs::distribution::{Beta, ContinuousCDF};

/// Each retry posterior uses 64 equal report bins.
///
/// A bin midpoint then differs from its region by at most 1/128.
pub(crate) const RELIABILITY_BIN_COUNT: u32 = 64;
/// The source identity spells `RELIABLE` in ASCII.
const RELIABILITY_ARTIFACT_SOURCE: u64 = 0x5245_4c49_4142_4c45;
/// Version one defines two 64-bin Beta reports.
const RELIABILITY_ARTIFACT_VERSION: u32 = 1;
/// A 64-bin report has a maximum midpoint error of 1/128.
const RELIABILITY_MIDPOINT_ERROR: f64 = 0.5_f64 / RELIABILITY_BIN_COUNT as f64;
/// The artifact reports 128 values in two value and probability buffer pairs.
const RELIABILITY_ARTIFACT_BUDGET: PriorArtifactBudget =
    PriorArtifactBudget::new(128, 2_048, 2, 0.0_f64, 0.0_f64, RELIABILITY_MIDPOINT_ERROR);
const RELIABILITY_COVERAGE: [PriorCoverageRecord; 2] = [
    PriorCoverageRecord::new(
        0.0_f64,
        1.0_f64,
        0.0_f64,
        0.0_f64,
        RELIABILITY_MIDPOINT_ERROR,
    ),
    PriorCoverageRecord::new(
        0.0_f64,
        1.0_f64,
        0.0_f64,
        0.0_f64,
        RELIABILITY_MIDPOINT_ERROR,
    ),
];

/// Proper population prior for Normal and Failure retry probabilities.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReliabilityPrior {
    artifact: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: [PriorCoverageRecord; 2],
    normal_retrying: f64,
    normal_final: f64,
    failure_retrying: f64,
    failure_final: f64,
}

impl ReliabilityPrior {
    /// Constructs four positive Beta shape parameters.
    ///
    /// # Errors
    ///
    /// Returns an error when a shape is not positive and finite.
    pub fn new(
        normal_retrying: f64,
        normal_final: f64,
        failure_retrying: f64,
        failure_final: f64,
    ) -> Result<Self, ConfigurationError> {
        let shapes = [
            normal_retrying,
            normal_final,
            failure_retrying,
            failure_final,
        ];
        if !shapes
            .iter()
            .all(|shape| shape.is_finite() && *shape > 0.0_f64)
        {
            return Err(ConfigurationError::InvalidReliabilityPrior);
        }
        let random_stream = normal_retrying.to_bits()
            ^ normal_final.to_bits().rotate_left(16)
            ^ failure_retrying.to_bits().rotate_left(32)
            ^ failure_final.to_bits().rotate_left(48)
            | 1;
        let artifact = PriorArtifactIdentity::new(
            RELIABILITY_ARTIFACT_SOURCE,
            RELIABILITY_ARTIFACT_VERSION,
            random_stream,
        );
        if !prior_artifact_contract_holds(
            artifact,
            RELIABILITY_ARTIFACT_BUDGET,
            &RELIABILITY_COVERAGE,
            reliability_report_value_count(),
            reliability_report_storage_bytes(),
            2,
        ) {
            return Err(ConfigurationError::InvalidReliabilityPrior);
        }
        Ok(Self {
            artifact,
            budget: RELIABILITY_ARTIFACT_BUDGET,
            coverage: RELIABILITY_COVERAGE,
            normal_retrying,
            normal_final,
            failure_retrying,
            failure_final,
        })
    }

    /// Returns the authored weak-information population prior.
    ///
    /// No representative retry corpus exists. This choice assigns a 10%
    /// retry mean and ten observations of strength to each class. Retries are
    /// expected to be uncommon. The weak strength lets early evidence replace
    /// that judgment. Retry behavior is a property of the deployed binary. A
    /// deploy replaces the model instance. The posterior therefore stays
    /// stationary for the life of the binary and needs no change kernel.
    ///
    /// # Errors
    ///
    /// Returns an error if the embedded artifact fails its contract.
    pub fn authored() -> Result<Self, ConfigurationError> {
        Self::new(1.0_f64, 9.0_f64, 1.0_f64, 9.0_f64)
    }

    /// Returns this prior's artifact identity.
    #[must_use]
    pub const fn artifact(self) -> PriorArtifactIdentity {
        self.artifact
    }

    /// Returns this prior's approximation budget.
    #[must_use]
    pub const fn budget(self) -> PriorArtifactBudget {
        self.budget
    }

    /// Returns the two retry-probability coverage records.
    #[must_use]
    pub const fn coverage(&self) -> &[PriorCoverageRecord] {
        &self.coverage
    }
}

fn reliability_report_value_count() -> usize {
    2 * RELIABILITY_BIN_COUNT as usize
}

fn reliability_report_storage_bytes() -> usize {
    2 * reliability_report_value_count() * size_of::<f64>()
}

pub(crate) struct ReliabilityFactor {
    normal: BetaFactor,
    failure: BetaFactor,
}

impl ReliabilityFactor {
    pub(crate) const fn new(prior: ReliabilityPrior) -> Self {
        Self {
            normal: BetaFactor::new(prior.normal_retrying, prior.normal_final),
            failure: BetaFactor::new(prior.failure_retrying, prior.failure_final),
        }
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "reliability_update"))]
    pub(crate) fn update(&mut self, evidence: AttemptOutcomeEvidence) {
        self.normal.update(evidence.normal);
        self.failure.update(evidence.failure);
    }

    pub(crate) fn sample_retry_probabilities(&self, random: &mut RandomStream) -> (f64, f64) {
        (self.normal.sample(random), self.failure.sample(random))
    }

    pub(crate) fn write_normal_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        self.normal.write_posterior(values, probabilities)
    }

    pub(crate) fn write_failure_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        self.failure.write_posterior(values, probabilities)
    }
}

struct BetaFactor {
    retrying: f64,
    final_outcome: f64,
}

impl BetaFactor {
    const fn new(retrying: f64, final_outcome: f64) -> Self {
        Self {
            retrying,
            final_outcome,
        }
    }

    fn update(&mut self, outcomes: crate::AttemptOutcomeCounts) {
        self.retrying += f64::from(outcomes.transient) + f64::from(outcomes.terminal);
        self.final_outcome += f64::from(outcomes.success) + f64::from(outcomes.permanent);
    }

    fn sample(&self, random: &mut RandomStream) -> f64 {
        let retrying = sample_gamma(self.retrying, random);
        retrying / (retrying + sample_gamma(self.final_outcome, random))
    }

    fn write_posterior(
        &self,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> Result<(), PosteriorError> {
        let expected = RELIABILITY_BIN_COUNT as usize;
        if values.len() != expected || probabilities.len() != expected {
            return Err(PosteriorError::BufferLength {
                expected: RELIABILITY_BIN_COUNT,
            });
        }
        let distribution = Beta::new(self.retrying, self.final_outcome)
            .map_err(|_| PosteriorError::ReliabilityDistribution)?;
        let width = f64::from(RELIABILITY_BIN_COUNT).recip();
        for (index, (value, probability)) in
            (0_u32..RELIABILITY_BIN_COUNT).zip(values.iter_mut().zip(probabilities))
        {
            let lower = f64::from(index) * width;
            let upper = f64::from(index + 1) * width;
            *value = f64::midpoint(lower, upper);
            *probability = distribution.cdf(upper) - distribution.cdf(lower);
        }
        Ok(())
    }
}
