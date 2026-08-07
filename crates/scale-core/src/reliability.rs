use crate::AttemptOutcomeEvidence;
use crate::ConfigurationError;
use crate::PosteriorError;
use crate::RandomStream;
use crate::random::sample_gamma;
use statrs::distribution::{Beta, ContinuousCDF};

pub(crate) const RELIABILITY_BIN_COUNT: u32 = 64;

/// Proper population prior for Normal and Failure retry probabilities.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReliabilityPrior {
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
        Ok(Self {
            normal_retrying,
            normal_final,
            failure_retrying,
            failure_final,
        })
    }

    /// Returns the declared fallback when no trained artifact exists.
    ///
    /// Each class starts with one retry and nine final pseudo-observations.
    #[must_use]
    pub const fn population_fallback() -> Self {
        Self {
            normal_retrying: 1.0_f64,
            normal_final: 9.0_f64,
            failure_retrying: 1.0_f64,
            failure_final: 9.0_f64,
        }
    }
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
        for (index, (value, probability)) in values.iter_mut().zip(probabilities).enumerate() {
            let lower = index as f64 * width;
            let upper = (index + 1) as f64 * width;
            *value = (lower + upper) * 0.5_f64;
            *probability = distribution.cdf(upper) - distribution.cdf(lower);
        }
        Ok(())
    }
}
