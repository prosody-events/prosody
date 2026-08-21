use std::f64::consts::E;
use std::time::Duration;

use rand::RngExt;
use statrs::distribution::{ContinuousCDF, DiscreteCDF, Gamma, NegativeBinomial, Poisson};
use statrs::function::gamma::{gamma_lr, gamma_ur, ln_gamma};
use thiserror::Error;

use crate::random::{PoissonMean, count_as_f64, sample_gamma, sample_poisson};
use crate::types::{CalendarColumns, CalendarForecast, prior_artifact_contract_holds};
use crate::{
    CalendarArtifactId, PriorArtifactBudget, PriorArtifactIdentity, PriorCoverageRecord,
    RandomStream,
};

const RESET_COUNT: usize = 3;
const MODEL_VERSION: u32 = 1;
const ARRIVAL_ARTIFACT_SOURCE: u64 = 0x0041_5252_4956_414c;
const T_MAX_SECONDS_U32: u32 = 7 * 24 * 60 * 60;
const T_MAX_SECONDS: f64 = T_MAX_SECONDS_U32 as f64;
// A rate cell can change its Poisson mean by at most two percent. The
// achieved half-cell relative error enters the decision-cost coverage record.
const EPSILON_GRID: f64 = 0.02_f64;
// Omitted prior tails have at most one part per million of total mass. This
// bounds total variation and expected bounded-loss error by the same fraction.
const EPSILON_BOUNDARY: f64 = 1.0e-6_f64;
// The exponential is the maximum-entropy hazard prior for the authored mean.
// It is also the center of the reset-shape family.
const HAZARD_SHAPE: f64 = 1.0_f64;
const HAZARD_TRANSITION_PROBABILITY_ERROR_MAX: f64 = 1.0_f64 / 8.0_f64;
// Path rejection omits at most one part per billion of predictive mass. This
// is the total-variation loss between full and buffer-conditioned path laws.
const EPSILON_PATH: f64 = 1.0e-9_f64;
// One scaling target can use at most 16 MiB for its arrival filter.
const STORAGE_BUDGET_BYTES: usize = 16 * 1_024 * 1_024;
// The path buffer holds one calendar segment per ten minutes for seven days.
// Finer forecasts remain valid when their total count fits this capacity.
const CALENDAR_SEGMENT_SECONDS_MIN: u64 = 600;
const CALENDAR_SEGMENT_LIMIT: usize =
    (T_MAX_SECONDS_U32 as u64 / CALENDAR_SEGMENT_SECONDS_MIN) as usize;
// One sampled path can contain at most 262,144 change segments. This authored
// limit bounds path construction work when an extreme hazard makes the
// Poisson tail contract larger than useful controller scratch.
const PATH_SEGMENT_LIMIT: usize = 262_144;
const ARRIVAL_COUNT_DOMAIN: u64 = 0x6172_7269_7661_6c73;

/// Exact count prediction from the finite arrival model.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArrivalCountPredictive {
    /// Probability of a count smaller than the observed count.
    pub lower_cdf: f64,
    /// Probability of a count no larger than the observed count.
    pub upper_cdf: f64,
    /// Predictive counts at probabilities 0.1, 0.5, and 0.9.
    pub quantiles: [u64; 3],
}

/// Posterior mass on the finite rate grid endpoints.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArrivalBoundaryDiagnostic {
    /// Posterior probability on the lower endpoint cell.
    pub lower_endpoint_probability: f64,
    /// Posterior probability on the upper endpoint cell.
    pub upper_endpoint_probability: f64,
    /// Maximum permitted probability on both endpoint cells.
    pub probability_budget: f64,
}

impl ArrivalBoundaryDiagnostic {
    /// Returns true when either endpoint probability exceeds the budget.
    #[must_use]
    pub fn exceeds_budget(self) -> bool {
        self.lower_endpoint_probability > self.probability_budget
            || self.upper_endpoint_probability > self.probability_budget
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct GridSpec {
    low: f64,
    log_step: f64,
    count: usize,
}

struct ArrivalGrids {
    hazards: Box<[f64]>,
    rates: Box<[f64]>,
}

/// A validated finite-state arrival model.
///
/// The declared model is discrete at evidence boundaries. A rate stays fixed
/// during an interval. It changes at the next boundary with probability
/// `1 - exp(-hazard * duration)`. The filter is exact for this finite model.
/// The caller authors the reset-shape center. The prior gives equal mass to
/// that shape and shapes one octave below and above it for scale robustness.
#[derive(Clone, Debug, PartialEq)]
pub struct ArrivalPrior {
    artifact: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: Box<[PriorCoverageRecord]>,
    authored_shape: f64,
    rate_seconds: f64,
    hazard_center: f64,
    hazard_low: f64,
    hazard_log_step: f64,
    hazard_count: usize,
    rate_low: f64,
    rate_log_step: f64,
    rate_count: usize,
    path_change_bound: usize,
}

impl ArrivalPrior {
    /// The certified path domain in microseconds.
    pub(crate) const MAXIMUM_PATH_MICROS: u64 = (T_MAX_SECONDS * 1_000_000.0_f64) as u64;
    /// Longest path duration inside the certified sampler domain.
    pub(crate) const MAXIMUM_PATH_SECONDS: f64 = T_MAX_SECONDS;

    /// Constructs a validated finite arrival model.
    ///
    /// `shape` and `rate_seconds` define the reset-rate prior. The change rate
    /// locates the hazard prior. Exact parameter bits identify this authored
    /// artifact. Its coverage records certify every reset-rate tail and the
    /// rejected change-path tail.
    ///
    /// # Errors
    ///
    /// Returns an error when the prior or its resource contract is invalid.
    pub fn new(
        shape: f64,
        rate_seconds: f64,
        change_rate_per_second: f64,
    ) -> Result<Self, ArrivalPriorError> {
        if !shape.is_finite() || shape <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidShape);
        }
        if !rate_seconds.is_finite() || rate_seconds <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidRate);
        }
        if !change_rate_per_second.is_finite() || change_rate_per_second <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidChangeRate);
        }
        let tail_limit = EPSILON_BOUNDARY * 0.25_f64;
        let mean = shape / rate_seconds;
        if !mean.is_finite() || mean <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidRate);
        }
        let hazard = derive_hazard_grid(change_rate_per_second, tail_limit)?;
        let rate = derive_rate_grid(shape, mean, tail_limit)?;
        let cell_count = cell_count(hazard.count, rate.count)?;
        let storage_bytes = Self::storage_bytes(hazard.count, rate.count)?;
        if storage_bytes > STORAGE_BUDGET_BYTES {
            return Err(ArrivalPriorError::StorageBudget {
                required: storage_bytes,
                budget: STORAGE_BUDGET_BYTES,
            });
        }

        let hazard_center = change_rate_per_second;
        let grids = arrival_grids(hazard, rate);
        let hazards = &grids.hazards;
        if hazards.iter().any(|value| !value.is_finite()) {
            return Err(ArrivalPriorError::InvalidChangeRate);
        }
        let rates = &grids.rates;
        if rates
            .iter()
            .any(|value| !value.is_finite() || *value <= 0.0_f64)
        {
            return Err(ArrivalPriorError::InvalidRate);
        }
        let achieved_rate_error = rate.log_step.mul_add(0.5_f64, 0.0_f64).exp() - 1.0_f64;
        let (coverage, path_change_bound) = arrival_coverage(
            shape,
            mean,
            rates,
            hazards,
            change_rate_per_second,
            achieved_rate_error,
        )?;
        let maximum_poisson_mean = rates[rate.count - 1] * T_MAX_SECONDS;
        if PoissonMean::new(maximum_poisson_mean).is_none() {
            return Err(ArrivalPriorError::InvalidPoissonMean);
        }
        let random_stream = shape.to_bits()
            ^ rate_seconds.to_bits().rotate_left(21)
            ^ change_rate_per_second.to_bits().rotate_left(42)
            | 1;
        let artifact =
            PriorArtifactIdentity::new(ARRIVAL_ARTIFACT_SOURCE, MODEL_VERSION, random_stream);
        let budget = arrival_artifact_budget(cell_count)?;
        if !prior_artifact_contract_holds(
            artifact,
            budget,
            &coverage,
            cell_count,
            storage_bytes,
            (7 * cell_count) as u64,
        ) {
            return Err(ArrivalPriorError::InvalidAccuracyBudget);
        }
        Ok(Self {
            artifact,
            budget,
            coverage: coverage.into(),
            authored_shape: shape,
            rate_seconds,
            hazard_center,
            hazard_low: hazard.low,
            hazard_log_step: hazard.log_step,
            hazard_count: hazard.count,
            rate_low: rate.low,
            rate_log_step: rate.log_step,
            rate_count: rate.count,
            path_change_bound,
        })
    }

    /// Returns the authored test prior: one arrival per second and one
    /// expected change per day. Use it only where the arrival prior is not
    /// the subject. A test pins it equal to validated construction.
    #[cfg(test)]
    pub(crate) fn test_artifact() -> Result<Self, ArrivalPriorError> {
        Self::new(1.0_f64, 1.0_f64, 1.0_f64 / 86_400.0_f64)
    }

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

    /// Returns the reset-rate and path-tail coverage records.
    #[must_use]
    pub const fn coverage(&self) -> &[PriorCoverageRecord] {
        &self.coverage
    }

    pub(crate) const fn path_segment_count_max(&self) -> usize {
        // The shared path buffer fits the larger stochastic or calendar bound.
        let stochastic = self.path_change_bound + 1;
        if stochastic > CALENDAR_SEGMENT_LIMIT {
            stochastic
        } else {
            CALENDAR_SEGMENT_LIMIT
        }
    }

    pub(crate) const fn shape(&self) -> f64 {
        self.authored_shape
    }

    #[cfg(test)]
    pub(crate) const fn rate_seconds(&self) -> f64 {
        self.rate_seconds
    }

    pub(crate) fn posterior_value_count(&self) -> u32 {
        u32::try_from(self.rate_count).unwrap_or(u32::MAX)
    }

    fn storage_bytes(hazard_count: usize, rate_count: usize) -> Result<usize, ArrivalPriorError> {
        let cell_count = cell_count(hazard_count, rate_count)?;
        cell_count
            .checked_mul(2)
            .and_then(|value| {
                value.checked_add(hazard_count + RESET_COUNT * rate_count + rate_count)
            })
            .and_then(|value| value.checked_mul(size_of::<f64>()))
            .ok_or(ArrivalPriorError::ArithmeticOverflow)
    }
}

/// One consumable count and exposure update.
#[must_use = "pass arrival evidence to the controller"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArrivalEvidence {
    count: u32,
    exposure_micros: u64,
}

impl ArrivalEvidence {
    pub(crate) const fn new(count: u32, exposure_micros: u64) -> Self {
        Self {
            count,
            exposure_micros,
        }
    }
}

pub(crate) struct ArrivalFactor {
    model: ArrivalPrior,
    hazards: Box<[f64]>,
    rates: Box<[f64]>,
    reset_probability: [Box<[f64]>; RESET_COUNT],
    reset_means: [f64; RESET_COUNT],
    probability: Box<[f64]>,
    scratch: Box<[f64]>,
    rate_scratch: Box<[f64]>,
    // Calendar segment changes take effect at the next evidence boundary.
    // Each interval belongs to the segment active at its start.
    // This assignment is exact for the declared discrete model.
    calendar_artifact: Option<CalendarArtifactId>,
    calendar_position: u32,
    calendar_shape: f64,
    calendar_rate: f64,
    calendar_log_odds: f64,
    calendar_active: bool,
    last_evidence_micros: u64,
}

/// Predictive mean rates at consecutive report boundaries.
///
/// This view contains only tick-time information. It cannot expose a sampled
/// latent arrival path.
#[derive(Clone, Copy)]
pub(crate) struct MeanRateTrajectory<'a> {
    rates: &'a [f64],
}

impl<'a> MeanRateTrajectory<'a> {
    pub(crate) const fn new(rates: &'a [f64]) -> Self {
        Self { rates }
    }

    pub(crate) fn rates(self) -> impl Iterator<Item = f64> + 'a {
        self.rates.iter().copied()
    }
}

impl ArrivalFactor {
    pub(crate) fn new(model: &ArrivalPrior) -> Self {
        let grids = arrival_grids(
            GridSpec {
                low: model.hazard_low,
                log_step: model.hazard_log_step,
                count: model.hazard_count,
            },
            GridSpec {
                low: model.rate_low,
                log_step: model.rate_log_step,
                count: model.rate_count,
            },
        );
        let hazards = grids.hazards;
        let hazard_prior = exact_gamma_masses(&hazards, HAZARD_SHAPE, model.hazard_center);
        let mean = model.authored_shape / model.rate_seconds;
        let rates = grids.rates;
        // The declared reset-shape prior is uniform on one octave below the
        // authored center, the center, and one octave above the center. Each
        // shape keeps the authored mean and changes only concentration.
        let reset_shapes = [
            model.authored_shape * 0.5_f64,
            model.authored_shape,
            model.authored_shape * 2.0_f64,
        ];
        let reset_probability = reset_shapes.map(|shape| {
            (0..model.rate_count)
                .map(|index| {
                    if index == 0 {
                        gamma_lr(shape, shape * geometric_upper(&rates, index) / mean)
                    } else if index == model.rate_count - 1 {
                        gamma_ur(shape, shape * geometric_lower(&rates, index) / mean)
                    } else {
                        let lower = geometric_lower(&rates, index) / mean;
                        let upper = geometric_upper(&rates, index) / mean;
                        // Select the dominant tail to prevent CDF subtraction cancellation.
                        if rates[index] <= mean {
                            gamma_lr(shape, shape * upper) - gamma_lr(shape, shape * lower)
                        } else {
                            gamma_ur(shape, shape * lower) - gamma_ur(shape, shape * upper)
                        }
                    }
                })
                .collect::<Box<[_]>>()
        });
        let reset_means = reset_probability.each_ref().map(|probabilities| {
            probabilities
                .iter()
                .zip(&rates)
                .map(|(probability, rate)| probability * rate)
                .sum::<f64>()
        });
        let cell_count = model.hazard_count * RESET_COUNT * model.rate_count;
        let mut probability = vec![0.0_f64; cell_count].into_boxed_slice();
        for hazard in 0..model.hazard_count {
            for reset in 0..RESET_COUNT {
                for rate in 0..model.rate_count {
                    probability[cell(hazard, reset, rate, model.rate_count)] =
                        hazard_prior[hazard] / 3.0_f64 * reset_probability[reset][rate];
                }
            }
        }
        Self {
            model: model.clone(),
            hazards,
            rates,
            reset_probability,
            reset_means,
            probability,
            scratch: vec![0.0_f64; cell_count].into_boxed_slice(),
            rate_scratch: vec![0.0_f64; model.rate_count].into_boxed_slice(),
            calendar_artifact: None,
            calendar_position: 0,
            calendar_shape: 0.0_f64,
            calendar_rate: 0.0_f64,
            calendar_log_odds: f64::NEG_INFINITY,
            calendar_active: false,
            last_evidence_micros: 0,
        }
    }

    pub(crate) fn posterior_value_count(&self) -> u32 {
        self.model.posterior_value_count()
    }

    /// Updates the model with one certified evidence interval.
    ///
    /// `now_micros` is the evidence interval end.
    /// The producer certifies that the count covers exactly `[now - exposure,
    /// now]`. The producer controls delivery delay.
    /// After the evidence applies, the update advances the calendar boundary to
    /// `now_micros`.
    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "arrival_update"))]
    pub(crate) fn update(
        &mut self,
        evidence: ArrivalEvidence,
        calendar: Option<CalendarForecast<'_>>,
        now_micros: u64,
    ) {
        let ArrivalEvidence {
            count,
            exposure_micros,
        } = evidence;
        let exposure = Duration::from_micros(exposure_micros).as_secs_f64();
        if exposure == 0.0_f64 {
            return;
        }
        let evidence_start = now_micros.saturating_sub(exposure_micros);
        self.transition(
            Duration::from_micros(evidence_start.saturating_sub(self.last_evidence_micros))
                .as_secs_f64(),
            None,
        );
        self.prepare_calendar(calendar, evidence_start);
        if let Some(forecast) = calendar
            && calendar_segment_at(forecast.segments, evidence_start).is_some()
        {
            self.calendar_log_odds +=
                log_predictive_mass(self.calendar_shape, self.calendar_rate, count, exposure)
                    - self.log_predictive_mass(count, exposure);
            self.calendar_shape += f64::from(count);
            self.calendar_rate += exposure;
        }
        self.transition(exposure, Some(count));
        self.prepare_calendar(calendar, now_micros);
        self.last_evidence_micros = now_micros;
    }

    fn transition(&mut self, duration: f64, count: Option<u32>) {
        if duration == 0.0_f64 {
            return;
        }
        self.scratch.fill(0.0_f64);
        if let Some(value) = count {
            for (likelihood, rate) in self.rate_scratch.iter_mut().zip(&self.rates) {
                *likelihood = log_poisson_mass(value, rate * duration);
            }
        }
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * duration).exp();
            for reset in 0..RESET_COUNT {
                let mut reset_mass = 0.0_f64;
                for source in 0..self.rates.len() {
                    reset_mass += self.probability[cell(hazard, reset, source, self.rates.len())]
                        * (1.0_f64 - retained);
                }
                for destination in 0..self.rates.len() {
                    let index = cell(hazard, reset, destination, self.rates.len());
                    let prior = self.probability[index] * retained
                        + reset_mass * self.reset_probability[reset][destination];
                    self.scratch[index] =
                        count.map_or(prior, |_| prior.ln() + self.rate_scratch[destination]);
                }
            }
        }
        if count.is_some() {
            let maximum = self
                .scratch
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            for value in &mut self.scratch {
                *value = (*value - maximum).exp();
            }
        }
        let normalizer = self.scratch.iter().sum::<f64>();
        assert!(
            normalizer.is_finite() && normalizer > 0.0_f64,
            "validated arrival evidence must have positive finite mass"
        );
        for (probability, next) in self
            .probability
            .iter_mut()
            .zip(self.scratch.iter().copied())
        {
            *probability = next / normalizer;
        }
    }

    pub(crate) fn prepare_calendar(
        &mut self,
        calendar: Option<CalendarForecast<'_>>,
        evidence_start_micros: u64,
    ) {
        let Some(forecast) = calendar else {
            self.calendar_active = false;
            return;
        };
        let Some(segment) = calendar_segment_at(forecast.segments, evidence_start_micros) else {
            self.calendar_active = false;
            return;
        };
        self.calendar_active = true;
        let artifact_changed = self.calendar_artifact != Some(forecast.artifact);
        if artifact_changed || self.calendar_position != forecast.segments.position(segment) {
            self.calendar_artifact = Some(forecast.artifact);
            self.calendar_position = forecast.segments.position(segment);
            self.calendar_shape = forecast.segments.shape(segment);
            self.calendar_rate = forecast.segments.rate_seconds(segment);
        }
        if artifact_changed {
            self.calendar_log_odds = logit(forecast.prior_probability);
        }
    }

    pub(crate) fn expected_rate(&self, now_micros: u64) -> f64 {
        let local = self.marginal_mean(now_micros);
        if !self.calendar_active {
            return local;
        }
        let calendar = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate
        } else {
            local
        };
        let probability = self.calendar_probability();
        (1.0_f64 - probability) * local + probability * calendar
    }

    /// Writes the expected sampled-path rate at each report boundary.
    ///
    /// The local component decays toward its reset mean at its hazard. Each
    /// calendar component uses its Gamma mean. The sampler excludes scheduled
    /// releases, so this trajectory excludes them too.
    pub(crate) fn write_mean_rate_trajectory<'a>(
        &self,
        duration_micros: u64,
        report_interval_micros: u64,
        calendar: Option<CalendarForecast<'_>>,
        now_micros: u64,
        rates: &'a mut [f64],
    ) -> MeanRateTrajectory<'a> {
        let count = (duration_micros / report_interval_micros) as usize;
        assert!(count <= rates.len(), "mean trajectory storage is too small");
        let duration_seconds = Duration::from_micros(duration_micros).as_secs_f64();
        let calendar = calendar
            .filter(|forecast| calendar_covers(forecast.segments, now_micros, duration_seconds));
        let calendar_probability = calendar.map_or(0.0_f64, |_| self.calendar_probability());
        for (index, output) in rates[..count].iter_mut().enumerate() {
            let offset = report_interval_micros.saturating_mul(index as u64 + 1);
            let at_micros = now_micros.saturating_add(offset);
            let local = self.marginal_mean(at_micros);
            let calendar_mean = calendar.map_or(local, |forecast| {
                calendar_segment_at(forecast.segments, at_micros).map_or(local, |segment| {
                    self.calendar_segment_mean(forecast, segment)
                })
            });
            *output =
                (1.0_f64 - calendar_probability) * local + calendar_probability * calendar_mean;
        }
        MeanRateTrajectory::new(&rates[..count])
    }

    fn calendar_segment_mean(&self, forecast: CalendarForecast<'_>, segment: usize) -> f64 {
        if self.calendar_artifact == Some(forecast.artifact)
            && self.calendar_position == forecast.segments.position(segment)
        {
            self.calendar_shape / self.calendar_rate
        } else {
            forecast.segments.shape(segment) / forecast.segments.rate_seconds(segment)
        }
    }

    fn marginal_mean(&self, now_micros: u64) -> f64 {
        let elapsed = Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
            .as_secs_f64();
        let mut mean = 0.0_f64;
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * elapsed).exp();
            for reset in 0..RESET_COUNT {
                let mut group = 0.0_f64;
                let mut retained_mean = 0.0_f64;
                for rate in 0..self.rates.len() {
                    let probability = self.probability[cell(hazard, reset, rate, self.rates.len())];
                    group += probability;
                    retained_mean += probability * self.rates[rate];
                }
                let reset_mean = self.reset_means[reset];
                mean += retained * retained_mean + (1.0_f64 - retained) * group * reset_mean;
            }
        }
        mean
    }

    pub(crate) fn write_posterior(
        &self,
        now_micros: u64,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> bool {
        if values.len() != self.rates.len() || probabilities.len() != self.rates.len() {
            return false;
        }
        let elapsed = Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
            .as_secs_f64();
        values.copy_from_slice(&self.rates);
        probabilities.fill(0.0_f64);
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * elapsed).exp();
            for reset in 0..RESET_COUNT {
                let group = (0..self.rates.len())
                    .map(|rate| self.probability[cell(hazard, reset, rate, self.rates.len())])
                    .sum::<f64>();
                for (rate, probability) in probabilities.iter_mut().enumerate() {
                    *probability += retained
                        * self.probability[cell(hazard, reset, rate, self.rates.len())]
                        + (1.0_f64 - retained) * group * self.reset_probability[reset][rate];
                }
            }
        }
        true
    }

    pub(crate) fn boundary_diagnostic(&self, now_micros: u64) -> ArrivalBoundaryDiagnostic {
        let elapsed = Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
            .as_secs_f64();
        let mut lower = 0.0_f64;
        let mut upper = 0.0_f64;
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * elapsed).exp();
            for reset in 0..RESET_COUNT {
                let group = (0..self.rates.len())
                    .map(|rate| self.probability[cell(hazard, reset, rate, self.rates.len())])
                    .sum::<f64>();
                let endpoint = |rate| {
                    retained * self.probability[cell(hazard, reset, rate, self.rates.len())]
                        + (1.0_f64 - retained) * group * self.reset_probability[reset][rate]
                };
                lower += endpoint(0);
                upper += endpoint(self.rates.len() - 1);
            }
        }
        ArrivalBoundaryDiagnostic {
            lower_endpoint_probability: lower,
            upper_endpoint_probability: upper,
            probability_budget: EPSILON_BOUNDARY,
        }
    }

    pub(crate) fn count_predictive(
        &mut self,
        now_micros: u64,
        observed_count: u32,
        exposure_seconds: f64,
    ) -> Result<ArrivalCountPredictive, ArrivalPredictiveError> {
        self.validate_predictive_exposure(exposure_seconds)?;
        let transition_seconds =
            Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
                .as_secs_f64()
                + exposure_seconds;
        let observed_count = u64::from(observed_count);
        let upper_cdf =
            self.predictive_cdf(observed_count, exposure_seconds, transition_seconds)?;
        let lower_cdf = if observed_count == 0 {
            0.0_f64
        } else {
            self.predictive_cdf(observed_count - 1, exposure_seconds, transition_seconds)?
        };
        let mut quantiles = [0_u64; 3];
        for (index, threshold) in [0.1_f64, 0.5_f64, 0.9_f64].into_iter().enumerate() {
            let mut high = 1_u64;
            while high < u64::MAX
                && self.predictive_cdf(high, exposure_seconds, transition_seconds)? < threshold
            {
                high = high.saturating_mul(2);
            }
            let mut low = 0_u64;
            while low < high {
                let middle = low.midpoint(high);
                if self.predictive_cdf(middle, exposure_seconds, transition_seconds)? >= threshold {
                    high = middle;
                } else {
                    low = middle.saturating_add(1);
                }
            }
            quantiles[index] = low;
        }
        Ok(ArrivalCountPredictive {
            lower_cdf,
            upper_cdf,
            quantiles,
        })
    }

    fn validate_predictive_exposure(
        &self,
        exposure_seconds: f64,
    ) -> Result<(), ArrivalPredictiveError> {
        if !exposure_seconds.is_finite()
            || exposure_seconds <= 0.0_f64
            || exposure_seconds > T_MAX_SECONDS
            || PoissonMean::new(self.rates[self.rates.len() - 1] * exposure_seconds).is_none()
        {
            return Err(ArrivalPredictiveError::InvalidExposure);
        }
        Ok(())
    }

    fn predictive_cdf(
        &mut self,
        count: u64,
        exposure_seconds: f64,
        transition_seconds: f64,
    ) -> Result<f64, ArrivalPredictiveError> {
        for (poisson_cdf, rate) in self.rate_scratch.iter_mut().zip(&self.rates) {
            let mean = rate * exposure_seconds;
            if PoissonMean::new(mean).is_none() {
                return Err(ArrivalPredictiveError::InvalidExposure);
            }
            let distribution =
                Poisson::new(mean).map_err(|_| ArrivalPredictiveError::InvalidDistribution)?;
            *poisson_cdf = distribution.cdf(count);
        }
        let mut local_cdf = 0.0_f64;
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * transition_seconds).exp();
            for reset in 0..RESET_COUNT {
                let group = (0..self.rates.len())
                    .map(|rate| self.probability[cell(hazard, reset, rate, self.rates.len())])
                    .sum::<f64>();
                for (rate, poisson_cdf) in self.rate_scratch.iter().copied().enumerate() {
                    let probability = retained
                        * self.probability[cell(hazard, reset, rate, self.rates.len())]
                        + (1.0_f64 - retained) * group * self.reset_probability[reset][rate];
                    local_cdf += probability * poisson_cdf;
                }
            }
        }
        let calendar_probability = self.calendar_probability();
        if calendar_probability <= 0.0_f64 || self.calendar_rate <= 0.0_f64 {
            return Ok(local_cdf.clamp(0.0_f64, 1.0_f64));
        }
        let success = self.calendar_rate / (self.calendar_rate + exposure_seconds);
        let calendar = NegativeBinomial::new(self.calendar_shape, success)
            .map_err(|_| ArrivalPredictiveError::InvalidDistribution)?;
        Ok(((1.0_f64 - calendar_probability) * local_cdf
            + calendar_probability * calendar.cdf(count))
        .clamp(0.0_f64, 1.0_f64))
    }

    pub(crate) fn sample_rate_path(
        &self,
        duration_seconds: f64,
        random: &mut RandomStream,
        end_seconds: &mut [f64],
        rates: &mut [f64],
        calendar: Option<CalendarForecast<'_>>,
        now_micros: u64,
    ) -> usize {
        if duration_seconds == 0.0_f64 {
            return 0;
        }
        assert!(
            duration_seconds <= T_MAX_SECONDS,
            "arrival path duration exceeds the validated horizon"
        );
        assert!(
            end_seconds.len().min(rates.len()) > self.model.path_change_bound,
            "arrival path storage is smaller than its validated bound"
        );
        if let Some(forecast) = calendar
            && calendar_covers(forecast.segments, now_micros, duration_seconds)
            && random.random::<f64>() < self.calendar_probability()
        {
            let length = self.sample_calendar_path(
                forecast,
                now_micros,
                duration_seconds,
                random,
                end_seconds,
                rates,
            );
            if sample_path_counts(
                &random.clone().domain(ARRIVAL_COUNT_DOMAIN),
                end_seconds,
                rates,
                length,
            ) {
                return length;
            }
        }
        let elapsed = Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
            .as_secs_f64();
        loop {
            let (hazard, reset, mut rate) = self.sample_joint(random);
            if random.random::<f64>() >= (-self.hazards[hazard] * elapsed).exp() {
                rate = sample_discrete(&self.reset_probability[reset], random);
            }
            let mut cursor = 0.0_f64;
            let mut length = 0;
            loop {
                let until_change = -random.open_unit_f64().ln() / self.hazards[hazard];
                let end = (cursor + until_change).min(duration_seconds);
                end_seconds[length] = end;
                rates[length] = self.rates[rate];
                length += 1;
                if end >= duration_seconds {
                    if sample_path_counts(
                        &random.clone().domain(ARRIVAL_COUNT_DOMAIN),
                        end_seconds,
                        rates,
                        length,
                    ) {
                        return length;
                    }
                    break;
                }
                if length > self.model.path_change_bound {
                    break;
                }
                cursor = end;
                rate = sample_discrete(&self.reset_probability[reset], random);
            }
        }
    }

    fn sample_joint(&self, random: &mut RandomStream) -> (usize, usize, usize) {
        let selected = sample_discrete(&self.probability, random);
        let hazard = selected / (RESET_COUNT * self.rates.len());
        let remainder = selected % (RESET_COUNT * self.rates.len());
        (
            hazard,
            remainder / self.rates.len(),
            remainder % self.rates.len(),
        )
    }

    fn sample_calendar_path(
        &self,
        forecast: CalendarForecast<'_>,
        now_micros: u64,
        duration_seconds: f64,
        random: &mut RandomStream,
        end_seconds: &mut [f64],
        rates: &mut [f64],
    ) -> usize {
        let end_micros = now_micros.saturating_add((duration_seconds * 1_000_000.0_f64) as u64);
        let mut length = 0;
        for segment in 0..forecast.segments.len() {
            if forecast.segments.end_micros(segment) <= now_micros
                || forecast.segments.start_micros(segment) >= end_micros
            {
                continue;
            }
            let uses_updated = self.calendar_artifact == Some(forecast.artifact)
                && self.calendar_position == forecast.segments.position(segment);
            let (shape, rate) = if uses_updated {
                (self.calendar_shape, self.calendar_rate)
            } else {
                (
                    forecast.segments.shape(segment),
                    forecast.segments.rate_seconds(segment),
                )
            };
            end_seconds[length] = Duration::from_micros(
                forecast
                    .segments
                    .end_micros(segment)
                    .min(end_micros)
                    .saturating_sub(now_micros),
            )
            .as_secs_f64();
            rates[length] = sample_gamma(shape, random) / rate;
            length += 1;
        }
        length
    }

    fn log_predictive_mass(&mut self, count: u32, exposure: f64) -> f64 {
        self.predictive_probability(count, exposure).ln()
    }

    fn predictive_probability(&mut self, count: u32, exposure: f64) -> f64 {
        for (mass, rate) in self.rate_scratch.iter_mut().zip(&self.rates) {
            *mass = poisson_mass(count, rate * exposure);
        }
        let mut mass = 0.0_f64;
        for hazard in 0..self.hazards.len() {
            let retained = (-self.hazards[hazard] * exposure).exp();
            for reset in 0..RESET_COUNT {
                let group = (0..self.rates.len())
                    .map(|rate| self.probability[cell(hazard, reset, rate, self.rates.len())])
                    .sum::<f64>();
                for rate in 0..self.rates.len() {
                    let destination = retained
                        * self.probability[cell(hazard, reset, rate, self.rates.len())]
                        + (1.0_f64 - retained) * group * self.reset_probability[reset][rate];
                    mass += destination * self.rate_scratch[rate];
                }
            }
        }
        mass
    }

    fn calendar_probability(&self) -> f64 {
        logistic(self.calendar_log_odds)
    }
}

const fn cell(hazard: usize, reset: usize, rate: usize, rate_count: usize) -> usize {
    (hazard * RESET_COUNT + reset) * rate_count + rate
}

fn derive_hazard_grid(mean: f64, tail_limit: f64) -> Result<GridSpec, ArrivalPriorError> {
    let distribution =
        Gamma::new(HAZARD_SHAPE, HAZARD_SHAPE).map_err(|_| ArrivalPriorError::InvalidChangeRate)?;
    let low = mean * distribution.inverse_cdf(tail_limit);
    let high = mean * distribution.inverse_cdf(1.0_f64 - tail_limit);
    let intervals =
        ((high / low).ln() / (E * HAZARD_TRANSITION_PROBABILITY_ERROR_MAX)).ceil() as usize;
    Ok(GridSpec {
        low,
        log_step: log_step(low, high, intervals)?,
        count: intervals
            .checked_add(1)
            .ok_or(ArrivalPriorError::ArithmeticOverflow)?,
    })
}

fn derive_rate_grid(shape: f64, mean: f64, tail_limit: f64) -> Result<GridSpec, ArrivalPriorError> {
    let (low, high) = rate_window(shape, mean, tail_limit)?;
    let intervals = ((high / low).ln() / (2.0_f64 * EPSILON_GRID.ln_1p())).ceil() as usize;
    Ok(GridSpec {
        low,
        log_step: log_step(low, high, intervals)?,
        count: intervals
            .checked_add(1)
            .ok_or(ArrivalPriorError::ArithmeticOverflow)?,
    })
}

fn arrival_artifact_budget(cell_count: usize) -> Result<PriorArtifactBudget, ArrivalPriorError> {
    let operations = 7_usize
        .checked_mul(cell_count)
        .ok_or(ArrivalPriorError::ArithmeticOverflow)?;
    Ok(PriorArtifactBudget::new(
        u32::try_from(cell_count).map_err(|_| ArrivalPriorError::ArithmeticOverflow)?,
        STORAGE_BUDGET_BYTES as u64,
        u64::try_from(operations).map_err(|_| ArrivalPriorError::ArithmeticOverflow)?,
        EPSILON_BOUNDARY,
        0.0_f64,
        EPSILON_GRID.max(HAZARD_TRANSITION_PROBABILITY_ERROR_MAX),
    ))
}

fn cell_count(hazard_count: usize, rate_count: usize) -> Result<usize, ArrivalPriorError> {
    hazard_count
        .checked_mul(RESET_COUNT)
        .and_then(|count| count.checked_mul(rate_count))
        .ok_or(ArrivalPriorError::ArithmeticOverflow)
}

fn log_step(low: f64, high: f64, interval_count: usize) -> Result<f64, ArrivalPriorError> {
    let intervals = u32::try_from(interval_count)
        .map(f64::from)
        .map_err(|_| ArrivalPriorError::ArithmeticOverflow)?;
    Ok((high.ln() - low.ln()) / intervals)
}

fn arrival_grids(hazard: GridSpec, rate: GridSpec) -> ArrivalGrids {
    ArrivalGrids {
        hazards: geometric_grid(hazard.low, hazard.log_step, hazard.count),
        rates: geometric_grid(rate.low, rate.log_step, rate.count),
    }
}

fn geometric_grid(low: f64, log_step: f64, count: usize) -> Box<[f64]> {
    (0..count)
        .map(|index| {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            (low.ln() + f64::from(index) * log_step).exp()
        })
        .collect()
}

fn geometric_lower(values: &[f64], index: usize) -> f64 {
    (values[index - 1] * values[index]).sqrt()
}

fn geometric_upper(values: &[f64], index: usize) -> f64 {
    (values[index] * values[index + 1]).sqrt()
}

fn rate_window(shape: f64, mean: f64, tail_limit: f64) -> Result<(f64, f64), ArrivalPriorError> {
    let mut low = f64::INFINITY;
    let mut high = 0.0_f64;
    for density_shape in [shape, shape * 0.5_f64, shape, shape * 2.0_f64] {
        let distribution =
            Gamma::new(density_shape, density_shape).map_err(|_| ArrivalPriorError::InvalidRate)?;
        low = low.min(mean * distribution.inverse_cdf(tail_limit));
        high = high.max(mean * distribution.inverse_cdf(1.0_f64 - tail_limit));
    }
    while [shape, shape * 0.5_f64, shape, shape * 2.0_f64]
        .into_iter()
        .any(|density_shape| gamma_lr(density_shape, density_shape * low / mean) > tail_limit)
    {
        low *= 0.5_f64;
    }
    while [shape, shape * 0.5_f64, shape, shape * 2.0_f64]
        .into_iter()
        .any(|density_shape| gamma_ur(density_shape, density_shape * high / mean) > tail_limit)
    {
        high *= 2.0_f64;
    }
    if !low.is_finite() || low <= 0.0_f64 || !high.is_finite() || high <= low {
        return Err(ArrivalPriorError::InvalidRate);
    }
    Ok((low, high))
}

fn exact_gamma_masses(values: &[f64], shape: f64, mean: f64) -> Box<[f64]> {
    (0..values.len())
        .map(|index| {
            let lower = if index == 0 {
                0.0_f64
            } else {
                geometric_lower(values, index)
            };
            let upper = if index + 1 == values.len() {
                f64::INFINITY
            } else {
                geometric_upper(values, index)
            };
            if index == 0 {
                gamma_lr(shape, shape * upper / mean)
            } else if index + 1 == values.len() {
                gamma_ur(shape, shape * lower / mean)
            } else if values[index] <= mean {
                gamma_lr(shape, shape * upper / mean) - gamma_lr(shape, shape * lower / mean)
            } else {
                gamma_ur(shape, shape * lower / mean) - gamma_ur(shape, shape * upper / mean)
            }
        })
        .collect()
}

fn sample_discrete(probability: &[f64], random: &mut RandomStream) -> usize {
    let draw = random.open_unit_f64();
    let mut cumulative = 0.0_f64;
    for (index, value) in probability.iter().enumerate() {
        cumulative += value;
        if draw <= cumulative {
            return index;
        }
    }
    probability.len() - 1
}

fn poisson_mass(count: u32, mean: f64) -> f64 {
    log_poisson_mass(count, mean).exp()
}

fn arrival_coverage(
    shape: f64,
    mean: f64,
    rates: &[f64],
    hazards: &[f64],
    hazard_mean: f64,
    achieved_rate_error: f64,
) -> Result<([PriorCoverageRecord; RESET_COUNT + 2], usize), ArrivalPriorError> {
    let mut coverage =
        [PriorCoverageRecord::new(0.0_f64, 1.0_f64, 0.0_f64, 0.0_f64, 0.0_f64); RESET_COUNT + 2];
    for (record, reset_shape) in coverage
        .iter_mut()
        .zip([shape * 0.5_f64, shape, shape * 2.0_f64])
    {
        let distribution =
            Gamma::new(reset_shape, reset_shape).map_err(|_| ArrivalPriorError::InvalidRate)?;
        let lower_endpoint = rates[0];
        let upper_endpoint = rates[rates.len() - 1];
        let lower_tail = distribution.cdf(lower_endpoint / mean);
        let upper_tail = gamma_ur(reset_shape, reset_shape * upper_endpoint / mean);
        if !lower_tail.is_finite() || !upper_tail.is_finite() {
            return Err(ArrivalPriorError::InvalidRate);
        }
        if lower_tail + upper_tail > EPSILON_BOUNDARY {
            return Err(ArrivalPriorError::BoundaryMass);
        }
        *record = PriorCoverageRecord::new(
            lower_endpoint,
            upper_endpoint,
            lower_tail,
            upper_tail,
            achieved_rate_error,
        );
    }
    let hazard_lower_endpoint = hazards[0];
    let hazard_upper_endpoint = hazards[hazards.len() - 1];
    let hazard_lower_tail = gamma_lr(
        HAZARD_SHAPE,
        HAZARD_SHAPE * hazard_lower_endpoint / hazard_mean,
    );
    let hazard_upper_tail = gamma_ur(
        HAZARD_SHAPE,
        HAZARD_SHAPE * hazard_upper_endpoint / hazard_mean,
    );
    if hazard_lower_tail + hazard_upper_tail > EPSILON_BOUNDARY {
        return Err(ArrivalPriorError::HazardTailMass);
    }
    coverage[RESET_COUNT] = PriorCoverageRecord::new(
        hazard_lower_endpoint,
        hazard_upper_endpoint,
        hazard_lower_tail,
        hazard_upper_tail,
        HAZARD_TRANSITION_PROBABILITY_ERROR_MAX,
    );
    let maximum_mean = hazards[hazards.len() - 1] * T_MAX_SECONDS;
    let path_change_bound = poisson_tail_bound(maximum_mean, EPSILON_PATH)?;
    let path_change_bound_u32 =
        u32::try_from(path_change_bound).map_err(|_| ArrivalPriorError::InvalidPathBound {
            required: path_change_bound,
            maximum: PATH_SEGMENT_LIMIT,
        })?;
    let path_distribution =
        Poisson::new(maximum_mean).map_err(|_| ArrivalPriorError::InvalidPathBound {
            required: 0,
            maximum: PATH_SEGMENT_LIMIT,
        })?;
    let path_tail = 1.0_f64 - path_distribution.cdf(u64::from(path_change_bound_u32));
    coverage[RESET_COUNT + 1] = PriorCoverageRecord::new(
        0.0_f64,
        f64::from(path_change_bound_u32),
        0.0_f64,
        path_tail,
        0.0_f64,
    );
    Ok((coverage, path_change_bound))
}

fn log_poisson_mass(count: u32, mean: f64) -> f64 {
    if mean == 0.0_f64 {
        return if count == 0 {
            0.0_f64
        } else {
            f64::NEG_INFINITY
        };
    }
    -mean + f64::from(count) * mean.ln() - ln_gamma(f64::from(count) + 1.0_f64)
}

fn poisson_tail_bound(mean: f64, epsilon: f64) -> Result<usize, ArrivalPriorError> {
    if !mean.is_finite() || mean <= 0.0_f64 {
        return Err(ArrivalPriorError::InvalidPathBound {
            required: 0,
            maximum: PATH_SEGMENT_LIMIT,
        });
    }
    let distribution = Poisson::new(mean).map_err(|_| ArrivalPriorError::InvalidPathBound {
        required: 0,
        maximum: PATH_SEGMENT_LIMIT,
    })?;
    let mut count = mean.floor() as usize;
    while 1.0_f64 - distribution.cdf(count as u64) > epsilon {
        count = count
            .checked_add(1)
            .ok_or(ArrivalPriorError::ArithmeticOverflow)?;
        if count >= PATH_SEGMENT_LIMIT {
            return Err(ArrivalPriorError::InvalidPathBound {
                required: count + 1,
                maximum: PATH_SEGMENT_LIMIT,
            });
        }
    }
    Ok(count)
}

fn sample_path_counts(
    random: &RandomStream,
    end_seconds: &[f64],
    rates: &mut [f64],
    length: usize,
) -> bool {
    let mut start = 0.0_f64;
    for segment in 0..length {
        let duration = end_seconds[segment] - start;
        if duration <= 0.0_f64 {
            return false;
        }
        let mean = PoissonMean::from_product(rates[segment], duration);
        // The named domain separates count noise from path-state draws.
        let mut segment_random = random.clone().domain(segment as u64);
        rates[segment] = count_as_f64(sample_poisson(mean, &mut segment_random)) / duration;
        start = end_seconds[segment];
    }
    true
}

fn calendar_segment_at(segments: &CalendarColumns, now_micros: u64) -> Option<usize> {
    (0..segments.len()).find(|&segment| {
        segments.start_micros(segment) <= now_micros && now_micros < segments.end_micros(segment)
    })
}

fn calendar_covers(segments: &CalendarColumns, now_micros: u64, duration_seconds: f64) -> bool {
    let Some(first) = calendar_segment_at(segments, now_micros) else {
        return false;
    };
    let end_micros = now_micros.saturating_add((duration_seconds * 1_000_000.0_f64) as u64);
    segments.start_micros(first) <= now_micros
        && segments.len() > 0
        && segments.end_micros(segments.len() - 1) >= end_micros
}

fn logit(probability: f64) -> f64 {
    probability.ln() - (-probability).ln_1p()
}

fn logistic(log_odds: f64) -> f64 {
    if log_odds >= 0.0_f64 {
        1.0_f64 / (1.0_f64 + (-log_odds).exp())
    } else {
        let odds = log_odds.exp();
        odds / (1.0_f64 + odds)
    }
}

fn log_predictive_mass(shape: f64, rate: f64, count: u32, exposure: f64) -> f64 {
    let count = f64::from(count);
    ln_gamma(count + shape) - ln_gamma(shape) - ln_gamma(count + 1.0_f64)
        + shape * (rate / (rate + exposure)).ln()
        + count * (exposure / (rate + exposure)).ln()
}

/// Failure while constructing an arrival model.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum ArrivalPriorError {
    /// The Gamma shape is not finite and positive.
    #[error("arrival prior shape must be finite and positive")]
    InvalidShape,
    /// The Gamma rate is not finite and positive.
    #[error("arrival prior rate must be finite and positive")]
    InvalidRate,
    /// The change rate is not finite and positive.
    #[error("arrival change rate must be finite and positive")]
    InvalidChangeRate,
    /// Model size arithmetic overflowed.
    #[error("arrival model size arithmetic overflowed")]
    ArithmeticOverflow,
    /// The finite-state arrays exceed the storage budget.
    #[error("arrival model requires {required} bytes but its budget is {budget} bytes")]
    StorageBudget {
        /// Required array bytes.
        required: usize,
        /// Permitted array bytes.
        budget: usize,
    },
    /// The path rejection buffer exceeds its limit.
    #[error("arrival path requires {required} segments but supports at most {maximum}")]
    InvalidPathBound {
        /// Required segment count.
        required: usize,
        /// Permitted segment count.
        maximum: usize,
    },
    /// The model can create a non-finite Poisson mean.
    #[error("arrival model creates an invalid Poisson mean")]
    InvalidPoissonMean,
    /// An accuracy budget is not a probability.
    #[error("arrival accuracy budgets must be between zero and one")]
    InvalidAccuracyBudget,
    /// The continuous reset prior has too much mass outside the rate grid.
    #[error("arrival reset prior exceeds the boundary-mass budget")]
    BoundaryMass,
    /// The continuous hazard prior has too much mass outside the hazard grid.
    #[error("arrival hazard prior exceeds the boundary-mass budget")]
    HazardTailMass,
}

/// Failure from an exact arrival-count prediction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ArrivalPredictiveError {
    /// The exposure is outside the validated Poisson domain.
    #[error("arrival predictive exposure is outside the validated domain")]
    InvalidExposure,
    /// A validated predictive distribution could not be constructed.
    #[error("arrival predictive distribution is invalid")]
    InvalidDistribution,
}

#[cfg(test)]
#[path = "arrival_tests.rs"]
mod tests;
