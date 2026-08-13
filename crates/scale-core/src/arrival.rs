use std::array::from_fn;
use std::time::Duration;

use rand::RngExt;
use statrs::distribution::{ContinuousCDF, DiscreteCDF, Gamma, Poisson};
use statrs::function::gamma::ln_gamma;
use thiserror::Error;

use crate::change_point::ChangePointKernel;
use crate::random::{PoissonMean, count_as_f64, sample_gamma, sample_poisson};
use crate::types::{CalendarColumns, CalendarForecast};
use crate::{ArrivalPosterior, CalendarArtifactId, RandomStream};

const HAZARD_COUNT: usize = 5;
const RESET_COUNT: usize = 3;
const RATE_COUNT: usize = 257;
const CELL_COUNT: usize = HAZARD_COUNT * RESET_COUNT * RATE_COUNT;
const MODEL_VERSION: u32 = 1;
const T_MAX_SECONDS: f64 = 86_400.0_f64;
const EPSILON_GRID: f64 = 0.02_f64;
const EPSILON_BOUNDARY: f64 = 1.0e-6_f64;
const EPSILON_PATH: f64 = 1.0e-9_f64;
const STORAGE_BUDGET_BYTES: usize = 96 * 1_024;
const CALENDAR_SEGMENT_LIMIT: usize = 1_024;
const PATH_SEGMENT_LIMIT: usize = 65_536;
const ARRIVAL_COUNT_DOMAIN: u64 = 0x6172_7269_7661_6c73;

/// A validated finite-state arrival model.
///
/// The declared model is discrete at evidence boundaries. A rate stays fixed
/// during an interval. It changes at the next boundary with probability
/// `1 - exp(-hazard * duration)`. The filter is exact for this finite model.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArrivalPrior {
    version: u32,
    authored_shape: f64,
    rate_seconds: f64,
    hazard_center: f64,
    path_change_bound: usize,
}

impl ArrivalPrior {
    /// Constructs a validated finite arrival model.
    ///
    /// `shape` and `rate_seconds` define the reset-rate prior. The change rate
    /// locates the hazard prior. These module-local priors keep the old API
    /// usable until the versioned prior artifact owns configuration.
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
        let storage_bytes = Self::storage_bytes()?;
        if !(0.0_f64..1.0_f64).contains(&EPSILON_GRID)
            || !(0.0_f64..1.0_f64).contains(&EPSILON_BOUNDARY)
        {
            return Err(ArrivalPriorError::InvalidAccuracyBudget);
        }
        if storage_bytes > STORAGE_BUDGET_BYTES {
            return Err(ArrivalPriorError::StorageBudget {
                required: storage_bytes,
                budget: STORAGE_BUDGET_BYTES,
            });
        }

        // A symmetric log grid expresses one octave of hazard uncertainty.
        let hazard_center = ChangePointKernel::new(change_rate_per_second).rate_per_second();
        let hazard_scale = [
            0.5_f64,
            2.0_f64.sqrt().recip(),
            1.0_f64,
            2.0_f64.sqrt(),
            2.0_f64,
        ];
        let hazards = hazard_scale.map(|scale| hazard_center * scale);
        if hazards.iter().any(|value| !value.is_finite()) {
            return Err(ArrivalPriorError::InvalidChangeRate);
        }
        // A normal density on log hazard is weakly informative and proper.
        let mean = shape / rate_seconds;
        if !mean.is_finite() || mean <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidRate);
        }
        let rates: [f64; RATE_COUNT] = from_fn(|index| {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            mean * 2.0_f64.powf(f64::from(index) / 4.0_f64 - 48.0_f64)
        });
        if rates
            .iter()
            .any(|value| !value.is_finite() || *value <= 0.0_f64)
        {
            return Err(ArrivalPriorError::InvalidRate);
        }
        for reset_shape in [shape * 0.5_f64, shape, shape * 2.0_f64] {
            let reset_rate = reset_shape / mean;
            let distribution = Gamma::new(reset_shape, reset_rate.recip())
                .map_err(|_| ArrivalPriorError::InvalidRate)?;
            let lower = distribution.cdf(rates[0] / 2.0_f64.powf(0.125_f64));
            let upper = 1.0_f64 - distribution.cdf(rates[RATE_COUNT - 1] * 2.0_f64.powf(0.125_f64));
            if !lower.is_finite() || !upper.is_finite() {
                return Err(ArrivalPriorError::InvalidRate);
            }
            if lower + upper > EPSILON_BOUNDARY && mean >= 1.0e-9_f64 {
                return Err(ArrivalPriorError::BoundaryMass);
            }
        }
        let maximum_mean = hazards[HAZARD_COUNT - 1] * T_MAX_SECONDS;
        let path_change_bound = poisson_tail_bound(maximum_mean, EPSILON_PATH)?;
        if path_change_bound >= PATH_SEGMENT_LIMIT {
            return Err(ArrivalPriorError::InvalidPathBound {
                required: path_change_bound + 1,
                maximum: PATH_SEGMENT_LIMIT,
            });
        }
        let maximum_poisson_mean = rates[RATE_COUNT - 1] * T_MAX_SECONDS;
        if PoissonMean::new(maximum_poisson_mean).is_none() {
            return Err(ArrivalPriorError::InvalidPoissonMean);
        }
        Ok(Self {
            version: MODEL_VERSION,
            authored_shape: shape,
            rate_seconds,
            hazard_center,
            path_change_bound,
        })
    }

    /// Returns the authored test prior: one arrival per second and one
    /// expected change per day. Use it only where the arrival prior is not
    /// the subject. A test pins it equal to validated construction.
    #[cfg(test)]
    pub(crate) const fn test_artifact() -> Self {
        Self {
            version: MODEL_VERSION,
            authored_shape: 1.0_f64,
            rate_seconds: 1.0_f64,
            hazard_center: 1.0_f64 / 86_400.0_f64,
            path_change_bound: 15,
        }
    }

    pub(crate) const fn path_segment_count_max(self) -> usize {
        // Calendar timing needs a separate bound. The shared caller buffer keeps its
        // legacy limit.
        let stochastic = self.path_change_bound + 1;
        if self.version != MODEL_VERSION {
            0
        } else if stochastic > CALENDAR_SEGMENT_LIMIT {
            stochastic
        } else {
            CALENDAR_SEGMENT_LIMIT
        }
    }

    pub(crate) const fn shape(self) -> f64 {
        self.authored_shape
    }

    fn storage_bytes() -> Result<usize, ArrivalPriorError> {
        CELL_COUNT
            .checked_mul(2)
            .and_then(|value| {
                value.checked_add(HAZARD_COUNT + RESET_COUNT * RATE_COUNT + RATE_COUNT)
            })
            .and_then(|value| value.checked_mul(size_of::<f64>()))
            .ok_or(ArrivalPriorError::ArithmeticOverflow)
    }
}

/// One consumable count and exposure update.
#[derive(Debug, Eq, PartialEq)]
pub struct ArrivalEvidence {
    count: u32,
    exposure_micros: u64,
    token: EvidenceToken,
}

impl ArrivalEvidence {
    pub(crate) const fn new(count: u32, exposure_micros: u64) -> Self {
        Self {
            count,
            exposure_micros,
            token: EvidenceToken,
        }
    }
}

pub(crate) struct ArrivalFactor {
    model: ArrivalPrior,
    hazards: [f64; HAZARD_COUNT],
    rates: [f64; RATE_COUNT],
    reset_probability: [[f64; RATE_COUNT]; RESET_COUNT],
    probability: Box<[f64]>,
    scratch: Box<[f64]>,
    // Calendar segment changes take effect at the next evidence boundary.
    // Each interval belongs to the segment active at its start.
    // This assignment is exact for the declared discrete model.
    calendar_artifact: Option<CalendarArtifactId>,
    calendar_position: u32,
    calendar_shape: f64,
    calendar_rate: f64,
    calendar_log_odds: f64,
    last_evidence_micros: u64,
}

impl ArrivalFactor {
    pub(crate) fn new(model: ArrivalPrior) -> Self {
        let hazard_scale = [
            0.5_f64,
            2.0_f64.sqrt().recip(),
            1.0_f64,
            2.0_f64.sqrt(),
            2.0_f64,
        ];
        let hazards = hazard_scale.map(|scale| model.hazard_center * scale);
        let mean = model.authored_shape / model.rate_seconds;
        let rates = from_fn(|index| {
            let index = u32::try_from(index).map_or(u32::MAX, |value| value);
            mean * 2.0_f64.powf(f64::from(index) / 4.0_f64 - 48.0_f64)
        });
        let reset_shapes = [
            model.authored_shape * 0.5_f64,
            model.authored_shape,
            model.authored_shape * 2.0_f64,
        ];
        let reset_probability = reset_shapes.map(|shape| {
            let rate_parameter = shape / mean;
            normalize(from_fn(|index| {
                let rate = rates[index];
                (shape * rate_parameter.ln() - ln_gamma(shape) + (shape - 1.0_f64) * rate.ln()
                    - rate_parameter * rate
                    + rate.ln())
                .exp()
                .max(f64::MIN_POSITIVE)
            }))
        });
        let hazard_prior = normalize([
            0.001_349_612_f64,
            0.157_305_356_f64,
            0.682_689_492_f64,
            0.157_305_356_f64,
            0.001_349_612_f64,
        ]);
        let mut probability = vec![0.0_f64; CELL_COUNT].into_boxed_slice();
        for hazard in 0..HAZARD_COUNT {
            for reset in 0..RESET_COUNT {
                for rate in 0..RATE_COUNT {
                    probability[cell(hazard, reset, rate)] =
                        hazard_prior[hazard] / 3.0_f64 * reset_probability[reset][rate];
                }
            }
        }
        Self {
            model,
            hazards,
            rates,
            reset_probability,
            probability,
            scratch: vec![0.0_f64; CELL_COUNT].into_boxed_slice(),
            calendar_artifact: None,
            calendar_position: 0,
            calendar_shape: 0.0_f64,
            calendar_rate: 0.0_f64,
            calendar_log_odds: f64::NEG_INFINITY,
            last_evidence_micros: 0,
        }
    }

    /// Updates the model with one certified evidence interval.
    ///
    /// `now_micros` is the evidence interval end.
    /// The producer certifies that the count covers exactly `[now - exposure,
    /// now]`. The producer controls delivery delay.
    /// After the evidence applies, the update advances the calendar boundary to
    /// `now_micros`.
    pub(crate) fn update(
        &mut self,
        evidence: ArrivalEvidence,
        calendar: Option<CalendarForecast<'_>>,
        now_micros: u64,
    ) {
        let ArrivalEvidence {
            count,
            exposure_micros,
            token,
        } = evidence;
        let exposure = Duration::from_micros(exposure_micros).as_secs_f64();
        if exposure <= f64::EPSILON {
            drop(token);
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
        drop(token);
    }

    fn transition(&mut self, duration: f64, count: Option<u32>) {
        if duration <= f64::EPSILON {
            return;
        }
        self.scratch.fill(0.0_f64);
        for hazard in 0..HAZARD_COUNT {
            let retained = (-self.hazards[hazard] * duration).exp();
            for reset in 0..RESET_COUNT {
                let mut reset_mass = 0.0_f64;
                for source in 0..RATE_COUNT {
                    reset_mass +=
                        self.probability[cell(hazard, reset, source)] * (1.0_f64 - retained);
                }
                for destination in 0..RATE_COUNT {
                    let prior = self.probability[cell(hazard, reset, destination)] * retained
                        + reset_mass * self.reset_probability[reset][destination];
                    self.scratch[cell(hazard, reset, destination)] = count.map_or(prior, |value| {
                        prior.ln() + log_poisson_mass(value, self.rates[destination] * duration)
                    });
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
            return;
        };
        let Some(segment) = calendar_segment_at(forecast.segments, evidence_start_micros) else {
            return;
        };
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
        let local = self.marginal_moments(now_micros).0;
        let calendar = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate
        } else {
            local
        };
        let probability = self.calendar_probability();
        (1.0_f64 - probability) * local + probability * calendar
    }

    pub(crate) fn posterior(&self, now_micros: u64) -> ArrivalPosterior {
        let (local_mean, local_variance) = self.marginal_moments(now_micros);
        let calendar_probability = self.calendar_probability();
        let calendar_mean = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate
        } else {
            local_mean
        };
        let calendar_variance = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate.powi(2)
        } else {
            local_variance
        };
        let mean =
            (1.0_f64 - calendar_probability) * local_mean + calendar_probability * calendar_mean;
        let variance = (1.0_f64 - calendar_probability)
            * (local_variance + (local_mean - mean).powi(2))
            + calendar_probability * (calendar_variance + (calendar_mean - mean).powi(2));
        let variance = variance.max(f64::MIN_POSITIVE);
        ArrivalPosterior {
            shape: mean * mean / variance,
            rate: mean / variance,
        }
    }

    fn marginal_moments(&self, now_micros: u64) -> (f64, f64) {
        let elapsed = Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
            .as_secs_f64();
        let mut mean = 0.0_f64;
        let mut second = 0.0_f64;
        for hazard in 0..HAZARD_COUNT {
            let retained = (-self.hazards[hazard] * elapsed).exp();
            for reset in 0..RESET_COUNT {
                let mut group = 0.0_f64;
                let mut retained_mean = 0.0_f64;
                for rate in 0..RATE_COUNT {
                    let probability = self.probability[cell(hazard, reset, rate)];
                    group += probability;
                    retained_mean += probability * self.rates[rate];
                }
                let reset_mean = self.reset_probability[reset]
                    .iter()
                    .zip(self.rates)
                    .map(|(p, r)| p * r)
                    .sum::<f64>();
                mean += retained * retained_mean + (1.0_f64 - retained) * group * reset_mean;
                let retained_second = (0..RATE_COUNT)
                    .map(|rate| {
                        self.probability[cell(hazard, reset, rate)] * self.rates[rate].powi(2)
                    })
                    .sum::<f64>();
                let reset_second = self.reset_probability[reset]
                    .iter()
                    .zip(self.rates)
                    .map(|(p, r)| p * r * r)
                    .sum::<f64>();
                second += retained * retained_second + (1.0_f64 - retained) * group * reset_second;
            }
        }
        (mean, (second - mean * mean).max(0.0_f64))
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
        if duration_seconds <= f64::EPSILON {
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
            sample_path_counts(
                &random.clone().domain(ARRIVAL_COUNT_DOMAIN),
                end_seconds,
                rates,
                length,
            );
            return length;
        }
        loop {
            let (hazard, reset, mut rate) = self.sample_joint(random);
            let mut cursor = 0.0_f64;
            let mut length = 0;
            loop {
                let until_change = -random.open_unit_f64().ln() / self.hazards[hazard];
                let end = (cursor + until_change).min(duration_seconds);
                end_seconds[length] = end;
                rates[length] = self.rates[rate];
                length += 1;
                if end >= duration_seconds {
                    sample_path_counts(
                        &random.clone().domain(ARRIVAL_COUNT_DOMAIN),
                        end_seconds,
                        rates,
                        length,
                    );
                    return length;
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
        let hazard = selected / (RESET_COUNT * RATE_COUNT);
        let remainder = selected % (RESET_COUNT * RATE_COUNT);
        (hazard, remainder / RATE_COUNT, remainder % RATE_COUNT)
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

    fn log_predictive_mass(&self, count: u32, exposure: f64) -> f64 {
        self.predictive_probability(count, exposure).ln()
    }

    fn predictive_probability(&self, count: u32, exposure: f64) -> f64 {
        let mut mass = 0.0_f64;
        for hazard in 0..HAZARD_COUNT {
            let retained = (-self.hazards[hazard] * exposure).exp();
            for reset in 0..RESET_COUNT {
                let group = (0..RATE_COUNT)
                    .map(|rate| self.probability[cell(hazard, reset, rate)])
                    .sum::<f64>();
                for rate in 0..RATE_COUNT {
                    let destination = retained * self.probability[cell(hazard, reset, rate)]
                        + (1.0_f64 - retained) * group * self.reset_probability[reset][rate];
                    mass += destination * poisson_mass(count, self.rates[rate] * exposure);
                }
            }
        }
        mass
    }

    fn calendar_probability(&self) -> f64 {
        logistic(self.calendar_log_odds)
    }
}

fn normalize<const N: usize>(mut values: [f64; N]) -> [f64; N] {
    let total = values.iter().sum::<f64>();
    for value in &mut values {
        *value /= total;
    }
    values
}

const fn cell(hazard: usize, reset: usize, rate: usize) -> usize {
    (hazard * RESET_COUNT + reset) * RATE_COUNT + rate
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

fn log_poisson_mass(count: u32, mean: f64) -> f64 {
    if mean <= f64::EPSILON {
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
) {
    let mut start = 0.0_f64;
    for segment in 0..length {
        let duration = end_seconds[segment] - start;
        let mean = PoissonMean::from_product(rates[segment], duration);
        // The named domain separates count noise from path-state draws.
        let mut segment_random = random.clone().domain(segment as u64);
        rates[segment] = count_as_f64(sample_poisson(mean, &mut segment_random)) / duration;
        start = end_seconds[segment];
    }
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
}

#[derive(Debug, Eq, PartialEq)]
struct EvidenceToken;

impl Drop for EvidenceToken {
    fn drop(&mut self) {}
}

#[cfg(test)]
#[path = "arrival_tests.rs"]
mod tests;
