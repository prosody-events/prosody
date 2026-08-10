use std::f64::consts::LN_2;
use std::time::Duration;

use rand::RngExt;
use rand_distr::{Distribution, Poisson};
use statrs::function::gamma::ln_gamma;
use thiserror::Error;

use crate::change_point::ChangePointKernel;
use crate::random::sample_gamma;
use crate::types::{CalendarColumns, CalendarForecast};
use crate::{ArrivalPosterior, CalendarArtifactId, RandomStream};

const RUN_LENGTH_CAPACITY: usize = 1_024;
const RATE_BIN_COUNT: usize = 64;
const RATE_LOG_MIN: f64 = -6.907_755_278_982_137_f64;
const RATE_LOG_WIDTH: f64 = 0.323_801_028_702_287_66_f64;
const RATE_LOG_WIDTH_RECIPROCAL: f64 = 3.088_316_315_756_457_7_f64;
const DEFAULT_PRIOR_TRUST_SECONDS: f64 = 86_400.0_f64;
const DEFAULT_OCCUPANCY_HALF_LIFE_SECONDS: f64 = 604_800.0_f64;

/// Prior for the current arrival-rate segment.
#[derive(Clone, Copy, Debug)]
pub struct ArrivalPrior {
    shape: f64,
    rate_seconds: f64,
    change_kernel: ChangePointKernel,
    run_length_max: usize,
    prior_trust_seconds: f64,
    occupancy_half_life_seconds: f64,
}

impl ArrivalPrior {
    /// Constructs one bounded Gamma-Poisson change-point prior.
    ///
    /// # Errors
    ///
    /// Returns an error when a parameter cannot define a proper prior.
    pub fn new(
        shape: f64,
        rate_seconds: f64,
        change_rate_per_second: f64,
        run_length_max: usize,
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
        if !(2..=RUN_LENGTH_CAPACITY).contains(&run_length_max) {
            return Err(ArrivalPriorError::InvalidRunLength {
                maximum: RUN_LENGTH_CAPACITY,
            });
        }
        Ok(Self {
            shape,
            rate_seconds,
            change_kernel: ChangePointKernel::new(change_rate_per_second),
            run_length_max,
            prior_trust_seconds: DEFAULT_PRIOR_TRUST_SECONDS,
            occupancy_half_life_seconds: DEFAULT_OCCUPANCY_HALF_LIFE_SECONDS,
        })
    }

    /// Sets how quickly observed rate levels replace the configured jump prior.
    ///
    /// `prior_trust_seconds` is the exposure that gives history and the prior
    /// equal weight. `occupancy_half_life_seconds` controls how quickly old
    /// rate levels lose weight.
    ///
    /// # Errors
    ///
    /// Returns an error when a duration is not finite and positive.
    pub fn with_transition_learning(
        mut self,
        prior_trust_seconds: f64,
        occupancy_half_life_seconds: f64,
    ) -> Result<Self, ArrivalPriorError> {
        if !prior_trust_seconds.is_finite() || prior_trust_seconds <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidPriorTrust);
        }
        if !occupancy_half_life_seconds.is_finite() || occupancy_half_life_seconds <= 0.0_f64 {
            return Err(ArrivalPriorError::InvalidOccupancyHalfLife);
        }
        self.prior_trust_seconds = prior_trust_seconds;
        self.occupancy_half_life_seconds = occupancy_half_life_seconds;
        Ok(self)
    }

    /// Returns a broad prior for tests that do not study arrival inference.
    #[must_use]
    pub const fn broad_fallback() -> Self {
        Self {
            shape: 1.0_f64,
            rate_seconds: 1.0_f64,
            change_kernel: ChangePointKernel::new(1.0_f64 / 300.0_f64),
            run_length_max: RUN_LENGTH_CAPACITY,
            prior_trust_seconds: DEFAULT_PRIOR_TRUST_SECONDS,
            occupancy_half_life_seconds: DEFAULT_OCCUPANCY_HALF_LIFE_SECONDS,
        }
    }

    pub(crate) const fn path_segment_count_max(self) -> usize {
        self.run_length_max
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

/// Exposure-weighted occupancy of observed arrival rates.
///
/// This model assumes that an arrival process revisits rate levels that it
/// occupied before. Weights are non-negative, and `total` is their sum. A
/// zero total makes the jump sampler use only the configured prior.
pub(crate) struct RateOccupancy {
    weights: [f64; RATE_BIN_COUNT],
    cumulative: [f64; RATE_BIN_COUNT],
    total: f64,
}

impl RateOccupancy {
    pub(crate) const fn new() -> Self {
        Self {
            weights: [0.0_f64; RATE_BIN_COUNT],
            cumulative: [0.0_f64; RATE_BIN_COUNT],
            total: 0.0_f64,
        }
    }

    pub(crate) fn record(
        &mut self,
        rate_per_second: f64,
        exposure_seconds: f64,
        elapsed_seconds: f64,
        half_life_seconds: f64,
    ) {
        let decay = (-LN_2 * elapsed_seconds / half_life_seconds).exp();
        for weight in &mut self.weights {
            *weight *= decay;
        }
        let index = rate_bin(rate_per_second);
        self.weights[index] += exposure_seconds;
        let mut cumulative = 0.0_f64;
        for (weight, value) in self.weights.iter().zip(&mut self.cumulative) {
            cumulative += weight;
            *value = cumulative;
        }
        self.total = cumulative;
    }

    pub(crate) fn sample(&self, prior: ArrivalPrior, random: &mut RandomStream) -> f64 {
        let prior_probability =
            prior.prior_trust_seconds / (prior.prior_trust_seconds + self.total);
        if self.total <= f64::EPSILON || random.random::<f64>() < prior_probability {
            return sample_gamma(prior.shape, random) / prior.rate_seconds;
        }
        let draw = random.random::<f64>() * self.total;
        let selected = self
            .cumulative
            .partition_point(|&cumulative| cumulative < draw)
            .min(RATE_BIN_COUNT - 1);
        let selected = u32::try_from(selected).map_or(u32::MAX, |index| index);
        let log_rate =
            RATE_LOG_MIN + (f64::from(selected) + random.open_unit_f64()) * RATE_LOG_WIDTH;
        log_rate.exp()
    }

    #[cfg(test)]
    pub(crate) fn total(&self) -> f64 {
        self.total
    }

    #[cfg(test)]
    pub(crate) const fn weights(&self) -> &[f64; RATE_BIN_COUNT] {
        &self.weights
    }

    #[cfg(test)]
    pub(crate) const fn cumulative(&self) -> &[f64; RATE_BIN_COUNT] {
        &self.cumulative
    }
}

/// Posterior evidence for the arrival change frequency.
pub(crate) struct ChangeHazard {
    change_mass: f64,
    exposure_seconds: f64,
}

impl ChangeHazard {
    pub(crate) const fn new() -> Self {
        Self {
            change_mass: 0.0_f64,
            exposure_seconds: 0.0_f64,
        }
    }

    pub(crate) fn record(
        &mut self,
        change_probability: f64,
        exposure_seconds: f64,
        elapsed_seconds: f64,
        half_life_seconds: f64,
    ) {
        let decay = (-LN_2 * elapsed_seconds / half_life_seconds).exp();
        self.change_mass = self.change_mass * decay + change_probability;
        self.exposure_seconds = self.exposure_seconds * decay + exposure_seconds;
    }

    fn sample(&self, prior: ArrivalPrior, random: &mut RandomStream) -> f64 {
        let prior_shape = prior.change_kernel.rate_per_second() * prior.prior_trust_seconds;
        sample_gamma(prior_shape + self.change_mass, random)
            / (prior.prior_trust_seconds + self.exposure_seconds)
    }

    #[cfg(test)]
    pub(crate) fn mean(&self, prior: ArrivalPrior) -> f64 {
        (prior.change_kernel.rate_per_second() * prior.prior_trust_seconds + self.change_mass)
            / (prior.prior_trust_seconds + self.exposure_seconds)
    }
}

pub(crate) struct ArrivalFactor {
    prior: ArrivalPrior,
    probability: [f64; RUN_LENGTH_CAPACITY],
    cumulative_probability: [f64; RUN_LENGTH_CAPACITY],
    shape: [f64; RUN_LENGTH_CAPACITY],
    rate: [f64; RUN_LENGTH_CAPACITY],
    next_probability: [f64; RUN_LENGTH_CAPACITY],
    next_shape: [f64; RUN_LENGTH_CAPACITY],
    next_rate: [f64; RUN_LENGTH_CAPACITY],
    length: usize,
    calendar_artifact: Option<CalendarArtifactId>,
    calendar_position: u32,
    calendar_shape: f64,
    calendar_rate: f64,
    calendar_log_odds: f64,
    last_evidence_micros: u64,
    rate_occupancy: RateOccupancy,
    change_hazard: ChangeHazard,
}

impl ArrivalFactor {
    pub(crate) fn new(prior: ArrivalPrior) -> Self {
        let mut factor = Self {
            prior,
            probability: [0.0_f64; RUN_LENGTH_CAPACITY],
            cumulative_probability: [0.0_f64; RUN_LENGTH_CAPACITY],
            shape: [0.0_f64; RUN_LENGTH_CAPACITY],
            rate: [0.0_f64; RUN_LENGTH_CAPACITY],
            next_probability: [0.0_f64; RUN_LENGTH_CAPACITY],
            next_shape: [0.0_f64; RUN_LENGTH_CAPACITY],
            next_rate: [0.0_f64; RUN_LENGTH_CAPACITY],
            length: 1,
            calendar_artifact: None,
            calendar_position: 0,
            calendar_shape: 0.0_f64,
            calendar_rate: 0.0_f64,
            calendar_log_odds: f64::NEG_INFINITY,
            last_evidence_micros: 0,
            rate_occupancy: RateOccupancy::new(),
            change_hazard: ChangeHazard::new(),
        };
        factor.probability[0] = 1.0_f64;
        factor.cumulative_probability[0] = 1.0_f64;
        factor.shape[0] = prior.shape;
        factor.rate[0] = prior.rate_seconds;
        factor
    }

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
        let exposure_duration = Duration::from_micros(exposure_micros);
        let exposure = exposure_duration.as_secs_f64();
        if exposure <= f64::EPSILON {
            drop(token);
            return;
        }
        let exposure_micros = exposure_micros.min(now_micros);
        let evidence_start_micros = now_micros - exposure_micros;
        let missing_micros = evidence_start_micros.saturating_sub(self.last_evidence_micros);
        let elapsed_seconds =
            Duration::from_micros(now_micros.saturating_sub(self.last_evidence_micros))
                .as_secs_f64();
        self.propagate_missing(Duration::from_micros(missing_micros));
        self.prepare_calendar(calendar, now_micros);
        if let Some(forecast) = calendar
            && calendar_segment_at(forecast.segments, now_micros).is_some()
        {
            self.calendar_log_odds +=
                log_predictive_mass(self.calendar_shape, self.calendar_rate, count, exposure)
                    - self.log_predictive_mass(count, exposure);
            self.calendar_shape += f64::from(count);
            self.calendar_rate += exposure;
        }
        self.update_local(count, exposure_duration);
        self.last_evidence_micros = now_micros;
        self.rate_occupancy.record(
            self.expected_rate(now_micros),
            exposure,
            elapsed_seconds,
            self.prior.occupancy_half_life_seconds,
        );
        self.change_hazard.record(
            self.probability[0],
            exposure,
            elapsed_seconds,
            self.prior.occupancy_half_life_seconds,
        );
        drop(token);
    }

    fn propagate_missing(&mut self, elapsed: Duration) {
        let transition = self.prior.change_kernel.probabilities(elapsed);
        if transition.redrawn <= f64::EPSILON {
            return;
        }
        let next_length = (self.length + 1).min(self.prior.run_length_max);
        self.next_probability[..next_length].fill(0.0_f64);
        self.next_probability[0] = transition.redrawn;
        self.next_shape[0] = self.prior.shape;
        self.next_rate[0] = self.prior.rate_seconds;
        for index in 0..self.length {
            let next_index = (index + 1).min(self.prior.run_length_max - 1);
            self.next_probability[next_index] = transition.retained * self.probability[index];
            self.next_shape[next_index] = self.shape[index];
            self.next_rate[next_index] = self.rate[index];
        }
        let normalizer = self.next_probability[..next_length].iter().sum::<f64>();
        for probability in &mut self.next_probability[..next_length] {
            *probability /= normalizer;
        }
        self.probability[..next_length].copy_from_slice(&self.next_probability[..next_length]);
        self.shape[..next_length].copy_from_slice(&self.next_shape[..next_length]);
        self.rate[..next_length].copy_from_slice(&self.next_rate[..next_length]);
        self.length = next_length;
        self.refresh_cumulative_probability();
    }

    pub(crate) fn prepare_calendar(
        &mut self,
        calendar: Option<CalendarForecast<'_>>,
        now_micros: u64,
    ) {
        let Some(forecast) = calendar else {
            return;
        };
        let Some(segment) = calendar_segment_at(forecast.segments, now_micros) else {
            return;
        };
        let artifact_changed = self.calendar_artifact != Some(forecast.artifact);
        let segment_changed =
            artifact_changed || self.calendar_position != forecast.segments.position(segment);
        if segment_changed {
            self.calendar_artifact = Some(forecast.artifact);
            self.calendar_position = forecast.segments.position(segment);
            self.calendar_shape = forecast.segments.shape(segment);
            self.calendar_rate = forecast.segments.rate_seconds(segment);
        }
        if artifact_changed {
            self.calendar_log_odds = logit(forecast.prior_probability);
        }
    }

    fn update_local(&mut self, count: u32, exposure: Duration) {
        let transition = self.prior.change_kernel.probabilities(exposure);
        let exposure = exposure.as_secs_f64();
        let prior_mass =
            log_predictive_mass(self.prior.shape, self.prior.rate_seconds, count, exposure);
        let next_length = (self.length + 1).min(self.prior.run_length_max);
        self.next_probability[..next_length].fill(f64::NEG_INFINITY);
        let mut change_probability = 0.0_f64;
        for index in 0..self.length {
            change_probability += self.probability[index] * transition.redrawn;
            let next_index = (index + 1).min(self.prior.run_length_max - 1);
            self.next_probability[next_index] = self.probability[index].ln()
                + transition.retained.ln()
                + log_predictive_mass(self.shape[index], self.rate[index], count, exposure);
            self.next_shape[next_index] = self.shape[index] + f64::from(count);
            self.next_rate[next_index] = self.rate[index] + exposure;
        }
        self.next_probability[0] = change_probability.ln() + prior_mass;
        self.next_shape[0] = self.prior.shape + f64::from(count);
        self.next_rate[0] = self.prior.rate_seconds + exposure;
        let maximum = self.next_probability[..next_length]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        if maximum.is_finite() {
            let normalizer = self.next_probability[..next_length]
                .iter_mut()
                .map(|log_probability| {
                    *log_probability = (*log_probability - maximum).exp();
                    *log_probability
                })
                .sum::<f64>();
            for probability in &mut self.next_probability[..next_length] {
                *probability /= normalizer;
            }
            self.probability[..next_length].copy_from_slice(&self.next_probability[..next_length]);
            self.shape[..next_length].copy_from_slice(&self.next_shape[..next_length]);
            self.rate[..next_length].copy_from_slice(&self.next_rate[..next_length]);
            self.length = next_length;
            self.refresh_cumulative_probability();
        }
    }

    fn refresh_cumulative_probability(&mut self) {
        let mut cumulative = 0.0_f64;
        for index in 0..self.length {
            cumulative += self.probability[index];
            self.cumulative_probability[index] = cumulative;
        }
        self.cumulative_probability[self.length - 1] = 1.0_f64;
    }

    pub(crate) fn expected_rate(&self, now_micros: u64) -> f64 {
        let retained = (0..self.length)
            .map(|index| self.probability[index] * self.shape[index] / self.rate[index])
            .sum::<f64>();
        let change_probability = self.missing_change_probability(now_micros);
        let prior = self.prior.shape / self.prior.rate_seconds;
        let live = (1.0_f64 - change_probability) * retained + change_probability * prior;
        let calendar_probability = self.calendar_probability();
        let calendar = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate
        } else {
            live
        };
        (1.0_f64 - calendar_probability) * live + calendar_probability * calendar
    }

    pub(crate) fn posterior(&self, now_micros: u64) -> ArrivalPosterior {
        let retained_mean = (0..self.length)
            .map(|index| self.probability[index] * self.shape[index] / self.rate[index])
            .sum::<f64>();
        let retained_variance = (0..self.length)
            .map(|index| {
                let state_mean = self.shape[index] / self.rate[index];
                let state_variance = self.shape[index] / self.rate[index].powi(2);
                self.probability[index]
                    * (state_variance + (state_mean - retained_mean) * (state_mean - retained_mean))
            })
            .sum::<f64>();
        let change_probability = self.missing_change_probability(now_micros);
        let prior_mean = self.prior.shape / self.prior.rate_seconds;
        let prior_variance = self.prior.shape / self.prior.rate_seconds.powi(2);
        let live_mean =
            (1.0_f64 - change_probability) * retained_mean + change_probability * prior_mean;
        let live_variance = (1.0_f64 - change_probability)
            * (retained_variance + (retained_mean - live_mean).powi(2))
            + change_probability * (prior_variance + (prior_mean - live_mean).powi(2));
        let calendar_probability = self.calendar_probability();
        let calendar_mean = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate
        } else {
            live_mean
        };
        let calendar_variance = if self.calendar_rate > 0.0_f64 {
            self.calendar_shape / self.calendar_rate.powi(2)
        } else {
            live_variance
        };
        let mean =
            (1.0_f64 - calendar_probability) * live_mean + calendar_probability * calendar_mean;
        let variance = (1.0_f64 - calendar_probability)
            * (live_variance + (live_mean - mean).powi(2))
            + calendar_probability * (calendar_variance + (calendar_mean - mean).powi(2));
        let shape = mean * mean / variance;
        ArrivalPosterior {
            shape,
            rate: mean / variance,
        }
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
        let bound = end_seconds
            .len()
            .min(rates.len())
            .min(self.prior.run_length_max);
        if bound == 0 || duration_seconds <= f64::EPSILON {
            return 0;
        }
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
                &random.clone().domain(0x6172_7269_7661_6c73),
                end_seconds,
                rates,
                length,
            );
            return length;
        }
        let hazard_rate = self.change_hazard.sample(self.prior, random);
        let mut rate = self.sample_current_rate(random);
        let missing_change_probability = self.missing_change_probability(now_micros);
        if missing_change_probability > 0.0_f64
            && random.random::<f64>() < missing_change_probability
        {
            rate = self.rate_occupancy.sample(self.prior, random);
        }
        let mut cursor = 0.0_f64;
        for index in 0..bound {
            let uniform = random.random::<f64>();
            let until_change = -(-uniform).ln_1p() / hazard_rate;
            cursor = (cursor + until_change).min(duration_seconds);
            end_seconds[index] = cursor;
            rates[index] = rate;
            if cursor >= duration_seconds {
                let length = index + 1;
                sample_path_counts(
                    &random.clone().domain(0x6172_7269_7661_6c73),
                    end_seconds,
                    rates,
                    length,
                );
                return length;
            }
            rate = self.rate_occupancy.sample(self.prior, random);
        }
        end_seconds[bound - 1] = duration_seconds;
        sample_path_counts(
            &random.clone().domain(0x6172_7269_7661_6c73),
            end_seconds,
            rates,
            bound,
        );
        bound
    }

    fn sample_current_rate(&self, random: &mut RandomStream) -> f64 {
        let draw = random.random::<f64>();
        let selected = self.cumulative_probability[..self.length]
            .partition_point(|&probability| probability < draw)
            .min(self.length - 1);
        sample_gamma(self.shape[selected], random) / self.rate[selected]
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
            if length == end_seconds.len().min(rates.len()) {
                break;
            }
            let uses_updated_segment = self.calendar_artifact == Some(forecast.artifact)
                && self.calendar_position == forecast.segments.position(segment);
            let (shape, rate) = if uses_updated_segment {
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
        let maximum = (0..self.length)
            .map(|index| {
                self.probability[index].ln()
                    + log_predictive_mass(self.shape[index], self.rate[index], count, exposure)
            })
            .fold(f64::NEG_INFINITY, f64::max);
        let normalized = (0..self.length)
            .map(|index| {
                (self.probability[index].ln()
                    + log_predictive_mass(self.shape[index], self.rate[index], count, exposure)
                    - maximum)
                    .exp()
            })
            .sum::<f64>();
        maximum + normalized.ln()
    }

    fn calendar_probability(&self) -> f64 {
        logistic(self.calendar_log_odds)
    }

    fn missing_change_probability(&self, now_micros: u64) -> f64 {
        self.prior
            .change_kernel
            .probabilities(Duration::from_micros(
                now_micros.saturating_sub(self.last_evidence_micros),
            ))
            .redrawn
    }

    #[cfg(test)]
    pub(crate) fn predictive_probability(&self, count: u32, exposure_seconds: f64) -> f64 {
        (0..self.length)
            .map(|index| {
                self.probability[index]
                    * predictive_mass(self.shape[index], self.rate[index], count, exposure_seconds)
            })
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn calendar_model_probability(&self) -> f64 {
        self.calendar_probability()
    }
}

pub(crate) fn rate_bin(rate_per_second: f64) -> usize {
    let position =
        (rate_per_second.max(1.0e-3_f64).ln() - RATE_LOG_MIN) * RATE_LOG_WIDTH_RECIPROCAL;
    (position as usize).min(RATE_BIN_COUNT - 1)
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
        let mean = rates[segment] * duration;
        rates[segment] = if duration <= f64::EPSILON || mean <= f64::EPSILON {
            0.0_f64
        } else if let Ok(distribution) = Poisson::new(mean) {
            let mut segment_random = random.clone().domain(segment as u64);
            distribution.sample(&mut segment_random) / duration
        } else {
            0.0_f64
        };
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

#[cfg(test)]
fn predictive_mass(shape: f64, rate: f64, count: u32, exposure: f64) -> f64 {
    log_predictive_mass(shape, rate, count, exposure).exp()
}

fn log_predictive_mass(shape: f64, rate: f64, count: u32, exposure: f64) -> f64 {
    let count = f64::from(count);
    ln_gamma(count + shape) - ln_gamma(shape) - ln_gamma(count + 1.0_f64)
        + shape * (rate / (rate + exposure)).ln()
        + count * (exposure / (rate + exposure)).ln()
}

/// Failure while constructing an arrival prior.
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
    /// The jump-prior trust exposure is not finite and positive.
    #[error("arrival prior trust must be finite and positive")]
    InvalidPriorTrust,
    /// The rate-occupancy half-life is not finite and positive.
    #[error("arrival occupancy half-life must be finite and positive")]
    InvalidOccupancyHalfLife,
    /// The run-length bound is outside the supported range.
    #[error("arrival run-length bound must be between 2 and {maximum}")]
    InvalidRunLength {
        /// Largest supported run-length bound.
        maximum: usize,
    },
}

#[derive(Debug, Eq, PartialEq)]
struct EvidenceToken;

impl Drop for EvidenceToken {
    fn drop(&mut self) {}
}
