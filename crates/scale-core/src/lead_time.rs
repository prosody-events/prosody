use std::f64::consts::{SQRT_2, TAU};
use std::time::Duration;

use fearless_simd::{Level, Simd, dispatch, prelude::*};
use thiserror::Error;

use crate::RandomStream;

const DIRECTION_COUNT: usize = 2;
const DELTA_BUCKET_COUNT: usize = 4;
const GRID_CELL_COUNT: usize = 12;
const DRIFT_PROBABILITY: f64 = 0.01_f64;
const AXIS_COUNT: usize = 4;
const SCALE_COUNT: usize = 3;

/// One versioned population prior for a positive transition duration.
#[derive(Clone, Debug, PartialEq)]
pub struct TransitionPrior {
    mu_log_seconds: [f64; AXIS_COUNT],
    sigma_log_seconds: [f64; SCALE_COUNT],
    probabilities: [f64; GRID_CELL_COUNT],
}

impl TransitionPrior {
    /// Constructs a normalized mixture over twelve log-normal hypotheses.
    ///
    /// The probability order is median-major and then deviation-major.
    ///
    /// # Errors
    ///
    /// Returns an error when a value is invalid or the mass is not positive.
    pub fn new(
        median_seconds: [f64; AXIS_COUNT],
        log_standard_deviations: [f64; SCALE_COUNT],
        mut probabilities: [f64; GRID_CELL_COUNT],
    ) -> Result<Self, TransitionPriorError> {
        if !median_seconds
            .iter()
            .all(|value| value.is_finite() && *value > 0.0_f64)
            || !log_standard_deviations
                .iter()
                .all(|value| value.is_finite() && *value > 0.0_f64)
            || !probabilities
                .iter()
                .all(|value| value.is_finite() && *value >= 0.0_f64)
        {
            return Err(TransitionPriorError::InvalidValue);
        }
        let total = probabilities.iter().sum::<f64>();
        if total <= f64::EPSILON {
            return Err(TransitionPriorError::EmptyMass);
        }
        for probability in &mut probabilities {
            *probability /= total;
        }
        Ok(Self {
            mu_log_seconds: median_seconds.map(f64::ln),
            sigma_log_seconds: log_standard_deviations,
            probabilities,
        })
    }

    /// Returns the explicit broad fallback for callers without an artifact.
    #[must_use]
    pub fn broad_fallback() -> Self {
        Self {
            mu_log_seconds: [15.0_f64.ln(), 30.0_f64.ln(), 60.0_f64.ln(), 120.0_f64.ln()],
            sigma_log_seconds: [0.1_f64, 0.3_f64, 0.6_f64],
            probabilities: [1.0_f64 / GRID_CELL_COUNT as f64; GRID_CELL_COUNT],
        }
    }
}

/// Direction of one replica transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionDirection {
    /// The requested replica count increased.
    Up,
    /// The requested replica count decreased.
    Down,
}

/// One consumable actuation lead-time observation.
#[derive(Debug, PartialEq)]
pub struct TransitionEvidence {
    kind: TransitionEvidenceKind,
    token: EvidenceToken,
}

impl TransitionEvidence {
    /// Constructs one completed transition observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the replica delta or elapsed time is zero.
    pub fn completed(
        direction: TransitionDirection,
        replica_delta: u32,
        elapsed_micros: u64,
    ) -> Result<Self, TransitionEvidenceError> {
        validate(replica_delta, elapsed_micros)?;
        Ok(Self {
            kind: TransitionEvidenceKind::Completed {
                direction,
                replica_delta,
                pre_pause_micros: elapsed_micros,
                pause_micros: None,
            },
            token: EvidenceToken,
        })
    }

    /// Constructs one completed transition with a KIP-848 partition pause.
    ///
    /// # Errors
    ///
    /// Returns an error when a count or phase duration is zero.
    pub fn completed_rebalance(
        direction: TransitionDirection,
        replica_delta: u32,
        pre_pause_micros: u64,
        pause_micros: u64,
    ) -> Result<Self, TransitionEvidenceError> {
        validate(replica_delta, pre_pause_micros)?;
        validate(replica_delta, pause_micros)?;
        Ok(Self {
            kind: TransitionEvidenceKind::Completed {
                direction,
                replica_delta,
                pre_pause_micros,
                pause_micros: Some(pause_micros),
            },
            token: EvidenceToken,
        })
    }

    /// Constructs one right-censored transition observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the replica delta or exposure is zero.
    pub fn censored(
        direction: TransitionDirection,
        replica_delta: u32,
        exposure_micros: u64,
    ) -> Result<Self, TransitionEvidenceError> {
        validate(replica_delta, exposure_micros)?;
        Ok(Self {
            kind: TransitionEvidenceKind::Censored {
                direction,
                replica_delta,
                exposure_micros,
            },
            token: EvidenceToken,
        })
    }

    /// Constructs one transition censored during a KIP-848 partition pause.
    ///
    /// # Errors
    ///
    /// Returns an error when a count or phase duration is zero.
    pub fn censored_rebalance(
        direction: TransitionDirection,
        replica_delta: u32,
        pre_pause_micros: u64,
        pause_exposure_micros: u64,
    ) -> Result<Self, TransitionEvidenceError> {
        validate(replica_delta, pre_pause_micros)?;
        validate(replica_delta, pause_exposure_micros)?;
        Ok(Self {
            kind: TransitionEvidenceKind::CensoredPause {
                direction,
                replica_delta,
                pre_pause_micros,
                pause_exposure_micros,
            },
            token: EvidenceToken,
        })
    }

    pub(crate) fn consume(self) -> (PhaseEvidence, Option<PhaseEvidence>) {
        let Self { kind, token } = self;
        let phases = match kind {
            TransitionEvidenceKind::Completed {
                direction,
                replica_delta,
                pre_pause_micros,
                pause_micros,
            } => (
                PhaseEvidence {
                    direction,
                    replica_delta,
                    elapsed_micros: pre_pause_micros,
                    completed: true,
                },
                pause_micros.map(|elapsed_micros| PhaseEvidence {
                    direction,
                    replica_delta,
                    elapsed_micros,
                    completed: true,
                }),
            ),
            TransitionEvidenceKind::Censored {
                direction,
                replica_delta,
                exposure_micros,
            } => (
                PhaseEvidence {
                    direction,
                    replica_delta,
                    elapsed_micros: exposure_micros,
                    completed: false,
                },
                None,
            ),
            TransitionEvidenceKind::CensoredPause {
                direction,
                replica_delta,
                pre_pause_micros,
                pause_exposure_micros,
            } => (
                PhaseEvidence {
                    direction,
                    replica_delta,
                    elapsed_micros: pre_pause_micros,
                    completed: true,
                },
                Some(PhaseEvidence {
                    direction,
                    replica_delta,
                    elapsed_micros: pause_exposure_micros,
                    completed: false,
                }),
            ),
        };
        drop(token);
        phases
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PhaseEvidence {
    direction: TransitionDirection,
    replica_delta: u32,
    elapsed_micros: u64,
    completed: bool,
}

#[derive(Debug, PartialEq)]
enum TransitionEvidenceKind {
    Completed {
        direction: TransitionDirection,
        replica_delta: u32,
        pre_pause_micros: u64,
        pause_micros: Option<u64>,
    },
    Censored {
        direction: TransitionDirection,
        replica_delta: u32,
        exposure_micros: u64,
    },
    CensoredPause {
        direction: TransitionDirection,
        replica_delta: u32,
        pre_pause_micros: u64,
        pause_exposure_micros: u64,
    },
}

pub(crate) struct LeadTimeFactor {
    mu_log_seconds: [f64; GRID_CELL_COUNT],
    sigma_log_seconds: [f64; GRID_CELL_COUNT],
    weights: Vec<f64>,
    weights_next: Vec<f64>,
    likelihoods: [f64; GRID_CELL_COUNT],
    last_direction: TransitionDirection,
    last_replica_delta: u32,
}

impl LeadTimeFactor {
    pub(crate) fn new(prior: &TransitionPrior) -> Self {
        let mut mu_log_seconds = [0.0_f64; GRID_CELL_COUNT];
        let mut sigma_log_seconds = [0.0_f64; GRID_CELL_COUNT];
        let mut cell = 0_usize;
        for &mu in &prior.mu_log_seconds {
            for &sigma in &prior.sigma_log_seconds {
                mu_log_seconds[cell] = mu;
                sigma_log_seconds[cell] = sigma;
                cell += 1;
            }
        }
        let factor_count = DIRECTION_COUNT * DELTA_BUCKET_COUNT;
        let mut weights = Vec::with_capacity(factor_count * GRID_CELL_COUNT);
        for _factor in 0..factor_count {
            weights.extend_from_slice(&prior.probabilities);
        }
        Self {
            mu_log_seconds,
            sigma_log_seconds,
            weights,
            weights_next: vec![0.0_f64; factor_count * GRID_CELL_COUNT],
            likelihoods: [0.0_f64; GRID_CELL_COUNT],
            last_direction: TransitionDirection::Up,
            last_replica_delta: 1,
        }
    }

    pub(crate) fn transition(&mut self) {
        self.weights_next.fill(0.0_f64);
        for factor in 0..DIRECTION_COUNT * DELTA_BUCKET_COUNT {
            let factor_start = factor * GRID_CELL_COUNT;
            for cell in 0..GRID_CELL_COUNT {
                let mu = cell / SCALE_COUNT;
                let sigma = cell % SCALE_COUNT;
                let neighbors = [
                    (mu > 0).then(|| cell - SCALE_COUNT),
                    (mu + 1 < AXIS_COUNT).then(|| cell + SCALE_COUNT),
                    (sigma > 0).then(|| cell - 1),
                    (sigma + 1 < SCALE_COUNT).then(|| cell + 1),
                ];
                let neighbor_count = neighbors.iter().flatten().count();
                let weight = self.weights[factor_start + cell];
                self.weights_next[factor_start + cell] += weight * (1.0_f64 - DRIFT_PROBABILITY);
                let divisor = match neighbor_count {
                    2 => 2.0_f64,
                    3 => 3.0_f64,
                    4 => 4.0_f64,
                    _ => 1.0_f64,
                };
                let moved = weight * DRIFT_PROBABILITY / divisor;
                for neighbor in neighbors.into_iter().flatten() {
                    self.weights_next[factor_start + neighbor] += moved;
                }
            }
        }
        self.weights.copy_from_slice(&self.weights_next);
    }

    pub(crate) fn update(&mut self, simd_level: Level, evidence: PhaseEvidence) {
        let PhaseEvidence {
            direction,
            replica_delta,
            elapsed_micros,
            completed,
        } = evidence;
        let elapsed_seconds = Duration::from_micros(elapsed_micros).as_secs_f64();
        self.last_direction = direction;
        self.last_replica_delta = replica_delta;
        let elapsed_log = elapsed_seconds.ln();
        for cell in 0..GRID_CELL_COUNT {
            let sigma = self.sigma_log_seconds[cell];
            let standardized = (elapsed_log - self.mu_log_seconds[cell]) / sigma;
            self.likelihoods[cell] = if completed {
                -elapsed_log - sigma.ln() - 0.5_f64 * standardized * standardized
            } else {
                normal_survival(standardized).max(f64::MIN_POSITIVE).ln()
            };
        }
        let start = factor_index(direction, replica_delta) * GRID_CELL_COUNT;
        let end = start + GRID_CELL_COUNT;
        apply_likelihood(
            simd_level,
            &mut self.weights[start..end],
            &mut self.likelihoods,
        );
    }

    pub(crate) fn expected_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
    ) -> f64 {
        let start = factor_index(direction, replica_delta) * GRID_CELL_COUNT;
        self.weights[start..start + GRID_CELL_COUNT]
            .iter()
            .enumerate()
            .map(|(cell, weight)| {
                weight
                    * (self.mu_log_seconds[cell]
                        + 0.5_f64 * self.sigma_log_seconds[cell] * self.sigma_log_seconds[cell])
                        .exp()
            })
            .sum()
    }

    pub(crate) fn expected_last_seconds(&self) -> f64 {
        self.expected_seconds(self.last_direction, self.last_replica_delta)
    }

    pub(crate) fn sample_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        random: &mut RandomStream,
    ) -> f64 {
        let selector = random.open_unit_f64();
        let start = factor_index(direction, replica_delta) * GRID_CELL_COUNT;
        let mut cumulative = 0.0_f64;
        let mut selected = GRID_CELL_COUNT - 1;
        for cell in 0..GRID_CELL_COUNT {
            cumulative += self.weights[start + cell];
            if cumulative >= selector {
                selected = cell;
                break;
            }
        }
        let radius = (-2.0_f64 * random.open_unit_f64().ln()).sqrt();
        let normal = radius * (TAU * random.open_unit_f64()).cos();
        (self.mu_log_seconds[selected] + self.sigma_log_seconds[selected] * normal).exp()
    }

    /// Draws each direction and delta bucket once for one posterior scenario.
    pub(crate) fn sample_bucket_seconds(&self, random: &mut RandomStream) -> [f64; 8] {
        let mut samples = [0.0_f64; 8];
        for (direction_offset, direction) in [TransitionDirection::Up, TransitionDirection::Down]
            .into_iter()
            .enumerate()
        {
            for (bucket, delta) in [1_u32, 2, 4, 8].into_iter().enumerate() {
                samples[direction_offset * DELTA_BUCKET_COUNT + bucket] =
                    self.sample_seconds(direction, delta, random);
            }
        }
        samples
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
        let elapsed_log = elapsed_seconds.ln();
        let start = factor_index(direction, replica_delta) * GRID_CELL_COUNT;
        self.weights[start..start + GRID_CELL_COUNT]
            .iter()
            .enumerate()
            .map(|(cell, weight)| {
                let standardized =
                    (elapsed_log - self.mu_log_seconds[cell]) / self.sigma_log_seconds[cell];
                weight * (1.0_f64 - normal_survival(standardized))
            })
            .sum::<f64>()
            .clamp(0.0_f64, 1.0_f64)
    }

    pub(crate) fn predictive_quantile(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        probability: f64,
    ) -> f64 {
        let probability = probability.clamp(0.0_f64, 1.0_f64);
        let sigma_max = self
            .sigma_log_seconds
            .iter()
            .copied()
            .fold(0.0_f64, f64::max);
        let mut low_log = self
            .mu_log_seconds
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min)
            - 8.0_f64 * sigma_max;
        let mut high_log = self
            .mu_log_seconds
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max)
            + 8.0_f64 * sigma_max;
        for _ in 0_u32..64 {
            let middle_log = f64::midpoint(low_log, high_log);
            if self.predictive_cdf(direction, replica_delta, middle_log.exp()) < probability {
                low_log = middle_log;
            } else {
                high_log = middle_log;
            }
        }
        f64::midpoint(low_log, high_log).exp()
    }

    pub(crate) fn sample_remaining_seconds(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        elapsed_seconds: f64,
        random: &mut RandomStream,
    ) -> f64 {
        let elapsed_cdf = self.predictive_cdf(direction, replica_delta, elapsed_seconds);
        let probability = elapsed_cdf + random.open_unit_f64() * (1.0_f64 - elapsed_cdf);
        self.predictive_quantile(direction, replica_delta, probability)
            .max(elapsed_seconds)
            - elapsed_seconds
    }

    pub(crate) const fn posterior_value_count() -> u32 {
        AXIS_COUNT as u32
    }

    pub(crate) fn write_posterior(
        &self,
        direction: TransitionDirection,
        replica_delta: u32,
        values: &mut [f64],
        probabilities: &mut [f64],
    ) -> bool {
        if values.len() != AXIS_COUNT || probabilities.len() != AXIS_COUNT {
            return false;
        }
        probabilities.fill(0.0_f64);
        let start = factor_index(direction, replica_delta) * GRID_CELL_COUNT;
        for mu in 0..AXIS_COUNT {
            values[mu] = self.mu_log_seconds[mu * SCALE_COUNT].exp();
            let cell = start + mu * SCALE_COUNT;
            probabilities[mu] = self.weights[cell..cell + SCALE_COUNT].iter().sum();
        }
        true
    }
}

#[derive(Debug, Eq, PartialEq)]
struct EvidenceToken;

impl Drop for EvidenceToken {
    fn drop(&mut self) {}
}

fn apply_likelihood(level: Level, weights: &mut [f64], likelihoods: &mut [f64]) {
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

fn factor_index(direction: TransitionDirection, replica_delta: u32) -> usize {
    let direction = match direction {
        TransitionDirection::Up => 0_usize,
        TransitionDirection::Down => 1_usize,
    };
    let delta = match replica_delta.ilog2() {
        0 => 0_usize,
        1 => 1_usize,
        2 => 2_usize,
        _ => 3_usize,
    };
    direction * DELTA_BUCKET_COUNT + delta
}

pub(crate) fn sample_index(direction: TransitionDirection, replica_delta: u32) -> usize {
    factor_index(direction, replica_delta)
}

fn validate(replica_delta: u32, elapsed_micros: u64) -> Result<(), TransitionEvidenceError> {
    if replica_delta == 0 {
        return Err(TransitionEvidenceError::ZeroReplicaDelta);
    }
    if elapsed_micros == 0 {
        return Err(TransitionEvidenceError::ZeroElapsedTime);
    }
    Ok(())
}

/// Invalid actuation lead-time evidence.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TransitionEvidenceError {
    /// The requested target did not change.
    #[error("a transition replica delta must be positive")]
    ZeroReplicaDelta,
    /// The transition supplied no elapsed exposure.
    #[error("a transition elapsed time must be positive")]
    ZeroElapsedTime,
}

/// Invalid population duration prior.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TransitionPriorError {
    /// A median, deviation, or probability is invalid.
    #[error("a transition prior value is invalid")]
    InvalidValue,
    /// The hypothesis probabilities have no positive mass.
    #[error("transition prior probability mass must be positive")]
    EmptyMass,
}
