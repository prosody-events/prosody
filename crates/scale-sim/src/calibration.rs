use std::thread::available_parallelism;

use prosody_scale_core::ArrivalPosterior;
use rayon::prelude::*;
use rayon::{ThreadPoolBuildError, ThreadPoolBuilder};
use statrs::distribution::{ContinuousCDF, Gamma, GammaError};
use thiserror::Error;

use crate::regime::run_capacity_evidence_regime_seeded_with_sensitivity;
use crate::{
    ArrivalEvidenceSample, CapacityEvidenceSample, CapacitySensitivity, PrincipalRegime,
    PrincipalRun, PrincipalRunError, RegimeExperiment, RegimeValidationError,
    run_capacity_evidence_regime_seeded, run_principal_regime_seeded, validate_principal_regime,
};

const COVERAGE_LEVELS: [f64; 4] = [0.5_f64, 0.8_f64, 0.9_f64, 0.95_f64];
const RANK_BIN_COUNT: usize = 10;
const MAX_CALIBRATION_THREADS: usize = 4;

/// Repeated capacity calibration results.
pub struct CapacityCalibration {
    trials: Vec<CapacityCalibrationTrial>,
}

/// Repeated capacity prior and grid sensitivity results.
pub struct CapacitySensitivityCalibration {
    trials: Vec<CapacitySensitivityTrial>,
}

/// Repeated arrival predictive calibration results.
pub struct DemandCalibration {
    trials: Vec<DemandCalibrationTrial>,
}

/// Repeated partition-shape predictive calibration results.
pub struct PartitionCalibration {
    trials: Vec<PartitionCalibrationTrial>,
}

/// Repeated actuation lead-time predictive calibration results.
pub struct LeadTimeCalibration {
    trials: Vec<LeadTimeCalibrationTrial>,
}

/// Lead-time calibration summary for one direction and seeded regime.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LeadTimeCalibrationTrial {
    /// Tested operating regime.
    pub regime: PrincipalRegime,
    /// Stochastic simulator seed.
    pub seed: u64,
    /// Transition direction.
    pub direction: prosody_scale_core::TransitionDirection,
    /// Completed transition observations.
    pub observation_count: u32,
    /// Right-censored transition observations.
    pub censored_count: u32,
    /// Covered observations for each predictive level.
    pub covered_counts: [u32; COVERAGE_LEVELS.len()],
    /// Predictive CDF rank counts.
    pub rank_counts: [u32; RANK_BIN_COUNT],
    /// Mean absolute duration error in seconds.
    pub mean_absolute_error_seconds: f64,
    /// Mean 80% predictive interval width in seconds.
    pub mean_uncertainty_seconds: f64,
    /// Relative contraction of the one-replica posterior width.
    pub posterior_contraction: f64,
}

/// Partition-shape calibration summary for one seeded regime.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PartitionCalibrationTrial {
    /// Tested operating regime.
    pub regime: PrincipalRegime,
    /// Stochastic simulator seed.
    pub seed: u64,
    /// Accepted individual partition assignments.
    pub observation_count: u32,
    /// Assignments covered by each highest-density credible set.
    pub covered_counts: [u32; COVERAGE_LEVELS.len()],
    /// Randomized predictive rank counts.
    pub rank_counts: [u32; RANK_BIN_COUNT],
    /// Mean negative log predictive probability.
    pub mean_log_loss: f64,
    /// Mean predictive entropy.
    pub mean_entropy: f64,
    /// Relative contraction of partition entropy.
    pub entropy_contraction: f64,
}

/// Arrival calibration summary for one seeded regime.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DemandCalibrationTrial {
    /// Tested operating regime.
    pub regime: PrincipalRegime,
    /// Stochastic simulator seed.
    pub seed: u64,
    /// Accepted predictive observations.
    pub observation_count: u32,
    /// Covered observations for each level from [`predictive_coverage_levels`].
    pub covered_counts: [u32; COVERAGE_LEVELS.len()],
    /// Randomized predictive rank counts.
    pub rank_counts: [u32; RANK_BIN_COUNT],
    /// Mean absolute arrival-count forecast error.
    pub mean_absolute_error: f64,
    /// Mean accepted arrival count.
    pub mean_observed_count: f64,
    /// Mean prequential median arrival count.
    pub mean_predicted_count: f64,
    /// Mean width of the 80% predictive count interval.
    pub mean_uncertainty: f64,
    /// Relative contraction of the arrival-rate posterior width.
    pub rate_contraction: f64,
}

/// Calibration summary for one sensitivity experiment.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CapacitySensitivityTrial {
    /// Tested prior or grid variant.
    pub sensitivity: CapacitySensitivity,
    /// Predictive calibration result for this trial.
    pub calibration: CapacityCalibrationTrial,
}

/// Calibration summary for one seeded experiment.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CapacityCalibrationTrial {
    /// Tested operating regime.
    pub regime: PrincipalRegime,
    /// Stochastic simulator seed.
    pub seed: u64,
    /// Accepted predictive observations.
    pub observation_count: u32,
    /// Covered observations for each level from [`predictive_coverage_levels`].
    pub covered_counts: [u32; COVERAGE_LEVELS.len()],
    /// Randomized predictive rank counts.
    pub rank_counts: [u32; RANK_BIN_COUNT],
    /// Mean absolute throughput forecast error.
    pub mean_absolute_error_per_second: f64,
    /// Mean width of the 80% predictive interval.
    pub mean_uncertainty_per_second: f64,
    /// Relative contraction of the capacity posterior width.
    pub capacity_contraction: f64,
}

impl CapacityCalibration {
    /// Returns each seeded trial in regime-major order.
    #[must_use]
    pub fn trials(&self) -> &[CapacityCalibrationTrial] {
        &self.trials
    }
}

impl CapacitySensitivityCalibration {
    /// Returns each seeded trial in regime-major and variant-major order.
    #[must_use]
    pub fn trials(&self) -> &[CapacitySensitivityTrial] {
        &self.trials
    }
}

impl DemandCalibration {
    /// Returns each seeded trial in regime-major order.
    #[must_use]
    pub fn trials(&self) -> &[DemandCalibrationTrial] {
        &self.trials
    }
}

impl PartitionCalibration {
    /// Returns each seeded trial in regime-major order.
    #[must_use]
    pub fn trials(&self) -> &[PartitionCalibrationTrial] {
        &self.trials
    }
}

impl LeadTimeCalibration {
    /// Returns each seeded and directional trial in regime-major order.
    #[must_use]
    pub fn trials(&self) -> &[LeadTimeCalibrationTrial] {
        &self.trials
    }
}

/// Returns the central predictive levels used by calibration reports.
#[must_use]
pub const fn predictive_coverage_levels() -> &'static [f64; COVERAGE_LEVELS.len()] {
    &COVERAGE_LEVELS
}

/// Runs seeded capacity experiments in parallel.
///
/// # Errors
///
/// Returns an error when a regime run or its declared validation fails.
pub fn run_capacity_calibration(
    regimes: &[PrincipalRegime],
    seeds: &[u64],
) -> Result<CapacityCalibration, CalibrationError> {
    let trial_count = regimes
        .len()
        .checked_mul(seeds.len())
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut requests = Vec::with_capacity(trial_count);
    for &regime in regimes {
        for &seed in seeds {
            requests.push((regime, seed));
        }
    }
    let thread_count = available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_CALIBRATION_THREADS);
    let pool = ThreadPoolBuilder::new().num_threads(thread_count).build()?;
    let trials = pool.install(|| {
        requests
            .into_par_iter()
            .map(|(regime, seed)| run_trial(regime, seed))
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(CapacityCalibration { trials })
}

/// Runs capacity prior and grid sensitivity experiments in parallel.
///
/// # Errors
///
/// Returns an error when a regime run or its declared validation fails.
pub fn run_capacity_sensitivity(
    regimes: &[PrincipalRegime],
    seeds: &[u64],
) -> Result<CapacitySensitivityCalibration, CalibrationError> {
    let trial_count = regimes
        .len()
        .checked_mul(CapacitySensitivity::ALL.len())
        .and_then(|count| count.checked_mul(seeds.len()))
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut requests = Vec::with_capacity(trial_count);
    for &regime in regimes {
        for sensitivity in CapacitySensitivity::ALL {
            for &seed in seeds {
                requests.push((regime, sensitivity, seed));
            }
        }
    }
    let thread_count = available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_CALIBRATION_THREADS);
    let pool = ThreadPoolBuilder::new().num_threads(thread_count).build()?;
    let trials = pool.install(|| {
        requests
            .into_par_iter()
            .map(|(regime, sensitivity, seed)| run_sensitivity_trial(regime, sensitivity, seed))
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(CapacitySensitivityCalibration { trials })
}

/// Runs seeded demand experiments in parallel.
///
/// # Errors
///
/// Returns an error when a regime run or its declared validation fails.
pub fn run_demand_calibration(
    regimes: &[PrincipalRegime],
    seeds: &[u64],
) -> Result<DemandCalibration, CalibrationError> {
    let trial_count = regimes
        .len()
        .checked_mul(seeds.len())
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut requests = Vec::with_capacity(trial_count);
    for &regime in regimes {
        for &seed in seeds {
            requests.push((regime, seed));
        }
    }
    let thread_count = available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_CALIBRATION_THREADS);
    let pool = ThreadPoolBuilder::new().num_threads(thread_count).build()?;
    let trials = pool.install(|| {
        requests
            .into_par_iter()
            .map(|(regime, seed)| run_demand_trial(regime, seed))
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(DemandCalibration { trials })
}

/// Runs seeded partition-shape experiments in parallel.
///
/// # Errors
///
/// Returns an error when a regime run or its declared validation fails.
pub fn run_partition_calibration(
    regimes: &[PrincipalRegime],
    seeds: &[u64],
) -> Result<PartitionCalibration, CalibrationError> {
    let trial_count = regimes
        .len()
        .checked_mul(seeds.len())
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut requests = Vec::with_capacity(trial_count);
    for &regime in regimes {
        for &seed in seeds {
            requests.push((regime, seed));
        }
    }
    let thread_count = available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_CALIBRATION_THREADS);
    let pool = ThreadPoolBuilder::new().num_threads(thread_count).build()?;
    let trials = pool.install(|| {
        requests
            .into_par_iter()
            .map(|(regime, seed)| run_partition_trial(regime, seed))
            .collect::<Result<Vec<_>, _>>()
    })?;
    Ok(PartitionCalibration { trials })
}

/// Runs seeded actuation lead-time experiments in parallel.
///
/// # Errors
///
/// Returns an error when a regime run or its declared validation fails.
pub fn run_lead_time_calibration(
    regimes: &[PrincipalRegime],
    seeds: &[u64],
) -> Result<LeadTimeCalibration, CalibrationError> {
    let request_count = regimes
        .len()
        .checked_mul(seeds.len())
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut requests = Vec::with_capacity(request_count);
    for &regime in regimes {
        for &seed in seeds {
            requests.push((regime, seed));
        }
    }
    let thread_count = available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_CALIBRATION_THREADS);
    let pool = ThreadPoolBuilder::new().num_threads(thread_count).build()?;
    let directional = pool.install(|| {
        requests
            .into_par_iter()
            .map(|(regime, seed)| run_lead_time_trial(regime, seed))
            .collect::<Result<Vec<_>, _>>()
    })?;
    let trial_count = directional
        .len()
        .checked_mul(2)
        .ok_or(CalibrationError::PlatformLimit)?;
    let mut trials = Vec::with_capacity(trial_count);
    for pair in directional {
        trials.extend(pair);
    }
    Ok(LeadTimeCalibration { trials })
}

fn run_trial(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<CapacityCalibrationTrial, CalibrationError> {
    let run = run_capacity_evidence_regime_seeded(regime, seed)?;
    validate_principal_regime(regime, RegimeExperiment::CapacityEvidence, &run)?;
    summarize_trial(regime, seed, &run)
}

fn run_sensitivity_trial(
    regime: PrincipalRegime,
    sensitivity: CapacitySensitivity,
    seed: u64,
) -> Result<CapacitySensitivityTrial, CalibrationError> {
    let run = run_capacity_evidence_regime_seeded_with_sensitivity(regime, seed, sensitivity)?;
    validate_principal_regime(regime, RegimeExperiment::CapacityEvidence, &run)?;
    Ok(CapacitySensitivityTrial {
        sensitivity,
        calibration: summarize_trial(regime, seed, &run)?,
    })
}

fn run_demand_trial(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<DemandCalibrationTrial, CalibrationError> {
    let run = run_principal_regime_seeded(regime, seed)?;
    validate_principal_regime(regime, RegimeExperiment::ClosedLoop, &run)?;
    summarize_demand_trial(regime, seed, &run)
}

fn run_partition_trial(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<PartitionCalibrationTrial, CalibrationError> {
    let run = run_principal_regime_seeded(regime, seed)?;
    validate_principal_regime(regime, RegimeExperiment::ClosedLoop, &run)?;
    summarize_partition_trial(regime, seed, &run)
}

fn run_lead_time_trial(
    regime: PrincipalRegime,
    seed: u64,
) -> Result<[LeadTimeCalibrationTrial; 2], CalibrationError> {
    let run = run_principal_regime_seeded(regime, seed)?;
    validate_principal_regime(regime, RegimeExperiment::ClosedLoop, &run)?;
    Ok([
        summarize_lead_time_trial(
            regime,
            seed,
            prosody_scale_core::TransitionDirection::Up,
            &run,
        )?,
        summarize_lead_time_trial(
            regime,
            seed,
            prosody_scale_core::TransitionDirection::Down,
            &run,
        )?,
    ])
}

fn summarize_lead_time_trial(
    regime: PrincipalRegime,
    seed: u64,
    direction: prosody_scale_core::TransitionDirection,
    run: &PrincipalRun,
) -> Result<LeadTimeCalibrationTrial, CalibrationError> {
    let mut observation_count = 0_u32;
    let mut censored_count = 0_u32;
    let mut covered_counts = [0_u32; COVERAGE_LEVELS.len()];
    let mut rank_counts = [0_u32; RANK_BIN_COUNT];
    let mut absolute_error_sum = 0.0_f64;
    let mut uncertainty_sum = 0.0_f64;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            return Err(CalibrationError::MissingControllerSample);
        };
        match sample.lead_time_evidence {
            crate::LeadTimeEvidenceSample::None => continue,
            crate::LeadTimeEvidenceSample::Censored {
                direction: observed,
                ..
            } if observed == direction => {
                censored_count = censored_count.saturating_add(1);
            }
            crate::LeadTimeEvidenceSample::Completed {
                direction: observed,
                elapsed_seconds,
                ..
            } if observed == direction => {
                observation_count = observation_count.saturating_add(1);
                for (covered, level) in covered_counts.iter_mut().zip(COVERAGE_LEVELS) {
                    let tail = (1.0_f64 - level) / 2.0_f64;
                    *covered = covered.saturating_add(u32::from(
                        sample.lead_time_predictive_rank >= tail
                            && sample.lead_time_predictive_rank <= 1.0_f64 - tail,
                    ));
                }
                let bin = rank_bin(sample.lead_time_predictive_rank);
                rank_counts[bin] = rank_counts[bin].saturating_add(1);
                absolute_error_sum +=
                    (elapsed_seconds - sample.lead_time_predictive_median_seconds).abs();
                uncertainty_sum += sample.lead_time_predictive_high_seconds
                    - sample.lead_time_predictive_low_seconds;
            }
            _ => {}
        }
    }
    let query = prosody_scale_core::PosteriorQuery::LeadTime {
        direction,
        replica_delta: 1,
    };
    let values = run
        .controller()
        .posterior_values(query)
        .ok_or(CalibrationError::MissingControllerSample)?;
    let prior = run
        .controller()
        .posterior_prior(query)
        .ok_or(CalibrationError::MissingControllerSample)?;
    let final_mass = run
        .controller()
        .posterior(query, run.controller().len().saturating_sub(1))
        .ok_or(CalibrationError::MissingControllerSample)?;
    let prior_width = posterior_width(values, prior);
    let final_width = posterior_width(values, final_mass);
    let count = f64::from(observation_count.max(1));
    Ok(LeadTimeCalibrationTrial {
        regime,
        seed,
        direction,
        observation_count,
        censored_count,
        covered_counts,
        rank_counts,
        mean_absolute_error_seconds: absolute_error_sum / count,
        mean_uncertainty_seconds: uncertainty_sum / count,
        posterior_contraction: if prior_width > f64::EPSILON {
            1.0_f64 - final_width / prior_width
        } else {
            0.0_f64
        },
    })
}

fn summarize_partition_trial(
    regime: PrincipalRegime,
    seed: u64,
    run: &PrincipalRun,
) -> Result<PartitionCalibrationTrial, CalibrationError> {
    let mut observation_count = 0_u32;
    let mut covered_counts = [0_u32; COVERAGE_LEVELS.len()];
    let mut rank_counts = [0_u32; RANK_BIN_COUNT];
    let mut log_loss_sum = 0.0_f64;
    let mut entropy_sum = 0.0_f64;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            return Err(CalibrationError::MissingControllerSample);
        };
        observation_count = observation_count.saturating_add(sample.partition_evidence_count);
        for (total, count) in covered_counts
            .iter_mut()
            .zip(sample.partition_predictive_covered_counts)
        {
            *total = total.saturating_add(count);
        }
        for (total, count) in rank_counts
            .iter_mut()
            .zip(sample.partition_predictive_rank_counts)
        {
            *total = total.saturating_add(count);
        }
        log_loss_sum += sample.partition_log_loss_sum;
        entropy_sum += sample.partition_entropy_sum;
    }
    if observation_count == 0 {
        return Err(CalibrationError::NoObservations);
    }
    let query = prosody_scale_core::PosteriorQuery::PartitionShare;
    let prior = run
        .controller()
        .posterior_prior(query)
        .ok_or(CalibrationError::MissingControllerSample)?;
    let final_mass = run
        .controller()
        .posterior(query, run.controller().len().saturating_sub(1))
        .ok_or(CalibrationError::MissingControllerSample)?;
    let prior_entropy = categorical_entropy(prior);
    let final_entropy = categorical_entropy(final_mass);
    let count = f64::from(observation_count);
    Ok(PartitionCalibrationTrial {
        regime,
        seed,
        observation_count,
        covered_counts,
        rank_counts,
        mean_log_loss: log_loss_sum / count,
        mean_entropy: entropy_sum / count,
        entropy_contraction: if prior_entropy > f64::EPSILON {
            1.0_f64 - final_entropy / prior_entropy
        } else {
            0.0_f64
        },
    })
}

fn categorical_entropy(probabilities: &[f64]) -> f64 {
    probabilities
        .iter()
        .copied()
        .filter(|probability| *probability > 0.0_f64)
        .map(|probability| -probability * probability.ln())
        .sum()
}

fn summarize_demand_trial(
    regime: PrincipalRegime,
    seed: u64,
    run: &PrincipalRun,
) -> Result<DemandCalibrationTrial, CalibrationError> {
    let mut observation_count = 0_u32;
    let mut covered_counts = [0_u32; COVERAGE_LEVELS.len()];
    let mut rank_counts = [0_u32; RANK_BIN_COUNT];
    let mut absolute_error_sum = 0.0_f64;
    let mut observed_sum = 0.0_f64;
    let mut predicted_sum = 0.0_f64;
    let mut uncertainty_sum = 0.0_f64;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            return Err(CalibrationError::MissingControllerSample);
        };
        let ArrivalEvidenceSample::Accepted(window) = sample.arrival_evidence else {
            continue;
        };
        if !sample.arrival_predictive_rank.is_finite() {
            continue;
        }
        observation_count = observation_count.saturating_add(1);
        for (covered, level) in covered_counts.iter_mut().zip(COVERAGE_LEVELS) {
            let tail = (1.0_f64 - level) / 2.0_f64;
            *covered = covered.saturating_add(u32::from(
                sample.arrival_predictive_rank >= tail
                    && sample.arrival_predictive_rank <= 1.0_f64 - tail,
            ));
        }
        let bin = rank_bin(sample.arrival_predictive_rank);
        rank_counts[bin] = rank_counts[bin].saturating_add(1);
        absolute_error_sum +=
            (f64::from(window.count) - sample.arrival_predictive_median_count).abs();
        observed_sum += f64::from(window.count);
        predicted_sum += sample.arrival_predictive_median_count;
        uncertainty_sum +=
            sample.arrival_predictive_high_count - sample.arrival_predictive_low_count;
    }
    if observation_count == 0 {
        return Err(CalibrationError::NoObservations);
    }
    let prior_width = arrival_posterior_width(run.controller().arrival_prior())?;
    let final_index = run.controller().len().saturating_sub(1);
    let final_posterior = run
        .controller()
        .arrival_posterior(final_index)
        .ok_or(CalibrationError::MissingControllerSample)?;
    let final_width = arrival_posterior_width(final_posterior)?;
    let count = f64::from(observation_count);
    Ok(DemandCalibrationTrial {
        regime,
        seed,
        observation_count,
        covered_counts,
        rank_counts,
        mean_absolute_error: absolute_error_sum / count,
        mean_observed_count: observed_sum / count,
        mean_predicted_count: predicted_sum / count,
        mean_uncertainty: uncertainty_sum / count,
        rate_contraction: 1.0_f64 - final_width / prior_width,
    })
}

fn arrival_posterior_width(posterior: ArrivalPosterior) -> Result<f64, CalibrationError> {
    let distribution = Gamma::new(posterior.shape, posterior.rate)?;
    let width = distribution.inverse_cdf(0.9_f64) - distribution.inverse_cdf(0.1_f64);
    Ok(width / (posterior.shape / posterior.rate))
}

fn summarize_trial(
    regime: PrincipalRegime,
    seed: u64,
    run: &PrincipalRun,
) -> Result<CapacityCalibrationTrial, CalibrationError> {
    let mut observation_count = 0_u32;
    let mut covered_counts = [0_u32; COVERAGE_LEVELS.len()];
    let mut rank_counts = [0_u32; RANK_BIN_COUNT];
    let mut absolute_error_sum = 0.0_f64;
    let mut uncertainty_sum = 0.0_f64;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            return Err(CalibrationError::MissingControllerSample);
        };
        let observed = match sample.capacity_evidence {
            CapacityEvidenceSample::None => continue,
            CapacityEvidenceSample::Window(window) => window.throughput_per_second(),
        };
        if !sample.capacity_predictive_rank.is_finite() {
            continue;
        }
        observation_count = observation_count.saturating_add(1);
        for (covered, level) in covered_counts.iter_mut().zip(COVERAGE_LEVELS) {
            let tail = (1.0_f64 - level) / 2.0_f64;
            *covered = covered.saturating_add(u32::from(
                sample.capacity_predictive_rank >= tail
                    && sample.capacity_predictive_rank <= 1.0_f64 - tail,
            ));
        }
        let bin = rank_bin(sample.capacity_predictive_rank);
        rank_counts[bin] = rank_counts[bin].saturating_add(1);
        absolute_error_sum += (observed - sample.capacity_predictive_median_per_second).abs();
        uncertainty_sum +=
            sample.capacity_predictive_high_per_second - sample.capacity_predictive_low_per_second;
    }
    if observation_count == 0 {
        return Err(CalibrationError::NoObservations);
    }
    let prior_width = posterior_width(
        run.controller().capacity_posterior_values(),
        run.controller().capacity_prior(),
    );
    let final_index = run.controller().len().saturating_sub(1);
    let final_mass = run
        .controller()
        .capacity_posterior(final_index)
        .ok_or(CalibrationError::MissingControllerSample)?;
    let final_width = posterior_width(run.controller().capacity_posterior_values(), final_mass);
    let count = f64::from(observation_count);
    Ok(CapacityCalibrationTrial {
        regime,
        seed,
        observation_count,
        covered_counts,
        rank_counts,
        mean_absolute_error_per_second: absolute_error_sum / count,
        mean_uncertainty_per_second: uncertainty_sum / count,
        capacity_contraction: if prior_width > f64::EPSILON {
            1.0_f64 - final_width / prior_width
        } else {
            0.0_f64
        },
    })
}

fn rank_bin(rank: f64) -> usize {
    const UPPER_BOUNDS: [f64; RANK_BIN_COUNT - 1] = [
        0.1_f64, 0.2_f64, 0.3_f64, 0.4_f64, 0.5_f64, 0.6_f64, 0.7_f64, 0.8_f64, 0.9_f64,
    ];
    UPPER_BOUNDS
        .into_iter()
        .position(|upper| rank < upper)
        .map_or(RANK_BIN_COUNT - 1, |bin| bin)
}

fn posterior_width(values: &[f64], probabilities: &[f64]) -> f64 {
    posterior_quantile(values, probabilities, 0.9_f64)
        - posterior_quantile(values, probabilities, 0.1_f64)
}

fn posterior_quantile(values: &[f64], probabilities: &[f64], threshold: f64) -> f64 {
    let fallback = values.last().copied().map_or(f64::NAN, |value| value);
    let mut cumulative = 0.0_f64;
    for (&value, &probability) in values.iter().zip(probabilities) {
        cumulative += probability;
        if cumulative >= threshold {
            return value;
        }
    }
    fallback
}

/// Failure from a repeated calibration experiment.
#[derive(Debug, Error)]
pub enum CalibrationError {
    /// The bounded calibration worker pool could not start.
    #[error(transparent)]
    ThreadPool(#[from] ThreadPoolBuildError),
    /// An arrival posterior has invalid parameters.
    #[error(transparent)]
    ArrivalPosterior(#[from] GammaError),
    /// A seeded regime failed.
    #[error(transparent)]
    Principal(#[from] PrincipalRunError),
    /// A seeded regime contradicted its declaration.
    #[error(transparent)]
    Validation(#[from] RegimeValidationError),
    /// A fixed calibration count exceeds this platform.
    #[error("a calibration count exceeds this platform")]
    PlatformLimit,
    /// A controller sample was unavailable.
    #[error("a calibration controller sample is missing")]
    MissingControllerSample,
    /// A calibration run accepted no predictive evidence.
    #[error("a calibration run accepted no predictive evidence")]
    NoObservations,
}

#[cfg(test)]
#[path = "calibration_tests.rs"]
mod tests;
