use std::{
    num::{ParseFloatError, ParseIntError},
    str::FromStr,
    time::Duration,
};

use allocation_counter::measure;
use fearless_simd::{Level, Simd, dispatch, prelude::*};
use quickcheck_macros::quickcheck;
use statrs::distribution::{ContinuousCDF, Gamma};
use statrs::function::gamma::ln_gamma;
use thiserror::Error;

use super::{
    CAPACITY_UPDATE_OPERATION_COUNT_MAX, CapacityAllocation, CapacityGrid, CapacityGridError,
    CapacityModelError, CompletionScratch, DescendingLogGamma, HAZARD_COVERAGE_INDEX,
    HAZARD_TRANSITION_PROBABILITY_ERROR_MAX, OBSERVATION_COVERAGE_INDEX,
    OBSERVATION_PROBABILITY_ERROR_MAX, ResourceWindow, ResourceWindowError, RetainedHistory,
    StartWindow, binomial_log_probability, binomial_log_probability_reference,
    capacity_model_artifact, capacity_update_operation_count, completion_expectation,
    completion_group_convolution, completion_log_likelihood, completion_log_likelihood_reference,
    contamination_prior, exponentiate_log_masses, fill_knee_state_rates, fill_no_knee_state_rates,
    fold_trace, hazard_prior, integer_ln_gamma_table, log_contamination_mixture,
    log_normal_axis_masses, log_weighted_sum, path_log_score, record_start_window, vector_exp,
};
use crate::change_point::ChangePointKernel;
use crate::types::occupancy_trace_for_test;
use crate::{ArrivalPrior, OccupancyTraceEvidence};

fn kernel_float_matches(actual: f64, expected: f64) -> bool {
    if actual.is_infinite() || expected.is_infinite() {
        return actual.is_infinite()
            && expected.is_infinite()
            && actual.is_sign_positive() == expected.is_sign_positive();
    }
    (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
}

fn update_constant_trace(
    factor: &mut super::CapacityFactor,
    concurrency: u32,
    exposure_seconds: f64,
    completed_attempts: u32,
) -> Result<(), ResourceWindowError> {
    let window = ResourceWindow::new_with_starts(
        f64::from(concurrency),
        exposure_seconds,
        completed_attempts,
        completed_attempts,
    )?;
    let exposure_micros = window.exposure_micros();
    let offsets = (0..completed_attempts)
        .map(|index| u64::from(index + 1) * exposure_micros / u64::from(completed_attempts + 1))
        .collect::<Vec<_>>();
    let completed = vec![1_u32; completed_attempts as usize];
    let started = vec![1_u32; completed_attempts as usize];
    factor.update(
        occupancy_trace_for_test(
            window,
            concurrency,
            concurrency,
            u128::from(concurrency) * u128::from(exposure_micros),
            &offsets,
            &completed,
            &started,
        ),
        Duration::from_micros(exposure_micros),
    );
    Ok(())
}

fn posterior_bits(factor: &super::CapacityFactor) -> Vec<u64> {
    factor
        .weights
        .iter()
        .chain(&factor.filter_weights)
        .chain(&factor.filter_log_weights)
        .chain(&factor.filter_curve_weights)
        .map(|value| value.to_bits())
        .collect()
}

fn storm_factor() -> Result<super::CapacityFactor, TestError> {
    let grid = CapacityGrid::new_with_prior(
        &[0.0005_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64],
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
        super::CapacityPrior::LogUniform,
    )?;
    Ok(super::CapacityFactor::new_with_prior_with_groups(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::new(1.0_f64, 86_400.0_f64, 1.0_f64 / 86_400.0_f64)?,
        256.0_f64,
        0.1_f64,
        10_000,
        1_000,
    )?)
}

fn update_storm_window(
    factor: &mut super::CapacityFactor,
    burst: bool,
) -> Result<(), ResourceWindowError> {
    let (window, initial, busy_micros, offsets, completed, started) = if burst {
        (
            ResourceWindow::new_with_starts(229.8_f64, 0.1_f64, 8, 8)?,
            230,
            22_980_000,
            [80_000, 100_000],
            [1, 7],
            [0, 8],
        )
    } else {
        (
            ResourceWindow::new_with_starts(50.5_f64, 0.1_f64, 50, 50)?,
            50,
            5_050_000,
            [50_000, 100_000],
            [24, 26],
            [25, 25],
        )
    };
    factor.update(
        occupancy_trace_for_test(
            window,
            initial,
            initial,
            busy_micros,
            &offsets,
            &completed,
            &started,
        ),
        Duration::from_millis(100),
    );
    Ok(())
}

fn filter_posterior_is_finite_and_normalized(factor: &super::CapacityFactor) -> bool {
    let sum = factor.filter_weights.iter().sum::<f64>();
    factor
        .filter_log_weights
        .iter()
        .all(|weight| weight.is_finite())
        && factor
            .filter_weights
            .iter()
            .all(|weight| weight.is_finite())
        && factor.weights.iter().all(|weight| weight.is_finite())
        && factor.no_knee_probability().is_finite()
        && (sum - 1.0_f64).abs() <= 64.0_f64 * f64::EPSILON
}

#[test]
fn storm_filter_posterior_stays_finite_past_window_266() -> Result<(), TestError> {
    let mut factor = storm_factor()?;
    for index in 0_usize..=266_usize {
        update_storm_window(&mut factor, index == 12_usize)?;
        assert!(filter_posterior_is_finite_and_normalized(&factor));
    }
    Ok(())
}

#[quickcheck]
fn filter_posterior_stays_finite_and_normalized_for_random_windows(codes: Vec<u8>) -> bool {
    let Ok(mut factor) = storm_factor() else {
        return false;
    };
    if update_storm_window(&mut factor, true).is_err() {
        return false;
    }
    for code in codes.into_iter().take(32) {
        let concurrency = 1_u32 + u32::from(code);
        let exposure_seconds = if code % 4 == 0 { 0.1_f64 } else { 1.0_f64 };
        let completed = if code % 7 == 0 {
            0
        } else {
            u32::from(code % 64)
        };
        if update_constant_trace(&mut factor, concurrency, exposure_seconds, completed).is_err()
            || !filter_posterior_is_finite_and_normalized(&factor)
        {
            return false;
        }
    }
    true
}

#[quickcheck]
fn rejecting_update_leaves_the_posterior_byte_identical(operation_codes: Vec<u8>) -> bool {
    let Ok(grid) = CapacityGrid::new(&[0.25_f64, 1.0_f64], &[100.0_f64], &[0.0_f64]) else {
        return false;
    };
    let Ok(prior) = ArrivalPrior::test_artifact() else {
        return false;
    };
    let Ok(mut factor) = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &prior,
        1.0_f64,
        1.0_f64,
        8,
    ) else {
        return false;
    };
    for code in operation_codes.into_iter().take(16) {
        if update_constant_trace(&mut factor, 1, 1.0_f64, u32::from(code % 2)).is_err() {
            return false;
        }
    }
    let before = posterior_bits(&factor);
    factor.likelihoods[0] = f64::NAN;
    factor.update_filters(0.0_f64);
    before == posterior_bits(&factor)
}

#[test]
fn missed_tick_update_matches_an_explicit_interval_start_transition() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.25_f64, 1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let prior = ArrivalPrior::test_artifact()?;
    let mut owned = super::CapacityFactor::new_with_prior(
        grid.clone(),
        1.0_f64 / 300.0_f64,
        &prior,
        1.0_f64,
        1.0_f64,
        8,
    )?;
    let mut explicit = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &prior,
        1.0_f64,
        1.0_f64,
        8,
    )?;
    for _ in 0_u8..8 {
        update_constant_trace(&mut owned, 1, 1.0_f64, 1)?;
        update_constant_trace(&mut explicit, 1, 1.0_f64, 1)?;
    }
    let window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 1, 1)?;
    let offsets = [500_000_u64];
    let completed = [1_u32];
    let started = [1_u32];
    let evidence =
        occupancy_trace_for_test(window, 1, 1, 1_000_000, &offsets, &completed, &started);
    owned.update(evidence, Duration::from_secs(3));
    explicit.transition(Duration::from_secs(2));
    explicit.update(evidence, Duration::ZERO);
    assert_eq!(posterior_bits(&owned), posterior_bits(&explicit));
    assert_eq!(owned.observation_clock_micros, 11_000_000);
    let latest =
        (owned.start_history_head + owned.start_history.len() - 1) % owned.start_history.len();
    assert_eq!(owned.start_history[latest].end_micros, 11_000_000);
    Ok(())
}

#[test]
fn first_completion_residual_after_a_gap_is_discarded() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        2.0_f64,
        1.0_f64,
        2,
    )?;
    factor.omit_observation(Duration::from_secs(2));
    let window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 1, 0)?;
    let offsets = [500_000_u64];
    let completed = [1_u32];
    let started = [0_u32];
    factor.update(
        occupancy_trace_for_test(window, 1, 0, 500_000, &offsets, &completed, &started),
        Duration::from_secs(1),
    );
    assert_eq!(factor.residual_sample_count, 0);
    assert_eq!(factor.start_history_len, 2);
    assert_eq!(factor.start_history[0].started_attempts, None);
    assert_eq!(factor.start_history[0].end_micros, 2_000_000);
    assert_eq!(factor.start_history[1].started_attempts, Some(0));
    assert_eq!(factor.start_history[1].end_micros, 3_000_000);
    Ok(())
}

#[quickcheck]
fn known_start_binomial_likelihood_is_normalized(trial_code: u8, probability_code: u8) -> bool {
    let trials = u32::from(trial_code % 32);
    let probability = (f64::from(probability_code) + 0.5_f64) / 256.0_f64;
    let Ok(ln_gamma_integers) = integer_ln_gamma_table(trials as usize + 1) else {
        return false;
    };
    let total = (0..=trials as usize)
        .map(|count| binomial_log_probability(trials, count, probability, &ln_gamma_integers).exp())
        .sum::<f64>();
    let operation_count = f64::from(trials + 1).powi(2);
    (total - 1.0_f64).abs() <= operation_count * 1_024.0_f64 * f64::EPSILON
}

#[quickcheck]
fn integer_binomial_table_matches_scalar_bits(trial_code: u16, probability_code: u16) -> bool {
    let trials = u32::from(trial_code % 4_096);
    let probability = (f64::from(probability_code) + 0.5_f64) / 65_536.0_f64;
    let Ok(ln_gamma_integers) = integer_ln_gamma_table(trials as usize + 1) else {
        return false;
    };
    (0..=trials as usize).all(|count| {
        binomial_log_probability(trials, count, probability, &ln_gamma_integers).to_bits()
            == binomial_log_probability_reference(trials, count, probability).to_bits()
    })
}

#[quickcheck]
fn descending_negative_binomial_gamma_matches_scalar(
    shape_code: u32,
    count_code: u32,
    span_code: u16,
) -> bool {
    let shape = 0.25_f64 + f64::from(shape_code % 400_000_000) / 4.0_f64;
    let low = count_code as usize % 100_001;
    let high = low
        .saturating_add(usize::from(span_code % 1_025))
        .min(100_000);
    let high_float = f64::from(u32::try_from(high).unwrap_or(u32::MAX));
    let mut count_shape_log_gamma = DescendingLogGamma::new(ln_gamma(high_float + shape));
    for count in (low..=high).rev() {
        let count_float = f64::from(u32::try_from(count).unwrap_or(u32::MAX));
        let expected = ln_gamma(count_float + shape);
        if !epsilon_matches(count_shape_log_gamma.value, expected) {
            return false;
        }
        if count > low {
            count_shape_log_gamma = count_shape_log_gamma.previous(shape, count);
        }
    }
    true
}

fn epsilon_matches(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
}

#[quickcheck]
fn capacity_region_interval_contains_samples(
    concurrency_code: u16,
    cell_code: u8,
    service_sample: u16,
    capacity_sample: u16,
    collapse_sample: u16,
) -> bool {
    let Ok(grid) = CapacityGrid::new(
        &[0.001_f64, 0.002_f64, 0.01_f64],
        &[500.0_f64, 2_000.0_f64],
        &[0.0_f64, 1.0_f64, 2.0_f64],
    ) else {
        return false;
    };
    let index = usize::from(cell_code) % grid.service_times_seconds.len();
    let concurrency = 0.1_f64 + f64::from(concurrency_code) / 10.0_f64;
    let fraction = |sample: u16| f64::from(sample) / f64::from(u16::MAX);
    let log_sample = |low: f64, high: f64, sample: u16| {
        (low.ln() + fraction(sample) * (high.ln() - low.ln())).exp()
    };
    let service = log_sample(
        grid.service_time_lows[index],
        grid.service_time_highs[index],
        service_sample,
    );
    let (low, high) = grid.throughput_interval(index, concurrency);
    let sampled = if grid.no_knee[index] > 0.0_f64 {
        super::throughput(service, 0.0_f64, 0.0_f64, true, concurrency)
    } else {
        let capacity = log_sample(
            grid.capacity_lows[index],
            grid.capacity_highs[index],
            capacity_sample,
        );
        let collapse =
            if grid.collapse_lows[index] == 0.0_f64 && grid.collapse_highs[index] == 0.0_f64 {
                0.0_f64
            } else {
                grid.collapse_lows[index]
                    + fraction(collapse_sample)
                        * (grid.collapse_highs[index] - grid.collapse_lows[index])
            };
        super::throughput(service, capacity, collapse, false, concurrency)
    };
    let tolerance = high.max(1.0_f64) * 1.0e-9_f64;
    sampled + tolerance >= low && sampled <= high + tolerance
}

#[quickcheck]
fn prehistory_mean_does_not_use_the_completion_response(
    first_completed: u16,
    second_completed: u16,
) -> bool {
    let Ok(grid) = CapacityGrid::new(&[0.5_f64], &[10.0_f64], &[0.0_f64]) else {
        return false;
    };
    let history = [StartWindow {
        end_micros: 0,
        exposure_seconds: 1.0_f64,
        started_attempts: None,
    }];
    let Ok(first) =
        ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, u32::from(first_completed), 0)
    else {
        return false;
    };
    let Ok(second) =
        ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, u32::from(second_completed), 0)
    else {
        return false;
    };
    let retained = RetainedHistory {
        windows: &history,
        head: 0,
        length: 0,
        end_micros: 0,
    };
    completion_expectation(&grid, 0, retained, &first, 2.0_f64, 1.0_f64)
        .total_cmp(&completion_expectation(
            &grid, 0, retained, &second, 2.0_f64, 1.0_f64,
        ))
        .is_eq()
}

#[test]
fn coverage_ring_matches_unbounded_history() -> Result<(), TestError> {
    let ln_gamma_integers = integer_ln_gamma_table(257)?;
    let grid = CapacityGrid::new(&[2.0_f64], &[10.0_f64], &[0.0_f64])?;
    let empty = StartWindow {
        end_micros: 0,
        exposure_seconds: 0.0_f64,
        started_attempts: None,
    };
    let mut ring = [empty; 3];
    let mut unbounded = [empty; 181];
    let mut ring_head = 0;
    let mut ring_len = 0;
    let mut unbounded_head = 0;
    let mut unbounded_len = 0;
    let mut ring_coefficients = [0.0_f64; 65];
    let mut ring_convolution = [0.0_f64; 65];
    let mut ring_binomial = [0.0_f64; 65];
    let mut reference_coefficients = [0.0_f64; 65];
    let mut reference_convolution = [0.0_f64; 65];
    let mut reference_binomial = [0.0_f64; 65];
    let mut end_micros = 0_u64;
    for index in 0_u32..180 {
        let exposure = if index % 2 == 0 { 1.0_f64 } else { 1.5_f64 };
        let starts = if index % 7 == 0 { 13 } else { 5 };
        let window = ResourceWindow::new_with_starts(1.0_f64, exposure, 5, starts)?;
        end_micros = end_micros.saturating_add(window.exposure_micros());
        record_start_window(
            &mut ring,
            &mut ring_head,
            &mut ring_len,
            &window,
            end_micros,
            Some(starts),
        );
        record_start_window(
            &mut unbounded,
            &mut unbounded_head,
            &mut unbounded_len,
            &window,
            end_micros,
            Some(starts),
        );
        let actual = completion_log_likelihood(
            &grid,
            0,
            RetainedHistory {
                windows: &ring,
                head: ring_head,
                length: ring_len,
                end_micros,
            },
            &window,
            1.0_f64,
            1.0_f64,
            CompletionScratch {
                simd_level: Level::new(),
                coefficients: &mut ring_coefficients,
                convolution: &mut ring_convolution,
                binomial: &mut ring_binomial,
                ln_gamma_integers: &ln_gamma_integers,
            },
        );
        let expected = completion_log_likelihood(
            &grid,
            0,
            RetainedHistory {
                windows: &unbounded,
                head: unbounded_head,
                length: unbounded_len,
                end_micros,
            },
            &window,
            1.0_f64,
            1.0_f64,
            CompletionScratch {
                simd_level: Level::new(),
                coefficients: &mut reference_coefficients,
                convolution: &mut reference_convolution,
                binomial: &mut reference_binomial,
                ln_gamma_integers: &ln_gamma_integers,
            },
        );
        let error_bound = 256.0_f64 * f64::EPSILON * actual.abs().max(expected.abs()).max(1.0_f64);
        assert!((actual - expected).abs() <= error_bound);
    }
    Ok(())
}

#[test]
fn contamination_cells_preserve_the_authored_beta_mass() -> Result<(), TestError> {
    let artifact = capacity_model_artifact(1.0_f64 / 300.0_f64, 4.0_f64)?;
    let (probabilities, weights) = contamination_prior(&artifact)?;
    assert_eq!(probabilities.len(), weights.len());
    assert!(probabilities.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        probabilities
            .iter()
            .all(|value| (0.0_f64..1.0_f64).contains(value))
    );
    assert!((weights.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    Ok(())
}

#[test]
fn an_identifiable_persistent_run_beats_contamination_redraws() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.25_f64, 1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        32,
    )?;
    let initial_persistent_mass = factor.weights[1] + factor.weights[3];
    let quality_count = factor.contamination_probabilities.len();
    let initial_clean_filter_mass = factor
        .filter_weights
        .iter()
        .step_by(quality_count)
        .sum::<f64>();
    let initial_contaminated_filter_mass = factor
        .filter_weights
        .iter()
        .skip(quality_count - 1)
        .step_by(quality_count)
        .sum::<f64>();
    for _ in 0_u32..180 {
        update_constant_trace(&mut factor, 1, 1.0_f64, 1)?;
    }
    let persistent_mass = factor.weights[1] + factor.weights[3];
    let clean_filter_mass = factor
        .filter_weights
        .iter()
        .step_by(quality_count)
        .sum::<f64>();
    let contaminated_filter_mass = factor
        .filter_weights
        .iter()
        .skip(quality_count - 1)
        .step_by(quality_count)
        .sum::<f64>();
    assert!(persistent_mass > initial_persistent_mass);
    assert!(
        clean_filter_mass * initial_contaminated_filter_mass
            > contaminated_filter_mass * initial_clean_filter_mass
    );
    Ok(())
}

#[test]
fn hazard_cells_cover_the_declared_gamma_tails() -> Result<(), TestError> {
    let mean_per_second = 1.0_f64 / 300.0_f64;
    let artifact = capacity_model_artifact(mean_per_second, 4.0_f64)?;
    assert_eq!(artifact.identity.version(), 2);
    assert!(
        artifact.coverage[HAZARD_COVERAGE_INDEX].tail_probability()
            <= artifact.budget.boundary_probability_max()
    );
    assert_eq!(
        artifact.coverage[OBSERVATION_COVERAGE_INDEX]
            .decision_cost_error()
            .to_bits(),
        OBSERVATION_PROBABILITY_ERROR_MAX.to_bits()
    );
    assert_eq!(
        artifact.coverage[HAZARD_COVERAGE_INDEX]
            .decision_cost_error()
            .to_bits(),
        HAZARD_TRANSITION_PROBABILITY_ERROR_MAX.to_bits()
    );
    let (rates, weights) = hazard_prior(&artifact)?;
    assert!(rates.windows(2).all(|pair| pair[0] < pair[1]));
    assert!((weights.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    let discrete_mean = rates
        .iter()
        .zip(&weights)
        .map(|(rate, weight)| rate * weight)
        .sum::<f64>();
    // The hazard grid controls transition probability to 1/8. Requiring the
    // same relative bound on its mean is no weaker than the declared loss.
    assert!(
        (discrete_mean - mean_per_second).abs()
            <= HAZARD_TRANSITION_PROBABILITY_ERROR_MAX * mean_per_second
    );
    assert!(matches!(
        capacity_model_artifact(0.0_f64, 4.0_f64),
        Err(CapacityModelError::InvalidHazardPrior)
    ));
    Ok(())
}

#[test]
fn one_second_transition_retains_an_informative_capacity_update() -> Result<(), TestError> {
    let grid = CapacityGrid::new(
        &[0.0505_f64, 0.101_f64, 0.202_f64],
        &[80.0_f64, 320.0_f64, 640.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?;
    let arrival_prior = ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64)?;
    let mut factor = super::CapacityFactor::new_with_prior_with_groups(
        grid,
        1.0_f64 / 300.0_f64,
        &arrival_prior,
        192.0_f64,
        1.0_f64,
        2_112,
        256,
    )?;
    let completed_attempts = 495_u32;
    let window =
        ResourceWindow::new_with_starts(50.0_f64, 1.0_f64, completed_attempts, completed_attempts)?;
    let offsets = (0..256_u64)
        .map(|index| (index + 1) * 1_000_000_u64 / 256_u64)
        .collect::<Vec<_>>();
    let completed = (0..256_u32)
        .map(|index| 1_u32 + u32::from(index < 239))
        .collect::<Vec<_>>();
    let started = completed.clone();
    let evidence =
        occupancy_trace_for_test(window, 50, 50, 50_000_000, &offsets, &completed, &started);
    fold_trace(
        evidence,
        &mut factor.state_exposure_seconds,
        &mut factor.state_completion_counts,
    );
    for index in 0..factor.grid.knee_cell_count as usize {
        fill_knee_state_rates(&factor.grid, index, &mut factor.state_rates);
        factor.update_cell_likelihood(index, evidence);
    }
    for index in factor.grid.knee_cell_count as usize..factor.likelihoods.len() {
        fill_no_knee_state_rates(&factor.grid, index, &mut factor.state_rates);
        factor.update_cell_likelihood(index, evidence);
    }
    let prior_predictive = log_weighted_sum(&factor.prior_weights, &factor.likelihoods);
    factor.update_filters(prior_predictive);
    let learned = factor.no_knee_probability();
    let expected_redraw = factor
        .hazard_rates_per_second
        .iter()
        .zip(
            factor
                .filter_weights
                .chunks_exact(factor.contamination_probabilities.len()),
        )
        .map(|(hazard, weights)| weights.iter().sum::<f64>() * (1.0_f64 - (-hazard).exp()))
        .sum::<f64>();
    factor.transition(Duration::from_secs(1));
    let retained = factor.no_knee_probability();
    // Since 1 - exp(-h) is at most h, a 1/300 mean hazard redraws about
    // 0.33 percent per second. A probability can move by at most that mass.
    assert!((learned - 0.5_f64).abs() > 0.25_f64);
    assert!((retained - learned).abs() <= expected_redraw);
    Ok(())
}

/// A single saturated window aliases per rate (the 0.404-second
/// no-knee cell predicts 316.8/s at occupancy 128 against the flat
/// plateau's 320/s). The joint trajectory still discriminates: the
/// below-knee phase already rejects every single no-knee cell that
/// could fit the plateau, so the true knee cell wins at both fleet
/// bounds.
#[test]
fn saturated_fleet_occupancy_discriminates_the_flat_plateau() -> Result<(), TestError> {
    let grid = CapacityGrid::new(
        &[0.101_f64, 0.202_f64, 0.404_f64],
        &[80.0_f64, 320.0_f64, 600.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?;
    let arrival_prior = ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64)?;
    let mut factor = super::CapacityFactor::new_with_prior_with_groups(
        grid,
        1.0_f64 / 300.0_f64,
        &arrival_prior,
        128.0_f64,
        1.0_f64,
        2_112,
        256,
    )?;
    apply_flat_capacity_windows(&mut factor, 20, 3)?;
    let below_knee_mass = factor.no_knee_probability();
    assert!(below_knee_mass > 0.1_f64, "{below_knee_mass}");

    apply_flat_capacity_windows(&mut factor, 128, 3)?;
    let bound_mass = factor.no_knee_probability();
    assert!(bound_mass <= 0.01_f64, "{bound_mass}");

    apply_flat_capacity_windows(&mut factor, 96, 3)?;
    let interior_mass = factor.no_knee_probability();
    assert!(interior_mass <= 0.01_f64, "{interior_mass}");
    Ok(())
}

/// Stores the captured 40-window flat-plateau capacity evidence.
const FLAT_CAPTURED_EVIDENCE: &str =
    "update: exposure 1.000s occ 18.01 completed 79 started 100 elapsed 1.000s no_knee_before \
     0.50000
  evidence: initial 0 final 21 busy_micros 18007820 offsets [11718, 23437, 31250, 42968, 50781, \
     62500, 70312, 82031, 93750, 101562, 113281, 121093, 132812, 140625, 152343, 160156, 171875, \
     183593, 191406, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1]
  gate: prior_predictive 274.857 eligible true finite_cells 39/39 max_loglik 275.729
update: exposure 1.000s occ 20.16 completed 100 started 100 elapsed 1.000s no_knee_before 0.58675
  evidence: initial 21 final 21 busy_micros 20160147 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 105468, 113281, 125000, \
     132812, 144531, 152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, 222656, \
     230468, 234375, 242187, 253906, 261718, 265625, 273437, 281250, 292968, 300781, 312500, \
     320312, 332031, 343750, 351562, 363281, 371093, 382812, 390625, 402343, 410156, 414062, \
     421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, \
     492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, \
     570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, \
     644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, \
     714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, \
     792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, \
     871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, \
     941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed \
     [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1]
  gate: prior_predictive 361.620 eligible true finite_cells 39/39 max_loglik 362.500
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60223
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.14 completed 100 started 100 elapsed 1.000s no_knee_before 0.60336
  evidence: initial 21 final 21 busy_micros 20140610 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, 152343, 164062, \
     175781, 183593, 195312, 203125, 210937, 222656, 230468, 242187, 253906, 261718, 273437, \
     281250, 292968, 300781, 312500, 320312, 332031, 343750, 351562, 363281, 371093, 382812, \
     390625, 402343, 410156, 414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, \
     464843, 472656, 480468, 484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, \
     542968, 550781, 554687, 562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, \
     621093, 625000, 632812, 640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, \
     691406, 695312, 703125, 710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, \
     765625, 773437, 781250, 785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, \
     843750, 851562, 855468, 863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, \
     914062, 921875, 925781, 933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, \
     984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 356.181 eligible true finite_cells 39/39 max_loglik 357.057
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60348
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.16 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20160147 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 140625, 144531, 152343, \
     160156, 164062, 171875, 175781, 183593, 191406, 195312, 203125, 210937, 222656, 230468, \
     242187, 253906, 261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, \
     351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, \
     421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, \
     492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, \
     570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, \
     644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, \
     714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, \
     792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, \
     871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, \
     941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed \
     [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 13, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 13, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1]
  gate: prior_predictive 358.731 eligible true finite_cells 39/39 max_loglik 359.598
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60349
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.18 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20183590 offsets [3906, 15625, 23437, 35156, 42968, \
     50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, 121093, 125000, \
     132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, 191406, 195312, \
     203125, 210937, 222656, 230468, 242187, 253906, 261718, 265625, 273437, 281250, 285156, \
     292968, 300781, 304687, 312500, 320312, 324218, 332031, 343750, 351562, 355468, 363281, \
     371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 5, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 5, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1]
  gate: prior_predictive 360.718 eligible true finite_cells 39/39 max_loglik 361.595
update: exposure 1.000s occ 20.17 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20167962 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     125000, 132812, 144531, 152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, \
     222656, 230468, 234375, 242187, 253906, 261718, 265625, 273437, 281250, 285156, 292968, \
     300781, 304687, 312500, 320312, 332031, 343750, 351562, 363281, 371093, 382812, 390625, \
     402343, 410156, 414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, \
     472656, 480468, 484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, \
     550781, 554687, 562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, \
     625000, 632812, 640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, \
     695312, 703125, 710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, \
     773437, 781250, 785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, \
     851562, 855468, 863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, \
     921875, 925781, 933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, \
     992187, 1000000] completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.652 eligible true finite_cells 39/39 max_loglik 362.532
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.15 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20148425 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, \
     152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, 222656, 230468, 234375, \
     242187, 253906, 261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, \
     351562, 363281, 371093, 382812, 390625, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 0, 1, 1, 0, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 17, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0] started [0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 1, \
     0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 360.165 eligible true finite_cells 39/39 max_loglik 361.045
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.15 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20148425 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, 152343, 164062, \
     171875, 175781, 183593, 191406, 195312, 203125, 210937, 222656, 230468, 242187, 253906, \
     261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, 351562, 363281, \
     371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 1, 1, 0, 1, 17, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 17, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 357.420 eligible true finite_cells 39/39 max_loglik 358.286
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.17 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20167962 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 101562, 105468, 113281, 121093, 125000, 132812, 140625, \
     144531, 152343, 160156, 164062, 171875, 175781, 183593, 191406, 195312, 203125, 210937, \
     222656, 230468, 242187, 253906, 261718, 273437, 281250, 292968, 300781, 304687, 312500, \
     320312, 324218, 332031, 343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, \
     394531, 402343, 410156, 414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, \
     464843, 472656, 480468, 484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, \
     542968, 550781, 554687, 562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, \
     621093, 625000, 632812, 640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, \
     691406, 695312, 703125, 710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, \
     765625, 773437, 781250, 785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, \
     843750, 851562, 855468, 863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, \
     914062, 921875, 925781, 933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, \
     984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 9, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 9, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 359.907 eligible true finite_cells 39/39 max_loglik 360.782
update: exposure 1.000s occ 20.18 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20179684 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 164062, 175781, 183593, 195312, 203125, \
     210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, 265625, 273437, 281250, \
     285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, 343750, 351562, 355468, \
     363281, 371093, 382812, 390625, 402343, 410156, 414062, 421875, 425781, 433593, 441406, \
     445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, 515625, \
     523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, 593750, \
     601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, 664062, \
     671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, 734375, \
     742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, 812500, \
     820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, 890625, \
     894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, 960937, \
     964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 5, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 0, 0, 0, 5, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1]
  gate: prior_predictive 361.260 eligible true finite_cells 39/39 max_loglik 362.139
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.16 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20160147 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 105468, 113281, 125000, \
     132812, 144531, 152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, 222656, \
     230468, 234375, 242187, 253906, 261718, 265625, 273437, 281250, 292968, 300781, 312500, \
     320312, 332031, 343750, 351562, 363281, 371093, 382812, 390625, 402343, 410156, 414062, \
     421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, \
     492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, \
     570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, \
     644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, \
     714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, \
     792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, \
     871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, \
     941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed \
     [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 13, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1]
  gate: prior_predictive 361.620 eligible true finite_cells 39/39 max_loglik 362.500
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.14 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20140610 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, 152343, 164062, \
     175781, 183593, 195312, 203125, 210937, 222656, 230468, 242187, 253906, 261718, 273437, \
     281250, 292968, 300781, 312500, 320312, 332031, 343750, 351562, 363281, 371093, 382812, \
     390625, 402343, 410156, 414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, \
     464843, 472656, 480468, 484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, \
     542968, 550781, 554687, 562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, \
     621093, 625000, 632812, 640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, \
     691406, 695312, 703125, 710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, \
     765625, 773437, 781250, 785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, \
     843750, 851562, 855468, 863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, \
     914062, 921875, 925781, 933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, \
     984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 356.181 eligible true finite_cells 39/39 max_loglik 357.057
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.16 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20160147 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 140625, 144531, 152343, \
     160156, 164062, 171875, 175781, 183593, 191406, 195312, 203125, 210937, 222656, 230468, \
     242187, 253906, 261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, \
     351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, \
     421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, \
     492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, \
     570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, \
     644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, \
     714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, \
     792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, \
     871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, \
     941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed \
     [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 13, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 13, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1]
  gate: prior_predictive 358.731 eligible true finite_cells 39/39 max_loglik 359.598
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.18 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20183590 offsets [3906, 15625, 23437, 35156, 42968, \
     50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, 121093, 125000, \
     132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, 191406, 195312, \
     203125, 210937, 222656, 230468, 242187, 253906, 261718, 265625, 273437, 281250, 285156, \
     292968, 300781, 304687, 312500, 320312, 324218, 332031, 343750, 351562, 355468, 363281, \
     371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 5, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 5, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1]
  gate: prior_predictive 360.718 eligible true finite_cells 39/39 max_loglik 361.595
update: exposure 1.000s occ 20.17 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20167962 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     125000, 132812, 144531, 152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, \
     222656, 230468, 234375, 242187, 253906, 261718, 265625, 273437, 281250, 285156, 292968, \
     300781, 304687, 312500, 320312, 332031, 343750, 351562, 363281, 371093, 382812, 390625, \
     402343, 410156, 414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, \
     472656, 480468, 484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, \
     550781, 554687, 562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, \
     625000, 632812, 640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, \
     695312, 703125, 710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, \
     773437, 781250, 785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, \
     851562, 855468, 863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, \
     921875, 925781, 933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, \
     992187, 1000000] completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] started [0, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 9, 1, 0, 1, 1, 0, 1, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.652 eligible true finite_cells 39/39 max_loglik 362.532
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.15 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20148425 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, \
     152343, 164062, 175781, 183593, 195312, 203125, 210937, 214843, 222656, 230468, 234375, \
     242187, 253906, 261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, \
     351562, 363281, 371093, 382812, 390625, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 0, 1, 1, 0, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 0, 0, 0, 0, 17, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0] started [0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 1, \
     0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 360.165 eligible true finite_cells 39/39 max_loglik 361.045
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 20.15 completed 100 started 100 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 21 busy_micros 20148425 offsets [3906, 15625, 23437, 35156, 42968, \
     54687, 62500, 74218, 82031, 93750, 105468, 113281, 125000, 132812, 144531, 152343, 164062, \
     171875, 175781, 183593, 191406, 195312, 203125, 210937, 222656, 230468, 242187, 253906, \
     261718, 273437, 281250, 292968, 300781, 312500, 320312, 332031, 343750, 351562, 363281, \
     371093, 375000, 382812, 390625, 394531, 402343, 410156, 414062, 421875, 425781, 433593, \
     441406, 445312, 453125, 460937, 464843, 472656, 480468, 484375, 492187, 503906, 511718, \
     515625, 523437, 531250, 535156, 542968, 550781, 554687, 562500, 570312, 574218, 582031, \
     593750, 601562, 605468, 613281, 621093, 625000, 632812, 640625, 644531, 652343, 660156, \
     664062, 671875, 675781, 683593, 691406, 695312, 703125, 710937, 714843, 722656, 730468, \
     734375, 742187, 753906, 761718, 765625, 773437, 781250, 785156, 792968, 800781, 804687, \
     812500, 820312, 824218, 832031, 843750, 851562, 855468, 863281, 871093, 875000, 882812, \
     890625, 894531, 902343, 910156, 914062, 921875, 925781, 933593, 941406, 945312, 953125, \
     960937, 964843, 972656, 980468, 984375, 992187, 1000000] completed [1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
     0, 1, 1, 0, 1, 17, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0] started [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 17, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, \
     0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 357.420 eligible true finite_cells 39/39 max_loglik 358.286
update: exposure 1.000s occ 20.19 completed 100 started 100 elapsed 1.000s no_knee_before 0.60350
  evidence: initial 21 final 21 busy_micros 20191406 offsets [3906, 11718, 15625, 23437, 31250, \
     35156, 42968, 50781, 54687, 62500, 70312, 74218, 82031, 93750, 101562, 105468, 113281, \
     121093, 125000, 132812, 140625, 144531, 152343, 160156, 164062, 171875, 175781, 183593, \
     191406, 195312, 203125, 210937, 214843, 222656, 230468, 234375, 242187, 253906, 261718, \
     265625, 273437, 281250, 285156, 292968, 300781, 304687, 312500, 320312, 324218, 332031, \
     343750, 351562, 355468, 363281, 371093, 375000, 382812, 390625, 394531, 402343, 410156, \
     414062, 421875, 425781, 433593, 441406, 445312, 453125, 460937, 464843, 472656, 480468, \
     484375, 492187, 503906, 511718, 515625, 523437, 531250, 535156, 542968, 550781, 554687, \
     562500, 570312, 574218, 582031, 593750, 601562, 605468, 613281, 621093, 625000, 632812, \
     640625, 644531, 652343, 660156, 664062, 671875, 675781, 683593, 691406, 695312, 703125, \
     710937, 714843, 722656, 730468, 734375, 742187, 753906, 761718, 765625, 773437, 781250, \
     785156, 792968, 800781, 804687, 812500, 820312, 824218, 832031, 843750, 851562, 855468, \
     863281, 871093, 875000, 882812, 890625, 894531, 902343, 910156, 914062, 921875, 925781, \
     933593, 941406, 945312, 953125, 960937, 964843, 972656, 980468, 984375, 992187, 1000000] \
     completed [1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0] \
     started [0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, \
     1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, \
     1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, \
     1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, \
     1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1]
  gate: prior_predictive 361.077 eligible true finite_cells 39/39 max_loglik 361.955
update: exposure 1.000s occ 84.46 completed 275 started 350 elapsed 1.000s no_knee_before 0.60351
  evidence: initial 21 final 96 busy_micros 84457049 offsets [3906, 15625, 19531, 23437, 27343, \
     31250, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 70312, 74218, 78125, \
     82031, 85937, 89843, 93750, 97656, 101562, 105468, 113281, 125000, 132812, 144531, 148437, \
     152343, 156250, 160156, 164062, 175781, 183593, 191406, 195312, 199218, 203125, 207031, \
     210937, 222656, 226562, 230468, 234375, 238281, 242187, 246093, 250000, 253906, 257812, \
     261718, 265625, 269531, 273437, 277343, 281250, 285156, 289062, 292968, 296875, 300781, \
     304687, 320312, 324218, 328125, 332031, 335937, 339843, 351562, 355468, 359375, 363281, \
     367187, 375000, 394531, 398437, 402343, 406250, 410156, 414062, 417968, 421875, 425781, \
     429687, 433593, 437500, 441406, 445312, 449218, 453125, 457031, 460937, 464843, 468750, \
     472656, 476562, 480468, 484375, 488281, 496093, 500000, 503906, 511718, 515625, 519531, \
     523437, 527343, 531250, 535156, 539062, 542968, 546875, 550781, 554687, 558593, 562500, \
     566406, 570312, 574218, 578125, 589843, 597656, 601562, 605468, 609375, 617187, 621093, \
     625000, 632812, 636718, 640625, 648437, 652343, 656250, 660156, 664062, 667968, 679687, \
     683593, 691406, 695312, 699218, 703125, 707031, 710937, 714843, 718750, 722656, 726562, \
     730468, 734375, 738281, 742187, 746093, 750000, 753906, 757812, 761718, 765625, 769531, \
     773437, 777343, 781250, 785156, 792968, 796875, 800781, 804687, 808593, 812500, 816406, \
     820312, 824218, 828125, 832031, 835937, 839843, 843750, 851562, 855468, 859375, 867187, \
     871093, 875000, 878906, 894531, 898437, 902343, 906250, 914062, 917968, 921875, 929687, \
     933593, 937500, 945312, 949218, 953125, 957031, 960937, 964843, 968750, 980468, 988281, \
     992187, 996093, 1000000] completed [1, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, \
     0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 0, 2, 2, 2, 2, 2, 2, 2, 1, \
     2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 1, 1, 2, 2, 7, 1, 1, \
     1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 2, 1, 4, 4, \
     3, 2, 1, 1, 2, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 4, \
     2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, \
     2, 2, 5, 4, 1, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, \
     2, 3, 3, 1] started [0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, \
     1, 0, 0, 0, 0, 2, 2, 2, 2, 1, 1, 0, 1, 2, 2, 31, 2, 2, 2, 2, 2, 4, 3, 2, 0, 0, 0, 0, 0, 1, \
     2, 2, 4, 4, 3, 2, 2, 0, 0, 0, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 1, 1, 2, 2, 7, 1, 1, 1, 2, 1, 1, \
     1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 2, 1, 4, 4, 3, 2, 1, 1, \
     2, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 4, 2, 1, 1, 2, \
     1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 2, 2, 5, 4, \
     1, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 2, 3, 3, 1]
  gate: prior_predictive 1295.472 eligible true finite_cells 39/39 max_loglik 1297.047
update: exposure 1.000s occ 96.00 completed 319 started 319 elapsed 1.000s no_knee_before 0.60318
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 15625, 19531, \
     23437, 27343, 31250, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 70312, \
     74218, 78125, 85937, 89843, 93750, 97656, 101562, 109375, 113281, 117187, 121093, 125000, \
     128906, 132812, 136718, 140625, 144531, 148437, 152343, 156250, 160156, 164062, 167968, \
     171875, 175781, 179687, 195312, 199218, 203125, 210937, 214843, 218750, 226562, 230468, \
     234375, 242187, 246093, 250000, 253906, 257812, 261718, 265625, 273437, 281250, 285156, \
     289062, 292968, 300781, 304687, 308593, 312500, 316406, 320312, 324218, 328125, 332031, \
     335937, 339843, 343750, 347656, 351562, 355468, 359375, 363281, 367187, 371093, 375000, \
     378906, 382812, 386718, 390625, 394531, 398437, 406250, 410156, 414062, 417968, 421875, \
     425781, 429687, 433593, 437500, 441406, 445312, 453125, 457031, 460937, 464843, 468750, \
     472656, 476562, 484375, 496093, 500000, 507812, 511718, 515625, 523437, 527343, 531250, \
     539062, 542968, 546875, 550781, 554687, 558593, 562500, 570312, 574218, 582031, 585937, \
     589843, 593750, 597656, 601562, 605468, 609375, 613281, 617187, 621093, 625000, 628906, \
     632812, 636718, 640625, 644531, 648437, 652343, 656250, 660156, 664062, 667968, 671875, \
     675781, 679687, 683593, 687500, 691406, 695312, 703125, 707031, 710937, 714843, 718750, \
     722656, 726562, 730468, 734375, 738281, 746093, 750000, 753906, 757812, 761718, 765625, \
     769531, 773437, 781250, 785156, 796875, 804687, 808593, 812500, 820312, 824218, 828125, \
     835937, 839843, 843750, 847656, 851562, 855468, 859375, 867187, 871093, 875000, 882812, \
     886718, 890625, 894531, 898437, 902343, 906250, 910156, 914062, 917968, 921875, 925781, \
     929687, 933593, 937500, 941406, 945312, 949218, 953125, 957031, 960937, 964843, 968750, \
     972656, 976562, 980468, 984375, 992187, 1000000] completed [1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, \
     2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 3, 2, 1, 2, 6, 3, 1, 1, 1, 2, 2, 1, 2, 2, 2, 2, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 2, 1, 4, 1, 1, 2, 1, 1, 1, 2, 1, \
     1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 3, 2, 3, 4, 1, 4, 1, 1, 1, 2, 2, 2, 3, \
     2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, \
     2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 2, 5, 2, 3, 3, 2, 1, 1, 1, \
     2, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, \
     1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 2, 4] started [1, 1, \
     1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 3, 2, 1, 2, 6, 3, 1, 1, \
     1, 2, 2, 1, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 2, 1, \
     4, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 3, 2, 3, 4, \
     1, 4, 1, 1, 1, 2, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, \
     1, 2, 1, 2, 2, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, \
     2, 5, 2, 3, 3, 2, 1, 1, 1, 2, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, \
     1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, \
     1, 2, 2, 4]
  gate: prior_predictive 1535.508 eligible true finite_cells 39/39 max_loglik 1537.083
update: exposure 1.000s occ 96.00 completed 323 started 323 elapsed 1.000s no_knee_before 0.60328
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 15625, 19531, \
     23437, 27343, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 70312, 78125, \
     82031, 85937, 101562, 105468, 109375, 117187, 121093, 125000, 132812, 136718, 140625, \
     144531, 148437, 152343, 156250, 164062, 167968, 171875, 179687, 183593, 187500, 191406, \
     195312, 199218, 207031, 210937, 214843, 218750, 222656, 226562, 230468, 234375, 238281, \
     242187, 246093, 250000, 253906, 257812, 261718, 265625, 269531, 273437, 277343, 281250, \
     285156, 289062, 292968, 296875, 300781, 304687, 308593, 312500, 316406, 320312, 324218, \
     328125, 332031, 335937, 339843, 343750, 347656, 351562, 355468, 359375, 363281, 367187, \
     375000, 378906, 382812, 390625, 402343, 406250, 414062, 417968, 421875, 429687, 433593, \
     437500, 441406, 445312, 449218, 453125, 460937, 464843, 468750, 476562, 480468, 484375, \
     488281, 492187, 496093, 500000, 503906, 507812, 511718, 515625, 519531, 523437, 527343, \
     531250, 535156, 539062, 542968, 546875, 550781, 554687, 558593, 562500, 566406, 570312, \
     574218, 578125, 582031, 585937, 589843, 593750, 597656, 601562, 605468, 609375, 613281, \
     617187, 621093, 625000, 628906, 632812, 636718, 640625, 644531, 648437, 652343, 656250, \
     660156, 664062, 671875, 675781, 679687, 687500, 691406, 703125, 710937, 714843, 718750, \
     726562, 730468, 734375, 738281, 742187, 746093, 750000, 757812, 761718, 765625, 773437, \
     777343, 781250, 785156, 792968, 796875, 800781, 804687, 808593, 812500, 816406, 820312, \
     824218, 828125, 832031, 835937, 839843, 843750, 847656, 851562, 855468, 859375, 863281, \
     867187, 871093, 875000, 878906, 882812, 886718, 890625, 894531, 898437, 902343, 906250, \
     910156, 914062, 917968, 921875, 925781, 929687, 933593, 937500, 941406, 945312, 949218, \
     953125, 957031, 960937, 968750, 972656, 976562, 984375, 988281, 992187] completed [2, 2, 2, \
     4, 1, 2, 2, 2, 1, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 3, 1, 1, 1, 1, \
     1, 1, 1, 1, 2, 2, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 2, \
     3, 2, 3, 3, 2, 1, 2, 1, 1, 1, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, \
     3, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, \
     1, 1, 1, 2, 1, 3, 1, 3, 1, 3, 2, 2, 2, 2, 1, 1, 2, 1, 2, 3, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, \
     1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 2, 2, 2, 3, 2, 2, 1, 2, 1, 1, 1, 3, 2, 3, 2, 3, 1, 1, 1, 1, \
     1, 1, 1, 1, 1] started [2, 2, 2, 4, 1, 2, 2, 2, 1, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 2, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, \
     1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 2, 3, 2, 3, 3, 2, 1, 2, 1, 1, 1, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, \
     1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 3, 1, 3, 1, 3, 2, 2, 2, 2, 1, 1, 2, 1, 2, 3, \
     3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 2, 1, \
     1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 2, 2, 2, 3, 2, 2, 1, 2, 1, \
     1, 1, 3, 2, 3, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1]
  gate: prior_predictive 1564.484 eligible true finite_cells 39/39 max_loglik 1566.059
update: exposure 1.000s occ 96.00 completed 312 started 312 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [7812, 11718, 15625, 23437, 27343, \
     31250, 35156, 39062, 42968, 46875, 54687, 58593, 62500, 70312, 74218, 78125, 82031, 85937, \
     89843, 93750, 97656, 101562, 105468, 113281, 117187, 121093, 125000, 128906, 132812, 136718, \
     140625, 144531, 148437, 152343, 156250, 160156, 164062, 167968, 171875, 175781, 179687, \
     183593, 187500, 191406, 195312, 199218, 203125, 207031, 210937, 214843, 218750, 222656, \
     226562, 230468, 234375, 238281, 242187, 246093, 250000, 253906, 261718, 265625, 269531, \
     273437, 281250, 285156, 289062, 296875, 308593, 312500, 320312, 324218, 328125, 332031, \
     335937, 339843, 343750, 351562, 355468, 359375, 367187, 371093, 375000, 378906, 382812, \
     386718, 390625, 394531, 398437, 402343, 406250, 410156, 414062, 417968, 421875, 425781, \
     429687, 433593, 437500, 441406, 445312, 449218, 453125, 457031, 460937, 464843, 468750, \
     472656, 476562, 480468, 484375, 488281, 492187, 496093, 500000, 503906, 507812, 511718, \
     515625, 519531, 523437, 527343, 531250, 535156, 539062, 542968, 546875, 550781, 558593, \
     562500, 566406, 570312, 578125, 582031, 585937, 593750, 597656, 609375, 617187, 621093, \
     625000, 628906, 632812, 636718, 644531, 648437, 652343, 656250, 664062, 667968, 671875, \
     675781, 679687, 683593, 691406, 695312, 699218, 703125, 707031, 710937, 714843, 718750, \
     722656, 726562, 730468, 734375, 738281, 742187, 746093, 750000, 753906, 757812, 761718, \
     765625, 769531, 773437, 777343, 781250, 785156, 789062, 792968, 796875, 800781, 804687, \
     808593, 812500, 816406, 820312, 824218, 828125, 832031, 835937, 839843, 843750, 847656, \
     855468, 859375, 863281, 867187, 875000, 878906, 882812, 890625, 894531, 898437, 914062, \
     917968, 921875, 925781, 929687, 933593, 941406, 945312, 949218, 953125, 960937, 964843, \
     968750, 972656, 976562, 980468, 984375, 988281, 992187, 996093, 1000000] completed [1, 1, 1, \
     1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, \
     1, 1, 2, 1, 1, 1, 4, 1, 1, 2, 2, 2, 2, 2, 2, 3, 1, 1, 2, 2, 3, 2, 2, 3, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, \
     1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 3, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 1, 1, 4, 3, 1, 3, 2, 2, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 3, 2, 1, 1, 2, 2, 2, 2, 2, 3, 2, 1, 1, 4, \
     2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1] started [1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, \
     1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 4, 1, 1, 2, 2, 2, 2, 2, 2, 3, 1, 1, 2, \
     2, 3, 2, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 3, 1, 1, 3, 2, 2, 2, \
     2, 2, 1, 2, 1, 1, 4, 3, 1, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 3, 2, \
     1, 1, 2, 2, 2, 2, 2, 3, 2, 1, 1, 4, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, \
     2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
  gate: prior_predictive 1496.693 eligible true finite_cells 39/39 max_loglik 1498.268
update: exposure 1.000s occ 96.00 completed 325 started 325 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 19531, 23437, \
     27343, 31250, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 70312, 74218, \
     78125, 82031, 85937, 89843, 93750, 97656, 101562, 105468, 109375, 113281, 117187, 121093, \
     125000, 128906, 132812, 136718, 140625, 148437, 152343, 156250, 160156, 167968, 171875, \
     175781, 179687, 187500, 191406, 195312, 203125, 214843, 218750, 222656, 226562, 230468, \
     238281, 242187, 246093, 250000, 257812, 261718, 265625, 269531, 273437, 277343, 281250, \
     285156, 289062, 292968, 296875, 300781, 304687, 308593, 312500, 316406, 320312, 324218, \
     328125, 332031, 335937, 339843, 343750, 347656, 351562, 355468, 359375, 363281, 367187, \
     371093, 375000, 378906, 382812, 386718, 390625, 394531, 398437, 402343, 406250, 410156, \
     414062, 417968, 421875, 425781, 429687, 433593, 437500, 445312, 449218, 453125, 457031, \
     464843, 468750, 472656, 476562, 484375, 488281, 492187, 500000, 503906, 511718, 515625, \
     519531, 523437, 527343, 535156, 539062, 542968, 550781, 554687, 558593, 562500, 566406, \
     570312, 574218, 578125, 582031, 585937, 589843, 593750, 597656, 601562, 605468, 609375, \
     613281, 617187, 621093, 625000, 628906, 632812, 636718, 640625, 644531, 648437, 652343, \
     656250, 660156, 664062, 667968, 671875, 675781, 679687, 683593, 687500, 691406, 695312, \
     699218, 703125, 707031, 710937, 714843, 718750, 722656, 726562, 730468, 734375, 742187, \
     746093, 750000, 753906, 761718, 765625, 769531, 773437, 781250, 785156, 789062, 796875, \
     800781, 804687, 812500, 816406, 820312, 824218, 832031, 835937, 839843, 847656, 851562, \
     855468, 859375, 863281, 867187, 871093, 875000, 878906, 882812, 890625, 894531, 898437, \
     902343, 906250, 910156, 914062, 917968, 921875, 925781, 929687, 933593, 937500, 941406, \
     945312, 949218, 953125, 957031, 960937, 964843, 968750, 972656, 976562, 980468, 984375, \
     988281, 992187, 996093, 1000000] completed [1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2, \
     3, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 2, 3, 3, 2, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, \
     2, 1, 1, 1, 2, 3, 1, 1, 2, 1, 1, 2, 3, 2, 1, 4, 1, 2, 2, 2, 3, 3, 2, 1, 4, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 3, 1, 2, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 3, 4, 3, 2, 2, \
     3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 3, 1, 1, 1, 2, 1, 1, 2, 4, 2, 2, 2] \
     started [1, 2, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 2, 3, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, \
     2, 3, 3, 2, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 3, 1, 1, 2, 1, 1, 2, \
     3, 2, 1, 4, 1, 2, 2, 2, 3, 3, 2, 1, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, \
     3, 1, 2, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 2, 3, 4, 3, 2, 2, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, \
     1, 1, 2, 1, 1, 2, 3, 1, 1, 1, 2, 1, 1, 2, 4, 2, 2, 2]
  gate: prior_predictive 1573.803 eligible true finite_cells 39/39 max_loglik 1575.378
update: exposure 1.000s occ 96.00 completed 320 started 320 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 15625, 19531, \
     23437, 27343, 31250, 35156, 39062, 42968, 46875, 50781, 58593, 62500, 66406, 70312, 78125, \
     82031, 85937, 93750, 97656, 101562, 105468, 113281, 117187, 121093, 128906, 132812, 136718, \
     144531, 148437, 152343, 156250, 160156, 164062, 167968, 171875, 175781, 179687, 187500, \
     191406, 195312, 199218, 203125, 207031, 210937, 214843, 218750, 226562, 230468, 234375, \
     238281, 242187, 246093, 250000, 253906, 257812, 261718, 265625, 269531, 273437, 277343, \
     281250, 285156, 289062, 292968, 296875, 300781, 304687, 308593, 312500, 316406, 320312, \
     324218, 328125, 332031, 335937, 339843, 343750, 347656, 355468, 359375, 363281, 367187, \
     375000, 378906, 382812, 390625, 394531, 398437, 402343, 410156, 414062, 417968, 425781, \
     429687, 433593, 441406, 445312, 449218, 457031, 460937, 464843, 468750, 472656, 476562, \
     484375, 488281, 492187, 496093, 500000, 503906, 507812, 511718, 515625, 519531, 523437, \
     527343, 531250, 535156, 539062, 542968, 546875, 550781, 554687, 558593, 562500, 566406, \
     570312, 574218, 578125, 582031, 585937, 589843, 593750, 597656, 601562, 605468, 609375, \
     613281, 617187, 621093, 625000, 628906, 632812, 636718, 640625, 644531, 652343, 656250, \
     660156, 664062, 671875, 675781, 679687, 687500, 691406, 695312, 699218, 707031, 710937, \
     714843, 722656, 726562, 730468, 738281, 742187, 746093, 753906, 757812, 761718, 765625, \
     769531, 773437, 781250, 785156, 789062, 796875, 800781, 804687, 808593, 812500, 816406, \
     820312, 824218, 828125, 832031, 835937, 839843, 843750, 847656, 851562, 855468, 859375, \
     863281, 867187, 871093, 875000, 878906, 882812, 886718, 890625, 894531, 898437, 902343, \
     906250, 910156, 914062, 917968, 921875, 925781, 929687, 933593, 937500, 941406, 949218, \
     953125, 957031, 960937, 968750, 972656, 976562, 984375, 988281, 992187, 996093] completed \
     [1, 2, 3, 3, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 4, 1, 1, 1, 2, 1, \
     1, 1, 3, 2, 2, 2, 2, 2, 1, 3, 3, 4, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, \
     3, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 4, 2, 2, 2, 3, 2, 2, 2, 3, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, \
     1, 1, 1, 1, 1, 3, 2, 1, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, 2, 2, 3, 2, 4, 3, 2, 2, 2, 2, 1, 1, 2, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] started [1, 2, 3, 3, 2, 2, 3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 1, 1, 2, 1, 1, 1, 4, 1, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, 2, 2, 1, 3, 3, 4, 2, 3, 2, 2, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 4, 2, 2, 2, 3, 2, 2, \
     2, 3, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 3, 2, 1, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, \
     2, 2, 3, 2, 4, 3, 2, 2, 2, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
  gate: prior_predictive 1548.376 eligible true finite_cells 39/39 max_loglik 1549.951
update: exposure 1.000s occ 96.00 completed 310 started 310 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 19531, 23437, \
     27343, 35156, 39062, 42968, 50781, 54687, 58593, 62500, 66406, 70312, 78125, 82031, 85937, \
     93750, 97656, 101562, 105468, 109375, 113281, 117187, 121093, 125000, 132812, 136718, \
     140625, 144531, 148437, 152343, 156250, 160156, 164062, 167968, 171875, 175781, 179687, \
     183593, 187500, 191406, 195312, 199218, 203125, 207031, 210937, 214843, 218750, 222656, \
     226562, 230468, 234375, 238281, 242187, 246093, 250000, 253906, 261718, 265625, 269531, \
     273437, 281250, 285156, 289062, 292968, 300781, 304687, 308593, 316406, 320312, 324218, \
     332031, 335937, 339843, 347656, 351562, 355468, 359375, 363281, 367187, 375000, 378906, \
     382812, 390625, 394531, 398437, 402343, 406250, 410156, 414062, 417968, 421875, 425781, \
     429687, 433593, 437500, 441406, 445312, 449218, 453125, 457031, 460937, 464843, 468750, \
     472656, 476562, 480468, 484375, 488281, 492187, 496093, 500000, 503906, 507812, 511718, \
     515625, 519531, 523437, 527343, 531250, 535156, 539062, 542968, 546875, 550781, 558593, \
     562500, 566406, 570312, 578125, 582031, 585937, 589843, 597656, 601562, 609375, 613281, \
     617187, 621093, 628906, 632812, 636718, 644531, 648437, 652343, 656250, 660156, 664062, \
     671875, 675781, 679687, 687500, 691406, 695312, 703125, 707031, 710937, 714843, 718750, \
     722656, 726562, 730468, 734375, 738281, 742187, 746093, 750000, 753906, 757812, 761718, \
     765625, 769531, 773437, 777343, 781250, 785156, 789062, 792968, 796875, 800781, 804687, \
     808593, 812500, 816406, 820312, 824218, 828125, 832031, 835937, 839843, 843750, 847656, \
     855468, 859375, 863281, 867187, 875000, 878906, 882812, 886718, 894531, 898437, 906250, \
     910156, 914062, 917968, 925781, 929687, 933593, 941406, 945312, 949218, 953125, 957031, \
     960937, 968750, 972656, 976562, 984375, 988281, 992187, 1000000] completed [2, 2, 1, 1, 1, \
     1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 1, 1, 1, 2, 1, 1, \
     1, 2, 1, 2, 2, 4, 2, 3, 2, 2, 2, 3, 2, 2, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, \
     1, 2, 1, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, 4, 2, 1, 4, 3, 2, 2, 2, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 5, 2, 2, 2, 3, 2, 2, 2, 2, 4, 2, 1, 1, 1, \
     1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1] started [2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 1, 2, 3, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 4, 2, 3, 2, 2, 2, 3, 2, 2, 3, 3, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, 4, 2, 1, 4, 3, 2, 2, 2, \
     2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 5, 2, 2, 2, \
     3, 2, 2, 2, 2, 4, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
  gate: prior_predictive 1487.896 eligible true finite_cells 39/39 max_loglik 1489.471
update: exposure 1.000s occ 96.00 completed 328 started 328 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 15625, 19531, \
     23437, 27343, 31250, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 70312, \
     74218, 78125, 82031, 85937, 89843, 93750, 97656, 101562, 105468, 109375, 113281, 117187, \
     121093, 125000, 128906, 132812, 136718, 140625, 144531, 148437, 152343, 156250, 160156, \
     167968, 171875, 175781, 179687, 187500, 191406, 195312, 203125, 207031, 210937, 214843, \
     222656, 226562, 230468, 238281, 242187, 246093, 250000, 253906, 261718, 265625, 269531, \
     273437, 281250, 285156, 289062, 296875, 300781, 304687, 308593, 312500, 316406, 320312, \
     324218, 328125, 332031, 335937, 339843, 343750, 347656, 351562, 355468, 359375, 363281, \
     367187, 371093, 375000, 378906, 382812, 386718, 390625, 394531, 398437, 402343, 406250, \
     410156, 414062, 417968, 421875, 425781, 429687, 433593, 437500, 441406, 445312, 449218, \
     453125, 457031, 464843, 468750, 472656, 476562, 484375, 488281, 492187, 500000, 503906, \
     507812, 515625, 519531, 523437, 527343, 535156, 539062, 542968, 546875, 550781, 558593, \
     562500, 566406, 570312, 578125, 582031, 585937, 593750, 597656, 601562, 609375, 613281, \
     617187, 621093, 625000, 628906, 632812, 636718, 644531, 648437, 652343, 656250, 660156, \
     664062, 667968, 671875, 675781, 679687, 683593, 687500, 691406, 695312, 699218, 703125, \
     707031, 710937, 714843, 718750, 722656, 726562, 730468, 734375, 738281, 742187, 746093, \
     750000, 753906, 761718, 765625, 769531, 773437, 781250, 785156, 789062, 796875, 800781, \
     804687, 812500, 816406, 820312, 824218, 832031, 835937, 839843, 843750, 847656, 855468, \
     859375, 863281, 867187, 875000, 878906, 882812, 890625, 894531, 898437, 906250, 910156, \
     914062, 917968, 921875, 925781, 929687, 933593, 941406, 945312, 949218, 953125, 957031, \
     960937, 964843, 968750, 972656, 976562, 980468, 984375, 988281, 992187, 996093, 1000000] \
     completed [1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 3, 2, 3, 2, 4, 2, 2, 3, 2, \
     2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 3, \
     4, 2, 3, 2, 2, 2, 2, 2, 3, 3, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, \
     1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 1, 1, 1, 2, \
     1, 1, 1, 5, 2, 2, 3, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 2, \
     1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, \
     1, 2, 1, 1, 1, 2, 2, 2, 2, 5, 2, 2, 2, 2, 2] started [1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 2, 1, 1, \
     1, 2, 1, 1, 1, 3, 2, 3, 2, 4, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, \
     1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 2, 3, 4, 2, 3, 2, 2, 2, 2, 2, 3, 3, 2, 1, 1, 1, 1, 2, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 5, 2, 2, 3, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, \
     1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 2, 5, 2, 2, 2, 2, 2]
  gate: prior_predictive 1591.494 eligible true finite_cells 39/39 max_loglik 1593.069
update: exposure 1.000s occ 96.00 completed 320 started 320 elapsed 1.000s no_knee_before 0.60343
  evidence: initial 96 final 96 busy_micros 96000000 offsets [3906, 7812, 11718, 15625, 19531, \
     23437, 27343, 31250, 35156, 39062, 42968, 46875, 50781, 54687, 58593, 62500, 66406, 74218, \
     78125, 82031, 85937, 93750, 97656, 101562, 109375, 113281, 117187, 121093, 128906, 132812, \
     136718, 140625, 144531, 152343, 156250, 160156, 167968, 171875, 175781, 179687, 187500, \
     191406, 195312, 203125, 207031, 210937, 214843, 222656, 226562, 234375, 238281, 242187, \
     246093, 250000, 253906, 257812, 261718, 265625, 269531, 273437, 277343, 281250, 285156, \
     289062, 292968, 296875, 300781, 304687, 308593, 312500, 316406, 320312, 324218, 328125, \
     332031, 335937, 339843, 343750, 347656, 351562, 355468, 359375, 363281, 371093, 375000, \
     378906, 382812, 386718, 390625, 394531, 398437, 406250, 410156, 414062, 417968, 425781, \
     429687, 433593, 437500, 441406, 449218, 453125, 457031, 464843, 468750, 472656, 476562, \
     484375, 488281, 492187, 500000, 503906, 507812, 511718, 515625, 519531, 523437, 527343, \
     531250, 535156, 539062, 542968, 546875, 550781, 554687, 558593, 562500, 566406, 570312, \
     574218, 578125, 582031, 585937, 589843, 593750, 597656, 601562, 605468, 609375, 613281, \
     617187, 621093, 625000, 628906, 632812, 636718, 640625, 644531, 648437, 652343, 656250, \
     660156, 667968, 671875, 675781, 679687, 683593, 687500, 691406, 695312, 703125, 707031, \
     710937, 714843, 722656, 726562, 730468, 734375, 738281, 746093, 750000, 753906, 761718, \
     765625, 769531, 773437, 781250, 785156, 789062, 796875, 800781, 804687, 808593, 812500, \
     816406, 820312, 824218, 828125, 832031, 835937, 839843, 847656, 851562, 855468, 859375, \
     863281, 867187, 871093, 875000, 878906, 882812, 886718, 890625, 894531, 898437, 902343, \
     906250, 910156, 914062, 917968, 921875, 925781, 929687, 933593, 937500, 941406, 945312, \
     949218, 953125, 957031, 960937, 964843, 968750, 972656, 976562, 980468, 984375, 988281, \
     992187, 1000000] completed [2, 2, 4, 2, 2, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, \
     1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, \
     1, 1, 1, 2, 1, 1, 2, 4, 3, 2, 2, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, \
     1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 4, 2, 2, 2, 3, 2, 2, 3, 2, 2, 2, 1, 1, 1, 2, \
     1, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 5, 2, 2, 2, 4, 2, 2, 2, 2, 2, \
     2, 2, 3, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1] started [2, 2, 4, 2, 2, \
     1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1, 2, 4, 3, 2, 2, 4, 2, 2, 2, \
     2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, \
     1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, \
     3, 4, 2, 2, 2, 3, 2, 2, 3, 2, 2, 2, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, \
     1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, \
     1, 1, 2, 1, 1, 2, 5, 2, 2, 2, 4, 2, 2, 2, 2, 2, 2, 2, 3, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, \
     1, 1, 2, 1, 1, 1, 1, 1]
  gate: prior_predictive 1546.047 eligible true finite_cells 39/39 max_loglik 1547.622
";

/// Pins no-knee mass collapse for the captured 96-slot saturated replay.
#[test]
fn saturated_bound_replay_collapses_no_knee_mass() -> Result<(), TestError> {
    let mut factor = flat_capacity_factor()?;
    let captured = FLAT_CAPTURED_EVIDENCE;
    let mut lines = captured.lines();
    for _ in 0_u32..39 {
        let update = lines.next().ok_or(TestError::CapturedEvidence)?;
        let evidence = lines.next().ok_or(TestError::CapturedEvidence)?;
        let gate = lines.next().ok_or(TestError::CapturedEvidence)?;
        if !gate.contains("eligible true") {
            return Err(TestError::CapturedEvidence);
        }
        let window = parse_captured_window(update, evidence)?;
        apply_flat_captured(&mut factor, &window)?;
    }
    assert!(factor.no_knee_probability() <= 0.01_f64);
    Ok(())
}

/// Uses the capacity-evidence construction from the simulation plant.
fn flat_capacity_factor() -> Result<super::CapacityFactor, TestError> {
    let grid = CapacityGrid::new_with_prior(
        &[0.101_f64, 0.202_f64, 0.404_f64],
        &[80.0_f64, 320.0_f64, 600.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
        super::CapacityPrior::LogUniform,
    )?;
    Ok(super::CapacityFactor::new_with_prior_with_groups(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64)?,
        96.0_f64,
        1.0_f64,
        48_096,
        256,
    )?)
}

/// Applies one captured capacity-evidence window without modification.
fn apply_flat_captured(
    factor: &mut super::CapacityFactor,
    captured: &CapturedWindow,
) -> Result<(), TestError> {
    let window = ResourceWindow::new_with_starts(
        captured.concurrency,
        captured.exposure_seconds,
        captured.completed_attempts,
        captured.started_attempts,
    )?;
    factor.update(
        occupancy_trace_for_test(
            window,
            captured.initial_busy_slots,
            captured.final_busy_slots,
            captured.busy_slot_micros,
            &captured.offsets_micros,
            &captured.completed,
            &captured.started,
        ),
        Duration::from_secs_f64(captured.elapsed_seconds),
    );
    Ok(())
}

/// Holds one parsed window from the embedded simulation evidence.
struct CapturedWindow {
    exposure_seconds: f64,
    concurrency: f64,
    completed_attempts: u32,
    started_attempts: u32,
    elapsed_seconds: f64,
    initial_busy_slots: u32,
    final_busy_slots: u32,
    busy_slot_micros: u128,
    offsets_micros: Vec<u64>,
    completed: Vec<u32>,
    started: Vec<u32>,
}

/// Parses one update and evidence line from the embedded simulation evidence.
fn parse_captured_window(update: &str, evidence: &str) -> Result<CapturedWindow, TestError> {
    let fields = update.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 13 {
        return Err(TestError::CapturedEvidence);
    }
    let exposure_seconds = fields[2].trim_end_matches('s').parse()?;
    let concurrency = fields[4].parse()?;
    let completed_attempts = fields[6].parse()?;
    let started_attempts = fields[8].parse()?;
    let elapsed_seconds = fields[10].trim_end_matches('s').parse()?;
    let (header, arrays) = evidence
        .split_once(" offsets [")
        .ok_or(TestError::CapturedEvidence)?;
    let header = header.split_ascii_whitespace().collect::<Vec<_>>();
    if header.len() != 7 {
        return Err(TestError::CapturedEvidence);
    }
    let initial_busy_slots = header[2].parse()?;
    let final_busy_slots = header[4].parse()?;
    let busy_slot_micros = header[6].parse()?;
    let (offsets, arrays) = arrays
        .split_once("] completed [")
        .ok_or(TestError::CapturedEvidence)?;
    let (completed, started) = arrays
        .split_once("] started [")
        .ok_or(TestError::CapturedEvidence)?;
    let started = started
        .strip_suffix(']')
        .ok_or(TestError::CapturedEvidence)?;
    Ok(CapturedWindow {
        exposure_seconds,
        concurrency,
        completed_attempts,
        started_attempts,
        elapsed_seconds,
        initial_busy_slots,
        final_busy_slots,
        busy_slot_micros,
        offsets_micros: parse_captured_values(offsets)?,
        completed: parse_captured_values(completed)?,
        started: parse_captured_values(started)?,
    })
}

/// Parses one comma-separated array from the embedded simulation evidence.
fn parse_captured_values<T>(values: &str) -> Result<Vec<T>, TestError>
where
    T: FromStr,
    TestError: From<T::Err>,
{
    if values.is_empty() {
        return Ok(Vec::new());
    }
    values
        .split(", ")
        .map(|value| value.parse().map_err(TestError::from))
        .collect()
}

fn apply_flat_capacity_windows(
    factor: &mut super::CapacityFactor,
    concurrency: u32,
    count: u32,
) -> Result<(), TestError> {
    let completions = if concurrency < 65 {
        (f64::from(concurrency) / 0.202_f64).round() as u32
    } else {
        320
    };
    let group_count = completions.min(128);
    let offsets = (0..group_count)
        .map(|index| u64::from(index + 1) * 1_000_000_u64 / u64::from(group_count + 1))
        .collect::<Vec<_>>();
    let completed = (0..group_count)
        .map(|index| completions / group_count + u32::from(index < completions % group_count))
        .collect::<Vec<_>>();
    for _ in 0..count {
        let window = ResourceWindow::new_with_starts(
            f64::from(concurrency),
            1.0_f64,
            completions,
            completions,
        )?;
        let started = completed.clone();
        factor.update(
            occupancy_trace_for_test(
                window,
                concurrency,
                concurrency,
                u128::from(concurrency) * 1_000_000_u128,
                &offsets,
                &completed,
                &started,
            ),
            Duration::from_secs(1),
        );
    }
    Ok(())
}

#[test]
fn log_normal_endpoint_cells_truncate_both_tail_masses() -> Result<(), TestError> {
    let masses = log_normal_axis_masses(&[1.0_f64, 2.0_f64, 4.0_f64], 2.0_f64, 1.0_f64)?;
    assert!((masses.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    assert!((masses[0] - masses[2]).abs() <= 16.0_f64 * f64::EPSILON);
    assert!(masses[1] > masses[0]);
    Ok(())
}

#[test]
fn no_knee_condition_reports_zero_mass_and_unbounded_capacity() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        3,
    )?;
    factor.weights.fill(0.0_f64);
    let no_knee = factor.grid.knee_cell_count as usize;
    factor.weights[no_knee] = 1.0_f64;
    let mut values = [0.0_f64];
    let mut probabilities = [0.0_f64];

    let conditioning_probability =
        factor.write_capacity_posterior(&mut values, &mut probabilities)?;
    let expected = factor.expected_capacity(Level::new());

    assert_eq!(conditioning_probability.to_bits(), 0.0_f64.to_bits());
    assert_eq!(probabilities[0].to_bits(), 0.0_f64.to_bits());
    assert_eq!(
        expected.conditioning_probability.to_bits(),
        0.0_f64.to_bits()
    );
    assert!(expected.value.is_infinite());
    Ok(())
}

#[test]
fn observation_contract_sizes_history_from_coverage() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[2.0_f64], &[10.0_f64], &[0.0_f64])?;
    let factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        64,
    )?;
    assert_eq!(factor.start_history.len(), 3);
    let collapse_grid = CapacityGrid::new(&[600.0_f64], &[0.01_f64], &[2.0_f64])?;
    let result = super::CapacityFactor::new_with_prior(
        collapse_grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        4_096.0_f64,
        1.0_f64,
        8,
    );
    assert!(matches!(result, Err(CapacityModelError::StorageBound)));
    Ok(())
}

#[test]
fn completion_convolution_update_does_not_allocate() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64], &[10.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        64,
    )?;
    update_constant_trace(&mut factor, 1, 1.0_f64, 32)?;
    let window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 32, 32)?;
    let offsets = (1_u64..=32).map(|value| value * 30_000).collect::<Vec<_>>();
    let completed = [1_u32; 32];
    let started = [1_u32; 32];
    let evidence =
        occupancy_trace_for_test(window, 1, 1, 1_000_000, &offsets, &completed, &started);
    let allocation = measure(|| factor.update(evidence, Duration::from_secs(1)));
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    assert_eq!(factor.start_history_len, 2);
    Ok(())
}

#[test]
fn raw_path_score_matches_the_exponential_clock_oracle() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64, 1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid.clone(),
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        8,
    )?;
    let window = ResourceWindow::new_with_starts(0.25_f64, 1.0_f64, 1, 0)?;
    let offsets = [250_000_u64];
    let completed = [1_u32];
    let started = [0_u32];
    let evidence = occupancy_trace_for_test(window, 1, 0, 250_000, &offsets, &completed, &started);

    for index in 0..grid.cell_count() as usize {
        let rate = super::state_rate(&grid, index, 1);
        let mut exposures = [0.0_f64; 2];
        let mut completion_counts = [0_u32; 2];
        fold_trace(evidence, &mut exposures, &mut completion_counts);
        let raw = path_log_score(&grid, index, &exposures, &completion_counts);
        let oracle = rate.ln() - rate * 0.25_f64;
        assert!((raw - oracle).abs() <= 256.0_f64 * f64::EPSILON);
    }

    assert_contamination_filter_parity(&mut factor, &grid, evidence);
    Ok(())
}

#[test]
fn below_bound_raw_score_matches_pre_removal_corrected_score() {
    const PREVIOUS_CORRECTIONS_NATS: [f64; 3] = [
        -0.000_000_000_000_015_f64,
        0.000_000_000_000_079_f64,
        0.000_000_000_000_141_f64,
    ];
    let mut exposures = [0.0_f64; 193];
    let mut completions = [0_u32; 193];
    exposures[30] = 1.0_f64;
    completions[30] = 500;

    for (service_seconds, correction) in [0.0505_f64, 0.101_f64, 0.202_f64]
        .into_iter()
        .zip(PREVIOUS_CORRECTIONS_NATS)
    {
        let mut rates = [0.0_f64; 193];
        for (state, rate) in rates.iter_mut().enumerate().skip(1) {
            *rate = f64::from(u32::try_from(state).unwrap_or(u32::MAX)) / service_seconds;
        }
        let raw = super::path_log_score_with_rates(&rates, &exposures, &completions);
        let previously_corrected = raw + correction;
        assert!((raw - previously_corrected).abs() <= 1.0e-9_f64);
    }
}

fn assert_contamination_filter_parity(
    factor: &mut super::CapacityFactor,
    grid: &CapacityGrid,
    evidence: OccupancyTraceEvidence<'_>,
) {
    let initial_filter_weights = factor.filter_weights.clone();
    let initial_curve_weights = factor.filter_curve_weights.clone();
    let mut exposures = [0.0_f64; 2];
    let mut completion_counts = [0_u32; 2];
    fold_trace(evidence, &mut exposures, &mut completion_counts);
    let likelihoods = (0..grid.cell_count() as usize)
        .map(|index| path_log_score(grid, index, &exposures, &completion_counts))
        .collect::<Vec<_>>();
    let prior_predictive = log_weighted_sum(&factor.prior_weights, &likelihoods);
    let cell_count = likelihoods.len();
    let quality_count = factor.contamination_probabilities.len();
    let mut direct_weights = vec![0.0_f64; cell_count];
    let mut filter_evidence = vec![0.0_f64; initial_filter_weights.len()];
    let mut conditional_weights = vec![0.0_f64; initial_curve_weights.len()];
    for (filter, prior) in initial_curve_weights.chunks_exact(cell_count).enumerate() {
        let contamination = factor.contamination_probabilities[filter % quality_count];
        let mixtures = likelihoods
            .iter()
            .map(|likelihood| {
                log_contamination_mixture(*likelihood, prior_predictive, contamination).exp()
            })
            .collect::<Vec<_>>();
        let predictive = prior
            .iter()
            .zip(&mixtures)
            .map(|(weight, likelihood)| weight * likelihood)
            .sum::<f64>();
        filter_evidence[filter] = initial_filter_weights[filter] * predictive;
        for cell in 0..cell_count {
            conditional_weights[filter * cell_count + cell] =
                prior[cell] * mixtures[cell] / predictive;
        }
    }
    let total_filter_evidence = filter_evidence.iter().sum::<f64>();
    for (filter, evidence) in filter_evidence.iter().enumerate() {
        let transition =
            ChangePointKernel::new(factor.hazard_rates_per_second[filter / quality_count])
                .probabilities(Duration::from_secs(1));
        for cell in 0..cell_count {
            let conditional = transition.retained * conditional_weights[filter * cell_count + cell]
                + transition.redrawn * factor.prior_weights[cell];
            direct_weights[cell] += evidence / total_filter_evidence * conditional;
        }
    }
    factor.update(evidence, Duration::from_secs(1));
    let operation_bound = 4_096.0_f64 * f64::EPSILON;
    assert!(
        factor
            .weights
            .iter()
            .zip(direct_weights)
            .all(|(actual, expected)| (actual - expected).abs() <= operation_bound)
    );
}

#[test]
fn honest_regime_prices_derive_the_capacity_operation_budget() -> Result<(), TestError> {
    let general = CapacityGrid::new(
        &[0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64],
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?;
    let capacity = CapacityGrid::new(
        &[0.2_f64, 0.4_f64, 0.8_f64],
        &[80.0_f64, 320.0_f64, 1_280.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?;
    let historical = CapacityGrid::new(
        &[0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64, 0.4_f64],
        &[64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?;
    let production_default = CapacityAllocation {
        cell_count: 7,
        state_count: 2,
        filter_count: 432,
        filter_curve_count: 3_024,
        transition_count: 200_001,
        ln_gamma_integer_count: 100_001,
        start_history_capacity: 2,
        group_count: 100_000,
    };
    let prices = [
        operation_price_for_test(&general, 257, 256)?,
        operation_price_for_test(&capacity, 193, 256)?,
        operation_price_for_test(&capacity, 65, 256)?,
        operation_price_for_test(&general, 1_537, 64)?,
        operation_price_for_test(&general, 257, 64)?,
        operation_price_for_test(&general, 65, 256)?,
        operation_price_for_test(&general, 65, 256)?,
        operation_price_for_test(&general, 257, 256)?,
        operation_price_for_test(&general, 257, 128)?,
        operation_price_for_test(&general, 257, 256)?,
        operation_price_for_test(&historical, 257, 256)?,
        operation_price_for_test(&historical, 449, 128)?,
        operation_price_for_test(&historical, 129, 256)?,
        capacity_update_operation_count(production_default)
            .ok_or(CapacityModelError::StorageBound)?,
    ];
    assert_eq!(
        prices,
        [
            1_824_191, 1_752_821, 1_742_837, 1_991_679, 1_774_079, 1_791_551, 1_791_551, 1_824_191,
            1_790_783, 1_824_191, 1_795_331, 1_794_563, 1_778_691, 6_303_059,
        ]
    );
    let maximum = prices
        .into_iter()
        .max()
        .ok_or(CapacityModelError::StorageBound)?;
    assert_eq!(CAPACITY_UPDATE_OPERATION_COUNT_MAX, 13_000_000);
    assert!(maximum.saturating_mul(2) <= CAPACITY_UPDATE_OPERATION_COUNT_MAX);
    Ok(())
}

fn operation_price_for_test(
    grid: &CapacityGrid,
    state_count: usize,
    group_count: usize,
) -> Result<u64, CapacityModelError> {
    let cell_count = grid.service_times_seconds.len();
    capacity_update_operation_count(CapacityAllocation {
        cell_count,
        state_count,
        filter_count: 160,
        filter_curve_count: 160 * cell_count,
        transition_count: 100_001,
        ln_gamma_integer_count: 50_001,
        start_history_capacity: 1,
        group_count,
    })
    .ok_or(CapacityModelError::StorageBound)
}

#[quickcheck]
fn batched_completion_residuals_accept_the_specified_clock(batch_code: u8) -> bool {
    let batch_count = u32::from(batch_code % 7 + 2);
    let Ok(grid) = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64]) else {
        return false;
    };
    let Ok(prior) = ArrivalPrior::test_artifact() else {
        return false;
    };
    let Ok(mut factor) = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &prior,
        20.0_f64,
        1.0_f64,
        32,
    ) else {
        return false;
    };
    let Ok(distribution) = Gamma::new(f64::from(batch_count), 1.0_f64) else {
        return false;
    };
    let Ok(window) = ResourceWindow::new_with_starts(20.0_f64, 1.0_f64, batch_count, batch_count)
    else {
        return false;
    };
    let offset = (distribution.inverse_cdf(0.5_f64) / 20.0_f64 * 1_000_000.0_f64) as u64;
    factor.update(
        occupancy_trace_for_test(
            window,
            20,
            20,
            20_000_000,
            &[offset],
            &[batch_count],
            &[batch_count],
        ),
        Duration::from_secs(1),
    );
    if factor.residual_sample_count != 1 {
        return false;
    }
    factor.residual_head = 0;
    factor.residual_len = 0;
    factor.residual_sample_count = 0;
    for sample in 0_u32..32 {
        let probability = (f64::from(sample) + 0.5_f64) / 32.0_f64;
        let hazard = distribution.inverse_cdf(probability);
        factor.residual_integrated_hazards.fill(hazard);
        let residual = factor.predictive_residual(batch_count);
        factor.record_residual(residual);
    }
    let sample_count = f64::from(factor.residual_sample_count);
    factor.refresh_residual_check(sample_count);
    let bound =
        (-(super::RESIDUAL_REJECTION_PROBABILITY * 0.5_f64).ln() / (2.0_f64 * sample_count)).sqrt();
    factor.residual_maximum_distance <= bound
}

#[test]
fn batched_completion_residuals_reject_a_misspecified_clock() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        20.0_f64,
        1.0_f64,
        32,
    )?;
    let completed = 4_u32;
    let distribution = Gamma::new(f64::from(completed), 1.0_f64)
        .map_err(|_| CapacityModelError::InvalidObservationContract)?;
    for _ in 0_u32..32 {
        let hazard = distribution.inverse_cdf(0.5_f64);
        factor.residual_integrated_hazards.fill(hazard);
        let residual = factor.predictive_residual(completed);
        factor.record_residual(residual);
    }
    let window = ResourceWindow::new_with_starts(0.0_f64, 1.0_f64, 0, 0)?;
    let offsets: [u64; 0] = [];
    let counts: [u32; 0] = [];
    factor.update_residual_check(occupancy_trace_for_test(
        window, 0, 0, 0, &offsets, &counts, &counts,
    ));
    assert!(factor.markov_clock_rejected());
    Ok(())
}

#[test]
fn residual_ring_retains_the_latest_contract_window() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        3,
    )?;
    for residual in [0.1_f64, 0.2_f64, 0.3_f64, 0.4_f64] {
        factor.record_residual(residual);
    }
    assert_eq!(factor.residual_sample_count, 3);
    assert!(!factor.residuals.contains(&0.1_f64));
    assert!(factor.residuals.contains(&0.2_f64));
    assert!(factor.residuals.contains(&0.3_f64));
    assert!(factor.residuals.contains(&0.4_f64));
    Ok(())
}

#[test]
fn curve_axis_rejects_more_than_two_octaves_per_cell() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[1.0_f64, 16.0_f64], &[100.0_f64], &[0.0_f64])?;
    let result = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        3,
    );
    assert!(matches!(result, Err(CapacityModelError::GridCoverage)));
    Ok(())
}

#[test]
fn residual_cdf_mixes_each_curve_before_the_clock_check() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64, 2.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        8,
    )?;
    factor.weights.fill(0.0_f64);
    factor.weights[0] = 0.25_f64;
    factor.weights[1] = 0.75_f64;
    factor.residual_integrated_hazards.fill(0.0_f64);
    factor.residual_integrated_hazards[0] = 0.5_f64;
    factor.residual_integrated_hazards[1] = 2.0_f64;
    let expected =
        0.25_f64 * (1.0_f64 - (-0.5_f64).exp()) + 0.75_f64 * (1.0_f64 - (-2.0_f64).exp());
    assert!((factor.predictive_residual(1) - -(-expected).ln_1p()).abs() <= 8.0_f64 * f64::EPSILON);
    Ok(())
}

#[quickcheck]
fn completion_predictive_cdf_is_monotone(count: u8) -> bool {
    let Ok(grid) = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64]) else {
        return false;
    };
    let Ok(prior) = ArrivalPrior::test_artifact() else {
        return false;
    };
    let Ok(mut factor) = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &prior,
        2.0_f64,
        1.0_f64,
        255,
    ) else {
        return false;
    };
    factor.previous_window_concurrency = Some(1.0_f64);
    let Ok(window) = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 0, 1) else {
        return false;
    };
    let lower = factor.completion_predictive_cdf(&window, u32::from(count));
    let upper = factor.completion_predictive_cdf(&window, u32::from(count) + 1);
    (0.0_f64..=1.0_f64).contains(&lower) && lower <= upper && upper <= 1.0_f64
}

#[quickcheck]
fn completion_predictive_sweep_matches_scalar_cdf_and_summary(
    seed: u8,
    exposure_millis: u16,
    high_concurrency: bool,
) -> bool {
    let Ok(grid) = CapacityGrid::new(&[0.5_f64, 2.0_f64], &[100.0_f64], &[0.0_f64]) else {
        return false;
    };
    let Ok(prior) = ArrivalPrior::test_artifact() else {
        return false;
    };
    let count_max = 16_u32 + u32::from(seed % 16);
    let Ok(mut factor) = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        &prior,
        2.0_f64,
        1.0_f64,
        count_max,
    ) else {
        return false;
    };
    let concurrency = if high_concurrency { 2.0_f64 } else { 1.0_f64 };
    factor.previous_window_concurrency = Some(concurrency);
    let started = u32::from(seed % 8);
    let exposure_seconds = f64::from(exposure_millis % 1_901 + 100) / 1_000.0_f64;
    let Ok(window) = ResourceWindow::new_with_starts(concurrency, exposure_seconds, 0, started)
    else {
        return false;
    };
    let mut sweep = vec![0.0_f64; count_max as usize + 1];
    factor.write_completion_predictive_cdfs(&window, &mut sweep);
    for (count, actual) in sweep.iter().enumerate() {
        if !kernel_float_matches(
            *actual,
            factor.completion_predictive_cdf(&window, count as u32),
        ) {
            return false;
        }
    }

    let observed = u32::from(seed) % (count_max + 1);
    let thresholds = [0.1_f64, 0.5_f64, 0.9_f64];
    let summary = factor.completion_predictive_summary(&window, observed, thresholds);
    let mut reference_quantiles = [count_max; 3];
    for (index, threshold) in thresholds.into_iter().enumerate() {
        let mut low = 0_u32;
        let mut high = count_max;
        while low < high {
            let middle = low + (high - low) / 2;
            if factor.completion_predictive_cdf(&window, middle) >= threshold {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        reference_quantiles[index] = low;
    }
    let reference_upper = factor.completion_predictive_cdf(&window, observed);
    let reference_lower = if observed == 0 {
        0.0_f64
    } else {
        factor.completion_predictive_cdf(&window, observed - 1)
    };
    let rank_offset = f64::from(seed) / f64::from(u8::MAX);
    let rank = summary.lower + rank_offset * (summary.upper - summary.lower);
    let reference_rank = reference_lower + rank_offset * (reference_upper - reference_lower);
    summary.quantile_counts == reference_quantiles
        && kernel_float_matches(summary.lower, reference_lower)
        && kernel_float_matches(summary.upper, reference_upper)
        && kernel_float_matches(rank, reference_rank)
}

#[quickcheck]
fn completion_group_convolution_matches_scalar_reference(
    seed: u8,
    service_millis: u16,
    capacity: u16,
) -> bool {
    let Ok(ln_gamma_integers) = integer_ln_gamma_table(32) else {
        return false;
    };
    let service_seconds = f64::from(service_millis % 1_900 + 100) / 1_000.0_f64;
    let capacity_per_second = f64::from(capacity % 900 + 100);
    let Ok(grid) = CapacityGrid::new(&[service_seconds], &[capacity_per_second], &[0.0_f64]) else {
        return false;
    };
    let history = [
        StartWindow {
            end_micros: 500_000,
            exposure_seconds: 0.5_f64,
            started_attempts: Some(u32::from(seed % 7 + 1)),
        },
        StartWindow {
            end_micros: 1_250_000,
            exposure_seconds: 0.75_f64,
            started_attempts: Some(u32::from(seed.rotate_left(2) % 9 + 1)),
        },
        StartWindow {
            end_micros: 2_250_000,
            exposure_seconds: 1.0_f64,
            started_attempts: Some(u32::from(seed.rotate_left(4) % 11 + 1)),
        },
    ];
    let retained = RetainedHistory {
        windows: &history,
        head: 0,
        length: history.len(),
        end_micros: 2_500_000,
    };
    for index in 0..grid.service_times_seconds.len() {
        for completed in 0..=31_u32 {
            let Ok(window) = ResourceWindow::new_with_starts(
                f64::from(seed % 4 + 1),
                f64::from(seed % 19 + 1) / 10.0_f64,
                completed,
                u32::from(seed % 13),
            ) else {
                return false;
            };
            let mut actual_coefficients = [0.0_f64; 32];
            let mut actual_convolution = [0.0_f64; 32];
            let mut actual_binomial = [0.0_f64; 32];
            let actual = completion_log_likelihood(
                &grid,
                index,
                retained,
                &window,
                2.0_f64,
                1.5_f64,
                CompletionScratch {
                    simd_level: Level::new(),
                    coefficients: &mut actual_coefficients,
                    convolution: &mut actual_convolution,
                    binomial: &mut actual_binomial,
                    ln_gamma_integers: &ln_gamma_integers,
                },
            );
            let mut reference_coefficients = [0.0_f64; 32];
            let mut reference_convolution = [0.0_f64; 32];
            let mut reference_binomial = [0.0_f64; 32];
            let reference = completion_log_likelihood_reference(
                &grid,
                index,
                retained,
                &window,
                2.0_f64,
                1.5_f64,
                CompletionScratch {
                    simd_level: Level::new(),
                    coefficients: &mut reference_coefficients,
                    convolution: &mut reference_convolution,
                    binomial: &mut reference_binomial,
                    ln_gamma_integers: &ln_gamma_integers,
                },
            );
            if !kernel_float_matches(actual, reference)
                || !actual_coefficients
                    .iter()
                    .zip(reference_coefficients)
                    .all(|(actual, expected)| kernel_float_matches(*actual, expected))
            {
                return false;
            }
        }
    }
    true
}

#[quickcheck]
fn eight_row_convolution_matches_one_output_scalar_order(
    seed: u8,
    coefficient_seed: u64,
    mass_seed: u64,
) -> bool {
    let degree = usize::from(seed % 9 + 7);
    let group_degree = usize::from(seed.rotate_left(3) % 9 + 7);
    let target = (degree + group_degree).min(31);
    let mut actual_coefficients = [0.0_f64; 32];
    let mut reference_coefficients = [0.0_f64; 32];
    for index in 0..=degree {
        let bits = coefficient_seed.rotate_left(index as u32)
            ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let exponent = 1_000 + bits % 40;
        let value = f64::from_bits((exponent << 52) | (bits & ((1_u64 << 52) - 1)));
        actual_coefficients[index] = value;
        reference_coefficients[index] = value;
    }
    let mut actual_binomial = [0.0_f64; 32];
    let mut reference_binomial = [0.0_f64; 32];
    for index in 0..=group_degree {
        let bits = mass_seed.rotate_right(index as u32)
            ^ (index as u64).wrapping_mul(0xd1b5_4a32_d192_ed03);
        let mass = u32::try_from(bits % 10_000).unwrap_or(u32::MAX);
        let value = -f64::from(mass) / 100.0_f64;
        actual_binomial[index] = value;
        reference_binomial[index] = value;
    }
    let mut actual_convolution = [0.0_f64; 32];
    let mut reference_convolution = [0.0_f64; 32];
    let actual = dispatch!(Level::new(), simd => completion_group_convolution(
        simd,
        &mut actual_coefficients,
        &mut actual_convolution,
        &mut actual_binomial,
        degree,
        group_degree,
        target,
    ));
    let reference = scalar_completion_group_convolution(
        &mut reference_coefficients,
        &mut reference_convolution,
        &mut reference_binomial,
        degree,
        group_degree,
        target,
    );
    tuple_bits_equal(actual, reference)
        && slice_bits_equal(&actual_coefficients, &reference_coefficients)
        && slice_bits_equal(&actual_convolution, &reference_convolution)
        && slice_bits_equal(&actual_binomial, &reference_binomial)
}

#[test]
fn convolution_sweep_ignores_poisoned_suffix() -> Result<(), TestError> {
    let ln_gamma_integers = integer_ln_gamma_table(32)?;
    let grid = CapacityGrid::new(&[0.2_f64], &[500.0_f64], &[0.0_f64])?;
    let history = [
        StartWindow {
            end_micros: 500_000,
            exposure_seconds: 0.5_f64,
            started_attempts: Some(9),
        },
        StartWindow {
            end_micros: 1_250_000,
            exposure_seconds: 0.75_f64,
            started_attempts: Some(11),
        },
    ];
    let retained = RetainedHistory {
        windows: &history,
        head: 0,
        length: history.len(),
        end_micros: 1_500_000,
    };
    let window = ResourceWindow::new_with_starts(2.0_f64, 1.0_f64, 15, 7)?;
    let mut clean_coefficients = [0.0_f64; 32];
    let mut clean_convolution = [0.0_f64; 32];
    let mut clean_binomial = [0.0_f64; 32];
    let clean = completion_log_likelihood(
        &grid,
        0,
        retained,
        &window,
        2.0_f64,
        1.5_f64,
        CompletionScratch {
            simd_level: Level::new(),
            coefficients: &mut clean_coefficients,
            convolution: &mut clean_convolution,
            binomial: &mut clean_binomial,
            ln_gamma_integers: &ln_gamma_integers,
        },
    );
    let mut poisoned_coefficients = [0.0_f64; 32];
    let mut poisoned_convolution = [1.0e300_f64; 32];
    let mut poisoned_binomial = [0.0_f64; 32];
    let poisoned = completion_log_likelihood(
        &grid,
        0,
        retained,
        &window,
        2.0_f64,
        1.5_f64,
        CompletionScratch {
            simd_level: Level::new(),
            coefficients: &mut poisoned_coefficients,
            convolution: &mut poisoned_convolution,
            binomial: &mut poisoned_binomial,
            ln_gamma_integers: &ln_gamma_integers,
        },
    );
    assert_eq!(clean.to_bits(), poisoned.to_bits());
    assert!(slice_bits_equal(
        &clean_coefficients,
        &poisoned_coefficients
    ));
    assert!(
        poisoned_convolution[16..]
            .iter()
            .all(|value| value.to_bits() == 1.0e300_f64.to_bits())
    );
    Ok(())
}

fn scalar_completion_group_convolution(
    coefficients: &mut [f64],
    convolution: &mut [f64],
    binomial: &mut [f64],
    degree: usize,
    group_degree: usize,
    target: usize,
) -> (f64, f64, usize) {
    let next_degree = target.min(degree + group_degree);
    let maximum = dispatch!(Level::new(), simd => exponentiate_log_masses(
        simd,
        &mut binomial[..=group_degree],
    ));
    convolution[..=next_degree].fill(0.0_f64);
    for (known, &coefficient) in coefficients[..=degree].iter().enumerate() {
        let added_count = group_degree.min(target - known) + 1;
        for (added, &mass) in binomial[..added_count].iter().enumerate() {
            let output = known + added;
            convolution[output] = coefficient.mul_add(mass, convolution[output]);
        }
    }
    let scale = convolution[..=next_degree]
        .iter()
        .copied()
        .fold(0.0_f64, f64::max);
    for index in 0..=next_degree {
        coefficients[index] = convolution[index] / scale;
    }
    (maximum, scale, next_degree)
}

fn tuple_bits_equal(left: (f64, f64, usize), right: (f64, f64, usize)) -> bool {
    left.0.to_bits() == right.0.to_bits()
        && left.1.to_bits() == right.1.to_bits()
        && left.2 == right.2
}

fn slice_bits_equal(left: &[f64], right: &[f64]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| left.to_bits() == right.to_bits())
}

#[quickcheck]
fn vector_exponential_matches_scalar_libm(seed: u64) -> bool {
    dispatch!(Level::new(), simd => {
        let input = <_>::from_fn(simd, |lane| {
            let mixed = seed.rotate_left(lane as u32)
                ^ (lane as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let part = u32::try_from(mixed % 1_000_001).unwrap_or(u32::MAX);
            -750.0_f64 + 751.0_f64 * f64::from(part) / 1_000_000.0_f64
        });
        let output = vector_exp(simd, input);
        input
            .as_slice()
            .iter()
            .zip(output.as_slice())
            .all(|(&input, &output)| exponential_matches_libm(input, output))
    })
}

#[test]
fn vector_exponential_preserves_required_edges() {
    assert_eq!(
        vector_exp_scalar(f64::NEG_INFINITY).to_bits(),
        0.0_f64.to_bits()
    );
    assert_eq!(vector_exp_scalar(-746.0_f64).to_bits(), 0.0_f64.to_bits());
    assert!(exponential_matches_libm(
        0.0_f64,
        vector_exp_scalar(0.0_f64)
    ));
    assert!(vector_exp_scalar(f64::NAN).is_nan());
    assert!(vector_exp_scalar(f64::INFINITY).is_nan());
}

#[test]
fn vector_exponential_dense_domain_reports_maximum_error() {
    let mut maximum_absolute_error = 0.0_f64;
    let mut maximum_relative_error = 0.0_f64;
    for step in 0..=100_000_u32 {
        let input = -750.0_f64 + 751.0_f64 * f64::from(step) / 100_000.0_f64;
        let expected = input.exp();
        let actual = vector_exp_scalar(input);
        let absolute_error = (actual - expected).abs();
        maximum_absolute_error = maximum_absolute_error.max(absolute_error);
        if expected > 0.0_f64 {
            maximum_relative_error = maximum_relative_error.max(absolute_error / expected);
        }
        assert!(exponential_matches_libm(input, actual));
    }
    assert!(maximum_absolute_error <= 1.0e-12_f64);
    assert!(maximum_relative_error <= 1.0e-9_f64);
}

fn vector_exp_scalar(value: f64) -> f64 {
    dispatch!(Level::new(), simd => vector_exp_first(simd, value))
}

fn vector_exp_first<S: Simd>(simd: S, value: f64) -> f64 {
    vector_exp(simd, S::f64s::splat(simd, value)).as_slice()[0]
}

fn exponential_matches_libm(input: f64, actual: f64) -> bool {
    let expected = input.exp();
    (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Arrival(#[from] crate::ArrivalPriorError),
    #[error(transparent)]
    Grid(#[from] CapacityGridError),
    #[error(transparent)]
    Model(#[from] CapacityModelError),
    #[error(transparent)]
    Posterior(#[from] crate::PosteriorError),
    #[error(transparent)]
    Window(#[from] ResourceWindowError),
    #[error(transparent)]
    ParseFloat(#[from] ParseFloatError),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
    #[error("the captured storm evidence has an invalid format")]
    CapturedEvidence,
}
