use std::{array, time::Duration};

use allocation_counter::measure;
use fearless_simd::Level;
use quickcheck_macros::quickcheck;
use statrs::distribution::{ContinuousCDF, Gamma};
use thiserror::Error;

use super::{
    CAPACITY_UPDATE_OPERATION_COUNT_MAX, CapacityAllocation, CapacityGrid, CapacityGridError,
    CapacityModelError, CompletionScratch, DeathBand, ErrorLedger, HAZARD_COVERAGE_INDEX,
    HAZARD_TRANSITION_PROBABILITY_ERROR_MAX, LinearRateBand, OBSERVATION_COVERAGE_INDEX,
    OBSERVATION_PROBABILITY_ERROR_MAX, PATH_SOLVER_PROBABILITY_ERROR_MAX, ResourceWindow,
    ResourceWindowError, RetainedHistory, SOLVER_COVERAGE_INDEX, SpreadTruncation, StartWindow,
    binomial_log_probability, capacity_model_artifact, capacity_update_operation_count,
    completion_expectation, completion_log_likelihood, completion_marginal_probability,
    contamination_prior, equal_rate_death_step, feasibility_probability,
    feasibility_probability_and_charge, fold_trace, hazard_prior, linear_rate_band,
    linear_rate_death_step, log_contamination_mixture, log_normal_axis_masses, log_weighted_sum,
    path_log_score, pure_death_step, pure_death_step_with_rates, record_start_window,
    uniformized_death_step,
};
use crate::change_point::ChangePointKernel;
use crate::types::occupancy_trace_for_test;
use crate::{ArrivalPrior, OccupancyTraceEvidence};

fn state_f64(value: usize) -> f64 {
    u32::try_from(value).map_or(f64::from(u32::MAX), f64::from)
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
        .chain(&factor.filter_curve_weights)
        .map(|value| value.to_bits())
        .collect()
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
    let total = (0..=trials as usize)
        .map(|count| binomial_log_probability(trials, count, probability).exp())
        .sum::<f64>();
    let operation_count = f64::from(trials + 1).powi(2);
    (total - 1.0_f64).abs() <= operation_count * 1_024.0_f64 * f64::EPSILON
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
                coefficients: &mut ring_coefficients,
                convolution: &mut ring_convolution,
                binomial: &mut ring_binomial,
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
                coefficients: &mut reference_coefficients,
                convolution: &mut reference_convolution,
                binomial: &mut reference_binomial,
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
    let artifact = capacity_model_artifact(1.0_f64 / 300.0_f64, 4.0_f64)?;
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
    assert_eq!(
        artifact.coverage[SOLVER_COVERAGE_INDEX]
            .decision_cost_error()
            .to_bits(),
        PATH_SOLVER_PROBABILITY_ERROR_MAX.to_bits()
    );
    let (rates, weights) = hazard_prior(&artifact)?;
    assert!(rates.windows(2).all(|pair| pair[0] < pair[1]));
    assert!((weights.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    assert!(matches!(
        capacity_model_artifact(0.0_f64, 4.0_f64),
        Err(CapacityModelError::InvalidHazardPrior)
    ));
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
fn finite_grid_oracle_matches_joint_normalization_and_filters() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64, 1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid.clone(),
        1.0_f64 / 300.0_f64,
        &ArrivalPrior::test_artifact()?,
        1.0_f64,
        1.0_f64,
        8,
    )?;
    let no_completion = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 0, 0)?;
    let empty: [u64; 0] = [];
    let empty_counts: [u32; 0] = [];
    let no_completion_evidence = occupancy_trace_for_test(
        no_completion,
        1,
        1,
        1_000_000,
        &empty,
        &empty_counts,
        &empty_counts,
    );
    let completion = ResourceWindow::new_with_starts(0.25_f64, 1.0_f64, 1, 0)?;
    let offsets = [250_000_u64];
    let completed = [1_u32];
    let started = [0_u32];
    let completion_evidence =
        occupancy_trace_for_test(completion, 1, 0, 250_000, &offsets, &completed, &started);
    let mut probabilities = [0.0_f64; 2];
    let mut coefficients = [0.0_f64; 2];
    let mut work = [0.0_f64; 2];
    for index in 0..grid.cell_count() as usize {
        let rate = super::state_rate(&grid, index, 1);
        let zero = completion_marginal_probability(
            &grid,
            index,
            no_completion_evidence,
            &mut probabilities,
            &mut coefficients,
            &mut work,
        )
        .ok_or(CapacityModelError::InvalidObservationContract)?;
        let one = completion_marginal_probability(
            &grid,
            index,
            completion_evidence,
            &mut probabilities,
            &mut coefficients,
            &mut work,
        )
        .ok_or(CapacityModelError::InvalidObservationContract)?;
        let operation_bound = 256.0_f64 * f64::EPSILON;
        assert!((zero - (-rate).exp()).abs() <= operation_bound);
        assert!((zero + one - 1.0_f64).abs() <= operation_bound);
        let conditional_integral = (1.0_f64 - (-rate).exp()) / one;
        assert!((conditional_integral - 1.0_f64).abs() <= operation_bound);
        let mut exposures = [0.0_f64; 2];
        let mut completion_counts = [0_u32; 2];
        fold_trace(completion_evidence, &mut exposures, &mut completion_counts);
        let path = path_log_score(&grid, index, &exposures, &completion_counts);
        let oracle_path = rate.ln() - rate * 0.25_f64;
        assert!((path - oracle_path).abs() <= operation_bound);

        let conditioned_window = ResourceWindow::new_with_starts(0.75_f64, 1.0_f64, 1, 1)?;
        let conditioned_offsets = [250_000_u64, 500_000_u64];
        let conditioned_completed = [1_u32, 0_u32];
        let conditioned_started = [0_u32, 1_u32];
        let conditioned = occupancy_trace_for_test(
            conditioned_window,
            1,
            1,
            750_000,
            &conditioned_offsets,
            &conditioned_completed,
            &conditioned_started,
        );
        let normalizer = feasibility_probability(
            &grid,
            index,
            conditioned,
            &mut probabilities,
            &mut coefficients,
            &mut work,
        )
        .ok_or(CapacityModelError::InvalidObservationContract)?;
        let expected_normalizer = 1.0_f64 - (-rate * 0.5_f64).exp();
        assert!((normalizer - expected_normalizer).abs() <= operation_bound);
        let conditioned_marginal = completion_marginal_probability(
            &grid,
            index,
            conditioned,
            &mut probabilities,
            &mut coefficients,
            &mut work,
        )
        .ok_or(CapacityModelError::InvalidObservationContract)?;
        assert!((conditioned_marginal - (-rate * 0.5_f64).exp()).abs() <= operation_bound);
        let conditional_path_integral = expected_normalizer / normalizer;
        assert!((conditional_path_integral - 1.0_f64).abs() <= operation_bound);
    }

    assert_contamination_filter_parity(&mut factor, &grid, completion_evidence);
    Ok(())
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

#[quickcheck]
fn finite_grid_oracle_covers_random_valid_transition_paths(
    state_code: u8,
    service_code: u8,
    event_codes: [u8; 8],
) -> bool {
    const STATE_COUNT: usize = 4;
    let initial = usize::from(state_code) % STATE_COUNT;
    let service = 0.5_f64 + f64::from(service_code) / 255.0_f64 * 1.5_f64;
    let Ok(grid) = CapacityGrid::new(&[service], &[100.0_f64], &[0.0_f64]) else {
        return false;
    };
    let mut state = initial;
    let mut busy_slot_micros = 0_u128;
    let mut previous_offset = 0_u64;
    let mut offsets = Vec::with_capacity(event_codes.len());
    let mut completed = Vec::with_capacity(event_codes.len());
    let mut started = Vec::with_capacity(event_codes.len());
    for (index, code) in event_codes.into_iter().enumerate() {
        let offset = (index as u64 + 1) * 100_000;
        busy_slot_micros += u128::from(offset - previous_offset) * state as u128;
        let completion_count = u32::from(code & 1 == 0 && state > 0);
        state -= completion_count as usize;
        let start_count = u32::from(code & 2 != 0 && state + 1 < STATE_COUNT);
        state += start_count as usize;
        offsets.push(offset);
        completed.push(completion_count);
        started.push(start_count);
        previous_offset = offset;
    }
    busy_slot_micros += u128::from(1_000_000 - previous_offset) * state as u128;
    let completion_count = completed.iter().copied().sum::<u32>();
    let start_count = started.iter().copied().sum::<u32>();
    let Ok(busy_slot_micros_u64) = u64::try_from(busy_slot_micros) else {
        return false;
    };
    let Ok(window) = ResourceWindow::new_with_starts(
        Duration::from_micros(busy_slot_micros_u64).as_secs_f64(),
        1.0_f64,
        completion_count,
        start_count,
    ) else {
        return false;
    };
    let evidence = occupancy_trace_for_test(
        window,
        initial as u32,
        state as u32,
        busy_slot_micros,
        &offsets,
        &completed,
        &started,
    );
    let rates = array::from_fn(|busy| super::state_rate(&grid, 0, busy));
    let (oracle_normalizer, oracle_final, operation_count) =
        oracle_forward(&rates, initial, &offsets, &started);
    let mut probabilities = [0.0_f64; STATE_COUNT];
    let mut coefficients = [0.0_f64; STATE_COUNT];
    let mut work = [0.0_f64; STATE_COUNT];
    let Some(normalizer) = feasibility_probability(
        &grid,
        0,
        evidence,
        &mut probabilities,
        &mut coefficients,
        &mut work,
    ) else {
        return false;
    };
    let Some(marginal) = completion_marginal_probability(
        &grid,
        0,
        evidence,
        &mut probabilities,
        &mut coefficients,
        &mut work,
    ) else {
        return false;
    };
    let tolerance =
        Duration::from_micros(operation_count).as_secs_f64() * 64_000_000.0_f64 * f64::EPSILON
            + 1.0e-10_f64;
    let oracle_marginal = oracle_final[state] / oracle_normalizer;
    let normalized_marginals = oracle_final.iter().sum::<f64>() / oracle_normalizer;
    (normalizer - oracle_normalizer).abs() <= tolerance
        && (marginal - oracle_marginal).abs() <= tolerance
        && (normalized_marginals - 1.0_f64).abs() <= tolerance
        && (oracle_final[state] / (oracle_normalizer * oracle_marginal) - 1.0_f64).abs()
            <= tolerance
}

#[test]
fn pure_death_band_keeps_states_when_remaining_starts_exceed_the_bound() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64], &[100.0_f64], &[0.0_f64])?;
    let offsets = [
        100_000_u64,
        200_000,
        300_000,
        400_000,
        500_000,
        600_000,
        700_000,
        800_000,
    ];
    let completed = [0_u32, 0, 1, 0, 0, 0, 1, 1];
    let started = [0_u32, 1, 0, 1, 1, 1, 0, 0];
    let mut state = 0_u32;
    let mut previous_offset = 0_u64;
    let mut busy_slot_micros = 0_u128;
    for ((&offset, &completion_count), &start_count) in offsets.iter().zip(&completed).zip(&started)
    {
        busy_slot_micros += u128::from(offset - previous_offset) * u128::from(state);
        state = state - completion_count + start_count;
        previous_offset = offset;
    }
    busy_slot_micros += u128::from(1_000_000 - previous_offset) * u128::from(state);
    let busy_slot_micros_u64 =
        u64::try_from(busy_slot_micros).map_err(|_| CapacityModelError::StorageBound)?;
    let window = ResourceWindow::new_with_starts(
        Duration::from_micros(busy_slot_micros_u64).as_secs_f64(),
        1.0_f64,
        completed.iter().sum(),
        started.iter().sum(),
    )?;
    let evidence = occupancy_trace_for_test(
        window,
        0,
        state,
        busy_slot_micros,
        &offsets,
        &completed,
        &started,
    );
    let rates = array::from_fn(|busy| super::state_rate(&grid, 0, busy));
    let (oracle, _, operation_count) = oracle_forward(&rates, 0, &offsets, &started);
    let mut probabilities = [0.0_f64; 4];
    let mut coefficients = [0.0_f64; 4];
    let mut work = [0.0_f64; 4];
    let actual = feasibility_probability(
        &grid,
        0,
        evidence,
        &mut probabilities,
        &mut coefficients,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    let tolerance =
        Duration::from_micros(operation_count).as_secs_f64() * 64_000_000.0_f64 * f64::EPSILON
            + 1.0e-10_f64;
    assert!((actual - oracle).abs() <= tolerance);
    Ok(())
}

#[test]
fn equal_and_mixed_rate_death_bands_match_the_erlang_limit() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.1_f64], &[1.0_f64], &[0.0_f64])?;
    let expected = (-1.0_f64).exp();
    let mut probabilities = [0.0_f64; 4];
    let mut coefficients = [0.0_f64; 4];
    let mut work = [0.0_f64; 4];
    probabilities[3] = 1.0_f64;
    pure_death_step(
        &grid,
        0,
        DeathBand {
            low: 1,
            high: 3,
            exposure_seconds: 1.0_f64,
        },
        &mut probabilities,
        &mut coefficients,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    let tolerance = 256.0_f64 * f64::EPSILON;
    assert!((probabilities[3] - expected).abs() <= tolerance);
    assert!((probabilities[2] - expected).abs() <= tolerance);
    assert!((probabilities[1] - expected * 0.5_f64).abs() <= tolerance);

    probabilities.fill(0.0_f64);
    probabilities[3] = 1.0_f64;
    pure_death_step(
        &grid,
        0,
        DeathBand {
            low: 0,
            high: 3,
            exposure_seconds: 1.0_f64,
        },
        &mut probabilities,
        &mut coefficients,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    let fallback_tolerance = 1.0e-10_f64 + 4_096.0_f64 * f64::EPSILON;
    assert!((probabilities[3] - expected).abs() <= fallback_tolerance);
    assert!((probabilities[2] - expected).abs() <= fallback_tolerance);
    assert!((probabilities[1] - expected * 0.5_f64).abs() <= fallback_tolerance);
    assert!((probabilities.iter().sum::<f64>() - 1.0_f64).abs() <= fallback_tolerance);
    Ok(())
}

#[test]
fn equal_rate_erlang_matches_uniformization_within_its_charge() -> Result<(), TestError> {
    let rates = [0.0_f64, 2.0_f64, 2.0_f64, 2.0_f64];
    let band = DeathBand {
        low: 1,
        high: 3,
        exposure_seconds: 0.75_f64,
    };
    let mut erlang = [0.0_f64, 0.2_f64, 0.3_f64, 0.5_f64];
    let mut uniform = erlang;
    let mut erlang_coefficients = [0.0_f64; 4];
    let mut erlang_work = [0.0_f64; 4];
    let mut uniform_current = [0.0_f64; 4];
    let mut uniform_next = [0.0_f64; 4];
    let mut ledger = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX)
        .ok_or(CapacityModelError::StorageBound)?;
    pure_death_step(
        &CapacityGrid::new(&[0.5_f64], &[2.0_f64], &[0.0_f64])?,
        0,
        band,
        &mut erlang,
        &mut erlang_coefficients,
        &mut erlang_work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    uniformized_death_step(
        &rates,
        band,
        &mut uniform,
        &mut uniform_current,
        &mut uniform_next,
        &mut ledger,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    assert!(
        erlang
            .iter()
            .zip(uniform)
            .map(|(left, right)| (left - right).abs())
            .sum::<f64>()
            <= ledger.charged + 512.0_f64 * f64::EPSILON
    );
    Ok(())
}

#[quickcheck]
fn equal_rate_erlang_matches_enumeration_on_random_bands(
    low_code: u8,
    width_code: u8,
    rate_code: u8,
    exposure_code: u8,
) -> bool {
    const STATE_COUNT: usize = 9;
    let low = usize::from(low_code) % 5;
    let high = low + usize::from(width_code) % (STATE_COUNT - low);
    let rate = (f64::from(rate_code) + 1.0_f64) / 16.0_f64;
    let exposure_seconds = (f64::from(exposure_code) + 1.0_f64) / 64.0_f64;
    let band = DeathBand {
        low,
        high,
        exposure_seconds,
    };
    let source_count = high - low + 1;
    let mut erlang = [0.0_f64; STATE_COUNT];
    for probability in &mut erlang[low..=high] {
        *probability = 1.0_f64 / state_f64(source_count);
    }
    let mut uniform = erlang;
    let mut log_factorials = [0.0_f64; STATE_COUNT];
    let mut work = [0.0_f64; STATE_COUNT];
    if equal_rate_death_step(
        rate,
        low,
        high,
        exposure_seconds,
        &mut erlang,
        &mut log_factorials,
        &mut work,
    )
    .is_none()
    {
        return false;
    }
    let rates: [f64; STATE_COUNT] =
        array::from_fn(|state| if state >= low { rate } else { 0.0_f64 });
    let mut current = [0.0_f64; STATE_COUNT];
    let mut next = [0.0_f64; STATE_COUNT];
    let Some(mut ledger) = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX) else {
        return false;
    };
    if uniformized_death_step(
        &rates,
        band,
        &mut uniform,
        &mut current,
        &mut next,
        &mut ledger,
    )
    .is_none()
    {
        return false;
    }
    erlang
        .iter()
        .zip(uniform)
        .map(|(left, right)| (left - right).abs())
        .sum::<f64>()
        <= ledger.charged + 4_096.0_f64 * f64::EPSILON
}

#[test]
fn equal_rate_erlang_retains_large_mean_mass() -> Result<(), TestError> {
    const STATE_COUNT: usize = 1_002;
    let mut probabilities = vec![0.0_f64; STATE_COUNT];
    probabilities[STATE_COUNT - 1] = 1.0_f64;
    let mut log_factorials = vec![0.0_f64; STATE_COUNT];
    let mut work = vec![0.0_f64; STATE_COUNT];
    equal_rate_death_step(
        800.0_f64,
        1,
        STATE_COUNT - 1,
        1.0_f64,
        &mut probabilities,
        &mut log_factorials,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    assert!(probabilities[1..].iter().sum::<f64>() > 0.99_f64);
    Ok(())
}

#[test]
fn multi_group_window_charges_at_most_the_path_budget() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64], &[1.5_f64], &[1.0_f64])?;
    let rates: [f64; 9] = array::from_fn(|state| super::state_rate(&grid, 0, state));
    let window = ResourceWindow::new_with_starts(2.4_f64, 1.2_f64, 0, 6)?;
    let offsets = [300_000_u64, 700_000, 1_000_000];
    let completed = [0_u32; 3];
    let started = [2_u32; 3];
    let evidence =
        occupancy_trace_for_test(window, 2, 8, 2_400_000, &offsets, &completed, &started);
    let mut probabilities = [0.0_f64; 9];
    let mut coefficients = [0.0_f64; 9];
    let mut work = [0.0_f64; 9];
    let (_, charged) = feasibility_probability_and_charge(
        &rates,
        linear_rate_band(&grid, 0),
        evidence,
        &mut probabilities,
        &mut coefficients,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    assert!(charged <= PATH_SOLVER_PROBABILITY_ERROR_MAX);
    Ok(())
}

#[test]
fn band_contraction_matches_the_finite_grid_oracle_within_its_charge() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.05_f64], &[100.0_f64], &[0.0_f64])?;
    let rates = array::from_fn(|state| super::state_rate(&grid, 0, state));
    let offsets = [1_000_000_u64];
    let started = [3_u32];
    let completed = [0_u32];
    let window = ResourceWindow::new_with_starts(3.0_f64, 1.0_f64, 0, 3)?;
    let evidence =
        occupancy_trace_for_test(window, 3, 6, 3_000_000, &offsets, &completed, &started);
    let (oracle, _, operation_count) = oracle_forward(&rates, 3, &offsets, &started);
    let mut probabilities = [0.0_f64; 4];
    let mut coefficients = [0.0_f64; 4];
    let mut work = [0.0_f64; 4];
    let (actual, charged) = feasibility_probability_and_charge(
        &rates,
        linear_rate_band(&grid, 0),
        evidence,
        &mut probabilities,
        &mut coefficients,
        &mut work,
    )
    .ok_or(CapacityModelError::InvalidObservationContract)?;
    let oracle_error =
        Duration::from_micros(operation_count).as_secs_f64() * 64_000_000.0_f64 * f64::EPSILON;
    assert!(charged > 0.0_f64);
    assert!((actual - oracle).abs() <= charged + oracle_error);
    Ok(())
}

#[quickcheck]
fn linear_rate_kernel_matches_enumeration_and_uniformization(
    low_code: u8,
    width_code: u8,
    exposure_code: u8,
) -> bool {
    const STATE_COUNT: usize = 9;
    let low = usize::from(low_code) % 5;
    let high = low + usize::from(width_code) % (STATE_COUNT - low);
    let exposure_seconds = (f64::from(exposure_code) + 1.0_f64) / 64.0_f64;
    let service_time_seconds = 0.5_f64;
    let survival = (-exposure_seconds / service_time_seconds).exp();
    let band = DeathBand {
        low,
        high,
        exposure_seconds,
    };
    let mut initial = [0.0_f64; STATE_COUNT];
    let source_count = high - low + 1;
    for probability in &mut initial[low..=high] {
        *probability = 1.0_f64 / state_f64(source_count);
    }
    let mut oracle = [0.0_f64; STATE_COUNT];
    for (source, weight) in initial.iter().enumerate().take(high + 1).skip(low) {
        for mask in 0_usize..1_usize << source {
            let survivors = mask.count_ones() as usize;
            if survivors >= low {
                oracle[survivors] += weight
                    * survival.powi(survivors as i32)
                    * (1.0_f64 - survival).powi((source - survivors) as i32);
            }
        }
    }
    let mut exact = initial;
    let mut exact_work = [0.0_f64; STATE_COUNT];
    let Some(mut exact_ledger) = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX)
    else {
        return false;
    };
    if linear_rate_death_step(
        service_time_seconds,
        band,
        &mut exact,
        &mut exact_work,
        SpreadTruncation::Disabled,
        &mut exact_ledger,
    )
    .is_none()
    {
        return false;
    }
    let mut linear = initial;
    let mut linear_work = [0.0_f64; STATE_COUNT];
    let mut linear_ledger = exact_ledger;
    let source_charge = linear_ledger.group_budget / state_f64(source_count + 1);
    if linear_rate_death_step(
        service_time_seconds,
        band,
        &mut linear,
        &mut linear_work,
        SpreadTruncation::Charged(source_charge),
        &mut linear_ledger,
    )
    .is_none()
    {
        return false;
    }
    let rates: [f64; STATE_COUNT] = array::from_fn(|state| state_f64(state) / service_time_seconds);
    let mut uniform = initial;
    let mut current = [0.0_f64; STATE_COUNT];
    let mut next = [0.0_f64; STATE_COUNT];
    let mut uniform_ledger = exact_ledger;
    if uniformized_death_step(
        &rates,
        band,
        &mut uniform,
        &mut current,
        &mut next,
        &mut uniform_ledger,
    )
    .is_none()
    {
        return false;
    }
    let roundoff = 4_096.0_f64 * f64::EPSILON;
    exact
        .iter()
        .zip(oracle)
        .all(|(actual, expected)| (actual - expected).abs() <= roundoff)
        && linear
            .iter()
            .zip(uniform)
            .map(|(left, right)| (left - right).abs())
            .sum::<f64>()
            <= linear_ledger.charged + uniform_ledger.charged + roundoff
}

#[quickcheck]
fn composed_death_kernel_matches_full_uniformization(
    structure_code: u8,
    low_code: u8,
    width_code: u8,
    exposure_code: u8,
) -> bool {
    const STATE_COUNT: usize = 9;
    let low = usize::from(low_code) % 5;
    let high = low + usize::from(width_code) % (STATE_COUNT - low);
    let exposure_seconds = (f64::from(exposure_code) + 1.0_f64) / 128.0_f64;
    let structure = structure_code % 3;
    let linear_state_max = match structure {
        0 => usize::MAX,
        1 => 0,
        _ => 3,
    };
    let rates: [f64; STATE_COUNT] = array::from_fn(|state| match structure {
        0 => state_f64(state) / 0.5_f64,
        1 if state > 0 => 2.0_f64,
        1 => 0.0_f64,
        _ if state <= linear_state_max => state_f64(state) / 0.5_f64,
        _ => 8.0_f64 / (1.0_f64 + 0.25_f64 * state_f64(state - linear_state_max)),
    });
    let band = DeathBand {
        low,
        high,
        exposure_seconds,
    };
    let source_count = high - low + 1;
    let mut composed = [0.0_f64; STATE_COUNT];
    for probability in &mut composed[low..=high] {
        *probability = 1.0_f64 / state_f64(source_count);
    }
    let mut uniform = composed;
    let mut coefficients = [0.0_f64; STATE_COUNT];
    let mut work = [0.0_f64; STATE_COUNT];
    let mut current = [0.0_f64; STATE_COUNT];
    let mut next = [0.0_f64; STATE_COUNT];
    let Some(mut composed_ledger) = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX)
    else {
        return false;
    };
    let Some(mut uniform_ledger) = ErrorLedger::with_budget(1, PATH_SOLVER_PROBABILITY_ERROR_MAX)
    else {
        return false;
    };
    if pure_death_step_with_rates(
        &rates,
        LinearRateBand {
            service_time_seconds: 0.5_f64,
            state_max: linear_state_max,
        },
        band,
        &mut composed,
        &mut coefficients,
        &mut work,
        &mut composed_ledger,
    )
    .is_none()
        || uniformized_death_step(
            &rates,
            band,
            &mut uniform,
            &mut current,
            &mut next,
            &mut uniform_ledger,
        )
        .is_none()
    {
        return false;
    }
    composed
        .iter()
        .zip(uniform)
        .map(|(left, right)| (left - right).abs())
        .sum::<f64>()
        <= composed_ledger.charged + uniform_ledger.charged + 8_192.0_f64 * f64::EPSILON
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
    let prices = [
        operation_price_for_test(&general, 257, 256, 0.1_f64)?,
        operation_price_for_test(&capacity, 193, 256, 1.0_f64)?,
        operation_price_for_test(&capacity, 65, 256, 1.0_f64)?,
        operation_price_for_test(&general, 1_537, 64, 1.0_f64)?,
        operation_price_for_test(&general, 257, 64, 1.0_f64)?,
        operation_price_for_test(&general, 65, 256, 1.0_f64)?,
        operation_price_for_test(&general, 65, 256, 1.0_f64)?,
        operation_price_for_test(&general, 257, 256, 0.1_f64)?,
        operation_price_for_test(&general, 257, 128, 0.5_f64)?,
        operation_price_for_test(&general, 257, 256, 0.1_f64)?,
        operation_price_for_test(&historical, 257, 256, 1.0_f64)?,
        operation_price_for_test(&historical, 449, 128, 1.0_f64)?,
        operation_price_for_test(&historical, 129, 256, 1.0_f64)?,
    ];
    assert_eq!(
        prices,
        [
            2_237_473_359,
            427_368_773,
            78_434_517,
            158_367_561_327,
            10_371_321_103,
            534_994_351,
            534_994_351,
            2_237_473_359,
            5_607_910_655,
            2_237_473_359,
            583_396_611,
            670_481_283,
            207_848_451,
        ]
    );
    let maximum = prices
        .into_iter()
        .max()
        .ok_or(CapacityModelError::StorageBound)?;
    assert_eq!(CAPACITY_UPDATE_OPERATION_COUNT_MAX, 400_000_000_000);
    assert!(maximum.saturating_mul(2) <= CAPACITY_UPDATE_OPERATION_COUNT_MAX);
    Ok(())
}

fn operation_price_for_test(
    grid: &CapacityGrid,
    state_count: usize,
    group_count: usize,
    exposure_seconds: f64,
) -> Result<u64, CapacityModelError> {
    let cell_count = grid.service_times_seconds.len();
    capacity_update_operation_count(
        grid,
        CapacityAllocation {
            cell_count,
            state_count,
            filter_count: 160,
            filter_curve_count: 160 * cell_count,
            transition_count: 100_001,
            start_history_capacity: 1,
            group_count,
        },
        exposure_seconds,
    )
    .ok_or(CapacityModelError::StorageBound)
}

fn oracle_forward(
    rates: &[f64; 4],
    initial: usize,
    offsets: &[u64],
    starts: &[u32],
) -> (f64, [f64; 4], u64) {
    let mut probabilities = [0.0_f64; 4];
    probabilities[initial] = 1.0_f64;
    let mut previous_offset = 0_u64;
    let mut operation_count = 0_u64;
    for (&offset, &start_count) in offsets.iter().zip(starts) {
        if start_count == 0 {
            continue;
        }
        operation_count += oracle_death_step(
            rates,
            Duration::from_micros(offset - previous_offset).as_secs_f64(),
            &mut probabilities,
        );
        for state in (0..probabilities.len()).rev() {
            let shifted = state + start_count as usize;
            if shifted < probabilities.len() {
                probabilities[shifted] = probabilities[state];
            }
            probabilities[state] = 0.0_f64;
        }
        previous_offset = offset;
    }
    operation_count += oracle_death_step(
        rates,
        Duration::from_micros(1_000_000 - previous_offset).as_secs_f64(),
        &mut probabilities,
    );
    (probabilities.iter().sum(), probabilities, operation_count)
}

fn oracle_death_step(rates: &[f64; 4], exposure_seconds: f64, probabilities: &mut [f64; 4]) -> u64 {
    let rate = rates.iter().copied().fold(0.0_f64, f64::max);
    if rate == 0.0_f64 || exposure_seconds == 0.0_f64 {
        return 1;
    }
    let mean = rate * exposure_seconds;
    let mut current = *probabilities;
    let mut result = [0.0_f64; 4];
    let mut poisson = (-mean).exp();
    let mut cumulative = poisson;
    for state in 0..4 {
        result[state] = poisson * current[state];
    }
    let mut term = 0_u32;
    while 1.0_f64 - cumulative > 1.0e-12_f64 {
        let mut next = [0.0_f64; 4];
        for state in 0..4 {
            let death = rates[state] / rate;
            next[state] += current[state] * (1.0_f64 - death);
            if state > 0 {
                next[state - 1] += current[state] * death;
            }
        }
        current = next;
        term += 1;
        poisson *= mean / f64::from(term);
        cumulative += poisson;
        for state in 0..4 {
            result[state] += poisson * current[state];
        }
    }
    *probabilities = result;
    u64::from(term).saturating_mul(16)
}

const ABLATION_ARM_COUNT: usize = 5;

struct AblationScratch {
    completion_coefficients: [f64; 3],
    completion_convolution: [f64; 3],
    completion_binomial: [f64; 3],
    forward: [f64; 2],
    forward_coefficients: [f64; 2],
    forward_work: [f64; 2],
}

impl AblationScratch {
    const fn new() -> Self {
        Self {
            completion_coefficients: [0.0_f64; 3],
            completion_convolution: [0.0_f64; 3],
            completion_binomial: [0.0_f64; 3],
            forward: [0.0_f64; 2],
            forward_coefficients: [0.0_f64; 2],
            forward_work: [0.0_f64; 2],
        }
    }
}

#[derive(Clone, Copy)]
struct AblationWindow<'a> {
    window: &'a ResourceWindow,
    evidence: OccupancyTraceEvidence<'a>,
    busy_seconds: f64,
    observed_completion: u32,
    offset_micros: u64,
}

fn ablation_likelihoods(
    grid: &CapacityGrid,
    scratch: &mut AblationScratch,
    cell_indexes: [usize; 2],
    observation: AblationWindow<'_>,
) -> Result<[[f64; 2]; ABLATION_ARM_COUNT], TestError> {
    let history = [StartWindow {
        end_micros: 1_000_000,
        exposure_seconds: 1.0_f64,
        started_attempts: Some(1),
    }];
    let mut likelihoods = [[0.0_f64; 2]; ABLATION_ARM_COUNT];
    for candidate in 0..2 {
        let index = cell_indexes[candidate];
        let landed = completion_log_likelihood(
            grid,
            index,
            RetainedHistory {
                windows: &history,
                head: 0,
                length: 1,
                end_micros: 1_000_000,
            },
            observation.window,
            1.0_f64,
            1.0_f64,
            CompletionScratch {
                coefficients: &mut scratch.completion_coefficients,
                convolution: &mut scratch.completion_convolution,
                binomial: &mut scratch.completion_binomial,
            },
        );
        let rate = super::state_rate(grid, index, 1);
        let old_kernel = deleted_poisson_log_kernel(
            observation.busy_seconds,
            deleted_in_flight_mean(1.0_f64, 1.0_f64 / rate),
        );
        let marginal = completion_marginal_probability(
            grid,
            index,
            observation.evidence,
            &mut scratch.forward,
            &mut scratch.forward_coefficients,
            &mut scratch.forward_work,
        )
        .ok_or(CapacityModelError::InvalidObservationContract)?
        .ln();
        let mut exposures = [0.0_f64; 2];
        let mut completion_counts = [0_u32; 2];
        fold_trace(observation.evidence, &mut exposures, &mut completion_counts);
        let joint = path_log_score(grid, index, &exposures, &completion_counts);
        let oracle = if observation.observed_completion == 0 {
            -rate
        } else {
            rate.ln() - rate * Duration::from_micros(observation.offset_micros).as_secs_f64()
        };
        likelihoods[0][candidate] = landed;
        likelihoods[1][candidate] = landed + old_kernel;
        likelihoods[2][candidate] = marginal;
        likelihoods[3][candidate] = joint;
        likelihoods[4][candidate] = oracle;
    }
    Ok(likelihoods)
}

#[test]
fn five_arm_double_counting_and_alternating_ablation_match_the_oracle() -> Result<(), TestError> {
    const WINDOW_COUNT: u32 = 180;
    let grid = CapacityGrid::new(&[0.25_f64, 1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let cell_indexes = [1_usize, 0_usize];
    let mut weights = [[0.5_f64; 2]; ABLATION_ARM_COUNT];
    let mut score = [0.0_f64; ABLATION_ARM_COUNT];
    let mut entropy = [0.0_f64; ABLATION_ARM_COUNT];
    let mut covered = [0_u32; ABLATION_ARM_COUNT];
    let mut rank = [0.0_f64; ABLATION_ARM_COUNT];
    let mut scratch = AblationScratch::new();
    let mut legitimate_completion_gain = 0.0_f64;
    let mut duplicated_inflation = 0.0_f64;
    for window_index in 0..WINDOW_COUNT {
        let generating_cell = (window_index % 2) as usize;
        let rate = super::state_rate(&grid, cell_indexes[generating_cell], 1);
        let stratum = f64::from((window_index * 73) % WINDOW_COUNT) + 0.5_f64;
        let uniform = stratum / f64::from(WINDOW_COUNT);
        let completion_time = -(-uniform).ln_1p() / rate;
        let observed_completion = u32::from(completion_time < 1.0_f64);
        let busy_seconds = completion_time.min(1.0_f64);
        let window =
            ResourceWindow::new_with_starts(busy_seconds, 1.0_f64, observed_completion, 0)?;
        let offset = [(busy_seconds * 1_000_000.0_f64).round() as u64];
        let completed = [1_u32];
        let started = [0_u32];
        let empty_offsets: [u64; 0] = [];
        let empty_counts: [u32; 0] = [];
        let evidence = if observed_completion == 0 {
            occupancy_trace_for_test(
                window,
                1,
                1,
                1_000_000,
                &empty_offsets,
                &empty_counts,
                &empty_counts,
            )
        } else {
            occupancy_trace_for_test(
                window,
                1,
                0,
                u128::from(offset[0]),
                &offset,
                &completed,
                &started,
            )
        };
        let arm_likelihoods = ablation_likelihoods(
            &grid,
            &mut scratch,
            cell_indexes,
            AblationWindow {
                window: &window,
                evidence,
                busy_seconds,
                observed_completion,
                offset_micros: offset[0],
            },
        )?;
        let completion_predictive =
            |arm: usize| log_weighted_sum(&weights[arm], &arm_likelihoods[2]);
        legitimate_completion_gain += completion_predictive(4) - completion_predictive(2);
        duplicated_inflation += completion_predictive(1) - completion_predictive(4);
        for arm in 0..ABLATION_ARM_COUNT {
            let predictive = log_weighted_sum(&weights[arm], &arm_likelihoods[arm]);
            score[arm] += predictive;
            for candidate in 0..2 {
                weights[arm][candidate] *= (arm_likelihoods[arm][candidate] - predictive).exp();
            }
            let total = weights[arm].iter().sum::<f64>();
            for weight in &mut weights[arm] {
                *weight /= total;
            }
            entropy[arm] += weights[arm]
                .iter()
                .filter(|weight| **weight > 0.0_f64)
                .map(|weight| -weight * weight.ln())
                .sum::<f64>();
            let generating_weight = weights[arm][generating_cell];
            rank[arm] += f64::from(u8::from(
                weights[arm][1 - generating_cell] > generating_weight,
            ));
            covered[arm] = covered[arm].saturating_add(u32::from(
                generating_weight >= 0.05_f64 || weights[arm][1 - generating_cell] < 0.95_f64,
            ));
        }
    }
    let operation_bound = f64::from(WINDOW_COUNT) * 512.0_f64 * f64::EPSILON;
    assert!((score[3] - score[4]).abs() <= operation_bound);
    assert!((entropy[3] - entropy[4]).abs() <= operation_bound);
    assert!((rank[3] - rank[4]).abs() <= operation_bound);
    assert_eq!(covered[3], covered[4]);
    assert!(covered[1] < covered[4]);
    assert!(entropy[1] < entropy[4]);
    assert!((legitimate_completion_gain - -2.543_012_689_671_795_3_f64).abs() <= operation_bound);
    assert!((duplicated_inflation - 1.352_425_081_789_762_7_f64).abs() <= operation_bound);
    Ok(())
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
        if actual.to_bits()
            != factor
                .completion_predictive_cdf(&window, count as u32)
                .to_bits()
        {
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
        && summary.lower.to_bits() == reference_lower.to_bits()
        && summary.upper.to_bits() == reference_upper.to_bits()
        && rank.to_bits() == reference_rank.to_bits()
}

fn deleted_poisson_log_kernel(count: f64, mean: f64) -> f64 {
    if mean > 0.0_f64 {
        count * mean.ln() - mean
    } else if count == 0.0_f64 {
        0.0_f64
    } else {
        f64::NEG_INFINITY
    }
}

fn deleted_in_flight_mean(exposure_seconds: f64, service_time_seconds: f64) -> f64 {
    let age = exposure_seconds;
    let rising_end = exposure_seconds.min(service_time_seconds);
    let plateau_end = exposure_seconds.max(service_time_seconds);
    let falling_end = exposure_seconds + service_time_seconds;
    let integral = |value: f64| {
        let rising = value.min(rising_end);
        let plateau = (value.min(plateau_end) - rising_end).max(0.0_f64);
        let falling = (value.min(falling_end) - plateau_end).max(0.0_f64);
        rising * rising * 0.5_f64 + plateau * rising_end + falling * rising_end
            - falling * falling * 0.5_f64
    };
    (integral(age + exposure_seconds) - integral(age)) / exposure_seconds
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
}
