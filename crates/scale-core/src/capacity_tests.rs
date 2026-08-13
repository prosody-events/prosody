use allocation_counter::measure;
use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{
    CAPACITY_MODEL_ARTIFACT, CapacityGrid, CapacityGridError, CapacityModelError,
    CompletionScratch, ResourceWindow, ResourceWindowError, RetainedHistory, StartWindow,
    binomial_log_probability, completion_expectation, completion_log_likelihood,
    contamination_prior, hazard_prior, log_normal_axis_masses, record_start_window,
};
use crate::ArrivalPrior;

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
        exposure_seconds: 1.0_f64,
        started_attempts: None,
    }];
    let Ok(first) = ResourceWindow::new(1.0_f64, 1.0_f64, u32::from(first_completed)) else {
        return false;
    };
    let Ok(second) = ResourceWindow::new(1.0_f64, 1.0_f64, u32::from(second_completed)) else {
        return false;
    };
    let retained = RetainedHistory {
        windows: &history,
        head: 0,
        length: 0,
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
    for index in 0_u32..180 {
        let exposure = if index % 2 == 0 { 1.0_f64 } else { 1.5_f64 };
        let starts = if index % 7 == 0 { 13 } else { 5 };
        let window = ResourceWindow::new_with_starts(1.0_f64, exposure, 5, starts)?;
        record_start_window(&mut ring, &mut ring_head, &mut ring_len, &window);
        record_start_window(
            &mut unbounded,
            &mut unbounded_head,
            &mut unbounded_len,
            &window,
        );
        let actual = completion_log_likelihood(
            &grid,
            0,
            RetainedHistory {
                windows: &ring,
                head: ring_head,
                length: ring_len,
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
    let (probabilities, weights) = contamination_prior(CAPACITY_MODEL_ARTIFACT)?;
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
        ArrivalPrior::test_artifact(),
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
    let mut previous_starts = 4;
    for index in 0_u32..180 {
        let starts = if index % 2 == 0 { 20 } else { 4 };
        let window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, previous_starts, starts)?;
        factor.update(&window);
        previous_starts = starts;
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
    let (rates, weights) = hazard_prior(1.0_f64 / 300.0_f64, 4.0_f64, CAPACITY_MODEL_ARTIFACT)?;
    assert!(rates.windows(2).all(|pair| pair[0] < pair[1]));
    assert!((weights.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    assert!(matches!(
        hazard_prior(0.0_f64, 4.0_f64, CAPACITY_MODEL_ARTIFACT),
        Err(CapacityModelError::InvalidHazardPrior)
    ));
    Ok(())
}

#[test]
fn log_normal_endpoint_cells_hold_both_tail_masses() {
    let masses = log_normal_axis_masses(&[1.0_f64, 2.0_f64, 4.0_f64], 2.0_f64, 1.0_f64);
    assert!((masses.iter().sum::<f64>() - 1.0_f64).abs() <= 16.0_f64 * f64::EPSILON);
    assert!(masses[0] > masses[1]);
    assert!((masses[0] - masses[2]).abs() <= 16.0_f64 * f64::EPSILON);
}

#[test]
fn observation_contract_sizes_history_from_coverage() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[2.0_f64], &[10.0_f64], &[0.0_f64])?;
    let factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        ArrivalPrior::test_artifact(),
        1.0_f64,
        1.0_f64,
        64,
    )?;
    assert_eq!(factor.start_history.len(), 3);
    Ok(())
}

#[test]
fn completion_convolution_update_does_not_allocate() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.5_f64], &[10.0_f64], &[0.0_f64])?;
    let mut factor = super::CapacityFactor::new_with_prior(
        grid,
        1.0_f64 / 300.0_f64,
        ArrivalPrior::test_artifact(),
        1.0_f64,
        1.0_f64,
        64,
    )?;
    let window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 32, 32)?;
    factor.update(&window);
    let allocation = measure(|| factor.update(&window));
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    Ok(())
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Grid(#[from] CapacityGridError),
    #[error(transparent)]
    Model(#[from] CapacityModelError),
    #[error(transparent)]
    Window(#[from] ResourceWindowError),
}
