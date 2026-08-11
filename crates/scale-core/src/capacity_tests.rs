use fearless_simd::Level;
use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{CapacityFactor, CapacityGrid, CapacityGridError, ResourceWindow, ResourceWindowError};

const ERROR_ALLOWANCE: f64 = 0.01_f64;
const INFLUENCE_BOUND_PROBABILITY: f64 = 0.05_f64;

#[test]
fn covering_capacity_families_converge_to_their_prior_odds() -> Result<(), TestError> {
    let grid = CapacityGrid::new(
        &[0.001_f64, 0.002_f64, 0.01_f64],
        &[500.0_f64, 2_000.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let window = ResourceWindow::new(3.0_f64, 0.1_f64, 100)?;
    let covering = (0..grid.service_times_seconds.len())
        .filter(|&index| {
            let (low, high) = grid.throughput_interval(index, window.concurrency);
            low * window.exposure_seconds <= f64::from(window.completed_attempts)
                && f64::from(window.completed_attempts) <= high * window.exposure_seconds
        })
        .collect::<Vec<_>>();
    let covering_no_knee_prior = covering
        .iter()
        .filter(|&&index| grid.no_knee[index] > 0.0_f64)
        .map(|&index| CapacityFactor::new(grid.clone(), 0.0_f64).prior_weights[index])
        .sum::<f64>();
    let covering_prior = covering
        .iter()
        .map(|&index| CapacityFactor::new(grid.clone(), 0.0_f64).prior_weights[index])
        .sum::<f64>();
    let analytic_limit = covering_no_knee_prior / covering_prior;
    for index in 0..grid.service_times_seconds.len() {
        if covering.contains(&index) {
            continue;
        }
        let (low, high) = grid.throughput_interval(index, window.concurrency);
        let clamped = f64::from(window.completed_attempts).clamp(
            low * window.exposure_seconds,
            high * window.exposure_seconds,
        );
        assert!((clamped / f64::from(window.completed_attempts) - 1.0_f64).abs() >= 0.1_f64);
    }
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    let mut after_300 = 0.0_f64;
    for update in 1_u16..=400_u16 {
        factor.update(Level::new(), &window);
        if update == 300_u16 {
            after_300 = factor.no_knee_probability();
        }
    }
    let after_400 = factor.no_knee_probability();
    assert!((after_400 - analytic_limit).abs() <= 1.0e-6_f64);
    assert!((after_300 - after_400).abs() <= 1.0e-9_f64);
    assert!((0.1_f64..0.9_f64).contains(&after_300));
    assert!((0.1_f64..0.9_f64).contains(&after_400));
    Ok(())
}

#[quickcheck]
fn capacity_region_supremum_and_floor_preserve_scores(
    concurrency_code: u16,
    exposure_code: u8,
    completed: u16,
) -> bool {
    let Ok(grid) = CapacityGrid::new(
        &[0.001_f64, 0.002_f64, 0.01_f64],
        &[500.0_f64, 2_000.0_f64],
        &[0.0_f64, 1.0_f64],
    ) else {
        return false;
    };
    let concurrency = 0.1_f64 + f64::from(concurrency_code) / 100.0_f64;
    let exposure = 0.01_f64 + f64::from(exposure_code) / 100.0_f64;
    let Ok(window) = ResourceWindow::new(concurrency, exposure, u32::from(completed)) else {
        return false;
    };
    let covering = (0..grid.service_times_seconds.len())
        .map(|index| {
            let (low, high) = grid.throughput_interval(index, concurrency);
            low * exposure <= f64::from(completed) && f64::from(completed) <= high * exposure
        })
        .collect::<Vec<_>>();
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    factor.update(Level::new(), &window);
    // The floor computes `(maximum + ln p) - maximum`, which carries the
    // maximum's ulp into the exponent. Allow that relative band below p.
    let floor = INFLUENCE_BOUND_PROBABILITY * (1.0_f64 - 1.0e-9_f64);
    factor.likelihoods.iter().enumerate().all(|(index, score)| {
        (floor..=1.0_f64).contains(score)
            && (!covering[index] || (*score - 1.0_f64).abs() <= f64::EPSILON)
    })
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

#[test]
#[should_panic(expected = "a capacity window must have a finite maximum likelihood")]
fn all_underflow_capacity_window_stops_before_normalization() {
    let grid = cap_fixture().map(|factor| factor.grid);
    if let Ok(grid) = grid {
        let mut factor = CapacityFactor::new(grid, 0.0_f64);
        factor.likelihoods.fill(f64::NEG_INFINITY);
        factor.apply_likelihood(Level::new(), INFLUENCE_BOUND_PROBABILITY);
    }
}

#[test]
fn alternating_pause_and_stable_windows_find_the_true_capacity_cell() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64])?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    let paused = ResourceWindow::new(5.0_f64, 1.0_f64, 50)?;
    let stable = ResourceWindow::new(20.0_f64, 1.0_f64, 100)?;

    for window in [paused, stable].iter().cycle().take(40) {
        factor.update(simd_level, window);
    }

    let covering_mass = factor
        .weights
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let (low, high) = factor.grid.throughput_interval(*index, 20.0_f64);
            low <= 100.0_f64 && 100.0_f64 <= high
        })
        .map(|(_, weight)| weight)
        .sum::<f64>();
    assert!(covering_mass >= 0.9_f64);
    Ok(())
}

#[test]
fn declining_mass_keeps_the_cap_below_the_knee() -> Result<(), TestError> {
    let mut factor = cap_fixture()?;
    let window = ResourceWindow::new(100.0_f64, 1.0_f64, 236)?;
    for _ in 0_u8..8 {
        factor.update(Level::new(), &window);
    }

    assert!(factor.declining_probability() > ERROR_ALLOWANCE);
    assert!(factor.no_knee_probability() <= ERROR_ALLOWANCE);
    assert_eq!(factor.cap(32, 128, ERROR_ALLOWANCE), 2);
    Ok(())
}

#[test]
fn flat_mass_keeps_the_ceiling_cap() -> Result<(), TestError> {
    let mut factor = cap_fixture()?;
    let window = ResourceWindow::new(100.0_f64, 1.0_f64, 340)?;
    for _ in 0_u8..8 {
        factor.update(Level::new(), &window);
    }

    assert!(factor.declining_probability() <= ERROR_ALLOWANCE);
    assert!(factor.no_knee_probability() <= ERROR_ALLOWANCE);
    assert_eq!(factor.cap(32, 128, ERROR_ALLOWANCE), 3);
    Ok(())
}

#[quickcheck]
fn declining_cap_does_not_admit_slots_above_the_knee(
    knee_code: u16,
    slots_code: u8,
    declining_code: u16,
    no_knee_code: u8,
) -> bool {
    let capacity = f64::from(knee_code % 4_096 + 1);
    let slots_per_replica = u32::from(slots_code % 64 + 1);
    let Ok(grid) = CapacityGrid::new(&[0.2_f64], &[capacity], &[0.0_f64, 2.0_f64]) else {
        return false;
    };
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    let no_knee = ERROR_ALLOWANCE * f64::from(no_knee_code) / f64::from(u8::MAX);
    let declining = ERROR_ALLOWANCE
        + (1.0_f64 - ERROR_ALLOWANCE) * f64::from(declining_code.saturating_add(1))
            / (f64::from(u16::MAX) + 1.0_f64);
    let knee = 1.0_f64 - no_knee;
    factor.weights[0] = knee * (1.0_f64 - declining);
    factor.weights[1] = knee * declining;
    factor.weights[2] = no_knee;

    let quantile = factor.knee_quantile(1.0_f64 - ERROR_ALLOWANCE);
    let cap = factor.cap(slots_per_replica, 128, ERROR_ALLOWANCE);
    cap == 1 || f64::from(cap * slots_per_replica) <= quantile
}

fn cap_fixture() -> Result<CapacityFactor, CapacityGridError> {
    let grid = CapacityGrid::new(&[0.2_f64], &[340.0_f64], &[0.0_f64, 2.0_f64])?;
    Ok(CapacityFactor::new(grid, 0.0_f64))
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Grid(#[from] CapacityGridError),
    #[error(transparent)]
    Window(#[from] ResourceWindowError),
}
