use fearless_simd::Level;
use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{CapacityFactor, CapacityGrid, CapacityGridError, ResourceWindow, ResourceWindowError};

const ERROR_ALLOWANCE: f64 = 0.01_f64;

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

    assert!(factor.weights[0] > factor.weights[1]);
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
