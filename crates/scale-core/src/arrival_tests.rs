use allocation_counter::measure;
use quickcheck_macros::quickcheck;

use super::{
    ArrivalEvidence, ArrivalFactor, ArrivalPrior, CELL_COUNT, HAZARD_COUNT, RATE_COUNT,
    RESET_COUNT, T_MAX_SECONDS, cell, poisson_mass,
};
use crate::RandomStream;

#[quickcheck]
fn boundary_filter_matches_exhaustive_one_step(count_code: u8, duration_code: u16) -> bool {
    let Ok(model) = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64) else {
        return false;
    };
    let duration = 0.1_f64 + 30.0_f64 * f64::from(duration_code) / f64::from(u16::MAX);
    let count = u32::from(count_code % 32);
    let before = ArrivalFactor::new(model);
    let mut expected = vec![0.0_f64; CELL_COUNT];
    for hazard in 0..HAZARD_COUNT {
        let retained = (-before.hazards[hazard] * duration).exp();
        for reset in 0..RESET_COUNT {
            for source in 0..RATE_COUNT {
                let source_mass = before.probability[cell(hazard, reset, source)];
                for destination in 0..RATE_COUNT {
                    let transition = if source == destination {
                        retained
                    } else {
                        0.0_f64
                    } + (1.0_f64 - retained)
                        * before.reset_probability[reset][destination];
                    expected[cell(hazard, reset, destination)] += source_mass
                        * transition
                        * poisson_mass(count, before.rates[destination] * duration);
                }
            }
        }
    }
    let total = expected.iter().sum::<f64>();
    let mut actual = before;
    actual.transition(duration, Some(count));
    expected
        .iter()
        .zip(actual.probability)
        .all(|(expected, actual)| (expected / total - actual).abs() <= 2.0e-12_f64)
}

#[test]
fn update_path_does_not_allocate() -> Result<(), super::ArrivalPriorError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    assert_eq!(
        ArrivalPrior::storage_bytes()?,
        8 * (2 * CELL_COUNT + HAZARD_COUNT + RESET_COUNT * RATE_COUNT + RATE_COUNT)
    );
    let mut factor = ArrivalFactor::new(model);
    let allocation = measure(|| factor.update(ArrivalEvidence::new(7, 1_000_000), None, 1_000_000));
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    Ok(())
}

#[quickcheck]
fn accepted_paths_end_at_the_requested_horizon(seed: u64, duration_code: u16) -> bool {
    let Ok(model) = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 86_400.0_f64) else {
        return false;
    };
    let duration =
        1.0_f64 + (T_MAX_SECONDS - 1.0_f64) * f64::from(duration_code) / f64::from(u16::MAX);
    let mut ends = vec![0.0_f64; model.path_segment_count_max()];
    let mut rates = vec![0.0_f64; model.path_segment_count_max()];
    let mut random = RandomStream::new(seed);
    let length = ArrivalFactor::new(model).sample_rate_path(
        duration,
        &mut random,
        &mut ends,
        &mut rates,
        None,
        0,
    );
    length > 0
        && length <= model.path_segment_count_max()
        && (ends[length - 1] - duration).abs() < f64::EPSILON
}
