use prosody_scale_core::{OccupancyTransition, ThroughputPosteriorCell};
use quickcheck_macros::quickcheck;

use super::{
    bucket_window_transitions, posterior_predictive_throughput_quantiles, predictive_rank_offset,
    predictive_throughput_cdf,
};
use crate::{AttemptTransition, AttemptTransitionKind, PlantError};

/// Bucketed traces must preserve totals, keep strictly increasing offsets
/// within the window, respect the group bound, and stay admissible under
/// the intake's batch semantics: each group's net state stays non-negative.
#[quickcheck]
fn bucketed_transitions_stay_admissible_and_preserve_totals(
    initial_code: u8,
    codes: Vec<u8>,
) -> bool {
    const EXPOSURE_MICROS: u64 = 10_000;
    const GROUP_COUNT_MAX: usize = 8;
    let initial_busy_slots = u32::from(initial_code % 4);
    let mut active = initial_busy_slots;
    let mut at_micros = 0_u64;
    let mut started = 0_u32;
    let mut completed = 0_u32;
    let mut transitions = Vec::with_capacity(codes.len());
    for code in codes {
        at_micros = (at_micros + u64::from(code >> 4_u32) * 199).min(EXPOSURE_MICROS + 500);
        let kind = if (code & 1) == 1 && active > 0 {
            active -= 1;
            completed += 1;
            AttemptTransitionKind::Completion
        } else {
            active += 1;
            started += 1;
            AttemptTransitionKind::Start
        };
        transitions.push(AttemptTransition { at_micros, kind });
    }
    let mut groups = Vec::with_capacity(GROUP_COUNT_MAX);
    let Ok(group_count_max) = u32::try_from(GROUP_COUNT_MAX) else {
        return false;
    };
    if bucket_window_transitions(
        &transitions,
        0,
        EXPOSURE_MICROS,
        group_count_max,
        &mut groups,
    )
    .is_err()
    {
        return false;
    }
    let totals_hold = groups
        .iter()
        .copied()
        .map(OccupancyTransition::completed_attempts)
        .sum::<u32>()
        == completed
        && groups
            .iter()
            .copied()
            .map(OccupancyTransition::started_attempts)
            .sum::<u32>()
            == started;
    let mut previous: Option<u64> = None;
    let ordered = groups.iter().all(|group| {
        let increasing = previous.is_none_or(|offset| group.offset_micros() > offset)
            && group.offset_micros() <= EXPOSURE_MICROS;
        previous = Some(group.offset_micros());
        increasing
    });
    let mut state = initial_busy_slots;
    let admissible = groups.iter().all(|group| {
        state
            .checked_add(group.started_attempts())
            .and_then(|value| value.checked_sub(group.completed_attempts()))
            .is_some_and(|next| {
                state = next;
                true
            })
    });
    groups.len() <= GROUP_COUNT_MAX && totals_hold && ordered && admissible
}

#[test]
fn predictive_throughput_includes_poisson_observation_noise() -> Result<(), PlantError> {
    let cells = [ThroughputPosteriorCell {
        throughput_per_second: 10.0_f64,
        throughput_low_per_second: 5.0_f64,
        throughput_high_per_second: 20.0_f64,
        probability: 1.0_f64,
    }];

    let quantiles = posterior_predictive_throughput_quantiles(&cells, 1.0_f64)?;

    assert!(
        quantiles
            .iter()
            .zip([6.0_f64, 10.0_f64, 14.0_f64])
            .all(|(actual, expected)| actual.to_bits() == expected.to_bits()),
        "the predictive quantiles must include count variation: {quantiles:?}"
    );
    Ok(())
}

#[test]
fn interval_predictive_rank_handles_zero_observed_count() -> Result<(), PlantError> {
    let cells = [ThroughputPosteriorCell {
        throughput_per_second: 1.0_f64,
        throughput_low_per_second: 0.5_f64,
        throughput_high_per_second: 2.0_f64,
        probability: 1.0_f64,
    }];

    let rank = predictive_throughput_cdf(&cells, 1.0_f64, 0)?;

    assert!((0.0_f64..=1.0_f64).contains(&rank));
    Ok(())
}

#[test]
fn interval_predictive_rank_guards_non_positive_bounds() -> Result<(), PlantError> {
    let cells = [ThroughputPosteriorCell {
        throughput_per_second: 0.0_f64,
        throughput_low_per_second: 0.0_f64,
        throughput_high_per_second: 0.0_f64,
        probability: 1.0_f64,
    }];

    let rank = predictive_throughput_cdf(&cells, 1.0_f64, 0)?;

    assert_eq!(rank.to_bits(), 1.0_f64.to_bits());
    Ok(())
}

#[test]
fn covering_cell_predictive_rank_stays_interior() -> Result<(), PlantError> {
    let cells = [ThroughputPosteriorCell {
        throughput_per_second: 500.0_f64,
        throughput_low_per_second: 500.0_f64,
        throughput_high_per_second: 2_000.0_f64,
        probability: 1.0_f64,
    }];
    let upper = predictive_throughput_cdf(&cells, 0.1_f64, 100)?;
    let lower = predictive_throughput_cdf(&cells, 0.1_f64, 99)?;
    let rank = lower.midpoint(upper);

    assert!((0.1_f64..=0.9_f64).contains(&rank), "rank={rank}");
    Ok(())
}

#[test]
fn predictive_rank_randomization_replays_and_separates_seeds() {
    let first = predictive_rank_offset(7, 15_000_000);
    let replay = predictive_rank_offset(7, 15_000_000);
    let other = predictive_rank_offset(8, 15_000_000);

    assert_eq!(first.to_bits(), replay.to_bits(), "equal seeds must replay");
    assert_ne!(
        first.to_bits(),
        other.to_bits(),
        "different seeds must separate randomized ranks"
    );
    assert!(
        (0.0_f64..1.0_f64).contains(&first),
        "a randomized rank offset must stay in the unit interval"
    );
}
