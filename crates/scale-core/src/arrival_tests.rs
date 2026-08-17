use allocation_counter::measure;
use quickcheck_macros::quickcheck;
use statrs::distribution::{ContinuousCDF, Gamma};
use statrs::function::gamma::{gamma_lr, gamma_ur};
use std::f64::consts::E;
use thiserror::Error;

use super::{
    ArrivalEvidence, ArrivalFactor, ArrivalPrior, ArrivalPriorError, EPSILON_BOUNDARY,
    EPSILON_GRID, HAZARD_TRANSITION_PROBABILITY_ERROR_MAX, RESET_COUNT, T_MAX_SECONDS, cell,
    poisson_mass, sample_path_counts,
};
use crate::types::{CalendarColumns, CalendarForecast};
use crate::{CalendarArtifactId, CalendarRateSegment, RandomStream};

#[test]
fn concentrated_posterior_has_a_finite_exact_predictive_rank() -> Result<(), TestError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let rate_count = factor.rates.len();
    factor.probability.fill(0.0_f64);
    factor.probability[cell(0, 0, rate_count / 2, rate_count)] = 1.0_f64;

    let predictive = factor.count_predictive(0, 7, 1.0_f64)?;
    let rank = predictive.lower_cdf.midpoint(predictive.upper_cdf);

    assert!(rank.is_finite());
    assert!((0.0_f64..=1.0_f64).contains(&rank));
    assert!(predictive.lower_cdf <= predictive.upper_cdf);
    Ok(())
}

#[quickcheck]
fn boundary_filter_matches_exhaustive_one_step(count_code: u8, duration_code: u16) -> bool {
    let Ok(model) = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64) else {
        return false;
    };
    let duration = 0.1_f64 + 30.0_f64 * f64::from(duration_code) / f64::from(u16::MAX);
    let count = u32::from(count_code % 32);
    let before = ArrivalFactor::new(&model);
    let rate_count = before.rates.len();
    let mut expected = vec![0.0_f64; before.probability.len()];
    for hazard in 0..before.hazards.len() {
        let retained = (-before.hazards[hazard] * duration).exp();
        for reset in 0..RESET_COUNT {
            let changed = (0..rate_count)
                .map(|source| before.probability[cell(hazard, reset, source, rate_count)])
                .sum::<f64>()
                * (1.0_f64 - retained);
            for destination in 0..rate_count {
                let prior = retained
                    * before.probability[cell(hazard, reset, destination, rate_count)]
                    + changed * before.reset_probability[reset][destination];
                expected[cell(hazard, reset, destination, rate_count)] =
                    prior * poisson_mass(count, before.rates[destination] * duration);
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
fn update_path_does_not_allocate() -> Result<(), ArrivalPriorError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    let factor = ArrivalFactor::new(&model);
    let rate_count = factor.rates.len();
    let hazard_count = factor.hazards.len();
    assert_eq!(
        ArrivalPrior::storage_bytes(hazard_count, rate_count)?,
        8 * (2 * factor.probability.len() + hazard_count + RESET_COUNT * rate_count + rate_count)
    );
    let mut factor = factor;
    let allocation = measure(|| factor.update(ArrivalEvidence::new(7, 1_000_000), None, 1_000_000));
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    Ok(())
}

#[test]
fn reset_cell_masses_are_exact_and_exhaustive() -> Result<(), TestError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    let factor = ArrivalFactor::new(&model);
    let rate_count = factor.rates.len();
    let mean = model.shape() / model.rate_seconds();

    for (shape, masses) in [1.0_f64, 2.0_f64, 4.0_f64]
        .into_iter()
        .zip(factor.reset_probability)
    {
        let distribution = Gamma::new(shape, shape).map_err(|_| TestError::Distribution)?;
        let lower_boundary = (factor.rates[0] * factor.rates[1]).sqrt() / mean;
        let upper_boundary =
            (factor.rates[rate_count - 2] * factor.rates[rate_count - 1]).sqrt() / mean;

        assert!((masses.iter().sum::<f64>() - 1.0_f64).abs() <= 2.0e-15_f64);
        assert!((masses[0] - distribution.cdf(lower_boundary)).abs() <= f64::EPSILON);
        assert!(
            (masses[rate_count - 1] - (1.0_f64 - distribution.cdf(upper_boundary))).abs()
                <= f64::EPSILON
        );
        let below_index = rate_count * 3 / 4;
        let below_lower = (factor.rates[below_index - 1] * factor.rates[below_index]).sqrt() / mean;
        let below_upper = (factor.rates[below_index] * factor.rates[below_index + 1]).sqrt() / mean;
        let below_mass =
            gamma_lr(shape, shape * below_upper) - gamma_lr(shape, shape * below_lower);
        assert!(factor.rates[below_index] < mean && masses[below_index] > 0.0_f64);
        assert!((masses[below_index] - below_mass).abs() <= 32.0_f64 * f64::EPSILON * below_mass);

        let first_above = factor
            .rates
            .iter()
            .position(|rate| *rate > mean)
            .ok_or(TestError::Distribution)?;
        let above_index = first_above + 2;
        assert!(above_index + 1 < rate_count);
        let above_lower = (factor.rates[above_index - 1] * factor.rates[above_index]).sqrt() / mean;
        let above_upper = (factor.rates[above_index] * factor.rates[above_index + 1]).sqrt() / mean;
        let above_mass =
            gamma_ur(shape, shape * above_lower) - gamma_ur(shape, shape * above_upper);
        assert!(factor.rates[above_index] > mean && masses[above_index] > 0.0_f64);
        assert!((masses[above_index] - above_mass).abs() <= 32.0_f64 * f64::EPSILON * above_mass);
    }
    for (record, shape) in model.coverage()[..RESET_COUNT]
        .iter()
        .zip([1.0_f64, 2.0_f64, 4.0_f64])
    {
        let distribution = Gamma::new(shape, shape).map_err(|_| TestError::Distribution)?;
        assert!(
            (record.lower_tail_probability() - distribution.cdf(record.lower_endpoint() / mean))
                .abs()
                <= f64::EPSILON
        );
        assert!(
            (record.upper_tail_probability()
                - (1.0_f64 - distribution.cdf(record.upper_endpoint() / mean)))
            .abs()
                <= f64::EPSILON
        );
    }
    Ok(())
}

#[test]
fn path_initial_draw_applies_the_elapsed_transition() -> Result<(), TestError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 90.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let rate_count = factor.rates.len();
    let maximum_rate = factor.rates[rate_count - 1];
    factor.probability.fill(0.0_f64);
    factor.probability[cell(factor.hazards.len() - 1, 1, rate_count - 1, rate_count)] = 1.0_f64;
    let mut ends = vec![0.0_f64; model.path_segment_count_max()];
    let mut rates = vec![0.0_f64; model.path_segment_count_max()];
    let mut total = 0.0_f64;

    for seed in 0_u64..128 {
        let length = factor.sample_rate_path(
            1.0_f64,
            &mut RandomStream::new(seed),
            &mut ends,
            &mut rates,
            None,
            604_800_000_000,
        );
        assert!(length > 0);
        total += rates[0];
    }

    assert!(total / 128.0_f64 < 0.5_f64 * maximum_rate);
    Ok(())
}

#[test]
fn derived_grids_meet_their_accuracy_and_coverage_budgets() -> Result<(), TestError> {
    let model = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 86_400.0_f64)?;
    let factor = ArrivalFactor::new(&model);
    let rate_error = (model.rate_log_step * 0.5_f64).exp() - 1.0_f64;
    let hazard_error = model.hazard_log_step / E;
    assert!(rate_error <= EPSILON_GRID);
    assert!(hazard_error <= HAZARD_TRANSITION_PROBABILITY_ERROR_MAX);
    assert!(model.coverage().iter().all(|record| {
        record.lower_tail_probability() <= EPSILON_BOUNDARY * 0.5_f64
            && record.upper_tail_probability() <= EPSILON_BOUNDARY * 0.5_f64
    }));
    assert!((factor.probability.iter().sum::<f64>() - 1.0_f64).abs() <= 2.0e-14_f64);
    assert!(
        factor.reset_probability.iter().all(|masses| {
            (masses.iter().sum::<f64>() - 1.0_f64).abs() <= 4.0_f64 * f64::EPSILON
        })
    );
    Ok(())
}

#[test]
fn expired_calendar_returns_the_local_marginal() -> Result<(), TestError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let mut segments = CalendarColumns::new(1);
    segments.extend(&[CalendarRateSegment {
        position: 0,
        start_micros: 0,
        end_micros: 2_000_000,
        shape: 1_000.0_f64,
        rate_seconds: 1.0_f64,
    }]);
    let calendar = CalendarForecast {
        artifact: CalendarArtifactId(7),
        prior_probability: 0.9_f64,
        segments: &segments,
    };

    factor.prepare_calendar(Some(calendar), 1_000_000);
    assert!(factor.expected_rate(1_000_000) > factor.marginal_mean(1_000_000));
    let log_odds = factor.calendar_log_odds;
    factor.prepare_calendar(Some(calendar), 2_000_000);

    assert_eq!(factor.calendar_log_odds.to_bits(), log_odds.to_bits());
    assert_eq!(
        factor.expected_rate(2_000_000).to_bits(),
        factor.marginal_mean(2_000_000).to_bits()
    );
    Ok(())
}

#[test]
fn zero_length_path_segment_is_rejected() {
    let random = RandomStream::new(1);
    let ends = [0.0_f64];
    let mut rates = [1.0_f64];

    assert!(!sample_path_counts(&random, &ends, &mut rates, 1));
    assert_eq!(rates[0].to_bits(), 1.0_f64.to_bits());
}

#[test]
fn crossing_interval_updates_its_start_calendar_segment() -> Result<(), ArrivalPriorError> {
    let model = ArrivalPrior::new(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut segments = CalendarColumns::new(2);
    segments.extend(&[
        CalendarRateSegment {
            position: 10,
            start_micros: 0,
            end_micros: 2_000_000,
            shape: 2.0_f64,
            rate_seconds: 2.0_f64,
        },
        CalendarRateSegment {
            position: 11,
            start_micros: 2_000_000,
            end_micros: 4_000_000,
            shape: 100.0_f64,
            rate_seconds: 20.0_f64,
        },
    ]);
    let calendar = CalendarForecast {
        artifact: CalendarArtifactId(1),
        prior_probability: 0.5_f64,
        segments: &segments,
    };
    let mut reference_segments = CalendarColumns::new(1);
    reference_segments.extend(&[CalendarRateSegment {
        position: 10,
        start_micros: 0,
        end_micros: 4_000_000,
        shape: 2.0_f64,
        rate_seconds: 2.0_f64,
    }]);
    let reference_calendar = CalendarForecast {
        artifact: CalendarArtifactId(1),
        prior_probability: 0.5_f64,
        segments: &reference_segments,
    };
    let mut factor = ArrivalFactor::new(&model);
    let mut reference = ArrivalFactor::new(&model);

    factor.update(
        ArrivalEvidence::new(4, 1_000_000),
        Some(calendar),
        1_000_000,
    );
    reference.update(
        ArrivalEvidence::new(4, 1_000_000),
        Some(reference_calendar),
        1_000_000,
    );

    factor.update(
        ArrivalEvidence::new(6, 2_000_000),
        Some(calendar),
        3_000_000,
    );
    reference.update(
        ArrivalEvidence::new(6, 2_000_000),
        Some(reference_calendar),
        3_000_000,
    );

    assert!((reference.calendar_shape - 12.0_f64).abs() <= f64::EPSILON);
    assert!((reference.calendar_rate - 5.0_f64).abs() <= f64::EPSILON);
    assert!((factor.calendar_log_odds - reference.calendar_log_odds).abs() <= f64::EPSILON);
    assert_eq!(factor.calendar_position, 11);
    assert!((factor.calendar_shape - 100.0_f64).abs() <= f64::EPSILON);
    assert!((factor.calendar_rate - 20.0_f64).abs() <= f64::EPSILON);
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
    let length = ArrivalFactor::new(&model).sample_rate_path(
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

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Prior(#[from] ArrivalPriorError),
    #[error(transparent)]
    Predictive(#[from] super::ArrivalPredictiveError),
    #[error("test distribution is invalid")]
    Distribution,
}
