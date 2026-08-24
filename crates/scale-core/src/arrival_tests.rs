use allocation_counter::measure;
use quickcheck_macros::quickcheck;
use statrs::distribution::{ContinuousCDF, Gamma};
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
fn authored_prior_respects_the_storage_budget() {
    let selected = ArrivalPrior::new(1.0_f64 / 3_600.0_f64);
    assert!(selected.is_ok(), "{selected:?}");
    assert!(matches!(
        ArrivalPrior::test_prior(0.05_f64, 0.05_f64, 1.0_f64 / 3_600.0_f64),
        Err(ArrivalPriorError::StorageBudget { .. })
    ));
}

#[test]
fn authored_grid_fits_the_storage_budget() -> Result<(), TestError> {
    let model = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let cell_count = model.hazard_count * RESET_COUNT * model.rate_count;
    let storage = ArrivalPrior::storage_bytes(model.hazard_count, model.rate_count)?;
    assert_eq!(
        (
            model.rate_low.to_bits(),
            model.coverage()[0].upper_endpoint().to_bits(),
            model.rate_count,
            cell_count,
            storage,
        ),
        (
            0x3770_0000_0000_0000,
            0x40bc_addc_7e9f_24d9,
            2_606,
            422_172,
            6_838_576,
        )
    );
    Ok(())
}

#[test]
fn concentrated_posterior_has_a_finite_exact_predictive_rank() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
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
    let Ok(model) = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64) else {
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
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
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
fn authored_component_targets_and_cell_masses_are_exact() -> Result<(), TestError> {
    let model = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let factor = ArrivalFactor::new(&model);
    let rate_count = factor.rates.len();
    let targets = [
        (0.6942_f64, 0.003_889_f64, 0.1425_f64),
        (0.2376_f64, 1.574_444_f64, 436.5617_f64),
        (0.0683_f64, 12.4275_f64, 739.1681_f64),
    ];

    for ((((weight, shape, rate), masses), record), (target_weight, median, p99)) in model
        .reset_components()
        .zip(factor.reset_probability)
        .zip(&model.coverage()[..RESET_COUNT])
        .zip(targets)
    {
        let distribution = Gamma::new(shape, rate).map_err(|_| TestError::Distribution)?;
        let lower_boundary = (factor.rates[0] * factor.rates[1]).sqrt();
        let upper_boundary = (factor.rates[rate_count - 2] * factor.rates[rate_count - 1]).sqrt();

        assert!((weight - target_weight).abs() <= 5.0e-5_f64);
        assert!((distribution.inverse_cdf(0.5_f64) - median).abs() <= 2.0e-12_f64 * median);
        assert!((distribution.inverse_cdf(0.99_f64) - p99).abs() <= 2.0e-12_f64 * p99);
        assert!((masses.iter().sum::<f64>() - 1.0_f64).abs() <= 2.0e-15_f64);
        assert!((masses[0] - distribution.cdf(lower_boundary)).abs() <= f64::EPSILON);
        assert!(
            (masses[rate_count - 1] - (1.0_f64 - distribution.cdf(upper_boundary))).abs()
                <= f64::EPSILON
        );
        assert!(
            (record.lower_tail_probability() - distribution.cdf(record.lower_endpoint())).abs()
                <= f64::EPSILON
        );
        assert!(
            (record.upper_tail_probability()
                - (1.0_f64 - distribution.cdf(record.upper_endpoint())))
            .abs()
                <= f64::EPSILON
        );
    }
    Ok(())
}

#[test]
fn authored_mixture_zero_evidence_tail_is_bounded() -> Result<(), TestError> {
    let model = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let mut tail = 0.0_f64;
    for (weight, shape, rate) in model.reset_components() {
        let distribution = Gamma::new(shape, rate).map_err(|_| TestError::Distribution)?;
        tail += weight * (1.0_f64 - distribution.cdf(100.0_f64));
    }
    assert!(tail <= 0.05_f64, "zero-evidence tail was {tail}");
    Ok(())
}

#[test]
fn silence_moves_reset_odds_toward_the_quiet_component() -> Result<(), TestError> {
    let model = ArrivalPrior::new(1.0_f64 / 3_600.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let initial = reset_masses(&factor);
    for report in 1_u64..=8 {
        factor.update(
            ArrivalEvidence::new(0, 60_000_000),
            None,
            report * 60_000_000,
        );
    }
    let silent = reset_masses(&factor);
    assert!(silent[0] / silent[2] > initial[0] / initial[2]);
    Ok(())
}

#[quickcheck]
fn transition_reinjection_preserves_each_reset_mass(duration_code: u16) -> bool {
    let Ok(model) = ArrivalPrior::new(1.0_f64 / 3_600.0_f64) else {
        return false;
    };
    let mut factor = ArrivalFactor::new(&model);
    let before = reset_masses_by_hazard(&factor);
    let duration = 0.1_f64 + 300.0_f64 * f64::from(duration_code) / f64::from(u16::MAX);
    factor.transition(duration, None);
    let after = reset_masses_by_hazard(&factor);
    before
        .iter()
        .zip(after)
        .all(|(before, after)| (before - after).abs() <= 2.0e-15_f64)
}

fn reset_masses(factor: &ArrivalFactor) -> [f64; RESET_COUNT] {
    let mut masses = [0.0_f64; RESET_COUNT];
    for hazard in 0..factor.hazards.len() {
        for (reset, mass) in masses.iter_mut().enumerate() {
            *mass += (0..factor.rates.len())
                .map(|rate| factor.probability[cell(hazard, reset, rate, factor.rates.len())])
                .sum::<f64>();
        }
    }
    masses
}

fn reset_masses_by_hazard(factor: &ArrivalFactor) -> Box<[f64]> {
    (0..factor.hazards.len())
        .flat_map(|hazard| {
            (0..RESET_COUNT).map(move |reset| {
                (0..factor.rates.len())
                    .map(|rate| factor.probability[cell(hazard, reset, rate, factor.rates.len())])
                    .sum::<f64>()
            })
        })
        .collect()
}

#[test]
fn path_initial_draw_applies_the_elapsed_transition() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 90.0_f64)?;
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
fn sampled_path_average_converges_to_mean_trajectory() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 90.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let mut mean_rates = [0.0_f64; 3];
    let mean = factor.write_mean_rate_trajectory(90_000_000, 30_000_000, None, 0, &mut mean_rates);
    let expected = mean.rates().collect::<Vec<_>>();
    let mut ends = vec![0.0_f64; model.path_segment_count_max()];
    let mut rates = vec![0.0_f64; model.path_segment_count_max()];
    let mut sums = [0.0_f64; 3];
    let mut squared_sums = [0.0_f64; 3];
    let sample_count = 4_096_u32;
    for seed in 0..sample_count {
        let length = factor.sample_rate_path(
            90.0_f64,
            &mut RandomStream::new(u64::from(seed)),
            &mut ends,
            &mut rates,
            None,
            0,
        );
        for (boundary, at) in [30.0_f64, 60.0_f64, 90.0_f64].into_iter().enumerate() {
            let segment = ends[..length]
                .partition_point(|end| *end < at)
                .min(length - 1);
            sums[boundary] += rates[segment];
            squared_sums[boundary] += rates[segment] * rates[segment];
        }
    }
    let count = f64::from(sample_count);
    for boundary in 0..expected.len() {
        let average = sums[boundary] / count;
        let variance = (squared_sums[boundary] / count - average * average).max(0.0_f64);
        let six_standard_errors = 6.0_f64 * (variance / count).sqrt();
        assert!((average - expected[boundary]).abs() <= six_standard_errors);
    }
    Ok(())
}

#[test]
fn calendar_mean_trajectory_follows_segment_boundaries() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 20.0_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let mut segments = CalendarColumns::new(2);
    segments.extend(&[
        CalendarRateSegment {
            position: 0,
            start_micros: 0,
            end_micros: 60_000_000,
            shape: 100.0_f64,
            rate_seconds: 1.0_f64,
        },
        CalendarRateSegment {
            position: 1,
            start_micros: 60_000_000,
            end_micros: 121_000_000,
            shape: 400.0_f64,
            rate_seconds: 1.0_f64,
        },
    ]);
    let calendar = CalendarForecast {
        artifact: CalendarArtifactId(9),
        prior_probability: 1.0_f64,
        segments: &segments,
    };
    factor.prepare_calendar(Some(calendar), 0);
    let mut rates = [0.0_f64; 4];
    let actual =
        factor.write_mean_rate_trajectory(120_000_000, 30_000_000, Some(calendar), 0, &mut rates);

    assert_eq!(
        actual.rates().collect::<Vec<_>>(),
        vec![100.0_f64, 400.0_f64, 400.0_f64, 400.0_f64]
    );
    Ok(())
}

#[test]
fn calendar_mean_trajectory_uses_the_authored_prior_before_evidence() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 20.0_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut local_factor = ArrivalFactor::new(&model);
    let mut factor = ArrivalFactor::new(&model);
    let mut segments = CalendarColumns::new(1);
    segments.extend(&[CalendarRateSegment {
        position: 0,
        start_micros: 0,
        end_micros: 2_000_000,
        shape: 100.0_f64,
        rate_seconds: 1.0_f64,
    }]);
    let calendar = CalendarForecast {
        artifact: CalendarArtifactId(9),
        prior_probability: 0.25_f64,
        segments: &segments,
    };
    let mut local_rates = [0.0_f64; 1];
    let local = local_factor
        .write_mean_rate_trajectory(1_000_000, 1_000_000, None, 0, &mut local_rates)
        .rates()
        .next()
        .ok_or(TestError::Distribution)?;
    let mut rates = [0.0_f64; 1];
    let actual = factor
        .write_mean_rate_trajectory(1_000_000, 1_000_000, Some(calendar), 0, &mut rates)
        .rates()
        .next()
        .ok_or(TestError::Distribution)?;
    let expected = 0.75_f64 * local + 0.25_f64 * 100.0_f64;

    assert_ne!(actual.to_bits(), local.to_bits());
    assert!((actual - expected).abs() <= f64::EPSILON * expected);
    Ok(())
}

#[test]
fn decision_read_does_not_reset_calendar_evidence() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(2.0_f64, 20.0_f64, 1.0_f64 / 3_600.0_f64)?;
    let mut factor = ArrivalFactor::new(&model);
    let mut segments = CalendarColumns::new(1);
    segments.extend(&[CalendarRateSegment {
        position: 0,
        start_micros: 0,
        end_micros: 3_000_000,
        shape: 100.0_f64,
        rate_seconds: 1.0_f64,
    }]);
    let calendar = CalendarForecast {
        artifact: CalendarArtifactId(9),
        prior_probability: 0.25_f64,
        segments: &segments,
    };
    factor.update(
        ArrivalEvidence::new(100, 1_000_000),
        Some(calendar),
        1_000_000,
    );
    let posterior_log_odds = factor.calendar_log_odds;
    let mut rates = [0.0_f64; 1];

    factor.write_mean_rate_trajectory(1_000_000, 1_000_000, Some(calendar), 1_000_000, &mut rates);

    assert_ne!(
        posterior_log_odds.to_bits(),
        super::logit(0.25_f64).to_bits()
    );
    assert_eq!(
        factor.calendar_log_odds.to_bits(),
        posterior_log_odds.to_bits()
    );
    Ok(())
}

#[test]
fn derived_grids_meet_their_accuracy_and_coverage_budgets() -> Result<(), TestError> {
    let model = ArrivalPrior::test_prior(1.0_f64, 1.0_f64, 1.0_f64 / 86_400.0_f64)?;
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
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
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
    let model = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 3_600.0_f64)?;
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
    let Ok(model) = ArrivalPrior::test_prior(2.0_f64, 0.2_f64, 1.0_f64 / 86_400.0_f64) else {
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
