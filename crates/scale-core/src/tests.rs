use std::hint::black_box;
use std::time::Duration;

use fearless_simd::Level;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use thiserror::Error;

use crate::arrival::{ArrivalEvidence, ArrivalFactor};
use crate::capacity::CapacityFactor;
use crate::edf::{
    CandidateLoss, CandidateSupply, EdfScratch, prepare, shortfall,
    shortfall_prepared_common_release_candidates,
};
use crate::lead_time::LeadTimeFactor;
use crate::partition::PartitionFactor;
use crate::types::WorkCohort;
use crate::{
    CapacityCurve, CapacityGrid, CapacityPrior, Cohort, Configuration, HoldReason, ModelTime,
    ObservationBuffer, PosteriorQuery, RandomStream, ResourceWindow, ScaleDecision, ScaleScratch,
    ScaleState, ServiceObjective, ThroughputPosteriorCell, TransitionDirection, TransitionEvidence,
    step,
};

#[derive(Clone, Debug)]
struct CohortSet(Vec<WorkCohort>);

#[derive(Clone, Copy, Debug)]
struct CapacityQuery {
    cell_count: u16,
    concurrency_tenths: u16,
}

impl Arbitrary for CapacityQuery {
    fn arbitrary(generator: &mut Gen) -> Self {
        Self {
            cell_count: u16::arbitrary(generator) % 256 + 2,
            concurrency_tenths: u16::arbitrary(generator) % 2_001,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let mut shrunk = Vec::with_capacity(2);
        if self.cell_count > 2 {
            shrunk.push(Self {
                cell_count: (self.cell_count / 2).max(2),
                ..*self
            });
        }
        if self.concurrency_tenths > 0 {
            shrunk.push(Self {
                concurrency_tenths: self.concurrency_tenths / 2,
                ..*self
            });
        }
        Box::new(shrunk.into_iter())
    }
}

impl Arbitrary for CohortSet {
    fn arbitrary(generator: &mut Gen) -> Self {
        let count = usize::arbitrary(generator) % 16;
        let mut cohorts = Vec::with_capacity(count);
        for partition in 0..count {
            let release_micros = u64::arbitrary(generator) % 20;
            let duration_micros = u64::arbitrary(generator) % 20 + 1;
            let work_slot_seconds = f64::from(u16::arbitrary(generator) % 40) / 1_000_000.0_f64;
            cohorts.push(WorkCohort {
                release_micros,
                deadline_micros: release_micros + duration_micros,
                work_slot_seconds,
                partition: partition as u32,
            });
        }
        Self(cohorts)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let mut shrunk = Vec::new();
        if !self.0.is_empty() {
            shrunk.push(Self(self.0[..self.0.len() / 2].to_vec()));
            shrunk.push(Self(self.0[1..].to_vec()));
        }
        Box::new(shrunk.into_iter())
    }
}

#[test]
fn capacity_curve_has_linear_plateau_and_collapse_shapes() {
    let linear = CapacityCurve::Knee {
        service_time_seconds: 0.1_f64,
        capacity_per_second: 100.0_f64,
        collapse: 0.0_f64,
    };
    let collapse = CapacityCurve::Knee {
        service_time_seconds: 0.1_f64,
        capacity_per_second: 100.0_f64,
        collapse: 1.0_f64,
    };

    assert!((linear.throughput(5.0) - 50.0_f64).abs() < f64::EPSILON);
    assert!((linear.throughput(20.0) - 100.0_f64).abs() < f64::EPSILON);
    assert!(collapse.throughput(20.0) < 100.0_f64);
}

#[test]
fn counter_stream_replays_and_separates_domains() {
    let mut first = RandomStream::new(7);
    let mut replay = RandomStream::new(7);
    let mut base = RandomStream::new(7);
    let mut other = RandomStream::new(7).domain(1);
    for _ in 0_u32..128 {
        assert_eq!(first.next_u64(), replay.next_u64());
        assert_ne!(base.next_u64(), other.next_u64());
        let uniform = replay.open_unit_f64();
        let _matched = first.open_unit_f64();
        assert!(uniform > 0.0_f64);
        assert!(uniform < 1.0_f64);
    }
}

#[test]
fn lead_time_updates_only_the_matching_direction_and_delta() -> Result<(), TestError> {
    let simd_level = Level::new();
    let mut factor = LeadTimeFactor::new();
    let up_one_prior = factor.expected_seconds(TransitionDirection::Up, 1);
    let up_four_prior = factor.expected_seconds(TransitionDirection::Up, 4);
    let down_prior = factor.expected_seconds(TransitionDirection::Down, 1);

    factor.update(
        simd_level,
        TransitionEvidence::completed(TransitionDirection::Up, 1, 15_000_000)?
            .consume()
            .0,
    );

    assert!(factor.expected_seconds(TransitionDirection::Up, 1) < up_one_prior);
    assert!(close_relative(
        factor.expected_seconds(TransitionDirection::Up, 4),
        up_four_prior,
    ));
    assert!(close_relative(
        factor.expected_seconds(TransitionDirection::Down, 1),
        down_prior,
    ));

    factor.update(
        simd_level,
        TransitionEvidence::censored(TransitionDirection::Down, 1, 120_000_000)?
            .consume()
            .0,
    );
    assert!(factor.expected_seconds(TransitionDirection::Down, 1) > down_prior);
    factor.update(
        simd_level,
        TransitionEvidence::censored(TransitionDirection::Down, 1, u64::MAX)?
            .consume()
            .0,
    );
    assert!(
        factor
            .expected_seconds(TransitionDirection::Down, 1)
            .is_finite()
    );
    Ok(())
}

#[test]
fn rebalance_evidence_updates_each_observed_phase() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let lead_query = PosteriorQuery::LeadTime {
        direction: TransitionDirection::Up,
        replica_delta: 1,
    };
    let pause_query = PosteriorQuery::RebalanceTime {
        direction: TransitionDirection::Up,
        replica_delta: 1,
    };
    let value_count = state.posterior_value_count(lead_query)? as usize;
    let mut values = vec![0.0_f64; value_count];
    let mut lead_before = vec![0.0_f64; value_count];
    let mut pause_before = vec![0.0_f64; value_count];
    state.write_posterior(lead_query, &mut values, &mut lead_before)?;
    state.write_posterior(pause_query, &mut values, &mut pause_before)?;
    observation.set_transition(TransitionEvidence::completed_rebalance(
        TransitionDirection::Up,
        1,
        15_000_000,
        120_000_000,
    )?)?;

    let _decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let mut lead_after = vec![0.0_f64; value_count];
    let mut pause_after = vec![0.0_f64; value_count];
    state.write_posterior(lead_query, &mut values, &mut lead_after)?;
    state.write_posterior(pause_query, &mut values, &mut pause_after)?;

    assert!(lead_after[0] > lead_before[0]);
    assert!(pause_after[value_count - 1] > pause_before[value_count - 1]);
    Ok(())
}

#[test]
fn stable_evidence_cannot_activate_a_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = grid()?;
    let mut factor = CapacityFactor::new(grid);
    factor.update(simd_level, &ResourceWindow::new(8.0, 1.0, 80)?);

    assert_eq!(factor.cap(4, 32, 0.01_f64), 32);
    Ok(())
}

#[test]
fn linear_evidence_retains_a_no_knee_explanation() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[10.0_f64, 20.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = CapacityFactor::new(grid);
    for concurrency in [1.0_f64, 2.0_f64, 4.0_f64, 8.0_f64] {
        let completions = (concurrency * 10.0_f64) as u32;
        factor.update(
            simd_level,
            &ResourceWindow::new(concurrency, 1.0_f64, completions)?,
        );
    }

    assert!(factor.expected_throughput(simd_level, 8.0_f64) > 70.0_f64);
    assert_eq!(factor.cap(1, 32, 0.01_f64), 32);
    Ok(())
}

#[test]
fn resource_window_is_consumed_once() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_resource_window(ResourceWindow::new(8.0, 1.0, 80)?)?;
    let replacement = observation.set_resource_window(ResourceWindow::new(8.0, 1.0, 80)?);
    assert!(matches!(
        replacement,
        Err(crate::ObservationError::ResourceWindowPending)
    ));

    let consumed = observation.observation();
    assert!(consumed.resource_window.is_some());
    let next = observation.observation();
    assert!(next.resource_window.is_none());
    Ok(())
}

#[test]
fn partition_arrival_update_is_consumed_once() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let mut counts = [0_u32; 16];
    counts[3] = 19;
    observation.set_partition_arrivals(&counts, 1_000_000)?;

    let consumed = observation.observation();
    assert!(consumed.arrivals.is_some());
    assert!(consumed.partition_arrivals.is_some());
    let next = observation.observation();
    assert!(next.partition_arrivals.is_none());
    Ok(())
}

#[test]
fn arrival_posterior_predictive_mass_normalizes() {
    let mut factor = ArrivalFactor::new();
    factor.update(ArrivalEvidence::new(4, 1_000_000));
    let mass = (0_u32..128)
        .map(|count| factor.predictive_probability(count, 1.0_f64))
        .sum::<f64>();
    assert!((mass - 1.0_f64).abs() < 1.0e-12_f64);
}

#[test]
fn partition_factor_learns_a_normalized_skew() -> Result<(), TestError> {
    let mut factor = PartitionFactor::new(4)?;
    factor.update(&[90, 10, 0, 0]);
    let sum = (0_u32..4)
        .map(|partition| factor.expected_share(partition))
        .sum::<f64>();
    assert!((sum - 1.0_f64).abs() < 1.0e-12_f64);
    assert!(factor.expected_share(0) > 0.85_f64);
    Ok(())
}

#[test]
fn moved_partition_draws_preserve_joint_skew_uncertainty() -> Result<(), TestError> {
    let mut factor = PartitionFactor::new(2)?;
    factor.update(&[1_000, 0]);
    let mut random = RandomStream::new(47);
    let mut order = [0_u32; 2];
    let mut shares = [0.0_f64; 2];
    let mut prefix = [0.0_f64; 3];
    let mut minimum_one_moved = 1.0_f64;
    let mut maximum_one_moved = 0.0_f64;
    for _ in 0_u32..128 {
        factor.sample_moved_prefix(&mut random, &mut order, &mut shares, &mut prefix);
        assert!(prefix[0] <= prefix[1] && prefix[1] <= prefix[2]);
        assert!(close_relative(prefix[2], 1.0_f64));
        minimum_one_moved = minimum_one_moved.min(prefix[1]);
        maximum_one_moved = maximum_one_moved.max(prefix[1]);
    }

    assert!(minimum_one_moved < 0.1_f64);
    assert!(maximum_one_moved > 0.9_f64);
    Ok(())
}

#[quickcheck]
fn simd_capacity_reductions_match_scalar(query: CapacityQuery) -> bool {
    let simd_level = Level::new();
    let capacities = (0..query.cell_count)
        .map(|cell| 10.0_f64 + f64::from(cell))
        .collect::<Vec<_>>();
    let Ok(grid) = CapacityGrid::new(&[0.01_f64], &capacities, &[0.5_f64]) else {
        return false;
    };
    let mut factor = CapacityFactor::new(grid);
    let Ok(window) = ResourceWindow::new(7.0_f64, 1.0_f64, 83) else {
        return false;
    };
    factor.update(simd_level, &window);
    let concurrency = f64::from(query.concurrency_tenths) / 10.0_f64;
    let capacity_matches = close_relative(
        factor.expected_capacity(simd_level),
        factor.expected_capacity_scalar(),
    );
    let throughput_matches = close_relative(
        factor.expected_throughput(simd_level, concurrency),
        factor.expected_throughput_scalar(concurrency),
    );
    let saturation_matches = close_relative(
        factor.saturation_probability(simd_level, concurrency),
        factor.saturation_probability_scalar(concurrency),
    );
    assert!(
        capacity_matches,
        "the SIMD capacity reduction {} must match scalar {}",
        factor.expected_capacity(simd_level),
        factor.expected_capacity_scalar()
    );
    assert!(
        throughput_matches,
        "the SIMD throughput reduction must match scalar"
    );
    assert!(
        saturation_matches,
        "the SIMD saturation reduction must match scalar"
    );
    true
}

#[test]
fn capacity_quantiles_preserve_posterior_order() -> Result<(), TestError> {
    assert!(matches!(
        CapacityGrid::new(&[0.02_f64, 0.01_f64], &[100.0_f64], &[0.0_f64]),
        Err(crate::CapacityGridError::AxisOrder)
    ));
    let grid = CapacityGrid::new(
        &[0.01_f64, 0.02_f64],
        &[100.0_f64, 200.0_f64, 400.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let factor = CapacityFactor::new(grid);

    let low = factor.capacity_quantile(0.1_f64);
    let median = factor.capacity_quantile(0.5_f64);
    let high = factor.capacity_quantile(0.9_f64);

    assert!(low <= median);
    assert!(median <= high);
    assert_eq!(low.to_bits(), 100.0_f64.to_bits());
    assert_eq!(median.to_bits(), 200.0_f64.to_bits());
    assert_eq!(high.to_bits(), 400.0_f64.to_bits());
    Ok(())
}

#[test]
fn capacity_prior_is_proper_and_stationary() -> Result<(), TestError> {
    let grid = CapacityGrid::new(
        &[0.01_f64],
        &[1.0_f64, 10.0_f64, 100.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let mut factor = CapacityFactor::new(grid);
    let mut values = [0.0_f64; 3];
    let mut prior = [0.0_f64; 3];
    factor.write_capacity_posterior(&mut values, &mut prior)?;

    assert!(
        values
            .iter()
            .zip([1.0_f64, 10.0_f64, 100.0_f64])
            .all(|(actual, expected)| actual.to_bits() == expected.to_bits())
    );
    assert!(close_relative(prior[0], 0.25_f64));
    assert!(close_relative(prior[1], 0.5_f64));
    assert!(close_relative(prior[2], 0.25_f64));
    assert!(close_relative(factor.no_collapse_probability(), 0.5_f64));

    for _ in 0_u32..100 {
        factor.transition();
    }
    let mut transitioned = [0.0_f64; 3];
    factor.write_capacity_posterior(&mut values, &mut transitioned)?;
    assert!(
        prior
            .iter()
            .zip(transitioned)
            .all(|(before, after)| close_relative(*before, after))
    );
    Ok(())
}

#[test]
fn every_discrete_posterior_has_an_ordered_normalized_view() -> Result<(), TestError> {
    let state = ScaleState::new(configuration()?, grid()?)?;
    let queries = [
        PosteriorQuery::Capacity,
        PosteriorQuery::ServiceTime,
        PosteriorQuery::Collapse,
        PosteriorQuery::Knee,
        PosteriorQuery::SaturationState,
        PosteriorQuery::PartitionShare,
        PosteriorQuery::LeadTime {
            direction: TransitionDirection::Up,
            replica_delta: 1,
        },
        PosteriorQuery::LeadTime {
            direction: TransitionDirection::Down,
            replica_delta: 1,
        },
    ];
    for query in queries {
        let count = usize::try_from(state.posterior_value_count(query)?)
            .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
        let mut values = vec![0.0_f64; count];
        let mut probabilities = vec![0.0_f64; count];
        state.write_posterior(query, &mut values, &mut probabilities)?;
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(close_relative(probabilities.iter().sum(), 1.0_f64));
    }
    let arrival = state.arrival_posterior();
    assert!(arrival.shape > 0.0_f64);
    assert!(arrival.rate > 0.0_f64);
    Ok(())
}

#[test]
fn narrower_log_capacity_prior_concentrates_more_mass_at_its_median() -> Result<(), TestError> {
    let service_times = [0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64];
    let capacities = [80.0_f64, 160.0_f64, 320.0_f64, 640.0_f64];
    let narrow = CapacityGrid::new_with_prior(
        &service_times,
        &capacities,
        &[0.0_f64, 0.5_f64, 1.0_f64],
        CapacityPrior::LogNormal {
            service_time_median_seconds: 0.1_f64,
            capacity_median_per_second: 320.0_f64,
            log_standard_deviation: 2.0_f64.ln(),
        },
    )?;
    let wide = CapacityGrid::new_with_prior(
        &service_times,
        &capacities,
        &[0.0_f64, 0.5_f64, 1.0_f64],
        CapacityPrior::LogNormal {
            service_time_median_seconds: 0.1_f64,
            capacity_median_per_second: 320.0_f64,
            log_standard_deviation: 8.0_f64.ln(),
        },
    )?;
    let narrow = capacity_prior(narrow)?;
    let wide = capacity_prior(wide)?;

    assert!(
        narrow[2] > wide[2],
        "the narrower prior must place more capacity mass at its median"
    );
    Ok(())
}

#[test]
fn default_capacity_prior_is_explicit_log_uniform() -> Result<(), TestError> {
    let service_times = [0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64];
    let capacities = [80.0_f64, 160.0_f64, 320.0_f64, 640.0_f64];
    let implicit = CapacityGrid::new(&service_times, &capacities, &[0.0_f64, 0.5_f64, 1.0_f64])?;
    let explicit = CapacityGrid::new_with_prior(
        &service_times,
        &capacities,
        &[0.0_f64, 0.5_f64, 1.0_f64],
        CapacityPrior::LogUniform,
    )?;

    assert_eq!(
        capacity_prior(implicit)?,
        capacity_prior(explicit)?,
        "the default constructor must retain the log-uniform production prior"
    );
    Ok(())
}

#[quickcheck]
fn capacity_grid_accepts_exactly_representable_log_normal_parameters(
    service_median: f64,
    capacity_median: f64,
    log_standard_deviation: f64,
) -> bool {
    let valid = service_median.is_finite()
        && service_median > 0.0_f64
        && capacity_median.is_finite()
        && capacity_median > 0.0_f64
        && log_standard_deviation.is_finite()
        && log_standard_deviation >= f64::EPSILON;
    let result = CapacityGrid::new_with_prior(
        &[0.05_f64, 0.1_f64],
        &[160.0_f64, 320.0_f64],
        &[0.0_f64, 1.0_f64],
        CapacityPrior::LogNormal {
            service_time_median_seconds: service_median,
            capacity_median_per_second: capacity_median,
            log_standard_deviation,
        },
    );
    result.is_ok() == valid
}

fn capacity_prior(grid: CapacityGrid) -> Result<Vec<f64>, TestError> {
    let state = ScaleState::new(configuration()?, grid)?;
    let count = usize::try_from(state.posterior_value_count(PosteriorQuery::Capacity)?)
        .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
    let mut values = vec![0.0_f64; count];
    let mut probabilities = vec![0.0_f64; count];
    state.write_posterior(PosteriorQuery::Capacity, &mut values, &mut probabilities)?;
    Ok(probabilities)
}

#[test]
fn one_knee_cell_still_competes_with_no_knee() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64])?;
    let factor = CapacityFactor::new(grid);
    assert!(close_relative(factor.no_knee_probability(), 0.5_f64));
    Ok(())
}

#[test]
fn identified_plateau_activates_a_knee_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = CapacityFactor::new(grid);
    for _ in 0_u32..8 {
        factor.update(simd_level, &ResourceWindow::new(5.0_f64, 1.0_f64, 50)?);
        factor.update(simd_level, &ResourceWindow::new(20.0_f64, 1.0_f64, 100)?);
    }
    assert_eq!(factor.cap(2, 32, 0.01_f64), 5);
    Ok(())
}

#[test]
fn linear_windows_below_the_knee_cannot_create_a_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = CapacityFactor::new(grid);
    factor.update(simd_level, &ResourceWindow::new(5.0_f64, 1.0_f64, 50)?);
    factor.update(simd_level, &ResourceWindow::new(20.0_f64, 1.0_f64, 200)?);
    assert_eq!(factor.cap(2, 32, 0.01_f64), 32);
    Ok(())
}

#[test]
fn fewer_useful_completions_cannot_loosen_a_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(
        &[0.1_f64],
        &[50.0_f64, 100.0_f64, 200.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let mut healthy = CapacityFactor::new(grid.clone());
    let mut failing = CapacityFactor::new(grid);
    for _ in 0_u32..8 {
        healthy.update(simd_level, &ResourceWindow::new(5.0_f64, 1.0_f64, 50)?);
        healthy.update(simd_level, &ResourceWindow::new(20.0_f64, 1.0_f64, 100)?);
        failing.update(simd_level, &ResourceWindow::new(5.0_f64, 1.0_f64, 50)?);
        failing.update(simd_level, &ResourceWindow::new(20.0_f64, 1.0_f64, 20)?);
    }
    assert!(failing.cap(2, 32, 0.01_f64) <= healthy.cap(2, 32, 0.01_f64));
    Ok(())
}

#[quickcheck]
fn fluid_edf_matches_exhaustive_interval_oracle(input: CohortSet, slots: u8) -> bool {
    let slots = f64::from(slots % 8 + 1);
    let Ok(mut scratch) = EdfScratch::new(16) else {
        return false;
    };
    let CohortSet(cohorts) = input;
    let actual = shortfall(&cohorts, slots, &mut scratch) <= f64::EPSILON;
    let expected = exhaustive_feasible(&cohorts, slots);
    actual == expected
}

#[test]
fn partial_observation_returns_a_decision() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );

    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    assert!(apply.diagnostics.saturation_probability <= f64::EPSILON);
    Ok(())
}

#[test]
fn joint_capacity_samples_match_direct_enumeration() -> Result<(), TestError> {
    let configuration = Configuration {
        cohort_count_max: 1,
        partition_count: 1,
        replica_count_max: 1,
        slots_per_replica: 1,
        posterior_sample_count: 1_024,
        objective: ServiceObjective::new(1_000_000, 0.01_f64)?,
    };
    let grid = CapacityGrid::new(&[0.001_f64], &[50.0_f64, 100.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        offered_events: 75.0_f64,
        partition: 0,
    })?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let exact_loss = 0.25_f64 * ((75.0_f64 - 50.0_f64) / 75.0_f64);

    assert!(close_relative(apply.diagnostics.expected_loss, exact_loss));
    assert!(close_relative(
        apply.diagnostics.saturation_probability,
        0.5_f64,
    ));
    Ok(())
}

#[test]
fn capacity_that_arrives_after_a_deadline_cannot_satisfy_it() -> Result<(), TestError> {
    let configuration = Configuration {
        cohort_count_max: 2,
        partition_count: 2,
        replica_count_max: 2,
        slots_per_replica: 1,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01_f64)?,
    };
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    for partition in 0_u32..2 {
        observation.push_cohort(Cohort {
            release_micros: 0,
            deadline_micros: 1_000_000,
            offered_events: 1.0_f64,
            partition,
        })?;
    }

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    assert_eq!(apply.target, 1);
    Ok(())
}

#[test]
fn partition_pause_removes_service_from_candidate_supply() -> Result<(), TestError> {
    let cohorts = [WorkCohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        work_slot_seconds: 0.75_f64,
        partition: 0,
    }];
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let mut credit = [0.0_f64];
    let mut loss = [0.0_f64];

    let supply = CandidateSupply {
        before: 1.0_f64,
        during: &[0.5_f64],
        after: &[2.0_f64],
        pause_seconds: &[0.0_f64],
        ready_seconds: &[1.0_f64],
    };
    let mut results = CandidateLoss {
        service_credit: &mut credit,
        shortfall: &mut loss,
    };
    shortfall_prepared_common_release_candidates(
        Level::new(),
        &cohorts,
        &supply,
        &mut results,
        &scratch,
    );

    assert!(close_relative(loss[0], 1.0_f64 / 3.0_f64));
    Ok(())
}

#[test]
fn decision_curve_contains_the_selected_expected_loss() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        offered_events: 100.0_f64,
        partition: 0,
    })?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let mut losses = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_expected_loss_curve(&mut losses)?;

    assert!(close_relative(
        losses[apply.target as usize - 1],
        apply.diagnostics.expected_loss,
    ));
    Ok(())
}

#[test]
fn throughput_posterior_preserves_joint_curve_mass() -> Result<(), TestError> {
    let configuration = configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[50.0_f64, 100.0_f64], &[0.0_f64])?;
    let state = ScaleState::new(configuration, grid)?;
    let cell_count = usize::try_from(state.throughput_posterior_value_count())
        .map_err(|_| TestError::PlatformLimit)?;
    let mut cells = vec![ThroughputPosteriorCell::default(); cell_count];

    state.write_throughput_posterior(7.0_f64, &mut cells)?;

    assert!(close_relative(
        cells.iter().map(|cell| cell.probability).sum(),
        1.0_f64,
    ));
    assert!(
        cells
            .iter()
            .any(|cell| close_relative(cell.throughput_per_second, 50.0_f64))
    );
    assert!(
        cells
            .iter()
            .any(|cell| close_relative(cell.throughput_per_second, 70.0_f64))
    );
    Ok(())
}

#[test]
fn steady_state_step_allocates_no_memory() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    for partition in 0..configuration.partition_count {
        observation.push_cohort(Cohort {
            release_micros: u64::from(partition) * 10_000,
            deadline_micros: 2_000_000 + u64::from(partition) * 10_000,
            offered_events: 50.0_f64,
            partition,
        })?;
    }

    let allocation = allocation_counter::measure(|| {
        let decision = step(
            &mut state,
            &mut scratch,
            observation.observation(),
            ModelTime::from_micros(1),
        );
        black_box(decision);
    });

    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    Ok(())
}

#[test]
fn regressed_model_time_returns_hold() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut first = ObservationBuffer::new(&configuration)?;
    let mut second = ObservationBuffer::new(&configuration)?;
    let _first_decision = step(
        &mut state,
        &mut scratch,
        first.observation(),
        ModelTime::from_micros(2),
    );

    let decision = step(
        &mut state,
        &mut scratch,
        second.observation(),
        ModelTime::from_micros(1),
    );

    assert!(matches!(
        decision,
        ScaleDecision::Hold(hold) if hold.reason == HoldReason::ModelTimeRegressed
    ));
    Ok(())
}

#[test]
fn one_hot_partition_cannot_claim_capacity_from_other_replicas() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        offered_events: 160.0_f64,
        partition: 0,
    })?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    assert_eq!(apply.target, 1);
    assert!(apply.diagnostics.shortfall > 0.0_f64);
    Ok(())
}

#[test]
fn wide_cohort_cannot_hide_one_hot_partition_deadline() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 100_000_000,
        offered_events: 0.0_f64,
        partition: 0,
    })?;
    observation.push_cohort(Cohort {
        release_micros: 50_000_000,
        deadline_micros: 51_000_000,
        offered_events: 160.0_f64,
        partition: 0,
    })?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    assert_eq!(apply.target, 1);
    assert!(apply.diagnostics.shortfall > 0.0_f64);
    Ok(())
}

#[quickcheck]
fn extra_replicas_cannot_fix_one_hot_partition_overload(
    duration_seed: u16,
    excess_seed: u16,
) -> bool {
    let duration_micros = u64::from(duration_seed) % 1_000_000 + 1;
    let duration_seconds = Duration::from_micros(duration_micros).as_secs_f64();
    let excess = f64::from(excess_seed % 32 + 1) / 16.0_f64;
    let result = (|| -> Result<bool, TestError> {
        let configuration = configuration()?;
        let mut state = ScaleState::new(configuration.clone(), grid()?)?;
        let mut scratch = ScaleScratch::new(&configuration)?;
        let mut observation = ObservationBuffer::new(&configuration)?;
        observation.push_cohort(Cohort {
            release_micros: 0,
            deadline_micros: duration_micros,
            offered_events: duration_seconds
                * f64::from(configuration.slots_per_replica)
                * (1.0_f64 + excess)
                / 0.05_f64,
            partition: 0,
        })?;
        let decision = step(
            &mut state,
            &mut scratch,
            observation.observation(),
            ModelTime::from_micros(1),
        );
        let ScaleDecision::Apply(apply) = decision else {
            return Ok(false);
        };
        assert!(
            apply.target == 1 && apply.diagnostics.shortfall > 0.0_f64,
            "duration={duration_micros}, excess={excess}, target={}, shortfall={}",
            apply.target,
            apply.diagnostics.shortfall
        );
        Ok(true)
    })();
    matches!(result, Ok(true))
}

fn exhaustive_feasible(cohorts: &[WorkCohort], slots: f64) -> bool {
    for start in cohorts {
        for end in cohorts {
            if start.release_micros >= end.deadline_micros {
                continue;
            }
            let demand = cohorts
                .iter()
                .filter(|cohort| {
                    cohort.release_micros >= start.release_micros
                        && cohort.deadline_micros <= end.deadline_micros
                })
                .map(|cohort| cohort.work_slot_seconds)
                .sum::<f64>();
            let elapsed =
                Duration::from_micros(end.deadline_micros - start.release_micros).as_secs_f64();
            let supply = slots * elapsed;
            if demand > supply + f64::EPSILON {
                return false;
            }
        }
    }
    true
}

#[test]
fn lead_time_predictive_quantile_inverts_the_predictive_cdf() -> Result<(), TestError> {
    let state = ScaleState::new(configuration()?, grid()?)?;
    for direction in [TransitionDirection::Up, TransitionDirection::Down] {
        for probability in [0.1_f64, 0.5_f64, 0.9_f64] {
            let quantile = state.lead_time_predictive_quantile(direction, 3, probability);
            let cumulative = state.lead_time_predictive_cdf(direction, 3, quantile);
            assert!(
                (cumulative - probability).abs() < 1.0e-9_f64,
                "the predictive quantile must invert its CDF"
            );
        }
    }
    Ok(())
}

fn close_relative(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0_f64);
    (left - right).abs() <= 1.0e-12_f64 * scale
}

fn configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 16,
        partition_count: 16,
        replica_count_max: 32,
        slots_per_replica: 4,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01)?,
    })
}

fn grid() -> Result<CapacityGrid, TestError> {
    Ok(CapacityGrid::new(
        &[0.05, 0.1],
        &[50.0, 100.0],
        &[0.0, 1.0],
    )?)
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    ResourceWindow(#[from] crate::ResourceWindowError),
    #[error(transparent)]
    CapacityGrid(#[from] crate::CapacityGridError),
    #[error(transparent)]
    Posterior(#[from] crate::PosteriorError),
    #[error(transparent)]
    Configuration(#[from] crate::ConfigurationError),
    #[error(transparent)]
    DecisionCurve(#[from] crate::DecisionCurveError),
    #[error(transparent)]
    Observation(#[from] crate::ObservationError),
    #[error(transparent)]
    TransitionEvidence(#[from] crate::TransitionEvidenceError),
    #[error("the model held when the test required an applied decision")]
    UnexpectedHold,
    #[error("a test count exceeds the platform limit")]
    PlatformLimit,
}
