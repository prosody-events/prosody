use std::hint::black_box;
use std::slice;
use std::time::Duration;

use fearless_simd::Level;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use thiserror::Error;

use crate::arrival::{ArrivalEvidence, ArrivalFactor};
use crate::capacity::CapacityFactor;
use crate::change_point::ChangePointKernel;
use crate::edf::{
    ArrivalPath, CandidateLoss, CandidateSupply, EdfScratch, prepare, shortfall,
    shortfall_prepared_common_release_candidates,
};
use crate::lead_time::LeadTimeFactor;
use crate::model::mixed_event_supply;
use crate::partition::PartitionFactor;
use crate::types::{CalendarForecast, WorkCohort};
use crate::{
    ActuationCommitment, AttemptOutcomeCounts, AttemptOutcomeEvidence, BacklogCohort,
    CalendarArtifactId, CalendarRateSegment, CapacityCurve, CapacityGrid, CapacityPrior, Cohort,
    Configuration, DemandClass, HoldReason, ModelTime, ObservationBuffer, PosteriorQuery,
    RandomStream, ReliabilityPrior, ResourceWindow, ScaleDecision, ScaleScratch, ScaleState,
    ServiceObjective, ThroughputPosteriorCell, TransitionDirection, TransitionEvidence,
    TransitionPrior, step,
};

const NO_FUTURE_ARRIVALS: ArrivalPath<'static> = ArrivalPath {
    start_seconds: 0.0_f64,
    end_seconds: &[f64::MAX],
    rates: &[0.0_f64],
};
const TEN_FUTURE_ARRIVALS_PER_SECOND: ArrivalPath<'static> = ArrivalPath {
    start_seconds: 0.0_f64,
    end_seconds: &[f64::MAX],
    rates: &[10.0_f64],
};

#[quickcheck]
fn change_point_kernel_satisfies_the_semigroup_law(
    rate_basis_points: u16,
    first_millis: u16,
    second_millis: u16,
) -> bool {
    let rate = f64::from(rate_basis_points) / 10_000.0_f64;
    let first = Duration::from_millis(u64::from(first_millis));
    let second = Duration::from_millis(u64::from(second_millis));
    let kernel = ChangePointKernel::new(rate);
    let combined = kernel.probabilities(first + second).retained;
    let successive = kernel.probabilities(first).retained * kernel.probabilities(second).retained;

    (combined - successive).abs() <= 16.0_f64 * f64::EPSILON
}

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
fn backlog_evidence_has_one_positive_observation_per_partition_and_class() -> Result<(), TestError>
{
    assert!(BacklogCohort::new(10, 0, 0, 0, DemandClass::Normal).is_err());
    assert!(BacklogCohort::new(10, 11, 1, 0, DemandClass::Normal).is_err());
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let backlog = BacklogCohort::new(10, 0, 1, 0, DemandClass::Normal)?;

    observation.set_backlog(backlog)?;

    assert!(observation.set_backlog(backlog).is_err());
    Ok(())
}

#[test]
fn lead_time_updates_only_the_matching_direction_and_delta() -> Result<(), TestError> {
    let simd_level = Level::new();
    let prior = TransitionPrior::broad_fallback();
    let mut factor = LeadTimeFactor::new(&prior);
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
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    factor.update(simd_level, &ResourceWindow::new(8.0, 1.0, 80)?);

    assert_eq!(factor.cap(4, 32, 0.01_f64), 32);
    Ok(())
}

#[test]
fn linear_evidence_retains_a_no_knee_explanation() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[10.0_f64, 20.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
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
    let mut factor = ArrivalFactor::new(crate::ArrivalPrior::broad_fallback());
    factor.update(ArrivalEvidence::new(4, 1_000_000), None, 1_000_000);
    let mass = (0_u32..128)
        .map(|count| factor.predictive_probability(count, 1.0_f64))
        .sum::<f64>();
    assert!((mass - 1.0_f64).abs() < 1.0e-12_f64);
}

#[test]
fn arrival_change_point_replaces_stale_rate_evidence() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(100.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64, 1_024)?;
    let mut factor = ArrivalFactor::new(prior);
    for _ in 0_u32..100 {
        factor.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);
    }
    let old_rate = factor.expected_rate(1_000_000);
    for _ in 0_u32..8 {
        factor.update(ArrivalEvidence::new(400, 1_000_000), None, 1_000_000);
    }
    assert!(
        old_rate < 110.0_f64 && factor.expected_rate(1_000_000) > 350.0_f64,
        "contrary evidence must replace a stale segment"
    );
    Ok(())
}

#[test]
fn arrival_change_point_normalizes_after_an_extreme_rate_change() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64, 1_024)?;
    let mut factor = ArrivalFactor::new(prior);

    factor.update(ArrivalEvidence::new(10_000, 1_000_000), None, 1_000_000);

    assert!(factor.expected_rate(1_000_000) > 4_000.0_f64);
    Ok(())
}

#[test]
fn missing_arrival_prediction_is_cadence_invariant() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(1.0_f64, 1.0_f64, 2.0_f64.ln(), 1_024)?;
    let mut coarse = ArrivalFactor::new(prior);
    let mut fine = ArrivalFactor::new(prior);
    coarse.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);
    fine.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);

    for tick in 1_u64..1_000 {
        let _prediction = fine.expected_rate(1_000_000 + tick * 1_000);
    }
    let coarse_prediction = coarse.expected_rate(2_000_000);
    let fine_prediction = fine.expected_rate(2_000_000);

    assert!((coarse_prediction - fine_prediction).abs() < 1.0e-12_f64);
    assert!((coarse_prediction - 25.75_f64).abs() < 1.0e-12_f64);
    Ok(())
}

#[test]
fn missing_interval_weakens_stale_arrival_evidence_before_the_next_update() -> Result<(), TestError>
{
    let prior = crate::ArrivalPrior::new(1.0_f64, 1.0_f64, 2.0_f64.ln(), 1_024)?;
    let mut contiguous = ArrivalFactor::new(prior);
    let mut missing = ArrivalFactor::new(prior);
    contiguous.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);
    missing.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);

    contiguous.update(ArrivalEvidence::new(30, 1_000_000), None, 2_000_000);
    missing.update(ArrivalEvidence::new(30, 1_000_000), None, 12_000_000);

    let missing_rate = missing.expected_rate(12_000_000);
    let contiguous_rate = contiguous.expected_rate(2_000_000);
    assert!(
        missing_rate < contiguous_rate,
        "missing rate={missing_rate}, contiguous rate={contiguous_rate}"
    );
    Ok(())
}

#[test]
fn live_evidence_selects_a_supported_calendar_model() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64, 1_024)?;
    let segment = CalendarRateSegment::new(7, 0, 60_000_000, 1_000.0_f64, 10.0_f64)?;
    let forecast = CalendarForecast {
        artifact: CalendarArtifactId(11),
        prior_probability: 0.5_f64,
        segments: slice::from_ref(&segment),
    };
    let mut factor = ArrivalFactor::new(prior);

    for second in 1_u64..=10 {
        factor.update(
            ArrivalEvidence::new(100, 1_000_000),
            Some(forecast),
            second * 1_000_000,
        );
    }

    assert!(factor.calendar_model_probability() > 0.9_f64);
    Ok(())
}

#[test]
fn live_evidence_rejects_a_stale_calendar_model() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64, 1_024)?;
    let segment = CalendarRateSegment::new(7, 0, 60_000_000, 1_000.0_f64, 10.0_f64)?;
    let forecast = CalendarForecast {
        artifact: CalendarArtifactId(11),
        prior_probability: 0.5_f64,
        segments: slice::from_ref(&segment),
    };
    let mut factor = ArrivalFactor::new(prior);

    for second in 1_u64..=10 {
        factor.update(
            ArrivalEvidence::new(1_000, 1_000_000),
            Some(forecast),
            second * 1_000_000,
        );
    }

    assert!(factor.calendar_model_probability() < 0.1_f64);
    Ok(())
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
fn partition_posterior_does_not_depend_on_evidence_segmentation() -> Result<(), TestError> {
    let mut combined = PartitionFactor::new(4)?;
    let mut segmented = PartitionFactor::new(4)?;
    combined.update(&[90, 10, 4, 0]);
    segmented.update(&[40, 4, 1, 0]);
    segmented.update(&[50, 6, 3, 0]);
    let mut combined_shares = [0.0_f64; 4];
    let mut segmented_shares = [0.0_f64; 4];

    assert!(combined.write_expected_shares(&mut combined_shares));
    assert!(segmented.write_expected_shares(&mut segmented_shares));
    assert!(
        combined_shares
            .iter()
            .zip(segmented_shares)
            .all(|(left, right)| close_relative(*left, right))
    );
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
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
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
    let factor = CapacityFactor::new(grid, 0.0_f64);

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
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
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
        factor.transition(Duration::from_secs(1));
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
fn capacity_transition_is_cadence_invariant() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(
        &[0.01_f64, 0.1_f64],
        &[100.0_f64, 1_000.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let cell_count = grid.cell_count() as usize;
    let change_rate = 2.0_f64.ln();
    let mut coarse = CapacityFactor::new(grid.clone(), change_rate);
    let mut fine = CapacityFactor::new(grid, change_rate);
    let evidence = ResourceWindow::new(32.0_f64, 10.0_f64, 3_200)?;
    coarse.update(simd_level, &evidence);
    fine.update(simd_level, &evidence);

    coarse.transition(Duration::from_secs(1));
    for _ in 0_u32..1_000 {
        fine.transition(Duration::from_millis(1));
    }

    let mut coarse_cells = vec![ThroughputPosteriorCell::default(); cell_count];
    let mut fine_cells = vec![ThroughputPosteriorCell::default(); cell_count];
    coarse.write_throughput_posterior(32.0_f64, &mut coarse_cells)?;
    fine.write_throughput_posterior(32.0_f64, &mut fine_cells)?;
    assert!(coarse_cells.iter().zip(fine_cells).all(|(left, right)| {
        close_relative(left.throughput_per_second, right.throughput_per_second)
            && (left.probability - right.probability).abs() < 1.0e-12_f64
    }));
    Ok(())
}

#[test]
fn actuation_transition_is_cadence_invariant() -> Result<(), TestError> {
    let prior = TransitionPrior::new(
        [15.0_f64, 30.0_f64, 60.0_f64, 120.0_f64],
        [0.1_f64, 0.3_f64, 0.6_f64],
        [1.0_f64; 12],
        2.0_f64.ln(),
    )?;
    let mut coarse = LeadTimeFactor::new(&prior);
    let mut fine = LeadTimeFactor::new(&prior);
    let (coarse_evidence, _) =
        TransitionEvidence::completed(TransitionDirection::Up, 2, 30_000_000)?.consume();
    let (fine_evidence, _) =
        TransitionEvidence::completed(TransitionDirection::Up, 2, 30_000_000)?.consume();
    coarse.update(Level::new(), coarse_evidence);
    fine.update(Level::new(), fine_evidence);

    coarse.transition(Duration::from_secs(1));
    for _ in 0_u32..1_000 {
        fine.transition(Duration::from_millis(1));
    }

    let mut values = [0.0_f64; 4];
    let mut coarse_probability = [0.0_f64; 4];
    let mut fine_probability = [0.0_f64; 4];
    assert!(coarse.write_posterior(
        TransitionDirection::Up,
        2,
        &mut values,
        &mut coarse_probability,
    ));
    assert!(fine.write_posterior(
        TransitionDirection::Up,
        2,
        &mut values,
        &mut fine_probability,
    ));
    assert!(
        coarse_probability
            .iter()
            .zip(fine_probability)
            .all(|(left, right)| (left - right).abs() < 1.0e-12_f64)
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
        PosteriorQuery::NormalRetryProbability,
        PosteriorQuery::FailureRetryProbability,
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
fn attempt_outcomes_update_only_their_retry_factors() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_attempt_outcomes(AttemptOutcomeEvidence::new(
        AttemptOutcomeCounts::new(80, 10, 10, 0),
        AttemptOutcomeCounts::new(10, 5, 0, 5),
    ))?;
    let _decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );

    let normal = posterior_mean(&state, PosteriorQuery::NormalRetryProbability)?;
    let failure = posterior_mean(&state, PosteriorQuery::FailureRetryProbability)?;
    assert!(normal < failure);
    assert!(failure < 0.5_f64);
    Ok(())
}

#[test]
fn mixed_demand_supply_respects_work_conservation_and_failure_share() {
    let failure_only = mixed_event_supply(100.0_f64, 0.2_f64, 0.5_f64, 0.3_f64, 0.0_f64, 10.0_f64);
    assert!(close_relative(failure_only, 50.0_f64));

    let mixed = mixed_event_supply(100.0_f64, 0.2_f64, 0.5_f64, 0.3_f64, 10.0_f64, 10.0_f64);
    assert!(close_relative(mixed, 25.0_f64));
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
    let factor = CapacityFactor::new(grid, 0.0_f64);
    assert!(close_relative(factor.no_knee_probability(), 0.5_f64));
    Ok(())
}

#[test]
fn identified_plateau_activates_a_knee_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
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
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    factor.update(simd_level, &ResourceWindow::new(5.0_f64, 1.0_f64, 50)?);
    factor.update(simd_level, &ResourceWindow::new(20.0_f64, 1.0_f64, 200)?);
    assert_eq!(factor.cap(2, 32, 0.01_f64), 32);
    Ok(())
}

#[test]
fn fewer_completed_attempts_cannot_loosen_a_cap() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new(
        &[0.1_f64],
        &[50.0_f64, 100.0_f64, 200.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let mut healthy = CapacityFactor::new(grid.clone(), 0.0_f64);
    let mut failing = CapacityFactor::new(grid, 0.0_f64);
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
    assert!((1..=configuration.replica_count_max).contains(&apply.target));
    Ok(())
}

#[test]
fn predictive_arrivals_request_capacity_before_work_is_released() -> Result<(), TestError> {
    let configuration = Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        partition_count: 4,
        replica_count_max: 4,
        slots_per_replica: 1,
        posterior_sample_count: 1_024,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::broad_fallback(),
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01_f64)?,
    };
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_arrivals(200, 10_000_000)?;
    observation.set_attempt_outcomes(AttemptOutcomeEvidence::new(
        AttemptOutcomeCounts::new(1_000_000, 0, 0, 0),
        AttemptOutcomeCounts::new(1_000_000, 0, 0, 0),
    ))?;
    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };

    assert!(
        apply.target > 1,
        "target={}, cap={}, loss={}",
        apply.target,
        apply.cap,
        apply.diagnostics.expected_loss
    );
    Ok(())
}

#[test]
fn joint_capacity_samples_match_direct_enumeration() -> Result<(), TestError> {
    let configuration = Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        partition_count: 1,
        replica_count_max: 1,
        slots_per_replica: 1,
        posterior_sample_count: 1_024,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64, 1_024)?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
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
        demand_class: DemandClass::Normal,
    })?;
    observation.set_attempt_outcomes(AttemptOutcomeEvidence::new(
        AttemptOutcomeCounts::new(1_000_000, 0, 0, 0),
        AttemptOutcomeCounts::new(1_000_000, 0, 0, 0),
    ))?;
    observation.set_arrivals(0, u64::MAX)?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let no_knee_area = 75.0_f64 * 75.0_f64 / (2.0_f64 * 1_000.0_f64);
    let low_knee_area = 75.0_f64 * 75.0_f64 / (2.0_f64 * 50.0_f64);
    let high_knee_area = 75.0_f64 * 75.0_f64 / (2.0_f64 * 100.0_f64);
    let exact_loss =
        (0.5_f64 * no_knee_area + 0.25_f64 * low_knee_area + 0.25_f64 * high_knee_area) / 75.0_f64;

    assert!((apply.diagnostics.expected_loss - exact_loss).abs() < 1.0e-5_f64);
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
        calendar_segment_count_max: 2,
        partition_count: 2,
        replica_count_max: 2,
        slots_per_replica: 1,
        posterior_sample_count: 64,
        failure_service_weight: 0.3_f64,
        arrival_prior: negligible_arrival_prior()?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
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
            demand_class: DemandClass::Normal,
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
    let mut delay_area = [0.0_f64];

    let supply = CandidateSupply {
        before: 1.0_f64,
        during: &[0.5_f64],
        after: &[2.0_f64],
        pause_seconds: &[0.0_f64],
        ready_seconds: &[1.0_f64],
    };
    let mut results = CandidateLoss {
        service_balance: &mut credit,
        shortfall: &mut loss,
        delay_area: &mut delay_area,
    };
    shortfall_prepared_common_release_candidates(
        Level::new(),
        &cohorts,
        &supply,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        0.0_f64,
        1.0_f64,
        &mut results,
        &scratch,
    );

    assert!(close_relative(loss[0], 1.0_f64 / 3.0_f64));
    Ok(())
}

#[test]
fn missed_work_remains_service_debt_and_rewards_faster_recovery() -> Result<(), TestError> {
    let cohorts = [WorkCohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        work_slot_seconds: 1.0_f64,
        partition: 0,
    }];
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let supply = CandidateSupply {
        before: 1.0_f64,
        during: &[1.0_f64, 2.0_f64],
        after: &[1.0_f64, 2.0_f64],
        pause_seconds: &[0.0_f64; 2],
        ready_seconds: &[0.0_f64; 2],
    };
    let mut balance = [0.0_f64; 2];
    let mut shortfall = [0.0_f64; 2];
    let mut delay_area = [0.0_f64; 2];
    let mut results = CandidateLoss {
        service_balance: &mut balance,
        shortfall: &mut shortfall,
        delay_area: &mut delay_area,
    };

    shortfall_prepared_common_release_candidates(
        Level::new(),
        &cohorts,
        &supply,
        0.75_f64,
        &NO_FUTURE_ARRIVALS,
        0.0_f64,
        1.0_f64,
        &mut results,
        &scratch,
    );

    assert!(shortfall[0] > shortfall[1], "shortfall: {shortfall:?}");
    assert!(delay_area[0] > delay_area[1]);
    assert!(balance[0] < 0.0_f64);
    Ok(())
}

#[test]
fn debt_only_observation_rewards_faster_recovery() -> Result<(), TestError> {
    let cohorts = [];
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let supply = CandidateSupply {
        before: 1.0_f64,
        during: &[1.0_f64, 2.0_f64],
        after: &[1.0_f64, 2.0_f64],
        pause_seconds: &[0.0_f64; 2],
        ready_seconds: &[0.0_f64; 2],
    };
    let mut balance = [0.0_f64; 2];
    let mut shortfall = [0.0_f64; 2];
    let mut delay_area = [0.0_f64; 2];
    let mut results = CandidateLoss {
        service_balance: &mut balance,
        shortfall: &mut shortfall,
        delay_area: &mut delay_area,
    };

    shortfall_prepared_common_release_candidates(
        Level::new(),
        &cohorts,
        &supply,
        2.0_f64,
        &NO_FUTURE_ARRIVALS,
        0.0_f64,
        2.0_f64,
        &mut results,
        &scratch,
    );

    assert!(delay_area[0] > 0.0_f64);
    assert!(delay_area[0] > delay_area[1]);
    Ok(())
}

#[test]
fn predictive_arrivals_consume_service_while_debt_drains() -> Result<(), TestError> {
    let cohorts = [];
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let supply = CandidateSupply {
        before: 10.0_f64,
        during: &[10.0_f64, 20.0_f64],
        after: &[10.0_f64, 20.0_f64],
        pause_seconds: &[0.0_f64; 2],
        ready_seconds: &[0.0_f64; 2],
    };
    let mut balance = [0.0_f64; 2];
    let mut shortfall = [0.0_f64; 2];
    let mut delay_area = [0.0_f64; 2];
    let mut results = CandidateLoss {
        service_balance: &mut balance,
        shortfall: &mut shortfall,
        delay_area: &mut delay_area,
    };

    shortfall_prepared_common_release_candidates(
        Level::new(),
        &cohorts,
        &supply,
        100.0_f64,
        &TEN_FUTURE_ARRIVALS_PER_SECOND,
        0.0_f64,
        10.0_f64,
        &mut results,
        &scratch,
    );

    assert!(close_relative(delay_area[0], 1_000.0_f64));
    assert!(close_relative(delay_area[1], 500.0_f64));
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
        demand_class: DemandClass::Normal,
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
    let mut probabilities = vec![0.0_f64; configuration.replica_count_max as usize];
    scratch.write_decision_curve(&mut losses, &mut probabilities)?;

    assert!(close_relative(
        losses[apply.target as usize - 1],
        apply.diagnostics.expected_loss,
    ));
    assert!(
        probabilities
            .iter()
            .all(|probability| (0.0_f64..=1.0_f64).contains(probability)),
        "each candidate pass probability must be normalized"
    );
    let required = 1.0_f64 - configuration.objective.epsilon();
    if let Some(first_feasible) = probabilities
        .iter()
        .position(|probability| *probability >= required)
    {
        assert_eq!(apply.target as usize - 1, first_feasible);
    }
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
            demand_class: DemandClass::Normal,
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
    let mut configuration = configuration()?;
    configuration.arrival_prior = negligible_arrival_prior()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 1_000_000,
        offered_events: 160.0_f64,
        partition: 0,
        demand_class: DemandClass::Normal,
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
    let mut configuration = configuration()?;
    configuration.arrival_prior = negligible_arrival_prior()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.push_cohort(Cohort {
        release_micros: 0,
        deadline_micros: 100_000_000,
        offered_events: 0.0_f64,
        partition: 0,
        demand_class: DemandClass::Normal,
    })?;
    observation.push_cohort(Cohort {
        release_micros: 50_000_000,
        deadline_micros: 51_000_000,
        offered_events: 160.0_f64,
        partition: 0,
        demand_class: DemandClass::Normal,
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
            demand_class: DemandClass::Normal,
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

#[test]
fn incomplete_actuation_uses_the_conditional_remaining_time() {
    let factor = LeadTimeFactor::new(&TransitionPrior::broad_fallback());
    let elapsed_seconds = 20.0_f64;
    let direction = TransitionDirection::Up;
    let delta = 1;
    let mut coordinate = RandomStream::new(91);
    let uniform = coordinate.open_unit_f64();
    let elapsed_cdf = factor.predictive_cdf(direction, delta, elapsed_seconds);
    let expected_cdf = elapsed_cdf + uniform * (1.0_f64 - elapsed_cdf);
    let mut draw = RandomStream::new(91);
    let remaining = factor.sample_remaining_seconds(direction, delta, elapsed_seconds, &mut draw);
    let actual_cdf = factor.predictive_cdf(direction, delta, elapsed_seconds + remaining);

    assert!(
        (actual_cdf - expected_cdf).abs() < 1.0e-12_f64,
        "the remaining-time draw must invert the conditional survival distribution"
    );
}

#[test]
fn larger_candidates_inherit_useful_pending_capacity() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = ScaleScratch::new(&configuration)?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_current_replicas(2)?;
    observation.push_actuation_commitment(ActuationCommitment::new(
        2,
        3,
        ModelTime::from_micros(1),
    )?)?;

    let _decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(20_000_000),
    );

    assert_eq!(scratch.trajectory_targets(2), Some([].as_slice()));
    assert_eq!(scratch.trajectory_targets(3), Some([3].as_slice()));
    let targets = scratch
        .trajectory_targets(4)
        .ok_or(TestError::MissingTrajectory)?;
    assert!(matches!(targets, [4] | [3, 4]));
    Ok(())
}

fn close_relative(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0_f64);
    (left - right).abs() <= 1.0e-12_f64 * scale
}

fn posterior_mean(state: &ScaleState, query: PosteriorQuery) -> Result<f64, TestError> {
    let count = usize::try_from(state.posterior_value_count(query)?)
        .map_err(|_| crate::ConfigurationError::PlatformLimit)?;
    let mut values = vec![0.0_f64; count];
    let mut probabilities = vec![0.0_f64; count];
    state.write_posterior(query, &mut values, &mut probabilities)?;
    Ok(values
        .iter()
        .zip(probabilities)
        .map(|(value, probability)| value * probability)
        .sum())
}

fn configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 16,
        calendar_segment_count_max: 16,
        partition_count: 16,
        replica_count_max: 32,
        slots_per_replica: 4,
        posterior_sample_count: 64,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::broad_fallback(),
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01)?,
    })
}

fn negligible_arrival_prior() -> Result<crate::ArrivalPrior, TestError> {
    Ok(crate::ArrivalPrior::new(
        1.0_f64,
        1.0e12_f64,
        1.0e-12_f64,
        1_024,
    )?)
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
    ArrivalPrior(#[from] crate::ArrivalPriorError),
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
    #[error(transparent)]
    TransitionPrior(#[from] crate::TransitionPriorError),
    #[error("the model held when the test required an applied decision")]
    UnexpectedHold,
    #[error("a test count exceeds the platform limit")]
    PlatformLimit,
    #[error("the candidate trajectory is missing")]
    MissingTrajectory,
}
