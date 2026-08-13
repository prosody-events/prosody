use std::hint::black_box;
use std::slice;
use std::time::Duration;

use fearless_simd::Level;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use statrs::distribution::{ContinuousCDF, Gamma};
use thiserror::Error;

use crate::arrival::{ArrivalEvidence, ArrivalFactor, ChangeHazard, RateOccupancy, rate_bin};
use crate::capacity::CapacityFactor;
use crate::change_point::ChangePointKernel;
use crate::controller::{
    DecisionRandomDomain, decision_random, minimal_moved_partitions, mixed_event_supply,
};
use crate::edf::{
    ArrivalPath, EdfOutcome, EdfScratch, EvaluationWindow, SupplyStep, SupplyTrajectory,
    evaluate_general_trajectory, evaluate_prepared_step, evaluate_prepared_trajectory, prepare,
    required_capacity_prepared,
};
use crate::lead_time::LeadTimeFactor;
use crate::partition::PartitionFactor;
use crate::planning::terminal_replica_seconds;
use crate::types::{CalendarColumns, CalendarForecast, WorkCohorts};
use crate::{
    ActuationCommitment, AttemptOutcomeCounts, AttemptOutcomeEvidence, BacklogCohort,
    CalendarArtifactId, CalendarRateSegment, CapacityCurve, CapacityGrid, CapacityPrior, Cohort,
    Configuration, ConfigurationError, DemandClass, HoldReason, ModelTime, ObservationBuffer,
    PosteriorQuery, RandomStream, ReliabilityPrior, ResourceWindow, ScaleDecision, ScaleState,
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

#[test]
fn service_objective_rejects_invalid_replica_second_delay_rates() {
    for rate in [
        f64::NEG_INFINITY,
        -f64::MIN_POSITIVE,
        0.0_f64,
        f64::INFINITY,
        f64::NAN,
    ] {
        assert!(
            ServiceObjective::new(1_000_000, 0.01_f64, rate).is_err(),
            "rate={rate}"
        );
    }
}

fn evaluate_constant_supply(
    cohorts: &WorkCohorts,
    capacity: f64,
    horizon_micros: u64,
    initial_debt_work: f64,
    arrivals: &ArrivalPath<'_>,
    scratch: &mut EdfScratch,
) -> EdfOutcome {
    evaluate_prepared_step(
        cohorts,
        SupplyStep {
            before: capacity,
            during: capacity,
            after: capacity,
            pause_micros: 0,
            ready_micros: 0,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros,
            initial_debt_work,
            deadline_budget_micros: 1_000_000,
        },
        arrivals,
        scratch,
    )
}

#[quickcheck]
fn admissible_closure_is_horizon_invariant(work_seed: u16, capacity_seed: u8) -> bool {
    let work = 100.0_f64 + f64::from(work_seed);
    let capacity = f64::from(capacity_seed % 10 + 1);
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, work, 0);
    let Ok(mut scratch) = EdfScratch::new(1) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);
    let first = evaluate_constant_supply(
        &cohorts,
        capacity,
        2_000_000,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let second = evaluate_constant_supply(
        &cohorts,
        capacity,
        3_000_000,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let first_late = first.late_area + first.terminal_late_area;
    let second_late = second.late_area + second.terminal_late_area;
    let first_resource =
        2.0_f64 + terminal_replica_seconds(2_000_000, first.drain_seconds, 3_000_000, 1);
    let second_resource =
        3.0_f64 + terminal_replica_seconds(3_000_000, second.drain_seconds, 3_000_000, 1);

    close_relative(first_late, second_late) && close_relative(first_resource, second_resource)
}

#[quickcheck]
fn drain_inside_horizon_has_no_terminal_terms(work_seed: u16, capacity_seed: u8) -> bool {
    let work = f64::from(work_seed % 1_000 + 1);
    let capacity = f64::from(capacity_seed % 20 + 1);
    let drain_seconds = work / capacity;
    let horizon_micros = ((drain_seconds + 2.0_f64) * 1_000_000.0_f64).ceil() as u64;
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, work, 0);
    let Ok(mut scratch) = EdfScratch::new(1) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);
    let outcome = evaluate_constant_supply(
        &cohorts,
        capacity,
        horizon_micros,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let late_work = (work - capacity).max(0.0_f64);
    let expected_late_area = late_work * late_work / (2.0_f64 * capacity);

    outcome.terminal_late_area.total_cmp(&0.0_f64).is_eq()
        && outcome.drain_seconds.total_cmp(&0.0_f64).is_eq()
        && close_relative(outcome.late_area, expected_late_area)
}

#[test]
fn forecast_work_that_waits_past_its_deadline_fails_the_chance_test() -> Result<(), TestError> {
    let cohorts = WorkCohorts::new(0);
    let mut scratch = EdfScratch::new(0)?;
    prepare(&cohorts, &mut scratch);
    let arrivals = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[4.0_f64],
        rates: &[10.0_f64],
    };

    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: 0.0_f64,
            during: 0.0_f64,
            after: 20.0_f64,
            pause_micros: 0,
            ready_micros: 2_000_000,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 4_000_000,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &arrivals,
        &mut scratch,
    );

    assert!(outcome.shortfall > 0.0_f64);
    assert!(outcome.drain_seconds.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[test]
fn forecast_path_stops_arrivals_at_its_declared_end() -> Result<(), TestError> {
    let cohorts = WorkCohorts::new(0);
    let mut scratch = EdfScratch::new(0)?;
    prepare(&cohorts, &mut scratch);
    let arrivals = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[1.0_f64],
        rates: &[10.0_f64],
    };

    let outcome = evaluate_constant_supply(
        &cohorts,
        0.0_f64,
        3_000_000,
        0.0_f64,
        &arrivals,
        &mut scratch,
    );

    assert!(outcome.drain_seconds.is_infinite());
    Ok(())
}

#[quickcheck]
fn edf_counts_missed_work_and_bounds_late_area(work_seed: u16, supply_seed: u16) -> bool {
    let work = f64::from(work_seed % 10_000 + 1);
    let supply = f64::from(supply_seed % 1_000 + 1);
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, work, 0);
    let Ok(mut scratch) = EdfScratch::new(1) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);
    let drain_seconds = work / supply;
    let horizon_micros = ((2.0_f64 + drain_seconds) * 1_000_000.0_f64) as u64;
    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: supply,
            during: supply,
            after: supply,
            pause_micros: 0,
            ready_micros: 0,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let missed = (work - supply).max(0.0_f64);
    let late_seconds = missed / supply;
    let expected_late_area = missed * late_seconds - 0.5_f64 * supply * late_seconds * late_seconds;
    close_relative(outcome.missed_work, missed)
        && close_relative(outcome.late_area, expected_late_area)
        && outcome.terminal_late_area.total_cmp(&0.0_f64).is_eq()
}

#[test]
fn edf_prices_preexisting_overdue_work() -> Result<(), TestError> {
    let cohorts = WorkCohorts::new(0);
    let mut scratch = EdfScratch::new(0)?;
    prepare(&cohorts, &mut scratch);
    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: 5.0_f64,
            during: 5.0_f64,
            after: 5.0_f64,
            pause_micros: 0,
            ready_micros: 0,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 1_000_000,
            initial_debt_work: 5.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(outcome.missed_work.total_cmp(&0.0_f64).is_eq());
    // Five work units drain at five units per second. The late triangle is
    // 5 * 1 / 2 = 2.5 event-seconds.
    assert!(outcome.late_area.total_cmp(&2.5_f64).is_eq());
    assert!(outcome.terminal_late_area.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[test]
fn edf_does_not_report_lateness_after_work_finishes_before_its_deadline() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, 188.0_f64, 0);
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: 686.0_f64,
            during: 686.0_f64,
            after: 686.0_f64,
            pause_micros: 0,
            ready_micros: 0,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 2_274_052,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(outcome.missed_work.total_cmp(&0.0_f64).is_eq());
    assert!(outcome.late_area.total_cmp(&0.0_f64).is_eq());
    assert!(outcome.terminal_late_area.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[test]
fn edf_counts_a_late_cohort_after_an_earlier_wide_interval() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(2);
    cohorts.push_values(0, 100_000_000, 0.0_f64, 0);
    cohorts.push_values(50_000_000, 51_000_000, 16.0_f64, 0);
    let mut scratch = EdfScratch::new(2)?;
    prepare(&cohorts, &mut scratch);
    let arrivals = ArrivalPath {
        start_seconds: 0.000_001_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: 3.806_668_352_250_828_2_f64,
            during: 3.806_668_352_250_828_2_f64,
            after: 3.806_668_352_250_828_2_f64,
            pause_micros: 1,
            ready_micros: 1,
        },
        EvaluationWindow {
            start_micros: 1,
            horizon_micros: 433_240_174,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &arrivals,
        &mut scratch,
    );

    let missed = 16.0_f64 - 3.806_668_352_250_828_2_f64;
    let late_seconds = missed / 3.806_668_352_250_828_2_f64;
    let expected_late_area =
        missed * late_seconds - 0.5_f64 * 3.806_668_352_250_828_2_f64 * late_seconds * late_seconds;
    assert!(close_relative(outcome.missed_work, missed));
    assert!(close_relative(outcome.late_area, expected_late_area));
    Ok(())
}

#[test]
fn decision_random_coordinates_do_not_shift_between_factors() {
    let mut first_arrival = decision_random(17, DecisionRandomDomain::Arrival);
    let mut first_lead = decision_random(17, DecisionRandomDomain::LeadTime);
    let expected_lead = first_lead.next_u64();
    for _ in 0_u8..100 {
        let _ = first_arrival.next_u64();
    }

    let mut second_lead = decision_random(17, DecisionRandomDomain::LeadTime);
    let mut other_scenario_lead = decision_random(18, DecisionRandomDomain::LeadTime);
    let mut reliability = decision_random(17, DecisionRandomDomain::Reliability);

    assert_eq!(second_lead.next_u64(), expected_lead);
    assert_ne!(other_scenario_lead.next_u64(), expected_lead);
    assert_ne!(reliability.next_u64(), expected_lead);
}

#[test]
fn trajectory_counts_a_cohort_released_before_its_horizon() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(50_000_000, 51_000_000, 160.0_f64, 0);
    let trajectory = SupplyTrajectory {
        initial: 45.0_f64,
        pause_seconds: &[77.0_f64],
        ready_seconds: &[78.0_f64],
        during: &[20.0_f64],
        after: &[45.0_f64],
    };
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);

    let outcome = evaluate_prepared_trajectory(
        &cohorts,
        &trajectory,
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 77_000_000,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(outcome.delay_area > 0.0_f64);
    assert!(outcome.drain_seconds.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[quickcheck]
fn common_cohort_trajectory_matches_general_edf(
    count_seed: u8,
    work_seed: u16,
    debt_seed: u8,
    supply_seed: u8,
) -> bool {
    let count = usize::from(count_seed % 8 + 1);
    let work = f64::from(work_seed % 1_000) / 10.0_f64;
    let debt = f64::from(debt_seed);
    let supply = f64::from(supply_seed) + 1.0_f64;
    let mut cohorts = WorkCohorts::new(count);
    for partition in 0..count {
        cohorts.push_values(250_000, 1_500_000, work, partition as u32);
    }
    let trajectory = SupplyTrajectory {
        initial: supply,
        pause_seconds: &[0.5_f64],
        ready_seconds: &[1.0_f64],
        during: &[supply * 0.5_f64],
        after: &[supply * 1.5_f64],
    };
    let Ok(mut scratch) = EdfScratch::new(count as u32) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);
    let fast = evaluate_prepared_trajectory(
        &cohorts,
        &trajectory,
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 2_000_000,
            initial_debt_work: debt,
            deadline_budget_micros: 1_500_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let general = evaluate_general_trajectory(
        &cohorts,
        &trajectory,
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 2_000_000,
            initial_debt_work: debt,
            deadline_budget_micros: 1_500_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    let matches = close_relative(fast.shortfall, general.shortfall)
        && close_relative(fast.delay_area, general.delay_area)
        && close_relative(fast.drain_seconds, general.drain_seconds);
    assert!(matches, "fast={fast:?}, general={general:?}");
    true
}

#[quickcheck]
fn ordered_deadline_trajectory_matches_general_edf(
    count_seed: u8,
    gap_seed: u8,
    work_seed: u16,
    supply_seed: u8,
) -> bool {
    let count = usize::from(count_seed % 16 + 1);
    let gap_micros = u64::from(gap_seed) * 10_000 + 1;
    let work = f64::from(work_seed % 1_000) / 10.0_f64;
    let supply = f64::from(supply_seed) + 1.0_f64;
    let mut cohorts = WorkCohorts::new(count);
    for cohort in 0..count {
        let release_micros = cohort as u64 * gap_micros;
        cohorts.push_values(
            release_micros,
            release_micros + 1_500_000,
            work + cohort_fraction(cohort),
            cohort as u32,
        );
    }
    let trajectory = SupplyTrajectory {
        initial: supply,
        pause_seconds: &[0.5_f64],
        ready_seconds: &[1.0_f64],
        during: &[supply * 0.5_f64],
        after: &[supply * 1.5_f64],
    };
    let Ok(mut scratch) = EdfScratch::new(count as u32) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);
    let fast = evaluate_prepared_trajectory(
        &cohorts,
        &trajectory,
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 3_000_000,
            initial_debt_work: 7.0_f64,
            deadline_budget_micros: 1_500_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let general = evaluate_general_trajectory(
        &cohorts,
        &trajectory,
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 3_000_000,
            initial_debt_work: 7.0_f64,
            deadline_budget_micros: 1_500_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    close_relative(fast.shortfall, general.shortfall)
        && close_relative(fast.delay_area, general.delay_area)
        && close_relative(fast.drain_seconds, general.drain_seconds)
}

#[quickcheck]
fn common_cohort_required_capacity_matches_fluid_work(count_seed: u8, work_seed: u16) -> bool {
    let count = usize::from(count_seed % 16 + 1);
    let work = f64::from(work_seed) / 10.0_f64;
    let mut cohorts = WorkCohorts::new(count);
    for partition in 0..count {
        cohorts.push_values(250_000, 1_500_000, work, partition as u32);
    }
    let Ok(mut scratch) = EdfScratch::new(count as u32) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);

    let required = required_capacity_prepared(&cohorts, &mut scratch);
    let expected = cohort_fraction(count) * work / 1.25_f64;

    close_relative(required, expected)
}

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
struct CohortSet(WorkCohorts);

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
        let mut cohorts = WorkCohorts::new(count);
        for partition in 0..count {
            let release_micros = u64::arbitrary(generator) % 20;
            let duration_micros = u64::arbitrary(generator) % 20 + 1;
            let work_slot_seconds = f64::from(u16::arbitrary(generator) % 40) / 1_000_000.0_f64;
            cohorts.push_values(
                release_micros,
                release_micros + duration_micros,
                work_slot_seconds,
                partition as u32,
            );
        }
        Self(cohorts)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let mut shrunk = Vec::new();
        if !self.0.is_empty() {
            shrunk.push(Self(copy_work_range(&self.0, 0, self.0.len() / 2)));
            shrunk.push(Self(copy_work_range(&self.0, 1, self.0.len())));
        }
        Box::new(shrunk.into_iter())
    }
}

fn copy_work_range(source: &WorkCohorts, start: usize, end: usize) -> WorkCohorts {
    let mut copy = WorkCohorts::new(end - start);
    for index in start..end {
        copy.push_values(
            source.release_micros(index),
            source.deadline_micros(index),
            source.work_slot_seconds(index),
            source.partition(index),
        );
    }
    copy
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
        let _ = first.open_unit_f64();
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
    let mut scratch = state.new_scratch()?;
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

    let _ = step(
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

#[quickcheck]
fn empty_rate_occupancy_samples_the_configured_prior(seed: u64) -> bool {
    let Ok(prior) = crate::ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64, 1_024)
        .and_then(|prior| prior.with_transition_learning(1.0_f64, 1.0e9_f64))
    else {
        return false;
    };
    let Ok(distribution) = Gamma::new(4.0_f64, 0.01_f64) else {
        return false;
    };
    let occupancy = RateOccupancy::new();
    let mut random = RandomStream::new(seed);
    let mut samples = vec![0.0_f64; 4_096];
    for sample in &mut samples {
        *sample = occupancy.sample(prior, &mut random);
    }
    samples.sort_by(f64::total_cmp);
    [0.1_f64, 0.5_f64, 0.9_f64]
        .into_iter()
        .zip([409_usize, 2_048, 3_686])
        .all(|(probability, index)| {
            let expected = distribution.inverse_cdf(probability);
            (samples[index] - expected).abs() / expected < 0.1_f64
        })
}

#[quickcheck]
fn observed_rate_occupancy_controls_jump_samples(rate_code: u16, seed: u64) -> bool {
    let Ok(prior) = crate::ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64, 1_024)
        .and_then(|prior| prior.with_transition_learning(1.0_f64, 1.0e9_f64))
    else {
        return false;
    };
    let exponent = -2.0_f64 + 6.0_f64 * f64::from(rate_code) / f64::from(u16::MAX);
    let rate = 10.0_f64.powf(exponent);
    let expected_bin = rate_bin(rate);
    let mut occupancy = RateOccupancy::new();
    occupancy.record(rate, 1_000.0_f64, 1_000.0_f64, 1.0e9_f64);
    let mut random = RandomStream::new(seed);
    let matching = (0_u32..4_096)
        .filter(|_| {
            let sampled_bin = rate_bin(occupancy.sample(prior, &mut random));
            sampled_bin.abs_diff(expected_bin) <= 1
        })
        .count();
    matching >= 3_972
}

#[quickcheck]
fn rate_occupancy_preserves_column_invariants(records: Vec<(u16, u16, u16)>) -> bool {
    let mut occupancy = RateOccupancy::new();
    for (rate_code, exposure_code, elapsed_code) in records.into_iter().take(128) {
        let exponent = -3.0_f64 + 9.0_f64 * f64::from(rate_code) / f64::from(u16::MAX);
        occupancy.record(
            10.0_f64.powf(exponent),
            f64::from(exposure_code) + 1.0_f64,
            f64::from(elapsed_code),
            86_400.0_f64,
        );
    }
    let sum = occupancy.weights().iter().sum::<f64>();
    let tolerance = occupancy.total().max(1.0_f64) * 1.0e-12_f64;
    let weights_valid = occupancy
        .weights()
        .iter()
        .all(|weight| weight.is_finite() && *weight >= 0.0_f64);
    let cumulative_valid = occupancy
        .cumulative()
        .windows(2)
        .all(|pair| pair[0] <= pair[1]);
    weights_valid
        && cumulative_valid
        && (sum - occupancy.total()).abs() <= tolerance
        && (occupancy.cumulative()[63] - occupancy.total()).abs() <= tolerance
}

#[quickcheck]
fn learned_hazard_mean_moves_from_the_prior_to_observed_frequency(frequency_code: u8) -> bool {
    let Ok(prior) = crate::ArrivalPrior::new(4.0_f64, 0.01_f64, 0.01_f64, 1_024)
        .and_then(|prior| prior.with_transition_learning(10.0_f64, 1.0e12_f64))
    else {
        return false;
    };
    let frequency = f64::from(frequency_code) / f64::from(u8::MAX);
    let mut hazard = ChangeHazard::new();
    for _ in 0_u32..1_000 {
        hazard.record(frequency, 1.0_f64, 1.0_f64, 1.0e12_f64);
    }
    let mean = hazard.mean(prior);
    let lower = frequency.min(0.01_f64);
    let upper = frequency.max(0.01_f64);
    (lower..=upper).contains(&mean) && (mean - frequency).abs() < 0.01_f64
}

#[test]
fn transition_learning_requires_positive_finite_durations() -> Result<(), TestError> {
    let prior = crate::ArrivalPrior::new(4.0_f64, 0.01_f64, 0.01_f64, 1_024)?;
    assert!(matches!(
        prior.with_transition_learning(0.0_f64, 1.0_f64),
        Err(crate::ArrivalPriorError::InvalidPriorTrust)
    ));
    assert!(matches!(
        prior.with_transition_learning(1.0_f64, f64::INFINITY),
        Err(crate::ArrivalPriorError::InvalidOccupancyHalfLife)
    ));
    Ok(())
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
        fine.expected_rate(1_000_000 + tick * 1_000);
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
    let mut segments = CalendarColumns::new(1);
    segments.extend(slice::from_ref(&segment));
    let forecast = CalendarForecast {
        artifact: CalendarArtifactId(11),
        prior_probability: 0.5_f64,
        segments: &segments,
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
    let mut segments = CalendarColumns::new(1);
    segments.extend(slice::from_ref(&segment));
    let forecast = CalendarForecast {
        artifact: CalendarArtifactId(11),
        prior_probability: 0.5_f64,
        segments: &segments,
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
fn moved_partition_formula_matches_assignment_overlap(
    partition_seed: u8,
    current_seed: u8,
    target_seed: u8,
) -> bool {
    let partitions = u32::from(partition_seed) + 1;
    let current = u32::from(current_seed) % partitions + 1;
    let target = u32::from(target_seed) % partitions + 1;
    let common = current.min(target);
    let overlap = (0..common)
        .map(|owner| {
            let current_count = partitions / current + u32::from(owner < partitions % current);
            let target_count = partitions / target + u32::from(owner < partitions % target);
            current_count.min(target_count)
        })
        .sum::<u32>();

    minimal_moved_partitions(partitions, current, target) == partitions - overlap
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
fn controller_transition_is_cadence_invariant() -> Result<(), TestError> {
    let configuration = configuration()?;
    let capacity_grid = grid()?;
    let mut coarse_state = ScaleState::new(configuration.clone(), capacity_grid.clone())?;
    let mut fine_state = ScaleState::new(configuration.clone(), capacity_grid)?;
    let mut coarse_scratch = coarse_state.new_scratch()?;
    let mut fine_scratch = fine_state.new_scratch()?;
    let mut coarse_observation = ObservationBuffer::new(&configuration)?;
    let mut fine_observation = ObservationBuffer::new(&configuration)?;

    for now_micros in [250_000_u64, 500_000, 750_000] {
        let _ = step(
            &mut fine_state,
            &mut fine_scratch,
            fine_observation.observation(),
            ModelTime::from_micros(now_micros),
        );
    }
    let coarse = step(
        &mut coarse_state,
        &mut coarse_scratch,
        coarse_observation.observation(),
        ModelTime::from_micros(1_000_000),
    );
    let fine = step(
        &mut fine_state,
        &mut fine_scratch,
        fine_observation.observation(),
        ModelTime::from_micros(1_000_000),
    );

    assert_eq!(coarse, fine);
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
            .map_err(|_| ConfigurationError::PlatformLimit)?;
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
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_attempt_outcomes(AttemptOutcomeEvidence::new(
        AttemptOutcomeCounts::new(80, 10, 10, 0),
        AttemptOutcomeCounts::new(10, 5, 0, 5),
    ))?;
    let _ = step(
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
            window_influence_bound_probability: 0.05_f64,
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
            window_influence_bound_probability: 0.05_f64,
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
        CapacityPrior::LogUniform {
            window_influence_bound_probability: 0.05_f64,
        },
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
            window_influence_bound_probability: 0.05_f64,
        },
    );
    result.is_ok() == valid
}

#[test]
fn consistent_capacity_windows_concentrate_the_posterior() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new_with_prior(
        &[0.1_f64],
        &[50.0_f64, 100.0_f64],
        &[0.0_f64],
        CapacityPrior::LogUniform {
            window_influence_bound_probability: 0.05_f64,
        },
    )?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    let window = ResourceWindow::new(20.0_f64, 1.0_f64, 100)?;
    for _ in 0_u32..128_u32 {
        factor.update(simd_level, &window);
    }
    let mut values = [0.0_f64; 2];
    let mut probabilities = [0.0_f64; 2];
    factor.write_capacity_posterior(&mut values, &mut probabilities)?;

    assert!((values[0] - 50.0_f64).abs() < f64::EPSILON);
    assert!((values[1] - 100.0_f64).abs() < f64::EPSILON);
    assert!(probabilities[1] > 0.99_f64);
    Ok(())
}

#[test]
fn one_application_trickle_window_cannot_erase_no_knee_mass() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new_with_prior(
        &[0.001_f64, 0.01_f64, 0.1_f64],
        &[2_000.0_f64, 4_000.0_f64],
        &[0.0_f64, 1.0_f64],
        CapacityPrior::LogUniform {
            window_influence_bound_probability: 0.05_f64,
        },
    )?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    factor.update(simd_level, &ResourceWindow::new(0.003_f64, 1.0_f64, 3)?);

    assert!(factor.no_knee_probability() >= 0.02_f64);
    Ok(())
}

#[test]
fn one_burst_onset_window_cannot_erase_no_knee_mass() -> Result<(), TestError> {
    let simd_level = Level::new();
    let grid = CapacityGrid::new_with_prior(
        &[0.1_f64, 1.0_f64],
        &[40.0_f64, 320.0_f64, 4_000.0_f64],
        &[0.0_f64, 1.0_f64],
        CapacityPrior::LogUniform {
            window_influence_bound_probability: 0.05_f64,
        },
    )?;
    let mut factor = CapacityFactor::new(grid, 0.0_f64);
    factor.update(simd_level, &ResourceWindow::new(256.0_f64, 1.0_f64, 0)?);

    assert!(factor.no_knee_probability() >= 0.02_f64);
    Ok(())
}

fn capacity_prior(grid: CapacityGrid) -> Result<Vec<f64>, TestError> {
    let state = ScaleState::new(configuration()?, grid)?;
    let count = usize::try_from(state.posterior_value_count(PosteriorQuery::Capacity)?)
        .map_err(|_| ConfigurationError::PlatformLimit)?;
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

#[quickcheck]
fn fluid_edf_matches_exhaustive_interval_oracle(input: CohortSet, slots: u8) -> bool {
    let slots = f64::from(slots % 8 + 1);
    let Ok(mut scratch) = EdfScratch::new(16) else {
        return false;
    };
    let CohortSet(cohorts) = input;
    prepare(&cohorts, &mut scratch);
    let horizon_micros = (0..cohorts.len())
        .map(|index| cohorts.deadline_micros(index))
        .max()
        .unwrap_or(0);
    let actual = evaluate_constant_supply(
        &cohorts,
        slots,
        horizon_micros,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    )
    .shortfall
        <= f64::EPSILON;
    let expected = exhaustive_feasible(&cohorts, slots);
    actual == expected
}

#[quickcheck]
fn required_capacity_matches_exhaustive_interval_oracle(input: CohortSet) -> bool {
    let CohortSet(cohorts) = input;
    let Ok(mut scratch) = EdfScratch::new(16) else {
        return false;
    };
    prepare(&cohorts, &mut scratch);

    let actual = required_capacity_prepared(&cohorts, &mut scratch);
    let expected = exhaustive_required_capacity(&cohorts);

    close_relative(actual, expected)
}

#[test]
fn partial_observation_returns_a_decision() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = state.new_scratch()?;
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
fn exact_capacity_mean_matches_direct_enumeration() -> Result<(), TestError> {
    let configuration = Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        partition_count: 1,
        replica_count_max: 1,
        slots_per_replica: 1,
        posterior_sample_count: 1_024,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64, 1_024)?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let grid = CapacityGrid::new(&[0.001_f64], &[50.0_f64, 100.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
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
    let missed_at_low_knee = 75.0_f64 - 50.0_f64;
    let low_knee_late_area = missed_at_low_knee * missed_at_low_knee / (2.0_f64 * 50.0_f64);
    let exact_loss = 0.25_f64 * low_knee_late_area / 75.0_f64;

    assert!(
        (apply.diagnostics.expected_loss - exact_loss).abs() < 1.0e-5_f64,
        "actual={}, exact={exact_loss}",
        apply.diagnostics.expected_loss
    );
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
        posterior_sample_count: 128,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: negligible_arrival_prior()?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let grid = CapacityGrid::new(&[1.0_f64], &[100.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
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
    let mut losses = [0.0_f64; 2];
    scratch.write_decision_expected_losses(&mut losses)?;
    assert!(
        losses.iter().all(|loss| *loss > 0.0_f64),
        "losses={losses:?}"
    );
    assert!(apply.diagnostics.shortfall > 0.0_f64);
    Ok(())
}

#[test]
fn partition_pause_removes_service_from_candidate_supply() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, 0.75_f64, 0);
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let outcome = evaluate_prepared_step(
        &cohorts,
        SupplyStep {
            before: 1.0_f64,
            during: 0.5_f64,
            after: 2.0_f64,
            pause_micros: 0,
            ready_micros: 1_000_000,
        },
        EvaluationWindow {
            start_micros: 0,
            horizon_micros: 1_000_000,
            initial_debt_work: 0.0_f64,
            deadline_budget_micros: 1_000_000,
        },
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(close_relative(outcome.shortfall, 1.0_f64 / 3.0_f64));
    Ok(())
}

#[test]
fn missed_work_remains_service_debt_and_rewards_faster_recovery() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(1);
    cohorts.push_values(0, 1_000_000, 1.0_f64, 0);
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let slow = evaluate_constant_supply(
        &cohorts,
        1.0_f64,
        1_000_000,
        0.75_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let fast = evaluate_constant_supply(
        &cohorts,
        2.0_f64,
        1_000_000,
        0.75_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(slow.shortfall > fast.shortfall);
    assert!(slow.delay_area > fast.delay_area);
    Ok(())
}

#[test]
fn debt_only_observation_rewards_faster_recovery() -> Result<(), TestError> {
    let cohorts = WorkCohorts::new(0);
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let slow = evaluate_constant_supply(
        &cohorts,
        1.0_f64,
        2_000_000,
        2.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let fast = evaluate_constant_supply(
        &cohorts,
        2.0_f64,
        2_000_000,
        2.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );

    assert!(slow.delay_area > 0.0_f64);
    assert!(slow.delay_area > fast.delay_area);
    Ok(())
}

#[test]
fn predictive_arrivals_consume_service_while_debt_drains() -> Result<(), TestError> {
    let cohorts = WorkCohorts::new(0);
    let mut scratch = EdfScratch::new(1)?;
    prepare(&cohorts, &mut scratch);
    let matched = evaluate_constant_supply(
        &cohorts,
        10.0_f64,
        10_000_000,
        100.0_f64,
        &TEN_FUTURE_ARRIVALS_PER_SECOND,
        &mut scratch,
    );
    let recovery = evaluate_constant_supply(
        &cohorts,
        20.0_f64,
        10_000_000,
        100.0_f64,
        &TEN_FUTURE_ARRIVALS_PER_SECOND,
        &mut scratch,
    );

    assert!(close_relative(matched.delay_area, 1_000.0_f64));
    assert!(close_relative(recovery.delay_area, 500.0_f64));
    assert!(close_relative(matched.drain_seconds, 10.0_f64));
    assert!(recovery.drain_seconds.total_cmp(&0.0_f64).is_eq());
    Ok(())
}

#[test]
fn decision_curve_contains_the_selected_expected_loss() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = state.new_scratch()?;
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
    scratch.write_decision_expected_losses(&mut losses)?;

    assert!(close_relative(
        losses[apply.target as usize - 1],
        apply.diagnostics.expected_loss,
    ));
    Ok(())
}

#[test]
fn decision_diagnostics_report_no_rejection_for_zero_demand() -> Result<(), TestError> {
    let mut configuration = configuration()?;
    configuration.arrival_prior = negligible_arrival_prior()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let mut state = ScaleState::new(configuration.clone(), grid)?;
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;

    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    if !matches!(decision, ScaleDecision::Apply(_)) {
        return Err(TestError::UnexpectedHold);
    }
    let mut probabilities = vec![0.0_f64; scratch.decision_candidate_count()];
    for reason in [
        crate::DecisionRejection::Deadline,
        crate::DecisionRejection::PartitionPlacement,
    ] {
        scratch.write_rejection_curve(reason, &mut probabilities)?;
        assert!(
            probabilities
                .iter()
                .all(|probability| *probability == 0.0_f64)
        );
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
    let mut scratch = state.new_scratch()?;
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
    let warm_decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    black_box(warm_decision);
    let warm_decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(2),
    );
    black_box(warm_decision);

    let pool = rayon::ThreadPoolBuilder::new().build()?;
    let _ = pool.install(|| {
        step(
            &mut state,
            &mut scratch,
            observation.observation(),
            ModelTime::from_micros(3),
        )
    });
    let allocation = allocation_counter::measure(|| {
        let _ = pool.install(|| {
            step(
                &mut state,
                &mut scratch,
                observation.observation(),
                ModelTime::from_micros(4),
            )
        });
    });
    assert_eq!(allocation.count_total, 0);
    assert_eq!(allocation.bytes_total, 0);
    Ok(())
}

#[test]
fn rayon_width_does_not_change_the_decision() -> Result<(), TestError> {
    let scalar = decision_with_threads(1)?;
    let parallel = decision_with_threads(4)?;

    assert_eq!(parallel, scalar);
    Ok(())
}

#[test]
fn regressed_model_time_returns_hold() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = state.new_scratch()?;
    let mut first = ObservationBuffer::new(&configuration)?;
    let mut second = ObservationBuffer::new(&configuration)?;
    let _ = step(
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
    let mut scratch = state.new_scratch()?;
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
    let mut scratch = state.new_scratch()?;
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
    let mut losses = [0.0_f64; 32];
    scratch.write_decision_expected_losses(&mut losses)?;
    assert!(
        losses.iter().all(|loss| *loss > 0.0_f64),
        "losses={losses:?}"
    );
    assert!(apply.diagnostics.shortfall > 0.0_f64);
    Ok(())
}

fn exhaustive_feasible(cohorts: &WorkCohorts, slots: f64) -> bool {
    exhaustive_required_capacity(cohorts) <= slots + 8.0_f64 * f64::EPSILON * slots.max(1.0_f64)
}

fn exhaustive_required_capacity(cohorts: &WorkCohorts) -> f64 {
    let mut required = 0.0_f64;
    for start in 0..cohorts.len() {
        for end in 0..cohorts.len() {
            if cohorts.release_micros(start) >= cohorts.deadline_micros(end) {
                continue;
            }
            let mut demand = 0.0_f64;
            for index in 0..cohorts.len() {
                if cohorts.release_micros(index) >= cohorts.release_micros(start)
                    && cohorts.deadline_micros(index) <= cohorts.deadline_micros(end)
                {
                    demand += cohorts.work_slot_seconds(index);
                }
            }
            let elapsed =
                Duration::from_micros(cohorts.deadline_micros(end) - cohorts.release_micros(start))
                    .as_secs_f64();
            required = required.max(demand / elapsed);
        }
    }
    required
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
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_current_replicas(2)?;
    observation.push_actuation_commitment(ActuationCommitment::launching(
        2,
        3,
        ModelTime::from_micros(1),
    )?)?;

    let _ = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(20_000_000),
    );

    assert!(matches!(scratch.trajectory_targets(2), Some([] | [1])));
    assert!(matches!(scratch.trajectory_targets(3), Some([3, ..])));
    let targets = scratch
        .trajectory_targets(4)
        .ok_or(TestError::MissingTrajectory)?;
    assert!(targets.contains(&4));
    Ok(())
}

#[test]
fn started_rebalance_is_carried_and_can_be_superseded() -> Result<(), TestError> {
    let mut configuration = configuration()?;
    configuration.launch_time_prior =
        TransitionPrior::new([1.0_f64; 4], [0.01_f64; 3], [1.0_f64; 12], 0.0_f64)?;
    configuration.rebalance_time_prior =
        TransitionPrior::new([100.0_f64; 4], [0.01_f64; 3], [1.0_f64; 12], 0.0_f64)?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_current_replicas(2)?;
    observation.push_actuation_commitment(ActuationCommitment::rebalancing(
        2,
        3,
        ModelTime::from_micros(1_000_000),
        ModelTime::from_micros(10_000_000),
    )?)?;

    let _ = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(20_000_000),
    );

    assert!(matches!(scratch.trajectory_targets(3), Some([3, ..])));
    assert!(matches!(
        scratch.trajectory_pause_seconds(3),
        Some([20.0_f64, ..])
    ));
    let retained = scratch
        .trajectory_targets(2)
        .zip(scratch.trajectory_pause_seconds(2))
        .zip(scratch.trajectory_ready_seconds(2))
        .ok_or(TestError::MissingTrajectory)?;
    assert!(matches!(retained.0.0, [3, 2, ..]));
    assert!(retained.0.1[0].total_cmp(&20.0_f64).is_eq());
    assert!(retained.0.1[1] > retained.0.1[0]);
    assert!(retained.1[0].total_cmp(&retained.0.1[1]).is_eq());
    assert!(retained.1[1] > retained.0.1[1]);
    Ok(())
}

#[quickcheck]
fn rebalance_phase_rejects_time_before_request(requested: u64, started: u64) -> bool {
    let result = ActuationCommitment::rebalancing(
        1,
        2,
        ModelTime::from_micros(requested),
        ModelTime::from_micros(started),
    );
    result.is_ok() == (started >= requested)
}

/// Mirrors the linear-throughput regime configuration.
fn plateau_configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 256,
        calendar_segment_count_max: 64,
        partition_count: 64,
        replica_count_max: 128,
        slots_per_replica: 32,
        posterior_sample_count: 4_096,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64, 1_024)?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::new(
            [30.0_f64, 45.0_f64, 60.0_f64, 90.0_f64],
            [0.1_f64, 0.2_f64, 0.3_f64],
            [1.0_f64; 12],
            0.0_f64,
        )?,
        rebalance_time_prior: TransitionPrior::new(
            [0.05_f64, 0.1_f64, 0.2_f64, 0.4_f64],
            [0.1_f64, 0.2_f64, 0.3_f64],
            [1.0_f64; 12],
            0.0_f64,
        )?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    })
}

fn plateau_grid() -> Result<CapacityGrid, TestError> {
    let capacities = (1_u32..=64)
        .map(|value| f64::from(value) * 20.0_f64)
        .collect::<Vec<_>>();
    Ok(CapacityGrid::new(
        &[0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64],
        &capacities,
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )?)
}

/// A steady plateau selects the minimum expected cost.
#[test]
fn steady_plateau_selects_the_cost_minimum() -> Result<(), TestError> {
    let configuration = plateau_configuration()?;
    let mut state = ScaleState::new(configuration.clone(), plateau_grid()?)?;
    let mut scratch = state.new_scratch()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    observation.set_arrivals(72_000, 240_000_000)?;
    observation.set_resource_window(ResourceWindow::new(30.0_f64, 240.0_f64, 72_000)?)?;
    observation.set_current_replicas(2)?;
    let ScaleDecision::Apply(_first) = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(240_000_000),
    ) else {
        return Err(TestError::UnexpectedHold);
    };

    let mut apply = None;
    for window in 2_u64..=15 {
        let now_micros = 239_000_000 + window * 1_000_000;
        let mut observation = ObservationBuffer::new(&configuration)?;
        observation.set_arrivals(300, 1_000_000)?;
        observation.set_resource_window(ResourceWindow::new(30.0_f64, 1.0_f64, 300)?)?;
        observation.set_current_replicas(2)?;
        for partition in 0..configuration.partition_count {
            observation.push_cohort(Cohort {
                release_micros: now_micros,
                deadline_micros: now_micros + 1_000_000,
                offered_events: 300.0_f64 / f64::from(configuration.partition_count),
                partition,
                demand_class: DemandClass::Normal,
            })?;
        }
        let ScaleDecision::Apply(decision) = step(
            &mut state,
            &mut scratch,
            observation.observation(),
            ModelTime::from_micros(now_micros),
        ) else {
            return Err(TestError::UnexpectedHold);
        };
        apply = Some(decision);
    }
    let apply = apply.ok_or(TestError::MissingDecisionCurve)?;
    let selected =
        usize::try_from(apply.target - 1).map_err(|_| ConfigurationError::PlatformLimit)?;
    let mut losses = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_losses(&mut losses)?;
    let summary = scratch
        .decision_column_summary(selected)
        .ok_or(TestError::MissingDecisionCurve)?;
    let runner_up = summary.runner_up.ok_or(TestError::MissingDecisionCurve)?;
    assert!(summary.selected.cost <= runner_up.cost);
    assert!(
        apply.target <= configuration.partition_count,
        "target={} partitions={}",
        apply.target,
        configuration.partition_count
    );
    // The anti-scaling wall stays gone: one step above the demand floor
    // costs less expected delay than holding at it.
    assert!(losses[2] <= losses[1], "losses={:?}", &losses[..8]);
    Ok(())
}

/// Deliverable supply never falls when slots grow; the physical curve
/// keeps its collapse for inference. See
/// [`CapacityCurve::sustainable_throughput`].
#[test]
fn sustainable_supply_never_falls_with_more_slots() {
    let curve = CapacityCurve::Knee {
        service_time_seconds: 0.1_f64,
        capacity_per_second: 300.0_f64,
        collapse: 2.0_f64,
    };
    let concurrency = [16.0_f64, 32.0_f64, 64.0_f64, 256.0_f64];
    let mut supply = [0.0_f64; 4];
    CapacityFactor::fill_throughput(Level::new(), curve, &concurrency, &mut supply);

    assert!(supply.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(close_relative(supply[3], 300.0_f64));
    assert!(curve.throughput(256.0_f64) < 300.0_f64);
}

fn cohort_fraction(count: usize) -> f64 {
    f64::from(u32::try_from(count).map_or(u32::MAX, |count| count))
}

fn close_relative(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0_f64);
    (left - right).abs() <= 1.0e-12_f64 * scale
}

fn posterior_mean(state: &ScaleState, query: PosteriorQuery) -> Result<f64, TestError> {
    let count = usize::try_from(state.posterior_value_count(query)?)
        .map_err(|_| ConfigurationError::PlatformLimit)?;
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
        posterior_sample_count: 128,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: crate::ArrivalPrior::broad_fallback(),
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01, 3.0_f64)?,
    })
}

fn decision_with_threads(thread_count: usize) -> Result<ScaleDecision, TestError> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()?;
    pool.install(|| {
        let configuration = configuration()?;
        let mut state = ScaleState::new(configuration.clone(), grid()?)?;
        let mut scratch = state.new_scratch()?;
        let mut observation = ObservationBuffer::new(&configuration)?;
        for cohort in 0..configuration.cohort_count_max {
            observation.push_cohort(Cohort {
                release_micros: u64::from(cohort) * 10_000,
                deadline_micros: 1_000_000 + u64::from(cohort) * 20_000,
                offered_events: f64::from(cohort + 1),
                partition: cohort % configuration.partition_count,
                demand_class: DemandClass::Normal,
            })?;
        }
        Ok(step(
            &mut state,
            &mut scratch,
            observation.observation(),
            ModelTime::from_micros(1),
        ))
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
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    DecisionCurve(#[from] crate::DecisionCurveError),
    #[error(transparent)]
    Observation(#[from] crate::ObservationError),
    #[error(transparent)]
    TransitionEvidence(#[from] crate::TransitionEvidenceError),
    #[error(transparent)]
    TransitionPrior(#[from] crate::TransitionPriorError),
    #[error(transparent)]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("the model held when the test required an applied decision")]
    UnexpectedHold,
    #[error("a test count exceeds the platform limit")]
    PlatformLimit,
    #[error("the candidate trajectory is missing")]
    MissingTrajectory,
    #[error("the decision columns are missing")]
    MissingDecisionCurve,
}
