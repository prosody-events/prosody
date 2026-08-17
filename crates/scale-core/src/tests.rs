use std::hint::black_box;
use std::time::Duration;

use fearless_simd::Level;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use thiserror::Error;

use crate::arrival::{ArrivalEvidence, ArrivalFactor, ArrivalPrior};
use crate::capacity::CapacityFactor;
use crate::change_point::ChangePointKernel;
use crate::controller::{
    DecisionRandomDomain, decision_random, minimal_moved_partitions, mixed_event_supply,
};
use crate::edf::{
    ArrivalPath, EdfOutcome, EdfScratch, EvaluationWindow, SupplyStep, SupplyTrajectory,
    evaluate_prepared_step, evaluate_prepared_trajectory, prepare, required_capacity_prepared,
};
use crate::lead_time::LaunchTimeFactor;
use crate::partition::PartitionFactor;
use crate::planning::terminal_replica_seconds;
use crate::types::{WorkCohorts, occupancy_trace_for_test};
use crate::{
    ActuationCommitment, AttemptOutcomeCounts, AttemptOutcomeEvidence, BacklogCohort,
    CapacityCurve, CapacityGrid, CapacityPrior, Cohort, Configuration, ConfigurationError,
    DemandClass, DurationCell, HoldReason, LaunchPrior, LaunchPriorGrid, ModelTime,
    ObservationBuffer, ObservationError, OccupancyTransition, PosteriorQuery, PriorArtifactBudget,
    PriorArtifactIdentity, PriorCoverageRecord, RandomStream, ReadinessGroupId, ReadinessLump,
    ReadinessObservation, RebalanceEvidence, RebalancePrior, ReliabilityPrior, ResourceWindow,
    ScaleDecision, ScaleState, ServiceObjective, ThroughputPosteriorCell, TransitionDirection,
    step,
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

#[test]
fn configuration_requires_two_samples_for_each_capacity_class() -> Result<(), TestError> {
    let mut configuration = configuration()?;
    configuration.posterior_sample_count = 1;

    assert!(matches!(
        configuration.validate(),
        Err(ConfigurationError::InsufficientPosteriorSamples {
            sample_count: 1,
            minimum: 2,
        })
    ));
    Ok(())
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
    let capacity = f64::from(capacity_seed % 10 + 1);
    let drain_fraction = f64::from(work_seed % 1_000 + 1) / 1_000.0_f64;
    let work = 1.5_f64 * capacity * drain_fraction;
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
    let first_resource = terminal_replica_seconds(0, 2_000_000, first.drain_seconds, 3_000_000, 1);
    let second_resource =
        terminal_replica_seconds(0, 3_000_000, second.drain_seconds, 3_000_000, 1);

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

    assert!(outcome.drain_seconds.total_cmp(&3.0_f64).is_eq());
    assert!(outcome.terminal_late_area.total_cmp(&60.0_f64).is_eq());
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
    let mut first_arrival = decision_random(3, 17, DecisionRandomDomain::Arrival);
    let mut first_lead = decision_random(3, 17, DecisionRandomDomain::LeadTime);
    let expected_lead = first_lead.next_u64();
    for _ in 0_u8..100 {
        let _ = first_arrival.next_u64();
    }

    let mut second_lead = decision_random(3, 17, DecisionRandomDomain::LeadTime);
    let mut other_decision_lead = decision_random(4, 17, DecisionRandomDomain::LeadTime);
    let mut other_scenario_lead = decision_random(3, 18, DecisionRandomDomain::LeadTime);
    let mut reliability = decision_random(3, 17, DecisionRandomDomain::Reliability);

    assert_eq!(second_lead.next_u64(), expected_lead);
    assert_ne!(other_decision_lead.next_u64(), expected_lead);
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
fn future_backlog_deadline_exceeding_horizon_is_rejected() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let model_time = 10_u64;
    observation.advance_model_time(ModelTime::from_micros(model_time))?;
    let oldest_arrival_micros =
        model_time + ArrivalPrior::MAXIMUM_PATH_MICROS - configuration.objective.budget_micros();
    let backlog = BacklogCohort::new(
        oldest_arrival_micros,
        oldest_arrival_micros,
        1,
        0,
        DemandClass::Normal,
    )?;

    assert_eq!(
        observation.set_backlog(backlog),
        Err(ObservationError::DeadlineHorizon)
    );
    Ok(())
}

#[test]
fn launch_evidence_accepts_incremental_groups_and_rejects_duplicate_groups() -> Result<(), TestError>
{
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let requested_at = ModelTime::from_micros(1_000_000);
    let observed_at = ModelTime::from_micros(3_000_000);
    let ready = ReadinessLump::new(
        ReadinessGroupId(1),
        1,
        ReadinessObservation::ready(requested_at, ModelTime::from_micros(2_000_000))?,
    )?;
    let pending = ReadinessLump::new(
        ReadinessGroupId(2),
        1,
        ReadinessObservation::pending(ModelTime::from_micros(2_000_000), observed_at)?,
    )?;

    observation.set_launch_evidence(requested_at, 3, observed_at, &[ready, pending])?;
    assert!(observation.observation().launch.is_some());

    let duplicate = [
        ready,
        ReadinessLump::new(ReadinessGroupId(1), 1, pending.observation())?,
    ];
    assert!(matches!(
        observation.set_launch_evidence(requested_at, 3, observed_at, &duplicate),
        Err(ObservationError::LaunchEvidence(
            crate::LaunchEvidenceError::DuplicateGroup
        ))
    ));
    Ok(())
}

#[test]
fn bimodal_launch_evidence_concentrates_without_a_compromise_cell() -> Result<(), TestError> {
    const PAIR_COUNT: u32 = 16;
    const OLD_MODEL_VALLEY_MASS: f64 = 0.842_456_256_292_198_1_f64;
    let mut witness = mixture_witness()?;
    let fast_interval = (4.95_f64, 5.05_f64);
    let slow_interval = (59.95_f64, 60.05_f64);
    let mut prior_half_mass = mode_mass(&witness.oracle, 1);
    let mut wrong_odds_at_half = [0.0_f64; 2];
    for pair in 1..=PAIR_COUNT {
        witness.factor.update(
            Level::new(),
            crate::LaunchEvidence::new(
                ModelTime::from_micros(0),
                2,
                ModelTime::from_micros(60_050_000),
                &witness.lumps,
            ),
        );
        update_launch_oracle(
            &mut witness.oracle,
            &witness.intercepts,
            &witness.fast_medians,
            &witness.slow_medians,
            witness.sigma,
            [fast_interval, slow_interval],
        );
        let roundoff = f64::EPSILON * 12.0_f64 * f64::from(pair) * 64.0_f64;
        assert!(
            witness
                .factor
                .posterior_weights()
                .iter()
                .zip(witness.oracle)
                .all(|(actual, expected)| (actual - expected).abs() <= roundoff),
            "the posterior must match the finite-grid oracle"
        );
        let half_mass = mode_mass(witness.factor.posterior_weights(), 1);
        assert!(
            half_mass >= prior_half_mass,
            "each fast and slow pair must increase mass on equal mode weight"
        );
        prior_half_mass = half_mass;
        if pair == PAIR_COUNT / 2 {
            wrong_odds_at_half = [
                mode_mass(witness.factor.posterior_weights(), 0) / half_mass,
                mode_mass(witness.factor.posterior_weights(), 2) / half_mass,
            ];
        }
    }
    let half_mass = mode_mass(witness.factor.posterior_weights(), 1);
    for (wrong, prior_odds) in [0_usize, 2].into_iter().zip(wrong_odds_at_half) {
        assert!(
            mode_mass(witness.factor.posterior_weights(), wrong) / half_mass < prior_odds,
            "doubling the evidence must reduce each wrong mode odds"
        );
    }
    let mut values = [0.0_f64; 4];
    let mut component_mass = [0.0_f64; 4];
    assert!(witness.factor.write_posterior(
        TransitionDirection::Up,
        2,
        &mut values,
        &mut component_mass,
    ));
    let generating_mass = component_mass[0] + component_mass[3];
    let compromise_mass = component_mass[1] + component_mass[2];
    assert!(
        generating_mass > compromise_mass,
        "the mixture must select the fast and slow cells, not compromise cells"
    );
    let valley_mass = witness
        .factor
        .predictive_cdf(TransitionDirection::Up, 2, 40.0_f64)
        - witness
            .factor
            .predictive_cdf(TransitionDirection::Up, 2, 10.0_f64);
    let oracle_valley_mass = launch_oracle_interval_mass(
        &witness.oracle,
        &witness.intercepts,
        &witness.fast_medians,
        &witness.slow_medians,
        witness.sigma,
        (10.0_f64, 40.0_f64),
    );
    let roundoff = f64::EPSILON * 12.0_f64 * f64::from(PAIR_COUNT) * 64.0_f64;
    assert!((valley_mass - oracle_valley_mass).abs() <= roundoff);
    assert!(
        valley_mass < OLD_MODEL_VALLEY_MASS,
        "the mixture must reject the old model's predictive valley mass"
    );
    Ok(())
}

struct MixtureWitness {
    factor: LaunchTimeFactor,
    oracle: [f64; 12],
    intercepts: [f64; 3],
    fast_medians: [f64; 2],
    slow_medians: [f64; 2],
    sigma: f64,
    lumps: [ReadinessLump; 2],
}

fn mixture_witness() -> Result<MixtureWitness, TestError> {
    let intercepts = [-9.0_f64.ln(), 0.0_f64, 9.0_f64.ln()];
    let fast_medians = [5.0_f64, 20.0_f64];
    let slow_medians = [20.0_f64, 60.0_f64];
    let sigma = 0.03_f64;
    let fast_cells = [
        DurationCell::new(fast_medians[0], sigma)?,
        DurationCell::new(fast_medians[1], sigma)?,
    ];
    let slow_cells = [
        DurationCell::new(slow_medians[0], sigma)?,
        DurationCell::new(slow_medians[1], sigma)?,
    ];
    let prior = LaunchPrior::new(
        PriorArtifactIdentity::new(1, 1, 1),
        PriorArtifactBudget::new(16, 512, 1_024, 1.0e-6_f64, 1.0e-6_f64, 1.0e-6_f64),
        &[
            PriorCoverageRecord::new(1.0_f64, 120.0_f64, 1.0e-12_f64, 1.0e-12_f64, 1.0e-12_f64),
            PriorCoverageRecord::new(1.0_f64, 120.0_f64, 1.0e-12_f64, 1.0e-12_f64, 1.0e-12_f64),
        ],
        LaunchPriorGrid::new(&intercepts, &[0.0_f64], &fast_cells, &slow_cells),
        &[1.0_f64; 12],
        0.0_f64,
    )?;
    Ok(MixtureWitness {
        factor: LaunchTimeFactor::new(&prior),
        oracle: [1.0_f64 / 12.0_f64; 12],
        intercepts,
        fast_medians,
        slow_medians,
        sigma,
        lumps: [
            readiness_lump(1, 4_950_000, 5_050_000)?,
            readiness_lump(2, 59_950_000, 60_050_000)?,
        ],
    })
}

fn readiness_lump(group: u64, after: u64, ready: u64) -> Result<ReadinessLump, TestError> {
    Ok(ReadinessLump::new(
        ReadinessGroupId(group),
        1,
        ReadinessObservation::ready(ModelTime::from_micros(after), ModelTime::from_micros(ready))?,
    )?)
}

fn mode_mass(weights: &[f64], intercept: usize) -> f64 {
    weights[intercept * 4..intercept * 4 + 4].iter().sum()
}

fn update_launch_oracle(
    weights: &mut [f64; 12],
    intercepts: &[f64; 3],
    fast_medians: &[f64; 2],
    slow_medians: &[f64; 2],
    sigma: f64,
    intervals: [(f64, f64); 2],
) {
    for (hypothesis, weight) in weights.iter_mut().enumerate() {
        let intercept = hypothesis / 4;
        let fast = (hypothesis / 2) % 2;
        let slow = hypothesis % 2;
        let slow_probability = 1.0_f64 / (1.0_f64 + (-intercepts[intercept]).exp());
        for interval in intervals {
            let fast_probability = test_log_normal_interval(fast_medians[fast], sigma, interval);
            let slow_probability_mass =
                test_log_normal_interval(slow_medians[slow], sigma, interval);
            *weight *= (1.0_f64 - slow_probability) * fast_probability
                + slow_probability * slow_probability_mass;
        }
    }
    let total = weights.iter().sum::<f64>();
    for weight in weights {
        *weight /= total;
    }
}

fn launch_oracle_interval_mass(
    weights: &[f64; 12],
    intercepts: &[f64; 3],
    fast_medians: &[f64; 2],
    slow_medians: &[f64; 2],
    sigma: f64,
    interval: (f64, f64),
) -> f64 {
    weights
        .iter()
        .enumerate()
        .map(|(hypothesis, weight)| {
            let intercept = hypothesis / 4;
            let fast = (hypothesis / 2) % 2;
            let slow = hypothesis % 2;
            let slow_probability = 1.0_f64 / (1.0_f64 + (-intercepts[intercept]).exp());
            weight
                * ((1.0_f64 - slow_probability)
                    * test_log_normal_interval(fast_medians[fast], sigma, interval)
                    + slow_probability
                        * test_log_normal_interval(slow_medians[slow], sigma, interval))
        })
        .sum()
}

fn test_log_normal_interval(median: f64, sigma: f64, interval: (f64, f64)) -> f64 {
    test_normal_cdf((interval.1.ln() - median.ln()) / sigma)
        - test_normal_cdf((interval.0.ln() - median.ln()) / sigma)
}

fn test_normal_cdf(value: f64) -> f64 {
    let sign = if value < 0.0_f64 { -1.0_f64 } else { 1.0_f64 };
    let value = value.abs() / 2.0_f64.sqrt();
    let t = 1.0_f64 / (1.0_f64 + 0.327_591_1_f64 * value);
    let polynomial = (((((1.061_405_429_f64 * t - 1.453_152_027_f64) * t) + 1.421_413_741_f64)
        * t
        - 0.284_496_736_f64)
        * t
        + 0.254_829_592_f64)
        * t;
    0.5_f64 * (1.0_f64 + sign * (1.0_f64 - polynomial * (-value * value).exp()))
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
    let launch_lump = ReadinessLump::new(
        ReadinessGroupId(1),
        1,
        ReadinessObservation::ready(
            ModelTime::from_micros(0),
            ModelTime::from_micros(15_000_000),
        )?,
    )?;
    observation.set_launch_evidence(
        ModelTime::from_micros(0),
        1,
        ModelTime::from_micros(15_000_000),
        &[launch_lump],
    )?;
    observation.set_rebalance_evidence(RebalanceEvidence::completed(
        ModelTime::from_micros(119_000_000),
        ModelTime::from_micros(120_000_000),
    )?)?;
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let _ = step(&mut state, &mut scratch, observation.observation());
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
    let grid = CapacityGrid::new(&[0.1_f64], &[10.0_f64, 20.0_f64], &[0.0_f64, 1.0_f64])?;
    let mut factor = capacity_factor(grid, 8.0_f64)?;
    for concurrency in [1.0_f64, 2.0_f64, 4.0_f64, 8.0_f64] {
        let completions = (concurrency * 10.0_f64) as u32;
        let window =
            ResourceWindow::new_with_starts(concurrency, 1.0_f64, completions, completions)?;
        update_constant_capacity_trace(&mut factor, window, concurrency as u32, completions);
    }

    assert!(factor.no_knee_probability() > 0.5_f64);
    Ok(())
}

#[test]
fn resource_window_is_consumed_once() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let transitions = constant_occupancy_transitions(80, 1_000_000);
    observation.set_resource_observation(
        ResourceWindow::new_with_starts(8.0, 1.0, 80, 80)?,
        8,
        8,
        &transitions,
    )?;
    let replacement = observation.set_resource_observation(
        ResourceWindow::new_with_starts(8.0, 1.0, 80, 80)?,
        8,
        8,
        &transitions,
    );
    assert!(matches!(
        replacement,
        Err(ObservationError::ResourceWindowPending)
    ));

    let consumed = observation.observation();
    assert!(consumed.resource.is_some());
    let next = observation.observation();
    assert!(next.resource.is_none());
    assert!(matches!(
        observation.set_resource_observation(
            ResourceWindow::new_with_starts(8.0_f64, 0.5_f64, 80, 80)?,
            8,
            8,
            &transitions
        ),
        Err(ObservationError::ResourceExposure)
    ));
    assert!(matches!(
        observation.set_resource_observation(
            ResourceWindow::new_with_starts(129.0_f64, 1.0_f64, 80, 80)?,
            129,
            129,
            &transitions
        ),
        Err(ObservationError::ResourceConcurrency)
    ));
    assert!(matches!(
        observation.set_resource_observation(
            ResourceWindow::new_with_starts(8.0_f64, 1.0_f64, 100_001, 100_001)?,
            8,
            8,
            &transitions,
        ),
        Err(ObservationError::ResourceAttemptCount)
    ));
    Ok(())
}

#[test]
fn occupancy_trace_contract_rejects_each_invalid_value() -> Result<(), TestError> {
    let mut configuration = configuration()?;
    configuration.resource_window_attempt_count_max = 2;
    let mut observation = ObservationBuffer::new(&configuration)?;
    assert!(matches!(
        ResourceWindow::new_with_starts(1.0_f64, 1.000_000_5_f64, 0, 0),
        Err(crate::ResourceWindowError::ClockResolution)
    ));
    let empty = ResourceWindow::new_with_starts(0.0_f64, 1.0_f64, 0, 0)?;
    assert!(matches!(
        observation.set_resource_observation(empty, 129, 129, &[]),
        Err(ObservationError::ResourceBusySlots)
    ));
    let outside = [OccupancyTransition::new(1_000_001, 0, 0)];
    assert!(matches!(
        observation.set_resource_observation(empty, 0, 0, &outside),
        Err(ObservationError::ResourceTransitionTime)
    ));
    let unordered = [
        OccupancyTransition::new(2, 0, 0),
        OccupancyTransition::new(1, 0, 0),
    ];
    assert!(matches!(
        observation.set_resource_observation(empty, 0, 0, &unordered),
        Err(ObservationError::ResourceTransitionOrder)
    ));
    let completion_underflow = [OccupancyTransition::new(1, 1, 0)];
    let one_completion = ResourceWindow::new_with_starts(0.0_f64, 1.0_f64, 1, 0)?;
    assert!(matches!(
        observation.set_resource_observation(one_completion, 0, 0, &completion_underflow),
        Err(ObservationError::ResourceBusySlots)
    ));
    let start_overflow = [OccupancyTransition::new(1, 0, 1)];
    let one_start = ResourceWindow::new_with_starts(128.0_f64, 1.0_f64, 0, 1)?;
    assert!(matches!(
        observation.set_resource_observation(one_start, 128, 128, &start_overflow),
        Err(ObservationError::ResourceBusySlots)
    ));
    let balanced = [OccupancyTransition::new(500_000, 1, 1)];
    assert!(matches!(
        observation.set_resource_observation(empty, 1, 1, &balanced),
        Err(ObservationError::ResourceTraceSummary)
    ));
    let wrong_starts = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 1, 0)?;
    assert!(matches!(
        observation.set_resource_observation(wrong_starts, 1, 1, &balanced),
        Err(ObservationError::ResourceTraceSummary)
    ));
    let balanced_window = ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 1, 1)?;
    assert!(matches!(
        observation.set_resource_observation(balanced_window, 1, 0, &balanced),
        Err(ObservationError::ResourceTraceSummary)
    ));
    let wrong_mean = ResourceWindow::new_with_starts(2.0_f64, 1.0_f64, 1, 1)?;
    assert!(matches!(
        observation.set_resource_observation(wrong_mean, 1, 1, &balanced),
        Err(ObservationError::ResourceTraceSummary)
    ));
    let too_many = [OccupancyTransition::new(0, 0, 0); 6];
    assert!(matches!(
        observation.set_resource_observation(empty, 0, 0, &too_many),
        Err(ObservationError::ResourceTransitionCapacity)
    ));
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
fn arrival_change_point_replaces_stale_rate_evidence() -> Result<(), TestError> {
    let prior = ArrivalPrior::new(100.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64)?;
    let mut factor = ArrivalFactor::new(&prior);
    for _ in 0_u32..100 {
        factor.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);
    }
    let old_rate = factor.expected_rate(1_000_000);
    for _ in 0_u32..8 {
        factor.update(ArrivalEvidence::new(400, 1_000_000), None, 1_000_000);
    }
    let replaced_rate = factor.expected_rate(1_000_000);
    assert!(
        old_rate < 110.0_f64 && replaced_rate > 2.0_f64 * old_rate,
        "old rate={old_rate}, replaced rate={replaced_rate}"
    );
    Ok(())
}

#[test]
fn arrival_change_point_normalizes_after_an_extreme_rate_change() -> Result<(), TestError> {
    let prior = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64)?;
    let upper_rate = prior.coverage()[0].upper_endpoint();
    let mut factor = ArrivalFactor::new(&prior);

    factor.update(ArrivalEvidence::new(10_000, 1_000_000), None, 1_000_000);

    let rate = factor.expected_rate(1_000_000);
    let boundary = factor.boundary_diagnostic(1_000_000);
    assert!(
        rate.is_finite()
            && rate >= 0.9_f64 * upper_rate
            && boundary.exceeds_budget()
            && boundary.upper_endpoint_probability > boundary.probability_budget,
        "rate={rate}, upper rate={upper_rate}, boundary={boundary:?}"
    );
    Ok(())
}

#[test]
fn missing_arrival_prediction_is_cadence_invariant() -> Result<(), TestError> {
    let prior = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64)?;
    let mut coarse = ArrivalFactor::new(&prior);
    let mut fine = ArrivalFactor::new(&prior);
    coarse.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);
    fine.update(ArrivalEvidence::new(100, 1_000_000), None, 1_000_000);

    for tick in 1_u64..1_000 {
        fine.expected_rate(1_000_000 + tick * 1_000);
    }
    let coarse_prediction = coarse.expected_rate(2_000_000);
    let fine_prediction = fine.expected_rate(2_000_000);

    assert!((coarse_prediction - fine_prediction).abs() < 1.0e-12_f64);
    Ok(())
}

#[test]
fn missing_interval_weakens_stale_arrival_evidence_before_the_next_update() -> Result<(), TestError>
{
    let prior = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 90.0_f64)?;
    let mut contiguous = ArrivalFactor::new(&prior);
    let mut missing = ArrivalFactor::new(&prior);
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
fn partition_factor_learns_a_normalized_skew() -> Result<(), TestError> {
    let mut factor = PartitionFactor::new(4)?;
    factor.update(&[90, 10, 0, 0]);
    let mut shares = [0.0_f64; 4];

    assert!(factor.write_expected_shares(&mut shares));
    let sum = shares.iter().copied().sum::<f64>();
    assert!((sum - 1.0_f64).abs() < 1.0e-12_f64);
    assert!(shares[0] > 0.85_f64);
    Ok(())
}

#[test]
fn partition_prior_uses_one_jeffreys_half_unit_per_partition() -> Result<(), TestError> {
    let mut factor = PartitionFactor::new(4)?;
    factor.update(&[1, 0, 0, 0]);
    let check = crate::partition_prior_predictive_check(4)?;
    let hottest = check.hottest_share_quantiles();
    let entropy = check.share_entropy_quantiles();
    let uniform_hottest = check.uniform_hottest_share_quantiles();
    let uniform_entropy = check.uniform_share_entropy_quantiles();
    let mut shares = [0.0_f64; 4];

    assert!(factor.write_expected_shares(&mut shares));
    assert!(close_relative(shares[0], 0.5_f64));
    assert!(close_relative(shares[1], 1.0_f64 / 6.0_f64));
    assert!(hottest[0] < hottest[1] && hottest[1] < hottest[2]);
    assert!(entropy[0] < entropy[1] && entropy[1] < entropy[2]);
    assert!(hottest[1] > uniform_hottest[1]);
    assert!(entropy[1] < uniform_entropy[1]);
    assert!(check.quantile_rank_error_max() < 0.1_f64);
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
    let factor = capacity_factor(grid, 1.0_f64)?;

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
    let mut factor = capacity_factor(grid, 1.0_f64)?;
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
    let grid = CapacityGrid::new(
        &[0.01_f64, 0.1_f64],
        &[100.0_f64, 1_000.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let cell_count = grid.cell_count() as usize;
    let change_rate = 2.0_f64.ln();
    let mut coarse = capacity_factor_with_rate(grid.clone(), change_rate, 32.0_f64)?;
    let mut fine = capacity_factor_with_rate(grid, change_rate, 32.0_f64)?;
    let evidence = ResourceWindow::new_with_starts(32.0_f64, 10.0_f64, 3_200, 3_200)?;
    update_constant_capacity_trace(&mut coarse, evidence, 32, 3_200);
    update_constant_capacity_trace(&mut fine, evidence, 32, 3_200);

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
    let prior = LaunchPrior::kubernetes()?;
    let mut coarse = LaunchTimeFactor::new(&prior);
    let mut fine = LaunchTimeFactor::new(&prior);
    let lumps = [ReadinessLump::new(
        ReadinessGroupId(1),
        2,
        ReadinessObservation::ready(
            ModelTime::from_micros(0),
            ModelTime::from_micros(30_000_000),
        )?,
    )?];
    coarse.update(
        Level::new(),
        crate::LaunchEvidence::new(
            ModelTime::from_micros(0),
            2,
            ModelTime::from_micros(30_000_000),
            &lumps,
        ),
    );
    fine.update(
        Level::new(),
        crate::LaunchEvidence::new(
            ModelTime::from_micros(0),
            2,
            ModelTime::from_micros(30_000_000),
            &lumps,
        ),
    );

    coarse.transition(Duration::from_secs(1));
    for _ in 0_u32..1_000 {
        fine.transition(Duration::from_millis(1));
    }

    let mut values = [0.0_f64; 6];
    let mut coarse_probability = [0.0_f64; 6];
    let mut fine_probability = [0.0_f64; 6];
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
        fine_observation.advance_model_time(ModelTime::from_micros(now_micros))?;
        let _ = step(
            &mut fine_state,
            &mut fine_scratch,
            fine_observation.observation(),
        );
    }
    coarse_observation.advance_model_time(ModelTime::from_micros(1_000_000))?;
    let coarse = step(
        &mut coarse_state,
        &mut coarse_scratch,
        coarse_observation.observation(),
    );
    fine_observation.advance_model_time(ModelTime::from_micros(1_000_000))?;
    let fine = step(
        &mut fine_state,
        &mut fine_scratch,
        fine_observation.observation(),
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
    let arrival_count = usize::try_from(state.arrival_posterior_value_count())
        .map_err(|_| ConfigurationError::PlatformLimit)?;
    let mut arrival_values = vec![0.0_f64; arrival_count];
    let mut arrival_probabilities = vec![0.0_f64; arrival_count];
    state.write_arrival_posterior(&mut arrival_values, &mut arrival_probabilities)?;
    assert!(arrival_values.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(close_relative(arrival_probabilities.iter().sum(), 1.0_f64));
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
    observation.advance_model_time(ModelTime::from_micros(1))?;
    let _ = step(&mut state, &mut scratch, observation.observation());

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
        .map_err(|_| ConfigurationError::PlatformLimit)?;
    let mut values = vec![0.0_f64; count];
    let mut probabilities = vec![0.0_f64; count];
    state.write_posterior(PosteriorQuery::Capacity, &mut values, &mut probabilities)?;
    Ok(probabilities)
}

#[test]
fn one_knee_cell_still_competes_with_no_knee() -> Result<(), TestError> {
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64])?;
    let factor = capacity_factor(grid, 1.0_f64)?;
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
    let outcome = evaluate_constant_supply(
        &cohorts,
        slots,
        horizon_micros,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    let roundoff = 8.0_f64 * f64::EPSILON * slots.max(1.0_f64);
    let actual = outcome.shortfall <= roundoff;
    let expected = exhaustive_feasible(&cohorts, slots);
    actual == expected
}

#[test]
fn feasible_fluid_schedule_tolerates_roundoff_slivers() -> Result<(), TestError> {
    let mut cohorts = WorkCohorts::new(3);
    for (partition, (release, deadline, work)) in [
        (0_u64, 11_u64, 3.2e-5_f64),
        (6, 15, 1.5e-5_f64),
        (15, 31, 3.2e-5_f64),
    ]
    .into_iter()
    .enumerate()
    {
        cohorts.push_values(release, deadline, work, partition as u32);
    }
    let mut scratch = EdfScratch::new(3)?;
    prepare(&cohorts, &mut scratch);
    let slots = 4.0_f64;
    let outcome = evaluate_constant_supply(
        &cohorts,
        slots,
        31,
        0.0_f64,
        &NO_FUTURE_ARRIVALS,
        &mut scratch,
    );
    assert!(outcome.shortfall <= 8.0_f64 * f64::EPSILON * slots);
    assert!(exhaustive_feasible(&cohorts, slots));
    Ok(())
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());

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
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 1,
        replica_count_max: 1,
        slots_per_replica: 1,
        posterior_sample_count: 1_024,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let missed_at_low_knee = 75.0_f64 - 50.0_f64;
    let low_knee_late_area = missed_at_low_knee * missed_at_low_knee / (2.0_f64 * 50.0_f64);
    let exact_loss = 0.25_f64 * low_knee_late_area / 75.0_f64;

    assert!(
        (apply.diagnostics.miss_delay_fraction - exact_loss).abs() < 1.0e-5_f64,
        "actual={}, exact={exact_loss}",
        apply.diagnostics.miss_delay_fraction
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
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 2,
        replica_count_max: 2,
        slots_per_replica: 1,
        posterior_sample_count: 128,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: negligible_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let mut costs = [0.0_f64; 2];
    scratch.write_decision_expected_costs(&mut costs)?;
    assert!(costs.iter().all(|cost| *cost > 0.0_f64), "costs={costs:?}");
    assert!(apply.diagnostics.miss_delay_fraction > 0.0_f64);
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
fn decision_curve_contains_the_selected_expected_cost() -> Result<(), TestError> {
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let mut costs = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_costs(&mut costs)?;

    assert!(close_relative(
        costs[apply.target as usize - 1],
        apply.diagnostics.expected_cost,
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
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
    observation.advance_model_time(ModelTime::from_micros(1))?;
    let warm_decision = step(&mut state, &mut scratch, observation.observation());
    black_box(warm_decision);
    observation.advance_model_time(ModelTime::from_micros(2))?;
    let warm_decision = step(&mut state, &mut scratch, observation.observation());
    black_box(warm_decision);

    let pool = rayon::ThreadPoolBuilder::new().build()?;
    observation.advance_model_time(ModelTime::from_micros(3))?;
    let _ = pool.install(|| step(&mut state, &mut scratch, observation.observation()));
    observation.advance_model_time(ModelTime::from_micros(4))?;
    let allocation = allocation_counter::measure(|| {
        let _ = pool.install(|| step(&mut state, &mut scratch, observation.observation()));
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
fn observation_stamp_drives_step_regression_check() -> Result<(), TestError> {
    let configuration = configuration()?;
    let mut state = ScaleState::new(configuration.clone(), grid()?)?;
    let mut scratch = state.new_scratch()?;
    let mut first = ObservationBuffer::new(&configuration)?;
    let mut second = ObservationBuffer::new(&configuration)?;
    first.advance_model_time(ModelTime::from_micros(2))?;
    let _ = step(&mut state, &mut scratch, first.observation());

    second.advance_model_time(ModelTime::from_micros(1))?;
    let decision = step(&mut state, &mut scratch, second.observation());

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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    assert_eq!(apply.target, 1);
    assert!(apply.diagnostics.miss_delay_fraction > 0.0_f64);
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
    observation.advance_model_time(ModelTime::from_micros(1))?;

    let decision = step(&mut state, &mut scratch, observation.observation());
    let ScaleDecision::Apply(apply) = decision else {
        return Err(TestError::UnexpectedHold);
    };
    let mut costs = [0.0_f64; 32];
    scratch.write_decision_expected_costs(&mut costs)?;
    assert!(costs.iter().all(|cost| *cost > 0.0_f64), "costs={costs:?}");
    assert!(apply.diagnostics.miss_delay_fraction > 0.0_f64);
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
            let quantile = state.lead_time_predictive_quantile(direction, 3, probability)?;
            let time_error = state
                .configuration()
                .launch_time_prior
                .budget()
                .path_time_error_seconds();
            let lower = state.lead_time_predictive_cdf(
                direction,
                3,
                (quantile - time_error).max(f64::MIN_POSITIVE),
            );
            let upper = state.lead_time_predictive_cdf(direction, 3, quantile + time_error);
            assert!(
                lower <= probability && upper >= probability,
                "the predictive quantile must meet its declared time error"
            );
        }
    }
    Ok(())
}

#[test]
fn incomplete_actuation_uses_the_conditional_remaining_time() -> Result<(), TestError> {
    let cells = [DurationCell::new(30.0_f64, 0.3_f64)?];
    let coverage = [
        PriorCoverageRecord::new(1.0_f64, 300.0_f64, 1.0e-12_f64, 1.0e-12_f64, 1.0e-12_f64),
        PriorCoverageRecord::new(1.0_f64, 300.0_f64, 1.0e-12_f64, 1.0e-12_f64, 1.0e-12_f64),
    ];
    let prior = LaunchPrior::new(
        PriorArtifactIdentity::new(2, 1, 2),
        PriorArtifactBudget::new(1, 64, 64, 1.0e-4_f64, 1.0e-6_f64, 1.0e-4_f64),
        &coverage,
        LaunchPriorGrid::new(&[0.0_f64], &[0.0_f64], &cells, &cells),
        &[1.0_f64],
        0.0_f64,
    )?;
    let factor = LaunchTimeFactor::new(&prior);
    let elapsed_seconds = 20.0_f64;
    let direction = TransitionDirection::Down;
    let delta = 1;
    let mut coordinate = RandomStream::new(91);
    let _component_selector = coordinate.open_unit_f64();
    let uniform = coordinate.open_unit_f64();
    let elapsed_cdf = factor.predictive_cdf(direction, delta, elapsed_seconds);
    let expected_cdf = elapsed_cdf + uniform * (1.0_f64 - elapsed_cdf);
    let mut draw = RandomStream::new(91);
    let remaining = factor.sample_remaining_seconds(direction, delta, elapsed_seconds, &mut draw);
    let actual_cdf = factor.predictive_cdf(direction, delta, elapsed_seconds + remaining);

    assert!(
        (actual_cdf - expected_cdf).abs() < 1.0e-4_f64,
        "the remaining-time draw must invert the conditional survival distribution"
    );
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
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 64,
        replica_count_max: 128,
        slots_per_replica: 32,
        posterior_sample_count: 4_096,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::new(4.0_f64, 0.01_f64, 1.0_f64 / 90.0_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
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
        &[0.0_f64],
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
    observation.set_current_replicas(2)?;
    observation.advance_model_time(ModelTime::from_micros(240_000_000))?;
    let ScaleDecision::Apply(_first) = step(&mut state, &mut scratch, observation.observation())
    else {
        return Err(TestError::UnexpectedHold);
    };

    let mut apply = None;
    for window in 2_u64..=15 {
        let now_micros = 239_000_000 + window * 1_000_000;
        let mut observation = ObservationBuffer::new(&configuration)?;
        observation.set_arrivals(300, 1_000_000)?;
        let transitions = constant_occupancy_transitions(300, 1_000_000);
        observation.set_resource_observation(
            ResourceWindow::new_with_starts(30.0_f64, 1.0_f64, 300, 300)?,
            30,
            30,
            &transitions,
        )?;
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
        observation.advance_model_time(ModelTime::from_micros(now_micros))?;
        let ScaleDecision::Apply(decision) =
            step(&mut state, &mut scratch, observation.observation())
        else {
            return Err(TestError::UnexpectedHold);
        };
        apply = Some(decision);
    }
    let apply = apply.ok_or(TestError::MissingDecisionCurve)?;
    let selected =
        usize::try_from(apply.target - 1).map_err(|_| ConfigurationError::PlatformLimit)?;
    let mut costs = vec![0.0_f64; scratch.decision_candidate_count()];
    scratch.write_decision_expected_costs(&mut costs)?;
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
    assert!(costs[2] <= costs[1], "costs={:?}", &costs[..8]);
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

#[test]
fn model_priors_carry_validated_artifacts() -> Result<(), TestError> {
    let validated = ArrivalPrior::new(1.0_f64, 1.0_f64, 1.0_f64 / 86_400.0_f64)?;
    assert_eq!(ArrivalPrior::test_artifact()?, validated);
    assert_eq!(validated.artifact().version(), 1);
    assert_eq!(validated.coverage().len(), 5);
    assert!(
        validated.coverage().iter().all(
            |record| record.tail_probability() <= validated.budget().boundary_probability_max()
        )
    );
    let reliability = ReliabilityPrior::authored()?;
    assert_eq!(reliability.artifact().version(), 1);
    assert_eq!(reliability.coverage().len(), 2);
    assert!(reliability.coverage().iter().all(|record| {
        record.tail_probability() <= reliability.budget().boundary_probability_max()
            && record.decision_cost_error() <= reliability.budget().decision_cost_error_max()
    }));
    Ok(())
}

fn configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 16,
        calendar_segment_count_max: 16,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 16,
        replica_count_max: 32,
        slots_per_replica: 4,
        posterior_sample_count: 128,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::test_artifact()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
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
        observation.advance_model_time(ModelTime::from_micros(1))?;
        Ok(step(&mut state, &mut scratch, observation.observation()))
    })
}

fn negligible_arrival_prior() -> Result<ArrivalPrior, TestError> {
    Ok(ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64)?)
}

fn grid() -> Result<CapacityGrid, TestError> {
    Ok(CapacityGrid::new(
        &[0.05, 0.1],
        &[50.0, 100.0],
        &[0.0, 1.0],
    )?)
}

fn capacity_factor(grid: CapacityGrid, concurrency_max: f64) -> Result<CapacityFactor, TestError> {
    capacity_factor_with_rate(grid, 1.0_f64 / 86_400.0_f64, concurrency_max)
}

fn constant_occupancy_transitions(
    completed_attempts: u32,
    exposure_micros: u64,
) -> Vec<OccupancyTransition> {
    (0..completed_attempts)
        .map(|index| {
            OccupancyTransition::new(
                u64::from(index + 1) * exposure_micros / u64::from(completed_attempts + 1),
                1,
                1,
            )
        })
        .collect()
}

fn update_constant_capacity_trace(
    factor: &mut CapacityFactor,
    window: ResourceWindow,
    concurrency: u32,
    completed_attempts: u32,
) {
    let transitions = constant_occupancy_transitions(completed_attempts, window.exposure_micros());
    let offsets = transitions
        .iter()
        .map(|transition| transition.offset_micros())
        .collect::<Vec<_>>();
    let completed = vec![1_u32; transitions.len()];
    let started = vec![1_u32; transitions.len()];
    factor.update(
        occupancy_trace_for_test(
            window,
            concurrency,
            concurrency,
            u128::from(concurrency) * u128::from(window.exposure_micros()),
            &offsets,
            &completed,
            &started,
        ),
        Duration::from_micros(window.exposure_micros()),
    );
}

fn capacity_factor_with_rate(
    grid: CapacityGrid,
    change_rate_per_second: f64,
    concurrency_max: f64,
) -> Result<CapacityFactor, TestError> {
    Ok(CapacityFactor::new_with_prior(
        grid,
        change_rate_per_second,
        &ArrivalPrior::test_artifact()?,
        concurrency_max,
        1.0_f64,
        100_000,
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
    CapacityModel(#[from] crate::CapacityModelError),
    #[error(transparent)]
    Posterior(#[from] crate::PosteriorError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    DecisionCurve(#[from] crate::DecisionCurveError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
    #[error(transparent)]
    LaunchEvidence(#[from] crate::LaunchEvidenceError),
    #[error(transparent)]
    LeadTimePrior(#[from] crate::LeadTimePriorError),
    #[error(transparent)]
    PredictiveQuantile(#[from] crate::PredictiveQuantileError),
    #[error(transparent)]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("the model held when the test required an applied decision")]
    UnexpectedHold,
    #[error("a test count exceeds the platform limit")]
    PlatformLimit,
    #[error("the decision columns are missing")]
    MissingDecisionCurve,
}
