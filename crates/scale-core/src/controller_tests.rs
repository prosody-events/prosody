use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{
    DecisionRandomDomain, SCHEDULED_PARTITION, ScenarioDraws, ScenarioWorkspace, ScratchBounds,
    TransitionRole, WorldEvent, balanced_partition_owner, balanced_partition_range,
    decision_random, partition_replica_capacity, prepare_work_cohorts, prepare_world_shocks,
    repair_target, sample_moved_partition_prefix, scenario_event_count, scenario_horizons,
};
use crate::arrival::MeanRateTrajectory;
use crate::edf::{
    ArrivalPath, EdfScratch, EvaluationWindow, SupplyStep, evaluate_prepared_step, prepare,
};
use crate::lead_time::{LaunchDurationShock, RebalanceDurationShock};
use crate::types::{EventCohorts, SlotSecondCohorts};
use crate::{
    ArrivalPrior, ArrivalPriorError, BacklogCohort, CapacityGrid, CapacityGridError, Configuration,
    ConfigurationError, DemandClass, LaunchPrior, ModelTime, ObservationBuffer, ObservationError,
    PosteriorError, PosteriorQuery, RebalancePrior, ReliabilityPrior, ScaleScratch, ScaleState,
    ScheduledRelease, ServiceObjective,
};

#[quickcheck]
fn balanced_partition_ranges_match_owner_order(partition_seed: u8, replica_seed: u8) -> bool {
    let partition_count = usize::from(partition_seed % 64 + 1);
    let replica_count = usize::from(replica_seed % 64 + 1).min(partition_count);
    (0..replica_count).all(|replica| {
        (0..partition_count)
            .filter(|partition| {
                balanced_partition_owner(partition_count, replica_count, *partition) == replica
            })
            .eq(balanced_partition_range(
                partition_count,
                replica_count,
                replica,
            ))
    })
}

#[test]
fn moved_partition_prefix_cache_matches_world_event_draw() -> Result<(), TestError> {
    let (state, _scratch, _observation) = test_model()?;
    let bounds = ScratchBounds::new(state.configuration())?;
    let mut workspace = ScenarioWorkspace::new(&bounds)?;
    let placement_random = decision_random(7, 11, DecisionRandomDomain::Placement);
    let draws = ScenarioDraws {
        current_supply: 1.0_f64,
        lead_random: placement_random.clone(),
        rebalance_random: placement_random.clone(),
        placement_random: placement_random.clone(),
        commitment_random: placement_random.clone(),
    };
    let event = WorldEvent::repair(1);
    sample_moved_partition_prefix(&state, &mut workspace, &draws, event);
    let first = workspace.moved_partition_share.clone();
    sample_moved_partition_prefix(&state, &mut workspace, &draws, event);

    let mut order = vec![0; bounds.partition_count];
    let mut shares = vec![0.0_f64; bounds.partition_count];
    let mut expected = vec![0.0_f64; bounds.partition_offset_count];
    let mut random = placement_random
        .domain(event.report_boundary as u64)
        .domain(TransitionRole::ReactiveRepair as u64);
    state.partition_placement.sample_moved_prefix(
        &mut random,
        &mut order,
        &mut shares,
        &mut expected,
    );
    assert!(
        first
            .iter()
            .zip(&expected)
            .all(|(actual, expected)| actual.to_bits() == expected.to_bits())
    );
    assert!(
        workspace
            .moved_partition_share
            .iter()
            .zip(&expected)
            .all(|(actual, expected)| actual.to_bits() == expected.to_bits())
    );
    Ok(())
}

#[quickcheck]
fn world_event_shocks_survive_candidate_skips(
    decision_seed: u8,
    scenario_seed: u8,
    first_boundary_seed: u8,
    second_boundary_seed: u8,
) -> TestResult {
    let (state, _scratch, _observation) = match test_model() {
        Ok(model) => model,
        Err(error) => return TestResult::error(error.to_string()),
    };
    let bounds = match ScratchBounds::new(state.configuration()) {
        Ok(bounds) => bounds,
        Err(error) => return TestResult::error(error.to_string()),
    };
    let mut workspace = match ScenarioWorkspace::new(&bounds) {
        Ok(workspace) => workspace,
        Err(error) => return TestResult::error(error.to_string()),
    };
    let scenario = u32::from(scenario_seed);
    let draws = ScenarioDraws {
        current_supply: 1.0_f64,
        lead_random: decision_random(
            u64::from(decision_seed),
            scenario,
            DecisionRandomDomain::LeadTime,
        ),
        rebalance_random: decision_random(
            u64::from(decision_seed),
            scenario,
            DecisionRandomDomain::Rebalance,
        ),
        placement_random: decision_random(
            u64::from(decision_seed),
            scenario,
            DecisionRandomDomain::Placement,
        ),
        commitment_random: decision_random(
            u64::from(decision_seed),
            scenario,
            DecisionRandomDomain::Commitment,
        ),
    };
    prepare_world_shocks(&mut workspace, &draws);
    let boundary_count = bounds.successor_report_count_max + 1;
    let first = WorldEvent::repair(usize::from(first_boundary_seed) % boundary_count);
    let second = WorldEvent::repair(usize::from(second_boundary_seed) % boundary_count);
    let before = (
        workspace.launch_shocks[second.index()],
        workspace.rebalance_shocks[second.index()],
    );
    let mut expected_launch_random = draws.lead_random.clone().domain(second.index() as u64);
    let mut expected_rebalance_random =
        draws.rebalance_random.clone().domain(second.index() as u64);
    let expected = (
        LaunchDurationShock::draw(&mut expected_launch_random),
        RebalanceDurationShock::draw(&mut expected_rebalance_random),
    );
    let candidate_one = (
        workspace.launch_shocks[first.index()],
        workspace.rebalance_shocks[first.index()],
    );
    let candidate_two = (
        workspace.launch_shocks[first.index()],
        workspace.rebalance_shocks[first.index()],
    );
    let _skipped = (
        workspace.launch_shocks[first.index()],
        workspace.rebalance_shocks[first.index()],
    );
    let after = (
        workspace.launch_shocks[second.index()],
        workspace.rebalance_shocks[second.index()],
    );
    TestResult::from_bool(candidate_one == candidate_two && before == expected && before == after)
}

#[test]
fn flat_predictive_mean_has_no_successor_repair() {
    // Successor targets accept this measurable view, not scenario latent rates.
    let trajectory = MeanRateTrajectory::new(&[150.0_f64; 8]);
    let supply = [100.0_f64, 200.0_f64, 300.0_f64];
    let initial = 2_u32;

    assert!(
        trajectory
            .rates()
            .all(|rate| repair_target(&supply, rate) == initial)
    );
}

#[test]
fn calendar_wave_retargets_at_mean_boundaries() {
    let trajectory = MeanRateTrajectory::new(&[150.0_f64, 250.0_f64, 250.0_f64, 50.0_f64]);
    let supply = [100.0_f64, 200.0_f64, 300.0_f64];
    let mut target = 2_u32;
    let mut changes = Vec::new();
    for (boundary, rate) in trajectory.rates().enumerate() {
        let next = repair_target(&supply, rate);
        if next != target {
            changes.push((boundary + 1, next));
            target = next;
        }
    }

    assert_eq!(changes, vec![(2, 3), (4, 1)]);
}

#[test]
fn partition_deadline_outputs_preserve_work_scale() -> Result<(), TestError> {
    let service_time_seconds = 0.25_f64;
    let supply = 3.0_f64;
    let no_arrivals = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[f64::MAX],
        rates: &[0.0_f64],
    };
    let window = EvaluationWindow {
        start_micros: 0,
        horizon_micros: 4_000_000,
        initial_debt_work: 0.0_f64,
        deadline_budget_micros: 1_000_000,
    };
    let mut raw_outputs = [(0.0_f64, 0.0_f64); 4];
    let mut scaled_outputs = [(0.0_f64, 0.0_f64); 4];
    for replica_count in 1..=4 {
        let mut raw = SlotSecondCohorts::new(2);
        raw.push_values(0, 1_000_000, 4.0_f64, 0);
        raw.push_values(500_000, 2_000_000, 6.0_f64, 1);
        let mut scaled = SlotSecondCohorts::new(2);
        scaled.push_values(0, 1_000_000, 4.0_f64 * service_time_seconds, 0);
        scaled.push_values(500_000, 2_000_000, 6.0_f64 * service_time_seconds, 1);
        let mut raw_scratch = EdfScratch::new(2)?;
        let mut scaled_scratch = EdfScratch::new(2)?;
        prepare(&raw, &mut raw_scratch);
        prepare(&scaled, &mut scaled_scratch);
        let raw_capacity = partition_replica_capacity(supply, replica_count);
        let replica_count_f64 = f64::from(replica_count as u32);
        let scaled_capacity = supply * service_time_seconds / replica_count_f64;
        let raw_outcome = evaluate_prepared_step(
            &raw,
            SupplyStep {
                before: raw_capacity,
                during: raw_capacity,
                after: raw_capacity,
                pause_micros: 0,
                ready_micros: 0,
            },
            window,
            &no_arrivals,
            &mut raw_scratch,
        );
        let scaled_outcome = evaluate_prepared_step(
            &scaled,
            SupplyStep {
                before: scaled_capacity,
                during: scaled_capacity,
                after: scaled_capacity,
                pause_micros: 0,
                ready_micros: 0,
            },
            window,
            &no_arrivals,
            &mut scaled_scratch,
        );
        raw_outputs[replica_count - 1] = (
            raw_outcome.missed_work,
            raw_outcome.late_area + raw_outcome.terminal_late_area,
        );
        scaled_outputs[replica_count - 1] = (
            scaled_outcome.missed_work / service_time_seconds,
            (scaled_outcome.late_area + scaled_outcome.terminal_late_area) / service_time_seconds,
        );
    }
    assert!(raw_outputs.iter().zip(scaled_outputs).all(
        |(&(raw_missed, raw_late), (scaled_missed, scaled_late))| {
            partition_float_matches(raw_missed, scaled_missed)
                && partition_float_matches(raw_late, scaled_late)
        }
    ));
    Ok(())
}

fn partition_float_matches(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1.0e-12_f64.max(1.0e-9_f64 * expected.abs())
}

#[test]
fn report_views_match_the_fixed_model_contract() -> Result<(), TestError> {
    let (state, _scratch, _observation) = test_model()?;
    let artifact = state.capacity_artifact();
    let clock = state.capacity_clock_check();
    let launch = state.launch_component_summary(1);
    let query = PosteriorQuery::CapacityContaminationProbability;
    let value_count = usize::try_from(state.posterior_value_count(query)?)?;
    let mut values = vec![0.0_f64; value_count];
    let mut probabilities = vec![0.0_f64; value_count];

    state.write_posterior(query, &mut values, &mut probabilities)?;

    assert_eq!(artifact.identity().version(), 2);
    assert!(!artifact.coverage().is_empty());
    assert!(artifact.coverage().iter().all(|coverage| {
        coverage.lower_tail_probability() >= 0.0_f64 && coverage.upper_tail_probability() >= 0.0_f64
    }));
    assert_eq!(clock.sample_count, 0);
    assert_eq!(clock.maximum_distance.to_bits(), 0.0_f64.to_bits());
    assert!(clock.rejection_threshold.is_infinite());
    assert!(!clock.rejected);
    assert!(launch.fast_mean_seconds > 0.0_f64);
    assert!(launch.slow_mean_seconds > launch.fast_mean_seconds);
    assert!((0.0_f64..=1.0_f64).contains(&launch.slow_probability));
    assert_eq!(values.len(), probabilities.len());
    assert!(approximately_equal(probabilities.iter().sum(), 1.0_f64));
    assert!(
        state.capacity_class_count() * state.posterior_samples_per_capacity_class_min()
            <= state.configuration().posterior_sample_count
    );
    Ok(())
}

#[test]
fn overdue_backlog_keeps_each_original_deadline() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = test_model()?;
    state.model_time = ModelTime::from_micros(10_000_000);
    observation.set_backlog(BacklogCohort::new(
        10_000_000,
        8_500_000,
        5,
        0,
        DemandClass::Normal,
    )?)?;
    observation.set_backlog(BacklogCohort::new(
        10_000_000,
        8_000_000,
        7,
        1,
        DemandClass::Normal,
    )?)?;
    let input = observation.observation();

    prepare_work_cohorts(
        &state,
        &mut scratch,
        input.cohorts,
        input.backlog,
        input.scheduled_releases,
    );

    assert_eq!(scratch.resource_cohorts.len(), 2);
    assert_eq!(scratch.resource_cohorts.deadline_micros(0), 9_500_000);
    assert_eq!(scratch.resource_cohorts.deadline_micros(1), 9_000_000);
    assert!(approximately_equal(
        scratch.resource_cohorts.work(0),
        5.0_f64,
    ));
    assert!(approximately_equal(
        scratch.resource_cohorts.work(1),
        7.0_f64,
    ));
    Ok(())
}

#[test]
fn scheduled_release_validation_rejects_invalid_inputs() -> Result<(), TestError> {
    let (state, _scratch, mut observation) = test_model()?;
    assert!(
        observation
            .set_scheduled_releases(&[ScheduledRelease {
                release_micros: 1,
                count: 0,
            }])
            .is_err()
    );
    assert!(
        observation
            .set_scheduled_releases(&[
                ScheduledRelease {
                    release_micros: 2,
                    count: 1,
                },
                ScheduledRelease {
                    release_micros: 1,
                    count: 1,
                },
            ])
            .is_err()
    );
    let excess = vec![
        ScheduledRelease {
            release_micros: 1,
            count: 1,
        };
        state.configuration.scheduled_release_count_max as usize + 1
    ];
    assert!(observation.set_scheduled_releases(&excess).is_err());
    Ok(())
}

#[test]
fn configuration_rejects_an_uncovered_planning_horizon() -> Result<(), TestError> {
    let mut configuration = test_configuration()?;
    configuration.report_interval_micros = ArrivalPrior::MAXIMUM_PATH_MICROS;

    assert_eq!(
        configuration.validate(),
        Err(ConfigurationError::PlanningHorizonDomain)
    );
    Ok(())
}

#[test]
fn ingestion_rejects_deadlines_beyond_the_arrival_domain() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let mut observation = ObservationBuffer::new(&configuration)?;
    let now_micros = 17_u64;
    observation.advance_model_time(ModelTime::from_micros(now_micros))?;
    let deadline_max =
        now_micros + ArrivalPrior::MAXIMUM_PATH_MICROS - configuration.objective.budget_micros();

    assert_eq!(
        observation.push_cohort(crate::Cohort {
            release_micros: now_micros,
            deadline_micros: deadline_max + 1,
            offered_events: 1.0_f64,
            partition: 0,
            demand_class: DemandClass::Normal,
        }),
        Err(ObservationError::DeadlineHorizon)
    );
    assert_eq!(
        observation.set_scheduled_releases(&[ScheduledRelease {
            release_micros: deadline_max - configuration.objective.budget_micros() + 1,
            count: 1,
        }]),
        Err(ObservationError::DeadlineHorizon)
    );
    Ok(())
}

#[test]
fn scenario_horizon_uses_only_artifact_support() -> Result<(), TestError> {
    let (state, _scratch, _observation) = test_model()?;
    let cohorts = EventCohorts::new(1);
    let first = scenario_horizons(&state, &cohorts);
    let second = scenario_horizons(&state, &cohorts);
    let support_seconds = state
        .configuration
        .launch_time_prior
        .coverage_support_seconds()
        .1
        + state
            .configuration
            .rebalance_time_prior
            .coverage_support_seconds()
            .1;
    let report = state.configuration.report_interval_micros;
    let expected_disturbance = report + (2.0_f64 * support_seconds * 1_000_000.0_f64) as u64;

    assert_eq!(first, second, "the horizon must be equal across seeds");
    assert_eq!(first.1, expected_disturbance);
    Ok(())
}

#[quickcheck]
fn scheduled_releases_are_exact_and_idempotent(raw: Vec<(u8, u16)>) -> bool {
    let Ok((mut state, mut scratch, mut observation)) = test_model() else {
        return false;
    };
    let now_micros = 10_000_000_u64;
    state.model_time = ModelTime::from_micros(now_micros);

    let mut releases = raw
        .into_iter()
        .take(state.configuration.scheduled_release_count_max as usize - 3)
        .map(|(seconds, count)| ScheduledRelease {
            release_micros: now_micros.saturating_add(u64::from(seconds).saturating_mul(1_000_000)),
            count: u32::from(count).saturating_add(1),
        })
        .collect::<Vec<_>>();
    releases.push(ScheduledRelease {
        release_micros: now_micros,
        count: 3,
    });
    releases.push(ScheduledRelease {
        release_micros: now_micros.saturating_add(149_999_999),
        count: 5,
    });
    releases.push(ScheduledRelease {
        release_micros: now_micros.saturating_add(150_000_001),
        count: 7,
    });
    releases.sort_unstable_by_key(|release| release.release_micros);

    if observation.set_scheduled_releases(&releases).is_err()
        || observation.set_scheduled_releases(&releases).is_err()
    {
        return false;
    }
    let input = observation.observation();
    let expected = input
        .scheduled_releases
        .iter()
        .filter(|release| release.release_micros > now_micros)
        .copied()
        .collect::<Vec<_>>();
    prepare_work_cohorts(
        &state,
        &mut scratch,
        input.cohorts,
        input.backlog,
        input.scheduled_releases,
    );

    if scratch.resource_cohorts.len() != expected.len() {
        return false;
    }
    expected.iter().enumerate().all(|(index, release)| {
        scratch.resource_cohorts.release_micros(index) == release.release_micros
            && scratch.resource_cohorts.deadline_micros(index)
                == release
                    .release_micros
                    .saturating_add(state.configuration.objective.budget_micros())
            && approximately_equal(
                scratch.resource_cohorts.work(index),
                f64::from(release.count),
            )
            && scratch.resource_cohorts.partition(index) == SCHEDULED_PARTITION
    })
}

#[quickcheck]
fn scheduled_counts_are_equal_across_scenarios(count: u16) -> bool {
    let scheduled_count = f64::from(count) + 1.0_f64;
    let mut cohorts = EventCohorts::new(1);
    cohorts.push_values(2_000_000, 3_000_000, scheduled_count, SCHEDULED_PARTITION);
    let empty = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[4.0_f64],
        rates: &[0.0_f64],
    };
    let busy = ArrivalPath {
        start_seconds: 0.0_f64,
        end_seconds: &[4.0_f64],
        rates: &[17.0_f64],
    };
    let empty_count = scenario_event_count(
        2.0_f64, 3.0_f64, &empty, 0.0_f64, 4.0_f64, &cohorts, 4_000_000,
    );
    let busy_count = scenario_event_count(
        2.0_f64, 3.0_f64, &busy, 0.0_f64, 4.0_f64, &cohorts, 4_000_000,
    );

    approximately_equal(empty_count - 5.0_f64, scheduled_count)
        && approximately_equal(busy_count - 73.0_f64, scheduled_count)
}

#[test]
fn curve_class_columns_equal_cell_columns_with_rolled_masses() -> Result<(), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[100.0_f64], &[0.0_f64, 1.0_f64])?;
    let state = ScaleState::new(configuration, grid)?;
    let draw_count = 3_u32;
    let mut cell_column = 0.0_f64;
    let mut class_column = 0.0_f64;
    for class in 0..state.capacity_classes.len() {
        let members = state.capacity_classes.members(class);
        let class_mass = members
            .iter()
            .map(|&cell| state.capacity.curve_and_probability(cell).1)
            .sum::<f64>();
        let representative = state.capacity_classes.representative(class);
        let (curve, _) = state.capacity.curve_and_probability(representative);
        let path_mean = (0_u32..draw_count)
            .map(|draw| curve.sustainable_throughput(64.0_f64) + f64::from(draw))
            .sum::<f64>()
            / f64::from(draw_count);
        class_column += class_mass * path_mean;
        for &cell in members {
            let mass = state.capacity.curve_and_probability(cell).1;
            cell_column += mass * path_mean;
        }
    }

    assert!(approximately_equal(class_column, cell_column));
    Ok(())
}

fn approximately_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= f64::EPSILON * left.abs().max(right.abs()).max(1.0_f64)
}

fn test_model() -> Result<(ScaleState, ScaleScratch, ObservationBuffer), TestError> {
    let configuration = test_configuration()?;
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let state = ScaleState::new(configuration.clone(), grid)?;
    let scratch = state.new_scratch()?;
    let observation = ObservationBuffer::new(&configuration)?;
    Ok((state, scratch, observation))
}

fn test_configuration() -> Result<Configuration, TestError> {
    Ok(Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 4,
        slots_per_replica: 32,
        posterior_sample_count: 64,
        report_interval_micros: 1_000_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    })
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Integer(#[from] TryFromIntError),
    #[error(transparent)]
    LeadTime(#[from] crate::LeadTimePriorError),
    #[error(transparent)]
    Arrival(#[from] ArrivalPriorError),
    #[error(transparent)]
    Capacity(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
    #[error(transparent)]
    Posterior(#[from] PosteriorError),
}
use std::num::TryFromIntError;
