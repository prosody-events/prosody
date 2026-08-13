use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{SCHEDULED_PARTITION, prepare_work_cohorts, scenario_event_count};
use crate::edf::ArrivalPath;
use crate::types::WorkCohorts;
use crate::{
    ArrivalPrior, ArrivalPriorError, BacklogCohort, CapacityGrid, CapacityGridError, Configuration,
    ConfigurationError, DemandClass, ModelTime, ObservationBuffer, ObservationError,
    ReliabilityPrior, SCHEDULED_RELEASE_COUNT_MAX, ScaleScratch, ScaleState, ScheduledRelease,
    ServiceObjective, TransitionPrior,
};

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
        scratch.resource_cohorts.work_slot_seconds(0),
        5.0_f64,
    ));
    assert!(approximately_equal(
        scratch.resource_cohorts.work_slot_seconds(1),
        7.0_f64,
    ));
    Ok(())
}

#[test]
fn scheduled_release_validation_rejects_invalid_inputs() -> Result<(), TestError> {
    let (_state, _scratch, mut observation) = test_model()?;
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
        SCHEDULED_RELEASE_COUNT_MAX + 1
    ];
    assert!(observation.set_scheduled_releases(&excess).is_err());
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
        .take(SCHEDULED_RELEASE_COUNT_MAX.saturating_sub(3))
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
    prepare_work_cohorts(
        &state,
        &mut scratch,
        input.cohorts,
        input.backlog,
        input.scheduled_releases,
    );

    let expected = releases
        .iter()
        .filter(|release| release.release_micros > now_micros)
        .collect::<Vec<_>>();
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
                scratch.resource_cohorts.work_slot_seconds(index),
                f64::from(release.count),
            )
            && scratch.resource_cohorts.partition(index) == SCHEDULED_PARTITION
    })
}

#[quickcheck]
fn scheduled_counts_are_equal_across_scenarios(count: u16) -> bool {
    let scheduled_count = f64::from(count) + 1.0_f64;
    let mut cohorts = WorkCohorts::new(1);
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
        partition_count: 4,
        replica_count_max: 4,
        slots_per_replica: 32,
        posterior_sample_count: 64,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64)?,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    })
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Arrival(#[from] ArrivalPriorError),
    #[error(transparent)]
    Capacity(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
}
