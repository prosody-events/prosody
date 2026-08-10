use quickcheck_macros::quickcheck;
use thiserror::Error;

use super::{
    SCHEDULE_VISIBILITY_MICROS, SCHEDULED_PARTITION, prepare_work_cohorts, scenario_event_count,
};
use crate::edf::ArrivalPath;
use crate::types::WorkCohorts;
use crate::{
    ArrivalPrior, ArrivalPriorError, CapacityGrid, CapacityGridError, Configuration,
    ConfigurationError, ModelTime, ObservationBuffer, ReliabilityPrior,
    SCHEDULED_RELEASE_COUNT_MAX, ScaleDecision, ScaleScratch, ScaleState, ScheduledRelease,
    ServiceObjective, TransitionPrior, step,
};

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

#[test]
fn schedule_only_demand_can_activate_all_partitions() -> Result<(), TestError> {
    let (mut state, mut scratch, mut observation) = test_model()?;
    assert!(
        observation
            .set_scheduled_releases(&[ScheduledRelease {
                release_micros: 100_000_000,
                count: 1_000,
            }])
            .is_ok()
    );
    let decision = step(
        &mut state,
        &mut scratch,
        observation.observation(),
        ModelTime::from_micros(1),
    );
    match decision {
        ScaleDecision::Apply(apply) => {
            assert!(apply.target > 1, "schedule-only target={}", apply.target);
        }
        ScaleDecision::Hold(_) => return Err(TestError::UnexpectedHold),
    }
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
        release_micros: now_micros
            .saturating_add(SCHEDULE_VISIBILITY_MICROS)
            .saturating_sub(1),
        count: 5,
    });
    releases.push(ScheduledRelease {
        release_micros: now_micros
            .saturating_add(SCHEDULE_VISIBILITY_MICROS)
            .saturating_add(1),
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
        .filter(|release| {
            release.release_micros > now_micros
                && release.release_micros <= now_micros.saturating_add(SCHEDULE_VISIBILITY_MICROS)
        })
        .collect::<Vec<_>>();
    if scratch.resource_cohorts.len() != expected.len()
        || scratch.handler_cohorts.len() != expected.len()
    {
        return false;
    }
    let handler_seconds = state.capacity.expected_service_time(state.simd_level);
    expected.iter().enumerate().all(|(index, release)| {
        scratch.resource_cohorts.release_micros(index) == release.release_micros
            && scratch.handler_cohorts.release_micros(index) == release.release_micros
            && scratch.resource_cohorts.deadline_micros(index)
                == release
                    .release_micros
                    .saturating_add(state.configuration.objective.budget_micros())
            && scratch.handler_cohorts.deadline_micros(index)
                == scratch.resource_cohorts.deadline_micros(index)
            && approximately_equal(
                scratch.resource_cohorts.work_slot_seconds(index),
                f64::from(release.count),
            )
            && approximately_equal(
                scratch.handler_cohorts.work_slot_seconds(index),
                f64::from(release.count) * handler_seconds,
            )
            && scratch.resource_cohorts.partition(index) == SCHEDULED_PARTITION
            && scratch.handler_cohorts.partition(index) == SCHEDULED_PARTITION
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

fn approximately_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= f64::EPSILON * left.abs().max(right.abs()).max(1.0_f64)
}

fn test_model() -> Result<(ScaleState, ScaleScratch, ObservationBuffer), TestError> {
    let arrival_prior = ArrivalPrior::new(1.0_f64, 1.0e12_f64, 1.0e-12_f64, 64)?;
    let configuration = Configuration {
        cohort_count_max: 1,
        calendar_segment_count_max: 1,
        partition_count: 4,
        replica_count_max: 4,
        slots_per_replica: 32,
        posterior_sample_count: 64,
        report_interval_micros: 1_000_000,
        failure_service_weight: 0.3_f64,
        arrival_prior,
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
        objective: ServiceObjective::new(1_000_000, 0.01_f64)?,
    };
    let grid = CapacityGrid::new(&[0.1_f64], &[1_000.0_f64], &[0.0_f64])?;
    let state = ScaleState::new(configuration.clone(), grid)?;
    let scratch = ScaleScratch::new(&configuration)?;
    let observation = ObservationBuffer::new(&configuration)?;
    Ok((state, scratch, observation))
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    Arrival(#[from] ArrivalPriorError),
    #[error(transparent)]
    Capacity(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error("schedule-only demand returned a hold decision")]
    UnexpectedHold,
}
