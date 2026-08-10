use super::{
    ArrivalSchedule, ArrivalSeries, HISTORY_EVENT_COUNT_MAX, IndexSeries, PrincipalDefinition,
    PrincipalRegime, RunSchedule, RunStopReason, SharedResourcePolicy, StopCondition,
    run_principal_definition, run_principal_regime_seeded,
};
use crate::model::{AttemptFrame, AttemptModel};
use crate::{ConcurrencyLatencyCurve, PrincipalRunError, SeriesCell};

#[test]
fn shared_resource_collapse_controls_latency_curve() {
    assert_eq!(
        super::overloaded_operation_micros(200_000, 64, 128, 0),
        400_000
    );
    assert_eq!(
        super::overloaded_operation_micros(200_000, 64, 128, 2),
        1_200_000
    );
}

#[test]
fn capacity_grid_covers_the_declared_collapse() {
    let definition = PrincipalDefinition::for_regime(PrincipalRegime::DecliningPostKnee);
    let collapse = f64::from(definition.inputs.shared_resource.collapse);

    assert!(super::CAPACITY_COLLAPSE_GRID.contains(&collapse));
}

#[test]
fn shared_resource_load_uses_active_dependency_count() -> Result<(), PrincipalRunError> {
    let mut model = super::PrincipalAttemptModel::new(
        SharedResourcePolicy::new(2, 10, 2),
        ConcurrencyLatencyCurve::zero(),
        4,
    )?;
    let lightly_loaded = model.calculate(attempt_frame(100, 2));
    let handler_heavy = model.calculate(attempt_frame(1_000, 2));
    let dependency_heavy = model.calculate(attempt_frame(100, 4));

    assert_eq!(
        lightly_loaded.dependency_operation_micros,
        handler_heavy.dependency_operation_micros
    );
    assert!(
        dependency_heavy.dependency_operation_micros > lightly_loaded.dependency_operation_micros
    );
    Ok(())
}

const fn attempt_frame(active_handlers: u32, dependency_concurrency: u32) -> AttemptFrame {
    AttemptFrame {
        now_micros: 0,
        event_index: 0,
        attempt: 1,
        replicas: 4,
        active_handlers,
        dependency_concurrency,
        queued_events: 0,
    }
}

#[test]
fn periodic_arrivals_do_not_depend_on_evaluation_cadence() {
    let series = ArrivalSeries::Periodic {
        count: 100,
        interval_micros: 100_000,
        count_max: 2_000,
    };

    let fine = emitted_at(series, (0_u64..20).map(|step| step * 100_000));
    let coarse = emitted_at(series, [0_u64, 500_000, 1_000_000, 1_900_000]);

    assert_eq!(fine, 2_000);
    assert_eq!(coarse, fine);
}

#[test]
fn deterministic_rate_release_times_do_not_depend_on_controller_cadence()
-> Result<(), PrincipalRunError> {
    for series in [
        ArrivalSeries::Rate {
            per_second: 3,
            count_max: 1_000,
        },
        ArrivalSeries::StaircaseRate {
            initial_per_second: 3,
            increment_per_second: 5,
            step_interval_micros: 2_000_000,
            count_max: 1_000,
        },
    ] {
        let coarse = deterministic_release_times(series, 1_000_000, 4_000_000)?;
        let fine = deterministic_release_times(series, 250_000, 4_000_000)?;
        assert_eq!(coarse, fine);
    }
    Ok(())
}

fn deterministic_release_times(
    series: ArrivalSeries,
    interval_micros: u64,
    end_micros: u64,
) -> Result<Vec<u64>, PrincipalRunError> {
    release_times(series, interval_micros, end_micros, false)
}

fn release_times(
    series: ArrivalSeries,
    interval_micros: u64,
    end_micros: u64,
    stochastic: bool,
) -> Result<Vec<u64>, PrincipalRunError> {
    let mut schedule = ArrivalSchedule::new(series, 0, end_micros, 7, 11, stochastic)?;
    let mut releases = Vec::new();
    let mut interval_end = interval_micros;
    while interval_end <= end_micros {
        let count = schedule.release_until(interval_end)?;
        for event_offset in 0..count {
            releases.push(schedule.release_at(event_offset)?);
        }
        interval_end = interval_end.saturating_add(interval_micros);
    }
    Ok(releases)
}

#[test]
fn stochastic_rate_release_times_do_not_depend_on_controller_cadence()
-> Result<(), PrincipalRunError> {
    let series = ArrivalSeries::StaircaseRate {
        initial_per_second: 3,
        increment_per_second: 5,
        step_interval_micros: 2_000_000,
        count_max: 1_000,
    };
    let coarse = release_times(series, 1_000_000, 4_000_000, true)?;
    let fine = release_times(series, 250_000, 4_000_000, true)?;

    assert_eq!(coarse, fine);
    Ok(())
}

#[test]
fn staircase_rate_does_not_depend_on_evaluation_cadence() {
    let series = ArrivalSeries::StaircaseRate {
        initial_per_second: 2,
        increment_per_second: 3,
        step_interval_micros: 3_000_000,
        count_max: 100,
    };

    let fine = emitted_at(series, (0_u64..=8).map(|step| step * 1_000_000));
    let coarse = emitted_at(series, [0_u64, 2_000_000, 3_000_000, 8_000_000]);

    assert_eq!(fine, 37);
    assert_eq!(coarse, fine);
}

#[test]
fn controller_trace_bound_comes_from_virtual_ticks() -> Result<(), crate::PlantError> {
    assert_eq!(
        RunSchedule::extended_capacity_evidence().controller_sample_count_max()?,
        182
    );
    assert_eq!(
        RunSchedule::standard().controller_sample_count_max()?,
        3_002
    );
    Ok(())
}

#[test]
fn capacity_experiment_uses_controller_actuation_for_a_fixed_duration() {
    let definition = PrincipalDefinition::capacity_evidence(PrincipalRegime::LinearThroughput);

    assert!(matches!(definition.inputs.scale, super::ScaleSeries::None));
    assert!(matches!(
        definition.schedule.stop,
        StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete
        }
    ));
}

#[test]
fn declining_capacity_experiment_has_a_fixed_duration() {
    let definition = PrincipalDefinition::capacity_evidence(PrincipalRegime::DecliningPostKnee);

    assert!(matches!(
        definition.schedule.stop,
        StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete
        }
    ));
}

#[test]
fn historical_definitions_sustain_their_relationships() {
    let matches = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMatch);
    let exceeded = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalExceeded);
    let under = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalUnder);
    let missing = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMissing);
    let times = (0_u64..=300).map(|step| step * 100_000);

    assert_eq!(emitted_at(matches.inputs.messages, times.clone()), 30_000);
    assert_eq!(emitted_at(exceeded.inputs.messages, times.clone()), 60_000);
    assert_eq!(emitted_at(under.inputs.messages, times.clone()), 15_000);
    assert_eq!(
        emitted_at(matches.inputs.history.demand, times.clone()),
        30_000
    );
    assert_eq!(
        emitted_at(exceeded.inputs.history.demand, times.clone()),
        30_000
    );
    assert_eq!(
        emitted_at(under.inputs.history.demand, times.clone()),
        30_000
    );
    assert_eq!(emitted_at(missing.inputs.history.demand, times), 0);
    for definition in [matches, exceeded, under, missing] {
        assert_eq!(definition.schedule.workload_end_micros, 30_000_000);
        assert!(matches!(
            definition.schedule.stop,
            StopCondition::FixedDuration { .. }
        ));
        assert_eq!(definition.event_count_max, HISTORY_EVENT_COUNT_MAX);
    }
}

#[test]
fn historical_match_changes_the_prearrival_decision() -> Result<(), PrincipalRunError> {
    let one_tick = RunSchedule {
        start_micros: 0,
        workload_end_micros: 0,
        workload_interval_micros: 1,
        followup_interval_micros: 1,
        maximum_micros: 0,
        stop: StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete,
        },
    };
    let mut matched = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMatch);
    let mut missing = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMissing);
    matched.schedule = one_tick;
    missing.schedule = one_tick;
    let matched = run_principal_definition(PrincipalRegime::HistoricalMatch, matched, None)?;
    let missing = run_principal_definition(PrincipalRegime::HistoricalMissing, missing, None)?;
    let matched_target = matched
        .controller()
        .sample(0)
        .map_or(0, |sample| sample.target);
    let missing_target = missing
        .controller()
        .sample(0)
        .map_or(0, |sample| sample.target);

    assert!(
        matched_target > missing_target,
        "matched target={matched_target}, missing target={missing_target}"
    );
    Ok(())
}

#[test]
fn key_count_changes_one_partition_throughput() -> Result<(), PrincipalRunError> {
    let schedule = RunSchedule {
        start_micros: 0,
        workload_end_micros: 5_000_000,
        workload_interval_micros: 1_000_000,
        followup_interval_micros: 1_000_000,
        maximum_micros: 5_000_000,
        stop: StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete,
        },
    };
    let one_key = PrincipalDefinition::for_regime(PrincipalRegime::HotSerializedKey)
        .messages(ArrivalSeries::Rate {
            per_second: 200,
            count_max: 1_000,
        })
        .schedule(schedule)
        .event_count_max(1_000);
    let many_keys = one_key.keys(IndexSeries::Striped);

    let serialized = run_principal_definition(PrincipalRegime::HotSerializedKey, one_key, None)?;
    let parallel = run_principal_definition(PrincipalRegime::HotSerializedKey, many_keys, None)?;

    let parallel_final = parallel
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros);
    let serialized_final = serialized
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros);
    assert!(parallel_final < serialized_final);
    assert!(parallel.events().iter().all(|event| event.partition == 0));
    assert!(serialized.events().iter().all(|event| event.key == 0));
    Ok(())
}

#[test]
fn retry_outcomes_increase_loss_without_creating_physical_saturation()
-> Result<(), PrincipalRunError> {
    let schedule = RunSchedule {
        start_micros: 0,
        workload_end_micros: 10_000_000,
        workload_interval_micros: 1_000_000,
        followup_interval_micros: 1_000_000,
        maximum_micros: 10_000_000,
        stop: StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete,
        },
    };
    let failures = PrincipalDefinition::for_regime(PrincipalRegime::TransientFailures)
        .messages(ArrivalSeries::Rate {
            per_second: 300,
            count_max: 3_000,
        })
        .schedule(schedule)
        .event_count_max(3_000);
    let control = failures.transient_failures(super::FailureSeries::None);
    let failed = run_principal_definition(PrincipalRegime::TransientFailures, failures, None)?;
    let healthy = run_principal_definition(PrincipalRegime::TransientFailures, control, None)?;
    let failed_attempts = failed
        .settlements()
        .iter()
        .map(|settlement| settlement.attempts)
        .sum::<u32>();
    let healthy_attempts = healthy
        .settlements()
        .iter()
        .map(|settlement| settlement.attempts)
        .sum::<u32>();
    let failed_clear_micros = failed
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros);
    let healthy_clear_micros = healthy
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros);
    let failed_final = failed
        .controller()
        .len()
        .checked_sub(1)
        .and_then(|index| failed.controller().sample(index));
    // The two runs' evidence trajectories legitimately diverge (the
    // healthy run descends and resolves its saturation prior; the failed
    // run holds capacity for real retry delay). The invariant is
    // within-run: retry outcomes must never push the saturation belief
    // above its prior.
    let failed_initial = failed
        .controller()
        .sample(0)
        .map_or(f64::NAN, |sample| sample.saturation_probability);
    let saturation_created = failed_final.map_or(f64::INFINITY, |sample| {
        sample.saturation_probability - failed_initial
    });
    let reliability_increases_loss = (1..failed.controller().len().min(healthy.controller().len()))
        .any(|index| {
            failed
                .controller()
                .decision_expected_losses(index)
                .zip(healthy.controller().decision_expected_losses(index))
                .is_some_and(|(failed, healthy)| failed[0] > healthy[0] + 1.0e-9_f64)
        });

    assert!(failed_attempts > healthy_attempts);
    assert!(failed_clear_micros > healthy_clear_micros);
    assert!(
        saturation_created <= 0.01_f64,
        "retry outcomes raised saturation from {failed_initial} to {}",
        failed_final.map_or(f64::NAN, |sample| sample.saturation_probability)
    );
    assert!(reliability_increases_loss);
    Ok(())
}

#[test]
fn seeded_regimes_replay_and_separate_input_draws() -> Result<(), PrincipalRunError> {
    let first = run_principal_regime_seeded(PrincipalRegime::ApplicationLimited, 7)?;
    let replay = run_principal_regime_seeded(PrincipalRegime::ApplicationLimited, 7)?;
    let other = run_principal_regime_seeded(PrincipalRegime::ApplicationLimited, 8)?;

    let same_seed_replays = (0..first.inputs().len()).all(|row| {
        first.inputs().cell("message_count", row) == replay.inputs().cell("message_count", row)
    });
    let different_seed_changes_input = (0..first.inputs().len()).any(|row| {
        matches!(
            (
                first.inputs().cell("message_count", row),
                other.inputs().cell("message_count", row),
            ),
            (Some(SeriesCell::Unsigned32(first)), Some(SeriesCell::Unsigned32(other)))
                if first != other
        )
    });

    assert!(
        same_seed_replays,
        "equal seeds must replay every demand draw"
    );
    assert!(
        different_seed_changes_input,
        "different seeds must change at least one demand draw"
    );
    Ok(())
}

fn emitted_at<Times>(series: ArrivalSeries, times: Times) -> u32
where
    Times: IntoIterator<Item = u64>,
{
    times
        .into_iter()
        .fold(0_u32, |emitted, now| emitted + series.at(now, emitted))
}
