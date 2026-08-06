use super::{
    ArrivalSeries, HISTORY_EVENT_COUNT_MAX, PrincipalDefinition, PrincipalRegime, RunSchedule,
    SharedResourcePolicy, StopCondition, run_principal_regime_seeded,
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
            reason: super::RunStopReason::DurationComplete
        }
    ));
}

#[test]
fn declining_capacity_experiment_has_a_fixed_duration() {
    let definition = PrincipalDefinition::capacity_evidence(PrincipalRegime::DecliningPostKnee);

    assert!(matches!(
        definition.schedule.stop,
        StopCondition::FixedDuration {
            reason: super::RunStopReason::DurationComplete
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
