use super::{
    ArrivalSchedule, ArrivalSeries, CALENDAR_PRIOR_RATE_SECONDS, CALENDAR_PRIOR_SHAPE,
    HISTORICAL_SCHEDULE, HISTORY_EVENT_COUNT_MAX, HistoricalSeries, IndexSeries,
    PrincipalDefinition, PrincipalRegime, RunSchedule, RunStopReason, SEASONAL_SCHEDULE,
    SharedResourcePolicy, StopCondition, binomial_coverage_lower_tail, capacity_regime_axes,
    format_clock, is_capacity_regime, live_launch_count_max, principal_capacity_grid_axes,
    replica_count_max, resource_attempt_count_max, run_principal_definition,
    run_principal_regime_seeded,
};
use crate::model::{AttemptFrame, AttemptModel};
use crate::{
    ConcurrencyLatencyCurve, EventOutcome, EventSource, EventSpec, FinalOutcome, Plant,
    PlantConfiguration, PlantError, PrincipalRunError, SeriesCell,
};
use quickcheck_macros::quickcheck;
use statrs::distribution::{Binomial, BinomialError, DiscreteCDF};
use std::time::Duration;

#[test]
fn non_capacity_grid_contains_the_configured_handler_as_an_interior_point() {
    // Capacity regimes are exempt. Their axes model combined handler and
    // dependency service, not the bare configured handler duration.
    let excluded: Vec<_> = PrincipalRegime::ALL
        .into_iter()
        .filter(|regime| !is_capacity_regime(*regime))
        .filter(|regime| {
            let handler_seconds = Duration::from_micros(
                PrincipalDefinition::for_regime(*regime)
                    .inputs
                    .handler_micros,
            )
            .as_secs_f64();
            let (service_axis, _) = principal_capacity_grid_axes(*regime);
            !service_axis
                .get(1..service_axis.len().saturating_sub(1))
                .is_some_and(|values| {
                    values
                        .iter()
                        .any(|value| value.to_bits() == handler_seconds.to_bits())
                })
        })
        .map(PrincipalRegime::name)
        .collect();

    assert!(excluded.is_empty(), "{excluded:?}");
}

#[test]
fn derived_principal_grid_preserves_existing_axis_bits() {
    let (standard_service, standard_capacity) = principal_capacity_grid_axes(PrincipalRegime::Idle);
    let (historical_service, historical_capacity) =
        principal_capacity_grid_axes(PrincipalRegime::HistoricalMatch);

    assert_eq!(
        standard_service.map(f64::to_bits),
        [0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64].map(f64::to_bits)
    );
    assert_eq!(
        historical_service.map(f64::to_bits),
        [0.025_f64, 0.05_f64, 0.1_f64, 0.2_f64, 0.4_f64].map(f64::to_bits)
    );
    assert_eq!(
        standard_capacity,
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64]
    );
    assert_eq!(
        historical_capacity,
        &[64_000.0_f64, 128_000.0_f64, 256_000.0_f64]
    );
}

#[test]
fn capacity_regime_service_truth_is_inside_its_axis() {
    for regime in [
        PrincipalRegime::LinearThroughput,
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
    ] {
        let definition = PrincipalDefinition::capacity_evidence(regime);
        let truth = attempt_service_seconds(definition);
        let (service_axis, _) = capacity_regime_axes(regime, None);
        assert!(
            service_axis
                .first()
                .zip(service_axis.last())
                .is_some_and(|(low, high)| *low < truth && truth < *high)
        );
    }
}

#[test]
fn linear_true_service_knees_are_below_the_driven_concurrency() {
    let regime = PrincipalRegime::LinearThroughput;
    let definition = PrincipalDefinition::capacity_evidence(regime);
    let truth = attempt_service_seconds(definition);
    let maximum = maximum_evidence_concurrency(regime, definition);
    let (service_axis, capacity_axis) = capacity_regime_axes(regime, None);

    assert!(
        service_axis
            .iter()
            .any(|service| service.to_bits() == truth.to_bits())
    );
    assert!(
        capacity_axis
            .iter()
            .all(|capacity| capacity * truth < maximum)
    );
}

#[test]
fn collapse_regime_fleets_exceed_the_physical_knee() {
    for regime in [
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
    ] {
        let definition = PrincipalDefinition::capacity_evidence(regime);
        let fleet_slots = replica_count_max(regime, definition.experiment)
            * crate::DEFAULT_CONCURRENCY_PER_REPLICA;
        assert!(fleet_slots > definition.inputs.shared_resource.parallelism);
    }
}

fn attempt_service_seconds(definition: PrincipalDefinition) -> f64 {
    let resource = definition.inputs.shared_resource;
    let resource_micros = u64::from(resource.parallelism)
        .saturating_mul(1_000_000)
        .div_ceil(u64::from(resource.capacity_per_second));
    Duration::from_micros(resource_micros.saturating_add(definition.inputs.handler_micros))
        .as_secs_f64()
}

fn maximum_evidence_concurrency(regime: PrincipalRegime, definition: PrincipalDefinition) -> f64 {
    let ArrivalSeries::StaircaseRate {
        initial_per_second,
        increment_per_second,
        step_interval_micros,
        ..
    } = definition.inputs.messages
    else {
        return f64::NAN;
    };
    let final_micros = definition.schedule.workload_end_micros.saturating_sub(1);
    let steps = u32::try_from(final_micros / step_interval_micros).unwrap_or(u32::MAX);
    let rate = initial_per_second.saturating_add(increment_per_second.saturating_mul(steps));
    let demand_concurrency = f64::from(rate) * attempt_service_seconds(definition);
    let fleet_slots =
        replica_count_max(regime, definition.experiment) * crate::DEFAULT_CONCURRENCY_PER_REPLICA;
    demand_concurrency.min(f64::from(fleet_slots))
}

#[test]
fn progress_clock_formats_boundaries() {
    assert_eq!(format_clock(0.0_f64), "00:00:00");
    assert_eq!(format_clock(59.0_f64), "00:00:59");
    assert_eq!(format_clock(3_661.0_f64), "01:01:01");
    assert_eq!(format_clock(90_061.0_f64), "1d 01:01:01");
}

#[quickcheck]
fn attempt_contract_is_the_smaller_authored_supply_bound(
    event_code: u16,
    replica_code: u8,
    slot_code: u8,
    window_code: u8,
    service_code: u8,
    retries: bool,
) -> bool {
    let events = u32::from(event_code) + 1;
    let replicas = u32::from(replica_code) % 32 + 1;
    let slots = u32::from(slot_code) % 64 + 1;
    let window_micros = (u64::from(window_code) + 1) * 10_000;
    let service_micros = (u64::from(service_code) + 1) * 1_000;
    let inflation = if retries {
        u32::from(crate::MAX_RETRY_FAILURES) + 1
    } else {
        1
    };
    let expected = events.saturating_mul(inflation).min(
        replicas
            .saturating_mul(slots)
            .saturating_mul((window_micros.div_ceil(service_micros) + 1) as u32)
            .saturating_mul(inflation),
    );
    resource_attempt_count_max(
        events,
        replicas,
        slots,
        window_micros,
        service_micros,
        retries,
    )
    .is_ok_and(|actual| actual == expected)
}

#[quickcheck]
fn calendar_forecast_round_trips_the_historical_schedule(seasonal: bool) -> bool {
    let history = if seasonal {
        HistoricalSeries::seasonal()
    } else {
        HistoricalSeries::standard()
    };
    let schedule = if seasonal {
        SEASONAL_SCHEDULE
    } else {
        HISTORICAL_SCHEDULE
    };
    let Ok(Some(forecast)) = history.forecast() else {
        return false;
    };
    let segments = forecast.segments();
    if segments.len() != schedule.len()
        || segments.first().map(|segment| segment.start_micros())
            != schedule.first().map(|segment| segment.start_micros)
        || segments.last().map(|segment| segment.end_micros())
            != schedule.last().map(|segment| segment.end_micros)
    {
        return false;
    }
    segments
        .iter()
        .zip(schedule)
        .enumerate()
        .all(|(position, (actual, expected))| {
            let posterior_mean = actual.shape() / actual.rate_seconds();
            let tolerance = (CALENDAR_PRIOR_SHAPE
                - f64::from(expected.rate_per_second) * CALENDAR_PRIOR_RATE_SECONDS)
                .abs()
                / actual.rate_seconds()
                + f64::EPSILON;
            actual.position() == position as u32
                && actual.start_micros() == expected.start_micros
                && actual.end_micros() == expected.end_micros
                && (posterior_mean - f64::from(expected.rate_per_second)).abs() <= tolerance
        })
        && segments
            .windows(2)
            .all(|pair| pair[0].end_micros() == pair[1].start_micros())
}

#[quickcheck]
fn retarget_churn_keeps_readiness_cursors_within_live_launches(seed: u64) -> bool {
    let regime = PrincipalRegime::Idle;
    let mut definition = PrincipalDefinition::for_regime(regime);
    definition.inputs.seed = seed;
    let replica_count_max = replica_count_max(regime, definition.experiment);
    let live_launch_count_max = live_launch_count_max(replica_count_max);
    let Ok(mut controller) = super::principal_graph(
        regime,
        false,
        definition,
        super::DEFAULT_CONCURRENCY_PER_REPLICA,
        None,
    ) else {
        return false;
    };
    let mut now_micros = 1_u64;
    for index in 0..=live_launch_count_max {
        let random_code = seed.rotate_left(index).wrapping_add(u64::from(index));
        let Ok(target_offset) = u32::try_from(random_code % u64::from(replica_count_max - 1))
        else {
            return false;
        };
        let target = 2 + target_offset;
        if controller
            .retarget_for_test(now_micros, 1, target, target - 1)
            .is_err()
        {
            return false;
        }
        now_micros = now_micros.saturating_add(1);
        let Ok(open_count) = controller.retarget_for_test(now_micros, 1, 1, 0) else {
            return false;
        };
        if open_count > live_launch_count_max as usize {
            return false;
        }
        now_micros = now_micros.saturating_add(1);
    }
    true
}

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
fn authored_regimes_meet_capacity_model_contracts() {
    for regime in PrincipalRegime::ALL {
        if matches!(
            regime,
            PrincipalRegime::ReplicaCeiling | PrincipalRegime::SnapshotFaults
        ) {
            continue;
        }
        let definition = PrincipalDefinition::for_regime(regime);
        let result = super::principal_graph(
            regime,
            is_capacity_regime(regime),
            definition,
            super::DEFAULT_CONCURRENCY_PER_REPLICA,
            None,
        );
        assert!(
            result.is_ok(),
            "{}: {:?}",
            regime.name(),
            result.as_ref().err()
        );
    }
    for sensitivity in super::CapacitySensitivity::ALL {
        let regime = PrincipalRegime::FlatPostKnee;
        let result = super::principal_graph(
            regime,
            true,
            PrincipalDefinition::capacity_evidence(regime),
            super::DEFAULT_CONCURRENCY_PER_REPLICA,
            Some(sensitivity),
        );
        assert!(
            result.is_ok(),
            "{}: {:?}",
            sensitivity.name(),
            result.as_ref().err()
        );
    }
}

#[test]
fn shared_resource_load_uses_active_dependency_count() -> Result<(), PrincipalRunError> {
    let mut model = super::PrincipalAttemptModel::new(
        SharedResourcePolicy::new(2, 10, 2),
        ConcurrencyLatencyCurve::zero(),
        4,
        0,
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

#[test]
fn rate_schedule_uses_the_declared_workload_window() -> Result<(), PrincipalRunError> {
    let mut schedule = ArrivalSchedule::new(
        ArrivalSeries::Rate {
            per_second: 1_000,
            count_max: HISTORY_EVENT_COUNT_MAX,
        },
        300_000_000,
        420_000_000,
        7,
        11,
        false,
    )?;
    assert_eq!(schedule.release_until(300_000_000)?, 0);
    assert_eq!(schedule.release_until(420_000_000)?, 120_000);
    assert_eq!(schedule.release_at(0)?, 300_001_000);
    assert_eq!(schedule.release_at(119_999)?, 420_000_000);
    Ok(())
}

#[test]
fn pending_timer_releases_are_grouped_and_removed_after_release() -> Result<(), PrincipalRunError> {
    let mut schedule = ArrivalSchedule::new(
        ArrivalSeries::PeriodicDelayed {
            count: 3,
            first_micros: 100,
            interval_micros: 100,
            count_max: 6,
        },
        0,
        200,
        7,
        11,
        false,
    )?;
    assert_eq!(
        schedule.pending_releases()?.releases(),
        [
            prosody_scale_core::ScheduledRelease {
                release_micros: 100,
                count: 3,
            },
            prosody_scale_core::ScheduledRelease {
                release_micros: 200,
                count: 3,
            },
        ]
    );
    assert_eq!(schedule.release_until(100)?, 3);
    assert_eq!(
        schedule.pending_releases()?.releases(),
        [prosody_scale_core::ScheduledRelease {
            release_micros: 200,
            count: 3,
        }]
    );
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
fn controller_trace_bound_comes_from_virtual_ticks() -> Result<(), PlantError> {
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
fn principal_graph_rejects_a_workload_start_outside_the_run() {
    let definition = PrincipalDefinition::for_regime(PrincipalRegime::ApplicationLimited);
    let mut before_run = definition;
    before_run.schedule.start_micros = 1;
    before_run.schedule.workload_start_micros = 0;
    let mut after_workload = definition;
    after_workload.schedule.workload_start_micros = after_workload
        .schedule
        .workload_end_micros
        .saturating_add(1);

    assert!(matches!(
        super::PrincipalGraph::new(before_run),
        Err(PlantError::WorkloadWindow)
    ));
    assert!(matches!(
        super::PrincipalGraph::new(after_workload),
        Err(PlantError::WorkloadWindow)
    ));
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
fn historical_definitions_sustain_their_relationships() -> Result<(), PrincipalRunError> {
    let matches = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMatch);
    let exceeded = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalExceeded);
    let under = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalUnder);
    let missing = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMissing);

    assert_eq!(definition_message_count(matches)?, 120_000);
    assert_eq!(definition_message_count(exceeded)?, 240_000);
    assert_eq!(definition_message_count(under)?, 60_000);
    assert_eq!(historical_message_count(matches.inputs.history)?, 120_000);
    assert_eq!(historical_message_count(exceeded.inputs.history)?, 120_000);
    assert_eq!(historical_message_count(under.inputs.history)?, 120_000);
    assert_eq!(historical_message_count(missing.inputs.history)?, 0);
    for definition in [matches, exceeded, under, missing] {
        assert_eq!(definition.schedule.start_micros, 0);
        assert_eq!(definition.schedule.workload_start_micros, 300_000_000);
        assert_eq!(definition.schedule.workload_end_micros, 420_000_000);
        assert_eq!(definition.schedule.maximum_micros, 480_000_000);
        assert!(matches!(
            definition.schedule.stop,
            StopCondition::FixedDuration { .. }
        ));
        assert_eq!(definition.event_count_max, HISTORY_EVENT_COUNT_MAX);
    }
    Ok(())
}

fn definition_message_count(definition: PrincipalDefinition) -> Result<u32, PrincipalRunError> {
    let mut schedule = ArrivalSchedule::new(
        definition.inputs.messages,
        definition.schedule.workload_start_micros,
        definition.schedule.workload_end_micros,
        0,
        0,
        false,
    )?;
    Ok(schedule.release_until(definition.schedule.workload_end_micros)?)
}

fn historical_message_count(history: HistoricalSeries) -> Result<u32, PrincipalRunError> {
    let mut schedule = ArrivalSchedule::from_segments(history.segments)?;
    Ok(schedule.release_until(u64::MAX)?)
}

#[test]
fn matching_history_changes_prearrival_decision_and_requests_step_capacity()
-> Result<(), PrincipalRunError> {
    let one_tick = RunSchedule {
        start_micros: 0,
        workload_start_micros: super::HISTORY_START_MICROS,
        workload_end_micros: super::HISTORY_END_MICROS,
        workload_interval_micros: 1,
        followup_interval_micros: 1,
        maximum_micros: 0,
        stop: StopCondition::FixedDuration {
            reason: RunStopReason::DurationComplete,
        },
    };
    let mut matched = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMatch);
    let mut missing = PrincipalDefinition::for_regime(PrincipalRegime::HistoricalMissing);
    let initial_replicas = matched.initial_replicas;
    let handler_micros = matched.inputs.handler_micros;
    let step_rate = matched.inputs.history.segments[1].rate_per_second;
    matched.schedule = one_tick;
    missing.schedule = one_tick;
    let matched = run_principal_definition(PrincipalRegime::HistoricalMatch, matched, None)?;
    let missing = run_principal_definition(PrincipalRegime::HistoricalMissing, missing, None)?;
    let matched_sample = matched.controller().sample(0);
    let matched_target = matched_sample.map_or(0, |sample| sample.target);
    let missing_target = missing
        .controller()
        .sample(0)
        .map_or(0, |sample| sample.target);
    let target_handler_micros_per_second =
        u64::from(matched_target) * u64::from(super::DEFAULT_CONCURRENCY_PER_REPLICA) * 1_000_000;
    let step_handler_micros_per_second = u64::from(step_rate) * handler_micros;

    assert_ne!(
        matched_target, missing_target,
        "matched target={matched_target}, missing target={missing_target}"
    );
    assert!(
        matched_sample.is_some_and(|sample| sample.at_micros < super::HISTORY_START_MICROS)
            && matched_target > initial_replicas
            && target_handler_micros_per_second >= step_handler_micros_per_second,
        "target {matched_target} did not prepare for the {step_rate}/s historical step"
    );
    Ok(())
}

#[test]
fn key_count_changes_one_partition_throughput() -> Result<(), PrincipalRunError> {
    let schedule = RunSchedule {
        start_micros: 0,
        workload_start_micros: 0,
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
        workload_start_micros: 0,
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
                .decision_expected_costs(index)
                .zip(healthy.controller().decision_expected_costs(index))
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

#[test]
fn fixed_seed_regime_replays_identical_summary() -> Result<(), PrincipalRunError> {
    let definition = PrincipalDefinition::standard()
        .messages(ArrivalSeries::Once(8))
        .event_count_max(8)
        .seeded(31);
    let run = |definition: PrincipalDefinition| -> Result<_, PrincipalRunError> {
        let configuration =
            PlantConfiguration::new(1, 8, 8, 1, 8, 8)?.with_service_seed(definition.inputs.seed);
        let mut plant = Plant::new(configuration, 1)?;
        for event_index in 0..8 {
            plant.add_event(EventSpec {
                release_micros: 0,
                partition: 0,
                key: event_index,
                handler_micros: definition.inputs.handler_micros,
                dependency_operations: 1,
                outcome: EventOutcome::Final(FinalOutcome::Success),
                source: EventSource::Message,
            })?;
        }
        Ok(plant.run())
    };
    let first = run(definition)?;
    let replay = run(definition)?;

    assert_eq!(first.events(), replay.events());
    assert_eq!(first.settlements(), replay.settlements());
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

#[test]
fn coverage_lower_tail_matches_the_binomial_oracle() -> Result<(), BinomialError> {
    // 1000 windows exceeds the linear-domain underflow point near 440.
    for (windows, covered) in [
        (10, 4),
        (10, 5),
        (100, 50),
        (180, 76),
        (400, 300),
        (1000, 760),
    ] {
        let oracle = Binomial::new(0.8_f64, windows)?.cdf(covered);
        let actual = binomial_coverage_lower_tail(windows, covered);
        let tolerance = 1e-9_f64 * oracle + 1e-300_f64;
        assert!(
            (actual - oracle).abs() <= tolerance,
            "windows {windows} covered {covered}: actual {actual} oracle {oracle}"
        );
    }
    Ok(())
}
