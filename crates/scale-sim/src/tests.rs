#[cfg(simulation_profile)]
use std::cmp::Ordering;
use std::iter::repeat;

use prosody_scale_core::{
    ArrivalPrior, ArrivalPriorError, CapacityGrid, Configuration as ControllerConfiguration,
    LaunchPrior, PosteriorQuery, RandomStream, RebalancePrior, ReliabilityPrior, ServiceObjective,
    sticky_assignment as model_assignment,
};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
#[cfg(simulation_profile)]
use rayon::prelude::*;
use thiserror::Error;

#[cfg(simulation_profile)]
use crate::RunStopReason;
#[cfg(simulation_profile)]
use crate::regime::linear_miss_accounting;
#[cfg(simulation_profile)]
use crate::run_capacity_evidence_regime;
use crate::series::{
    OutputFunction, SeriesCell, SeriesContext, SeriesFunction, SeriesHistory, SeriesKey,
    SeriesMetadata, SeriesRole, SeriesUnit, series_graph, series_graph_is_acyclic,
};
use crate::{
    ArrivalEvidenceSample, AttemptContext, AttemptFrame, AttemptGenerator, AttemptModel,
    AttemptParameters, ClosedLoop, ClosedLoopError, ConcurrencyLatencyCurve, ControllerSample,
    ControllerTrace, EventContext, EventInputs, EventOutcome, EventOutcomeRule, EventSource,
    EventSpec, FaultPattern, FinalOutcome, HistoricalAttemptModel, Kip848Rebalance, Plant,
    PlantConfiguration, PlantError, PrincipalRegime, PrincipalRunError, PriorArtifactKind,
    QuantileTable, RegimeExperiment, RegimeValidationError, ReliabilityEvidenceSample,
    ReporterDirective, RetryCount, RetryOutcome, ScaleChange, ScaleDirective, ScaleRequest,
    SimulationHarness, Snapshot, SnapshotChannel, SnapshotCursor, SnapshotTable, StepSeries,
    TickContext, TickGenerator, TickInputs, W6AblationArm, WorkloadSeries, run_batch_regime,
    run_batch_slo, run_parallel, run_principal_regime, validate_principal_regime,
};
use crate::{CapacityEvidenceKind, CapacityEvidenceSample};

/// Returns the authored test prior: one arrival per second and one expected
/// change per day. These tests exercise the controller, not this prior.
fn test_arrival_prior() -> Result<ArrivalPrior, ArrivalPriorError> {
    ArrivalPrior::new(1.0_f64 / 86_400.0_f64)
}

#[test]
fn generated_outcome_rules_reject_zero_sentinels() {
    assert!(EventOutcomeRule::permanent_every(0).is_none());
    assert!(EventOutcomeRule::transient_then_success(0).is_err());
}

#[test]
fn graph_validation_accepts_unordered_dags_and_rejects_cycles() {
    let names = &["latency", "demand", "handler"];
    assert!(series_graph_is_acyclic(
        names,
        &[("latency", "handler"), ("handler", "demand")],
    ));
    assert!(!series_graph_is_acyclic(
        names,
        &[("latency", "handler"), ("handler", "latency")],
    ));
    assert!(!series_graph_is_acyclic(names, &[("latency", "missing")],));
}

#[test]
fn series_history_preserves_large_integer_values_exactly() -> Result<(), TestError> {
    const LARGE_VALUE: u64 = 9_007_199_254_740_993;
    const METADATA: &[SeriesMetadata] = &[SeriesMetadata {
        name: "duration",
        label: "duration",
        unit: SeriesUnit::Microseconds,
        role: SeriesRole::Input,
    }];
    let mut history = SeriesHistory::new(METADATA, 2)?;

    history.push_indexed(7, &[(0, SeriesCell::Unsigned64(LARGE_VALUE))]);

    assert_eq!(history.view().get(SeriesKey::new(0), 0), Some(LARGE_VALUE));
    assert_eq!(history.metadata(), METADATA);
    Ok(())
}

#[test]
fn replay_is_deterministic() -> Result<(), TestError> {
    assert_eq!(scenario()?.run(), scenario()?.run());
    Ok(())
}

#[test]
fn attempt_functions_receive_time_and_bounded_history() -> Result<(), TestError> {
    let mut model = HistoricalAttemptModel::new(HistoryGenerator, 2)?;
    let first = model.calculate(attempt_frame(10, 2, 3));
    let second = model.calculate(attempt_frame(20, 4, 5));
    let third = model.calculate(attempt_frame(30, 8, 7));
    let fourth = model.calculate(attempt_frame(40, 16, 11));

    assert_eq!(first.dependency_operation_micros, 10);
    assert_eq!(second.dependency_operation_micros, 23);
    assert_eq!(third.dependency_operation_micros, 38);
    assert_eq!(fourth.dependency_operation_micros, 52);
    assert_eq!(fourth.handler_added_micros, 28);
    Ok(())
}

struct HistoryGenerator;

impl AttemptGenerator for HistoryGenerator {
    fn calculate(&self, context: AttemptContext<'_>) -> AttemptParameters {
        let queue_latest = context.history.queued_events(0).unwrap_or(0);
        let queue_previous = context.history.queued_events(1).unwrap_or(0);
        let replicas_latest = context.history.replicas(0).unwrap_or(0);
        let replicas_previous = context.history.replicas(1).unwrap_or(0);
        AttemptParameters {
            dependency_operation_micros: context
                .frame
                .now_micros
                .saturating_add(u64::from(queue_latest))
                .saturating_add(u64::from(queue_previous)),
            handler_added_micros: u64::from(
                context
                    .frame
                    .replicas
                    .saturating_add(replicas_latest)
                    .saturating_add(replicas_previous),
            ),
        }
    }
}

const fn attempt_frame(now_micros: u64, replicas: u32, queued_events: u32) -> AttemptFrame {
    AttemptFrame {
        now_micros,
        event_index: 0,
        attempt: 1,
        replicas,
        active_handlers: 1,
        dependency_concurrency: 0,
        queued_events,
    }
}

#[test]
fn incremental_virtual_time_matches_one_shot_replay() -> Result<(), TestError> {
    let expected = scenario()?.run();
    let mut incremental = scenario()?;
    let snapshot = incremental.advance_until(1_500);
    assert_eq!(snapshot.released, 2);
    assert_eq!(snapshot.settled, 0);
    let mut partition_backlog = [0_u32; 4];
    let mut normal_release = [0_u64; 4];
    let mut failure_backlog = [0_u32; 4];
    let mut failure_release = [0_u64; 4];
    incremental.write_partition_backlogs(
        1_500,
        &mut partition_backlog,
        &mut normal_release,
        &mut failure_backlog,
        &mut failure_release,
    )?;
    assert_eq!(partition_backlog.iter().sum::<u32>(), snapshot.backlog);
    assert_eq!(incremental.run(), expected);
    Ok(())
}

#[test]
fn time_driven_demand_can_add_events_at_current_time() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 2)?;
    let snapshot = plant.advance_until(100);
    assert_eq!(snapshot.released, 0);

    plant.add_event(event(100, 0, 5_000))?;
    assert!(matches!(
        plant.add_event(event(99, 1, 5_000)),
        Err(PlantError::EventTimeRegressed)
    ));
    let result = plant.run();

    assert_eq!(result.settlements().len(), 1);
    assert_eq!(result.settlements()[0].release_micros, 100);
    Ok(())
}

#[test]
fn dependency_concurrency_includes_all_active_operations() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(3, 3, 3, 1, 3, 1)?;
    let mut plant = Plant::with_attempt_model(configuration, 1, ConcurrencyDuration)?;
    for key in 0..3 {
        plant.add_event(event(0, key, 0))?;
    }

    let result = plant.run();
    let durations = result
        .settlements()
        .iter()
        .map(|settlement| settlement.dependency_micros)
        .collect::<Vec<_>>();

    assert_eq!(durations, [1, 2, 3]);
    Ok(())
}

#[test]
fn handler_metric_includes_dependency_wall_time() -> Result<(), TestError> {
    let configuration =
        PlantConfiguration::new(1, 1, 1, 1, 1, 1)?.with_dependency_operation_micros(100);
    let mut plant = Plant::new(configuration, 1)?;
    plant.add_event(event(0, 0, 10))?;

    let trace = plant.run().metric_trace(1_000, 1_000)?;
    let handler_micros = super::exponential_duration_micros(0, 0, 1, super::HANDLER_COMPONENT, 10);

    assert_eq!(
        trace.handler_elapsed_p99_micros[0],
        100_u64.saturating_add(handler_micros)
    );
    Ok(())
}

#[test]
fn kip848_pauses_only_partitions_that_move() -> Result<(), TestError> {
    let rebalance = Kip848Rebalance::new(
        QuantileTable::new(&[10])?,
        QuantileTable::new(&[20])?,
        QuantileTable::new(&[30])?,
        QuantileTable::new(&[40])?,
        7,
    );
    let configuration = PlantConfiguration::new(6, 6, 2, 1, 1, 1)?
        .with_dependency_operation_micros(0)
        .with_kip848_rebalance(rebalance);
    let mut plant = Plant::new(configuration, 2)?;
    plant.add_scale_change(ScaleChange {
        at_micros: 0,
        replicas: 3,
    })?;
    plant.add_event(event(11, 0, 1))?;
    plant.add_event(event(11, 4, 1))?;

    let pending = plant.advance_until(5);
    let paused = plant.advance_until(10);
    let ready = plant.advance_until(100);
    let result = plant.run();

    assert!(!pending.partitions_ready);
    assert_eq!(pending.reconciling_partitions, 2);
    assert_eq!(pending.paused_partitions, 0);
    assert_eq!(pending.rebalance_pause_micros, 0);
    assert_eq!(paused.paused_partitions, 2);
    assert_eq!(paused.rebalance_pause_micros, 0);
    assert_eq!(paused.reconciliation_started_micros, Some(10));
    assert_eq!(ready.reconciliation_completed_micros, Some(100));
    assert_eq!(ready.rebalance_pause_micros, 90);
    let completed = ready
        .reconciliation_completed_micros
        .ok_or(TestError::MissingReconciliationCompletion)?;
    let unmoved = result
        .settlements()
        .iter()
        .find(|settlement| settlement.event == 0)
        .ok_or(TestError::MissingSettlement)?;
    let moved = result
        .settlements()
        .iter()
        .find(|settlement| settlement.event == 1)
        .ok_or(TestError::MissingSettlement)?;
    assert!(unmoved.settle_micros < completed);
    assert!(moved.settle_micros >= completed);
    Ok(())
}

#[test]
fn kip848_waits_for_a_revoked_partition_to_drain() -> Result<(), TestError> {
    let rebalance = Kip848Rebalance::new(
        QuantileTable::new(&[0])?,
        QuantileTable::new(&[10])?,
        QuantileTable::new(&[0])?,
        QuantileTable::new(&[0])?,
        11,
    );
    let configuration = PlantConfiguration::new(6, 6, 2, 1, 1, 1)?
        .with_dependency_operation_micros(0)
        .with_kip848_rebalance(rebalance);
    let mut plant = Plant::new(configuration, 2)?;
    plant.add_event(event(0, 4, 200))?;
    plant.add_event(event(11, 4, 1))?;
    plant.add_scale_change(ScaleChange {
        at_micros: 10,
        replicas: 3,
    })?;

    let drained = plant.advance_until(1_000);
    let result = plant.run();

    let completed = drained
        .reconciliation_completed_micros
        .ok_or(TestError::MissingReconciliationCompletion)?;
    let first = result
        .settlements()
        .iter()
        .find(|settlement| settlement.event == 0)
        .ok_or(TestError::MissingSettlement)?;
    let queued = result
        .settlements()
        .iter()
        .find(|settlement| settlement.event == 1)
        .ok_or(TestError::MissingSettlement)?;
    let queued_dispatch = queued
        .release_micros
        .saturating_add(queued.permit_wait_micros);
    assert!(completed >= first.settle_micros);
    assert!(queued_dispatch >= completed);
    Ok(())
}

#[test]
fn moved_partitions_use_the_new_replica_slots() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(4, 4, 4, 1, 1, 1)?
        .with_dependency_operation_micros(0)
        .with_rebalance(0, 0);
    let mut before = Plant::new(configuration.clone(), 2)?;
    let mut after = Plant::new(configuration, 2)?;
    after.add_scale_change(ScaleChange {
        at_micros: 0,
        replicas: 4,
    })?;
    for partition in 0..4 {
        before.add_event(event(1, partition, 100))?;
        after.add_event(event(1, partition, 100))?;
    }

    let before = before.run();
    let after = after.run();

    let before_dispatches = before
        .settlements()
        .iter()
        .map(|settlement| {
            settlement
                .release_micros
                .saturating_add(settlement.permit_wait_micros)
        })
        .collect::<Vec<_>>();
    let after_dispatches = after
        .settlements()
        .iter()
        .map(|settlement| {
            settlement
                .release_micros
                .saturating_add(settlement.permit_wait_micros)
        })
        .collect::<Vec<_>>();
    assert!(maximum_equal_values(&before_dispatches) <= 2);
    assert_eq!(maximum_equal_values(&after_dispatches), 4);
    assert!(after_dispatches.iter().all(|dispatch| *dispatch == 1));
    assert_eq!(
        after
            .settlements()
            .iter()
            .map(|settlement| settlement.in_flight_at_dispatch)
            .max(),
        Some(4)
    );
    Ok(())
}

struct ConcurrencyDuration;

impl AttemptModel for ConcurrencyDuration {
    fn calculate(&mut self, frame: AttemptFrame) -> AttemptParameters {
        AttemptParameters {
            dependency_operation_micros: u64::from(frame.dependency_concurrency),
            handler_added_micros: 0,
        }
    }
}

#[test]
fn one_harness_calculates_time_history_and_function_dependencies() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(4, 16, 32, 4, 4, 2)?;
    let graph = RegimeGraph::new(2, 1_000, 2, 2)?;
    let mut harness = SimulationHarness::new(configuration, 2, 2, graph)?;

    let first = harness.tick(0)?;
    let second = harness.tick(10_000)?;
    let third = harness.tick(20_000)?;
    let fourth = harness.tick(40_000)?;
    let result = harness.finish();

    assert_eq!(first.released, 2);
    assert_eq!(second.released, 5);
    assert_eq!(third.released, 9);
    assert_eq!(fourth.replicas, 4);
    assert_eq!(result.settlements().len(), 14);
    assert_eq!(result.events()[5].handler_micros, 1_044);
    Ok(())
}

#[test]
fn closed_loop_emits_passive_resource_windows() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(CapacityWorkload, 8)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?
        .with_rebalance(0, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 8, closed_loop)?;

    let mut replicas = Vec::with_capacity(8);
    for tick in 0_u64..8 {
        replicas.push(harness.tick(tick * 10_000)?.replicas);
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let kinds = (0..closed_loop.trace().len())
        .filter_map(|index| closed_loop.trace().sample(index))
        .map(|sample| sample.capacity_evidence.kind())
        .collect::<Vec<_>>();

    assert!(kinds.contains(&CapacityEvidenceKind::Window));
    let changed_tick = replicas
        .windows(2)
        .position(|pair| pair[0] != pair[1])
        .map(|index| index + 1)
        .ok_or(TestError::MissingScaleChange)?;
    let changed_sample = closed_loop
        .trace()
        .sample(changed_tick)
        .ok_or(TestError::MissingControllerSample)?;
    assert!(matches!(
        changed_sample.capacity_evidence,
        CapacityEvidenceSample::Window(_)
    ));
    Ok(())
}

#[test]
fn draining_scale_down_reports_physical_slots() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(DrainingScaleDownWorkload, 4)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 16, 4, 2, 8)?
        .with_rebalance(0, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 2, 4, closed_loop)?;
    let mut physical_slots = Vec::with_capacity(4);

    for tick in 0_u64..4 {
        let snapshot = harness.tick(tick * 10_000)?;
        assert!(snapshot.active_handlers <= snapshot.physical_slots);
        physical_slots.push(snapshot.physical_slots);
    }
    let (_result, closed_loop) = harness.finish_with_graph();

    assert_eq!(
        closed_loop
            .trace()
            .capacity_evidence_count(CapacityEvidenceKind::Window),
        3
    );
    assert_eq!(physical_slots, [4, 4, 4, 2]);
    Ok(())
}

#[test]
fn irregular_tick_omits_paired_capacity_and_reliability_evidence() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(CapacityWorkload, 4)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?
        .with_rebalance(0, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 4, closed_loop)?;
    for at_micros in [0_u64, 10_000, 25_000, 35_000] {
        harness.tick(at_micros)?;
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let sample = closed_loop
        .trace()
        .sample(2)
        .ok_or(TestError::MissingControllerSample)?;

    assert!(matches!(
        sample.capacity_evidence,
        CapacityEvidenceSample::None
    ));
    assert!(matches!(
        sample.reliability_evidence,
        ReliabilityEvidenceSample::None
    ));
    Ok(())
}

#[test]
fn controller_trace_exposes_report_evidence_contract() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(CapacityWorkload, 8)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?
        .with_rebalance(0, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 8, closed_loop)?;
    for tick in 0_u64..8 {
        harness.tick(tick * 10_000)?;
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let trace = closed_loop.trace();

    assert_trace_catalog(trace);
    let (arrival_windows, capacity_windows, reliability_observations) =
        assert_trace_samples(trace)?;
    assert!(arrival_windows > 0);
    assert!(capacity_windows > 0);
    assert!(reliability_observations > 0);
    assert_reported_arrival_intervals()?;
    Ok(())
}

fn assert_trace_catalog(trace: &ControllerTrace) {
    assert_eq!(trace.artifacts().len(), 5);
    for artifact in trace.artifacts() {
        assert_eq!(artifact.schema_version(), 1);
        assert!(!artifact.coverage().is_empty());
        assert!(artifact.coverage().iter().all(|coverage| {
            coverage.lower_tail_probability() >= 0.0_f64
                && coverage.upper_tail_probability() >= 0.0_f64
        }));
    }
    assert!(trace.artifact(PriorArtifactKind::Capacity).is_some());
    assert_eq!(trace.w6_ablation_witnesses().len(), 5);
    assert_eq!(
        trace.w6_ablation_witnesses()[3].arm,
        W6AblationArm::ProperJoint
    );
    assert_eq!(
        trace.w6_ablation_witnesses()[3].joint_log_score.to_bits(),
        trace.w6_ablation_witnesses()[4].joint_log_score.to_bits()
    );
    assert!(
        trace
            .posterior_values(PosteriorQuery::CapacityContaminationProbability)
            .is_some()
    );
}

fn assert_trace_samples(trace: &ControllerTrace) -> Result<(u32, u32, u32), TestError> {
    let mut capacity_windows = 0_u32;
    let mut arrival_windows = 0_u32;
    let mut reliability_observations = 0_u32;
    for index in 0..trace.len() {
        let sample = trace
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
        assert_sample_fields(&sample);
        if let ArrivalEvidenceSample::Accepted(window) = sample.arrival_evidence {
            arrival_windows = arrival_windows.saturating_add(1);
            assert_arrival_interval(sample.at_micros, window);
        }
        if let ReliabilityEvidenceSample::Accepted { normal, failure } = sample.reliability_evidence
        {
            reliability_observations = reliability_observations
                .saturating_add(normal.success)
                .saturating_add(normal.permanent)
                .saturating_add(normal.transient)
                .saturating_add(normal.terminal)
                .saturating_add(failure.success)
                .saturating_add(failure.permanent)
                .saturating_add(failure.transient)
                .saturating_add(failure.terminal);
        }
        if let CapacityEvidenceSample::Window(window) = sample.capacity_evidence {
            let capacity_trace = trace
                .capacity_trace(index)
                .ok_or(TestError::MissingCapacityWindow)?;
            capacity_windows = capacity_windows.saturating_add(1);
            assert_eq!(
                capacity_trace.state_completion_counts.iter().sum::<u32>(),
                window.completed_attempts
            );
            let exposure = capacity_trace.state_exposure_seconds.iter().sum::<f64>();
            assert!((exposure - window.exposure_seconds).abs() <= f64::EPSILON);
            assert_eq!(
                capacity_trace.busy_slot_micros,
                (window.concurrency * exposure * 1_000_000.0_f64).round() as u128
            );
        }
    }
    Ok((arrival_windows, capacity_windows, reliability_observations))
}

fn assert_sample_fields(sample: &ControllerSample) {
    assert!(sample.samples_per_capacity_class >= sample.samples_per_capacity_class_min);
    assert_eq!(
        sample.scenario_count,
        sample
            .capacity_class_count
            .saturating_mul(sample.samples_per_capacity_class)
    );
    assert!(sample.selected_late_area_mean.is_finite());
    assert!(sample.selected_replica_seconds_mean.is_finite());
    assert!(sample.selected_cost.is_finite());
    assert!(sample.lead_time_fast_seconds.is_finite());
    assert!(sample.lead_time_slow_seconds.is_finite());
    assert!((0.0_f64..=1.0_f64).contains(&sample.lead_time_slow_probability));
}

fn assert_arrival_interval(at_micros: u64, window: crate::ArrivalWindowSample) {
    assert_eq!(window.end_micros, at_micros);
    assert_eq!(
        window.end_micros.saturating_sub(window.start_micros),
        (window.exposure_seconds * 1_000_000.0_f64).round() as u64
    );
}

fn assert_reported_arrival_intervals() -> Result<(), TestError> {
    let reported = run_reported_arrivals(
        FaultPattern {
            drop_every: 0,
            duplicate_every: 0,
            delay_micros: 0,
            odd_sequence_delay_micros: 0,
        },
        None,
    )?;
    for index in 0..reported.len() {
        let sample = reported
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
        if let ArrivalEvidenceSample::Accepted(window) = sample.arrival_evidence {
            assert_arrival_interval(sample.at_micros, window);
        }
    }
    Ok(())
}

#[test]
fn closed_loop_accepts_ready_window_with_rebalance_pause() -> Result<(), TestError> {
    const TICK_COUNT: u32 = 3;
    let closed_loop = capacity_test_closed_loop(PauseWitnessWorkload, TICK_COUNT)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?
        .with_rebalance(2_000, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, TICK_COUNT, closed_loop)?;
    let mut snapshots = Vec::with_capacity(TICK_COUNT as usize);
    for tick in 0..u64::from(TICK_COUNT) {
        snapshots.push(harness.tick(tick * 10_000)?);
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let eligible_tick = snapshots
        .windows(2)
        .position(|pair| {
            pair[0].partitions_ready
                && pair[1].partitions_ready
                && pair[0].rebalance_pause_micros < pair[1].rebalance_pause_micros
        })
        .map(|index| index + 1)
        .ok_or(TestError::MissingPauseWindow)?;
    let sample = closed_loop
        .trace()
        .sample(eligible_tick)
        .ok_or(TestError::MissingControllerSample)?;

    assert!(matches!(
        sample.capacity_evidence,
        CapacityEvidenceSample::Window(_)
    ));
    Ok(())
}

#[test]
fn reconciliation_churn_does_not_starve_capacity_evidence() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(ReconciliationChurnWorkload, 12)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 240, 12, 2, 16)?
        .with_rebalance(20_000, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 12, closed_loop)?;
    let mut unready_ticks = 0_u8;
    for tick in 0_u64..12 {
        unready_ticks =
            unready_ticks.saturating_add(u8::from(!harness.tick(tick * 10_000)?.partitions_ready));
    }
    let (_result, closed_loop) = harness.finish_with_graph();

    assert!(unready_ticks >= 5);
    assert!(
        closed_loop
            .trace()
            .capacity_evidence_count(CapacityEvidenceKind::Window)
            >= 5
    );
    Ok(())
}

#[test]
fn unused_paused_partition_does_not_change_capacity_posterior() -> Result<(), TestError> {
    let (baseline, baseline_pause) = run_idle_partition_capacity_trace(1)?;
    let (with_idle_partition, idle_partition_pause) = run_idle_partition_capacity_trace(2)?;

    assert_eq!(baseline_pause, 0);
    assert!(idle_partition_pause > 0);
    assert_eq!(baseline, with_idle_partition);
    Ok(())
}

fn run_idle_partition_capacity_trace(partition_count: u32) -> Result<(Vec<u64>, u64), TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        calendar_segment_count_max: 4,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count,
        replica_count_max: 2,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        report_interval_micros: 10_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: test_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let capacity_grid = CapacityGrid::new(
        &[0.005_f64, 0.01_f64],
        &[200.0_f64, 400.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let closed_loop = ClosedLoop::new(
        IdlePartitionCapacityWorkload {
            move_idle_partition: partition_count > 1,
        },
        &controller_configuration,
        capacity_grid,
        12,
    )?;
    let plant_configuration = PlantConfiguration::new(partition_count, 100, 240, 12, 2, 16)?
        .with_rebalance(2_000, 0)
        .with_metric_poll_interval_micros(10_000);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 12, closed_loop)?;
    let mut rebalance_pause_micros = 0;
    for tick in 0_u64..12 {
        rebalance_pause_micros = harness.tick(tick * 10_000)?.rebalance_pause_micros;
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let posterior = (0..closed_loop.trace().len())
        .filter_map(|index| closed_loop.trace().sample(index))
        .map(|sample| sample.no_knee_probability.to_bits())
        .collect();
    Ok((posterior, rebalance_pause_micros))
}

#[test]
fn generated_trace_mean_matches_legacy_concurrency_for_busy_zero_completion()
-> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(RampCapacityWorkload, 2)?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?;
    let mut harness = SimulationHarness::new(plant_configuration, 1, 3, closed_loop)?;

    harness.tick(0)?;
    harness.tick(10_000)?;
    let sample = harness
        .graph()
        .trace()
        .sample(1)
        .ok_or(TestError::MissingControllerSample)?;

    let CapacityEvidenceSample::Window(window) = sample.capacity_evidence else {
        return Err(TestError::MissingCapacityWindow);
    };
    assert_eq!(window.completed_attempts, 0);
    assert!(window.concurrency > 0.0_f64);
    Ok(())
}

fn capacity_test_closed_loop<Workload: TickGenerator>(
    workload: Workload,
    sample_count_max: u32,
) -> Result<ClosedLoop<Workload>, TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        calendar_segment_count_max: 4,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        report_interval_micros: 10_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: test_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let capacity_grid = CapacityGrid::new(
        &[0.005_f64, 0.01_f64],
        &[200.0_f64, 400.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    Ok(ClosedLoop::new(
        workload,
        &controller_configuration,
        capacity_grid,
        sample_count_max,
    )?)
}

#[test]
fn retargeted_up_cohorts_record_disjoint_replica_deltas() -> Result<(), TestError> {
    let closed_loop = capacity_test_closed_loop(CohortSegmentWorkload, 10)?;
    let plant_configuration = PlantConfiguration::new(4, 16, 16, 10, 2, 8)?;
    let mut harness = SimulationHarness::new(plant_configuration, 1, 10, closed_loop)?;

    for at_micros in [
        0_u64,
        10_000_000,
        20_000_000,
        100_000_000,
        100_200_000,
        120_000_000,
        120_200_000,
        121_000_000,
    ] {
        harness.tick(at_micros)?;
    }
    let actual_addition = harness.tick(122_000_000)?.replicas.saturating_sub(1);
    let (_result, closed_loop) = harness.finish_with_graph();
    let recorded_delta = (0..closed_loop.trace().len())
        .filter_map(|index| closed_loop.trace().sample(index))
        .filter_map(|sample| match sample.lead_time_evidence {
            crate::LeadTimeEvidenceSample::Completed {
                direction: prosody_scale_core::TransitionDirection::Up,
                replica_delta,
                ..
            } => Some(replica_delta),
            crate::LeadTimeEvidenceSample::None
            | crate::LeadTimeEvidenceSample::Completed { .. }
            | crate::LeadTimeEvidenceSample::Censored { .. } => None,
        })
        .sum::<u32>();

    assert_eq!(actual_addition, 4);
    assert_eq!(recorded_delta, actual_addition);
    Ok(())
}

#[test]
fn metric_flaps_between_polls_are_invisible_to_the_plant() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(4, 4, 1, 4, 4, 2)?
        .with_rebalance(0, 0)
        .with_metric_poll_interval_micros(10);
    let mut harness = SimulationHarness::new(configuration, 1, 5, MetricFlapWorkload)?;

    for at_micros in [0_u64, 1, 2, 10, 11] {
        harness.tick(at_micros)?;
    }
    let (result, _) = harness.finish_with_graph();
    assert_eq!(result.changes.len(), 1);
    assert_eq!(result.changes[0].replicas, 2);
    Ok(())
}

#[test]
fn higher_retarget_preserves_each_completed_lead_time() -> Result<(), TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        calendar_segment_count_max: 4,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        report_interval_micros: 10_000_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: test_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01, 3.0_f64)?,
    };
    let capacity_grid = CapacityGrid::new(&[0.001_f64], &[1_000.0_f64], &[0.0_f64, 1.0_f64])?;
    let closed_loop = ClosedLoop::new(
        RetargetWorkload,
        &controller_configuration,
        capacity_grid,
        8,
    )?;
    let plant_configuration = PlantConfiguration::new(4, 16, 16, 4, 2, 8)?;
    let mut harness = SimulationHarness::new(plant_configuration, 1, 4, closed_loop)?;

    harness.tick(0)?;
    harness.tick(10_000_000)?;
    harness.tick(100_000_000)?;
    let first_ready = harness.tick(100_200_000)?;
    harness.tick(110_000_000)?;
    let second_ready = harness.tick(110_200_000)?;
    let (_result, closed_loop) = harness.finish_with_graph();
    let completed = (0..closed_loop.trace().len())
        .filter_map(|index| closed_loop.trace().sample(index))
        .filter(|sample| {
            matches!(
                sample.lead_time_evidence,
                crate::LeadTimeEvidenceSample::Completed { .. }
            )
        })
        .count();

    assert_eq!(first_ready.replicas, 2);
    assert_eq!(second_ready.replicas, 3);
    assert_eq!(completed, 2);
    Ok(())
}

struct RetargetWorkload;

struct ObservationBoundaryWorkload {
    scheduled_released: u32,
    observed_released: u32,
}

impl TickGenerator for ObservationBoundaryWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        self.scheduled_released = context.plant.released;
        Ok(TickInputs {
            message_count: 1,
            timer_count: 0,
            handler_micros: 1,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Hold,
        })
    }

    fn observe(
        &mut self,
        context: TickContext<'_>,
        inputs: TickInputs,
    ) -> Result<TickInputs, PlantError> {
        self.observed_released = context.plant.released;
        Ok(inputs)
    }

    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        Ok(EventInputs {
            release_micros: 500_000,
            partition: context.event_index % context.partition_count,
            key: context.event_index % context.key_count,
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            outcome: EventOutcome::Final(FinalOutcome::Success),
        })
    }
}

#[test]
fn controller_observes_only_evidence_available_at_its_execution_time() -> Result<(), TestError> {
    let graph = ObservationBoundaryWorkload {
        scheduled_released: u32::MAX,
        observed_released: u32::MAX,
    };
    let configuration = PlantConfiguration::new(1, 1, 1, 1, 1, 1)?;
    let mut harness = SimulationHarness::new(configuration, 1, 2, graph)?;

    harness.tick(1_000_000)?;

    assert_eq!(harness.graph().scheduled_released, 0);
    assert_eq!(harness.graph().observed_released, 1);
    Ok(())
}

impl TickGenerator for RetargetWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 1,
            timer_count: 0,
            handler_micros: 1_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 100_000_000,
            scale: ScaleDirective::Request {
                replicas: if context.tick_index == 0 { 2 } else { 3 },
            },
        })
    }
}

struct CapacityWorkload;

struct DrainingScaleDownWorkload;

struct PauseWitnessWorkload;

struct CohortSegmentWorkload;

struct MetricFlapWorkload;

impl TickGenerator for MetricFlapWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        let scale = match context.tick_index {
            1 => ScaleDirective::Request { replicas: 5 },
            2 => ScaleDirective::Request { replicas: 2 },
            _ => ScaleDirective::Hold,
        };
        Ok(TickInputs {
            message_count: 0,
            timer_count: 0,
            handler_micros: 1_000,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale,
        })
    }
}

impl TickGenerator for CohortSegmentWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        let replicas = if context.tick_index == 1 { 3 } else { 5 };
        Ok(TickInputs {
            message_count: 0,
            timer_count: 0,
            handler_micros: 1_000,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 100_000_000,
            scale: ScaleDirective::Request { replicas },
        })
    }
}

impl TickGenerator for CapacityWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 10_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Request {
                replicas: u32::from(context.tick_index >= 3) + 1,
            },
        })
    }
}

impl TickGenerator for DrainingScaleDownWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: u32::from(context.tick_index == 0) * 4,
            timer_count: 0,
            handler_micros: 25_000,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: if context.tick_index == 0 {
                ScaleDirective::Request { replicas: 1 }
            } else {
                ScaleDirective::ExternalHold
            },
        })
    }
}

impl TickGenerator for PauseWitnessWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 1,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: if context.tick_index == 1 {
                ScaleDirective::Request { replicas: 2 }
            } else {
                ScaleDirective::ExternalHold
            },
        })
    }
}

struct IdlePartitionCapacityWorkload {
    move_idle_partition: bool,
}

impl TickGenerator for IdlePartitionCapacityWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 10_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Request {
                replicas: u32::from(self.move_idle_partition && context.tick_index % 4 >= 2) + 1,
            },
        })
    }

    fn event(&self, context: EventContext<'_>) -> Result<EventInputs, PlantError> {
        Ok(EventInputs {
            release_micros: context.tick.now_micros,
            partition: 0,
            key: context.event_index % context.key_count,
            handler_micros: context.inputs.handler_micros,
            dependency_operations: context.inputs.dependency_operations,
            outcome: EventOutcome::Final(FinalOutcome::Success),
        })
    }
}

struct RampCapacityWorkload;

struct ReconciliationChurnWorkload;

impl TickGenerator for ReconciliationChurnWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 1_000_000,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Request {
                replicas: context.tick_index % 2 + 1,
            },
        })
    }
}

impl TickGenerator for RampCapacityWorkload {
    fn calculate(&mut self, _: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 1_000_000,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::ExternalHold,
        })
    }
}

struct ReportedArrivalWorkload {
    reporter_tick: Option<(u32, ReporterDirective)>,
}

struct SourceArrivalWorkload {
    messages: u32,
    timers: u32,
}

impl TickGenerator for SourceArrivalWorkload {
    fn calculate(&mut self, _: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: self.messages,
            timer_count: self.timers,
            handler_micros: 1,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Hold,
        })
    }
}

impl TickGenerator for ReportedArrivalWorkload {
    fn calculate(&mut self, _: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 10,
            timer_count: 0,
            handler_micros: 1,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 0,
            scale: ScaleDirective::Hold,
        })
    }

    fn reporter(&self, context: TickContext<'_>) -> ReporterDirective {
        self.reporter_tick
            .filter(|(tick, _directive)| *tick == context.tick_index)
            .map_or(ReporterDirective::Send, |(_tick, directive)| directive)
    }
}

series_graph! {
    struct RegimeGraph(TickContext<'_>) with (
        initial_demand: u32,
        base_handler_micros: u64,
        dependency_divisor: u64,
    ) {
        series dependency: u64 ["dependency time", Microseconds, State] =
            DependencyFunction { divisor: dependency_divisor } => (handler);
        series handler: u64 ["handler time", Microseconds, State] =
            HandlerFunction { base_micros: base_handler_micros } => (demand);
        series demand: u32 ["message arrivals", Count, Input] =
            DemandFunction { initial: initial_demand } => () previous (demand);
        output output: TickInputs = RegimeOutput {} => (demand, handler, dependency);
    }
}

impl TickGenerator for RegimeGraph {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(RegimeGraph::evaluate(self, context.now_micros, context))
    }
}

struct RegimeOutput;

impl OutputFunction<TickContext<'_>, (u32, u64, u64)> for RegimeOutput {
    type Output = TickInputs;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        values: (u32, u64, u64),
    ) -> Self::Output {
        TickInputs {
            message_count: values.0,
            timer_count: 0,
            handler_micros: values.1,
            dependency_operations: 1,
            dependency_operation_micros: values.2,
            handler_added_micros: 0,
            outcome: EventOutcomeRule::Success,
            launch_delay_micros: 30_000,
            scale: if context.frame.tick_index == 0 {
                ScaleDirective::Request { replicas: 4 }
            } else {
                ScaleDirective::Hold
            },
        }
    }
}

struct DemandFunction {
    initial: u32,
}

impl SeriesFunction<TickContext<'_>, (Option<u32>,)> for DemandFunction {
    type Output = u32;

    fn calculate(
        &self,
        _: SeriesContext<'_, TickContext<'_>>,
        (previous,): (Option<u32>,),
    ) -> Self::Output {
        previous.map_or(self.initial, |value| value.saturating_add(1))
    }
}

struct HandlerFunction {
    base_micros: u64,
}

impl SeriesFunction<TickContext<'_>, (u32,)> for HandlerFunction {
    type Output = u64;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (message_count,): (u32,),
    ) -> Self::Output {
        let desired_replicas = context
            .frame
            .history
            .desired_replicas(0)
            .unwrap_or(context.frame.plant.replicas);
        self.base_micros
            .saturating_add(u64::from(message_count) * 10)
            .saturating_add(u64::from(desired_replicas))
    }
}

struct DependencyFunction {
    divisor: u64,
}

impl SeriesFunction<TickContext<'_>, (u64,)> for DependencyFunction {
    type Output = u64;

    fn calculate(
        &self,
        context: SeriesContext<'_, TickContext<'_>>,
        (handler_micros,): (u64,),
    ) -> Self::Output {
        handler_micros
            .saturating_div(self.divisor)
            .saturating_add(u64::from(context.frame.plant.backlog))
    }
}

#[test]
fn parallel_batch_preserves_serial_results_and_order() -> Result<(), TestError> {
    let serial = vec![scenario()?.run(), scenario()?.run()];
    let parallel = run_parallel(vec![scenario()?, scenario()?]);
    assert_eq!(parallel, serial);
    Ok(())
}

#[test]
fn workload_series_replays_demand_and_handler_distributions() -> Result<(), TestError> {
    let handlers = QuantileTable::new(&[100_u64, 200, 400])?;
    let workload = WorkloadSeries::new(&[0_u64, 10], &[2_u32, 1], handlers, 4, 4, 0, 91)?;
    let mut first = Plant::new(configuration()?, 1)?;
    let mut replay = Plant::new(configuration()?, 1)?;
    workload.add_to(&mut first)?;
    workload.add_to(&mut replay)?;
    let first = first.run();
    let replay = replay.run();

    assert_eq!(first, replay);
    assert_eq!(first.events().len(), 3);
    assert!(
        first
            .events()
            .iter()
            .all(|event| [100_u64, 200, 400].contains(&event.handler_micros))
    );
    Ok(())
}

#[test]
fn dependency_inputs_raise_measured_handler_latency() -> Result<(), TestError> {
    let base = PlantConfiguration::new(4, 4, 4, 1, 4, 1)?;
    let latency = StepSeries::new(&[0_u64, 5_000], &[1_000_u64, 2_000])?;
    let curve = ConcurrencyLatencyCurve::new(&[0_u32, 1], &[0_u64, 10_000])?;
    let mut baseline = Plant::new(base.clone(), 1)?;
    let mut bottleneck = Plant::new(
        base.with_dependency_latency_series(latency)
            .with_dependency_latency_curve(curve),
        1,
    )?;
    for key in 0_u32..4 {
        let event = event(0, key, 0);
        baseline.add_event(event)?;
        bottleneck.add_event(event)?;
    }
    let baseline = baseline.run();
    let bottleneck = bottleneck.run();
    assert!(maximum_dependency_time(&bottleneck) > maximum_dependency_time(&baseline));
    Ok(())
}

#[test]
fn principal_regimes_exercise_distinct_failure_mechanisms() -> Result<(), TestError> {
    let (application, (hot_key, (rebalance, contention))) = rayon::join(
        || run_principal_regime(PrincipalRegime::ApplicationLimited),
        || {
            rayon::join(
                || run_principal_regime(PrincipalRegime::HotSerializedKey),
                || {
                    rayon::join(
                        || run_principal_regime(PrincipalRegime::RebalanceStorm),
                        || run_principal_regime(PrincipalRegime::HandlerContention),
                    )
                },
            )
        },
    );
    let application = application?;
    let hot_key = hot_key?;
    let rebalance = rebalance?;
    let contention = contention?;

    assert_eq!(
        application.inputs().names().collect::<Vec<_>>(),
        vec![
            "message_count",
            "timer_count",
            "historical_message_count",
            "historical_replicas",
            "external_target",
            "scale_changed",
            "handler_micros",
            "shared_resource_capacity_per_second",
            "dependency_operation_micros",
            "launch_delay_micros",
        ]
    );
    assert_eq!(
        application.inputs().cell("message_count", 1),
        Some(SeriesCell::Unsigned32(100))
    );
    assert!(final_settle(&hot_key) > final_settle(&application));
    assert!(final_settle(&rebalance) > final_settle(&application));
    assert!(maximum_handler_time(&contention) > maximum_handler_time(&application));
    Ok(())
}

#[test]
fn snapshot_fault_replay_is_deterministic() -> Result<(), TestError> {
    let (first, second) = rayon::join(
        || run_principal_regime(PrincipalRegime::SnapshotFaults),
        || run_principal_regime(PrincipalRegime::SnapshotFaults),
    );
    assert_eq!(first?.settlements(), second?.settlements());
    Ok(())
}

#[test]
fn replica_ceiling_exposes_unmet_demand_at_the_limit() -> Result<(), TestError> {
    let run = run_principal_regime(PrincipalRegime::ReplicaCeiling)?;
    let targets = (0..run.controller().len())
        .filter_map(|index| run.controller().sample(index))
        .filter(|sample| !sample.hold)
        .map(|sample| sample.target)
        .collect::<Vec<_>>();

    assert!(targets.iter().all(|target| *target <= 8));
    // The burst work is sunk before any transition can land, so the
    // controller does not buy replicas that save nothing. The limit
    // candidate's expected cost stays positive: the ceiling hides no
    // unmet demand.
    let exposed = (0..run.controller().len()).any(|index| {
        run.controller()
            .decision_expected_costs(index)
            .and_then(|losses| losses.last().copied())
            .is_some_and(|loss| loss > 0.0_f64)
    });
    assert!(exposed);
    Ok(())
}

#[test]
#[cfg(simulation_profile)]
/// Runs only with the simulation profile because it executes three full
/// workloads.
fn capacity_regimes_record_passive_resource_windows() -> Result<(), TestError> {
    [
        PrincipalRegime::LinearThroughput,
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
    ]
    .par_iter()
    .try_for_each(|&regime| {
        let run = run_capacity_evidence_regime(regime)?;
        for row in 0..run.inputs().len() {
            assert_eq!(
                run.inputs().cell("external_target", row),
                Some(SeriesCell::Unsigned32(0))
            );
        }
        validate_principal_regime(regime, RegimeExperiment::CapacityEvidence, &run)?;
        assert_eq!(run.stop().reason, RunStopReason::DurationComplete);
        assert_eq!(run.stop().at_micros, 180_000_000);
        assert!(
            run.controller()
                .capacity_evidence_count(CapacityEvidenceKind::Window)
                > 0
        );
        assert!(run.events().len() > 100_000);
        let mut recorded_windows = 0_usize;
        for index in 0..run.controller().len() {
            let sample = run
                .controller()
                .sample(index)
                .ok_or(TestError::MissingControllerSample)?;
            if let CapacityEvidenceSample::Window(window) = sample.capacity_evidence {
                recorded_windows += 1;
                assert!(window.exposure_seconds > 0.0_f64);
                assert!(window.concurrency > 0.0_f64);
                assert!(window.throughput_per_second().is_finite());
            }
        }
        assert!(recorded_windows > 0);
        let capacity_values = run.controller().capacity_posterior_values();
        let configured_capacity_values: &[f64] = match regime {
            PrincipalRegime::LinearThroughput => &[80.0_f64, 320.0_f64, 640.0_f64],
            PrincipalRegime::FlatPostKnee | PrincipalRegime::DecliningPostKnee => {
                &[80.0_f64, 320.0_f64, 600.0_f64]
            }
            _ => &[],
        };
        assert_eq!(capacity_values, configured_capacity_values);
        for index in 0..run.controller().len() {
            let posterior = run
                .controller()
                .capacity_posterior(index)
                .ok_or(TestError::MissingControllerSample)?;
            let total = posterior.iter().sum::<f64>();
            assert!(total == 0.0_f64 || (total - 1.0_f64).abs() < 1.0e-9_f64);
        }
        let prior = run
            .controller()
            .capacity_posterior(0)
            .ok_or(TestError::MissingControllerSample)?;
        // A refuted knee family can relax to prior-shaped redraw cohorts. The
        // final marginal can then equal the prior. Movement during the run is
        // the non-vacuity claim.
        assert!(
            (1..run.controller().len())
                .filter_map(|index| run.controller().capacity_posterior(index))
                .any(|posterior| prior
                    .iter()
                    .zip(posterior)
                    .any(|(before, after)| (before - after).abs() > 1.0e-12_f64))
        );
        Ok(())
    })
}

// One test per principal regime: nextest schedules the runs concurrently,
// a red regime never hides another, and one regime re-runs in isolation
// with `-E 'test(<name>_regime_satisfies)'`.
macro_rules! closed_loop_regime_tests {
    ($($name:ident => $regime:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() -> Result<(), TestError> {
                let run = run_principal_regime(PrincipalRegime::$regime)?;
                validate_principal_regime(
                    PrincipalRegime::$regime,
                    RegimeExperiment::ClosedLoop,
                    &run,
                )?;
                Ok(())
            }
        )+
    };
}

// LinearThroughput is absent:
// `linear_closed_loop_satisfies_its_declared_outcome` runs the same simulation
// and ends with the same claim validation, so a macro entry would run the
// ~20-minute simulation twice.
closed_loop_regime_tests! {
    idle_regime_satisfies_its_claims => Idle,
    application_limited_regime_satisfies_its_claims => ApplicationLimited,
    flat_post_knee_regime_satisfies_its_claims => FlatPostKnee,
    declining_post_knee_regime_satisfies_its_claims => DecliningPostKnee,
    short_burst_regime_satisfies_its_claims => ShortBurst,
    seasonal_waves_regime_satisfies_its_claims => SeasonalWaves,
    hot_partition_regime_satisfies_its_claims => HotPartition,
    timer_wave_regime_satisfies_its_claims => TimerWave,
    hot_serialized_key_regime_satisfies_its_claims => HotSerializedKey,
    transient_failures_regime_satisfies_its_claims => TransientFailures,
    permanent_rejections_regime_satisfies_its_claims => PermanentRejections,
    rebalance_storm_regime_satisfies_its_claims => RebalanceStorm,
    handler_contention_regime_satisfies_its_claims => HandlerContention,
    loose_budget_backlog_regime_satisfies_its_claims => LooseBudgetBacklog,
    snapshot_faults_regime_satisfies_its_claims => SnapshotFaults,
    missing_reporter_regime_satisfies_its_claims => MissingReporter,
    aggregator_replacement_regime_satisfies_its_claims => AggregatorReplacement,
    replica_ceiling_regime_satisfies_its_claims => ReplicaCeiling,
    historical_match_regime_satisfies_its_claims => HistoricalMatch,
    historical_exceeded_regime_satisfies_its_claims => HistoricalExceeded,
    historical_under_regime_satisfies_its_claims => HistoricalUnder,
    historical_missing_regime_satisfies_its_claims => HistoricalMissing,
}

#[test]
fn partition_diagnostics_account_for_each_accepted_assignment() -> Result<(), TestError> {
    let run = run_principal_regime(PrincipalRegime::HotPartition)?;
    let mut evidence = 0_u32;
    let mut ranks = 0_u32;
    for index in 0..run.controller().len() {
        let sample = run
            .controller()
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
        evidence = evidence.saturating_add(sample.partition_evidence_count);
        ranks = sample
            .partition_predictive_rank_counts
            .into_iter()
            .fold(ranks, u32::saturating_add);
        assert!(
            sample
                .partition_predictive_covered_counts
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
        );
    }
    assert!(evidence > 0);
    assert_eq!(ranks, evidence);
    Ok(())
}

#[test]
fn hot_partition_exposes_unavoidable_placement_loss() -> Result<(), TestError> {
    const EVENT_COUNT: u32 = 400;
    let configuration = PlantConfiguration::new(4, EVENT_COUNT, EVENT_COUNT, 1, 1, 1)?
        .with_dependency_operation_micros(0)
        .with_rebalance(0, 0);
    let mut hot = Plant::new(configuration.clone(), 4)?;
    let mut striped = Plant::new(configuration, 4)?;
    for key in 0..EVENT_COUNT {
        let mut hot_event = event(0, key, 100_000);
        hot_event.partition = 0;
        hot.add_event(hot_event)?;
        let mut striped_event = hot_event;
        striped_event.partition = key % 4;
        striped.add_event(striped_event)?;
    }

    let hot = hot.run();
    let striped = striped.run();
    assert_eq!(hot.settlements().len(), striped.settlements().len());
    assert!(final_settle(&hot) > final_settle(&striped));
    Ok(())
}

#[cfg(simulation_profile)]
fn assert_lead_time_diagnostics_use_prequential_predictive_distributions(
    run: &crate::PrincipalRun,
) -> Result<(), TestError> {
    let mut completed = 0_u32;
    let mut maximum_target = 0_u32;
    let mut minimum_loss = f64::INFINITY;
    let mut maximum_loss = 0.0_f64;
    let mut target_streak = 0_u32;
    let mut maximum_target_streak = 0_u32;
    let mut first_two = None;
    let mut after_two = None;
    let mut target_two_count = 0_u32;
    let mut last_two = None;
    let mut final_state = None;
    for index in 0..run.controller().len() {
        let sample = run
            .controller()
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
        maximum_target = maximum_target.max(sample.target);
        target_streak = if sample.target > 1 {
            target_streak.saturating_add(1)
        } else {
            0
        };
        maximum_target_streak = maximum_target_streak.max(target_streak);
        let losses = run.controller().decision_expected_costs(index);
        let satisfactions = run
            .controller()
            .decision_deadline_satisfaction_probabilities(index);
        if sample.target == 2 && first_two.is_none() {
            first_two = Some((
                index,
                losses.and_then(|values| values.first()).copied(),
                losses.and_then(|values| values.get(1)).copied(),
                satisfactions.and_then(|values| values.first()).copied(),
                satisfactions.and_then(|values| values.get(1)).copied(),
                sample.arrival_predictive_median_count,
                sample.capacity_median_per_second,
            ));
        } else if first_two.is_some() && sample.target == 1 && after_two.is_none() {
            after_two = Some((
                index,
                losses.and_then(|values| values.first()).copied(),
                losses.and_then(|values| values.get(1)).copied(),
                satisfactions.and_then(|values| values.first()).copied(),
                satisfactions.and_then(|values| values.get(1)).copied(),
                sample.arrival_predictive_median_count,
                sample.capacity_median_per_second,
            ));
        }
        if sample.target == 2 {
            target_two_count = target_two_count.saturating_add(1);
            last_two = Some((index, sample.arrival_predictive_median_count));
        }
        final_state = Some((
            index,
            sample.target,
            sample.arrival_predictive_median_count,
            sample.capacity_median_per_second,
            sample.no_knee_probability,
        ));
        minimum_loss = minimum_loss.min(sample.expected_cost);
        maximum_loss = maximum_loss.max(sample.expected_cost);
        if matches!(
            sample.lead_time_evidence,
            crate::LeadTimeEvidenceSample::Completed { .. }
        ) {
            completed = completed.saturating_add(1);
            assert!(
                sample.lead_time_predictive_low_seconds
                    <= sample.lead_time_predictive_median_seconds
                    && sample.lead_time_predictive_median_seconds
                        <= sample.lead_time_predictive_high_seconds
            );
            assert!((0.0_f64..=1.0_f64).contains(&sample.lead_time_predictive_rank));
        }
    }
    assert!(
        completed > 0,
        "the regime must complete a scale transition: applied={:?}, target_max={maximum_target}, \
         target_two_count={target_two_count}, target_streak_max={maximum_target_streak}, \
         loss_min={minimum_loss}, loss_max={maximum_loss}, first_two={first_two:?}, \
         after_two={after_two:?}, last_two={last_two:?}, final={final_state:?}",
        run.applied_changes(),
    );
    Ok(())
}

#[cfg(simulation_profile)]
fn assert_linear_closed_loop_uses_only_controller_scale_targets(run: &crate::PrincipalRun) {
    assert_eq!(run.stop().reason, RunStopReason::DurationComplete);
    for row in 0..run.inputs().len() {
        assert_eq!(
            run.inputs().cell("external_target", row),
            Some(SeriesCell::Unsigned32(0))
        );
    }
}

#[test]
#[cfg(simulation_profile)]
/// Runs only with the simulation profile because it executes a full workload.
fn linear_closed_loop_satisfies_its_declared_outcome() -> Result<(), TestError> {
    let run = run_principal_regime(PrincipalRegime::LinearThroughput)?;
    assert_lead_time_diagnostics_use_prequential_predictive_distributions(&run)?;
    assert_linear_closed_loop_uses_only_controller_scale_targets(&run);
    assert_linear_miss_bound(&run);
    assert_linear_scale_response(&run);
    validate_principal_regime(
        PrincipalRegime::LinearThroughput,
        RegimeExperiment::ClosedLoop,
        &run,
    )?;
    Ok(())
}

#[cfg(simulation_profile)]
fn assert_linear_miss_bound(run: &crate::PrincipalRun) {
    let accounting = linear_miss_accounting(run);
    assert!(
        accounting.reaction_window_misses <= accounting.reaction_window_allowance
            && accounting.outside_misses.saturating_mul(100) <= accounting.outside_settlements,
        "linear miss accounting failed: {accounting:?}",
    );
}

#[cfg(simulation_profile)]
fn assert_linear_scale_response(run: &crate::PrincipalRun) {
    assert!(
        run.applied_changes()
            .iter()
            .any(|change| change.replicas >= 3),
        "the linear regime did not cross three ready replicas: {:?}",
        run.applied_changes()
    );
}

#[test]
#[cfg(simulation_profile)]
/// Runs only with the simulation profile because it executes a full workload.
fn capacity_median_matches_each_posterior_slice() -> Result<(), TestError> {
    let run = run_capacity_evidence_regime(PrincipalRegime::DecliningPostKnee)?;
    let values = run.controller().capacity_posterior_values();
    for index in 0..run.controller().len() {
        let sample = run
            .controller()
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
        let posterior = run
            .controller()
            .capacity_posterior(index)
            .ok_or(TestError::MissingControllerSample)?;
        let mut cumulative = 0.0_f64;
        let mut expected = values
            .last()
            .copied()
            .ok_or(TestError::MissingControllerSample)?;
        for (&value, &mass) in values.iter().zip(posterior) {
            cumulative += mass;
            if cumulative >= 0.5_f64 {
                expected = value;
                break;
            }
        }
        assert_eq!(
            sample.capacity_median_per_second.partial_cmp(&expected),
            Some(Ordering::Equal)
        );
    }
    Ok(())
}

#[test]
fn batch_regime_has_realistic_cardinality_and_task_durations() -> Result<(), TestError> {
    let result = run_batch_regime(8)?;
    assert_eq!(result.settlements().len(), 50_000);
    assert_eq!(result.events().len(), 50_000);
    assert!(result.events().iter().all(|event| {
        (60_000_000..=600_000_000).contains(&event.handler_micros) && event.release_micros == 0
    }));
    assert!(
        result
            .events()
            .iter()
            .any(|event| event.handler_micros == 60_000_000)
    );
    assert!(
        result
            .events()
            .iter()
            .any(|event| event.handler_micros == 600_000_000)
    );
    Ok(())
}

#[test]
fn looser_batch_budget_does_not_raise_the_replica_target() -> Result<(), TestError> {
    let short = run_batch_slo(6 * 60 * 60 * 1_000_000, 0.05)?;
    let medium = run_batch_slo(12 * 60 * 60 * 1_000_000, 0.05)?;
    let long = run_batch_slo(24 * 60 * 60 * 1_000_000, 0.05)?;

    assert!(short.target >= medium.target, "{short:?} {medium:?}");
    assert!(medium.target >= long.target, "{medium:?} {long:?}");
    assert!([short, medium, long].iter().all(|summary| {
        (30_000_000..=90_000_000).contains(&summary.actuation_micros)
            && summary.initial_replicas == 1
    }));
    assert!(short.miss_fraction <= short.epsilon, "{short:?}");
    assert!(medium.miss_fraction <= medium.epsilon, "{medium:?}");
    assert!(long.miss_fraction <= long.epsilon, "{long:?}");
    let instant = run_batch_regime(short.target)?;
    assert!(short.completion_micros > final_settle(&instant));
    Ok(())
}

fn final_settle(result: &crate::SimulationResult) -> u64 {
    result
        .settlements()
        .last()
        .map_or(0, |settlement| settlement.settle_micros)
}

fn maximum_equal_values(values: &[u64]) -> usize {
    values
        .iter()
        .map(|value| {
            values
                .iter()
                .filter(|candidate| *candidate == value)
                .count()
        })
        .max()
        .unwrap_or(0)
}

fn maximum_handler_time(result: &crate::SimulationResult) -> u64 {
    result
        .settlements()
        .iter()
        .map(|settlement| settlement.handler_micros)
        .max()
        .unwrap_or(0)
}

fn maximum_dependency_time(result: &crate::SimulationResult) -> u64 {
    result
        .settlements()
        .iter()
        .map(|settlement| settlement.dependency_micros)
        .max()
        .unwrap_or(0)
}

#[test]
fn one_hot_key_serializes_non_preemptive_work() -> Result<(), TestError> {
    let mut one_replica = Plant::new(configuration()?, 1)?;
    let mut four_replicas = Plant::new(configuration()?, 4)?;
    for plant in [&mut one_replica, &mut four_replicas] {
        plant.add_event(event(0, 0, 10_000))?;
        plant.add_event(event(0, 0, 10_000))?;
    }
    assert_eq!(one_replica.advance_until(0).dispatchable_demand_ceiling, 1);
    assert_eq!(
        four_replicas.advance_until(0).dispatchable_demand_ceiling,
        1
    );
    let one_replica = one_replica.run();
    let four_replicas = four_replicas.run();
    let first = one_replica.settlements()[0];
    let second = one_replica.settlements()[1];
    let first_dispatch = first
        .release_micros
        .saturating_add(first.permit_wait_micros);
    let second_dispatch = second
        .release_micros
        .saturating_add(second.permit_wait_micros);
    assert_eq!([first.event, second.event], [0, 1]);
    assert!(first_dispatch < first.settle_micros);
    assert!(first.settle_micros <= second_dispatch);
    assert!(second_dispatch < second.settle_micros);
    assert_eq!(four_replicas.settlements(), one_replica.settlements());
    Ok(())
}

#[test]
fn independent_keys_use_parallel_slots() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.add_event(event(0, 0, 10_000))?;
    plant.add_event(event(0, 1, 10_000))?;
    let snapshot = plant.advance_until(0);
    assert_eq!(
        snapshot.dispatchable_demand_ceiling,
        snapshot.physical_slots
    );
    let result = plant.run();
    let settlements = result.settlements();
    let first_dispatch = settlements[0]
        .release_micros
        .saturating_add(settlements[0].permit_wait_micros);
    let second_dispatch = settlements[1]
        .release_micros
        .saturating_add(settlements[1].permit_wait_micros);
    assert_eq!(first_dispatch, second_dispatch);
    assert_eq!(settlements[0].permit_wait_micros, 0);
    assert_eq!(settlements[1].permit_wait_micros, 0);
    let mut in_flight = settlements
        .iter()
        .map(|settlement| settlement.in_flight_at_dispatch)
        .collect::<Vec<_>>();
    in_flight.sort_unstable();
    assert_eq!(in_flight, [1, 2]);
    Ok(())
}

#[test]
fn distinct_keys_fill_but_cannot_exceed_replica_slots() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(8, 100, 100, 1, 32, 100)?
        .with_dependency_operation_micros(0)
        .with_rebalance(0, 0);
    let mut plant = Plant::new(configuration, 2)?;
    for key in 0_u32..100 {
        let mut event = event(0, key, 1_000_000);
        event.partition = key % 8;
        plant.add_event(event)?;
    }

    let snapshot = plant.advance_until(0);
    assert_eq!(snapshot.active_handlers, 64);
    assert_eq!(
        snapshot.backlog.saturating_sub(snapshot.active_handlers),
        36
    );
    Ok(())
}

#[test]
fn plant_does_not_add_a_second_dependency_queue() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(8, 8, 8, 1, 32, 4)?
        .with_dependency_operation_micros(50_000)
        .with_rebalance(0, 0);
    let mut plant = Plant::new(configuration, 1)?;
    for key in 0_u32..8 {
        plant.add_event(event(0, key, 0))?;
    }

    let result = plant.run();
    assert_eq!(result.settlements()[3].settle_micros, 50_000);
    assert_eq!(result.settlements()[7].settle_micros, 50_000);
    Ok(())
}

#[test]
fn pending_pod_readiness_does_not_pause_existing_work() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let delays = QuantileTable::new(&[50_000])?;
    let mut random = RandomStream::new(7);
    let actuation = plant.add_scale_request(
        ScaleRequest {
            at_micros: 5_000,
            replicas: 2,
        },
        &delays,
        &mut random,
    )?;
    assert_eq!(actuation.ready_micros, 55_000);
    plant.add_event(event(10_000, 0, 10_000))?;
    plant.add_event(event(60_000, 2, 10_000))?;
    let result = plant.run();
    let first = result.settlements()[0];
    let later = result.settlements()[1];
    let first_dispatch = first
        .release_micros
        .saturating_add(first.permit_wait_micros);
    let later_dispatch = later
        .release_micros
        .saturating_add(later.permit_wait_micros);
    assert_eq!(first_dispatch, 10_000);
    assert!(first.settle_micros < actuation.ready_micros);
    assert!(later_dispatch >= actuation.ready_micros);
    Ok(())
}

#[test]
fn target_eight_cancels_pending_down_without_reconciliation() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(64, 1, 1, 4, 32, 1)?.with_rebalance(10, 90);
    let mut plant = Plant::new(configuration, 8)?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 10,
        replicas: 1,
    })?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 11,
        replicas: 8,
    })?;

    let after = plant.advance_until(1_000_000);

    assert_eq!(after.replicas, 8);
    assert_eq!(after.paused_partitions, 0);
    assert_eq!(after.rebalance_pause_micros, 0);
    assert_eq!(after.reconciliation_started_micros, None);
    assert_eq!(after.reconciliation_completed_micros, None);
    Ok(())
}

#[quickcheck]
fn assignment_model_matches_plant_sticky_rule(
    partition_seed: u8,
    initial_seed: u8,
    targets: Vec<u8>,
) -> TestResult {
    let partition_count = usize::from(partition_seed % 64 + 1);
    let replica_max = partition_count.min(8);
    let initial = u32::from(initial_seed) % replica_max as u32 + 1;
    let mut current = super::initial_assignment(partition_count, initial);
    let mut plant_target = vec![0; partition_count];
    let mut model_target = vec![0; partition_count];
    let mut plant_counts = vec![0; replica_max];
    let mut model_counts = vec![0; replica_max];
    let mut model_moved = vec![false; partition_count];
    let termination_order = (0..replica_max as u32).rev().collect::<Vec<_>>();
    for seed in targets.into_iter().take(32) {
        let target = u32::from(seed) % replica_max as u32 + 1;
        super::sticky_assignment(&current, target, &mut plant_target, &mut plant_counts);
        if let Err(error) = model_assignment(
            &current,
            target,
            &termination_order,
            &mut model_target,
            &mut model_counts,
            &mut model_moved,
        ) {
            return TestResult::error(error.to_string());
        }
        let plant_moved = current
            .iter()
            .zip(&plant_target)
            .map(|(before, after)| before != after);
        if model_target != plant_target || !model_moved.iter().copied().eq(plant_moved) {
            return TestResult::failed();
        }
        current.copy_from_slice(&plant_target);
    }
    TestResult::passed()
}

#[test]
fn seasoned_idle_cancel_and_reissue_bills_the_plant_transition() -> Result<(), TestError> {
    const FRESH_READY_MICROS: u64 = 28_964_442;
    const RESIDUAL_READY_MICROS: u64 = 30_932_467;
    const REPORT_MICROS: u64 = 1_000_000;
    const WINDOW_MICROS: u64 = 32_000_000;
    let configuration = PlantConfiguration::new(64, 1, 1, 8, 32, 1)?.with_rebalance(0, 0);

    let mut continued = Plant::new(configuration.clone(), 8)?;
    continued.replace_scale_target(ScaleChange {
        at_micros: RESIDUAL_READY_MICROS,
        replicas: 1,
    })?;
    continued.replace_scale_target(ScaleChange {
        at_micros: RESIDUAL_READY_MICROS,
        replicas: 1,
    })?;

    let mut reissued = Plant::new(configuration.clone(), 8)?;
    reissued.replace_scale_target(ScaleChange {
        at_micros: RESIDUAL_READY_MICROS,
        replicas: 1,
    })?;
    reissued.replace_scale_target(ScaleChange {
        at_micros: 0,
        replicas: 8,
    })?;
    let reissued_ready_micros = REPORT_MICROS.saturating_add(FRESH_READY_MICROS);
    reissued.replace_scale_target(ScaleChange {
        at_micros: reissued_ready_micros,
        replicas: 1,
    })?;

    let mut held = Plant::new(configuration, 8)?;
    held.replace_scale_target(ScaleChange {
        at_micros: RESIDUAL_READY_MICROS,
        replicas: 1,
    })?;
    held.replace_scale_target(ScaleChange {
        at_micros: 0,
        replicas: 8,
    })?;

    assert_eq!(continued.advance_until(RESIDUAL_READY_MICROS).replicas, 1);
    assert_eq!(reissued.advance_until(reissued_ready_micros).replicas, 1);
    assert_eq!(held.advance_until(WINDOW_MICROS).replicas, 8);
    let continued_seconds = 32.0_f64 + 7.0_f64 * 30.932_467_f64;
    let reissued_seconds = 32.0_f64 + 7.0_f64 * 29.964_442_f64;
    let held_seconds = 8.0_f64 * 32.0_f64;
    assert!(held_seconds > continued_seconds);
    assert!(reissued_seconds < continued_seconds);
    Ok(())
}

#[test]
fn higher_target_preserves_earlier_capacity() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 100_000,
        replicas: 2,
    })?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 200_000,
        replicas: 3,
    })?;

    assert_eq!(plant.advance_until(150_000).replicas, 2);
    assert_eq!(plant.advance_until(200_000).replicas, 3);
    Ok(())
}

#[test]
fn lower_target_preserves_its_pending_replica_subset() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 100_000,
        replicas: 3,
    })?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 200_000,
        replicas: 2,
    })?;

    assert_eq!(plant.advance_until(100_000).replicas, 2);
    Ok(())
}

#[test]
fn repeated_pending_scale_target_keeps_original_readiness() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 100_000,
        replicas: 2,
    })?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 200_000,
        replicas: 2,
    })?;

    assert_eq!(plant.advance_until(100_000).replicas, 2);
    Ok(())
}

#[test]
fn adversarial_up_delays_never_reduce_ready_replicas() -> Result<(), TestError> {
    let configuration = PlantConfiguration::new(4, 4, 1, 8, 4, 2)?.with_rebalance(0, 0);
    let mut plant = Plant::new(configuration, 1)?;
    for change in [
        ScaleChange {
            at_micros: 90,
            replicas: 2,
        },
        ScaleChange {
            at_micros: 30,
            replicas: 3,
        },
        ScaleChange {
            at_micros: 60,
            replicas: 4,
        },
        ScaleChange {
            at_micros: 20,
            replicas: 5,
        },
    ] {
        plant.replace_scale_target(change)?;
    }

    let mut previous = 1;
    for at_micros in [20_u64, 30, 60, 90, 100] {
        let ready = plant.advance_until(at_micros).replicas;
        assert!(ready >= previous);
        previous = ready;
    }
    assert_eq!(previous, 5);
    Ok(())
}

#[quickcheck]
fn latest_count_actuation_converges_and_preserves_bounds(input: Vec<(u8, u8)>) -> bool {
    let Ok(configuration) = PlantConfiguration::new(4, 4, 1, 64, 4, 2) else {
        return false;
    };
    let Ok(mut plant) = Plant::new(configuration.with_rebalance(0, 0), 1) else {
        return false;
    };
    let mut now_micros = 0_u64;
    let mut prior_ready = 1_u32;
    for (target, delay) in input.into_iter().take(32) {
        let replicas = u32::from(target % 16) + 1;
        let ready_micros = now_micros.saturating_add(u64::from(delay) + 1);
        if plant
            .replace_scale_target(ScaleChange {
                at_micros: ready_micros,
                replicas,
            })
            .is_err()
        {
            return false;
        }
        let snapshot = plant.advance_until(now_micros);
        if snapshot.replicas < prior_ready
            && !plant.applied_changes.last().is_some_and(|change| {
                change.at_micros == now_micros && change.replicas == snapshot.replicas
            })
        {
            return false;
        }
        let mode_holds = match &plant.pending_actuation {
            crate::PendingActuation::Converged(cohorts) => {
                cohorts.iter().all(|cohort| cohort.count == 0)
                    && snapshot.replicas == plant.desired_replicas
            }
            crate::PendingActuation::Up(_) => {
                snapshot.replicas <= plant.desired_replicas
                    && snapshot.replicas.saturating_add(plant.in_flight_replicas())
                        <= plant.desired_replicas
            }
            crate::PendingActuation::Down {
                down,
                inactive_up_storage,
            } => {
                inactive_up_storage.iter().all(|cohort| cohort.count == 0)
                    && down.target == plant.desired_replicas
                    && plant.desired_replicas < snapshot.replicas
            }
        };
        if !mode_holds {
            return false;
        }
        prior_ready = snapshot.replicas;
        now_micros = now_micros.saturating_add(1);
    }
    let final_snapshot = plant.advance_until(now_micros.saturating_add(256));
    final_snapshot.replicas == plant.desired_replicas && plant.in_flight_replicas() == 0
}

#[test]
fn transient_failures_consume_attempts_and_backoff() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let mut retried = event(0, 0, 2_000);
    retried.outcome = EventOutcome::from_transient_failures(2, FinalOutcome::Success)
        .map_err(PlantError::from)?;
    plant.add_event(retried)?;
    let during_backoff = plant.advance_until(500_000);
    assert_eq!(during_backoff.active_handlers, 0);
    assert_eq!(during_backoff.backlog, 1);
    let mut normal_backlog = [0_u32; 4];
    let mut failure_backlog = [0_u32; 4];
    let mut normal_release = [0_u64; 4];
    let mut failure_release = [0_u64; 4];
    plant.write_partition_backlogs(
        500_000,
        &mut normal_backlog,
        &mut normal_release,
        &mut failure_backlog,
        &mut failure_release,
    )?;
    assert_eq!(normal_backlog.iter().sum::<u32>(), 0);
    assert_eq!(failure_backlog.iter().sum::<u32>(), 1);
    assert!(failure_release[0] > 500_000);
    let result = plant.run();
    assert_eq!(result.settlements()[0].attempts, 3);
    assert_eq!(result.settlements()[0].settle_micros, 1_004_848);
    Ok(())
}

#[quickcheck]
fn maintained_plant_counters_match_full_recounts(operation_codes: Vec<u8>) -> TestResult {
    let Ok(mut plant) = counter_parity_plant(operation_codes) else {
        return TestResult::error("fixed counter parity plant failed");
    };
    for at_micros in (0_u64..=9_000).step_by(1_000) {
        let snapshot = plant.advance_until(at_micros);
        if !counter_recounts_match(&plant, at_micros, snapshot.released, snapshot.settled) {
            return TestResult::failed();
        }
    }
    TestResult::passed()
}

fn counter_parity_plant(operation_codes: Vec<u8>) -> Result<Plant, PlantError> {
    let event_count = operation_codes.len().clamp(1, 32);
    let configuration = PlantConfiguration::new(4, 4, event_count as u32, 4, 4, 1)?;
    let mut plant = Plant::new(configuration.with_retry_backoff_micros(0), 1)?;
    let codes = operation_codes.into_iter().chain(repeat(0));
    for (index, code) in codes.take(event_count).enumerate() {
        let release_micros = u64::from(code % 8) * 1_000;
        let mut work = event(release_micros, index as u32 % 4, u64::from(code % 5 + 1));
        work.partition = index as u32 % 4;
        work.outcome = match code % 3 {
            0 => EventOutcome::Final(FinalOutcome::Success),
            1 => EventOutcome::Final(FinalOutcome::PermanentFailure),
            _ => EventOutcome::from_transient_failures(1, FinalOutcome::Success)
                .map_err(PlantError::from)?,
        };
        plant.add_event(work)?;
    }
    Ok(plant)
}

fn counter_recounts_match(
    plant: &Plant,
    at_micros: u64,
    actual_released: u32,
    actual_settled: u32,
) -> bool {
    let released = plant
        .events
        .release_micros
        .iter()
        .filter(|release| **release <= at_micros)
        .count() as u32;
    let settled = plant
        .events
        .release_micros
        .iter()
        .zip(&plant.settled_by_event)
        .filter(|(release, is_settled)| **release <= at_micros && **is_settled)
        .count() as u32;
    let failures = plant
        .attempt_outcomes
        .iter()
        .filter(|outcome| outcome.result == crate::AttemptResult::Failure)
        .count();
    actual_released == released
        && actual_settled == settled
        && plant.attempt_failure_count == failures
        && backlog_columns_match_recount(plant, at_micros)
}

fn backlog_columns_match_recount(plant: &Plant, at_micros: u64) -> bool {
    let mut actual_normal = [0_u32; 4];
    let mut actual_normal_release = [0_u64; 4];
    let mut actual_failure = [0_u32; 4];
    let mut actual_failure_release = [0_u64; 4];
    if plant
        .write_partition_backlogs(
            at_micros,
            &mut actual_normal,
            &mut actual_normal_release,
            &mut actual_failure,
            &mut actual_failure_release,
        )
        .is_err()
    {
        return false;
    }
    let mut expected_normal = [0_u32; 4];
    let mut expected_normal_release = [u64::MAX; 4];
    let mut expected_failure = [0_u32; 4];
    let mut expected_failure_release = [u64::MAX; 4];
    for event_index in 0..plant.events.len() {
        let partition = plant.events.partition[event_index] as usize;
        let release = plant.events.release_micros[event_index];
        if release <= at_micros
            && !plant.settled_by_event[event_index]
            && plant.retry_mode_by_event[event_index] == crate::RetryMode::Inline
        {
            expected_normal[partition] = expected_normal[partition].saturating_add(1);
            expected_normal_release[partition] = expected_normal_release[partition].min(release);
        }
        let queued = matches!(
            plant.attempt_state[event_index],
            crate::AttemptState::Backoff(crate::RetryWait::Deferred)
                | crate::AttemptState::Ready(prosody_scale_core::DemandClass::Failure)
        );
        if plant.retry_mode_by_event[event_index] == crate::RetryMode::Deferred && queued {
            expected_failure[partition] = expected_failure[partition].saturating_add(1);
            expected_failure_release[partition] =
                expected_failure_release[partition].min(plant.retry_ready_micros[event_index]);
        }
    }
    for partition in 0..4 {
        expected_normal_release[partition] =
            u64::from(expected_normal_release[partition] != u64::MAX)
                .saturating_mul(expected_normal_release[partition]);
        expected_failure_release[partition] =
            u64::from(expected_failure_release[partition] != u64::MAX)
                .saturating_mul(expected_failure_release[partition]);
    }
    actual_normal == expected_normal
        && actual_normal_release == expected_normal_release
        && actual_failure == expected_failure
        && actual_failure_release == expected_failure_release
}

#[test]
fn failure_attempts_use_the_configured_service_share() -> Result<(), TestError> {
    let mut configuration = PlantConfiguration::new(1, 100, 100, 1, 1, 1)?
        .with_dependency_operation_micros(0)
        .with_retry_backoff_micros(0);
    configuration.retry_policy.defer_threshold = 0.0_f64;
    let mut plant = Plant::new(configuration, 1)?;
    for key in 0_u32..100 {
        let mut work = event(0, key, 1_000);
        work.partition = 0;
        work.outcome = EventOutcome::from_transient_failures(1, FinalOutcome::Success)
            .map_err(PlantError::from)?;
        plant.add_event(work)?;
    }

    let _ = plant.advance_until(80_000);
    let total = plant
        .normal_service_micros
        .saturating_add(plant.failure_service_micros);
    let weighted_failure = plant.failure_service_micros.saturating_mul(10);
    assert!(weighted_failure >= total.saturating_mul(2));
    assert!(weighted_failure <= total.saturating_mul(4));
    Ok(())
}

#[test]
fn message_and_timer_failures_share_retry_semantics() -> Result<(), TestError> {
    let mut message_plant = Plant::new(configuration()?, 1)?;
    let mut timer_plant = Plant::new(configuration()?, 1)?;
    let mut message = event(0, 0, 2_000);
    message.outcome = EventOutcome::from_transient_failures(2, FinalOutcome::Success)
        .map_err(PlantError::from)?;
    let mut timer = message;
    timer.source = EventSource::Timer;
    message_plant.add_event(message)?;
    timer_plant.add_event(timer)?;

    assert_eq!(
        message_plant.run().settlements(),
        timer_plant.run().settlements()
    );
    Ok(())
}

#[test]
fn terminal_outcome_creates_failure_demand_without_losing_its_category() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let count = RetryCount::new(1).map_err(PlantError::from)?;
    let mut terminated = event(0, 0, 2_000);
    terminated.outcome = EventOutcome::Retry {
        outcome: RetryOutcome::Terminal,
        count,
        final_outcome: FinalOutcome::Success,
    };
    plant.add_event(terminated)?;

    let snapshot = plant.advance_until(u64::MAX);
    assert_eq!(snapshot.normal_terminal_failures, 1);
    assert_eq!(snapshot.failure_successes, 1);
    assert_eq!(snapshot.normal_transient_failures, 0);
    Ok(())
}

#[test]
fn permanent_rejection_settles_without_retry() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let mut rejected = event(0, 0, 2_000);
    rejected.outcome = EventOutcome::Final(FinalOutcome::PermanentFailure);
    plant.add_event(rejected)?;
    let result = plant.run();
    assert_eq!(result.settlements()[0].attempts, 1);
    assert_eq!(
        result.settlements()[0].final_outcome,
        FinalOutcome::PermanentFailure
    );
    Ok(())
}

#[test]
fn snapshot_loss_does_not_create_zero_evidence() -> Result<(), TestError> {
    let mut channel = SnapshotChannel::new(
        4,
        FaultPattern {
            drop_every: 2,
            duplicate_every: 0,
            delay_micros: 0,
            odd_sequence_delay_micros: 0,
        },
    )?;
    let mut table = SnapshotTable::new(1)?;
    let mut cursor = SnapshotCursor::new(1)?;
    channel.send(snapshot(1, 10))?;
    channel.deliver(10, &mut table);
    assert_eq!(cursor.next(0, &table), None);
    channel.send(snapshot(2, 20))?;
    channel.send(snapshot(3, 30))?;
    channel.deliver(30, &mut table);
    assert!(matches!(
        cursor.next(0, &table),
        Some(interval) if interval.count == 20 && interval.exposure_micros == 20
    ));
    channel.send(snapshot(4, 40))?;
    channel.deliver(40, &mut table);
    assert_eq!(cursor.next(0, &table), None);
    Ok(())
}

#[test]
fn closed_loop_snapshot_loss_preserves_cumulative_arrival_evidence() -> Result<(), TestError> {
    let complete = run_reported_arrivals(
        FaultPattern {
            drop_every: 0,
            duplicate_every: 0,
            delay_micros: 0,
            odd_sequence_delay_micros: 0,
        },
        None,
    )?;
    let lost = run_reported_arrivals(
        FaultPattern {
            drop_every: 2,
            duplicate_every: 0,
            delay_micros: 0,
            odd_sequence_delay_micros: 0,
        },
        None,
    )?;
    let Some(complete_final) = complete.sample(2) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(lost_final) = lost.sample(2) else {
        return Err(TestError::MissingControllerSample);
    };

    let scale = lost_final
        .arrival_rate_per_second
        .abs()
        .max(complete_final.arrival_rate_per_second.abs())
        .max(1.0_f64);
    assert!(
        (lost_final.arrival_rate_per_second - complete_final.arrival_rate_per_second).abs()
            <= scale * 1.0e-9_f64,
        "cumulative evidence must preserve the arrival posterior"
    );
    Ok(())
}

#[test]
fn missing_reporter_matches_an_equivalent_transport_drop() -> Result<(), TestError> {
    let no_fault = FaultPattern {
        drop_every: 0,
        duplicate_every: 0,
        delay_micros: 0,
        odd_sequence_delay_micros: 0,
    };
    let dropped = run_reported_arrivals(
        FaultPattern {
            drop_every: 2,
            ..no_fault
        },
        None,
    )?;
    let missing = run_reported_arrivals(no_fault, Some((1, ReporterDirective::Missing)))?;
    let Some(dropped_during_gap) = dropped.sample(1) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(missing_during_gap) = missing.sample(1) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(dropped_final) = dropped.sample(2) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(missing_final) = missing.sample(2) else {
        return Err(TestError::MissingControllerSample);
    };

    assert_eq!(
        missing_during_gap.arrival_rate_per_second.to_bits(),
        dropped_during_gap.arrival_rate_per_second.to_bits()
    );
    assert_eq!(missing_during_gap.reporter, ReporterDirective::Missing);
    assert_eq!(
        missing_final.arrival_rate_per_second.to_bits(),
        dropped_final.arrival_rate_per_second.to_bits()
    );
    Ok(())
}

#[test]
fn aggregator_replacement_starts_from_the_proper_prior() -> Result<(), TestError> {
    let fault = FaultPattern {
        drop_every: 0,
        duplicate_every: 0,
        delay_micros: 0,
        odd_sequence_delay_micros: 0,
    };
    let replaced = run_reported_arrivals(fault, Some((2, ReporterDirective::ReplaceAggregator)))?;
    let Some(initial) = replaced.sample(0) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(before_replacement) = replaced.sample(1) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(after_replacement) = replaced.sample(2) else {
        return Err(TestError::MissingControllerSample);
    };

    assert_ne!(
        before_replacement.arrival_rate_per_second.to_bits(),
        initial.arrival_rate_per_second.to_bits()
    );
    assert_eq!(
        after_replacement.arrival_rate_per_second.to_bits(),
        initial.arrival_rate_per_second.to_bits()
    );
    Ok(())
}

#[test]
fn reordered_and_duplicate_snapshots_cannot_regress_state() -> Result<(), TestError> {
    let mut channel = SnapshotChannel::new(
        8,
        FaultPattern {
            drop_every: 0,
            duplicate_every: 1,
            delay_micros: 0,
            odd_sequence_delay_micros: 100,
        },
    )?;
    let mut table = SnapshotTable::new(1)?;
    channel.send(snapshot(1, 10))?;
    channel.send(snapshot(2, 20))?;
    channel.deliver(50, &mut table);
    channel.deliver(200, &mut table);
    let reporter = table.reporter(0);
    assert!(matches!(reporter, Some(state) if state.sequence == 2 && state.arrival_count == 20));
    Ok(())
}

#[test]
fn timer_releases_do_not_enter_message_arrival_evidence() -> Result<(), TestError> {
    let messages_only = source_arrival_count(10, 0)?;
    let messages_and_timers = source_arrival_count(10, 10)?;
    let timers_only = source_arrival_count(0, 10)?;

    assert_eq!(messages_only, 10);
    assert_eq!(messages_and_timers, messages_only);
    assert_eq!(timers_only, 0);
    Ok(())
}

fn source_arrival_count(messages: u32, timers: u32) -> Result<u32, TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        calendar_segment_count_max: 4,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        report_interval_micros: 10_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: test_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    };
    let capacity_grid = CapacityGrid::new(&[0.001_f64], &[1_000.0_f64], &[0.0_f64])?;
    let closed_loop = ClosedLoop::new(
        SourceArrivalWorkload { messages, timers },
        &controller_configuration,
        capacity_grid,
        2,
    )?;
    let plant_configuration = PlantConfiguration::new(4, 40, 80, 8, 2, 8)?;
    let mut harness = SimulationHarness::new(plant_configuration, 2, 2, closed_loop)?;
    harness.tick(0)?;
    harness.tick(10_000)?;
    let sample = harness
        .graph()
        .trace()
        .sample(1)
        .ok_or(TestError::MissingControllerSample)?;
    Ok(match sample.arrival_evidence {
        ArrivalEvidenceSample::Accepted(window) => window.count,
        ArrivalEvidenceSample::None => 0,
    })
}

fn run_reported_arrivals(
    fault: FaultPattern,
    reporter_tick: Option<(u32, ReporterDirective)>,
) -> Result<ControllerTrace, TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        calendar_segment_count_max: 4,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        report_interval_micros: 10_000,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: test_arrival_prior()?,
        capacity_change_rate_per_second: 1.0_f64 / 86_400.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01, 3.0_f64)?,
    };
    let capacity_grid = CapacityGrid::new(&[0.001_f64], &[1_000.0_f64], &[0.0_f64, 1.0_f64])?;
    let closed_loop = ClosedLoop::new(
        ReportedArrivalWorkload { reporter_tick },
        &controller_configuration,
        capacity_grid,
        3,
    )?
    .with_snapshot_transport(4, fault)?;
    let plant_configuration = PlantConfiguration::new(4, 40, 40, 3, 2, 8)?;
    let mut harness = SimulationHarness::new(plant_configuration, 2, 3, closed_loop)?;
    for tick in 0_u64..3 {
        harness.tick(tick * 10_000)?;
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    Ok(closed_loop.into_trace())
}

#[quickcheck]
fn arbitrary_trace_replays_exactly(input: EventTrace) -> bool {
    let EventTrace(events) = input;
    let first = run_events(&events);
    let replay = run_events(&events);
    matches!((first, replay), (Ok(first), Ok(replay)) if first == replay)
}

fn scenario() -> Result<Plant, TestError> {
    let mut plant = Plant::new(configuration()?, 2)?;
    for key in 0_u32..4 {
        plant.add_event(event(u64::from(key) * 1_000, key, 5_000))?;
    }
    Ok(plant)
}

fn configuration() -> Result<PlantConfiguration, PlantError> {
    PlantConfiguration::new(4, 4, 16, 4, 4, 2)
}

#[test]
fn service_draw_identity_uses_every_coordinate() {
    let reference = super::exponential_duration_micros(7, 11, 3, 13, 1_000_000);

    assert_eq!(
        reference,
        super::exponential_duration_micros(7, 11, 3, 13, 1_000_000)
    );
    assert_ne!(
        reference,
        super::exponential_duration_micros(8, 11, 3, 13, 1_000_000)
    );
    assert_ne!(
        reference,
        super::exponential_duration_micros(7, 12, 3, 13, 1_000_000)
    );
    assert_ne!(
        reference,
        super::exponential_duration_micros(7, 11, 4, 13, 1_000_000)
    );
    assert_ne!(
        reference,
        super::exponential_duration_micros(7, 11, 3, 14, 1_000_000)
    );
}

#[test]
fn service_draws_have_exponential_mean_and_dispersion() {
    const DRAW_COUNT: u32 = 100_000;
    const MEAN_MICROS: u64 = 1_000_000;
    let (sum, squared_sum) = (0..DRAW_COUNT).fold((0.0_f64, 0.0_f64), |state, event| {
        let draw = super::u64_to_f64(super::exponential_duration_micros(
            19,
            event,
            1,
            23,
            MEAN_MICROS,
        ));
        (state.0 + draw, state.1 + draw * draw)
    });
    let sample_mean = sum / f64::from(DRAW_COUNT);
    let variance = squared_sum / f64::from(DRAW_COUNT) - sample_mean * sample_mean;
    let coefficient_of_variation = variance.sqrt() / sample_mean;

    assert!((sample_mean / super::u64_to_f64(MEAN_MICROS) - 1.0_f64).abs() < 0.01_f64);
    assert!((coefficient_of_variation - 1.0_f64).abs() < 0.02_f64);
}

fn event(release_micros: u64, key: u32, handler_micros: u64) -> EventSpec {
    EventSpec {
        release_micros,
        partition: key,
        key,
        handler_micros,
        dependency_operations: 1,
        outcome: EventOutcome::Final(FinalOutcome::Success),
        source: EventSource::Message,
    }
}

fn snapshot(sequence: u64, arrival_count: u64) -> Snapshot {
    Snapshot {
        sender: 0,
        incarnation: 7,
        sequence,
        observed_at_micros: sequence.saturating_mul(10),
        arrival_count,
    }
}

fn run_events(events: &[EventSpec]) -> Result<crate::SimulationResult, PlantError> {
    let configuration = PlantConfiguration::new(8, 8, 64, 4, 4, 2)?;
    let mut plant = Plant::new(configuration, 2)?;
    for &event in events {
        plant.add_event(event)?;
    }
    Ok(plant.run())
}

#[derive(Clone, Debug)]
struct EventTrace(Vec<EventSpec>);

impl Arbitrary for EventTrace {
    fn arbitrary(generator: &mut Gen) -> Self {
        let count = usize::arbitrary(generator) % 64 + 1;
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let final_outcome = if bool::arbitrary(generator) {
                FinalOutcome::PermanentFailure
            } else {
                FinalOutcome::Success
            };
            let retry_count = u8::arbitrary(generator) % 3;
            let Ok(outcome) = EventOutcome::from_transient_failures(retry_count, final_outcome)
            else {
                return Self(events);
            };
            events.push(EventSpec {
                release_micros: u64::arbitrary(generator) % 1_000_000,
                partition: u32::arbitrary(generator) % 8,
                key: u32::arbitrary(generator) % 8,
                handler_micros: u64::arbitrary(generator) % 100_000,
                dependency_operations: u32::arbitrary(generator) % 8,
                outcome,
                source: if bool::arbitrary(generator) {
                    EventSource::Timer
                } else {
                    EventSource::Message
                },
            });
        }
        Self(events)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let mut shrunk = Vec::new();
        if self.0.len() > 1 {
            shrunk.push(Self(self.0[..self.0.len() / 2].to_vec()));
            shrunk.push(Self(self.0[1..].to_vec()));
        }
        Box::new(shrunk.into_iter())
    }
}

#[derive(Debug, Error)]
enum TestError {
    #[error(transparent)]
    LeadTimePrior(#[from] prosody_scale_core::LeadTimePriorError),
    #[error(transparent)]
    ArrivalPrior(#[from] ArrivalPriorError),
    #[error(transparent)]
    Batch(#[from] crate::BatchSloError),
    #[error(transparent)]
    CapacityGrid(#[from] prosody_scale_core::CapacityGridError),
    #[error(transparent)]
    ClosedLoop(#[from] ClosedLoopError),
    #[error(transparent)]
    Configuration(#[from] prosody_scale_core::ConfigurationError),
    #[error(transparent)]
    Input(#[from] crate::InputError),
    #[error(transparent)]
    Plant(#[from] PlantError),
    #[error(transparent)]
    Principal(#[from] PrincipalRunError),
    #[error(transparent)]
    RegimeValidation(#[from] RegimeValidationError),
    #[error("the closed loop did not record a controller sample")]
    MissingControllerSample,
    #[error("the closed loop did not record a capacity window")]
    MissingCapacityWindow,
    #[error("the plant did not change its replica count")]
    MissingScaleChange,
    #[error("the plant did not produce a ready interval with pause time")]
    MissingPauseWindow,
    #[error("the plant did not complete reconciliation")]
    MissingReconciliationCompletion,
    #[error("the plant did not settle an expected event")]
    MissingSettlement,
}
