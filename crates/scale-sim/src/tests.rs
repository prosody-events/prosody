use prosody_scale_core::{
    CapacityGrid, Configuration as ControllerConfiguration, RandomStream, ServiceObjective,
};
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rayon::prelude::*;
use thiserror::Error;

use crate::series::{
    OutputFunction, SeriesCell, SeriesContext, SeriesFunction, SeriesHistory, SeriesKey,
    SeriesMetadata, SeriesRole, SeriesUnit, series_graph, series_graph_is_acyclic,
};
use crate::{
    AttemptContext, AttemptFrame, AttemptGenerator, AttemptModel, AttemptParameters, ClosedLoop,
    ClosedLoopError, ConcurrencyLatencyCurve, EventSpec, FaultPattern, HistoricalAttemptModel,
    Kip848Rebalance, Plant, PlantConfiguration, PlantError, PrincipalRegime, PrincipalRunError,
    QuantileTable, RegimeExperiment, RegimeValidationError, ReporterDirective, RunStopReason,
    ScaleChange, ScaleDirective, ScaleRequest, SimulationHarness, Snapshot, SnapshotChannel,
    SnapshotCursor, SnapshotTable, StepSeries, TickContext, TickGenerator, TickInputs,
    WorkloadSeries, run_batch_regime, run_batch_slo, run_capacity_evidence_regime, run_parallel,
    run_principal_regime, validate_principal_regime,
};
use crate::{CapacityEvidenceKind, CapacityEvidenceSample};

const PRINCIPAL_EVENT_COUNT: usize = 2_000;

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
    incremental.write_partition_backlog(1_500, &mut partition_backlog)?;
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

    assert_eq!(trace.handler_elapsed_p99_micros[0], 110);
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
    assert_eq!(paused.paused_partitions, 2);
    assert_eq!(paused.reconciliation_started_micros, Some(10));
    assert_eq!(ready.reconciliation_completed_micros, Some(100));
    assert_eq!(result.settlements()[0].settle_micros, 12);
    assert_eq!(result.settlements()[1].settle_micros, 101);
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

    let result = plant.run();

    assert_eq!(result.settlements()[0].settle_micros, 200);
    assert_eq!(result.settlements()[1].settle_micros, 201);
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

    assert_eq!(
        before
            .settlements()
            .iter()
            .map(|settlement| settlement.settle_micros)
            .max(),
        Some(201)
    );
    assert!(
        after
            .settlements()
            .iter()
            .all(|settlement| settlement.settle_micros == 101)
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
fn uncertain_late_capacity_does_not_change_the_target() -> Result<(), TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01)?,
    };
    let capacity_grid = CapacityGrid::new(&[0.04_f64], &[1_000_000.0_f64], &[0.0_f64, 1.0_f64])?;
    let closed_loop = ClosedLoop::new(
        ClosedLoopWorkload { emitted: false },
        &controller_configuration,
        capacity_grid,
        4,
    )?;
    let plant_configuration = PlantConfiguration::new(4, 100, 100, 4, 2, 16)?.with_rebalance(0, 0);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 4, closed_loop)?;

    let initial = harness.tick(0)?;
    let ready = harness.tick(30_000)?;
    let (_result, closed_loop) = harness.finish_with_graph();
    let Some(decision) = closed_loop.trace().sample(0) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(after_actuation) = closed_loop.trace().sample(1) else {
        return Err(TestError::MissingControllerSample);
    };

    assert_eq!(initial.replicas, 1);
    assert_eq!(decision.target, 1);
    assert_eq!(decision.cap, 8);
    assert!(!decision.hold);
    assert_eq!(ready.replicas, 1);
    assert_eq!(
        after_actuation.lead_time_evidence,
        crate::LeadTimeEvidenceSample::None
    );
    Ok(())
}

#[test]
fn closed_loop_emits_passive_resource_windows() -> Result<(), TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01)?,
    };
    let capacity_grid = CapacityGrid::new(
        &[0.005_f64, 0.01_f64],
        &[200.0_f64, 400.0_f64],
        &[0.0_f64, 1.0_f64],
    )?;
    let closed_loop = ClosedLoop::new(
        CapacityWorkload,
        &controller_configuration,
        capacity_grid,
        8,
    )?;
    let plant_configuration = PlantConfiguration::new(4, 100, 200, 8, 2, 16)?.with_rebalance(0, 0);
    let mut harness = SimulationHarness::new(plant_configuration, 1, 8, closed_loop)?;

    for tick in 0_u64..8 {
        let _snapshot = harness.tick(tick * 10_000)?;
    }
    let (_result, closed_loop) = harness.finish_with_graph();
    let kinds = (0..closed_loop.trace().len())
        .filter_map(|index| closed_loop.trace().sample(index))
        .map(|sample| sample.capacity_evidence.kind())
        .collect::<Vec<_>>();

    assert!(kinds.contains(&CapacityEvidenceKind::Window));
    Ok(())
}

#[test]
fn retargeted_actuation_emits_right_censored_lead_time() -> Result<(), TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01)?,
    };
    let capacity_grid = CapacityGrid::new(&[0.001_f64], &[1_000.0_f64], &[0.0_f64, 1.0_f64])?;
    let closed_loop = ClosedLoop::new(
        RetargetWorkload,
        &controller_configuration,
        capacity_grid,
        4,
    )?;
    let plant_configuration = PlantConfiguration::new(4, 16, 16, 4, 2, 8)?;
    let mut harness = SimulationHarness::new(plant_configuration, 1, 4, closed_loop)?;

    let _first = harness.tick(0)?;
    let _retarget = harness.tick(10_000_000)?;
    let _evidence = harness.tick(20_000_000)?;
    let (_result, closed_loop) = harness.finish_with_graph();
    let Some(before) = closed_loop.trace().sample(0) else {
        return Err(TestError::MissingControllerSample);
    };
    let Some(after) = closed_loop.trace().sample(2) else {
        return Err(TestError::MissingControllerSample);
    };

    assert!(after.lead_time_up_seconds > before.lead_time_up_seconds);
    Ok(())
}

struct RetargetWorkload;

impl TickGenerator for RetargetWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 1,
            timer_count: 0,
            handler_micros: 1_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            transient_failures: 0,
            permanent_rejection_every: 0,
            launch_delay_micros: 100_000_000,
            scale: ScaleDirective::Request {
                replicas: if context.tick_index == 0 { 2 } else { 3 },
                delay_micros: 100_000_000,
            },
        })
    }
}

struct CapacityWorkload;

impl TickGenerator for CapacityWorkload {
    fn calculate(&mut self, context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 20,
            timer_count: 0,
            handler_micros: 10_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            transient_failures: 0,
            permanent_rejection_every: 0,
            launch_delay_micros: 0,
            scale: ScaleDirective::Request {
                replicas: u32::from(context.tick_index >= 3) + 1,
                delay_micros: 0,
            },
        })
    }
}

struct ClosedLoopWorkload {
    emitted: bool,
}

struct ReportedArrivalWorkload {
    reporter_tick: Option<(u32, ReporterDirective)>,
}

impl TickGenerator for ReportedArrivalWorkload {
    fn calculate(&mut self, _context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        Ok(TickInputs {
            message_count: 10,
            timer_count: 0,
            handler_micros: 1,
            dependency_operations: 0,
            dependency_operation_micros: 0,
            handler_added_micros: 0,
            transient_failures: 0,
            permanent_rejection_every: 0,
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

impl TickGenerator for ClosedLoopWorkload {
    fn calculate(&mut self, _context: TickContext<'_>) -> Result<TickInputs, PlantError> {
        let message_count = if self.emitted { 0 } else { 100 };
        self.emitted = true;
        Ok(TickInputs {
            message_count,
            timer_count: 0,
            handler_micros: 40_000,
            dependency_operations: 1,
            dependency_operation_micros: 1,
            handler_added_micros: 0,
            transient_failures: 0,
            permanent_rejection_every: 0,
            launch_delay_micros: 30_000,
            scale: ScaleDirective::Hold,
        })
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
            transient_failures: 0,
            permanent_rejection_every: 0,
            launch_delay_micros: 30_000,
            scale: if context.frame.tick_index == 0 {
                ScaleDirective::Request {
                    replicas: 4,
                    delay_micros: 30_000,
                }
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
        _context: SeriesContext<'_, TickContext<'_>>,
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
    let application = run_principal_regime(PrincipalRegime::ApplicationLimited)?;
    let hot_key = run_principal_regime(PrincipalRegime::HotSerializedKey)?;
    let timers = run_principal_regime(PrincipalRegime::TimerWave)?;
    let transient = run_principal_regime(PrincipalRegime::TransientFailures)?;
    let rejected = run_principal_regime(PrincipalRegime::PermanentRejections)?;
    let rebalance = run_principal_regime(PrincipalRegime::RebalanceStorm)?;
    let contention = run_principal_regime(PrincipalRegime::HandlerContention)?;

    assert_eq!(
        application.inputs().names().collect::<Vec<_>>(),
        vec![
            "message_count",
            "timer_count",
            "message_emitted",
            "timer_emitted",
            "historical_message_count",
            "historical_emitted",
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
    assert!(
        timers
            .settlements()
            .iter()
            .all(|settlement| settlement.release_micros == 1_000_000)
    );
    assert_eq!(attempt_count(&transient), 2_400);
    assert_eq!(
        rejected
            .settlements()
            .iter()
            .filter(|settlement| settlement.permanent_rejection)
            .count(),
        200
    );
    assert!(final_settle(&rebalance) > final_settle(&application));
    assert!(maximum_handler_time(&contention) > maximum_handler_time(&application));
    Ok(())
}

#[test]
fn extended_principal_regimes_enforce_their_physical_invariants() -> Result<(), TestError> {
    let idle = run_principal_regime(PrincipalRegime::Idle)?;
    let burst = run_principal_regime(PrincipalRegime::ShortBurst)?;
    let seasonal = run_principal_regime(PrincipalRegime::SeasonalWaves)?;
    let hot_partition = run_principal_regime(PrincipalRegime::HotPartition)?;
    let loose = run_principal_regime(PrincipalRegime::LooseBudgetBacklog)?;
    let faults = run_principal_regime(PrincipalRegime::SnapshotFaults)?;
    let faults_replay = run_principal_regime(PrincipalRegime::SnapshotFaults)?;
    let missing = run_principal_regime(PrincipalRegime::MissingReporter)?;
    let replacement = run_principal_regime(PrincipalRegime::AggregatorReplacement)?;

    assert!(idle.settlements().is_empty());
    assert!(burst.settlements().iter().all(|settlement| {
        settlement
            .settle_micros
            .saturating_sub(settlement.release_micros)
            < 30_000_000
    }));
    let mut release_times = seasonal
        .events()
        .iter()
        .map(|event| event.release_micros)
        .collect::<Vec<_>>();
    release_times.dedup();
    assert_eq!(release_times, [0, 500_000, 1_000_000, 1_500_000]);
    assert!(
        hot_partition
            .events()
            .iter()
            .all(|event| event.partition == 0)
    );
    assert!(loose.settlements().iter().all(|settlement| {
        settlement
            .settle_micros
            .saturating_sub(settlement.release_micros)
            <= 60_000_000
    }));
    assert_eq!(faults.settlements().len(), PRINCIPAL_EVENT_COUNT);
    assert_eq!(faults.settlements(), faults_replay.settlements());
    assert_eq!(missing.settlements().len(), PRINCIPAL_EVENT_COUNT);
    assert_eq!(replacement.settlements().len(), PRINCIPAL_EVENT_COUNT);
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
    assert!(targets.contains(&8));
    Ok(())
}

#[test]
fn capacity_regimes_record_passive_resource_windows() -> Result<(), TestError> {
    for regime in [
        PrincipalRegime::LinearThroughput,
        PrincipalRegime::FlatPostKnee,
        PrincipalRegime::DecliningPostKnee,
    ] {
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
                assert!(window.useful_completions > 0);
                assert!(window.concurrency > 0.0_f64);
                assert!(window.throughput_per_second().is_finite());
            }
        }
        assert!(recorded_windows > 0);
        let capacity_values = run.controller().capacity_posterior_values();
        assert_eq!(capacity_values.len(), 64);
        assert_eq!(capacity_values.first(), Some(&20.0_f64));
        assert_eq!(capacity_values.last(), Some(&1_280.0_f64));
        for index in 0..run.controller().len() {
            let posterior = run
                .controller()
                .capacity_posterior(index)
                .ok_or(TestError::MissingControllerSample)?;
            let total = posterior.iter().sum::<f64>();
            assert!(total <= f64::EPSILON || (total - 1.0_f64).abs() < 1.0e-9_f64);
        }
        let prior = run
            .controller()
            .capacity_posterior(0)
            .ok_or(TestError::MissingControllerSample)?;
        let final_posterior = run
            .controller()
            .capacity_posterior(run.controller().len() - 1)
            .ok_or(TestError::MissingControllerSample)?;
        assert!(
            prior
                .iter()
                .zip(final_posterior)
                .any(|(before, after)| (before - after).abs() > 1.0e-12_f64)
        );
    }
    Ok(())
}

#[test]
fn every_closed_loop_regime_satisfies_its_declared_stimulus() -> Result<(), TestError> {
    PrincipalRegime::ALL.par_iter().try_for_each(|&regime| {
        let run = run_principal_regime(regime)?;
        validate_principal_regime(regime, RegimeExperiment::ClosedLoop, &run)?;
        Ok(())
    })
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
    let run = run_principal_regime(PrincipalRegime::HotPartition)?;
    let trace = run.metric_trace(run.metric_window_micros(), 5_000_000)?;
    assert!(trace.backlog.iter().copied().max().unwrap_or_default() > 0);
    assert!(
        trace
            .replicas
            .iter()
            .zip(&trace.backlog)
            .any(|(&replicas, &backlog)| replicas == 1 && backlog > 10_000)
    );
    assert!((0..run.controller().len()).any(|index| {
        let Some(sample) = run.controller().sample(index) else {
            return false;
        };
        let Some(losses) = run.controller().decision_expected_losses(index) else {
            return false;
        };
        let Some((&one_replica, &maximum_replicas)) = losses.first().zip(losses.last()) else {
            return false;
        };
        !sample.hold
            && matches!(sample.capacity_evidence, CapacityEvidenceSample::Window(_))
            && sample.target == 1
            && one_replica > 0.0_f64
            && (one_replica - maximum_replicas).abs() <= 1.0e-9_f64
    }));
    assert_eq!(trace.target.iter().copied().max().unwrap_or_default(), 1);
    Ok(())
}

#[test]
fn lead_time_diagnostics_use_prequential_predictive_distributions() -> Result<(), TestError> {
    let run = run_principal_regime(PrincipalRegime::LinearThroughput)?;
    let mut completed = 0_u32;
    for index in 0..run.controller().len() {
        let sample = run
            .controller()
            .sample(index)
            .ok_or(TestError::MissingControllerSample)?;
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
    assert!(completed > 0, "the regime must complete a scale transition");
    Ok(())
}

#[test]
fn linear_closed_loop_uses_only_controller_scale_targets() -> Result<(), TestError> {
    let run = run_principal_regime(PrincipalRegime::LinearThroughput)?;

    assert_eq!(run.stop().reason, RunStopReason::DurationComplete);
    for row in 0..run.inputs().len() {
        assert_eq!(
            run.inputs().cell("external_target", row),
            Some(SeriesCell::Unsigned32(0))
        );
    }
    Ok(())
}

#[test]
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
        assert_eq!(sample.capacity_median_per_second, expected);
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
fn looser_batch_budget_reduces_the_replica_target() -> Result<(), TestError> {
    let short = run_batch_slo(6 * 60 * 60 * 1_000_000, 0.05)?;
    let medium = run_batch_slo(12 * 60 * 60 * 1_000_000, 0.05)?;
    let long = run_batch_slo(24 * 60 * 60 * 1_000_000, 0.05)?;

    assert!(short.target > medium.target);
    assert!(medium.target >= long.target);
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

fn attempt_count(result: &crate::SimulationResult) -> u32 {
    result
        .settlements()
        .iter()
        .map(|settlement| settlement.attempts)
        .sum()
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
    let one_replica = one_replica.run();
    let four_replicas = four_replicas.run();
    assert_eq!(one_replica.settlements()[0].settle_micros, 11_000);
    assert_eq!(one_replica.settlements()[1].settle_micros, 22_000);
    assert_eq!(four_replicas.settlements(), one_replica.settlements());
    Ok(())
}

#[test]
fn independent_keys_use_parallel_slots() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.add_event(event(0, 0, 10_000))?;
    plant.add_event(event(0, 1, 10_000))?;
    let result = plant.run();
    assert_eq!(result.settlements()[0].settle_micros, 11_000);
    assert_eq!(result.settlements()[1].settle_micros, 11_000);
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
fn every_controller_sample_preserves_target_and_cap() -> Result<(), TestError> {
    let run = run_capacity_evidence_regime(PrincipalRegime::DecliningPostKnee)?;
    for index in 0..run.controller().len() {
        let Some(sample) = run.controller().sample(index) else {
            return Err(TestError::MissingControllerSample);
        };
        assert!(sample.target > 0);
        assert!(sample.cap > 0);
        assert!(sample.target <= sample.cap);
    }
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
    plant.add_event(event(60_000, 1, 10_000))?;
    let result = plant.run();
    assert_eq!(result.settlements()[0].settle_micros, 21_000);
    assert_eq!(result.settlements()[1].settle_micros, 266_000);
    Ok(())
}

#[test]
fn latest_pending_scale_target_replaces_older_target() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 100_000,
        replicas: 2,
    })?;
    plant.replace_scale_target(ScaleChange {
        at_micros: 200_000,
        replicas: 3,
    })?;

    assert_eq!(plant.advance_until(150_000).replicas, 1);
    assert_eq!(plant.advance_until(200_000).replicas, 3);
    Ok(())
}

#[test]
fn transient_failures_consume_attempts_and_backoff() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let mut retried = event(0, 0, 2_000);
    retried.transient_failures = 2;
    plant.add_event(retried)?;
    let result = plant.run();
    assert_eq!(result.settlements()[0].attempts, 3);
    assert_eq!(result.settlements()[0].settle_micros, 29_000);
    Ok(())
}

#[test]
fn permanent_rejection_settles_without_retry() -> Result<(), TestError> {
    let mut plant = Plant::new(configuration()?, 1)?;
    let mut rejected = event(0, 0, 2_000);
    rejected.permanent_rejection = true;
    plant.add_event(rejected)?;
    let result = plant.run();
    assert_eq!(result.settlements()[0].attempts, 1);
    assert!(result.settlements()[0].permanent_rejection);
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

    assert_eq!(
        lost_final.arrival_rate_per_second.to_bits(),
        complete_final.arrival_rate_per_second.to_bits()
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

fn run_reported_arrivals(
    fault: FaultPattern,
    reporter_tick: Option<(u32, ReporterDirective)>,
) -> Result<crate::ControllerTrace, TestError> {
    let controller_configuration = ControllerConfiguration {
        cohort_count_max: 4,
        partition_count: 4,
        replica_count_max: 8,
        slots_per_replica: 2,
        posterior_sample_count: 64,
        objective: ServiceObjective::new(1_000_000, 0.01)?,
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
        let _snapshot = harness.tick(tick * 10_000)?;
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

fn event(release_micros: u64, key: u32, handler_micros: u64) -> EventSpec {
    EventSpec {
        release_micros,
        partition: key,
        key,
        handler_micros,
        dependency_operations: 1,
        transient_failures: 0,
        permanent_rejection: false,
        timer: false,
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
            events.push(EventSpec {
                release_micros: u64::arbitrary(generator) % 1_000_000,
                partition: u32::arbitrary(generator) % 8,
                key: u32::arbitrary(generator) % 8,
                handler_micros: u64::arbitrary(generator) % 100_000,
                dependency_operations: u32::arbitrary(generator) % 8,
                transient_failures: u8::arbitrary(generator) % 4,
                permanent_rejection: bool::arbitrary(generator),
                timer: bool::arbitrary(generator),
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
}
