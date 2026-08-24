//! Cold construction and steady-state transition benchmarks.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use prosody_scale_core::{
    ArrivalPrior, ArrivalPriorError, CapacityGrid, CapacityGridError, Cohort, Configuration,
    ConfigurationError, DemandClass, LaunchPrior, LeadTimePriorError, ModelTime, ObservationBuffer,
    ObservationError, OccupancyTransition, RebalancePrior, ReliabilityPrior, ResourceWindow,
    ScaleScratch, ScaleState, ServiceObjective, step,
};
use std::fmt::Display;
use std::hint::black_box;
use std::io::{self, Write};
use std::process::exit;
use std::time::Instant;
use thiserror::Error;

const REPORT_INTERVAL_MICROS: u64 = 1_000_000;
const SMALL: BenchmarkCase = BenchmarkCase {
    name: "low_volume_synthetic",
    replica_count_max: 2,
    offered_events_per_second: 1_000,
};
const TYPICAL: BenchmarkCase = BenchmarkCase {
    name: "service_reference_synthetic",
    replica_count_max: 8,
    offered_events_per_second: 50_000,
};
const MAXIMUM: BenchmarkCase = BenchmarkCase {
    name: "high_volume_synthetic",
    replica_count_max: 48,
    offered_events_per_second: 250_000,
};
const CASES: [BenchmarkCase; 3] = [SMALL, TYPICAL, MAXIMUM];

#[derive(Clone, Copy)]
struct BenchmarkCase {
    name: &'static str,
    replica_count_max: u32,
    offered_events_per_second: u32,
}

fn benchmarks(criterion: &mut Criterion) {
    construction(criterion);
    steady_state(criterion);
    staggered_cohort_step(criterion);
    rayon_worker_step(criterion);
    capacity_grid(criterion);
    capacity_convolution(criterion);
    capacity_rate_magnitude(criterion);
    resource_grid_step(criterion);
    posterior_sample_count_step(criterion);
}

fn or_exit<T, E: Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            drop(writeln!(io::stderr(), "benchmark setup failed: {error}"));
            let terminate: fn(i32) -> ! = exit;
            terminate(1);
        }
    }
}

fn capacity_convolution(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(SMALL));
    let grid = or_exit(CapacityGrid::new(&[0.5_f64], &[10.0_f64], &[0.0_f64]));
    let mut state = or_exit(ScaleState::new(configuration.clone(), grid));
    let mut scratch = or_exit(state.new_scratch());
    let mut observation = or_exit(ObservationBuffer::new(&configuration));
    let mut now = 1_u64;
    let transitions = constant_transitions(64);
    criterion.bench_function("capacity_convolution/64_starts", |bencher| {
        bencher.iter(|| {
            let window = or_exit(ResourceWindow::new_with_starts(1.0_f64, 1.0_f64, 64, 64));
            or_exit(observation.set_resource_observation(window, 1, 1, &transitions));
            or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
            let decision = step(&mut state, &mut scratch, observation.observation());
            now = now.wrapping_add(REPORT_INTERVAL_MICROS);
            black_box(decision);
        });
    });
}

fn capacity_rate_magnitude(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(SMALL));
    let mut group = criterion.benchmark_group("capacity_rate_magnitude");
    for (name, service_time) in [("low", 10.0_f64), ("high", 0.001_f64)] {
        let grid = or_exit(CapacityGrid::new(&[service_time], &[100.0_f64], &[0.0_f64]));
        let mut state = or_exit(ScaleState::new(configuration.clone(), grid));
        let mut scratch = or_exit(state.new_scratch());
        let mut observation = or_exit(ObservationBuffer::new(&configuration));
        let transitions = [OccupancyTransition::new(500_000, 1, 1)];
        let mut now = 1_u64;
        group.bench_function(name, |bencher| {
            bencher.iter(|| {
                let window = or_exit(ResourceWindow::new_with_starts(32.0_f64, 1.0_f64, 1, 1));
                or_exit(observation.set_resource_observation(window, 32, 32, &transitions));
                or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                black_box(step(&mut state, &mut scratch, observation.observation()));
                now = now.wrapping_add(REPORT_INTERVAL_MICROS);
            });
        });
    }
    group.finish();
}

fn staggered_cohort_step(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(TYPICAL));
    let capacity_grid = or_exit(grid(TYPICAL));
    let mut state = or_exit(ScaleState::new(configuration.clone(), capacity_grid));
    let mut scratch = or_exit(state.new_scratch());
    let mut observation = or_exit(ObservationBuffer::new(&configuration));
    or_exit(populate_staggered(&mut observation, TYPICAL));
    let mut now = 1_u64;
    criterion.bench_function("step/staggered/service_reference_synthetic", |bencher| {
        bencher.iter(|| {
            or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
            let decision = step(&mut state, &mut scratch, observation.observation());
            now = now.wrapping_add(REPORT_INTERVAL_MICROS);
            black_box(decision);
        });
    });
}

fn rayon_worker_step(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(TYPICAL));
    let capacity_grid = or_exit(grid(TYPICAL));
    let mut state = or_exit(ScaleState::new(configuration.clone(), capacity_grid));
    let mut scratch = or_exit(state.new_scratch());
    let mut observation = or_exit(ObservationBuffer::new(&configuration));
    let pool = or_exit(rayon::ThreadPoolBuilder::new().build());
    or_exit(populate(&mut observation, TYPICAL));
    let mut now = 1_u64;
    criterion.bench_function("step/rayon_worker/service_reference_synthetic", |bencher| {
        bencher.iter_custom(|iterations| {
            pool.install(|| {
                let started = Instant::now();
                for _ in 0..iterations {
                    or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                    let decision = step(&mut state, &mut scratch, observation.observation());
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                }
                started.elapsed()
            })
        });
    });
}

fn posterior_sample_count_step(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("posterior_sample_count_step");
    for sample_count in [64_u32, 1_024_u32, 16_384_u32] {
        let mut configuration = or_exit(configuration(TYPICAL));
        configuration.posterior_sample_count = sample_count;
        let grid = or_exit(grid(TYPICAL));
        let mut state = or_exit(ScaleState::new(configuration.clone(), grid));
        let mut scratch = or_exit(state.new_scratch());
        let mut observation = or_exit(ObservationBuffer::new(&configuration));
        or_exit(populate(&mut observation, TYPICAL));
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("service_reference_synthetic", sample_count),
            &sample_count,
            |bencher, _sample_count| {
                bencher.iter(|| {
                    or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                    let decision = step(&mut state, &mut scratch, observation.observation());
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                });
            },
        );
    }
    group.finish();
}

fn resource_grid_step(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(TYPICAL));
    let mut group = criterion.benchmark_group("resource_grid_step");
    for cell_count in [64_u32, 1_024_u32] {
        let grid = or_exit(realistic_capacity_grid(TYPICAL, cell_count));
        let mut state = or_exit(ScaleState::new(configuration.clone(), grid));
        let mut scratch = or_exit(state.new_scratch());
        let mut observation = or_exit(ObservationBuffer::new(&configuration));
        or_exit(populate(&mut observation, TYPICAL));
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("service_reference_synthetic", cell_count),
            &cell_count,
            |bencher, _cell_count| {
                bencher.iter(|| {
                    or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                    let decision = step(&mut state, &mut scratch, observation.observation());
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                });
            },
        );
    }
    group.finish();
}

fn construction(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("construction");
    for case in CASES {
        let configuration = or_exit(configuration(case));
        or_exit(construct(&configuration, case));
        group.bench_with_input(
            BenchmarkId::new("cold", case.name),
            &case,
            |bencher, _case| {
                bencher.iter(|| {
                    black_box(or_exit(construct(&configuration, case)));
                });
            },
        );
    }
    group.finish();
}

fn steady_state(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("step");
    for case in CASES {
        let configuration = or_exit(configuration(case));
        let capacity_grid = or_exit(grid(case));
        let mut state = or_exit(ScaleState::new(configuration.clone(), capacity_grid));
        let mut scratch = or_exit(state.new_scratch());
        let mut observation = or_exit(ObservationBuffer::new(&configuration));
        or_exit(populate(&mut observation, case));
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("populated", case.name),
            &case,
            |bencher, _case| {
                bencher.iter(|| {
                    or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                    let decision = step(&mut state, &mut scratch, observation.observation());
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                });
            },
        );
    }
    group.finish();
}

fn capacity_grid(criterion: &mut Criterion) {
    let configuration = or_exit(configuration(SMALL));
    let mut group = criterion.benchmark_group("capacity_grid");
    for (name, cell_count) in [("one", 1_u32), ("medium", 64_u32), ("large", 1_024_u32)] {
        let grid = or_exit(capacity_grid_with_cells(cell_count));
        let mut state = or_exit(ScaleState::new(configuration.clone(), grid));
        let mut scratch = or_exit(state.new_scratch());
        let mut observation = or_exit(ObservationBuffer::new(&configuration));
        let mut now = 1_u64;
        let transitions = constant_transitions(4_000);
        group.bench_function(BenchmarkId::new("stable_update", name), |bencher| {
            bencher.iter(|| {
                let window = or_exit(ResourceWindow::new_with_starts(
                    4.0_f64, 1.0_f64, 4_000, 4_000,
                ));
                or_exit(observation.set_resource_observation(window, 4, 4, &transitions));
                or_exit(observation.advance_model_time(ModelTime::from_micros(now)));
                let decision = step(&mut state, &mut scratch, observation.observation());
                now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                black_box(decision);
            });
        });
    }
    group.finish();
}

fn constant_transitions(count: u32) -> Vec<OccupancyTransition> {
    (0..count)
        .map(|index| {
            OccupancyTransition::new(
                u64::from(index + 1) * REPORT_INTERVAL_MICROS / u64::from(count + 1),
                1,
                1,
            )
        })
        .collect()
}

fn configuration(case: BenchmarkCase) -> Result<Configuration, BenchmarkError> {
    Ok(Configuration {
        cohort_count_max: 64,
        calendar_segment_count_max: 64,
        scheduled_release_count_max: 64,
        readiness_lump_count_max: 64,
        partition_count: 64,
        replica_count_max: case.replica_count_max,
        slots_per_replica: 32,
        posterior_sample_count: 4_096,
        report_interval_micros: REPORT_INTERVAL_MICROS,
        resource_window_attempt_count_max: 100_000,
        resource_window_group_count_max: 256,
        failure_service_weight: 0.3_f64,
        arrival_prior: ArrivalPrior::new(1.0_f64 / 90.0_f64)?,
        capacity_change_rate_per_second: 1.0_f64 / 300.0_f64,
        reliability_prior: ReliabilityPrior::authored()?,
        launch_time_prior: LaunchPrior::kubernetes()?,
        rebalance_time_prior: RebalancePrior::kip848()?,
        objective: ServiceObjective::new(1_000_000, 0.01_f64, 3.0_f64)?,
    })
}

fn construct(
    configuration: &Configuration,
    case: BenchmarkCase,
) -> Result<(ScaleState, ScaleScratch, ObservationBuffer), BenchmarkError> {
    let state = ScaleState::new(configuration.clone(), grid(case)?)?;
    let scratch = state.new_scratch()?;
    let observation = ObservationBuffer::new(configuration)?;
    Ok((state, scratch, observation))
}

fn populate(
    observation: &mut ObservationBuffer,
    case: BenchmarkCase,
) -> Result<(), BenchmarkError> {
    const HORIZON_SECONDS: u32 = 30;
    let event_count = case
        .offered_events_per_second
        .saturating_mul(HORIZON_SECONDS);
    let events_per_cohort = f64::from(event_count) / 64.0_f64;
    for cohort in 0..64 {
        observation.push_cohort(Cohort {
            release_micros: 0,
            deadline_micros: u64::from(HORIZON_SECONDS) * 1_000_000,
            offered_events: events_per_cohort,
            partition: cohort % 64,
            demand_class: DemandClass::Normal,
        })?;
    }
    Ok(())
}

fn populate_staggered(
    observation: &mut ObservationBuffer,
    case: BenchmarkCase,
) -> Result<(), BenchmarkError> {
    const HORIZON_SECONDS: u32 = 30;
    let events_per_cohort =
        f64::from(case.offered_events_per_second) * f64::from(HORIZON_SECONDS) / 64.0_f64;
    for cohort in 0..64 {
        let release_micros = u64::from(cohort) * 100_000;
        observation.push_cohort(Cohort {
            release_micros,
            deadline_micros: release_micros + u64::from(HORIZON_SECONDS) * 1_000_000,
            offered_events: events_per_cohort,
            partition: cohort % 64,
            demand_class: DemandClass::Normal,
        })?;
    }
    Ok(())
}

fn capacity_grid_with_cells(cell_count: u32) -> Result<CapacityGrid, CapacityGridError> {
    let mut capacities = Vec::with_capacity(cell_count as usize);
    for cell in 0..cell_count {
        capacities.push(1_000.0_f64 + f64::from(cell));
    }
    CapacityGrid::new(&[0.001_f64], &capacities, &[0.0_f64])
}

fn realistic_capacity_grid(
    case: BenchmarkCase,
    cell_count: u32,
) -> Result<CapacityGrid, CapacityGridError> {
    let offered_rate = f64::from(case.offered_events_per_second);
    let denominator = f64::from(cell_count.saturating_sub(1).max(1));
    let mut capacities = Vec::with_capacity(cell_count as usize);
    for cell in 0..cell_count {
        let fraction = f64::from(cell) / denominator;
        capacities.push(offered_rate * (0.5_f64 + 3.5_f64 * fraction));
    }
    CapacityGrid::new(&[0.002_f64], &capacities, &[0.25_f64])
}

fn grid(case: BenchmarkCase) -> Result<CapacityGrid, CapacityGridError> {
    let _ = case;
    CapacityGrid::new(
        &[0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64, 0.008_f64],
        &[32_000.0_f64, 64_000.0_f64, 128_000.0_f64, 256_000.0_f64],
        &[0.0_f64, 0.5_f64, 1.0_f64, 2.0_f64],
    )
}

#[derive(Debug, Error)]
enum BenchmarkError {
    #[error(transparent)]
    LeadTimePrior(#[from] LeadTimePriorError),
    #[error(transparent)]
    ArrivalPrior(#[from] ArrivalPriorError),
    #[error(transparent)]
    CapacityGrid(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

criterion_group!(scale_core, benchmarks);
criterion_main!(scale_core);
