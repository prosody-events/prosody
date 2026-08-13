//! Cold construction and steady-state transition benchmarks.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use prosody_scale_core::{
    CapacityGrid, CapacityGridError, Cohort, Configuration, ConfigurationError, DemandClass,
    ModelTime, ObservationBuffer, ObservationError, ReliabilityPrior, ResourceWindow, ScaleScratch,
    ScaleState, ServiceObjective, TransitionPrior, step,
};
use std::hint::black_box;
use std::time::Instant;
use thiserror::Error;

const REPORT_INTERVAL_MICROS: u64 = 1_000_000;
const SMALL: BenchmarkCase = BenchmarkCase {
    name: "low_volume_synthetic",
    cohort_count_max: 8,
    replica_count_max: 8,
    slots_per_replica: 4,
    posterior_sample_count: 32,
    offered_events_per_second: 1_000,
};
const TYPICAL: BenchmarkCase = BenchmarkCase {
    name: "service_reference_synthetic",
    cohort_count_max: 256,
    replica_count_max: 128,
    slots_per_replica: 16,
    posterior_sample_count: 1_024,
    offered_events_per_second: 50_000,
};
const MAXIMUM: BenchmarkCase = BenchmarkCase {
    name: "high_volume_synthetic",
    cohort_count_max: 1_024,
    replica_count_max: 256,
    slots_per_replica: 64,
    posterior_sample_count: 16_384,
    offered_events_per_second: 250_000,
};
const CASES: [BenchmarkCase; 3] = [SMALL, TYPICAL, MAXIMUM];

#[derive(Clone, Copy)]
struct BenchmarkCase {
    name: &'static str,
    cohort_count_max: u32,
    replica_count_max: u32,
    slots_per_replica: u32,
    posterior_sample_count: u32,
    offered_events_per_second: u32,
}

fn benchmarks(criterion: &mut Criterion) {
    construction(criterion);
    steady_state(criterion);
    staggered_cohort_step(criterion);
    rayon_worker_step(criterion);
    capacity_grid(criterion);
    resource_grid_step(criterion);
    posterior_sample_count_step(criterion);
}

fn staggered_cohort_step(criterion: &mut Criterion) {
    let Ok(configuration) = configuration(TYPICAL) else {
        return;
    };
    let Ok(capacity_grid) = grid(TYPICAL) else {
        return;
    };
    let Ok(mut state) = ScaleState::new(configuration.clone(), capacity_grid) else {
        return;
    };
    let scratch = state.new_scratch();
    let observation = ObservationBuffer::new(&configuration);
    let (Ok(mut scratch), Ok(mut observation)) = (scratch, observation) else {
        return;
    };
    if populate_staggered(&mut observation, TYPICAL).is_err() {
        return;
    }
    let mut now = 1_u64;
    criterion.bench_function("step/staggered/service_reference_synthetic", |bencher| {
        bencher.iter(|| {
            let decision = step(
                &mut state,
                &mut scratch,
                observation.observation(),
                ModelTime::from_micros(now),
            );
            now = now.wrapping_add(REPORT_INTERVAL_MICROS);
            black_box(decision);
        });
    });
}

fn rayon_worker_step(criterion: &mut Criterion) {
    let Ok(configuration) = configuration(TYPICAL) else {
        return;
    };
    let Ok(capacity_grid) = grid(TYPICAL) else {
        return;
    };
    let Ok(mut state) = ScaleState::new(configuration.clone(), capacity_grid) else {
        return;
    };
    let scratch = state.new_scratch();
    let observation = ObservationBuffer::new(&configuration);
    let pool = rayon::ThreadPoolBuilder::new().build();
    let (Ok(mut scratch), Ok(mut observation), Ok(pool)) = (scratch, observation, pool) else {
        return;
    };
    if populate(&mut observation, TYPICAL).is_err() {
        return;
    }
    let mut now = 1_u64;
    criterion.bench_function("step/rayon_worker/service_reference_synthetic", |bencher| {
        bencher.iter_custom(|iterations| {
            pool.install(|| {
                let started = Instant::now();
                for _ in 0..iterations {
                    let decision = step(
                        &mut state,
                        &mut scratch,
                        observation.observation(),
                        ModelTime::from_micros(now),
                    );
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
        let Ok(mut configuration) = configuration(TYPICAL) else {
            return;
        };
        configuration.posterior_sample_count = sample_count;
        let Ok(grid) = grid(TYPICAL) else {
            return;
        };
        let Ok(mut state) = ScaleState::new(configuration.clone(), grid) else {
            return;
        };
        let scratch = state.new_scratch();
        let observation = ObservationBuffer::new(&configuration);
        let (Ok(mut scratch), Ok(mut observation)) = (scratch, observation) else {
            return;
        };
        if populate(&mut observation, TYPICAL).is_err() {
            return;
        }
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("service_reference_synthetic", sample_count),
            &sample_count,
            |bencher, _sample_count| {
                bencher.iter(|| {
                    let decision = step(
                        &mut state,
                        &mut scratch,
                        observation.observation(),
                        ModelTime::from_micros(now),
                    );
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                });
            },
        );
    }
    group.finish();
}

fn resource_grid_step(criterion: &mut Criterion) {
    let Ok(configuration) = configuration(TYPICAL) else {
        return;
    };
    let mut group = criterion.benchmark_group("resource_grid_step");
    for cell_count in [64_u32, 4_096_u32] {
        let Ok(grid) = realistic_capacity_grid(TYPICAL, cell_count) else {
            return;
        };
        let Ok(mut state) = ScaleState::new(configuration.clone(), grid) else {
            return;
        };
        let scratch = state.new_scratch();
        let observation = ObservationBuffer::new(&configuration);
        let (Ok(mut scratch), Ok(mut observation)) = (scratch, observation) else {
            return;
        };
        if populate(&mut observation, TYPICAL).is_err() {
            return;
        }
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("service_reference_synthetic", cell_count),
            &cell_count,
            |bencher, _cell_count| {
                bencher.iter(|| {
                    let decision = step(
                        &mut state,
                        &mut scratch,
                        observation.observation(),
                        ModelTime::from_micros(now),
                    );
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
        let Ok(configuration) = configuration(case) else {
            return;
        };
        group.bench_with_input(
            BenchmarkId::new("cold", case.name),
            &case,
            |bencher, _case| {
                bencher.iter(|| {
                    let _ = black_box(construct(&configuration, case));
                });
            },
        );
    }
    group.finish();
}

fn steady_state(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("step");
    for case in CASES {
        let Ok(configuration) = configuration(case) else {
            return;
        };
        let Ok(capacity_grid) = grid(case) else {
            return;
        };
        let Ok(mut state) = ScaleState::new(configuration.clone(), capacity_grid) else {
            return;
        };
        let scratch = state.new_scratch();
        let observation = ObservationBuffer::new(&configuration);
        let (Ok(mut scratch), Ok(mut observation)) = (scratch, observation) else {
            return;
        };
        if populate(&mut observation, case).is_err() {
            return;
        }
        let mut now = 1_u64;
        group.bench_with_input(
            BenchmarkId::new("populated", case.name),
            &case,
            |bencher, _case| {
                bencher.iter(|| {
                    let decision = step(
                        &mut state,
                        &mut scratch,
                        observation.observation(),
                        ModelTime::from_micros(now),
                    );
                    now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                    black_box(decision);
                });
            },
        );
    }
    group.finish();
}

fn capacity_grid(criterion: &mut Criterion) {
    let Ok(configuration) = configuration(SMALL) else {
        return;
    };
    let mut group = criterion.benchmark_group("capacity_grid");
    for (name, cell_count) in [("one", 1_u32), ("medium", 64_u32), ("maximum", 4_096_u32)] {
        let Ok(grid) = capacity_grid_with_cells(cell_count) else {
            return;
        };
        let Ok(mut state) = ScaleState::new(configuration.clone(), grid) else {
            return;
        };
        let scratch = state.new_scratch();
        let observation = ObservationBuffer::new(&configuration);
        let (Ok(mut scratch), Ok(mut observation)) = (scratch, observation) else {
            return;
        };
        let mut now = 1_u64;
        group.bench_function(BenchmarkId::new("stable_update", name), |bencher| {
            bencher.iter(|| {
                let Ok(window) = ResourceWindow::new(4.0_f64, 1.0_f64, 4_000) else {
                    return;
                };
                if observation.set_resource_window(window).is_err() {
                    return;
                }
                let decision = step(
                    &mut state,
                    &mut scratch,
                    observation.observation(),
                    ModelTime::from_micros(now),
                );
                now = now.wrapping_add(REPORT_INTERVAL_MICROS);
                black_box(decision);
            });
        });
    }
    group.finish();
}

fn configuration(case: BenchmarkCase) -> Result<Configuration, ConfigurationError> {
    Ok(Configuration {
        cohort_count_max: case.cohort_count_max,
        calendar_segment_count_max: case.cohort_count_max,
        partition_count: 64,
        replica_count_max: case.replica_count_max,
        slots_per_replica: case.slots_per_replica,
        posterior_sample_count: case.posterior_sample_count,
        report_interval_micros: REPORT_INTERVAL_MICROS,
        failure_service_weight: 0.3_f64,
        arrival_prior: prosody_scale_core::ArrivalPrior::broad_fallback(),
        capacity_change_rate_per_second: 0.0_f64,
        reliability_prior: ReliabilityPrior::population_fallback(),
        launch_time_prior: TransitionPrior::broad_fallback(),
        rebalance_time_prior: TransitionPrior::broad_fallback(),
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
    let events_per_cohort = f64::from(event_count) / f64::from(case.cohort_count_max);
    for cohort in 0..case.cohort_count_max {
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
    let events_per_cohort = f64::from(case.offered_events_per_second) * f64::from(HORIZON_SECONDS)
        / f64::from(case.cohort_count_max);
    for cohort in 0..case.cohort_count_max {
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
    let offered_rate = f64::from(case.offered_events_per_second);
    CapacityGrid::new(
        &[0.000_5_f64, 0.001_f64, 0.002_f64, 0.004_f64],
        &[
            offered_rate * 0.5_f64,
            offered_rate,
            offered_rate * 2.0_f64,
            offered_rate * 4.0_f64,
        ],
        &[0.0_f64, 0.25_f64, 0.5_f64, 1.0_f64],
    )
}

#[derive(Debug, Error)]
enum BenchmarkError {
    #[error(transparent)]
    CapacityGrid(#[from] CapacityGridError),
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

criterion_group!(scale_core, benchmarks);
criterion_main!(scale_core);
