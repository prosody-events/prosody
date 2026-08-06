//! Independent simulation batch benchmarks.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use prosody_scale_sim::{EventSpec, Plant, PlantConfiguration, PlantError, run_parallel};

const PLANT_COUNT: u32 = 8;
const EVENT_COUNT: u32 = 50_000;

fn benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("simulation_batch");
    group.sample_size(10);
    group.bench_function("serial_service_reference_synthetic", |bencher| {
        bencher.iter_batched(
            plants,
            |plants| black_box(plants.map(run_serial)),
            BatchSize::LargeInput,
        );
    });
    group.bench_function("rayon_service_reference_synthetic", |bencher| {
        bencher.iter_batched(
            plants,
            |plants| black_box(plants.map(run_parallel)),
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn plants() -> Result<Vec<Plant>, PlantError> {
    let mut plants = Vec::with_capacity(PLANT_COUNT as usize);
    for replication in 0..PLANT_COUNT {
        let configuration = PlantConfiguration::new(64, 1_024, EVENT_COUNT, 1, 16, 128)?;
        let mut plant = Plant::new(configuration, 16)?;
        for event in 0..EVENT_COUNT {
            plant.add_event(EventSpec {
                release_micros: u64::from(event) * 20,
                partition: event % 64,
                key: event.wrapping_mul(2_654_435_761).wrapping_add(replication) % 1_024,
                handler_micros: 5_000,
                dependency_operations: 1,
                transient_failures: 0,
                permanent_rejection: false,
                timer: event % 20 == 0,
            })?;
        }
        plants.push(plant);
    }
    Ok(plants)
}

fn run_serial(plants: Vec<Plant>) -> Vec<prosody_scale_sim::SimulationResult> {
    plants.into_iter().map(Plant::run).collect()
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
