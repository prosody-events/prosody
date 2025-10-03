use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use prosody::Key;
use prosody::consumer::DemandType;
use prosody::consumer::middleware::scheduler::dispatch::{Selector, Task};
use quanta::Instant;
use std::time::Duration;
use tokio::sync::oneshot;

/// Helper to create a task for benchmarking
fn create_task(key: &str, demand_type: DemandType, age_secs: u64) -> Task {
    let (tx, _rx) = oneshot::channel();
    Task {
        timestamp: Instant::now() - Duration::from_secs(age_secs),
        key: Key::from(key),
        demand_type,
        key_time: None,
        tx,
    }
}

fn bench_get_next_task(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_next_task");

    // Benchmark with different numbers of key_times (simulating different cache
    // sizes)
    for num_keys in [100, 1000, 5000].iter() {
        // Benchmark with different numbers of pending tasks
        for num_tasks in [10, 50, 100].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("keys_{}_tasks_{}", num_keys, num_tasks)),
                &(num_keys, num_tasks),
                |b, &(num_keys, num_tasks)| {
                    b.iter_batched(
                        || {
                            // Setup: create a selector with many key_times and pending tasks
                            let mut selector = Selector::new();

                            // Populate key_times cache with various VT values
                            for i in 0..*num_keys {
                                let key = Key::from(format!("key_{}", i));
                                let vt = Duration::from_millis((i % 1000) * 10);
                                selector.key_times.insert(key, vt.into());
                            }

                            // Enqueue tasks with a mix of demand types and ages
                            for i in 0..*num_tasks {
                                let key = format!("key_{}", i % num_keys);
                                let demand_type = if i % 3 == 0 {
                                    DemandType::Failure
                                } else {
                                    DemandType::Normal
                                };
                                let age = (i % 10) as u64;
                                selector.enqueue_task(create_task(&key, demand_type, age));
                            }

                            // Set some class times
                            selector.success_time = Duration::from_secs(100).into();
                            selector.failure_time = Duration::from_secs(30).into();

                            selector
                        },
                        |mut selector| {
                            // Benchmark: call get_next_task
                            black_box(selector.get_next_task())
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

fn bench_get_next_task_varied_vt(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_next_task_varied_vt");

    // Test with high VT spread (simulating monopoly scenarios)
    for vt_spread in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("vt_spread_{}ms", vt_spread)),
            vt_spread,
            |b, &vt_spread| {
                b.iter_batched(
                    || {
                        let mut selector = Selector::new();

                        // Create keys with varying VT to simulate different scenarios
                        for i in 0..100 {
                            let key = Key::from(format!("key_{}", i));
                            let vt = Duration::from_millis(i * vt_spread);
                            selector.key_times.insert(key, vt.into());
                        }

                        // Enqueue 50 tasks
                        for i in 0..50 {
                            let key = format!("key_{}", i);
                            let demand_type = if i % 4 == 0 {
                                DemandType::Failure
                            } else {
                                DemandType::Normal
                            };
                            selector.enqueue_task(create_task(&key, demand_type, 0));
                        }

                        selector.success_time = Duration::from_secs(70).into();
                        selector.failure_time = Duration::from_secs(30).into();

                        selector
                    },
                    |mut selector| black_box(selector.get_next_task()),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_get_next_task, bench_get_next_task_varied_vt);
criterion_main!(benches);
