//! Calculation graph overhead benchmarks.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use prosody_scale_sim::{
    AttemptContext, AttemptFrame, AttemptGenerator, AttemptModel, AttemptParameters,
    HistoricalAttemptModel,
};

const FRAME_COUNT: u64 = 1_048_576;
const TIME_THRESHOLDS: [u64; 5] = [0, 300_000_000, 900_000_000, 1_800_000_000, 3_600_000_000];
const BASE_DEPENDENCY_MICROS: [u64; 5] = [800, 1_000, 1_600, 900, 750];
const DEPENDENCY_THRESHOLDS: [u32; 6] = [0, 64, 128, 256, 512, 1_024];
const DEPENDENCY_ADDED_MICROS: [u64; 6] = [0, 50, 250, 1_200, 8_000, 40_000];
const HANDLER_THRESHOLDS: [u32; 6] = [0, 128, 256, 512, 1_024, 2_048];
const HANDLER_ADDED_MICROS: [u64; 6] = [0, 20, 100, 600, 4_000, 25_000];

fn benchmarks(criterion: &mut Criterion) {
    let frames = frames();
    let mut group = criterion.benchmark_group("attempt_model");
    group.throughput(Throughput::Elements(FRAME_COUNT));
    group.bench_function("direct", |bencher| {
        bencher.iter(|| black_box(evaluate_direct(black_box(&frames))));
    });
    group.bench_function("static_typed_graph", |bencher| {
        let graph = AttemptGraph::new();
        bencher.iter(|| black_box(evaluate_graph(black_box(&frames), &graph)));
    });
    let Ok(mut historical) = HistoricalAttemptModel::new(HistoryGraph, FRAME_COUNT as u32) else {
        return;
    };
    group.bench_function("static_graph_with_history", |bencher| {
        bencher.iter(|| black_box(evaluate_historical(black_box(&frames), &mut historical)));
    });
    group.finish();
}

fn frames() -> Vec<AttemptFrame> {
    (0..FRAME_COUNT)
        .map(|index| AttemptFrame {
            now_micros: index.saturating_mul(5_000),
            event_index: index as u32,
            attempt: 1 + (index % 3) as u32,
            replicas: 1 + (index % 128) as u32,
            active_handlers: 1 + (index.wrapping_mul(47) % 2_048) as u32,
            dependency_concurrency: (index.wrapping_mul(29) % 1_280) as u32,
            queued_events: (index.wrapping_mul(997) % 50_001) as u32,
        })
        .collect()
}

fn evaluate_direct(frames: &[AttemptFrame]) -> u64 {
    frames.iter().fold(0_u64, |checksum, &frame| {
        let parameters = AttemptParameters {
            dependency_operation_micros: lookup(
                frame.now_micros,
                &TIME_THRESHOLDS,
                &BASE_DEPENDENCY_MICROS,
            )
            .saturating_add(lookup(
                frame.dependency_concurrency,
                &DEPENDENCY_THRESHOLDS,
                &DEPENDENCY_ADDED_MICROS,
            )),
            handler_added_micros: lookup(
                frame.active_handlers,
                &HANDLER_THRESHOLDS,
                &HANDLER_ADDED_MICROS,
            ),
        };
        checksum
            ^ parameters.dependency_operation_micros.rotate_left(17)
            ^ parameters.handler_added_micros
    })
}

fn evaluate_graph(frames: &[AttemptFrame], graph: &AttemptGraph) -> u64 {
    frames.iter().fold(0_u64, |checksum, &frame| {
        let parameters = graph.calculate(frame);
        checksum
            ^ parameters.dependency_operation_micros.rotate_left(17)
            ^ parameters.handler_added_micros
    })
}

fn evaluate_historical(
    frames: &[AttemptFrame],
    model: &mut HistoricalAttemptModel<HistoryGraph>,
) -> u64 {
    frames.iter().fold(0_u64, |checksum, &frame| {
        let parameters = model.calculate(frame);
        checksum
            ^ parameters.dependency_operation_micros.rotate_left(17)
            ^ parameters.handler_added_micros
    })
}

fn lookup<Key: Ord + Copy, const N: usize>(
    key: Key,
    thresholds: &[Key; N],
    values: &[u64; N],
) -> u64 {
    let index = thresholds
        .partition_point(|&threshold| threshold <= key)
        .saturating_sub(1);
    values[index]
}

trait Node<Input> {
    type Output;

    fn calculate(&self, input: Input) -> Self::Output;
}

struct AttemptGraph {
    dependency: Sum<TimeDependency, DependencyContention>,
    handler: HandlerContention,
}

impl AttemptGraph {
    const fn new() -> Self {
        Self {
            dependency: Sum(TimeDependency, DependencyContention),
            handler: HandlerContention,
        }
    }

    fn calculate(&self, frame: AttemptFrame) -> AttemptParameters {
        AttemptParameters {
            dependency_operation_micros: self.dependency.calculate(frame),
            handler_added_micros: self.handler.calculate(frame),
        }
    }
}

struct Sum<Left, Right>(Left, Right);

impl<Input: Copy, Left, Right> Node<Input> for Sum<Left, Right>
where
    Left: Node<Input, Output = u64>,
    Right: Node<Input, Output = u64>,
{
    type Output = u64;

    fn calculate(&self, input: Input) -> Self::Output {
        self.0
            .calculate(input)
            .saturating_add(self.1.calculate(input))
    }
}

struct TimeDependency;

impl Node<AttemptFrame> for TimeDependency {
    type Output = u64;

    fn calculate(&self, input: AttemptFrame) -> Self::Output {
        lookup(input.now_micros, &TIME_THRESHOLDS, &BASE_DEPENDENCY_MICROS)
    }
}

struct DependencyContention;

impl Node<AttemptFrame> for DependencyContention {
    type Output = u64;

    fn calculate(&self, input: AttemptFrame) -> Self::Output {
        lookup(
            input.dependency_concurrency,
            &DEPENDENCY_THRESHOLDS,
            &DEPENDENCY_ADDED_MICROS,
        )
    }
}

struct HandlerContention;

impl Node<AttemptFrame> for HandlerContention {
    type Output = u64;

    fn calculate(&self, input: AttemptFrame) -> Self::Output {
        lookup(
            input.active_handlers,
            &HANDLER_THRESHOLDS,
            &HANDLER_ADDED_MICROS,
        )
    }
}

struct HistoryGraph;

impl AttemptGenerator for HistoryGraph {
    fn calculate(&self, context: AttemptContext<'_>) -> AttemptParameters {
        let dependency_history = u64::from(context.history.queued_events(0).unwrap_or(0) % 8);
        AttemptParameters {
            dependency_operation_micros: lookup(
                context.frame.now_micros,
                &TIME_THRESHOLDS,
                &BASE_DEPENDENCY_MICROS,
            )
            .saturating_add(lookup(
                context.frame.dependency_concurrency,
                &DEPENDENCY_THRESHOLDS,
                &DEPENDENCY_ADDED_MICROS,
            ))
            .saturating_add(dependency_history),
            handler_added_micros: lookup(
                context.frame.active_handlers,
                &HANDLER_THRESHOLDS,
                &HANDLER_ADDED_MICROS,
            ),
        }
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
