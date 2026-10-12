//! The model property for [`Backpressure`]: records dispatch in arrival order,
//! the in-flight limit holds, and a revoked or stopped partition loses only
//! its own waiting records.

use super::*;
use crate::Offset;
use crate::consumer::decode::DecodedMessage;
use crate::consumer::message::ConsumerMessageValue;
use crate::test_util::{ModelProperty, TEST_RUNTIME};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use opentelemetry::Context;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use std::sync::LazyLock;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, channel};

/// The number of partitions in each trace.
const PARTITIONS: usize = 3;

/// The in-flight limit. It exceeds the channel capacity, so a full channel
/// and a free permit can occur together.
const MAX_IN_FLIGHT: usize = 3;

/// The capacity of each partition channel.
const CAPACITY: usize = 2;

/// One librdkafka client for every iteration. It knows no topic, so a pause or
/// a resume skips the partition and succeeds.
static CONSUMER: LazyLock<Result<BaseConsumer, KafkaError>> =
    LazyLock::new(|| ClientConfig::new().create());

/// One step of a trace.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// The poll loop receives the next record of the partition.
    Submit(usize),
    /// The poll loop drains the waiting records without blocking.
    Drain,
    /// The partition task takes its next record and starts its handler.
    Take(usize),
    /// A running handler finishes and frees its permit. The index is reduced
    /// modulo the number of running handlers.
    Finish(usize),
    /// The partition is revoked and assigned again with a new channel.
    Reassign(usize),
    /// The partition task stops while the partition stays assigned.
    Stop(usize),
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let partition = usize::arbitrary(g) % PARTITIONS;
        match u8::arbitrary(g) % 16 {
            0..=4 => Self::Submit(partition),
            5..=7 => Self::Drain,
            8..=10 => Self::Take(partition),
            11..=13 => Self::Finish(usize::arbitrary(g)),
            14 => Self::Reassign(partition),
            _ => Self::Stop(partition),
        }
    }
}

/// The model of one partition assignment.
#[derive(Default)]
struct Assignment {
    /// Counts the assignments of the partition.
    generation: u32,
    /// The offsets in the channel, in order.
    queued: VecDeque<Offset>,
    /// Whether the partition task stopped.
    stopped: bool,
}

/// A deliberately simple model of the dispatch rules.
#[derive(Default)]
struct Model {
    assignments: [Assignment; PARTITIONS],
    /// Waiting records as (partition, generation, offset), in arrival order.
    waiting: VecDeque<(usize, u32, Offset)>,
    /// The number of running handlers.
    running: usize,
}

impl Model {
    fn free_permits(&self) -> usize {
        let queued: usize = self.assignments.iter().map(|a| a.queued.len()).sum();
        MAX_IN_FLIGHT - self.running - queued
    }

    fn has_room(&self, partition: usize) -> bool {
        let assignment = &self.assignments[partition];
        self.free_permits() > 0 && !assignment.stopped && assignment.queued.len() < CAPACITY
    }

    fn submit(&mut self, partition: usize, offset: Offset) {
        if self.waiting.is_empty() && self.has_room(partition) {
            self.assignments[partition].queued.push_back(offset);
        } else {
            let generation = self.assignments[partition].generation;
            self.waiting.push_back((partition, generation, offset));
        }
    }

    fn drain(&mut self) {
        while let Some(&(partition, generation, offset)) = self.waiting.front() {
            let assignment = &self.assignments[partition];
            if assignment.generation == generation && !assignment.stopped {
                if !self.has_room(partition) {
                    return;
                }
                self.assignments[partition].queued.push_back(offset);
            }
            self.waiting.pop_front();
        }
    }
}

/// The real [`Backpressure`] and the partition channels it feeds.
struct Harness<'a> {
    consumer: &'a BaseConsumer,
    backpressure: Backpressure<()>,
    senders: Vec<Sender<ConsumerRecord<()>>>,
    receivers: Vec<Option<Receiver<ConsumerRecord<()>>>>,
    running: Vec<ConsumerRecord<()>>,
    next_offset: Offset,
}

/// Runs a trace against the real [`Backpressure`] and the model in lockstep.
///
/// After every step, the waiting count and the free permits match the model.
/// Each record that a partition task takes has the offset the model expects,
/// so records dispatch in arrival order, and none dispatches twice.
///
/// FALSIFICATION: in `drain`, push a record that cannot dispatch to the back
/// instead of the front → a later record overtakes it and a take reds. Let
/// `submit` dispatch while records wait → the waiting count reds. Skip the
/// closed-channel discard in `drain` → a stopped partition blocks every later
/// record and the waiting count reds.
#[test]
fn prop_backpressure_matches_model() {
    fn property(trace: Vec<Op>) -> Result<bool> {
        let _runtime = TEST_RUNTIME.enter();
        let consumer = CONSUMER
            .as_ref()
            .map_err(|error| eyre!("failed to create the consumer: {error}"))?;

        let mut harness = Harness {
            consumer,
            backpressure: Backpressure::new(MAX_IN_FLIGHT, SpanRelation::Child),
            senders: Vec::with_capacity(PARTITIONS),
            receivers: Vec::with_capacity(PARTITIONS),
            running: Vec::with_capacity(MAX_IN_FLIGHT),
            next_offset: 0,
        };
        for _ in 0..PARTITIONS {
            let (sender, receiver) = channel(CAPACITY);
            harness.senders.push(sender);
            harness.receivers.push(Some(receiver));
        }

        let mut model = Model::default();
        for op in trace {
            if !harness.step(&mut model, op)?
                || harness.backpressure.waiting.len() != model.waiting.len()
                || harness.backpressure.permits.available_permits() != model.free_permits()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    QuickCheck::new().quickcheck(ModelProperty(property as fn(Vec<Op>) -> Result<bool>));
}

impl Harness<'_> {
    /// Applies `op` to the harness and to `model`. Returns `false` when a
    /// taken record differs from the model.
    fn step(&mut self, model: &mut Model, op: Op) -> Result<bool> {
        match op {
            Op::Submit(partition) => {
                let offset = self.next_offset;
                self.next_offset += 1;
                self.backpressure.submit(
                    self.consumer,
                    record(partition, offset)?,
                    &self.senders[partition],
                )?;
                model.submit(partition, offset);
            }
            Op::Drain => {
                self.backpressure.drain(self.consumer, Duration::ZERO)?;
                model.drain();
            }
            Op::Take(partition) => {
                let Some(receiver) = &mut self.receivers[partition] else {
                    return Ok(true);
                };
                let expected = model.assignments[partition].queued.pop_front();
                match (receiver.try_recv(), expected) {
                    (Ok(taken), Some(offset)) if taken.offset() == offset => {
                        self.running.push(taken);
                        model.running += 1;
                    }
                    (Err(TryRecvError::Empty), None) => {}
                    _ => return Ok(false),
                }
            }
            Op::Finish(index) => {
                if !self.running.is_empty() {
                    self.running.swap_remove(index % self.running.len());
                    model.running -= 1;
                }
            }
            Op::Reassign(partition) => {
                let (sender, receiver) = channel(CAPACITY);
                self.senders[partition] = sender;
                self.receivers[partition] = Some(receiver);
                let assignment = &mut model.assignments[partition];
                assignment.generation += 1;
                assignment.queued.clear();
                assignment.stopped = false;
            }
            Op::Stop(partition) => {
                self.receivers[partition] = None;
                let assignment = &mut model.assignments[partition];
                assignment.queued.clear();
                assignment.stopped = true;
            }
        }
        Ok(true)
    }
}

/// A decoded record from `partition` at `offset`.
fn record(partition: usize, offset: Offset) -> Result<DecodedRecord<()>> {
    Ok(DecodedRecord::Message(DecodedMessage {
        value: Arc::new(ConsumerMessageValue {
            topic: "backpressure".into(),
            partition: i32::try_from(partition)?,
            offset,
            ..ConsumerMessageValue::default()
        }),
        parent_context: Context::new(),
    }))
}
