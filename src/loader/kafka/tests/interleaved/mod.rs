use super::*;
use operations::{assert_load_result, delete_scenario_records, produce_all_partitions};

// ---------------------------------------------------------------------------
// Property-based test: interleaved Request/Await operations
// ---------------------------------------------------------------------------

/// One partition's setup: how many messages to produce and how far to delete.
/// `lso == 0` means no deletion; otherwise `offsets[lso]` is the new LSO.
#[derive(Clone, Debug)]
struct PartitionSpec {
    message_count: usize,
    lso: usize,
}

/// One topic's setup: 1–16 partitions.
#[derive(Clone, Debug)]
struct TopicSpec {
    partitions: Vec<PartitionSpec>,
}

/// An operation in an interleaved scenario. `Request` sends a load future
/// (non-blocking); `Await` polls one previously sent future to completion and
/// asserts the correct outcome. Each `Await` is a synchronisation point that
/// proves the poll loop completed at least one full cycle, so a subsequent
/// `Request` lands in a later drain pass.
#[derive(Clone, Debug)]
enum Op {
    Request {
        topic: usize,
        partition: usize,
        offset: usize,
    },
    Await {
        topic: usize,
        partition: usize,
        offset: usize,
    },
}

/// A complete interleaved scenario: 1–4 topics (each 1–16 partitions) and an
/// ordered sequence of `Request`/`Await` operations.
#[derive(Clone, Debug)]
struct InterleavedScenario {
    topics: Vec<TopicSpec>,
    ops: Vec<Op>,
}

/// Index-triple key used during scenario generation and shrinking, before
/// domain values (Topic names, Offset i64s) are known.
type Pending = AHashMap<(usize, usize, usize), usize>;

/// Domain-typed key used in the runner once broker values are resolved.
type ResolvedKey = (Topic, Partition, Offset);

const INTERLEAVED_TOPIC_COUNT: usize = 4;
const INTERLEAVED_PARTITION_COUNT: u16 = 16;

/// Topics shared by all generated interleaving cases in this process.
static INTERLEAVED_TOPICS: OnceLock<Vec<String>> = OnceLock::new();
static INTERLEAVED_LOADER: OnceLock<Arc<KafkaLoader<JsonCodec>>> = OnceLock::new();
static INTERLEAVED_PRODUCER: OnceLock<FutureProducer> = OnceLock::new();

/// Replay `ops`, tracking unmatched Requests. Returns the pending map.
fn tally_pending(ops: &[Op]) -> Pending {
    let mut pending = Pending::new();
    for op in ops {
        match *op {
            Op::Request {
                topic,
                partition,
                offset,
            } => {
                *pending.entry((topic, partition, offset)).or_default() += 1;
            }
            Op::Await {
                topic,
                partition,
                offset,
            } => {
                let c = pending.entry((topic, partition, offset)).or_default();
                if *c > 0 {
                    *c -= 1;
                }
            }
        }
    }
    pending.retain(|_, &mut c| c > 0);
    pending
}

/// Append one `Await` for every unmatched `Request` in `pending`.
fn drain_pending(ops: &mut Vec<Op>, pending: Pending) {
    for ((topic, partition, offset), count) in pending {
        for _ in 0..count {
            ops.push(Op::Await {
                topic,
                partition,
                offset,
            });
        }
    }
}

/// Clamp an `Op`'s offset index to the new (smaller) topic/partition bounds.
fn clamp_op(op: &Op, topics: &[TopicSpec]) -> Op {
    let (topic, partition, offset) = match *op {
        Op::Request {
            topic,
            partition,
            offset,
        }
        | Op::Await {
            topic,
            partition,
            offset,
        } => (topic, partition, offset),
    };
    let offset = offset.min(topics[topic].partitions[partition].message_count - 1);
    match op {
        Op::Request { .. } => Op::Request {
            topic,
            partition,
            offset,
        },
        Op::Await { .. } => Op::Await {
            topic,
            partition,
            offset,
        },
    }
}

impl Arbitrary for InterleavedScenario {
    fn arbitrary(g: &mut Gen) -> Self {
        let topic_count = (usize::arbitrary(g) % 4) + 1; // 1..=4
        let topics: Vec<TopicSpec> = (0..topic_count).map(|_| TopicSpec::arbitrary(g)).collect();

        let mut pending = Pending::new();
        let op_count = (usize::arbitrary(g) % 13) + 4; // 4..=16
        let mut ops = Vec::with_capacity(op_count + 32);

        for _ in 0..op_count {
            let has_pending = !pending.is_empty();
            // 60% Request, 40% Await when both are possible; always Request when nothing is
            // pending.
            let do_request = !has_pending || (usize::arbitrary(g) % 5) < 3;

            if do_request {
                let topic = usize::arbitrary(g) % topics.len();
                let partition = usize::arbitrary(g) % topics[topic].partitions.len();
                let offset =
                    usize::arbitrary(g) % topics[topic].partitions[partition].message_count;
                *pending.entry((topic, partition, offset)).or_default() += 1;
                ops.push(Op::Request {
                    topic,
                    partition,
                    offset,
                });
            } else {
                let keys: Vec<(usize, usize, usize)> = pending.keys().copied().collect();
                let (topic, partition, offset) = keys[usize::arbitrary(g) % keys.len()];
                let c = pending.entry((topic, partition, offset)).or_default();
                *c -= 1;
                if *c == 0 {
                    pending.remove(&(topic, partition, offset));
                }
                ops.push(Op::Await {
                    topic,
                    partition,
                    offset,
                });
            }
        }

        drain_pending(&mut ops, pending);
        InterleavedScenario { topics, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let s = self.clone();
        Box::new(
            // 1. Truncate ops from the end, re-draining so the scenario stays balanced.
            (4..s.ops.len())
                .rev()
                .filter_map({
                    let s = s.clone();
                    move |len| {
                        let mut ops = s.ops[..len].to_vec();
                        let pending = tally_pending(&ops);
                        drain_pending(&mut ops, pending);
                        ops.iter()
                            .any(|op| matches!(op, Op::Request { .. }))
                            .then_some(InterleavedScenario {
                                topics: s.topics.clone(),
                                ops,
                            })
                    }
                })
                // 2. Drop the last topic (keep ≥ 1); filter and re-balance ops.
                .chain(
                    once_with({
                        let s = s.clone();
                        move || {
                            (s.topics.len() > 1).then(|| {
                                let topics = s.topics[..s.topics.len() - 1].to_vec();
                                let mut pending = Pending::new();
                                let mut ops = Vec::new();
                                for op in &s.ops {
                                    match *op {
                                        Op::Request { topic, .. } if topic >= topics.len() => {}
                                        Op::Request {
                                            topic,
                                            partition,
                                            offset,
                                        } => {
                                            *pending
                                                .entry((topic, partition, offset))
                                                .or_default() += 1;
                                            ops.push(op.clone());
                                        }
                                        Op::Await { topic, .. } if topic >= topics.len() => {}
                                        Op::Await {
                                            topic,
                                            partition,
                                            offset,
                                        } => {
                                            let c = pending
                                                .entry((topic, partition, offset))
                                                .or_default();
                                            if *c > 0 {
                                                *c -= 1;
                                                ops.push(op.clone());
                                            }
                                        }
                                    }
                                }
                                ops.iter()
                                    .any(|op| matches!(op, Op::Request { .. }))
                                    .then_some(InterleavedScenario { topics, ops })
                            })?
                        }
                    })
                    .flatten(),
                )
                // 3. Halve message counts (min 20); clamp lso and offset indices.
                .chain(once_with(move || {
                    let topics: Vec<TopicSpec> = s
                        .topics
                        .iter()
                        .map(|t| TopicSpec {
                            partitions: t
                                .partitions
                                .iter()
                                .map(|p| {
                                    let message_count = (p.message_count / 2).max(20);
                                    PartitionSpec {
                                        message_count,
                                        lso: p.lso.min(message_count - 1),
                                    }
                                })
                                .collect(),
                        })
                        .collect();
                    let ops = s.ops.iter().map(|op| clamp_op(op, &topics)).collect();
                    InterleavedScenario { topics, ops }
                })),
        )
    }
}

async fn run_interleaved_async(scenario: InterleavedScenario) -> color_eyre::Result<()> {
    let execution = execution_topics(&scenario);
    let topic_names = INTERLEAVED_TOPICS
        .get()
        .ok_or_else(|| color_eyre::eyre::eyre!("the interleaving topics are not ready"))?;
    let topics: Vec<Topic> = topic_names
        .iter()
        .take(scenario.topics.len())
        .map(|name| Topic::from(name.as_str()))
        .collect();
    let producer = INTERLEAVED_PRODUCER
        .get()
        .ok_or_else(|| color_eyre::eyre::eyre!("the interleaving producer is not ready"))?;

    // Produce to all partitions, delete records where lso > 0.
    let offsets: Vec<Vec<Vec<i64>>> = join_all(
        topic_names
            .iter()
            .zip(execution.iter())
            .map(|(name, topic)| produce_all_partitions(producer, name, &topic.partitions)),
    )
    .await
    .into_iter()
    .collect::<color_eyre::Result<_>>()?;

    delete_scenario_records(&topics, &execution, &offsets).await?;

    let loader = INTERLEAVED_LOADER
        .get()
        .ok_or_else(|| color_eyre::eyre::eyre!("the interleaving loader is not ready"))?;

    // Spawn each load_message immediately so tokio drives it in the
    // background even when we're blocked awaiting a different handle.
    // SlotMap gives stable DefaultKey handles with no index bookkeeping.
    let mut handles: SlotMap<
        DefaultKey,
        JoinHandle<Result<ConsumerMessage<serde_json::Value>, KafkaLoaderError>>,
    > = SlotMap::new();
    let mut pending: AHashMap<ResolvedKey, Vec<DefaultKey>> = AHashMap::new();

    for op in &scenario.ops {
        match *op {
            Op::Request {
                topic: t_idx,
                partition: p_idx,
                offset: o_idx,
            } => {
                let topic: Topic = topics[t_idx];
                let partition: Partition = p_idx as Partition;
                let offset: Offset = offsets[t_idx][p_idx][o_idx];
                let loader = Arc::clone(loader);
                let key = handles.insert(tokio::spawn(async move {
                    loader.load_message(topic, partition, offset).await
                }));
                pending
                    .entry((topic, partition, offset))
                    .or_default()
                    .push(key);
            }
            Op::Await {
                topic: t_idx,
                partition: p_idx,
                offset: o_idx,
            } => {
                let topic: Topic = topics[t_idx];
                let partition: Partition = p_idx as Partition;
                let offset: Offset = offsets[t_idx][p_idx][o_idx];
                let keys = pending
                    .get_mut(&(topic, partition, offset))
                    .ok_or_else(|| color_eyre::eyre::eyre!("Await without matching Request"))?;
                let key = keys
                    .pop()
                    .ok_or_else(|| color_eyre::eyre::eyre!("Await without matching Request"))?;
                if keys.is_empty() {
                    pending.remove(&(topic, partition, offset));
                }
                let result = timeout(
                    Duration::from_mins(1),
                    handles
                        .remove(key)
                        .ok_or_else(|| color_eyre::eyre::eyre!("handle already consumed"))?,
                )
                .await??;
                let lso = scenario.topics[t_idx].partitions[p_idx].lso;
                assert_load_result(result, t_idx, partition, o_idx, offset, lso)?;
            }
        }
    }

    Ok(())
}

/// Reduces each partition to the prefix needed by this scenario's requests.
///
/// The effective deletion boundary gives every requested offset the same
/// deleted or present classification as the generated boundary.
fn execution_topics(scenario: &InterleavedScenario) -> Vec<TopicSpec> {
    scenario
        .topics
        .iter()
        .enumerate()
        .map(|(topic, spec)| TopicSpec {
            partitions: spec
                .partitions
                .iter()
                .enumerate()
                .map(|(partition, original)| {
                    let requested = scenario.ops.iter().filter_map(|op| match *op {
                        Op::Request {
                            topic: op_topic,
                            partition: op_partition,
                            offset,
                        } if op_topic == topic && op_partition == partition => Some(offset),
                        Op::Request { .. } | Op::Await { .. } => None,
                    });
                    let Some(last) = requested.max() else {
                        return PartitionSpec {
                            message_count: 0,
                            lso: 0,
                        };
                    };
                    if original.lso > last {
                        PartitionSpec {
                            message_count: last + 2,
                            lso: last + 1,
                        }
                    } else {
                        PartitionSpec {
                            message_count: last + 1,
                            lso: original.lso,
                        }
                    }
                })
                .collect(),
        })
        .collect()
}

async fn cleanup_topics(topic_names: &[String]) -> color_eyre::Result<()> {
    join_all(topic_names.iter().map(|name| delete_topic(name)))
        .await
        .into_iter()
        .collect::<color_eyre::Result<Vec<()>>>()?;
    Ok(())
}

async fn prepare_interleaved_topics() -> color_eyre::Result<Vec<String>> {
    let mut topics = Vec::with_capacity(INTERLEAVED_TOPIC_COUNT);
    for _ in 0..INTERLEAVED_TOPIC_COUNT {
        let name = test_topic("prop_interleaved");
        if let Err(error) = create_topic_with_partitions(&name, INTERLEAVED_PARTITION_COUNT).await {
            return match cleanup_topics(&topics).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(color_eyre::eyre::eyre!(
                    "topic setup failed: {error:#}; cleanup failed: {cleanup:#}"
                )),
            };
        }
        topics.push(name);
    }
    Ok(topics)
}

fn run_interleaved_scenario(scenario: InterleavedScenario) -> TestResult {
    match TEST_RUNTIME.block_on(run_interleaved_async(scenario)) {
        Ok(()) => TestResult::passed(),
        Err(e) => TestResult::error(e.to_string()),
    }
}

#[test]
fn prop_interleaved_requests() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();
    let test_count = env::var("INTEGRATION_TESTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);
    let topic_names = TEST_RUNTIME.block_on(prepare_interleaved_topics())?;
    if INTERLEAVED_TOPICS.set(topic_names.clone()).is_err() {
        TEST_RUNTIME.block_on(cleanup_topics(&topic_names))?;
        color_eyre::eyre::bail!("the interleaving topics were already ready");
    }
    let loader = {
        let _runtime = TEST_RUNTIME.enter();
        Arc::new(KafkaLoader::<JsonCodec>::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?)
    };
    if INTERLEAVED_LOADER.set(loader).is_err() {
        TEST_RUNTIME.block_on(cleanup_topics(&topic_names))?;
        color_eyre::eyre::bail!("the interleaving loader was already ready");
    }
    if INTERLEAVED_PRODUCER.set(producer()?).is_err() {
        TEST_RUNTIME.block_on(cleanup_topics(&topic_names))?;
        color_eyre::eyre::bail!("the interleaving producer was already ready");
    }

    let result = QuickCheck::new()
        .tests(test_count)
        .quicktest(run_interleaved_scenario as fn(InterleavedScenario) -> TestResult);
    let cleanup = TEST_RUNTIME.block_on(cleanup_topics(&topic_names));
    match (result, cleanup) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(failure), Ok(())) => Err(color_eyre::eyre::eyre!(
            "the interleaving property failed: {failure:?}"
        )),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(failure), Err(cleanup)) => Err(color_eyre::eyre::eyre!(
            "the interleaving property failed: {failure:?}; cleanup failed: {cleanup:#}"
        )),
    }
}

mod operations;
