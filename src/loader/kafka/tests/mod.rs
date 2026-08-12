use super::*;
use crate::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use crate::codec::JsonCodec;
use crate::heartbeat::HeartbeatRegistry;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use crate::{Offset, Partition, Topic};
use ahash::AHashMap;
use futures::future::join_all;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use slotmap::{DefaultKey, SlotMap};
use std::env;
use std::iter::once_with;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::timeout;

mod result_request;

fn test_topic(name: &str) -> String {
    format!("loader_test_{name}_{}", uuid::Uuid::new_v4())
}

fn loader_config() -> LoaderConfiguration {
    LoaderConfiguration {
        bootstrap_servers: vec!["localhost:9094".to_owned()],
        group_id: "prosody-test".to_owned(),
        max_permits: 10,
        cache_size: 1, // Minimal size to stress test deadlock prevention and eviction
        poll_interval: Duration::from_millis(1),
        seek_timeout: Duration::from_secs(5),
        discard_threshold: 10,
        message_spans: SpanRelation::default(),
        responder: None,
    }
}

#[test]
fn consumer_capacity_provisions_one_resolve_batch_per_event() {
    assert_eq!(loader_capacity(64), 64 * RESOLVE_FANOUT);
}

#[tokio::test]
async fn try_load_reports_transient_capacity_exhaustion() -> color_eyre::Result<()> {
    let (tx, _rx) = mpsc::channel(1);
    let loader = KafkaLoader::<JsonCodec> {
        tx,
        semaphore: Arc::new(Semaphore::new(0)),
        cache: Arc::new(Cache::new(1)),
        message_spans: SpanRelation::default(),
    };

    let error = timeout(
        Duration::from_millis(100),
        loader.try_load_message(Topic::from("orders"), 0, 1),
    )
    .await
    .map_err(|_| color_eyre::eyre::eyre!("non-waiting load waited for capacity"))?
    .err()
    .ok_or_else(|| color_eyre::eyre::eyre!("load must fail without capacity"))?;
    assert!(matches!(error, KafkaLoaderError::CapacityExhausted));
    assert_eq!(error.classify_error(), ErrorCategory::Transient);
    Ok(())
}

fn producer() -> color_eyre::Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .create()?;
    Ok(producer)
}

fn admin() -> color_eyre::Result<&'static ProsodyAdminClient> {
    let config = AdminConfiguration::new(vec!["localhost:9094".to_owned()])?;
    Ok(ProsodyAdminClient::cached(&config)?)
}

async fn create_topic(name: &str) -> color_eyre::Result<()> {
    create_topic_with_partitions(name, 1).await
}

async fn create_topic_with_partitions(name: &str, partitions: u16) -> color_eyre::Result<()> {
    let admin = admin()?;
    let topic_config = TopicConfiguration::builder()
        .name(name)
        .partition_count(partitions)
        .build()?;
    admin.create_topic(&topic_config).await?;
    Ok(())
}

async fn delete_topic(name: &str) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin.delete_topic(name).await?;
    Ok(())
}

async fn with_topic(
    name: &str,
    body: impl AsyncFn(&str) -> color_eyre::Result<()>,
) -> color_eyre::Result<()> {
    let topic_name = test_topic(name);
    create_topic(&topic_name).await?;
    let result = body(&topic_name).await;
    delete_topic(&topic_name).await?;
    result
}

async fn with_partitioned_topic(
    name: &str,
    partitions: u16,
    body: impl AsyncFn(&str) -> color_eyre::Result<()>,
) -> color_eyre::Result<()> {
    let topic_name = test_topic(name);
    create_topic_with_partitions(&topic_name, partitions).await?;
    let result = body(&topic_name).await;
    delete_topic(&topic_name).await?;
    result
}

async fn produce_messages_to_partition(
    topic: &str,
    partition: Partition,
    count: usize,
) -> color_eyre::Result<Vec<i64>> {
    let producer = producer()?;
    produce_messages(&producer, topic, partition, count).await
}

async fn produce_messages(
    producer: &FutureProducer,
    topic: &str,
    partition: Partition,
    count: usize,
) -> color_eyre::Result<Vec<i64>> {
    join_all((0..count).map(|i| async move {
        let payload = format!(r#"{{"test_id":{i},"data":"message-{i}"}}"#);
        let key = format!("key-{i}");
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&payload)
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map(|delivery| delivery.offset)
            .map_err(|(error, _)| error.into())
    }))
    .await
    .into_iter()
    .collect()
}

/// Deletes records across multiple partitions in a single admin call, then
/// waits for each partition's LSO to reach the requested level.
///
/// `deletions` is a slice of `(partition, offset)` pairs.
async fn delete_records_multi(
    topic: &Topic,
    deletions: &[(Partition, Offset)],
) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin
        .delete_records(deletions.iter().map(|&(p, o)| (*topic, p, o)))
        .await?;
    let consumer = watermark_consumer()?;
    join_all(deletions.iter().map(|&(partition, offset)| {
        wait_for_lso(Arc::clone(&consumer), topic.to_string(), partition, offset)
    }))
    .await
    .into_iter()
    .collect::<color_eyre::Result<Vec<()>>>()?;
    Ok(())
}

fn watermark_consumer() -> color_eyre::Result<Arc<BaseConsumer>> {
    Ok(Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9094")
            .set("group.id", "prosody-test-setup")
            .create()?,
    ))
}

/// Polls until the low watermark reaches `expected_lso`.
///
/// Each fetch has a 500 ms bound. Concurrent partition waits share one client.
async fn wait_for_lso(
    consumer: Arc<BaseConsumer>,
    topic: String,
    partition: Partition,
    expected_lso: Offset,
) -> color_eyre::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let lso = spawn_blocking({
            let topic = topic.clone();
            let consumer = Arc::clone(&consumer);
            move || {
                consumer
                    .fetch_watermarks(&topic, partition, Duration::from_millis(500))
                    .map(|(low, _high)| low)
            }
        })
        .await?;

        if let Ok(lso) = lso
            && lso >= expected_lso
        {
            return Ok(());
        }

        if Instant::now() >= deadline {
            color_eyre::eyre::bail!(
                "LSO for {topic}/{partition} did not reach {expected_lso} within 15s"
            );
        }
    }
}

/// Test: Request offset that was deleted before the request
/// Expected: Should detect deleted offset via lazy validation
#[tokio::test]
async fn test_partition_truncated_mid_flight() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("truncated", async |topic_name| {
        let offsets = produce_messages_to_partition(topic_name, 0, 100).await?;

        // Delete partition FIRST
        let topic = Topic::from(topic_name);
        delete_records_multi(&topic, &[(0_i32, 50)]).await?;

        let loader = KafkaLoader::<JsonCodec>::new(loader_config(), &HeartbeatRegistry::test())?;

        // NOW request the deleted offset 40
        let offset_40 = offsets[40];
        let result = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offset_40),
        )
        .await?;

        let Err(KafkaLoaderError::OffsetDeleted {
            requested_offset,
            next_offset,
            ..
        }) = result
        else {
            color_eyre::eyre::bail!("Expected OffsetDeleted, got: {result:?}");
        };

        assert_eq!(requested_offset, offset_40);
        assert!(next_offset >= offsets[50]);

        Ok(())
    })
    .await
}

/// Test: Recovery from seek failure
/// Expected: Should poll to recover and continue
#[tokio::test]
async fn test_seek_failure_recovery() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("seek_recovery", async |topic_name| {
        let offsets = produce_messages_to_partition(topic_name, 0, 100).await?;

        // Delete some offsets to set up potential seek failure
        let topic = Topic::from(topic_name);
        delete_records_multi(&topic, &[(0_i32, 30)]).await?;

        let loader = KafkaLoader::<JsonCodec>::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load valid offset after LSO
        let result = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;

        assert_eq!(result.offset(), offsets[50]);

        Ok(())
    })
    .await
}

/// Test: Discard threshold boundary conditions
/// Verifies seeking vs reading optimization based on threshold
#[tokio::test]
async fn test_discard_threshold_boundary() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("threshold", async |topic_name| {
        let offsets = produce_messages_to_partition(topic_name, 0, 100).await?;
        let topic = Topic::from(topic_name);

        let config = LoaderConfiguration {
            discard_threshold: 5, // Small threshold for testing
            ..loader_config()
        };
        let loader = KafkaLoader::<JsonCodec>::new(config, &HeartbeatRegistry::test())?;

        // Load offset 50 (position will be at 51)
        let msg1 = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;
        assert_eq!(msg1.offset(), offsets[50]);

        // Load offset 55 - partition was unassigned after 50, position is Invalid.
        // We seek to min_offset (55) in this state.
        let msg2 = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[55]),
        )
        .await??;
        assert_eq!(msg2.offset(), offsets[55]);

        // Load offset 70 - partition was unassigned after 55, position is Invalid.
        // We seek to min_offset (70) in this state.
        let msg3 = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[70]),
        )
        .await??;
        assert_eq!(msg3.offset(), offsets[70]);

        Ok(())
    })
    .await
}

/// Test: Multi-partition recovery from seek failure
/// Verifies recovery poll can get message from different partition
/// Note: This test uses single partition, but documents expected behavior
#[tokio::test]
async fn test_multi_partition_recovery() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    // This test verifies that seek failure recovery works
    // In a real multi-partition scenario, the recovery poll might return
    // a message from a different partition, which should still exit
    // the erroneous state for all partitions

    with_topic("recovery", async |topic_name| {
        let offsets = produce_messages_to_partition(topic_name, 0, 100).await?;

        // Delete offsets to trigger seek failure
        let topic = Topic::from(topic_name);
        delete_records_multi(&topic, &[(0_i32, 50)]).await?;

        let loader = KafkaLoader::<JsonCodec>::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load deleted offset (should trigger seek failure and recovery)
        let result = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[25]),
        )
        .await?;

        // Should get OffsetDeleted error
        let Err(KafkaLoaderError::OffsetDeleted {
            requested_offset, ..
        }) = result
        else {
            color_eyre::eyre::bail!("Expected OffsetDeleted error, got: {result:?}");
        };
        assert_eq!(requested_offset, offsets[25]);

        // Verify loader recovered and can load valid offsets
        let msg = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, offsets[60]),
        )
        .await??;
        assert_eq!(msg.offset(), offsets[60]);

        Ok(())
    })
    .await
}

/// Test: Decode error path
/// Expected: `DecodeError` returned when message payload is malformed
#[tokio::test]
async fn test_decode_error() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("decode_error", async |topic_name| {
        let producer = producer()?;

        // Produce message with invalid JSON payload
        let delivery = producer
            .send(
                FutureRecord::to(topic_name)
                    .key("test-key")
                    .payload(b"this is not valid JSON {{{")
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;

        let bad_offset = delivery.offset;
        let topic = Topic::from(topic_name);

        let loader = KafkaLoader::<JsonCodec>::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load the malformed message
        let result = timeout(
            Duration::from_mins(1),
            loader.load_message(topic, 0, bad_offset),
        )
        .await?;

        // Should get DecodeError
        let Err(KafkaLoaderError::DecodeError(t, p, offset)) = result else {
            color_eyre::eyre::bail!("Expected DecodeError, got: {result:?}");
        };

        assert_eq!(t, topic);
        assert_eq!(p, 0_i32);
        assert_eq!(offset, bad_offset);

        Ok(())
    })
    .await
}

/// Test: Multiple concurrent requests for same offset with decode error
/// Expected: All waiters receive the decode error
#[tokio::test]
async fn test_concurrent_decode_error() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("concurrent_decode_error", async |topic_name| {
        let producer = producer()?;

        // Produce malformed message
        let delivery = producer
            .send(
                FutureRecord::to(topic_name)
                    .key("test-key")
                    .payload(b"not json")
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;

        let bad_offset = delivery.offset;
        let topic = Topic::from(topic_name);

        let loader = Arc::new(KafkaLoader::<JsonCodec>::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Launch 3 concurrent requests for same bad offset
        let mut handles = Vec::new();
        for _ in 0_i32..3_i32 {
            let loader = Arc::clone(&loader);
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_mins(1),
                    loader.load_message(topic, 0, bad_offset),
                )
                .await
            }));
        }

        // All should receive DecodeError
        let results = join_all(handles).await;
        for result in results {
            let load_result = result??;
            let Err(KafkaLoaderError::DecodeError(_, _, offset)) = load_result else {
                color_eyre::eyre::bail!("Expected DecodeError, got: {load_result:?}");
            };
            assert_eq!(offset, bad_offset);
        }

        Ok(())
    })
    .await
}

/// Test: Cache permit exhaustion with many concurrent loads
///
/// This test stresses the cache permit system by launching many concurrent
/// cache misses that exceed cache capacity. With `cache_size=2`, loading 10
/// different offsets concurrently should cause cache evictions which must
/// properly release cache permits to avoid deadlock.
///
/// **Deadlock Risk:** If cache eviction doesn't release permits before the
/// next acquire blocks, threads will deadlock waiting for permits that are
/// held by cached messages that can't be evicted.
#[tokio::test]
async fn test_cache_permit_exhaustion() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("cache_permits", async |topic_name| {
        let offsets = produce_messages_to_partition(topic_name, 0, 50).await?;
        let topic = Topic::from(topic_name);

        // Small cache to force evictions
        let config = LoaderConfiguration {
            max_permits: 20, // Allow many concurrent loads
            cache_size: 2,   // But only 2 cache permits
            ..loader_config()
        };
        let loader = Arc::new(KafkaLoader::<JsonCodec>::new(
            config,
            &HeartbeatRegistry::test(),
        )?);

        // Launch 10 concurrent loads for DIFFERENT offsets
        // This will cause 8+ cache evictions since cache_size=2
        let mut handles = Vec::new();
        for i in 0..10 {
            let loader = Arc::clone(&loader);
            let offset = offsets[i * 4]; // Spread out offsets
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_mins(1),
                    loader.load_message(topic, 0, offset),
                )
                .await
            }));
        }

        // All loads should complete without deadlock
        let results = join_all(handles).await;
        let mut messages = Vec::new();
        for result in results {
            let msg = result???;
            messages.push(msg);
        }

        // Verify we got all 10 messages
        assert_eq!(messages.len(), 10);

        // Hold onto all messages to keep load permits held
        // This tests that cache permits are separate from load permits
        for (idx, msg) in messages.iter().enumerate() {
            let expected_offset = offsets[idx * 4];
            assert_eq!(msg.offset(), expected_offset);
        }

        Ok(())
    })
    .await
}

/// Regression test for cross-partition LSO contamination.
///
/// Scenario (shrunk from property test):
/// - 2-partition topic
/// - Partition 1: all but the last offset deleted (lso = offsets[19])
/// - Partition 0: no deletion at all
///
/// Bug: concurrent requests to both partitions caused partition 1's deletion
/// state to bleed into partition 0, incorrectly reporting valid offsets on
/// partition 0 as `OffsetDeleted`.
#[tokio::test]
async fn test_cross_partition_lso_contamination() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_partitioned_topic("cross_partition_lso", 2, async |topic_name| {
        let offsets_p0 = produce_messages_to_partition(topic_name, 0, 20).await?;
        let offsets_p1 = produce_messages_to_partition(topic_name, 1, 20).await?;

        // Delete all of partition 1 except the last offset (lso = offsets_p1[19])
        let topic = Topic::from(topic_name);
        delete_records_multi(&topic, &[(1_i32, offsets_p1[19])]).await?;

        let loader = Arc::new(KafkaLoader::<JsonCodec>::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Fire requests concurrently across both partitions.
        // Partition 1 requests include deleted offsets (indices 0–18) and the
        // LSO boundary (index 19). Partition 0 requests are all valid.
        let requests: &[(Partition, Offset)] = &[
            (1, offsets_p1[16]),
            (0, offsets_p0[18]),
            (0, offsets_p0[18]),
            (1, offsets_p1[11]),
            (1, offsets_p1[8]),
            (0, offsets_p0[19]),
            (0, offsets_p0[19]),
            (0, offsets_p0[6]),
            (0, offsets_p0[19]),
            (0, offsets_p0[19]),
            (0, offsets_p0[3]),
        ];

        let results = join_all(requests.iter().map(|&(partition, offset)| {
            let loader = Arc::clone(&loader);
            async move {
                timeout(
                    Duration::from_mins(1),
                    loader.load_message(topic, partition, offset),
                )
                .await
            }
        }))
        .await;

        for (result, &(partition, offset)) in results.into_iter().zip(requests) {
            let load_result = result?;
            if partition == 1_i32 && offset < offsets_p1[19] {
                // Deleted — expect OffsetDeleted
                let Err(KafkaLoaderError::OffsetDeleted {
                    partition: got_partition,
                    requested_offset: got_offset,
                    ..
                }) = load_result
                else {
                    color_eyre::eyre::bail!(
                        "partition {partition} offset {offset} expected OffsetDeleted, got: \
                         {load_result:?}"
                    );
                };
                assert_eq!(got_partition, partition);
                assert_eq!(got_offset, offset);
            } else {
                // Valid — expect Ok
                let Ok(msg) = load_result else {
                    color_eyre::eyre::bail!(
                        "partition {partition} offset {offset} expected Ok, got: {load_result:?}"
                    );
                };
                assert_eq!(msg.partition(), partition);
                assert_eq!(msg.offset(), offset);
            }
        }

        Ok(())
    })
    .await
}

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

impl Arbitrary for PartitionSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let message_count = (usize::arbitrary(g) % 41) + 20; // 20..=60
        let lso = usize::arbitrary(g) % message_count; // 0..message_count (0 = no deletion)
        PartitionSpec { message_count, lso }
    }
}

impl Arbitrary for TopicSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let partition_count = (usize::arbitrary(g) % 16) + 1; // 1..=16
        TopicSpec {
            partitions: (0..partition_count)
                .map(|_| PartitionSpec::arbitrary(g))
                .collect(),
        }
    }
}

/// Produce messages to all partitions of a single topic concurrently.
async fn produce_all_partitions(
    producer: &FutureProducer,
    topic_name: &str,
    specs: &[PartitionSpec],
) -> color_eyre::Result<Vec<Vec<i64>>> {
    join_all(specs.iter().enumerate().map(|(p, spec)| {
        produce_messages(producer, topic_name, p as Partition, spec.message_count)
    }))
    .await
    .into_iter()
    .collect()
}

/// Delete all scenario prefixes in one broker request.
async fn delete_scenario_records(
    topics: &[Topic],
    specs: &[TopicSpec],
    offsets: &[Vec<Vec<i64>>],
) -> color_eyre::Result<()> {
    let deletions: Vec<(Topic, Partition, Offset)> = specs
        .iter()
        .enumerate()
        .flat_map(|(topic, spec)| {
            spec.partitions
                .iter()
                .enumerate()
                .filter(|(_, partition)| partition.lso > 0)
                .map(move |(partition, spec)| {
                    (
                        topics[topic],
                        partition as Partition,
                        offsets[topic][partition][spec.lso],
                    )
                })
        })
        .collect();
    if deletions.is_empty() {
        return Ok(());
    }

    admin()?.delete_records(deletions.iter().copied()).await?;
    let consumer = watermark_consumer()?;
    join_all(deletions.into_iter().map(|(topic, partition, offset)| {
        wait_for_lso(Arc::clone(&consumer), topic.to_string(), partition, offset)
    }))
    .await
    .into_iter()
    .collect::<color_eyre::Result<Vec<()>>>()?;
    Ok(())
}

/// Assert the outcome of one load request against the expected deleted/valid
/// boundary.
fn assert_load_result(
    result: Result<ConsumerMessage<serde_json::Value>, KafkaLoaderError>,
    topic: usize,
    partition: Partition,
    offset_idx: usize,
    expected_offset: Offset,
    lso: usize,
) -> color_eyre::Result<()> {
    if offset_idx < lso {
        let Err(KafkaLoaderError::OffsetDeleted {
            partition: got_partition,
            requested_offset: got_offset,
            ..
        }) = result
        else {
            color_eyre::eyre::bail!(
                "topic {topic} partition {partition} offset_idx {offset_idx} (offset \
                 {expected_offset}) expected OffsetDeleted (lso_idx={lso}), got: {result:?}"
            );
        };
        assert_eq!(got_partition, partition);
        assert_eq!(got_offset, expected_offset);
    } else {
        let Ok(msg) = result else {
            color_eyre::eyre::bail!(
                "topic {topic} partition {partition} offset_idx {offset_idx} (offset \
                 {expected_offset}) expected Ok (lso_idx={lso}), got: {result:?}"
            );
        };
        assert_eq!(msg.offset(), expected_offset);
        assert_eq!(msg.partition(), partition);
    }
    Ok(())
}

/// Unit tests proving `create_load_span` wires the "load" span to the
/// producer-propagated context exactly as [`related_span!`] dictates, for
/// both `SpanRelation` modes. No Kafka required.
mod span_wiring {
    use super::*;
    use crate::consumer::message::ConsumerMessageValue;
    use crate::test_util::{assert_span_relation, captured_spans, sampled_remote_context};
    use opentelemetry::Context;
    use opentelemetry::trace::TraceContextExt as _;

    /// A `DecodedMessage` carrying `context` as its `parent_context`, the
    /// same shape `decode_message` produces after extracting headers via the
    /// propagator.
    fn decoded_message(context: Context) -> DecodedMessage<serde_json::Value> {
        DecodedMessage {
            value: Arc::new(ConsumerMessageValue::default()),
            parent_context: context,
        }
    }

    /// The "load" span parents on the producer-propagated context under
    /// child-of and links back to it under follows-from.
    #[test]
    fn load_span_connects_to_producer_context() -> color_eyre::Result<()> {
        let context = sampled_remote_context();
        let target = context.span().span_context().clone();

        for relation in [SpanRelation::Child, SpanRelation::FollowsFrom] {
            let decoded = decoded_message(context.clone());
            let spans = captured_spans(move || {
                let _span = create_load_span(&decoded, false, relation);
            });
            assert_span_relation(&spans, "load", relation, &target)?;
        }
        Ok(())
    }
}

/// Unit tests for seek decision logic (no Kafka required)
mod seek_decision {
    /// Helper to compute `should_seek` using the same logic as
    /// `seek_to_first_active_offset`. This mirrors the production logic for
    /// testability.
    fn should_seek(current_position: Option<i64>, min_offset: i64, discard_threshold: i64) -> bool {
        match current_position {
            None => true,
            Some(position) => {
                let past_target = position > min_offset;
                let too_far_behind = position + discard_threshold < min_offset;
                past_target || too_far_behind
            }
        }
    }

    #[test]
    fn invalid_position_always_seeks() {
        // After incremental_assign() but before first poll(), position() returns
        // Invalid (None). assign_if_needed only assigns on the first request;
        // concurrent lower-offset requests skip re-assignment, so the consumer may
        // be anchored above min_offset. Always seek when Invalid.
        assert!(should_seek(None, 70, 5));
        assert!(should_seek(None, 0, 10));
        assert!(should_seek(None, 1000, 100));
    }

    #[test]
    fn position_past_target_seeks() {
        // Current position (60) > target (50) - need to seek backward
        assert!(should_seek(Some(60), 50, 5));
        assert!(should_seek(Some(100), 50, 10));
    }

    #[test]
    fn position_too_far_behind_seeks() {
        // Position (50) + threshold (5) < target (70) → too far behind
        // 50 + 5 = 55 < 70 → seek
        assert!(should_seek(Some(50), 70, 5));
        // 50 + 10 = 60 < 100 → seek
        assert!(should_seek(Some(50), 100, 10));
    }

    #[test]
    fn position_within_threshold_does_not_seek() {
        // Position (50) + threshold (5) >= target (55) → within range, read
        // sequentially 50 + 5 = 55 >= 55 → don't seek
        assert!(!should_seek(Some(50), 55, 5));
        // 50 + 5 = 55 >= 54 → don't seek
        assert!(!should_seek(Some(50), 54, 5));
        // 50 + 10 = 60 >= 55 → don't seek
        assert!(!should_seek(Some(50), 55, 10));
    }

    #[test]
    fn position_at_target_does_not_seek() {
        // Position equals target - already there
        assert!(!should_seek(Some(50), 50, 5));
    }

    #[test]
    fn position_past_target_always_seeks() {
        // Position (52) > target (50) → past_target is true → must seek backward
        assert!(should_seek(Some(52), 50, 5));
    }

    #[test]
    fn threshold_boundary_exact() {
        // position + threshold == min_offset → NOT too far behind → don't seek
        // 50 + 5 = 55, min = 55 → exactly at boundary → don't seek
        assert!(!should_seek(Some(50), 55, 5));
        // 50 + 5 = 55, min = 56 → one past boundary → seek
        assert!(should_seek(Some(50), 56, 5));
    }

    #[test]
    fn zero_threshold_seeks_any_forward_gap() {
        // threshold=0: position + 0 < min_offset whenever position < min_offset
        assert!(should_seek(Some(49), 50, 0));
        assert!(should_seek(Some(0), 1, 0));
        // position == min_offset: not past, not behind → don't seek
        assert!(!should_seek(Some(50), 50, 0));
    }
}
