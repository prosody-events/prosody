use super::*;
use crate::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use crate::codec::JsonCodec;
use crate::consumer::Keyed;
use crate::consumer::message::ConsumerRecord;
use crate::error::{ClassifyError, ErrorCategory};
use crate::heartbeat::HeartbeatRegistry;
use crate::producer::{ProducerConfiguration, ProsodyProducer};
use crate::telemetry::Telemetry;
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

mod interleaved;
/// Test: Request offset that was deleted before the request
/// Expected: Should detect deleted offset via lazy validation
mod loading;
mod seek_decision;
/// Unit tests proving `create_load_span` wires the "load" span to the
/// producer-propagated context exactly as [`related_span!`] dictates, for
/// both `SpanRelation` modes. No Kafka required.
mod span_wiring;
