use super::*;
use crate::Topic;
use crate::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use crate::heartbeat::HeartbeatRegistry;
use crate::tracing::init_test_logging;
use futures::future::join_all;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::spawn_blocking;
use tokio::time::timeout;

fn test_topic(name: &str) -> String {
    format!("loader_test_{name}_{}", uuid::Uuid::new_v4())
}

fn loader_config() -> LoaderConfiguration {
    LoaderConfiguration {
        bootstrap_servers: vec!["localhost:9094".to_owned()],
        group_id: "prosody-test".to_owned(),
        max_permits: 10,
        cache_size: 1, // Minimal size to stress test deadlock prevention and eviction
        poll_interval: Duration::from_millis(50),
        seek_timeout: Duration::from_secs(5),
        discard_threshold: 10,
    }
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
    wait_for_topic(name, partitions).await?;
    Ok(())
}

/// Polls Kafka metadata until the topic is visible with the expected number of
/// partitions. Replaces the fixed 2-second sleep that previously preceded every
/// test, cutting topic-creation overhead from ~2 s to the actual broker
/// propagation time (~50–200 ms locally).
///
/// Each `fetch_metadata` call has a built-in 500ms timeout that yields the
/// async runtime while the blocking call is in-flight — no extra sleep needed.
async fn wait_for_topic(name: &str, expected_partitions: u16) -> color_eyre::Result<()> {
    let consumer: Arc<BaseConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9094")
            .set("group.id", "prosody-test-setup")
            .create()?,
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let meta = spawn_blocking({
            let name = name.to_owned();
            let consumer = Arc::clone(&consumer);
            move || consumer.fetch_metadata(Some(&name), Duration::from_millis(500))
        })
        .await?;

        if let Ok(meta) = meta {
            let ready = meta.topics().iter().any(|t| {
                t.name() == name
                    && t.partitions().len() == usize::from(expected_partitions)
                    && t.partitions().iter().all(|p| p.error().is_none())
            });
            if ready {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            color_eyre::eyre::bail!("topic {name} did not become ready within 10s");
        }
    }
}

async fn delete_topic(name: &str) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin.delete_topic(name).await?;
    Ok(())
}

async fn produce_messages(topic: &str, count: usize) -> color_eyre::Result<Vec<i64>> {
    produce_messages_to_partition(topic, 0, count).await
}

async fn produce_messages_to_partition(
    topic: &str,
    partition: i32,
    count: usize,
) -> color_eyre::Result<Vec<i64>> {
    let producer = producer()?;
    let mut offsets = Vec::new();

    for i in 0..count {
        // Produce JSON payload as expected by decode_message
        let payload = format!(r#"{{"test_id":{i},"data":"message-{i}"}}"#);

        let delivery = producer
            .send(
                FutureRecord::to(topic)
                    .partition(partition)
                    .key(&format!("key-{i}"))
                    .payload(&payload)
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
        offsets.push(delivery.offset);
    }

    Ok(offsets)
}

async fn delete_records_up_to(topic: &Topic, offset: crate::Offset) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin.delete_records([(*topic, 0_i32, offset)]).await?;
    wait_for_lso(topic, offset).await?;
    Ok(())
}

/// Polls `fetch_watermarks` until the low watermark (LSO) reaches `expected`.
/// Replaces the fixed 5-second sleep, cutting deletion propagation overhead
/// from ~5 s to the actual broker propagation time (~50–500 ms locally).
///
/// Each `fetch_watermarks` call has a built-in 500ms timeout that yields the
/// async runtime while the blocking call is in-flight — no extra sleep needed.
async fn wait_for_lso(topic: &Topic, expected_lso: crate::Offset) -> color_eyre::Result<()> {
    let consumer: Arc<BaseConsumer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", "localhost:9094")
            .set("group.id", "prosody-test-setup")
            .create()?,
    );

    let topic_str = topic.to_string();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let lso = spawn_blocking({
            let topic_str = topic_str.clone();
            let consumer = Arc::clone(&consumer);
            move || {
                consumer
                    .fetch_watermarks(&topic_str, 0, Duration::from_millis(500))
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
            color_eyre::eyre::bail!("LSO for {topic}/0 did not reach {expected_lso} within 15s");
        }
    }
}

/// Test: Load a valid message successfully
#[tokio::test]
async fn test_load_valid_offset() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("valid");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 10).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load offset 5
        let result = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[5]),
        )
        .await??;

        assert_eq!(result.offset(), offsets[5]);
        assert_eq!(result.partition(), 0_i32);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Load multiple sequential offsets
#[tokio::test]
async fn test_load_sequential_offsets() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("sequential");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 20).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load offsets 5, 6, 7 in sequence
        for &offset in &offsets[5..8] {
            let result = timeout(
                Duration::from_secs(10),
                loader.load_message(topic, 0, offset),
            )
            .await??;
            assert_eq!(result.offset(), offset);
        }

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Load offsets from multiple partitions (if we had multiple partitions)
/// For now, just test multiple requests to same partition
#[tokio::test]
async fn test_multiple_concurrent_requests() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("concurrent");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 30).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Request multiple offsets concurrently
        let mut handles = Vec::new();
        for i in [5, 10, 15, 20, 25] {
            let loader = Arc::clone(&loader);
            let offset = offsets[i];
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_secs(10),
                    loader.load_message(topic, 0, offset),
                )
                .await
            }));
        }

        let results = join_all(handles).await;
        for (idx, result) in results.into_iter().enumerate() {
            let msg = result???;
            let expected_offset = offsets[[5, 10, 15, 20, 25][idx]];
            assert_eq!(msg.offset(), expected_offset);
        }

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Request deleted offset (offset < LSO)
/// Expected: Should return `OffsetDeleted` error
#[tokio::test]
async fn test_load_deleted_offset() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load deleted offset 25
        let result = timeout(
            Duration::from_secs(30),
            loader.load_message(topic, 0, offsets[25]),
        )
        .await?;

        let Err(KafkaLoaderError::OffsetDeleted(t, p, requested, lso)) = result else {
            color_eyre::eyre::bail!("Expected OffsetDeleted error, got: {result:?}");
        };

        assert_eq!(t, topic);
        assert_eq!(p, 0_i32);
        assert_eq!(requested, offsets[25]);
        assert_eq!(lso, offsets[50]); // LSO should be at offset 50

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Request multiple offsets where first 2 are deleted
/// Expected: Both should return `OffsetDeleted` errors
#[tokio::test]
async fn test_load_multiple_deleted_offsets() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("multi_deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Request deleted offsets 10, 20, and valid offset 60 concurrently
        let loader1 = Arc::clone(&loader);
        let loader2 = Arc::clone(&loader);
        let loader3 = Arc::clone(&loader);

        let offset_10 = offsets[10];
        let offset_20 = offsets[20];
        let offset_60 = offsets[60];

        let h1 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader1.load_message(topic, 0, offset_10),
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader2.load_message(topic, 0, offset_20),
            )
            .await
        });
        let h3 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader3.load_message(topic, 0, offset_60),
            )
            .await
        });

        let (r1, r2, r3) = tokio::join!(h1, h2, h3);

        // First two should be OffsetDeleted
        let result1 = r1??;
        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, _)) = result1 else {
            color_eyre::eyre::bail!("Expected OffsetDeleted for offset 10, got: {result1:?}");
        };
        assert_eq!(requested, offset_10);

        let result2 = r2??;
        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, _)) = result2 else {
            color_eyre::eyre::bail!("Expected OffsetDeleted for offset 20, got: {result2:?}");
        };
        assert_eq!(requested, offset_20);

        // Third should succeed
        let result3 = r3??;
        let Ok(msg) = result3 else {
            color_eyre::eyre::bail!("Expected success for offset 60, got error: {result3:?}");
        };
        assert_eq!(msg.offset(), offsets[60]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Request offset exactly at LSO boundary
/// Expected: Should succeed (experiments showed LSO is valid for assign)
#[tokio::test]
async fn test_load_offset_at_lso() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("at_lso");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load offset at LSO (50)
        let result = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;

        assert_eq!(result.offset(), offsets[50]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Request offset that was deleted before the request
/// Expected: Should detect deleted offset via lazy validation
#[tokio::test]
async fn test_partition_truncated_mid_flight() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("truncated");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete partition FIRST
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // NOW request the deleted offset 40
        let offset_40 = offsets[40];
        let result = timeout(
            Duration::from_secs(15),
            loader.load_message(topic, 0, offset_40),
        )
        .await?;

        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, lso)) = result else {
            color_eyre::eyre::bail!("Expected OffsetDeleted, got: {result:?}");
        };

        assert_eq!(requested, offset_40);
        assert!(lso >= offsets[50]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Recovery from seek failure
/// Expected: Should poll to recover and continue
#[tokio::test]
async fn test_seek_failure_recovery() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("seek_recovery");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete some offsets to set up potential seek failure
        delete_records_up_to(&Topic::from(topic_name.as_str()), 30).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load valid offset after LSO
        let result = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;

        assert_eq!(result.offset(), offsets[50]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Sparse offset requests (10, 50, 90)
/// Expected: Should load all successfully with seeks
#[tokio::test]
async fn test_sparse_offset_requests() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("sparse");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Request sparse offsets
        for &idx in &[10, 50, 90] {
            let result = timeout(
                Duration::from_secs(10),
                loader.load_message(topic, 0, offsets[idx]),
            )
            .await??;
            assert_eq!(result.offset(), offsets[idx]);
        }

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Backwards seek (request offset < current position)
/// Verifies seeking backwards works correctly
#[tokio::test]
async fn test_backwards_seek() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("backwards");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load offset 80 (position will be at 81)
        let msg1 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[80]),
        )
        .await??;
        assert_eq!(msg1.offset(), offsets[80]);

        // Now load offset 30 (should seek backwards)
        let msg2 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[30]),
        )
        .await??;
        assert_eq!(msg2.offset(), offsets[30]);

        // Verify we can continue reading forward
        let msg3 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[31]),
        )
        .await??;
        assert_eq!(msg3.offset(), offsets[31]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Discard threshold boundary conditions
/// Verifies seeking vs reading optimization based on threshold
#[tokio::test]
async fn test_discard_threshold_boundary() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("threshold");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;
        let topic = Topic::from(topic_name.as_str());

        let config = LoaderConfiguration {
            bootstrap_servers: vec!["localhost:9094".to_owned()],
            group_id: "prosody-test".to_owned(),
            max_permits: 10,
            cache_size: 1, // Minimal size to stress test deadlock prevention and eviction
            poll_interval: Duration::from_millis(50),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 5, // Small threshold for testing
        };
        let loader = KafkaLoader::new(config, &HeartbeatRegistry::test())?;

        // Load offset 50 (position will be at 51)
        let msg1 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;
        assert_eq!(msg1.offset(), offsets[50]);

        // Load offset 55 - partition was unassigned after 50, position is Invalid.
        // We seek to min_offset (55) in this state.
        let msg2 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[55]),
        )
        .await??;
        assert_eq!(msg2.offset(), offsets[55]);

        // Load offset 70 - partition was unassigned after 55, position is Invalid.
        // We seek to min_offset (70) in this state.
        let msg3 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[70]),
        )
        .await??;
        assert_eq!(msg3.offset(), offsets[70]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Concurrent same-offset requests
/// Verifies multiple waiters for same offset all receive cloned message
#[tokio::test]
async fn test_concurrent_same_offset_requests() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("same_offset");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 10).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        let target_offset = offsets[5];

        // Launch 5 concurrent requests for same offset
        let mut handles = Vec::new();
        for _ in 0_i32..5_i32 {
            let loader = Arc::clone(&loader);
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_secs(10),
                    loader.load_message(topic, 0, target_offset),
                )
                .await
            }));
        }

        // All should succeed with same offset
        let results = join_all(handles).await;
        for result in results {
            let msg = result???;
            assert_eq!(msg.offset(), target_offset);
        }

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
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

    let topic_name = test_topic("recovery");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets to trigger seek failure
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load deleted offset (should trigger seek failure and recovery)
        let result = timeout(
            Duration::from_secs(30),
            loader.load_message(topic, 0, offsets[25]),
        )
        .await?;

        // Should get OffsetDeleted error
        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, _)) = result else {
            color_eyre::eyre::bail!("Expected OffsetDeleted error, got: {result:?}");
        };
        assert_eq!(requested, offsets[25]);

        // Verify loader recovered and can load valid offsets
        let msg = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[60]),
        )
        .await??;
        assert_eq!(msg.offset(), offsets[60]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Decode error path
/// Expected: `DecodeError` returned when message payload is malformed
#[tokio::test]
async fn test_decode_error() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("decode_error");
    create_topic(&topic_name).await?;

    let result = async {
        let producer = producer()?;

        // Produce message with invalid JSON payload
        let delivery = producer
            .send(
                FutureRecord::to(&topic_name)
                    .key("test-key")
                    .payload(b"this is not valid JSON {{{")
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;

        let bad_offset = delivery.offset;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Try to load the malformed message
        let result = timeout(
            Duration::from_secs(10),
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
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Multiple concurrent requests for same offset with decode error
/// Expected: All waiters receive the decode error
#[tokio::test]
async fn test_concurrent_decode_error() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("concurrent_decode_error");
    create_topic(&topic_name).await?;

    let result = async {
        let producer = producer()?;

        // Produce malformed message
        let delivery = producer
            .send(
                FutureRecord::to(&topic_name)
                    .key("test-key")
                    .payload(b"not json")
                    .headers(OwnedHeaders::new()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;

        let bad_offset = delivery.offset;
        let topic = Topic::from(topic_name.as_str());

        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Launch 3 concurrent requests for same bad offset
        let mut handles = Vec::new();
        for _ in 0_i32..3_i32 {
            let loader = Arc::clone(&loader);
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_secs(10),
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
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Request older deleted offset when partition already has newer active
/// requests This tests the scenario where:
/// 1. Partition has request for newer offset (70), consumer positioned at 71
/// 2. New request arrives for older deleted offset (10, where LSO=50)
/// 3. This triggers seek(10) which enters erroneous state
/// 4. With `continue` on seek failure, we'd skip polling and never recover
/// Expected: Should NOT timeout (either recovers via poll or fails quickly)
#[tokio::test]
async fn test_older_deleted_offset_after_newer_request() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("older_deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&Topic::from(topic_name.as_str()), 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Launch BOTH requests simultaneously
        // This ensures both are in active before any seeking happens
        let loader1 = Arc::clone(&loader);
        let loader2 = Arc::clone(&loader);
        let offset_70 = offsets[70];
        let offset_10 = offsets[10];

        let h1 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader1.load_message(topic, 0, offset_70),
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader2.load_message(topic, 0, offset_10),
            )
            .await
        });

        // Wait for both
        let (r1, r2) = tokio::join!(h1, h2);

        // First should succeed
        let msg1 = r1???;
        assert_eq!(msg1.offset(), offset_70);

        // Second should get OffsetDeleted error (not timeout!)
        let result2 = r2??;
        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, lso)) = result2 else {
            color_eyre::eyre::bail!("Expected OffsetDeleted for offset 10, got: {result2:?}");
        };
        assert_eq!(requested, offset_10);
        assert!(lso >= offsets[50]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
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

    let topic_name = test_topic("cache_permits");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 50).await?;
        let topic = Topic::from(topic_name.as_str());

        // Small cache to force evictions
        let config = LoaderConfiguration {
            bootstrap_servers: vec!["localhost:9094".to_owned()],
            group_id: "prosody-test".to_owned(),
            max_permits: 20, // Allow many concurrent loads
            cache_size: 2,   // But only 2 cache permits
            poll_interval: Duration::from_millis(50),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 10,
        };
        let loader = Arc::new(KafkaLoader::new(config, &HeartbeatRegistry::test())?);

        // Launch 10 concurrent loads for DIFFERENT offsets
        // This will cause 8+ cache evictions since cache_size=2
        let mut handles = Vec::new();
        for i in 0..10 {
            let loader = Arc::clone(&loader);
            let offset = offsets[i * 4]; // Spread out offsets
            handles.push(tokio::spawn(async move {
                timeout(
                    Duration::from_secs(15),
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
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Concurrent loading from multiple partitions
///
/// This test verifies that `incremental_assign()` works correctly by loading
/// messages from multiple partitions concurrently. With the old `assign()`
/// approach (full replacement), loading from partition 1 would unassign
/// partition 0, breaking concurrent multi-partition requests.
///
/// The test:
/// 1. Creates a topic with 3 partitions
/// 2. Produces messages to each partition
/// 3. Concurrently loads messages from all partitions
/// 4. Verifies all messages are loaded correctly
#[tokio::test]
async fn test_concurrent_multi_partition_loading() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("multi_partition");
    create_topic_with_partitions(&topic_name, 3).await?;

    let result = async {
        // Produce messages to each partition
        let offsets_p0 = produce_messages_to_partition(&topic_name, 0, 10).await?;
        let offsets_p1 = produce_messages_to_partition(&topic_name, 1, 10).await?;
        let offsets_p2 = produce_messages_to_partition(&topic_name, 2, 10).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = Arc::new(KafkaLoader::new(
            loader_config(),
            &HeartbeatRegistry::test(),
        )?);

        // Request messages from all 3 partitions concurrently
        // With old assign() behavior, this would fail because assigning
        // partition 1 would replace the assignment for partition 0
        let loader_a = Arc::clone(&loader);
        let loader_b = Arc::clone(&loader);
        let loader_c = Arc::clone(&loader);

        let target_0 = offsets_p0[5];
        let target_1 = offsets_p1[5];
        let target_2 = offsets_p2[5];

        let h0 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader_a.load_message(topic, 0, target_0),
            )
            .await
        });
        let h1 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader_b.load_message(topic, 1, target_1),
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(30),
                loader_c.load_message(topic, 2, target_2),
            )
            .await
        });

        let (r0, r1, r2) = tokio::join!(h0, h1, h2);

        // All should succeed
        let msg0 = r0???;
        assert_eq!(msg0.partition(), 0_i32);
        assert_eq!(msg0.offset(), target_0);

        let msg1 = r1???;
        assert_eq!(msg1.partition(), 1_i32);
        assert_eq!(msg1.offset(), target_1);

        let msg2 = r2???;
        assert_eq!(msg2.partition(), 2_i32);
        assert_eq!(msg2.offset(), target_2);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Sequential loads from multiple partitions maintains assignments
///
/// Verifies that loading from partition 0, then partition 1, then partition 0
/// again works correctly. With old `assign()` behavior, the second load from
/// partition 0 would require re-assignment because partition 1's assignment
/// replaced it.
#[tokio::test]
async fn test_sequential_multi_partition_loading() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("seq_multi_partition");
    create_topic_with_partitions(&topic_name, 2).await?;

    let result = async {
        let offsets_p0 = produce_messages_to_partition(&topic_name, 0, 10).await?;
        let offsets_p1 = produce_messages_to_partition(&topic_name, 1, 10).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config(), &HeartbeatRegistry::test())?;

        // Load from partition 0
        let msg0 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets_p0[3]),
        )
        .await??;
        assert_eq!(msg0.partition(), 0_i32);
        assert_eq!(msg0.offset(), offsets_p0[3]);

        // Load from partition 1
        let msg1 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 1, offsets_p1[5]),
        )
        .await??;
        assert_eq!(msg1.partition(), 1_i32);
        assert_eq!(msg1.offset(), offsets_p1[5]);

        // Load again from partition 0 (different offset)
        // With old assign() this would fail or timeout
        let msg0_again = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets_p0[7]),
        )
        .await??;
        assert_eq!(msg0_again.partition(), 0_i32);
        assert_eq!(msg0_again.offset(), offsets_p0[7]);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Requests for lower offsets arrive while partition is already assigned
/// at a higher offset.
///
/// # Bug reproduced
///
/// `assign_if_needed` only assigns a partition on the *first* request for that
/// partition. If subsequent requests target *lower* offsets, no re-assignment
/// and no seek is issued (position is `Invalid` after assign → `should_seek`
/// is false). The poll loop reads from the high offset, and the lazy-validation
/// `split_off` logic then misidentifies all pending lower offsets as deleted.
///
/// # Trigger conditions
///
/// 1. Request A arrives for offset HIGH → `assign_if_needed` assigns at HIGH.
/// 2. Requests B, C arrive for offsets LOW1, LOW2 (< HIGH) before A is
///    fulfilled → partition already in `active`, no re-assign, no seek.
/// 3. Poll returns message at HIGH.
/// 4. `split_off(HIGH)` leaves LOW1, LOW2 in `deleted_offsets`.
/// 5. `notify_deleted_offsets` fires `OffsetDeleted` for LOW1 and LOW2, even
///    though those offsets exist and have not been compacted or expired.
///
/// # Why `tokio::join!` makes the race deterministic
///
/// All three `load_message` calls run concurrently within a single task.
/// `semaphore.acquire_owned()` returns `Poll::Ready` immediately when permits
/// are available, so all three futures advance past the semaphore without
/// yielding to the runtime. The mpsc channel has `max_permits` capacity, so
/// all three `tx.send()` calls also complete without yielding. By the time any
/// future yields on `rx.await`, all three requests are already queued in the
/// channel. The poll loop's first drain therefore sees HIGH, LOW1, LOW2 in
/// arrival order, processes them in a single iteration, and triggers the bug.
#[tokio::test]
async fn test_lower_offsets_falsely_reported_deleted_when_assigned_at_higher_offset()
-> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("false_deleted");
    create_topic(&topic_name).await?;

    let result = async {
        // Produce 50 messages. The gap between low and high offsets (35
        // messages) far exceeds discard_threshold=0, so the loader cannot read
        // forward from HIGH to reach LOW1/LOW2; the only path back is a seek,
        // which the bug suppresses.
        let offsets = produce_messages(&topic_name, 50).await?;
        let topic = Topic::from(topic_name.as_str());

        // discard_threshold=0: every forward gap triggers a seek. With the
        // bug, no seek fires because position() is Invalid after the initial
        // assign(). Setting it to 0 makes the intended seek behaviour explicit.
        let config = LoaderConfiguration {
            bootstrap_servers: vec!["localhost:9094".to_owned()],
            group_id: "prosody-test".to_owned(),
            max_permits: 10,
            cache_size: 10,
            poll_interval: Duration::from_millis(100),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 0,
        };
        let loader = KafkaLoader::new(config, &HeartbeatRegistry::test())?;

        let low1 = offsets[5];
        let low2 = offsets[10];
        let high = offsets[40];

        // Run all three loads concurrently from one task. Because the semaphore
        // has 10 permits and the channel has 10 slots, all three futures
        // advance through acquire() + send() without yielding, queuing HIGH
        // first (join! polls left-to-right), then LOW1, LOW2. The poll loop
        // drains all three in its first iteration: HIGH anchors the assignment,
        // LOW1/LOW2 skip assign_if_needed, and the bug fires.
        let (r_high, r_low1, r_low2) = tokio::join!(
            timeout(Duration::from_secs(15), loader.load_message(topic, 0, high)),
            timeout(Duration::from_secs(15), loader.load_message(topic, 0, low1)),
            timeout(Duration::from_secs(15), loader.load_message(topic, 0, low2)),
        );

        // The high-offset load should always succeed.
        let msg_high = r_high??;
        assert_eq!(msg_high.offset(), high, "high-offset load should succeed");

        // LOW1 and LOW2 exist on the broker (infinite retention, no compaction).
        // With the bug they receive OffsetDeleted; without the bug they succeed.
        let msg_low1 = r_low1?.map_err(|e| {
            color_eyre::eyre::eyre!(
                "low1 (offset {low1}) should load successfully on a non-compacted topic, got: {e}"
            )
        })?;
        assert_eq!(msg_low1.offset(), low1);

        let msg_low2 = r_low2?.map_err(|e| {
            color_eyre::eyre::eyre!(
                "low2 (offset {low2}) should load successfully on a non-compacted topic, got: {e}"
            )
        })?;
        assert_eq!(msg_low2.offset(), low2);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
}

/// Test: Concurrent requests where the first-arriving offset is high (valid),
/// a second is low (valid), and a third is deleted — all on the same partition.
///
/// This is the realistic production scenario: deferred messages from a
/// compacted topic where some older messages have been deleted and some
/// lower-offset messages are still valid. The seek after seeing `Invalid`
/// position must land at `min_offset` (the lowest valid request), not at the
/// high-offset assignment anchor.
///
/// With the fix (`None => true`):
/// - Seek fires to `min_offset` = `low_deleted` (the lowest pending offset).
/// - `low_deleted` is below LSO; Kafka auto-resets to LSO (40).
/// - `split_off(40)` marks only `low_deleted` as deleted; `low_valid` and
///   `high_valid` remain in the active map and are fulfilled normally.
///
/// Without the fix:
/// - No seek fires (`Invalid` position → `should_seek = false`).
/// - Consumer reads from `high_valid` (the assignment anchor).
/// - `split_off(high_valid)` misidentifies `low_valid` as deleted.
#[tokio::test]
async fn test_concurrent_mixed_deleted_and_valid_lower_offsets() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    let topic_name = test_topic("mixed_deleted_valid");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0–39 (LSO becomes 40). offsets[40] is now the LSO.
        delete_records_up_to(&Topic::from(topic_name.as_str()), 40).await?;

        let topic = Topic::from(topic_name.as_str());

        // discard_threshold=50: after the seek to min_offset lands at LSO (40),
        // the consumer reads sequentially through 40→50 and 50→80 without
        // additional seeks. This avoids repeated seek round-trips on slow CI
        // runners while still exercising the critical path: the Invalid-position
        // seek fires once, Kafka resets to LSO, and split_off correctly
        // classifies deleted vs valid offsets.
        let config = LoaderConfiguration {
            bootstrap_servers: vec!["localhost:9094".to_owned()],
            group_id: "prosody-test".to_owned(),
            max_permits: 10,
            cache_size: 10,
            poll_interval: Duration::from_millis(100),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 50,
        };
        let loader = KafkaLoader::new(config, &HeartbeatRegistry::test())?;

        // high_valid (80): valid, arrives first → assignment anchored here.
        // low_deleted (20): deleted (< LSO 40), arrives second.
        // low_valid (50): valid (>= LSO 40), arrives third, below assignment anchor.
        //
        // Without the fix: consumer reads from 80, split_off(80) marks both 20
        // and 50 as deleted — 50 is a false positive.
        // With the fix: seek to min_offset=20 (below LSO → auto-reset to 40),
        // poll returns 40 (LSO, discarded), split_off(40) marks only 20 as
        // deleted. Consumer then reads sequentially 41→50 (fulfills r_valid)
        // and 51→80 (fulfills r_high). Both valid offsets succeed.
        let high_valid = offsets[80];
        let low_deleted = offsets[20];
        let low_valid = offsets[50];

        let (r_high, r_deleted, r_valid) = tokio::join!(
            timeout(
                Duration::from_secs(60),
                loader.load_message(topic, 0, high_valid)
            ),
            timeout(
                Duration::from_secs(60),
                loader.load_message(topic, 0, low_deleted)
            ),
            timeout(
                Duration::from_secs(60),
                loader.load_message(topic, 0, low_valid)
            ),
        );

        // High offset succeeds.
        let msg_high = r_high??;
        assert_eq!(msg_high.offset(), high_valid);

        // low_deleted is genuinely deleted — must get OffsetDeleted.
        let result_deleted = r_deleted?;
        let Err(KafkaLoaderError::OffsetDeleted(_, _, requested, lso)) = result_deleted else {
            color_eyre::eyre::bail!(
                "low_deleted (offset {low_deleted}) should be OffsetDeleted, got: {:?}",
                result_deleted
            );
        };
        assert_eq!(requested, low_deleted);
        assert!(
            lso >= offsets[40],
            "lso {lso} should be >= LSO boundary {}",
            offsets[40]
        );

        // low_valid exists on the broker — must succeed, not get OffsetDeleted.
        let msg_valid = r_valid?.map_err(|e| {
            color_eyre::eyre::eyre!(
                "low_valid (offset {low_valid}) should succeed on a non-deleted offset, got: {e}"
            )
        })?;
        assert_eq!(msg_valid.offset(), low_valid);

        Ok(())
    }
    .await;

    delete_topic(&topic_name).await?;
    result
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
