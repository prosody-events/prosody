use super::*;
use crate::Topic;
use futures::future::join_all;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

fn test_topic(name: &str) -> String {
    format!("loader_test_{name}_{}", uuid::Uuid::new_v4())
}

fn loader_config() -> LoaderConfiguration {
    LoaderConfiguration {
        bootstrap_servers: vec!["localhost:9094".to_owned()],
        group_id: "prosody-test".to_owned(),
        max_permits: 10,
        cache_size: 1, // Minimal size to stress test deadlock prevention and eviction
        poll_interval: Duration::from_secs(1),
        seek_timeout: Duration::from_secs(5),
        discard_threshold: 10,
    }
}

fn producer() -> color_eyre::Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("message.timeout.ms", "5000")
        .create()?)
}

fn admin() -> color_eyre::Result<AdminClient<DefaultClientContext>> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .create()?)
}

async fn create_topic(name: &str) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin
        .create_topics(
            &[NewTopic::new(name, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

async fn delete_topic(name: &str) -> color_eyre::Result<()> {
    let admin = admin()?;
    admin.delete_topics(&[name], &AdminOptions::new()).await?;
    Ok(())
}

async fn produce_messages(topic: &str, count: usize) -> color_eyre::Result<Vec<i64>> {
    let producer = producer()?;
    let mut offsets = Vec::new();

    for i in 0..count {
        // Produce JSON payload as expected by decode_message
        let payload = format!(r#"{{"test_id":{i},"data":"message-{i}"}}"#);

        let delivery = producer
            .send(
                FutureRecord::to(topic)
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

async fn delete_records_up_to(topic: &str, offset: i64) -> color_eyre::Result<()> {
    let admin = admin()?;
    let mut delete_list = TopicPartitionList::new();
    delete_list.add_partition_offset(topic, 0, rdkafka::Offset::Offset(offset))?;
    admin
        .delete_records(&delete_list, &AdminOptions::new())
        .await?;
    // Wait for Kafka to propagate the deletion across all brokers
    // Experiments show this can take time - increase to 5 seconds
    sleep(Duration::from_secs(5)).await;
    Ok(())
}

/// Test: Load a valid message successfully
#[tokio::test]
async fn test_load_valid_offset() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    crate::tracing::init_test_logging();

    let topic_name = test_topic("valid");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 10).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("sequential");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 20).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("concurrent");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 30).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = Arc::new(KafkaLoader::new(loader_config())?);

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

        // Try to load deleted offset 25
        let result = timeout(
            Duration::from_secs(10),
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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("multi_deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = Arc::new(KafkaLoader::new(loader_config())?);

        // Request deleted offsets 10, 20, and valid offset 60 concurrently
        let loader1 = Arc::clone(&loader);
        let loader2 = Arc::clone(&loader);
        let loader3 = Arc::clone(&loader);

        let offset_10 = offsets[10];
        let offset_20 = offsets[20];
        let offset_60 = offsets[60];

        let h1 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(10),
                loader1.load_message(topic, 0, offset_10),
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(10),
                loader2.load_message(topic, 0, offset_20),
            )
            .await
        });
        let h3 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(10),
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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("at_lso");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("truncated");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete partition FIRST
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("seek_recovery");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete some offsets to set up potential seek failure
        delete_records_up_to(&topic_name, 30).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("sparse");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("backwards");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

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
            poll_interval: Duration::from_secs(1),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 5, // Small threshold for testing
        };
        let loader = KafkaLoader::new(config)?;

        // Load offset 50 (position will be at 51)
        let msg1 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[50]),
        )
        .await??;
        assert_eq!(msg1.offset(), offsets[50]);

        // Load offset 55 (51 + 4 = 55, within threshold, should NOT seek)
        let msg2 = timeout(
            Duration::from_secs(10),
            loader.load_message(topic, 0, offsets[55]),
        )
        .await??;
        assert_eq!(msg2.offset(), offsets[55]);

        // Load offset 70 (56 + 14 = 70, beyond threshold, should seek)
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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("same_offset");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 10).await?;
        let topic = Topic::from(topic_name.as_str());

        let loader = Arc::new(KafkaLoader::new(loader_config())?);

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
    crate::tracing::init_test_logging();

    // This test verifies that seek failure recovery works
    // In a real multi-partition scenario, the recovery poll might return
    // a message from a different partition, which should still exit
    // the erroneous state for all partitions

    let topic_name = test_topic("recovery");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets to trigger seek failure
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = KafkaLoader::new(loader_config())?;

        // Try to load deleted offset (should trigger seek failure and recovery)
        let result = timeout(
            Duration::from_secs(15),
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
    crate::tracing::init_test_logging();

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

        let loader = KafkaLoader::new(loader_config())?;

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
    crate::tracing::init_test_logging();

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

        let loader = Arc::new(KafkaLoader::new(loader_config())?);

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
    crate::tracing::init_test_logging();

    let topic_name = test_topic("older_deleted");
    create_topic(&topic_name).await?;

    let result = async {
        let offsets = produce_messages(&topic_name, 100).await?;

        // Delete offsets 0-49 (LSO becomes 50)
        delete_records_up_to(&topic_name, 50).await?;

        let topic = Topic::from(topic_name.as_str());
        let loader = Arc::new(KafkaLoader::new(loader_config())?);

        // Launch BOTH requests simultaneously
        // This ensures both are in active before any seeking happens
        let loader1 = Arc::clone(&loader);
        let loader2 = Arc::clone(&loader);
        let offset_70 = offsets[70];
        let offset_10 = offsets[10];

        let h1 = tokio::spawn(async move {
            timeout(
                Duration::from_secs(10),
                loader1.load_message(topic, 0, offset_70),
            )
            .await
        });
        let h2 = tokio::spawn(async move {
            // Short timeout - if infinite loop, this will timeout
            timeout(
                Duration::from_secs(15),
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
    crate::tracing::init_test_logging();

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
            poll_interval: Duration::from_secs(1),
            seek_timeout: Duration::from_secs(5),
            discard_threshold: 10,
        };
        let loader = Arc::new(KafkaLoader::new(config)?);

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
