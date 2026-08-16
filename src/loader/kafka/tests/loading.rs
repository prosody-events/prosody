use super::*;

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

#[tokio::test]
async fn null_payload_loads_as_excise() -> color_eyre::Result<()> {
    with_topic("excise", async |topic_name| {
        let topic = Topic::from(topic_name);
        let producer = ProsodyProducer::<JsonCodec>::new(
            &ProducerConfiguration::builder()
                .bootstrap_servers(loader_config().bootstrap_servers)
                .source_system("loader-test")
                .build()?,
            Telemetry::new().sender(),
        )?;
        producer.excise([], topic, "test-key").await?;
        let loader = KafkaLoader::<JsonCodec>::new(loader_config(), &HeartbeatRegistry::test())?;

        let message = timeout(Duration::from_mins(1), loader.load_message(topic, 0, 0)).await??;

        assert_eq!(message.key().as_ref(), "test-key");
        assert!(matches!(message.record(), Record::Excise));
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
