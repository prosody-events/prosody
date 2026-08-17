use super::*;

async fn run_test<F, Fut>(test_name: &str, test_fn: F) -> Result<()>
where
    F: FnOnce(TestEnvironment) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    init_test_logging();

    let result = timeout(TIMER_TEST_TIMEOUT, async {
        let env = TestEnvironment::new(test_name).await?;

        test_fn(env).await
    })
    .await;

    result.map_err(|_| eyre!("Test '{test_name}' timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

/// Build a `HighLevelClient` for the inline-replacement test — the one test
/// in this file that must drive `clear_and_schedule` through the full
/// pipeline-mode middleware stack rather than the direct-consumer harness.
async fn build_inline_replacement_client(
    source_topic: &str,
    telemetry_topic: &str,
) -> Result<CassandraHighLevelClient<InlineReplacementHandler>> {
    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .source_system("timer-inline-replacement-test");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(vec!["localhost:9094".to_owned()])
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(vec![source_topic.to_owned()])
        .probe_port(None);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: true,
        },
        peer: common::test_peer_config()?,
        ..ConsumerBuilders::new()?
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec!["localhost:9042".to_owned()]);

    let client = CassandraHighLevelClient::new(
        cassandra_builder.build()?,
        Mode::Pipeline,
        &mut producer_builder,
        &consumer_builders,
    )
    .await?;
    Ok(client)
}

/// Tests basic timer scheduling and triggering functionality.
#[tokio::test]
async fn test_timer_scheduling_and_triggering() -> Result<()> {
    run_test("timer-test", |mut env| async move {
        let key = "test-key";
        let delay_secs = 1u32;
        let schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });

        // Schedule the timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        let message_event = env.expect_message().await?;
        TestEnvironment::verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests edge case: scheduling multiple timers for the same key.
#[tokio::test]
async fn test_same_key_multiple_timers() -> Result<()> {
    run_test("timer-same-key-test", |mut env| async move {
        let key = "same-key";
        let delays = vec![1u32, 2u32, 3u32];

        // Schedule multiple timers for the same key
        for delay_secs in &delays {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..delays.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i + 1, e))?;
        }

        // Wait for all timer events
        let received_timers = env.expect_timers(delays.len()).await?;

        // Verify all timers are for the same key
        for timer_event in &received_timers {
            TestEnvironment::verify_timer_event(timer_event, key)?;
        }

        // Verify timers triggered in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests immediate timer scheduling (1 second delay).
#[tokio::test]
async fn test_immediate_timer() -> Result<()> {
    run_test("timer-immediate-test", |mut env| async move {
        let key = "immediate-key";
        let delay_secs = 1u32;

        let start_time = CompactDateTime::now()?;

        // Schedule immediate timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify message was received
        env.expect_message().await?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Verify timing accuracy (should trigger within 1-2 seconds)
        let end_time = CompactDateTime::now()?;
        let elapsed = end_time.epoch_seconds() - start_time.epoch_seconds();
        ensure!(
            (1..=3).contains(&elapsed),
            "Timer took {} seconds, expected 1-3 seconds",
            elapsed
        );

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_timer_scheduled_time_accuracy() -> Result<()> {
    run_test("timer-accuracy-test", |mut env| async move {
        let key = "accuracy-key";

        // Calculate a specific target time (2 seconds from now)
        let target_time_secs = u64::from(
            CompactDateTime::now()
                .map_err(|e| eyre!("Failed to get current time: {e}"))?
                .add_duration(CompactDuration::new(2))
                .map_err(|e| eyre!("Failed to add duration: {e}"))?
                .epoch_seconds(),
        );

        let expected_time = CompactDateTime::from(target_time_secs as u32);
        let schedule_message = json!({
            "action": "schedule_timer",
            "target_time_secs": target_time_secs
        });

        // Schedule timer at specific absolute time
        env.schedule_timer_at(key, target_time_secs).await?;

        // Verify message was received
        let message_event = env.expect_message().await?;
        TestEnvironment::verify_message_event(&message_event, key, &schedule_message)?;

        // Wait for timer to trigger
        let timer_event = env.expect_timer().await?;
        TestEnvironment::verify_timer_event(&timer_event, key)?;

        // Verify timer accuracy - should match exactly
        ensure!(
            timer_event.time == expected_time,
            "Timer triggered at different time than scheduled. Expected: {}, Actual: {}",
            expected_time,
            timer_event.time
        );

        // Additional verification: timer should have triggered at the right wall-clock
        // time
        let actual_trigger_time = CompactDateTime::now()?;
        let time_diff = actual_trigger_time
            .epoch_seconds()
            .abs_diff(expected_time.epoch_seconds());
        ensure!(
            time_diff <= 1,
            "Timer triggered too far from expected time. Expected: {}, Now: {}, Diff: {} seconds",
            expected_time,
            actual_trigger_time,
            time_diff
        );

        info!(
            "✓ Timer accuracy test passed: scheduled time {} matches trigger time {}",
            expected_time, timer_event.time
        );

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests timer cancellation functionality.
#[tokio::test]
async fn test_timer_cancellation() -> Result<()> {
    run_test("timer-cancellation-test", |mut env| async move {
        let key = "cancellation-key";
        let delay_secs = 3u32;

        // Schedule a timer
        env.schedule_timer(key, delay_secs).await?;

        // Verify schedule message was received
        let message_event = env.expect_message().await?;
        let expected_schedule_message = json!({
            "action": "schedule_timer",
            "delay_secs": delay_secs
        });
        TestEnvironment::verify_message_event(&message_event, key, &expected_schedule_message)?;

        // Cancel the timer
        env.cancel_timer(key).await?;

        // Verify cancellation message was received
        let cancel_message_event = env.expect_message().await?;
        let expected_cancel_message = json!({
            "action": "cancel_timer"
        });
        TestEnvironment::verify_message_event(
            &cancel_message_event,
            key,
            &expected_cancel_message,
        )?;

        // Verify no timer fires (wait longer than the original delay)
        env.expect_no_timer(delay_secs + 2).await?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests multiple timers with different keys and timing.
#[tokio::test]
async fn test_multiple_timers() -> Result<()> {
    run_test("timer-multiple-test", |mut env| async move {
        // Schedule multiple timers with staggered delays
        let timers_data = vec![("key1", 1u32), ("key2", 2u32), ("key3", 3u32)];

        for (key, delay_secs) in &timers_data {
            env.schedule_timer(key, *delay_secs).await?;
        }

        // Verify all schedule messages were received
        for i in 0..timers_data.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Collect timer events as they trigger
        let received_timers = env.expect_timers(timers_data.len()).await?;

        // Verify timers are in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Verify all expected keys are present
        let expected_keys: HashSet<String> =
            timers_data.iter().map(|(k, _)| (*k).to_owned()).collect();
        TestEnvironment::verify_timer_keys(&received_timers, &expected_keys)?;

        // Log all received timer events
        for timer_event in &received_timers {
            info!(
                "Timer for key {} triggered with scheduled time: {}",
                timer_event.key, timer_event.time
            );
        }

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Tests timer behavior for different keys.
#[tokio::test]
async fn test_timer_different_keys() -> Result<()> {
    run_test("timer-keys-test", |mut env| async move {
        // Schedule timers for different keys
        let timers = vec!["key-a", "key-b"];
        let delay_secs = 2u32;

        for key in &timers {
            env.schedule_timer(key, delay_secs).await?;
        }

        // Verify schedule messages were received
        for i in 0..timers.len() {
            env.expect_message()
                .await
                .map_err(|e| eyre!("Failed to receive schedule message {}: {}", i, e))?;
        }

        // Wait for timers to trigger
        let received_timers = env.expect_timers(2).await?;

        // Verify timers are in chronological order
        TestEnvironment::verify_timer_order(&received_timers)?;

        // Verify both expected keys are present
        let expected_keys: HashSet<String> = timers.iter().map(|k| (*k).to_owned()).collect();
        TestEnvironment::verify_timer_keys(&received_timers, &expected_keys)?;

        // Clean up
        env.cleanup().await;
        Ok(())
    })
    .await
}

/// Verifies the Inline→Inline tombstone-free timer replacement path:
/// `schedule` followed by `clear_and_schedule` on the same key results in
/// exactly one timer firing at the replacement time.
#[tokio::test(flavor = "multi_thread")]
async fn inline_replacement_fires_once_at_replacement_time() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (source, admin) = common::kafka::create_topic_with_partitions(1).await?;
        let (telemetry_topic, _) = common::kafka::create_topic_with_partitions(1).await?;
        let source_topic = source.to_string();

        let client: CassandraHighLevelClient<InlineReplacementHandler> =
            build_inline_replacement_client(&source_topic, telemetry_topic.as_ref()).await?;

        let (messages, mut msg_rx) = channel(16);
        let (replacement_time, mut replacement_time_rx) = channel(16);
        let (timer_fired, mut timer_rx) = channel(16);
        client
            .subscribe(InlineReplacementHandler {
                messages,
                replacement_time,
                timer_fired,
            })
            .await?;

        // Step 1: schedule at t+3s
        client
            .send(source, "replace-key", json!({"step": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Step 2: clear_and_schedule at t+5s (replaces the original)
        client
            .send(source, "replace-key", json!({"step": 2_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Capture the replacement time the handler recorded
        let replacement_time = timeout(RECEIVE_TIMEOUT, replacement_time_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for replacement time"))?
            .ok_or_else(|| eyre!("replacement_time channel closed"))?;

        // Wait for the timer to fire (should be ~5s from step 2)
        let trigger_time = timeout(RECEIVE_TIMEOUT, timer_rx.recv())
            .await
            .map_err(|_| eyre!("timeout waiting for on_timer — timer never fired"))?
            .ok_or_else(|| eyre!("timer_fired channel closed"))?;

        // The timer must fire at the replacement time, not the original
        ensure!(
            trigger_time == replacement_time,
            "timer fired at {trigger_time:?} but expected replacement time {replacement_time:?}"
        );

        // Verify no second timer fires (the original t+3s was replaced)
        let second = timeout(Duration::from_secs(5), timer_rx.recv()).await;
        ensure!(second.is_err(), "expected no second timer but received one");

        client.unsubscribe().await?;
        admin.delete_topic(&source).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}
