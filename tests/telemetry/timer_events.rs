use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn message_failed_event_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client: CassandraHighLevelClient<FailingHandler> =
            build_typed_client(&source_topic, &telemetry_topic).await?;

        let (fail_tx, mut fail_rx) = channel(16);
        client.subscribe(FailingHandler { tx: fail_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client.send(source, "fail-key", json!({"v": 1_i32})).await?;
        let _ = timeout(RECEIVE_TIMEOUT, fail_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "fail-key",
            "prosody.message.",
            RECEIVE_TIMEOUT,
            |evts| has_message_lifecycle(evts, false),
        )
        .await?;
        assert_message_two_event_invariant(&events, false)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_lifecycle_events_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client: CassandraHighLevelClient<TimerSchedulingHandler> =
            build_typed_client(&source_topic, &telemetry_topic).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (timer_tx, mut timer_rx) = channel(16);
        client
            .subscribe(TimerSchedulingHandler { msg_tx, timer_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "timer-key", json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (timer scheduled inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for timer handler to fire (~3 s delay)
        let _ = timeout(RECEIVE_TIMEOUT, timer_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "timer-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.succeeded", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "application", true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_failed_event_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client: CassandraHighLevelClient<TimerFailingHandler> =
            build_typed_client(&source_topic, &telemetry_topic).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (timer_tx, mut timer_rx) = channel(16);
        client
            .subscribe(TimerFailingHandler { msg_tx, timer_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "timer-fail-key", json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (timer scheduled inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for timer handler to fire (~3 s delay)
        let _ = timeout(RECEIVE_TIMEOUT, timer_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "timer-fail-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.failed", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "application", false)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_message_timer_three_event_invariant() -> Result<()> {
    Box::pin(timeout(DEFER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let mut defer = DeferConfigurationBuilder::default();
        defer.failure_threshold(1.0_f64);
        let client: CassandraHighLevelClient<TransientMessageHandler> =
            build_typed_client_with_defer(&source_topic, &telemetry_topic, defer).await?;

        let (done_tx, mut done_rx) = channel(16);
        client
            .subscribe(TransientMessageHandler { done_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // Seed the FailureTracker with successes so failure_rate < 1.0 when
        // the transient failure arrives and deferral is enabled.
        for i in 0_i32..3_i32 {
            client
                .send(source, &format!("warmup-{i}"), json!({"v": 0_i32}))
                .await?;
            let _ = timeout(RECEIVE_TIMEOUT, done_rx.recv()).await?;
        }

        client
            .send(source, "defer-msg-key", json!({"v": 1_i32}))
            .await?;

        // Wait for the retry to succeed
        let _ = timeout(RECEIVE_TIMEOUT, done_rx.recv()).await?;

        // Collect all timer events — there must be a full scheduled → dispatched
        // → succeeded lifecycle for the deferredMessage timer.
        let events = collect_events_for_key(
            &telemetry_consumer,
            "defer-msg-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.dispatched", 1)
                    && has_at_least(evts, "prosody.timer.succeeded", 1)
            },
        )
        .await?;
        assert_timer_three_event_invariant(&events, "deferredMessage", true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    }))
    .await
    .map_err(|_| eyre!("test timed out after {DEFER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_timer_timer_three_event_invariant() -> Result<()> {
    Box::pin(timeout(DEFER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let mut defer = DeferConfigurationBuilder::default();
        defer.failure_threshold(1.0_f64);
        let client: CassandraHighLevelClient<TransientTimerHandler> =
            build_typed_client_with_defer(&source_topic, &telemetry_topic, defer).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        let (done_tx, mut done_rx) = channel(16);
        client
            .subscribe(TransientTimerHandler { msg_tx, done_tx })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // Seed the FailureTracker with successes so failure_rate < 1.0 when
        // the transient timer failure arrives and deferral is enabled.
        for i in 0_i32..3_i32 {
            client
                .send(source, &format!("warmup-{i}"), json!({"v": 0_i32}))
                .await?;
            let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;
        }

        client
            .send(source, "defer-timer-key", json!({"v": 1_i32}))
            .await?;

        // Wait for the message handler to schedule the application timer
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Wait for the deferred timer retry to succeed (~3 s application timer
        // + defer backoff)
        let _ = timeout(RECEIVE_TIMEOUT, done_rx.recv()).await?;

        // Collect timer events until both the application and deferredTimer
        // lifecycles are complete (each has scheduled + dispatched + terminal).
        let all_events = collect_events_for_key(
            &telemetry_consumer,
            "defer-timer-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_timer_lifecycle(evts, "application")
                    && has_timer_lifecycle(evts, "deferredTimer")
            },
        )
        .await?;
        assert_timer_three_event_invariant(&all_events, "application", true)?;

        // The deferredTimer retry timer must also have its three events.
        assert_timer_three_event_invariant(&all_events, "deferredTimer", true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    }))
    .await
    .map_err(|_| eyre!("test timed out after {DEFER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_cancelled_event_on_kafka() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client: CassandraHighLevelClient<TimerCancellingHandler> =
            build_typed_client(&source_topic, &telemetry_topic).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(TimerCancellingHandler { msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "cancel-key", json!({"v": 1_i32}))
            .await?;

        // Wait for message handler to complete (schedule + cancel inside)
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "cancel-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 1)
                    && has_at_least(evts, "prosody.timer.cancelled", 1)
            },
        )
        .await?;

        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("type").and_then(Value::as_str))
            .collect();

        ensure!(
            types.contains(&"prosody.timer.scheduled"),
            "missing prosody.timer.scheduled; got: {types:?}"
        );
        ensure!(
            types.contains(&"prosody.timer.cancelled"),
            "missing prosody.timer.cancelled; got: {types:?}"
        );
        ensure!(
            !types.contains(&"prosody.timer.dispatched"),
            "prosody.timer.dispatched should NOT be present (timer was cancelled); got: {types:?}"
        );

        // Validate field contract on the cancelled event
        let cancelled = events
            .iter()
            .find(|e| e.get("type").and_then(Value::as_str) == Some("prosody.timer.cancelled"))
            .ok_or_else(|| eyre!("cancelled event not found"))?;
        assert_timer_cancelled_contract(cancelled, "cancel-key")?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn clear_and_schedule_emits_cancelled_and_scheduled() -> Result<()> {
    timeout(TIMER_TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client: CassandraHighLevelClient<ClearAndScheduleHandler> =
            build_typed_client(&source_topic, &telemetry_topic).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        client.subscribe(ClearAndScheduleHandler { msg_tx }).await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        // First message: schedule a timer at t+60
        client
            .send(source, "cas-key", json!({"step": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        // Second message (same key): clear_and_schedule at t+120
        client
            .send(source, "cas-key", json!({"step": 2_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "cas-key",
            "prosody.timer.",
            RECEIVE_TIMEOUT,
            |evts| {
                has_at_least(evts, "prosody.timer.scheduled", 2)
                    && has_at_least(evts, "prosody.timer.cancelled", 1)
            },
        )
        .await?;

        let types: Vec<&str> = events
            .iter()
            .filter_map(|e| e.get("type").and_then(Value::as_str))
            .collect();

        // Should have: scheduled (first), cancelled (from clear), scheduled (new)
        let scheduled_count = types
            .iter()
            .filter(|&&t| t == "prosody.timer.scheduled")
            .count();
        let cancelled_count = types
            .iter()
            .filter(|&&t| t == "prosody.timer.cancelled")
            .count();

        ensure!(
            scheduled_count >= 2,
            "expected at least 2 scheduled events (original + new); got {scheduled_count}; types: \
             {types:?}"
        );
        ensure!(
            cancelled_count >= 1,
            "expected at least 1 cancelled event (old timer cleared); got {cancelled_count}; \
             types: {types:?}"
        );

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TIMER_TEST_TIMEOUT:?}"))?
}
