use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn message_lifecycle_events_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client = build_client(&source_topic, &telemetry_topic, true).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        client
            .subscribe(FallibleTestHandler {
                messages_tx: msg_tx,
            })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client.send(source, "test-key", json!({"v": 1_i32})).await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let events = collect_events_for_key(
            &telemetry_consumer,
            "test-key",
            "prosody.message.",
            RECEIVE_TIMEOUT,
            |evts| has_message_lifecycle(evts, true),
        )
        .await?;
        assert_message_two_event_invariant(&events, true)?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn producer_message_sent_on_kafka() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, dest_topic) = create_telemetry_topics().await?;
        let dest: Topic = dest_topic.as_str().into();

        let client = build_client(&dest_topic, &telemetry_topic, true).await?;
        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client.send(dest, "sent-key", json!({"v": 1_i32})).await?;

        let sent = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.sent",
            RECEIVE_TIMEOUT,
        )
        .await?;

        assert_eq!(
            sent.get("type").and_then(Value::as_str),
            Some("prosody.message.sent")
        );
        assert_eq!(sent.get("key").and_then(Value::as_str), Some("sent-key"));
        assert!(
            sent.get("offset").and_then(Value::as_i64).is_some(),
            "sent event should have offset"
        );
        assert!(
            sent.get("source").and_then(Value::as_str).is_some(),
            "sent event should have source"
        );

        let sent_event_time = sent
            .get("eventTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("sent: missing eventTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(sent_event_time).is_ok(),
            "sent: eventTime not RFC 3339: {sent_event_time}"
        );
        ensure!(
            sent.get("topic")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "sent: topic should be a non-empty string"
        );
        ensure!(
            sent.get("partition").and_then(Value::as_i64).is_some(),
            "sent: partition should be an integer"
        );
        ensure!(
            sent.get("hostname")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty()),
            "sent: hostname should be non-empty"
        );

        admin.delete_topic(&dest_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn emitter_disabled_no_events() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client = build_client(&source_topic, &telemetry_topic, false).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        client
            .subscribe(FallibleTestHandler {
                messages_tx: msg_tx,
            })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "no-emit-key", json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        assert_no_telemetry_events(&telemetry_consumer, Duration::from_secs(5)).await?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}

#[tokio::test(flavor = "multi_thread")]
async fn json_payload_contract_validation() -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        init_test_logging();

        let (admin, telemetry_topic, source_topic) = create_telemetry_topics().await?;
        let source: Topic = source_topic.as_str().into();

        let client = build_client(&source_topic, &telemetry_topic, true).await?;

        let (msg_tx, mut msg_rx) = channel(16);
        client
            .subscribe(FallibleTestHandler {
                messages_tx: msg_tx,
            })
            .await?;

        let telemetry_consumer = create_telemetry_consumer(&telemetry_topic)?;

        client
            .send(source, "contract-key", json!({"v": 1_i32}))
            .await?;
        let _ = timeout(RECEIVE_TIMEOUT, msg_rx.recv()).await?;

        let event = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.dispatched",
            RECEIVE_TIMEOUT,
        )
        .await?;

        // type
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("prosody.message.dispatched"),
            "type field mismatch"
        );

        // eventTime (RFC 3339)
        let event_time = event
            .get("eventTime")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing eventTime"))?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
            "eventTime is not valid RFC 3339: {event_time}"
        );

        // offset (integer)
        ensure!(
            event.get("offset").and_then(Value::as_i64).is_some(),
            "offset should be an integer"
        );

        // topic (string)
        ensure!(
            event.get("topic").and_then(Value::as_str).is_some(),
            "topic should be a string"
        );

        // partition (integer)
        ensure!(
            event.get("partition").and_then(Value::as_i64).is_some(),
            "partition should be an integer"
        );

        // key
        assert_eq!(
            event.get("key").and_then(Value::as_str),
            Some("contract-key"),
            "key field mismatch"
        );

        // source (non-empty string)
        let source_val = event
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing source"))?;
        ensure!(!source_val.is_empty(), "source should be non-empty");

        // hostname (non-empty string)
        let hostname = event
            .get("hostname")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing hostname"))?;
        ensure!(!hostname.is_empty(), "hostname should be non-empty");

        // demandType (one of "normal"/"failure")
        let demand_type = event
            .get("demandType")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("missing demandType"))?;
        ensure!(
            demand_type == "normal" || demand_type == "failure",
            "demandType should be 'normal' or 'failure', got: {demand_type}"
        );

        // Error fields should NOT be present on dispatched
        ensure!(
            event.get("errorCategory").is_none(),
            "errorCategory should be absent on dispatched event"
        );
        ensure!(
            event.get("exception").is_none(),
            "exception should be absent on dispatched event"
        );

        // ── succeeded event contract ──
        let succeeded = consume_telemetry_event_by_type(
            &telemetry_consumer,
            "prosody.message.succeeded",
            RECEIVE_TIMEOUT,
        )
        .await?;
        assert_succeeded_contract(&succeeded, "contract-key")?;

        client.unsubscribe().await?;
        admin.delete_topic(&source_topic).await?;
        admin.delete_topic(&telemetry_topic).await?;
        Ok(())
    })
    .await
    .map_err(|_| eyre!("test timed out after {TEST_TIMEOUT:?}"))?
}
mod timer_events;
