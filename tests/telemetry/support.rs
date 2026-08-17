use super::*;

pub(super) fn bootstrap_servers() -> Vec<String> {
    vec![BOOTSTRAP.to_owned()]
}

pub(super) async fn create_topic(admin: &ProsodyAdminClient, name: &str) -> Result<()> {
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(name.to_owned())
                .partition_count(1_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;
    Ok(())
}

/// Mints a fresh telemetry topic and a second (source/destination) topic, both
/// single-partition, and returns the shared admin client alongside their names.
pub(super) async fn create_telemetry_topics()
-> Result<(&'static ProsodyAdminClient, String, String)> {
    let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers())?)?;
    let telemetry_topic = Uuid::new_v4().to_string();
    let topic = Uuid::new_v4().to_string();
    create_topic(admin, &telemetry_topic).await?;
    create_topic(admin, &topic).await?;
    Ok((admin, telemetry_topic, topic))
}

pub(super) fn create_telemetry_consumer(telemetry_topic: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", Uuid::new_v4().to_string())
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;
    consumer.subscribe(&[telemetry_topic])?;
    Ok(consumer)
}

/// Reads telemetry events from Kafka until finding one matching `type_str`.
pub(super) async fn consume_telemetry_event_by_type(
    consumer: &StreamConsumer,
    type_str: &str,
    deadline: Duration,
) -> Result<Value> {
    let start = Instant::now();
    loop {
        let remaining = deadline
            .checked_sub(start.elapsed())
            .ok_or_else(|| eyre!("timeout waiting for telemetry event type={type_str}"))?;

        let msg = timeout(remaining, consumer.recv()).await??;
        let payload = msg
            .payload()
            .ok_or_else(|| eyre!("telemetry message has no payload"))?;
        let value: Value = serde_json::from_slice(payload)?;
        if value.get("type").and_then(Value::as_str) == Some(type_str) {
            return Ok(value);
        }
    }
}

/// Validates the field contract for a `prosody.message.succeeded` event.
pub(super) fn assert_succeeded_contract(succeeded: &Value, expected_key: &str) -> Result<()> {
    assert_eq!(
        succeeded.get("type").and_then(Value::as_str),
        Some("prosody.message.succeeded"),
        "succeeded type mismatch"
    );
    let event_time = succeeded
        .get("eventTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("succeeded: missing eventTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
        "succeeded: eventTime not RFC 3339: {event_time}"
    );
    ensure!(
        succeeded.get("offset").and_then(Value::as_i64).is_some(),
        "succeeded: offset should be an integer"
    );
    ensure!(
        succeeded.get("topic").and_then(Value::as_str).is_some(),
        "succeeded: topic should be a string"
    );
    ensure!(
        succeeded.get("partition").and_then(Value::as_i64).is_some(),
        "succeeded: partition should be an integer"
    );
    assert_eq!(
        succeeded.get("key").and_then(Value::as_str),
        Some(expected_key),
        "succeeded: key mismatch"
    );
    ensure!(
        succeeded
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "succeeded: source should be non-empty"
    );
    ensure!(
        succeeded
            .get("hostname")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "succeeded: hostname should be non-empty"
    );
    let demand = succeeded
        .get("demandType")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("succeeded: missing demandType"))?;
    ensure!(
        demand == "normal" || demand == "failure",
        "succeeded: demandType invalid: {demand}"
    );
    ensure!(
        succeeded.get("errorCategory").is_none(),
        "succeeded: errorCategory should be absent"
    );
    ensure!(
        succeeded.get("exception").is_none(),
        "succeeded: exception should be absent"
    );
    Ok(())
}

/// Collects telemetry events whose `key` field matches `key` and whose
/// `type` starts with `type_prefix` (`"prosody.timer."` or
/// `"prosody.message."`) until `done` returns `true` for the accumulated set.
///
/// Each individual `recv` is guarded by `per_event_timeout`.
pub(super) async fn collect_events_for_key(
    consumer: &StreamConsumer,
    key: &str,
    type_prefix: &str,
    per_event_timeout: Duration,
    done: impl Fn(&[Value]) -> bool,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    loop {
        if done(&events) {
            break;
        }
        let msg = timeout(per_event_timeout, consumer.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "timed out waiting for {type_prefix}* events for key {key:?}; collected so \
                     far: {events:?}",
                )
            })?
            .map_err(|e| eyre!("consumer error: {e}"))?;
        let Some(payload) = msg.payload() else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(payload) else {
            continue;
        };
        let matches_key = value.get("key").and_then(Value::as_str) == Some(key);
        let matches_type = value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.starts_with(type_prefix));
        if matches_key && matches_type {
            events.push(value);
        }
    }
    Ok(events)
}

/// Returns the number of events whose `type` field equals `event_type`.
pub(super) fn count_type(events: &[Value], event_type: &str) -> usize {
    events
        .iter()
        .filter(|e| e.get("type").and_then(Value::as_str) == Some(event_type))
        .count()
}

/// Returns `true` when `events` contains at least `n` events whose
/// `type` field equals `event_type`.
pub(super) fn has_at_least(events: &[Value], event_type: &str, n: usize) -> bool {
    count_type(events, event_type) >= n
}

/// Returns `true` when `events` contains the two-event message invariant:
/// dispatched + succeeded (or dispatched + failed).
pub(super) fn has_message_lifecycle(events: &[Value], expect_success: bool) -> bool {
    let has_dispatched = has_at_least(events, "prosody.message.dispatched", 1);
    let has_outcome = if expect_success {
        has_at_least(events, "prosody.message.succeeded", 1)
    } else {
        has_at_least(events, "prosody.message.failed", 1)
    };
    has_dispatched && has_outcome
}

/// Returns `true` when `events` contains the three-event invariant
/// (scheduled, dispatched, succeeded/failed) for `timer_type`.
pub(super) fn has_timer_lifecycle(events: &[Value], timer_type: &str) -> bool {
    let matching: Vec<&str> = events
        .iter()
        .filter(|e| e.get("timerType").and_then(Value::as_str) == Some(timer_type))
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();
    matching.contains(&"prosody.timer.scheduled")
        && matching.contains(&"prosody.timer.dispatched")
        && (matching.contains(&"prosody.timer.succeeded")
            || matching.contains(&"prosody.timer.failed"))
}

/// Asserts the three-event invariant for a timer: scheduled → dispatched →
/// succeeded/failed, all with the expected `timerType`.
///
/// Filters the event slice to only those matching `expected_timer_type` before
/// checking, so mixed-type slices (e.g. both `application` and `deferredTimer`
/// events) can each be validated independently.
pub(super) fn assert_timer_three_event_invariant(
    events: &[Value],
    expected_timer_type: &str,
    expect_success: bool,
) -> Result<()> {
    let matching: Vec<&Value> = events
        .iter()
        .filter(|e| e.get("timerType").and_then(Value::as_str) == Some(expected_timer_type))
        .collect();

    let types: Vec<&str> = matching
        .iter()
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();

    ensure!(
        types.contains(&"prosody.timer.scheduled"),
        "missing prosody.timer.scheduled for timerType={expected_timer_type}; got: {types:?}"
    );
    ensure!(
        types.contains(&"prosody.timer.dispatched"),
        "missing prosody.timer.dispatched for timerType={expected_timer_type}; got: {types:?}"
    );
    if expect_success {
        ensure!(
            types.contains(&"prosody.timer.succeeded"),
            "missing prosody.timer.succeeded for timerType={expected_timer_type}; got: {types:?}"
        );
    } else {
        ensure!(
            types.contains(&"prosody.timer.failed"),
            "missing prosody.timer.failed for timerType={expected_timer_type}; got: {types:?}"
        );
    }

    Ok(())
}

/// Validates the JSON field contract for a `prosody.timer.cancelled` event.
pub(super) fn assert_timer_cancelled_contract(event: &Value, expected_key: &str) -> Result<()> {
    assert_eq!(
        event.get("type").and_then(Value::as_str),
        Some("prosody.timer.cancelled"),
        "cancelled type mismatch"
    );
    let event_time = event
        .get("eventTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("cancelled: missing eventTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(event_time).is_ok(),
        "cancelled: eventTime not RFC 3339: {event_time}"
    );
    let scheduled_time = event
        .get("scheduledTime")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("cancelled: missing scheduledTime"))?;
    ensure!(
        chrono::DateTime::parse_from_rfc3339(scheduled_time).is_ok(),
        "cancelled: scheduledTime not RFC 3339: {scheduled_time}"
    );
    ensure!(
        event
            .get("timerType")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: timerType should be non-empty"
    );
    assert_eq!(
        event.get("key").and_then(Value::as_str),
        Some(expected_key),
        "cancelled: key mismatch"
    );
    ensure!(
        event
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: source should be non-empty"
    );
    ensure!(
        event
            .get("hostname")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        "cancelled: hostname should be non-empty"
    );
    ensure!(
        event.get("demandType").is_none(),
        "cancelled: demandType should be absent"
    );
    ensure!(
        event.get("errorCategory").is_none(),
        "cancelled: errorCategory should be absent"
    );
    ensure!(
        event.get("exception").is_none(),
        "cancelled: exception should be absent"
    );
    Ok(())
}

/// Asserts the two-event invariant for a message: dispatched →
/// succeeded/failed.
pub(super) fn assert_message_two_event_invariant(
    events: &[Value],
    expect_success: bool,
) -> Result<()> {
    let types: Vec<&str> = events
        .iter()
        .filter_map(|e| e.get("type").and_then(Value::as_str))
        .collect();

    ensure!(
        types.contains(&"prosody.message.dispatched"),
        "missing prosody.message.dispatched; got: {types:?}"
    );
    if expect_success {
        ensure!(
            types.contains(&"prosody.message.succeeded"),
            "missing prosody.message.succeeded; got: {types:?}"
        );
    } else {
        ensure!(
            types.contains(&"prosody.message.failed"),
            "missing prosody.message.failed; got: {types:?}"
        );
    }

    Ok(())
}

/// Asserts no telemetry events arrive within the given duration.
pub(super) async fn assert_no_telemetry_events(
    consumer: &StreamConsumer,
    wait: Duration,
) -> Result<()> {
    let result = timeout(wait, consumer.recv()).await;
    ensure!(
        result.is_err(),
        "expected no telemetry events but received one"
    );
    Ok(())
}

/// Build a `HighLevelClient` in the given mode with a custom telemetry topic.
pub(super) async fn build_client_with<T: ClientHandler<Payload = Value>>(
    mode: Mode,
    source_topic: &str,
    telemetry_topic: &str,
    emitter_enabled: bool,
    defer: DeferConfigurationBuilder,
) -> Result<CassandraHighLevelClient<T>> {
    let mut producer_builder = ProducerConfigurationBuilder::default();
    producer_builder
        .bootstrap_servers(bootstrap_servers())
        .source_system("test-telemetry");

    let mut consumer_builder = ConsumerConfigurationBuilder::default();
    consumer_builder
        .bootstrap_servers(bootstrap_servers())
        .group_id(Uuid::new_v4().to_string())
        .subscribed_topics(vec![source_topic.to_owned()])
        .probe_port(None);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_builder,
        defer,
        emitter: TelemetryEmitterConfiguration {
            topic: telemetry_topic.to_owned(),
            enabled: emitter_enabled,
        },
        peer: common::test_peer_config()?,
        ..ConsumerBuilders::new()?
    };

    let mut cassandra_builder = CassandraConfigurationBuilder::default();
    cassandra_builder.nodes(vec![CASSANDRA_HOST.to_owned()]);

    let client = CassandraHighLevelClient::new(
        cassandra_builder.build()?,
        mode,
        &mut producer_builder,
        &consumer_builders,
    )
    .await?;
    Ok(client)
}

/// Best-effort-mode client using the shared forward-to-channel handler.
pub(super) async fn build_client(
    source_topic: &str,
    telemetry_topic: &str,
    emitter_enabled: bool,
) -> Result<CassandraHighLevelClient<FallibleTestHandler>> {
    build_client_with(
        Mode::BestEffort,
        source_topic,
        telemetry_topic,
        emitter_enabled,
        DeferConfigurationBuilder::default(),
    )
    .await
}

pub(super) async fn build_typed_client<T: ClientHandler<Payload = Value>>(
    source_topic: &str,
    telemetry_topic: &str,
) -> Result<CassandraHighLevelClient<T>> {
    build_client_with(
        Mode::Pipeline,
        source_topic,
        telemetry_topic,
        true,
        DeferConfigurationBuilder::default(),
    )
    .await
}

pub(super) async fn build_typed_client_with_defer<T: ClientHandler<Payload = Value>>(
    source_topic: &str,
    telemetry_topic: &str,
    defer: DeferConfigurationBuilder,
) -> Result<CassandraHighLevelClient<T>> {
    build_client_with(Mode::Pipeline, source_topic, telemetry_topic, true, defer).await
}

// ── Integration Tests ────────────────────────────────────────────────────────
