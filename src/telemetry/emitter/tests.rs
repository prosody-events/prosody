use super::*;
use crate::consumer::DemandType;
use crate::error::ErrorCategory;
use crate::telemetry::Telemetry;
use crate::telemetry::event::{
    KeyEvent, KeyState, MessageEventType, MessageSentEvent, MessageTelemetryEvent, PartitionEvent,
    PartitionState, TimerEventType, TimerTelemetryEvent,
};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use chrono::Utc;
use color_eyre::eyre::{Result, ensure, eyre};
use std::sync::Arc;

fn timer_event(key: &str) -> Data {
    Data::Timer(TimerTelemetryEvent {
        event_type: TimerEventType::Scheduled,
        event_time: Utc::now(),
        scheduled_time: CompactDateTime::from(1000_u32),
        timer_type: TimerType::Application,
        key: Arc::from(key),
        source: Arc::from("src"),
        trace_parent: None,
        trace_state: None,
    })
}

fn message_event(key: &str) -> Data {
    Data::Message(MessageTelemetryEvent {
        event_type: MessageEventType::Dispatched {
            demand_type: DemandType::Normal,
        },
        event_time: Utc::now(),
        offset: 0,
        key: Arc::from(key),
        source: Arc::from("src"),
        trace_parent: None,
        trace_state: None,
    })
}

fn message_sent_event(key: &str) -> Data {
    Data::MessageSent(MessageSentEvent {
        event_time: Utc::now(),
        topic: "t".into(),
        partition: 0,
        offset: 0,
        key: Arc::from(key),
        source: Arc::from("src"),
        trace_parent: None,
        trace_state: None,
    })
}

#[test]
fn serialize_event_timer_uses_payload_key() -> Result<()> {
    let data = timer_event("my-timer-key");
    ensure!(serialize_event(&data, "topic", 0, "host").is_some());
    ensure!(event_key(&data) == Some("my-timer-key"));
    Ok(())
}

#[test]
fn serialize_event_message_uses_payload_key() -> Result<()> {
    let data = message_event("my-message-key");
    ensure!(serialize_event(&data, "topic", 0, "host").is_some());
    ensure!(event_key(&data) == Some("my-message-key"));
    Ok(())
}

#[test]
fn serialize_event_message_sent_uses_payload_key() -> Result<()> {
    let data = message_sent_event("my-sent-key");
    ensure!(serialize_event(&data, "topic", 0, "host").is_some());
    ensure!(event_key(&data) == Some("my-sent-key"));
    Ok(())
}

#[test]
fn serialize_returns_none_for_internal_variants() {
    let partition = Data::Partition(PartitionEvent {
        state: PartitionState::Assigned,
    });
    assert!(serialize_event(&partition, "topic", 0, "host").is_none());

    let key = Data::Key(KeyEvent {
        key: Arc::from("k"),
        demand_type: DemandType::Normal,
        state: KeyState::HandlerInvoked,
    });
    assert!(serialize_event(&key, "topic", 0, "host").is_none());
}

fn parse_serialized(data: &Data) -> Result<serde_json::Value> {
    let bytes = serialize_event(data, "src-topic", 7, "test-host")
        .ok_or_else(|| eyre!("serialize_event returned None"))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    Ok(value)
}

#[test]
fn serialize_timer_scheduled_omits_optional_fields() -> Result<()> {
    let data = Data::Timer(TimerTelemetryEvent {
        event_type: TimerEventType::Scheduled,
        event_time: Utc::now(),
        scheduled_time: CompactDateTime::from(1_700_000_000_u32),
        timer_type: TimerType::Application,
        key: Arc::from("t-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.timer.scheduled");
    assert_eq!(v["timerType"], "application");
    assert_eq!(v["key"], "t-key");
    assert_eq!(v["hostname"], "test-host");
    ensure!(
        DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
        "eventTime not RFC 3339"
    );
    ensure!(
        DateTime::parse_from_rfc3339(v["scheduledTime"].as_str().unwrap_or("")).is_ok(),
        "scheduledTime not RFC 3339"
    );
    // Optional fields must be absent
    assert!(v.get("demandType").is_none());
    assert!(v.get("errorCategory").is_none());
    assert!(v.get("exception").is_none());
    Ok(())
}

#[test]
fn serialize_timer_cancelled_omits_optional_fields() -> Result<()> {
    let data = Data::Timer(TimerTelemetryEvent {
        event_type: TimerEventType::Cancelled,
        event_time: Utc::now(),
        scheduled_time: CompactDateTime::from(1_700_000_000_u32),
        timer_type: TimerType::Application,
        key: Arc::from("cancel-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.timer.cancelled");
    assert_eq!(v["timerType"], "application");
    assert_eq!(v["key"], "cancel-key");
    assert!(v.get("demandType").is_none());
    assert!(v.get("errorCategory").is_none());
    assert!(v.get("exception").is_none());
    ensure!(
        DateTime::parse_from_rfc3339(v["scheduledTime"].as_str().unwrap_or("")).is_ok(),
        "scheduledTime not RFC 3339"
    );
    Ok(())
}

#[test]
fn serialize_timer_failed_includes_error_fields() -> Result<()> {
    let data = Data::Timer(TimerTelemetryEvent {
        event_type: TimerEventType::Failed {
            demand_type: DemandType::Failure,
            error_category: ErrorCategory::Permanent,
            exception: "boom".into(),
        },
        event_time: Utc::now(),
        scheduled_time: CompactDateTime::from(1_700_000_000_u32),
        timer_type: TimerType::DeferredTimer,
        key: Arc::from("t-fail"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.timer.failed");
    assert_eq!(v["errorCategory"], "permanent");
    assert_eq!(v["exception"], "boom");
    assert_eq!(v["demandType"], "failure");
    assert_eq!(v["timerType"], "deferredTimer");
    Ok(())
}

#[test]
fn serialize_message_dispatched_omits_error_fields() -> Result<()> {
    let data = Data::Message(MessageTelemetryEvent {
        event_type: MessageEventType::Dispatched {
            demand_type: DemandType::Normal,
        },
        event_time: Utc::now(),
        offset: 42,
        key: Arc::from("m-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.message.dispatched");
    assert_eq!(v["demandType"], "normal");
    assert_eq!(v["offset"], 42_i32);
    assert!(v.get("errorCategory").is_none());
    assert!(v.get("exception").is_none());
    Ok(())
}

#[test]
fn serialize_message_failed_includes_error_fields() -> Result<()> {
    let data = Data::Message(MessageTelemetryEvent {
        event_type: MessageEventType::Failed {
            demand_type: DemandType::Normal,
            error_category: ErrorCategory::Transient,
            exception: "oops".into(),
        },
        event_time: Utc::now(),
        offset: 99,
        key: Arc::from("m-fail"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.message.failed");
    assert_eq!(v["errorCategory"], "transient");
    assert_eq!(v["exception"], "oops");
    assert_eq!(v["demandType"], "normal");
    Ok(())
}

#[test]
fn serialize_message_succeeded_omits_error_fields() -> Result<()> {
    let data = Data::Message(MessageTelemetryEvent {
        event_type: MessageEventType::Succeeded {
            demand_type: DemandType::Normal,
        },
        event_time: Utc::now(),
        offset: 10,
        key: Arc::from("m-ok"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.message.succeeded");
    assert_eq!(v["demandType"], "normal");
    assert_eq!(v["key"], "m-ok");
    assert_eq!(v["offset"], 10_i32);
    ensure!(
        DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
        "eventTime not RFC 3339"
    );
    assert!(v.get("errorCategory").is_none());
    assert!(v.get("exception").is_none());
    Ok(())
}

#[test]
fn serialize_message_sent_fields() -> Result<()> {
    let data = Data::MessageSent(MessageSentEvent {
        event_time: Utc::now(),
        topic: "dest-topic".into(),
        partition: 3,
        offset: 77,
        key: Arc::from("s-key"),
        source: Arc::from("producer-src"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    assert_eq!(v["type"], "prosody.message.sent");
    assert_eq!(v["key"], "s-key");
    assert_eq!(v["topic"], "dest-topic");
    assert_eq!(v["partition"], 3_i32);
    assert_eq!(v["offset"], 77_i32);
    assert_eq!(v["source"], "producer-src");
    assert_eq!(v["hostname"], "test-host");
    ensure!(
        DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
        "eventTime not RFC 3339"
    );
    Ok(())
}

#[test]
fn config_default_values() {
    let config = TelemetryEmitterConfiguration::default();
    assert_eq!(config.topic, "prosody.telemetry-events");
    assert!(config.enabled, "default should be enabled");
}

#[test]
fn config_builder_topic_override() -> Result<()> {
    let config = TelemetryEmitterConfiguration::builder()
        .topic("custom")
        .build()?;
    assert_eq!(config.topic, "custom");
    Ok(())
}

#[test]
fn config_builder_enabled_false() -> Result<()> {
    let config = TelemetryEmitterConfiguration::builder()
        .enabled(false)
        .build()?;
    ensure!(!config.enabled, "should be disabled");
    Ok(())
}

#[test]
fn spawn_emitter_disabled_returns_ok() -> Result<()> {
    let config = TelemetryEmitterConfiguration {
        topic: "test".to_owned(),
        enabled: false,
    };
    let telemetry = Telemetry::new();
    spawn_telemetry_emitter(&config, &[], &telemetry)?;
    Ok(())
}

/// Asserts that a serialized event time has millisecond precision and the
/// UTC `Z` suffix — i.e., `YYYY-MM-DDTHH:MM:SS.mmmZ`.
fn assert_millis_format(raw: &str, field: &str) -> Result<()> {
    ensure!(
        raw.ends_with('Z'),
        "{field} timezone must be Z, got: {raw:?}"
    );
    let frac = raw
        .rfind('.')
        .map(|i| &raw[i + 1..raw.len() - 1])
        .ok_or_else(|| eyre!("{field} has no fractional seconds: {raw:?}"))?;
    ensure!(
        frac.len() == 3 && frac.chars().all(|c| c.is_ascii_digit()),
        "{field} must have exactly 3 fractional digits (milliseconds), got: {raw:?}"
    );
    Ok(())
}

/// Asserts that a serialized scheduled time has second precision and the
/// UTC `Z` suffix — i.e., `YYYY-MM-DDTHH:MM:SSZ` (no fractional part).
fn assert_secs_format(raw: &str, field: &str) -> Result<()> {
    ensure!(
        raw.ends_with('Z'),
        "{field} timezone must be Z, got: {raw:?}"
    );
    ensure!(
        raw.rfind('.').is_none(),
        "{field} must have no fractional seconds (second precision), got: {raw:?}"
    );
    Ok(())
}

#[test]
fn event_time_has_millisecond_precision_and_z_suffix_timer() -> Result<()> {
    let data = Data::Timer(TimerTelemetryEvent {
        event_type: TimerEventType::Scheduled,
        event_time: Utc::now(),
        scheduled_time: CompactDateTime::from(1_700_000_000_u32),
        timer_type: TimerType::Application,
        key: Arc::from("prec-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    let event_time = v["eventTime"]
        .as_str()
        .ok_or_else(|| eyre!("eventTime missing"))?;
    let scheduled_time = v["scheduledTime"]
        .as_str()
        .ok_or_else(|| eyre!("scheduledTime missing"))?;
    assert_millis_format(event_time, "eventTime")?;
    assert_secs_format(scheduled_time, "scheduledTime")?;
    Ok(())
}

#[test]
fn event_time_has_millisecond_precision_and_z_suffix_message() -> Result<()> {
    let data = Data::Message(MessageTelemetryEvent {
        event_type: MessageEventType::Dispatched {
            demand_type: DemandType::Normal,
        },
        event_time: Utc::now(),
        offset: 1,
        key: Arc::from("prec-msg-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    let event_time = v["eventTime"]
        .as_str()
        .ok_or_else(|| eyre!("eventTime missing"))?;
    assert_millis_format(event_time, "eventTime")?;
    Ok(())
}

#[test]
fn event_time_has_millisecond_precision_and_z_suffix_message_sent() -> Result<()> {
    let data = Data::MessageSent(MessageSentEvent {
        event_time: Utc::now(),
        topic: "t".into(),
        partition: 0,
        offset: 0,
        key: Arc::from("prec-sent-key"),
        source: Arc::from("grp"),
        trace_parent: None,
        trace_state: None,
    });
    let v = parse_serialized(&data)?;

    let event_time = v["eventTime"]
        .as_str()
        .ok_or_else(|| eyre!("eventTime missing"))?;
    assert_millis_format(event_time, "eventTime")?;
    Ok(())
}

#[test]
fn serialize_and_event_key_cover_same_variants() {
    let variants: Vec<Data> = vec![
        timer_event("k"),
        message_event("k"),
        message_sent_event("k"),
        Data::Partition(PartitionEvent {
            state: PartitionState::Assigned,
        }),
        Data::Key(KeyEvent {
            key: Arc::from("k"),
            demand_type: DemandType::Normal,
            state: KeyState::HandlerInvoked,
        }),
    ];
    for data in &variants {
        assert_eq!(
            serialize_event(data, "t", 0, "h").is_some(),
            event_key(data).is_some(),
            "serialize_event and event_key disagree on {data:?}",
        );
    }
}
