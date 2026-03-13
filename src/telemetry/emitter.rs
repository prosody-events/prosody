//! Background Kafka emitter for telemetry events.
//!
//! Subscribes to the internal broadcast channel, serializes events to JSON,
//! and produces them concurrently to a configured Kafka topic using
//! `buffer_unordered`.

use crate::Key;
use crate::consumer::DemandType;
use crate::error::ErrorCategory;
use crate::telemetry::Telemetry;
use crate::telemetry::event::{
    Data, MessageEventType, MessageSentEvent, MessageTelemetryEvent, TimerEventType,
    TimerTelemetryEvent,
};
use crate::timers::TimerType;
use crate::util::from_env_with_fallback;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use derive_builder::Builder;
use futures::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::cell::RefCell;
use std::sync::Arc;
use thiserror::Error;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tracing::warn;
use validator::Validate;
use whoami::hostname;

#[cfg(target_arch = "arm")]
use serde_json as json;

#[cfg(not(target_arch = "arm"))]
use simd_json as json;

/// Environment variable for the telemetry Kafka topic.
const PROSODY_TELEMETRY_TOPIC: &str = "PROSODY_TELEMETRY_TOPIC";

/// Environment variable to enable/disable the telemetry emitter.
const PROSODY_TELEMETRY_ENABLED: &str = "PROSODY_TELEMETRY_ENABLED";

/// Default Kafka topic for telemetry events.
const DEFAULT_TELEMETRY_TOPIC: &str = "prosody.telemetry-events";

/// Number of produce futures to keep in flight concurrently.
const PRODUCE_CONCURRENCY: usize = 256;

/// Configuration for the telemetry Kafka emitter.
#[derive(Builder, Clone, Debug, Validate)]
pub struct TelemetryEmitterConfiguration {
    /// Kafka topic to produce telemetry events to.
    ///
    /// Environment variable: `PROSODY_TELEMETRY_TOPIC`
    /// Default: `prosody.telemetry-events`
    #[builder(
        default = "from_env_with_fallback(PROSODY_TELEMETRY_TOPIC, \
                   DEFAULT_TELEMETRY_TOPIC.to_owned())?",
        setter(into)
    )]
    pub topic: String,

    /// Whether the telemetry emitter is enabled.
    ///
    /// Environment variable: `PROSODY_TELEMETRY_ENABLED`
    /// Default: `true`
    #[builder(
        default = "from_env_with_fallback(PROSODY_TELEMETRY_ENABLED, true)?",
        setter(into)
    )]
    pub enabled: bool,
}

impl TelemetryEmitterConfiguration {
    /// Creates a new `TelemetryEmitterConfigurationBuilder`.
    #[must_use]
    pub fn builder() -> TelemetryEmitterConfigurationBuilder {
        TelemetryEmitterConfigurationBuilder::default()
    }
}

impl Default for TelemetryEmitterConfiguration {
    fn default() -> Self {
        Self {
            topic: DEFAULT_TELEMETRY_TOPIC.to_owned(),
            enabled: true,
        }
    }
}

// ── Serialization payload structs ────────────────────────────────────────────

/// Serialization payload for timer events.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TimerEventPayload<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    event_time: &'a str,
    scheduled_time: &'a str,
    timer_type: TimerType,
    topic: &'a str,
    partition: i32,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    demand_type: Option<DemandType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<ErrorCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
}

/// Serialization payload for message lifecycle events.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageEventPayload<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    event_time: &'a str,
    offset: i64,
    topic: &'a str,
    partition: i32,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
    demand_type: DemandType,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<ErrorCategory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exception: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
}

/// Serialization payload for producer message sent events.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MessageSentPayload<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    event_time: &'a str,
    topic: &'a str,
    partition: i32,
    offset: i64,
    key: &'a str,
    source: &'a str,
    hostname: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_parent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_state: Option<&'a str>,
}

// ── Serialization helpers
// ─────────────────────────────────────────────────────

thread_local! {
    static SERIALIZE_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn serialize_timer(
    buf: &mut Vec<u8>,
    data: &TimerTelemetryEvent,
    topic: &str,
    partition: i32,
    hostname: &str,
) -> bool {
    let event_time_str = data.event_time.to_rfc3339_opts(SecondsFormat::Millis, true);
    let scheduled_time_str =
        DateTime::<Utc>::from(data.scheduled_time).to_rfc3339_opts(SecondsFormat::Secs, true);

    let (type_str, demand_type, error_category, exception) = match &data.event_type {
        TimerEventType::Scheduled => ("prosody.timer.scheduled", None, None, None),
        TimerEventType::Dispatched { demand_type } => {
            ("prosody.timer.dispatched", Some(*demand_type), None, None)
        }
        TimerEventType::Succeeded { demand_type } => {
            ("prosody.timer.succeeded", Some(*demand_type), None, None)
        }
        TimerEventType::Failed {
            demand_type,
            error_category,
            exception,
        } => (
            "prosody.timer.failed",
            Some(*demand_type),
            Some(*error_category),
            Some(exception.as_ref()),
        ),
    };

    let payload = TimerEventPayload {
        event_type: type_str,
        event_time: &event_time_str,
        scheduled_time: &scheduled_time_str,
        timer_type: data.timer_type,
        topic,
        partition,
        key: data.key.as_ref(),
        source: data.source.as_ref(),
        hostname,
        demand_type,
        error_category,
        exception,
        trace_parent: data.trace_parent.as_deref(),
        trace_state: data.trace_state.as_deref(),
    };

    json::to_writer(buf as &mut Vec<u8>, &payload).is_ok()
}

fn serialize_message(
    buf: &mut Vec<u8>,
    data: &MessageTelemetryEvent,
    topic: &str,
    partition: i32,
    hostname: &str,
) -> bool {
    let event_time_str = data.event_time.to_rfc3339_opts(SecondsFormat::Millis, true);

    let (type_str, demand_type, error_category, exception) = match &data.event_type {
        MessageEventType::Dispatched { demand_type } => {
            ("prosody.message.dispatched", *demand_type, None, None)
        }
        MessageEventType::Succeeded { demand_type } => {
            ("prosody.message.succeeded", *demand_type, None, None)
        }
        MessageEventType::Failed {
            demand_type,
            error_category,
            exception,
        } => (
            "prosody.message.failed",
            *demand_type,
            Some(*error_category),
            Some(exception.as_ref()),
        ),
    };

    let payload = MessageEventPayload {
        event_type: type_str,
        event_time: &event_time_str,
        offset: data.offset,
        topic,
        partition,
        key: data.key.as_ref(),
        source: data.source.as_ref(),
        hostname,
        demand_type,
        error_category,
        exception,
        trace_parent: data.trace_parent.as_deref(),
        trace_state: data.trace_state.as_deref(),
    };

    json::to_writer(buf as &mut Vec<u8>, &payload).is_ok()
}

fn serialize_message_sent(buf: &mut Vec<u8>, data: &MessageSentEvent, hostname: &str) -> bool {
    let event_time_str = data.event_time.to_rfc3339_opts(SecondsFormat::Millis, true);

    let payload = MessageSentPayload {
        event_type: "prosody.message.sent",
        event_time: &event_time_str,
        topic: data.topic.as_ref(),
        partition: data.partition,
        offset: data.offset,
        key: data.key.as_ref(),
        source: data.source.as_ref(),
        hostname,
        trace_parent: data.trace_parent.as_deref(),
        trace_state: data.trace_state.as_deref(),
    };

    json::to_writer(buf as &mut Vec<u8>, &payload).is_ok()
}

/// Serializes a `Data` variant into a thread-local buffer and returns the
/// record key alongside owned bytes.
///
/// Returns `Some((key, Bytes))` for `Timer`, `Message`, and `MessageSent`
/// variants; `None` for internal-only variants (`Partition`, `Key`).
fn serialize_event(
    data: &Data,
    topic: &str,
    partition: i32,
    hostname: &str,
) -> Option<(Key, Bytes)> {
    SERIALIZE_BUF.with_borrow_mut(|buf| {
        buf.clear();

        let (wrote, key) = match data {
            Data::Timer(t) => (
                serialize_timer(buf, t, topic, partition, hostname),
                t.key.clone(),
            ),
            Data::Message(m) => (
                serialize_message(buf, m, topic, partition, hostname),
                m.key.clone(),
            ),
            Data::MessageSent(s) => (serialize_message_sent(buf, s, hostname), s.key.clone()),
            Data::Partition(_) | Data::Key(_) => return None,
        };

        wrote.then(|| (key, Bytes::copy_from_slice(buf)))
    })
}

// ── Emitter entry point
// ───────────────────────────────────────────────────────

/// Spawns a background task that subscribes to the telemetry broadcast channel
/// and produces serialized events to Kafka concurrently.
///
/// If `config.enabled` is `false`, this is a no-op and returns `Ok(())`.
///
/// # Errors
///
/// Returns [`EmitterError`] if the Kafka producer cannot be created or the
/// hostname cannot be resolved.
pub fn spawn_telemetry_emitter(
    config: &TelemetryEmitterConfiguration,
    bootstrap_servers: &[String],
    telemetry: &Telemetry,
) -> Result<(), EmitterError> {
    if !config.enabled {
        return Ok(());
    }

    let hostname: Arc<str> = hostname()?.into();
    let bootstrap = bootstrap_servers.join(",");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("client.id", hostname.as_ref())
        // Telemetry is best-effort: ack from leader only, no idempotence overhead
        .set("acks", "1")
        .set("enable.idempotence", "false")
        // Batch aggressively: 1 s linger, 4 MiB per-partition batch, 128 MiB buffer
        .set("linger.ms", "1000")
        .set("batch.size", "4194304")
        .set("queue.buffering.max.kbytes", "131072")
        // 1 in-flight request per connection preserves per-partition ordering
        .set("max.in.flight.requests.per.connection", "1")
        .set("compression.codec", "lz4")
        .set_log_level(RDKafkaLogLevel::Error)
        .create()?;

    let rx = telemetry.subscribe();
    let telemetry_topic: Arc<str> = config.topic.as_str().into();

    tokio::spawn(async move {
        BroadcastStream::new(rx)
            .filter_map(|result| {
                let hostname = hostname.clone();
                async move {
                    match result {
                        Ok(event) => serialize_event(
                            &event.data,
                            event.topic.as_ref(),
                            event.partition,
                            &hostname,
                        ),
                        Err(BroadcastStreamRecvError::Lagged(n)) => {
                            warn!("telemetry emitter lagged, dropped {n} events");
                            None
                        }
                    }
                }
            })
            .map(|(key, bytes)| {
                let producer = producer.clone();
                let topic = telemetry_topic.clone();
                async move {
                    let record = FutureRecord::to(&topic)
                        .key(key.as_ref())
                        .payload(bytes.as_ref());
                    if let Err((e, _)) = producer.send(record, Timeout::Never).await {
                        warn!("telemetry produce error: {e}");
                    }
                }
            })
            .buffer_unordered(PRODUCE_CONCURRENCY)
            .for_each(|()| async {})
            .await;
    });

    Ok(())
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors that can occur when creating the telemetry emitter.
#[derive(Debug, Error)]
pub enum EmitterError {
    /// Failed to resolve the machine hostname.
    #[error("failed to resolve hostname: {0}")]
    Hostname(#[from] whoami::Error),

    /// Failed to create the Kafka producer.
    #[error("failed to create Kafka producer: {0}")]
    Kafka(#[from] KafkaError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::DemandType;
    use crate::error::ErrorCategory;
    use crate::telemetry::Telemetry;
    use crate::telemetry::event::{
        KeyEvent, KeyState, MessageEventType, MessageSentEvent, MessageTelemetryEvent,
        PartitionEvent, PartitionState, TimerEventType, TimerTelemetryEvent,
    };
    use crate::timers::TimerType;
    use crate::timers::datetime::CompactDateTime;
    use chrono::Utc;
    use color_eyre::eyre::{Result, bail, ensure, eyre};
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
        let Some((key, _)) = serialize_event(&data, "topic", 0, "host") else {
            bail!("timer event should serialize");
        };
        ensure!(key.as_ref() == "my-timer-key");
        Ok(())
    }

    #[test]
    fn serialize_event_message_uses_payload_key() -> Result<()> {
        let data = message_event("my-message-key");
        let Some((key, _)) = serialize_event(&data, "topic", 0, "host") else {
            bail!("message event should serialize");
        };
        ensure!(key.as_ref() == "my-message-key");
        Ok(())
    }

    #[test]
    fn serialize_event_message_sent_uses_payload_key() -> Result<()> {
        let data = message_sent_event("my-sent-key");
        let Some((key, _)) = serialize_event(&data, "topic", 0, "host") else {
            bail!("message sent event should serialize");
        };
        ensure!(key.as_ref() == "my-sent-key");
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
        let (_, bytes) = serialize_event(data, "src-topic", 7, "test-host")
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
            chrono::DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
            "eventTime not RFC 3339"
        );
        ensure!(
            chrono::DateTime::parse_from_rfc3339(v["scheduledTime"].as_str().unwrap_or("")).is_ok(),
            "scheduledTime not RFC 3339"
        );
        // Optional fields must be absent
        assert!(v.get("demandType").is_none());
        assert!(v.get("errorCategory").is_none());
        assert!(v.get("exception").is_none());
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
            chrono::DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
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
            chrono::DateTime::parse_from_rfc3339(v["eventTime"].as_str().unwrap_or("")).is_ok(),
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
}
