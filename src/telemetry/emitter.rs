//! Background Kafka emitter for telemetry events.
//!
//! Subscribes to the internal broadcast channel, serializes events to JSON,
//! and produces them concurrently to a configured Kafka topic using
//! `buffer_unordered`.

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
use chrono::{DateTime, Utc};
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
    let event_time_str = data.event_time.to_rfc3339();
    let scheduled_time_str = DateTime::<Utc>::from(data.scheduled_time).to_rfc3339();

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
    let event_time_str = data.event_time.to_rfc3339();

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
    let event_time_str = data.event_time.to_rfc3339();

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

/// Serializes a `Data` variant into a thread-local buffer and returns owned
/// bytes.
///
/// Returns `Some(Bytes)` for `Timer`, `Message`, and `MessageSent` variants;
/// `None` for internal-only variants (`Partition`, `Key`).
fn serialize_event(data: &Data, topic: &str, partition: i32, hostname: &str) -> Option<Bytes> {
    SERIALIZE_BUF.with_borrow_mut(|buf| {
        buf.clear();

        let wrote = match data {
            Data::Timer(t) => serialize_timer(buf, t, topic, partition, hostname),
            Data::Message(m) => serialize_message(buf, m, topic, partition, hostname),
            Data::MessageSent(s) => serialize_message_sent(buf, s, hostname),
            Data::Partition(_) | Data::Key(_) => return None,
        };

        wrote.then(|| Bytes::copy_from_slice(buf))
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
                        )
                        .map(|bytes| (event.topic, bytes)),
                        Err(_lagged) => None,
                    }
                }
            })
            .map(|(event_topic, bytes)| {
                let producer = producer.clone();
                async move {
                    let record =
                        FutureRecord::<str, [u8]>::to(event_topic.as_ref()).payload(bytes.as_ref());
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
