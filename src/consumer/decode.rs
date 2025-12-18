//! Decoding and validation of Kafka messages.
//!
//! This module provides functionality for converting rdkafka's
//! `BorrowedMessage` into Prosody's `ConsumerMessage` type. The decoding
//! process includes:
//!
//! - Distributed tracing context extraction
//! - Message header parsing (source system)
//! - JSON payload validation and parsing
//! - Key extraction and UTF-8 validation
//! - Timestamp resolution from Kafka metadata
//!
//! The main entry point is [`decode_message`], which performs all validation
//! and returns `None` if the message is invalid or should be filtered out.

use chrono::{MappedLocalTime, TimeZone, Utc};
use internment::Intern;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{Message, Timestamp};
use std::str;
use std::sync::Arc;
use tracing::field::Empty;
use tracing::{Span, debug, error, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(not(target_arch = "arm"))]
use simd_json::Buffers;
#[cfg(not(target_arch = "arm"))]
use simd_json::serde::from_reader_with_buffers;

use crate::consumer::extractor::MessageExtractor;
use crate::consumer::message::ConsumerMessageValue;
use crate::{Payload, SOURCE_SYSTEM_HEADER, SourceSystem, Topic};

/// A decoded Kafka message without processing state.
///
/// Contains the immutable message data and the receive span for tracing
/// lineage, but no semaphore permit. This allows separating the decode
/// lifecycle from caching and processing lifecycles.
#[derive(Clone, Debug)]
pub struct DecodedMessage {
    /// Shared immutable message data
    pub value: Arc<ConsumerMessageValue>,

    /// Span for tracing lineage
    pub span: Span,
}

/// Decodes and validates a Kafka message into a `DecodedMessage`.
///
/// This function performs comprehensive message processing:
/// 1. Creates a tracing span with message metadata for observability
/// 2. Extracts distributed tracing context from message headers
/// 3. Parses and validates the JSON payload
/// 4. Extracts and validates the message key
/// 5. Resolves the message timestamp from Kafka metadata
///
/// The decoded message contains immutable data and the receive span but no
/// semaphore permit, allowing the caller to manage permit lifecycle separately.
///
/// # Arguments
///
/// * `message` - The Kafka message to decode
/// * `propagator` - Distributed tracing context propagator
/// * `buffers` - (Non-ARM only) Buffers for SIMD JSON parsing
///
/// # Returns
///
/// * `Some(DecodedMessage)` - A validated, parsed message with tracing context
/// * `None` - If the message is invalid or missing required fields
pub fn decode_message(
    message: &BorrowedMessage,
    propagator: &TextMapCompositePropagator,
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
) -> Option<DecodedMessage> {
    // Extract basic message coordinates
    let topic: Topic = Intern::from(message.topic());
    let partition = message.partition();
    let offset = message.offset();

    // Create and configure tracing span with distributed context
    let context = propagator.extract(&MessageExtractor::new(message));
    let receive_span = info_span!(
        "receive",
        partition,
        offset,
        topic = topic.as_ref(),
        key = Empty,
        payload_size = Empty,
        skipped = Empty,
        event_type = Empty,
    );

    if let Err(error) = receive_span.set_parent(context) {
        debug!("failed to set parent span: {error:#}");
    }

    let enter = receive_span.enter();

    // Extract source system header if present
    let source_system = extract_source_system(message);

    // Validate and parse payload
    let Some(payload_data) = message.payload() else {
        error!("missing payload; discarding message");
        return None;
    };
    receive_span.record("payload_size", payload_data.len());

    let payload = parse_payload(
        payload_data,
        #[cfg(not(target_arch = "arm"))]
        buffers,
    )?;

    // Validate and extract key
    let Some(key_data) = message.key() else {
        error!("missing key; discarding message");
        return None;
    };

    let key = match str::from_utf8(key_data) {
        Ok(key_str) => {
            receive_span.record("key", key_str);
            key_str.into()
        }
        Err(error) => {
            error!("invalid key encoding: {error:#}; discarding message");
            return None;
        }
    };

    // Determine message timestamp based on available metadata
    let timestamp = resolve_timestamp(message);

    // Create and return decoded message (without permit)
    let value = Arc::new(ConsumerMessageValue {
        source_system,
        topic,
        partition,
        offset,
        key,
        timestamp,
        payload,
    });

    // Exit the span context before moving the span into the result
    drop(enter);

    Some(DecodedMessage {
        value,
        span: receive_span,
    })
}

/// Extracts the source system header from a Kafka message.
///
/// Logs an error if the header value is invalid UTF-8 and treats it as absent.
///
/// # Arguments
///
/// * `message` - The Kafka message containing headers
///
/// # Returns
///
/// * `Some(SourceSystem)` - If the header is present and valid
/// * `None` - If the header is not present or invalid
fn extract_source_system(message: &BorrowedMessage) -> Option<SourceSystem> {
    match message
        .headers()
        .into_iter()
        .flat_map(|headers| headers.iter())
        .find(|header| header.key == SOURCE_SYSTEM_HEADER)
        .and_then(|header| header.value)
        .map(str::from_utf8)
        .transpose()
    {
        Ok(source_system) => source_system.map(SourceSystem::from),
        Err(error) => {
            error!("invalid source system encoding: {error:#}; ignoring");
            None
        }
    }
}

/// Parses a message payload as JSON using platform-optimized implementations.
///
/// # Arguments
///
/// * `payload_data` - Raw payload bytes
/// * `buffers` - (Non-ARM only) Buffers for SIMD JSON parsing
///
/// # Returns
///
/// * `Some(Payload)` - Successfully parsed JSON payload
/// * `None` - If parsing fails
fn parse_payload(
    payload_data: &[u8],
    #[cfg(not(target_arch = "arm"))] buffers: &mut Buffers,
) -> Option<Payload> {
    #[cfg(target_arch = "arm")]
    let payload = serde_json::from_slice(payload_data);
    #[cfg(not(target_arch = "arm"))]
    let payload = from_reader_with_buffers(payload_data, buffers);

    match payload {
        Ok(p) => Some(p),
        Err(error) => {
            error!("invalid payload: {error:#}; discarding message");
            None
        }
    }
}

/// Resolves the message timestamp from Kafka metadata.
///
/// Handles different timestamp types and fallback scenarios:
/// - Uses `CreateTime` or `LogAppendTime` if available
/// - Falls back to current time if timestamp is not available
/// - Handles ambiguous timestamps by selecting the earliest
///
/// # Arguments
///
/// * `message` - The Kafka message containing timestamp metadata
///
/// # Returns
///
/// The resolved message timestamp
fn resolve_timestamp(message: &BorrowedMessage) -> chrono::DateTime<chrono::Utc> {
    match message.timestamp() {
        Timestamp::NotAvailable => Utc::now(),
        Timestamp::CreateTime(millis) | Timestamp::LogAppendTime(millis) => {
            match Utc.timestamp_millis_opt(millis) {
                MappedLocalTime::Single(ts) => ts,
                MappedLocalTime::Ambiguous(earliest, ..) => earliest,
                MappedLocalTime::None => Utc::now(),
            }
        }
    }
}
