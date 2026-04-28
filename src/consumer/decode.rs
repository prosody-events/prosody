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
use opentelemetry::Context;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{Message, Timestamp};
use std::str;
use std::sync::Arc;
use tracing::error;

use crate::Codec;
use crate::consumer::extractor::MessageExtractor;
use crate::consumer::message::ConsumerMessageValue;
use crate::{SOURCE_SYSTEM_HEADER, SourceSystem, Topic};

/// A decoded Kafka message without live span references.
///
/// Contains the immutable message data and the parent trace context extracted
/// from Kafka headers. The context contains only the upstream service's trace
/// identifiers (`trace_id`, `span_id`, flags) and baggage - no active span
/// reference. Callers construct their own spans and link them to the context
/// via the [`related_span!`](crate::related_span) macro.
///
/// This design ensures spans have independent lifecycles from cache entries:
/// - Context is safely cached (contains only remote trace identifiers)
/// - Each load site creates its own span from the cached context
/// - Spans close when processing completes, not on cache eviction
///
/// # Type Parameters
///
/// * `P` – The deserialized payload type.
#[derive(Clone, Debug)]
pub struct DecodedMessage<P> {
    /// Shared immutable message data
    pub value: Arc<ConsumerMessageValue<P>>,

    /// Parent trace context extracted from original Kafka headers.
    ///
    /// This is a "remote" context with `inner: None` - it contains only the
    /// upstream service's `SpanContext` (`trace_id`, `span_id`, flags) and
    /// baggage, not a reference to any local span. Safe to cache and clone.
    pub parent_context: Context,
}

/// Decodes and validates a Kafka message into a `DecodedMessage`.
///
/// This function performs comprehensive message processing:
/// 1. Extracts distributed tracing context from message headers
/// 2. Parses and validates the payload via the provided codec
/// 3. Extracts and validates the message key
/// 4. Resolves the message timestamp from Kafka metadata
///
/// The decoded message contains immutable data and parent trace context.
/// Callers create their own spans from the context, ensuring span lifecycles
/// are independent of cache eviction.
///
/// # Arguments
///
/// * `message` - The Kafka message to decode. Taken as `&mut` so the codec can
///   parse the payload in place via `payload_mut`, avoiding a copy. The payload
///   bytes are consumed (left in an unspecified state) by this call.
/// * `propagator` - Distributed tracing context propagator
/// * `codec` - Codec used to deserialize the raw payload bytes
///
/// # Returns
///
/// * `Some(DecodedMessage)` - A validated, parsed message with parent context
/// * `None` - If the message is invalid or missing required fields
pub fn decode_message<C: Codec>(
    message: &mut BorrowedMessage,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
) -> Option<DecodedMessage<C::Payload>> {
    let topic: Topic = Intern::from(message.topic());
    let partition = message.partition();
    let offset = message.offset();

    let parent_context = propagator.extract(&MessageExtractor::new(message));

    let source_system = extract_source_system(message);
    let timestamp = resolve_timestamp(message);

    let Some(key_data) = message.key() else {
        error!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "missing key; discarding message"
        );
        return None;
    };

    let key = match str::from_utf8(key_data) {
        Ok(key_str) => key_str.into(),
        Err(error) => {
            error!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "invalid key encoding: {error:#}; discarding message"
            );
            return None;
        }
    };

    // SAFETY: librdkafka does not formally promise the payload is mutable,
    // but on the consumer poll path:
    //   - `on_consume` interceptors run inside `rd_kafka_message_setup` before the
    //     message is returned to the application, so no librdkafka-internal code
    //     reads these bytes after delivery.
    //   - The payload occupies a disjoint slice of the refcounted fetch buffer;
    //     mutating it cannot corrupt sibling messages.
    //   - `decode_message` is the only site in the crate that reads the borrowed
    //     payload bytes, so the codec's destructive parse cannot affect downstream
    //     code.
    // The audit boundary is the rdkafka version resolved in `Cargo.lock`, not
    // `Cargo.toml` (which uses caret semver and accepts any 0.39.x via
    // `cargo update`). Re-audit on any rdkafka bump, including patch updates.
    #[allow(unsafe_code)]
    let Some(payload_bytes) = (unsafe { message.payload_mut() }) else {
        error!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "missing payload; discarding message"
        );
        return None;
    };

    let payload = match codec.deserialize(payload_bytes) {
        Ok(p) => p,
        Err(error) => {
            error!("invalid payload: {error:#}; discarding message");
            return None;
        }
    };

    let value = Arc::new(ConsumerMessageValue {
        source_system,
        topic,
        partition,
        offset,
        key,
        timestamp,
        payload,
    });

    Some(DecodedMessage {
        value,
        parent_context,
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
