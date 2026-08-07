//! Decoding and validation of Kafka messages.
//!
//! This module provides functionality for converting rdkafka's
//! `BorrowedMessage` into Prosody's [`DecodedMessage`] type. The decoding
//! process includes:
//!
//! - Distributed tracing context extraction
//! - Message header parsing (source system, and the reserved response headers)
//! - Payload parsing via the configured codec
//! - Key extraction and UTF-8 validation
//! - Timestamp resolution from Kafka metadata
//!
//! The main entry point is [`decode_message`], which performs all validation
//! and returns `None` if the message is invalid or should be filtered out.

use chrono::{TimeZone, Utc};
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
use crate::response::headers::{RequestTag, parse_request_tag};
use crate::subsystem::SubsystemName;
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

/// Selects whether this consumer accepts peer request headers.
pub(crate) trait RequestAdmission: Send {
    /// Reads a request tag admitted by this consumer.
    fn request(&self, message: &BorrowedMessage) -> Option<RequestTag>;
}

/// Rejects all peer request headers.
pub(crate) struct NoRequests;

/// Accepts peer requests for one subsystem.
pub(crate) struct SubsystemRequests(pub(crate) SubsystemName);

/// Decodes and validates a Kafka message into a `DecodedMessage`.
///
/// The decoded message contains immutable data and parent trace context.
/// Callers create their own spans from the context, ensuring span lifecycles
/// are independent of cache eviction.
///
/// `responder` is the subsystem this consumer answers peer requests for, or
/// `None` when it answers none.
///
/// `message` is taken as `&mut` so the codec can parse the payload in place
/// via `payload_mut`, avoiding a copy; its payload bytes are left in an
/// unspecified state after this call.
pub fn decode_message<C: Codec, A: RequestAdmission>(
    message: &mut BorrowedMessage,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
    admission: &A,
) -> Option<DecodedMessage<C::Payload>> {
    let topic: Topic = Intern::from(message.topic());
    let partition = message.partition();
    let offset = message.offset();

    let parent_context = propagator.extract(&MessageExtractor::new(message));

    let source_system = extract_source_system(message);
    let request = admission.request(message);
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
            error!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "invalid payload: {error:#}; discarding message"
            );
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
        request,
    });

    Some(DecodedMessage {
        value,
        parent_context,
    })
}

/// Extracts the source system header from a Kafka message.
///
/// Logs an error if the header value is invalid UTF-8 and treats it as absent.
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

/// Reads the reserved response headers, or nothing when this consumer answers
/// no requests.
///
/// An unusable header set is counted and dropped, never failed.
/// [`HeaderRejection`](crate::response::headers::HeaderRejection) states why.
impl RequestAdmission for NoRequests {
    fn request(&self, _message: &BorrowedMessage) -> Option<RequestTag> {
        None
    }
}

impl RequestAdmission for SubsystemRequests {
    fn request(&self, message: &BorrowedMessage) -> Option<RequestTag> {
        let headers = message.headers()?;

        match parse_request_tag(
            headers.iter().map(|header| (header.key, header.value)),
            &self.0,
        ) {
            Ok(tag) => tag,
            Err(rejection) => {
                rejection.record();
                None
            }
        }
    }
}

impl RequestAdmission for Option<&SubsystemName> {
    fn request(&self, message: &BorrowedMessage) -> Option<RequestTag> {
        let subsystem = self.as_ref()?;
        SubsystemRequests((*subsystem).clone()).request(message)
    }
}

/// Resolves the message timestamp from Kafka metadata.
///
/// Uses `CreateTime` or `LogAppendTime` when the value is in range. Falls back
/// to the current time when Kafka gives no timestamp, or when the value is out
/// of range.
fn resolve_timestamp(message: &BorrowedMessage) -> chrono::DateTime<Utc> {
    match message.timestamp() {
        Timestamp::NotAvailable => Utc::now(),
        Timestamp::CreateTime(millis) | Timestamp::LogAppendTime(millis) => Utc
            .timestamp_millis_opt(millis)
            .single()
            .unwrap_or_else(Utc::now),
    }
}
