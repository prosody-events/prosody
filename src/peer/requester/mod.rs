//! Sends one Kafka request and collects one response per named subsystem.

mod collect;
pub(crate) mod registry;

use self::collect::collect;
use self::registry::PendingRegistry;
use crate::consumer::Record;
use crate::peer::response::RequestId;
use crate::peer::response::headers::{
    ID_TEXT_LEN, RESPONSE_AWAITED_HEADER, RESPONSE_DEADLINE_HEADER, RESPONSE_PEER_HEADER,
    RESPONSE_REQUEST_ID_HEADER, RequestDeadline, id_text, is_reserved,
};
use crate::peer::router::PeerId;
use crate::producer::{ProducerError, ProsodyProducer};
use crate::subsystem::SubsystemName;
use crate::{Codec, EventIdentity, Topic};
use opentelemetry::KeyValue;
use opentelemetry_semantic_conventions::attribute::{
    ERROR_TYPE, MESSAGING_MESSAGE_CONVERSATION_ID,
};
use rdkafka::message::{Header, OwnedHeaders};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;
use tracing::field::{Empty, display};
use tracing::{Span, instrument};

#[cfg(test)]
mod tests;

/// Reserved headers that occur exactly once in every request.
const RESERVED_SINGLETONS: usize = 3;

/// One outcome for each requested subsystem.
///
/// The keys are canonical subsystem names. The map contains one entry for
/// every accepted subsystem, including an explicit timeout for no response.
pub type SubsystemOutcomes<T> = HashMap<SubsystemName, Result<T, ResponseError>>;

/// Why one requested subsystem produced no successful response.
#[derive(Debug, Error, PartialEq)]
pub enum ResponseError {
    /// The handler returned an error.
    #[error("handler failed: {message}")]
    Handler {
        /// The handler's display text.
        message: String,
    },
    /// No response arrived before the deadline.
    #[error("no response arrived before the deadline")]
    Timeout,
    /// The responder used a different response format.
    #[error("the responder answered in another format")]
    FormatMismatch,
    /// The response payload did not decode.
    #[error("the response did not decode")]
    Malformed,
}

/// Why one complete request failed before it could return subsystem results.
///
/// This enum has no [`crate::error::ClassifyError`] impl on purpose. Nothing
/// retries a request for the caller, so a classification would have no
/// consumer.
#[derive(Debug, Error)]
pub enum RequestError<E: Error> {
    /// The request named no subsystem.
    #[error("a request must name at least one subsystem")]
    NoSubsystems,
    /// The request named one subsystem more than once.
    #[error("subsystem {name} occurs more than once")]
    DuplicateSubsystem {
        /// The repeated subsystem name.
        name: SubsystemName,
    },
    /// A caller-supplied header belongs to the request protocol.
    #[error("header {name} is reserved for response requests")]
    ReservedHeader {
        /// The reserved header name.
        name: String,
    },
    /// The timeout cannot be represented as a wire or runtime deadline.
    #[error("the request timeout is too large")]
    DeadlineOutOfRange,
    /// Registry shutdown has started.
    #[error("the requester is shutting down")]
    ShuttingDown,
    /// Kafka did not accept the request.
    #[error(transparent)]
    Produce(#[from] ProducerError<E>),
}

/// Sends requests and returns one outcome per subsystem.
pub struct ProsodyRequester<C: Codec, R: Codec> {
    producer: ProsodyProducer<C>,
    peer: PeerId,
    registry: Arc<PendingRegistry>,
    _response: PhantomData<fn() -> R>,
}

impl<C: Codec, R: Codec> ProsodyRequester<C, R> {
    /// Creates a requester for one peer and one response codec.
    pub(crate) fn new(
        producer: ProsodyProducer<C>,
        peer: PeerId,
        registry: Arc<PendingRegistry>,
    ) -> Self {
        Self {
            producer,
            peer,
            registry,
            _response: PhantomData,
        }
    }

    /// Sends one request and waits for one answer per subsystem.
    ///
    /// A complete response set still waits for Kafka to report delivery. This
    /// preserves the producer's telemetry and idempotence update.
    ///
    /// The span is named for the function, exactly as
    /// [`ProsodyProducer::send`] is, and it covers the whole wait. A client
    /// span covers a request and its response together, so nothing opens a
    /// second span for the answer arriving. The produce nests inside it.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] for invalid arguments, a produce failure, or
    /// shutdown.
    pub async fn request<'a, H, V>(
        &self,
        headers: H,
        topic: Topic,
        key: &str,
        payload: C::Payload,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<V>, RequestError<C::Error>>
    where
        H: IntoIterator<Item = (&'a str, &'a str)> + Send,
        H::IntoIter: ExactSizeIterator + Send,
        C::Payload: EventIdentity,
        R: Codec<Payload = V>,
    {
        let record_headers = request_headers(headers, subsystems.len())?;
        self.request_prepared(
            record_headers,
            topic,
            key,
            Record::Message(payload),
            subsystems,
            timeout,
        )
        .await
    }

    /// Sends one excise request and waits for one answer per subsystem.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] for invalid arguments, a produce failure, or
    /// shutdown.
    pub async fn request_excise<'a, H, V>(
        &self,
        headers: H,
        topic: Topic,
        key: &str,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<V>, RequestError<C::Error>>
    where
        H: IntoIterator<Item = (&'a str, &'a str)> + Send,
        H::IntoIter: ExactSizeIterator + Send,
        C::Payload: EventIdentity,
        R: Codec<Payload = V>,
    {
        let record_headers = request_headers(headers, subsystems.len())?;
        self.request_prepared(
            record_headers,
            topic,
            key,
            Record::Excise,
            subsystems,
            timeout,
        )
        .await
    }

    #[instrument(
        name = "request",
        skip_all,
        fields(
            otel.kind = "client",
            messaging.system = "kafka",
            messaging.operation.name = "request",
            messaging.operation.type = "request",
            messaging.destination.name = topic.as_ref(),
            messaging.kafka.message.key = %key,
            messaging.message.conversation_id = Empty,
            topic = topic.as_ref(),
            key = %key,
            response.peer = %self.peer,
            request.id = Empty,
            request.outcome = Empty,
            request.latency_ms = Empty,
            responses.received = Empty,
            responses.succeeded = Empty,
            responses.failed = Empty,
            responses.missing = Empty,
            responses.errors = Empty,
            error.type = Empty,
            subsystems = subsystems.len() as i64,
        ),
        err
    )]
    async fn request_prepared<V>(
        &self,
        mut record_headers: OwnedHeaders,
        topic: Topic,
        key: &str,
        record: Record<C::Payload>,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<SubsystemOutcomes<V>, RequestError<C::Error>>
    where
        C::Payload: EventIdentity,
        R: Codec<Payload = V>,
    {
        // The two id texts are declared before the header list, so they outlive
        // the borrows the list holds on them.
        let mut request_buf = [0_u8; ID_TEXT_LEN];
        let mut peer_buf = [0_u8; ID_TEXT_LEN];
        let mut deadline_buf = itoa::Buffer::new();

        let deadline = RequestDeadline::after(timeout).ok_or_else(|| {
            record_request_error(&RequestError::<C::Error>::DeadlineOutOfRange);
            RequestError::DeadlineOutOfRange
        })?;
        let registration = self
            .registry
            .register(subsystems, deadline)
            .inspect_err(|error| {
                record_request_error(error);
            })?;
        Span::current().record("request.id", display(registration.id()));
        Span::current().record(
            MESSAGING_MESSAGE_CONVERSATION_ID,
            display(registration.id()),
        );

        record_headers = append_request_headers(
            record_headers,
            (registration.id(), &mut request_buf),
            (self.peer, &mut peer_buf),
            (deadline, &mut deadline_buf),
            subsystems,
        );

        let started = Instant::now();
        let collected = collect::<R, _, _>(registration, subsystems, async move {
            match record {
                Record::Message(payload) => {
                    self.producer
                        .send_owned(record_headers, topic, key, payload)
                        .await
                }
                Record::Excise => self.producer.excise_owned(record_headers, topic, key).await,
            }
        })
        .await;
        // A request refused before this point sent nothing, so it has no
        // latency to report. Only a call that really waited records one.
        let elapsed = started.elapsed();
        let waited = elapsed.as_secs_f64();
        Span::current().record(
            "request.latency_ms",
            elapsed.as_millis().min(i64::MAX as u128) as i64,
        );
        if let Ok(results) = &collected {
            let (succeeded, failed) = response_counts(results);
            let received = succeeded + failed;
            let completeness = completeness(received, results.len());
            Span::current().record("responses.received", received as i64);
            Span::current().record("responses.succeeded", succeeded as i64);
            Span::current().record("responses.failed", failed as i64);
            Span::current().record("responses.missing", display(Missing(results)));
            Span::current().record("responses.errors", display(Failures(results)));
            Span::current().record("request.outcome", completeness);
            self.registry
                .metrics()
                .request_latency
                .record(waited, &[KeyValue::new("outcome", completeness)]);
        } else {
            if let Err(error) = &collected {
                record_request_error(error);
            }
            Span::current().record("request.outcome", "failed");
            self.registry
                .metrics()
                .request_latency
                .record(waited, &[KeyValue::new("outcome", "failed")]);
        }
        collected
    }
}

fn record_request_error<E: Error>(error: &RequestError<E>) {
    let error_type = match error {
        RequestError::NoSubsystems => "no_subsystems",
        RequestError::DuplicateSubsystem { .. } => "duplicate_subsystem",
        RequestError::ReservedHeader { .. } => "reserved_header",
        RequestError::DeadlineOutOfRange => "deadline_out_of_range",
        RequestError::ShuttingDown => "shutting_down",
        RequestError::Produce(_) => "produce_error",
    };
    Span::current().record(ERROR_TYPE, error_type);
}

fn request_headers<'name, 'value, H, E>(
    headers: H,
    subsystem_count: usize,
) -> Result<OwnedHeaders, RequestError<E>>
where
    H: IntoIterator<Item = (&'name str, &'value str)>,
    H::IntoIter: ExactSizeIterator,
    E: Error,
{
    let headers = headers.into_iter();
    let capacity = headers
        .len()
        .saturating_add(subsystem_count)
        .saturating_add(RESERVED_SINGLETONS + 1);
    let mut owned = OwnedHeaders::new_with_capacity(capacity);
    for (name, value) in headers {
        if is_reserved(name) {
            return Err(RequestError::ReservedHeader {
                name: name.to_owned(),
            });
        }
        owned = insert_header(owned, name, value);
    }
    Ok(owned)
}

struct Missing<'a, V>(&'a SubsystemOutcomes<V>);

struct Failures<'a, V>(&'a SubsystemOutcomes<V>);

impl<V> Display for Missing<'_, V> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        let mut separator = "";
        for (subsystem, result) in self.0 {
            if !answered(result) {
                write!(formatter, "{separator}{subsystem}")?;
                separator = ",";
            }
        }
        Ok(())
    }
}

impl<V> Display for Failures<'_, V> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        let mut separator = "";
        for (subsystem, result) in self.0 {
            let Err(error) = result else {
                continue;
            };
            let Some(reason) = response_failure(error) else {
                continue;
            };
            write!(formatter, "{separator}{subsystem}={reason}")?;
            separator = ",";
        }
        Ok(())
    }
}

fn response_failure(error: &ResponseError) -> Option<&'static str> {
    match error {
        ResponseError::Handler { .. } => Some("handler"),
        ResponseError::FormatMismatch => Some("format_mismatch"),
        ResponseError::Malformed => Some("malformed"),
        ResponseError::Timeout => None,
    }
}

fn response_counts<V>(responses: &SubsystemOutcomes<V>) -> (usize, usize) {
    responses
        .values()
        .fold((0, 0), |(succeeded, failed), response| match response {
            Ok(_) => (succeeded + 1, failed),
            Err(ResponseError::Timeout) => (succeeded, failed),
            Err(_) => (succeeded, failed + 1),
        })
}

/// Whether one subsystem answered at all.
fn answered<V>(response: &Result<V, ResponseError>) -> bool {
    !matches!(response, Err(ResponseError::Timeout))
}

/// How complete one request's answers were, as the fixed label its latency is
/// recorded under.
///
/// A label rather than a subsystem name or a peer id: those arrive from the
/// network, and a metric keyed by one is a cardinality attack.
const fn completeness(answered: usize, awaited: usize) -> &'static str {
    if answered == 0 {
        "none"
    } else if answered == awaited {
        "complete"
    } else {
        "partial"
    }
}

/// Appends the reserved headers that tell a responder where to answer.
fn append_request_headers<'a>(
    mut headers: OwnedHeaders,
    request: (RequestId, &'a mut [u8; ID_TEXT_LEN]),
    peer: (PeerId, &'a mut [u8; ID_TEXT_LEN]),
    deadline: (RequestDeadline, &'a mut itoa::Buffer),
    subsystems: &'a [SubsystemName],
) -> OwnedHeaders {
    let (request, request_buf) = request;
    let (peer, peer_buf) = peer;
    let (deadline, deadline_buf) = deadline;
    headers = insert_header(
        headers,
        RESPONSE_REQUEST_ID_HEADER,
        id_text(request.into(), request_buf),
    );
    headers = insert_header(
        headers,
        RESPONSE_PEER_HEADER,
        id_text(peer.into(), peer_buf),
    );
    headers = insert_header(
        headers,
        RESPONSE_DEADLINE_HEADER,
        deadline.text(deadline_buf),
    );
    for subsystem in subsystems {
        headers = insert_header(headers, RESPONSE_AWAITED_HEADER, subsystem.as_str());
    }
    headers
}

fn insert_header(headers: OwnedHeaders, key: &str, value: &str) -> OwnedHeaders {
    headers.insert(Header {
        key,
        value: Some(value.as_bytes()),
    })
}
