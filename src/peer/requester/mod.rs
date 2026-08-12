//! Sends one Kafka request and collects one response per named subsystem.

mod collect;
pub(crate) mod registry;

use self::collect::collect;
use self::registry::PendingRegistry;
use crate::error::{ClassifyError, ErrorCategory};
use crate::peer::response::RequestId;
use crate::peer::response::headers::{
    ID_TEXT_LEN, REQUEST_REVISION, RESPONSE_AWAITED_HEADER, RESPONSE_DEADLINE_HEADER,
    RESPONSE_PEER_HEADER, RESPONSE_REQUEST_ID_HEADER, RESPONSE_VERSION_HEADER, RequestDeadline,
    id_text, is_reserved,
};
use crate::peer::router::PeerId;
use crate::producer::{ProducerError, ProsodyProducer};
use crate::subsystem::SubsystemName;
use crate::{Codec, EventIdentity, Topic};
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Histogram;
use rdkafka::message::{Header, OwnedHeaders};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;
use tracing::field::{Empty, display};
use tracing::{Span, instrument};

#[cfg(test)]
mod tests;

/// Reserved headers that occur exactly once in every request.
const RESERVED_SINGLETONS: usize = 4;

/// How long one request waited, by how complete its answers were.
///
/// A sustained `none` is what says synchrony waiting has stopped working:
/// callers are paying full deadlines for answers that never come.
static LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    meter("prosody")
        .f64_histogram("prosody.peer.request.latency")
        .with_description("How long one peer request waited for its answers")
        .with_unit("s")
        .build()
});

/// Why one requested subsystem produced no successful response.
///
/// Handler errors keep their category. A timeout is transient. An invalid or
/// incompatible response is permanent for that request.
#[derive(Debug, Error, PartialEq)]
pub enum ResponseError {
    /// The handler returned an error.
    #[error("handler failed: {message}")]
    Handler {
        /// The handler's retry classification.
        category: ErrorCategory,
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

impl ClassifyError for ResponseError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Handler { category, .. } => *category,
            Self::Timeout => ErrorCategory::Transient,
            Self::FormatMismatch | Self::Malformed => ErrorCategory::Permanent,
        }
    }
}

/// Why one complete request failed before it could return subsystem results.
///
/// This enum has no [`ClassifyError`] impl on purpose. Nothing retries a
/// request for the caller, so a classification would have no consumer.
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

/// Sends requests and returns responses in subsystem order.
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
    ) -> Result<Vec<Result<V, ResponseError>>, RequestError<C::Error>>
    where
        H: IntoIterator<Item = (&'a str, &'a str)> + Send,
        H::IntoIter: ExactSizeIterator + Send,
        C::Payload: EventIdentity,
        R: Codec<Payload = V>,
    {
        let record_headers = request_headers(headers, subsystems.len())?;
        self.request_prepared(record_headers, topic, key, payload, subsystems, timeout)
            .await
    }

    #[instrument(
        name = "request",
        skip_all,
        fields(
            otel.kind = "client",
            messaging.system = "kafka",
            topic = topic.as_ref(),
            key = %key,
            response.peer = %self.peer,
            request.id = Empty,
            request.outcome = Empty,
            request.latency_ms = Empty,
            responses.received = Empty,
            responses.missing = Empty,
            subsystems = subsystems.len() as i64,
        ),
        err
    )]
    async fn request_prepared<V>(
        &self,
        mut record_headers: OwnedHeaders,
        topic: Topic,
        key: &str,
        payload: C::Payload,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<Vec<Result<V, ResponseError>>, RequestError<C::Error>>
    where
        C::Payload: EventIdentity,
        R: Codec<Payload = V>,
    {
        // The two id texts are declared before the header list, so they outlive
        // the borrows the list holds on them.
        let mut request_buf = [0_u8; ID_TEXT_LEN];
        let mut peer_buf = [0_u8; ID_TEXT_LEN];
        let mut deadline_buf = itoa::Buffer::new();

        let deadline = RequestDeadline::after(timeout).ok_or(RequestError::DeadlineOutOfRange)?;
        let registration = self.registry.register(subsystems, deadline)?;
        Span::current().record("request.id", display(registration.id()));

        record_headers = append_request_headers(
            record_headers,
            (registration.id(), &mut request_buf),
            (self.peer, &mut peer_buf),
            (deadline, &mut deadline_buf),
            subsystems,
        );

        let started = Instant::now();
        let collected = collect::<R, _, _>(
            registration,
            self.producer
                .send_owned(record_headers, topic, key, payload),
        )
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
            let answered = results.iter().filter(|result| answered(result)).count();
            let completeness = completeness(answered, results.len());
            Span::current().record("responses.received", answered as i64);
            Span::current().record("responses.missing", display(Missing(subsystems, results)));
            Span::current().record("request.outcome", completeness);
            LATENCY.record(waited, &[KeyValue::new("outcome", completeness)]);
        } else {
            Span::current().record("request.outcome", "failed");
            LATENCY.record(waited, &[KeyValue::new("outcome", "failed")]);
        }
        collected
    }
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

struct Missing<'a, V>(&'a [SubsystemName], &'a [Result<V, ResponseError>]);

impl<V> Display for Missing<'_, V> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        let mut separator = "";
        for (subsystem, result) in self.0.iter().zip(self.1) {
            if !answered(result) {
                write!(formatter, "{separator}{subsystem}")?;
                separator = ",";
            }
        }
        Ok(())
    }
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
    headers = insert_header(headers, RESPONSE_VERSION_HEADER, REQUEST_REVISION);
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
