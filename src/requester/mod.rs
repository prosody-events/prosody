//! Sends one Kafka request and collects one response per named subsystem.

mod collect;
pub(crate) mod config;
pub(crate) mod registry;

use self::collect::collect;
use self::registry::{Admission, PendingRegistry};
use crate::error::ClassifyError;
use crate::producer::{ProducerError, ProsodyProducer};
use crate::response::RequestId;
use crate::response::headers::{
    ID_TEXT_LEN, REQUEST_REVISION, RESPONSE_AWAITED_HEADER, RESPONSE_NODE_HEADER,
    RESPONSE_REQUEST_ID_HEADER, RESPONSE_VERSION_HEADER, id_text, is_reserved,
};
use crate::router::NodeId;
use crate::subsystem::SubsystemName;
use crate::{Codec, EventIdentity, Topic};
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Histogram;
use smallvec::SmallVec;
use std::error::Error;
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
const RESERVED_SINGLETONS: usize = 3;

/// Headers one request carries before the list uses the heap.
const HEADER_INLINE: usize = 8;

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

/// One answer for one requested subsystem.
///
/// This flat union can cross the JavaScript, Python, Ruby, and C boundaries as
/// a value. Its order matches the requested subsystem slice.
#[derive(Debug, PartialEq)]
pub enum Outcome<V, E> {
    /// The handler succeeded and the payload decoded.
    Ok(V),
    /// The handler failed and its error decoded.
    ///
    /// The wire status agrees with what this error classifies as, because a
    /// frame whose two accounts disagree is [`ResponseFailure::Malformed`].
    /// So the category is read off the error and is never stored beside it.
    Handler(E),
    /// The subsystem produced no usable answer.
    Failed(ResponseFailure),
}

/// Why one requested subsystem produced no usable answer.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ResponseFailure {
    /// No response arrived before the deadline.
    #[error("no response arrived before the deadline")]
    Timeout,
    /// The responder used a different response format.
    #[error("the responder answered in another format")]
    FormatMismatch,
    /// The response was larger than this process accepts.
    #[error("the response was over the configured response ceiling")]
    TooLarge,
    /// The response payload did not decode or disagreed with its status.
    #[error("the response did not decode")]
    Malformed,
}

/// Why one complete request failed before it could return subsystem outcomes.
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
    /// The request named more subsystems than the configured limit.
    #[error("the request names {count} subsystems; the limit is {max}")]
    TooManySubsystems {
        /// Number of requested subsystems.
        count: usize,
        /// Configured subsystem limit.
        max: usize,
    },
    /// The timeout is shorter or longer than the configured range.
    #[error("the request timeout {timeout:?} is outside {min:?}..={max:?}")]
    TimeoutOutOfRange {
        /// Requested timeout.
        timeout: Duration,
        /// Shortest accepted timeout.
        min: Duration,
        /// Longest accepted timeout.
        max: Duration,
    },
    /// A caller-supplied header belongs to the request protocol.
    #[error("header {name} is reserved for response requests")]
    ReservedHeader {
        /// The reserved header name.
        name: &'static str,
    },
    /// Every in-flight request permit is in use.
    #[error("request admission is exhausted")]
    AdmissionExhausted,
    /// Registry shutdown has started.
    #[error("the requester is shutting down")]
    ShuttingDown,
    /// Kafka did not accept the request and no response arrived first.
    #[error(transparent)]
    Produce(#[from] ProducerError<E>),
}

/// Maps a registry refusal onto the error a caller sees.
///
/// An id already in use reads as exhausted capacity: a fresh `UUIDv7` puts a
/// collision out of reach, and refusing is what keeps the live request under
/// that id from being overwritten.
impl<E: Error> From<Admission> for RequestError<E> {
    fn from(admission: Admission) -> Self {
        match admission {
            Admission::Exhausted | Admission::IdInUse => Self::AdmissionExhausted,
            Admission::ShuttingDown => Self::ShuttingDown,
        }
    }
}

/// Sends requests and returns responses in subsystem order.
pub struct ProsodyRequester<C: Codec, R: Codec> {
    producer: ProsodyProducer<C>,
    node: NodeId,
    registry: Arc<PendingRegistry>,
    _response: PhantomData<fn() -> R>,
}

impl<C: Codec, R: Codec> ProsodyRequester<C, R> {
    /// Creates a requester for one node and one response codec.
    pub(crate) fn new(
        producer: ProsodyProducer<C>,
        node: NodeId,
        registry: Arc<PendingRegistry>,
    ) -> Self {
        Self {
            producer,
            node,
            registry,
            _response: PhantomData,
        }
    }

    /// Sends one request and waits for one answer per subsystem.
    ///
    /// A complete response set can return before Kafka reports delivery. In
    /// that case, producer telemetry and deduplication after the report do not
    /// run because this function drops the send future.
    ///
    /// The span is named for the function, exactly as
    /// [`ProsodyProducer::send`] is, and it covers the whole wait. A client
    /// span covers a request and its response together, so nothing opens a
    /// second span for the answer arriving. The produce nests inside it.
    ///
    /// # Errors
    ///
    /// Returns [`RequestError`] for invalid arguments, failed admission, a
    /// produce failure without a response, or shutdown.
    #[instrument(
        skip_all,
        fields(
            otel.kind = "client",
            messaging.system = "kafka",
            topic = topic.as_ref(),
            key = %key,
            response.node = %self.node,
            request.id = Empty,
            responses.received = Empty,
            subsystems = subsystems.len() as i64,
        ),
        err
    )]
    pub async fn request<'a, H, V, E>(
        &self,
        headers: H,
        topic: Topic,
        key: &'a str,
        payload: C::Payload,
        subsystems: &'a [SubsystemName],
        timeout: Duration,
    ) -> Result<Vec<Outcome<V, E>>, RequestError<C::Error>>
    where
        H: IntoIterator<Item = (&'static str, &'a str)>,
        H::IntoIter: ExactSizeIterator,
        C::Payload: EventIdentity,
        R: Codec<Payload = Result<V, E>>,
        E: ClassifyError,
    {
        // The two id texts are declared before the header list, so they outlive
        // the borrows the list holds on them.
        let mut request_buf = [0_u8; ID_TEXT_LEN];
        let mut node_buf = [0_u8; ID_TEXT_LEN];
        let user_headers = headers.into_iter();
        let capacity = user_headers
            .len()
            .saturating_add(subsystems.len())
            .saturating_add(RESERVED_SINGLETONS);
        let mut record_headers =
            SmallVec::<[(&'static str, &str); HEADER_INLINE]>::with_capacity(capacity);
        for (name, value) in user_headers {
            if is_reserved(name) {
                return Err(RequestError::ReservedHeader { name });
            }
            record_headers.push((name, value));
        }

        let registration = self.registry.register(subsystems, timeout, R::FORMAT_ID)?;
        Span::current().record("request.id", display(registration.id()));

        append_request_headers(
            &mut record_headers,
            registration.id(),
            &mut request_buf,
            self.node,
            &mut node_buf,
            subsystems,
        );

        let deadline = registration.deadline();
        let started = Instant::now();
        let collected = collect::<R, V, E, _, _>(
            &registration,
            self.producer
                .send(record_headers.iter().copied(), topic, key, payload),
            deadline,
        )
        .await;
        // A request refused before this point sent nothing, so it has no
        // latency to report. Only a call that really waited records one.
        let waited = started.elapsed().as_secs_f64();
        match &collected {
            Ok(outcomes) => {
                let answered = outcomes.iter().filter(|outcome| outcome.answered()).count();
                Span::current().record("responses.received", answered as i64);
                LATENCY.record(
                    waited,
                    &[KeyValue::new(
                        "outcome",
                        completeness(answered, outcomes.len()),
                    )],
                );
            }
            Err(_) => LATENCY.record(waited, &[KeyValue::new("outcome", "failed")]),
        }
        collected
    }
}

impl<V, E> Outcome<V, E> {
    /// Whether the subsystem this outcome stands for answered at all.
    ///
    /// Every other failure is an answer that could not be used, which is a
    /// different fact from silence.
    const fn answered(&self) -> bool {
        !matches!(self, Self::Failed(ResponseFailure::Timeout))
    }
}

/// How complete one request's answers were, as the fixed label its latency is
/// recorded under.
///
/// A label rather than a subsystem name or a node id: those arrive from the
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
///
/// Each id arrives as its own type and is rendered here, so the two cannot be
/// written to each other's header. One `response-awaited` header carries one
/// name. A comma is legal in a subsystem name, so one joined header could not
/// be read back.
fn append_request_headers<'a>(
    headers: &mut SmallVec<[(&'static str, &'a str); HEADER_INLINE]>,
    request: RequestId,
    request_buf: &'a mut [u8; ID_TEXT_LEN],
    node: NodeId,
    node_buf: &'a mut [u8; ID_TEXT_LEN],
    subsystems: &'a [SubsystemName],
) {
    headers.push((RESPONSE_VERSION_HEADER, REQUEST_REVISION));
    headers.push((
        RESPONSE_REQUEST_ID_HEADER,
        id_text(request.into(), request_buf),
    ));
    headers.push((RESPONSE_NODE_HEADER, id_text(node.into(), node_buf)));
    headers.extend(
        subsystems
            .iter()
            .map(|subsystem| (RESPONSE_AWAITED_HEADER, subsystem.as_str())),
    );
}
