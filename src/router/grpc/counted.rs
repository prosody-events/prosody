//! Counting the frames the transport refuses before the peer method can run.
//!
//! The reader counts what it refuses itself, but it never sees a frame over the
//! listener's configured ceiling: the transport answers that one `OUT_OF_RANGE`
//! from above the codec, where no counter of this crate's can reach. So the
//! refusal is counted where it is observable — on the answer the peer service
//! sends back.

use super::TRANSPORT;
use futures::FutureExt;
use futures::future::Map;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http::{Request, Response};
use tonic::server::NamedService;
use tonic::{Code, Status};

/// What one call answered, before it reaches the peer that made it.
type Answer<E> = Result<Response<Body>, E>;

/// One service, with every frame it refused for size counted.
///
/// It wraps the peer service alone. The health and reflection services carry a
/// service name at most, and a caller that sends more than that is not sending
/// a response frame.
#[derive(Clone, Debug)]
pub(super) struct Counted<S>(S);

impl<S> Counted<S> {
    /// Counts what `service` refuses.
    pub(super) const fn new(service: S) -> Self {
        Self(service)
    }
}

/// The wrapped service answers under the name it already had, so wrapping it
/// cannot move it to another route.
impl<S: NamedService> NamedService for Counted<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<Request<B>> for Counted<S>
where
    S: Service<Request<B>, Response = Response<Body>>,
{
    type Error = S::Error;
    type Future = Map<S::Future, fn(Answer<S::Error>) -> Answer<S::Error>>;
    type Response = Response<Body>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.0.poll_ready(context)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.0.call(request).map(count_refused)
    }
}

/// Counts an answer the transport refused for size.
///
/// A refused message never reaches the peer method, so the status is carried in
/// the response head rather than in a trailer. `OUT_OF_RANGE` names the size
/// refusal alone: every disposition the peer method can answer with names a
/// different status.
fn count_refused<E>(answer: Answer<E>) -> Answer<E> {
    if let Ok(response) = &answer
        && Status::from_header_map(response.headers())
            .is_some_and(|status| status.code() == Code::OutOfRange)
    {
        TRANSPORT.record_rejected_frame();
    }
    answer
}
