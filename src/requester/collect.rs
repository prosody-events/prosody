//! Completion and response decoding for one request.

use super::registry::{Arrival, Awaited, Registration, Status, Terminal};
use super::{Outcome, RequestError, ResponseFailure};
use crate::Codec;
use crate::error::ClassifyError;
use crate::producer::ProducerError;
use crate::response::ResponseStatus;
use std::error::Error;
use std::future::Future;
use std::pin::pin;
use tokio::select;
use tokio::time::{Instant, timeout_at};

/// Races the delivery report against request completion, then decodes results.
///
/// The produce arm runs first in each ready tie. Thus this function hands the
/// record to the producer, and reads a report that already resolved, before the
/// completion arm can close the request out from under it.
///
/// # Errors
///
/// Returns a produce error when no response preceded a failed report. Returns
/// [`RequestError::ShuttingDown`] when shutdown ends the request.
pub(crate) async fn collect<R, V, E, F, PE>(
    registration: &Registration,
    produce: F,
    deadline: Instant,
) -> Result<Vec<Outcome<V, E>>, RequestError<PE>>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
    F: Future<Output = Result<(), ProducerError<PE>>>,
    PE: Error,
{
    let request = registration.request();
    let produce = pin!(produce);
    let mut parked = pin!(timeout_at(deadline, request.park()));
    select! {
        biased;
        report = produce => {
            if let Err(error) = report
                && request.abandon_unanswered(Terminal::Cancelled)
            {
                return Err(RequestError::Produce(error));
            }
            drop(parked.as_mut().await);
        }
        _ = parked.as_mut() => {}
    }
    let finished = registration.finish();
    if finished.status == Status::ShuttingDown {
        return Err(RequestError::ShuttingDown);
    }
    Ok(decode::<R, V, E, _>(finished.awaited))
}

/// Decodes each position once through the response codec's local instance.
pub(in crate::requester) fn decode<R, V, E, I>(awaited: I) -> Vec<Outcome<V, E>>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
    I: IntoIterator<Item = Awaited>,
    I::IntoIter: ExactSizeIterator,
{
    R::with_cached_local(|codec| {
        let awaited = awaited.into_iter();
        let mut outcomes = Vec::with_capacity(awaited.len());
        for awaited in awaited {
            let outcome = match awaited.arrival {
                None => Outcome::Failed(ResponseFailure::Timeout),
                Some(Arrival::FormatMismatch) => Outcome::Failed(ResponseFailure::FormatMismatch),
                Some(Arrival::TooLarge) => Outcome::Failed(ResponseFailure::TooLarge),
                Some(Arrival::Response {
                    status,
                    mut payload,
                }) => match codec.deserialize(&mut payload) {
                    Ok(Ok(value)) if status == ResponseStatus::Success => Outcome::Ok(value),
                    Ok(Err(error)) => match status {
                        ResponseStatus::Error(category) if category == error.classify_error() => {
                            Outcome::Handler(error)
                        }
                        ResponseStatus::Success | ResponseStatus::Error(_) => {
                            Outcome::Failed(ResponseFailure::Malformed)
                        }
                    },
                    Err(_) | Ok(Ok(_)) => Outcome::Failed(ResponseFailure::Malformed),
                },
            };
            outcomes.push(outcome);
        }
        outcomes
    })
}
