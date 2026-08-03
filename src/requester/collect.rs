//! Completion and response decoding for one request.

use super::registry::{Arrival, Awaited, Registration, Status};
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
/// The producer future runs first in each ready tie. Thus, Kafka receives the
/// record before this function can observe a response completion.
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
    let mut produce = pin!(produce);
    let mut parked = pin!(timeout_at(deadline, request.park()));
    let mut reported = false;
    loop {
        if reported {
            drop(parked.as_mut().await);
            break;
        }
        select! {
            biased;
            report = &mut produce => {
                if let Err(error) = report
                    && request.abandon_if_empty(Status::Cancelled)
                {
                    return Err(RequestError::Produce(error));
                }
                reported = true;
            }
            _ = parked.as_mut() => break,
        }
    }
    let finished = registration.finish(Status::TimedOut);
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
{
    R::with_cached_local(|codec| {
        let awaited = awaited.into_iter();
        let mut outcomes = Vec::with_capacity(awaited.size_hint().0);
        for awaited in awaited {
            let outcome = match awaited.arrival {
                None => Outcome::Failed(ResponseFailure::Timeout),
                Some(Arrival::Unreadable(failure)) => Outcome::Failed(failure),
                Some(Arrival::Response {
                    status,
                    mut payload,
                }) => match codec.deserialize(&mut payload) {
                    Ok(Ok(value)) if status == ResponseStatus::Success => Outcome::Ok(value),
                    Ok(Err(error)) => match status {
                        ResponseStatus::Error(category) if category == error.classify_error() => {
                            Outcome::Handler { error, category }
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
