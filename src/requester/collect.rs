//! Concurrent Kafka delivery and subsystem response collection.

use super::registry::Registration;
use super::{Outcome, RequestError, ResponseFailure};
use crate::Codec;
use crate::error::ClassifyError;
use crate::producer::ProducerError;
use crate::response::ResponseStatus;
use crate::response::frame::ResponseFrame;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::error::Error;
use std::future::Future;
use std::iter::repeat_with;
use std::pin::pin;
use tokio::select;
use tokio::time::sleep_until;

/// Races Kafka delivery, subsystem responses, and the request deadline.
///
/// Before the deadline, completion requires both the Kafka report and every
/// subsystem outcome. Thus, responses cannot cancel producer side effects.
pub(crate) async fn collect<R, V, E, F, PE>(
    registration: &mut Registration,
    produce: F,
) -> Result<Vec<Outcome<V, E>>, RequestError<PE>>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
    F: Future<Output = Result<(), ProducerError<PE>>>,
    PE: Error,
{
    let deadline = registration.deadline();
    let waiters = registration.take_waiters();
    let mut outcomes: Vec<Option<Outcome<V, E>>> =
        repeat_with(|| None).take(waiters.len()).collect();
    let mut responses = FuturesUnordered::new();
    for (index, waiter) in waiters.into_iter().enumerate() {
        responses.push(waiter.map(move |frame| (index, frame)));
    }

    let mut produce = pin!(produce);
    let mut deadline = pin!(sleep_until(deadline));
    let mut sent = false;
    while !responses.is_empty() || !sent {
        select! {
            biased;
            report = &mut produce, if !sent => {
                report.map_err(RequestError::Produce)?;
                sent = true;
            }
            Some((index, frame)) = responses.next() => {
                if let Ok(frame) = frame {
                    outcomes[index] = Some(decode::<R, V, E>(frame));
                } else if registration.is_closed() {
                    return Err(RequestError::ShuttingDown);
                }
            }
            () = &mut deadline => break,
        }
    }

    Ok(outcomes
        .into_iter()
        .map(|outcome| match outcome {
            Some(outcome) => outcome,
            None => Outcome::Failed(ResponseFailure::Timeout),
        })
        .collect())
}

fn decode<R, V, E>(frame: ResponseFrame) -> Outcome<V, E>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
{
    if frame.format.to_str() != R::FORMAT_ID {
        return Outcome::Failed(ResponseFailure::FormatMismatch);
    }
    R::with_cached_local(|codec| match codec.deserialize_owned(frame.payload) {
        Ok(Ok(value)) if frame.header.status == ResponseStatus::Success => Outcome::Ok(value),
        Ok(Err(error)) => match frame.header.status {
            ResponseStatus::Error(category) if category == error.classify_error() => {
                Outcome::Handler(error)
            }
            ResponseStatus::Success | ResponseStatus::Error(_) => {
                Outcome::Failed(ResponseFailure::Malformed)
            }
        },
        Err(_) | Ok(Ok(_)) => Outcome::Failed(ResponseFailure::Malformed),
    })
}
