//! Concurrent Kafka delivery and subsystem response collection.

use super::registry::Registration;
use super::{RequestError, ResponseError};
use crate::Codec;
use crate::error::ClassifyError;
use crate::producer::ProducerError;
use crate::response::ResponseStatus;
use crate::response::frame::ResponseFrame;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::error::Error;
use std::future::Future;
use std::pin::pin;
use tokio::select;
use tokio::time::sleep_until;

/// Races Kafka delivery, subsystem responses, and the request deadline.
///
/// Before the deadline, completion requires both the Kafka report and every
/// subsystem result. Thus, responses cannot cancel producer side effects.
pub(crate) async fn collect<R, V, E, F, PE>(
    registration: &mut Registration,
    produce: F,
) -> Result<Vec<Result<V, ResponseError<E>>>, RequestError<PE>>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
    F: Future<Output = Result<(), ProducerError<PE>>>,
    PE: Error,
{
    let deadline = registration.deadline();
    let waiters = registration.take_waiters();
    let mut results = (0..waiters.len())
        .map(|_| Err(ResponseError::Timeout))
        .collect::<Vec<_>>();
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
                    results[index] = decode::<R, V, E>(frame);
                } else if registration.is_closed() {
                    return Err(RequestError::ShuttingDown);
                }
            }
            () = &mut deadline => break,
        }
    }

    Ok(results)
}

fn decode<R, V, E>(frame: ResponseFrame) -> Result<V, ResponseError<E>>
where
    R: Codec<Payload = Result<V, E>>,
    E: ClassifyError,
{
    if frame.format.to_str() != R::FORMAT_ID {
        return Err(ResponseError::FormatMismatch);
    }
    R::with_cached_local(|codec| match codec.deserialize_bytes(frame.payload) {
        Ok(Ok(value)) if frame.header.status == ResponseStatus::Success => Ok(value),
        Ok(Err(error)) => match frame.header.status {
            ResponseStatus::Error(category) if category == error.classify_error() => {
                Err(ResponseError::Handler(error))
            }
            ResponseStatus::Success | ResponseStatus::Error(_) => Err(ResponseError::Malformed),
        },
        Err(_) | Ok(Ok(_)) => Err(ResponseError::Malformed),
    })
}
