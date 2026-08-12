//! Concurrent Kafka delivery and subsystem response collection.

use super::registry::Registration;
use super::{RequestError, ResponseError};
use crate::Codec;
use crate::peer::response::frame::{FrameResult, HandlerError, ResponseFrame, ResponseSuccess};
use crate::producer::ProducerError;
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
pub(crate) async fn collect<R, V, F, PE>(
    registration: &mut Registration,
    produce: F,
) -> Result<Vec<Result<V, ResponseError>>, RequestError<PE>>
where
    R: Codec<Payload = V>,
    F: Future<Output = Result<(), ProducerError<PE>>>,
    PE: Error,
{
    let deadline = registration.deadline();
    let receivers = registration.take_receivers();
    let mut results = (0..receivers.len())
        .map(|_| Err(ResponseError::Timeout))
        .collect::<Vec<_>>();
    let mut responses = FuturesUnordered::new();
    for (index, receiver) in receivers.into_iter().enumerate() {
        responses.push(receiver.map(move |frame| (index, frame)));
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
                    results[index] = decode::<R, V>(frame);
                } else if registration.is_closed() {
                    return Err(RequestError::ShuttingDown);
                }
            }
            () = &mut deadline => break,
        }
    }

    Ok(results)
}

fn decode<R, V>(frame: ResponseFrame) -> Result<V, ResponseError>
where
    R: Codec<Payload = V>,
{
    match frame.result {
        FrameResult::Success(ResponseSuccess { format, payload }) => {
            if format.as_bytes() != R::FORMAT_ID.as_bytes() {
                return Err(ResponseError::FormatMismatch);
            }
            R::with_cached_local(|codec| codec.deserialize_bytes(payload))
                .map_err(|_| ResponseError::Malformed)
        }
        FrameResult::HandlerError(HandlerError { category, message }) => {
            let message = match message.try_into_mut() {
                Ok(message) => String::from_utf8(message.into()),
                Err(message) => String::from_utf8(message.to_vec()),
            }
            .map_err(|_| ResponseError::Malformed)?;
            Err(ResponseError::Handler { category, message })
        }
    }
}
