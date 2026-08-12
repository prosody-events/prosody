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
pub(crate) async fn collect<R, F, PE>(
    registration: Registration,
    produce: F,
) -> Result<Vec<Result<R::Payload, ResponseError>>, RequestError<PE>>
where
    R: Codec,
    F: Future<Output = Result<(), ProducerError<PE>>>,
    PE: Error,
{
    let (pending, receivers) = registration.into_parts();
    let deadline = pending.deadline();
    let mut results = (0..receivers.len())
        .map(|_| Err(ResponseError::Timeout))
        .collect::<Vec<_>>();
    // This set allocates per receiver but polls only receivers that wake. A
    // contiguous scan can make an unfavorable response order quadratic.
    let mut responses = FuturesUnordered::new();
    for (index, receiver) in receivers.into_iter().enumerate() {
        responses.push(receiver.map(move |frame| (index, frame)));
    }

    let mut produce = pin!(produce);
    let mut deadline = pin!(sleep_until(deadline));
    let mut reported = false;
    while !responses.is_empty() || !reported {
        select! {
            biased;
            report = &mut produce, if !reported => {
                report.map_err(RequestError::Produce)?;
                reported = true;
            }
            Some((index, frame)) = responses.next() => {
                match frame {
                    Ok(frame) => results[index] = decode::<R>(frame),
                    Err(_) => return Err(RequestError::ShuttingDown),
                }
            }
            () = &mut deadline => break,
        }
    }

    Ok(results)
}

fn decode<R: Codec>(frame: ResponseFrame) -> Result<R::Payload, ResponseError> {
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
