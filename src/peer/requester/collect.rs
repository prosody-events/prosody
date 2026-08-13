//! Concurrent Kafka delivery and subsystem response collection.

use super::registry::{IndexedFrameReceivers, Registration};
use super::{RequestError, ResponseError};
use crate::Codec;
use crate::peer::response::frame::{FrameResult, HandlerError, ResponseFrame, ResponseSuccess};
use crate::producer::ProducerError;
use std::error::Error;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::Poll;
use tokio::select;
use tokio::sync::oneshot::error::RecvError;
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
    let mut responses = receivers
        .into_iter()
        .enumerate()
        .collect::<IndexedFrameReceivers>();

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
            (index, frame) = next_response(&mut responses), if !responses.is_empty() => {
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

/// Returns the next response without allocating one task node per receiver.
/// The collection loop calls this only while at least one receiver remains.
async fn next_response(
    responses: &mut IndexedFrameReceivers,
) -> (usize, Result<ResponseFrame, RecvError>) {
    poll_fn(|context| {
        for slot in 0..responses.len() {
            if let Poll::Ready(frame) = Pin::new(&mut responses[slot].1).poll(context) {
                let (index, _) = responses.swap_remove(slot);
                return Poll::Ready((index, frame));
            }
        }
        Poll::Pending
    })
    .await
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
