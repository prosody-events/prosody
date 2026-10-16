//! Reads the records of each assigned partition from its own librdkafka queue.
//!
//! The rebalance context splits the queue of each partition before librdkafka
//! starts to fetch it. So every record of the partition goes to that queue,
//! and only the partition task reads it. [`Reader::read`] turns the queue into
//! the record stream of the partition task.
//!
//! Prosody never pauses a partition. A partition that holds
//! [`max_uncommitted`](crate::consumer::ConsumerConfiguration::max_uncommitted)
//! uncommitted messages stops reading, and its fetched records stay in
//! librdkafka. librdkafka stops fetching a partition when its queue holds the
//! [`partition_prefetch`](crate::consumer::ConsumerConfiguration::partition_prefetch).
//! A pause would discard those records and fetch them again.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use educe::Educe;
use futures::{Stream, StreamExt, pin_mut};
use rdkafka::consumer::ConsumerContext;
use rdkafka::consumer::base_consumer::PartitionQueue;
use tokio::sync::Notify;
use tracing::error;

use crate::Codec;
use crate::consumer::decode::{ResultRequestReader, decode_record};
use crate::consumer::message::ConsumerRecord;
use crate::otel::SpanRelation;
use crate::propagator::new_propagator;

#[cfg(test)]
mod tests;

/// Reads partition queues into record streams. Every partition of a consumer
/// reads through a clone of one reader.
///
/// # Invariant
///
/// A record is decoded only when the partition task asks for the next record.
/// So the reader adds no buffer of its own.
#[derive(Educe)]
#[educe(Clone(bound(R: Clone)))]
pub(crate) struct Reader<C, R> {
    /// Reads the result-request headers that this consumer answers.
    requests: R,

    /// How a receive span relates to the producer's trace context.
    spans: SpanRelation,

    _codec: PhantomData<fn() -> C>,
}

impl<C, R> Reader<C, R>
where
    C: Codec,
    R: ResultRequestReader + 'static,
{
    /// Creates a reader that answers `requests` and relates each receive span
    /// to its producer by `spans`.
    pub(crate) fn new(requests: R, spans: SpanRelation) -> Self {
        Self {
            requests,
            spans,
            _codec: PhantomData,
        }
    }

    /// Reads the records of `queue` in offset order.
    ///
    /// The stream never ends. The partition task stops when it drops the
    /// stream, and the queue drops with it. The stream logs and skips a
    /// partition error and a record that does not decode.
    pub(crate) fn read<Ctx: ConsumerContext>(
        &self,
        mut queue: PartitionQueue<Ctx>,
    ) -> impl Stream<Item = ConsumerRecord<C::Payload>> + Send + use<Ctx, C, R> {
        let ready = Arc::new(Notify::new());
        let notify = Arc::clone(&ready);
        queue.set_nonempty_callback(move || notify.notify_one());

        let Self {
            requests, spans, ..
        } = self.clone();

        stream! {
            let propagator = new_propagator();
            let arrived = arrivals(|| queue.poll(Duration::ZERO), &ready);
            pin_mut!(arrived);

            while let Some(polled) = arrived.next().await {
                let mut message = match polled {
                    Ok(message) => message,
                    Err(error) => {
                        error!("partition queue error: {error:#}");
                        continue;
                    }
                };

                let decoded = C::with_cached_local(|codec| {
                    decode_record(&mut message, &propagator, codec, &requests)
                });
                if let Some(decoded) = decoded {
                    yield decoded.into_record(None, |meta| meta.receive_span(spans));
                }
            }
        }
    }
}

/// Yields each item that `poll` returns, in order, and waits on `ready` when
/// `poll` finds nothing.
///
/// `poll` must not block. It returns `None` when the queue is empty. `ready`
/// must get a `notify_one` each time the queue changes from empty to not
/// empty.
///
/// # Invariant
///
/// **No lost wakeup.** The stream waits on `ready` only after `poll` returns
/// `None`. An item that arrives after that check calls `notify_one`, which
/// [`Notify`] keeps when nothing waits yet. So the wait ends at once.
fn arrivals<'a, M: 'a>(
    mut poll: impl FnMut() -> Option<M> + 'a,
    ready: &'a Notify,
) -> impl Stream<Item = M> + 'a {
    stream! {
        loop {
            match poll() {
                Some(item) => yield item,
                None => ready.notified().await,
            }
        }
    }
}
