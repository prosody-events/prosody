//! Reads the records of each assigned partition from its own librdkafka queue.
//!
//! The rebalance context splits the queue of each partition before librdkafka
//! starts to fetch it. So every record of the partition goes to that queue,
//! and only the partition task reads it. [`Reader::read`] turns the queue into
//! the record stream of the partition task.
//!
//! Prosody never pauses a partition. A partition without an in-flight permit
//! stops reading, and its fetched records stay in librdkafka. librdkafka stops
//! fetching a partition when its queue holds the
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
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::Codec;
use crate::consumer::decode::{ResultRequestReader, decode_record};
use crate::consumer::message::ConsumerRecord;
use crate::kafka_log::log_error;
use crate::otel::SpanRelation;
use crate::propagator::new_propagator;

#[cfg(test)]
mod tests;

/// Reads partition queues into record streams. Every partition of a consumer
/// reads through a clone of one reader.
///
/// # Invariant
///
/// A record is decoded only while it holds one of `permits`. The record keeps
/// the permit until it completes. So the consumer holds at most
/// `max_uncommitted` decoded records, across all partitions.
#[derive(Educe)]
#[educe(Clone(bound(R: Clone)))]
pub(crate) struct Reader<C, R> {
    /// Limits the records in flight across all partitions.
    permits: Arc<Semaphore>,

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
    /// Creates a reader that allows `max_in_flight` records in flight.
    pub(crate) fn new(max_in_flight: usize, requests: R, spans: SpanRelation) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_in_flight)),
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
            permits,
            requests,
            spans,
            ..
        } = self.clone();

        stream! {
            let propagator = new_propagator();
            let admitted = admit(|| queue.poll(Duration::ZERO), &ready, &permits);
            pin_mut!(admitted);

            while let Some((polled, permit)) = admitted.next().await {
                let mut message = match polled {
                    Ok(message) => message,
                    Err(error) => {
                        log_error(&error, "partition queue poll failed");
                        continue;
                    }
                };

                let decoded = C::with_cached_local(|codec| {
                    decode_record(&mut message, &propagator, codec, &requests)
                });
                if let Some(decoded) = decoded {
                    yield decoded.into_record(permit, |meta| meta.receive_span(spans));
                }
            }
        }
    }
}

/// Pairs each item that `poll` returns with an in-flight permit, in order.
///
/// `poll` must not block. It returns `None` when the queue is empty. `ready`
/// must get a `notify_one` each time the queue changes from empty to not
/// empty.
///
/// # Invariants
///
/// - **At most one item waits.** The stream polls an item only after it yields
///   the previous one. While it waits for a permit, it holds that one item, and
///   the rest stay in the queue.
/// - **No lost wakeup.** The stream waits on `ready` only after `poll` returns
///   `None`. An item that arrives after that check calls `notify_one`, which
///   stores a permit when nothing waits yet. So the wait ends at once. The
///   stream polls again after each permit, so a queue that filled while the
///   stream waited for a permit gets read without a new notification.
fn admit<'a, M: 'a>(
    mut poll: impl FnMut() -> Option<M> + 'a,
    ready: &'a Notify,
    permits: &'a Arc<Semaphore>,
) -> impl Stream<Item = (M, OwnedSemaphorePermit)> + 'a {
    stream! {
        loop {
            let Some(item) = poll() else {
                ready.notified().await;
                continue;
            };

            // Nothing closes the semaphore.
            let Ok(permit) = Arc::clone(permits).acquire_owned().await else {
                break;
            };
            yield (item, permit);
        }
    }
}
