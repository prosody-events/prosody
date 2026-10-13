//! Backpressure for the poll loop: records that wait for room to dispatch.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{Permit, Sender, WeakSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout_at};
use tracing::{Span, error};

use super::create_receive_span;
use crate::consumer::decode::DecodedRecord;
use crate::consumer::message::ConsumerRecord;
use crate::otel::SpanRelation;

#[cfg(test)]
mod tests;

/// Limits the records in flight across all partitions, and never holds a
/// partition at a batch that it cannot deliver.
///
/// A record dispatches when an in-flight permit is free and its partition
/// channel has room. Otherwise the record waits here, and its partition pauses
/// until the record dispatches. Waiting records dispatch strictly in arrival
/// order, so the records of one partition reach its channel in order.
///
/// # Invariant
///
/// The poll loop pauses a partition only while a record that the partition
/// delivered waits here. librdkafka resumes the partition at the next offset.
/// A fetch can return only a batch with no deliverable record, such as a batch
/// at the end of a compacted or transactional log segment. librdkafka then
/// moves its fetch position past the batch, but a pause moves it back.
/// A partition that delivers nothing never pauses, so it always gets past
/// such a batch.
pub(super) struct Backpressure<P> {
    /// Limits the records in flight across all partitions.
    permits: Arc<Semaphore>,

    /// Records in arrival order. A partition pauses when its record enters the
    /// queue. So the queue holds one record for each paused partition, unless a
    /// pause fails.
    waiting: VecDeque<Waiting<P>>,

    /// How a receive span relates to the producer's trace context.
    spans: SpanRelation,
}

/// A decoded record that waits for a permit or for room in its partition.
struct Waiting<P> {
    record: DecodedRecord<P>,

    /// The receive span, which starts when the record arrives.
    span: Span,

    /// The channel of the partition assignment that delivered the record.
    ///
    /// A revoke drops the only strong sender, so a failed upgrade means that
    /// the assignment ended. Do not hold a strong clone: it keeps a revoked
    /// channel open. Do not look up the channel by partition: a later
    /// assignment of the partition would get the record twice.
    channel: WeakSender<ConsumerRecord<P>>,
}

impl<P> Backpressure<P> {
    /// Creates backpressure that allows `max_in_flight` records in flight.
    pub(super) fn new(max_in_flight: usize, spans: SpanRelation) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_in_flight)),
            waiting: VecDeque::new(),
            spans,
        }
    }

    /// Returns whether a record waits. [`Self::drain`] then waits for room, so
    /// the poll loop must not block in its poll as well.
    pub(super) fn has_waiting(&self) -> bool {
        !self.waiting.is_empty()
    }

    /// Dispatches `record` to `channel`, or holds it and pauses its partition.
    ///
    /// A record waits behind every earlier waiting record, so partitions get
    /// permits in the order their records arrive.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition does not pause. The record still
    /// waits, and the next record from the partition pauses it again.
    pub(super) fn submit<C: ConsumerContext>(
        &mut self,
        consumer: &BaseConsumer<C>,
        record: DecodedRecord<P>,
        channel: &Sender<ConsumerRecord<P>>,
    ) -> KafkaResult<()> {
        let span = create_receive_span(record.meta(), self.spans);
        if self.waiting.is_empty()
            && let Some((permit, slot)) = self.reserve(channel).now_or_never().flatten()
        {
            slot.send(record.into_record(permit, |_| span));
            return Ok(());
        }

        let paused = partition_list(&record);
        self.waiting.push_back(Waiting {
            record,
            span,
            channel: channel.downgrade(),
        });
        consumer.pause(&paused)
    }

    /// Dispatches waiting records in arrival order, and resumes the partition
    /// of each record it dispatches.
    ///
    /// Waits up to `timeout` for room when a record waits, so the poll loop
    /// sees each permit as soon as a handler frees it.
    ///
    /// The drain stops at the first record that cannot dispatch. This costs
    /// nothing: a channel holds at most one record for each permit, so a full
    /// channel leaves no free permit.
    ///
    /// # Errors
    ///
    /// Returns an error if a partition does not resume. Its record still waits
    /// and the next drain tries again.
    pub(super) fn drain<C: ConsumerContext>(
        &mut self,
        consumer: &BaseConsumer<C>,
        timeout: Duration,
    ) -> KafkaResult<()> {
        let deadline = Instant::now() + timeout;
        while let Some(waiting) = self.waiting.pop_front() {
            // A revoked partition's next owner reads the record again.
            let Some(channel) = waiting.channel.upgrade() else {
                continue;
            };

            let reserved = timeout_at(deadline, self.reserve(&channel));
            let Some((permit, slot)) = Handle::current().block_on(reserved).unwrap_or_default()
            else {
                if channel.is_closed() {
                    let meta = waiting.record.meta();
                    error!(
                        topic = %meta.topic,
                        partition = meta.partition,
                        offset = meta.offset,
                        "partition processing stopped; discarding message"
                    );
                    continue;
                }
                self.waiting.push_front(waiting);
                return Ok(());
            };

            if let Err(error) = consumer.resume(&partition_list(&waiting.record)) {
                self.waiting.push_front(waiting);
                return Err(error);
            }
            slot.send(waiting.record.into_record(permit, |_| waiting.span));
        }
        Ok(())
    }

    /// Waits for an in-flight permit and a slot in `channel`. Returns `None`
    /// if the channel closes.
    async fn reserve<'a>(
        &self,
        channel: &'a Sender<ConsumerRecord<P>>,
    ) -> Option<(OwnedSemaphorePermit, Permit<'a, ConsumerRecord<P>>)> {
        let Ok(permit) = Arc::clone(&self.permits).acquire_owned().await else {
            return None;
        };
        let Ok(slot) = channel.reserve().await else {
            return None;
        };
        Some((permit, slot))
    }
}

/// A list that names the partition of `record`, for a pause or a resume.
///
/// The list allocates. The pause and resume calls also allocate inside
/// librdkafka, and both run only when a record waits.
fn partition_list<P>(record: &DecodedRecord<P>) -> TopicPartitionList {
    let meta = record.meta();
    let mut list = TopicPartitionList::with_capacity(1);
    list.add_partition(meta.topic.as_ref(), meta.partition);
    list
}
