//! Backpressure for the poll loop: records that wait for room to dispatch.

use std::collections::VecDeque;
use std::sync::Arc;

use rdkafka::TopicPartitionList;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Sender, WeakSender};

use super::create_receive_span;
use crate::consumer::decode::DecodedRecord;
use crate::consumer::message::ConsumerRecord;
use crate::otel::SpanRelation;
use crate::{Partition, Topic};

/// Limits the records in flight across all partitions, and never strands a
/// partition.
///
/// A record dispatches when an in-flight permit is free and its partition
/// channel has room. Otherwise the record waits here, and its partition pauses
/// until the record dispatches. Waiting records dispatch in arrival order.
///
/// # Invariant
///
/// The poll loop pauses a partition only while a record that the partition
/// delivered waits here. librdkafka resumes the partition after that record.
/// A fetch can return only a batch with no deliverable record, such as a batch
/// at the end of a compacted or transactional log segment. librdkafka then
/// moves its fetch position past the batch, but a pause moves it back.
/// A partition that delivers nothing never pauses, so it always gets past
/// such a batch.
pub(super) struct Backpressure<P> {
    /// Limits the records in flight across all partitions.
    permits: Arc<Semaphore>,

    /// Records in arrival order, at most one for each partition assignment.
    waiting: VecDeque<Waiting<P>>,

    /// How a receive span relates to the producer's trace context.
    spans: SpanRelation,
}

/// A decoded record that waits for a permit or for room in its partition.
struct Waiting<P> {
    record: DecodedRecord<P>,

    /// The channel of the partition assignment that delivered the record.
    /// Revoking the assignment closes it.
    channel: WeakSender<ConsumerRecord<P>>,

    topic: Topic,
    partition: Partition,
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

    /// Dispatches `record` to `channel`, or holds it and pauses its partition.
    ///
    /// A record waits behind every earlier waiting record, so partitions get
    /// permits in the order their records arrive.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition does not pause. The record still
    /// waits.
    pub(super) fn submit<C: ConsumerContext>(
        &mut self,
        consumer: &BaseConsumer<C>,
        record: DecodedRecord<P>,
        channel: &Sender<ConsumerRecord<P>>,
        topic: Topic,
        partition: Partition,
    ) -> KafkaResult<()> {
        let record = if self.waiting.is_empty() {
            match self.dispatch(record, channel) {
                Ok(()) => return Ok(()),
                Err(record) => record,
            }
        } else {
            record
        };

        self.waiting.push_back(Waiting {
            record,
            channel: channel.downgrade(),
            topic,
            partition,
        });

        let mut paused = TopicPartitionList::with_capacity(1);
        paused.add_partition(topic.as_ref(), partition);
        consumer.pause(&paused)
    }

    /// Dispatches waiting records in arrival order while permits remain, and
    /// resumes the partition of each record it dispatches.
    ///
    /// # Errors
    ///
    /// Returns an error if the dispatched partitions do not resume.
    pub(super) fn drain<C: ConsumerContext>(
        &mut self,
        consumer: &BaseConsumer<C>,
    ) -> KafkaResult<()> {
        if self.permits.available_permits() == 0 {
            return Ok(());
        }

        let mut resumed: Option<TopicPartitionList> = None;
        for _ in 0..self.waiting.len() {
            let Some(waiting) = self.waiting.pop_front() else {
                break;
            };

            // A revoked partition's next owner reads the record again.
            let Some(channel) = waiting.channel.upgrade() else {
                continue;
            };

            match self.dispatch(waiting.record, &channel) {
                Ok(()) => {
                    resumed
                        .get_or_insert_with(TopicPartitionList::new)
                        .add_partition(waiting.topic.as_ref(), waiting.partition);
                }
                Err(record) => self.waiting.push_back(Waiting { record, ..waiting }),
            }
        }

        resumed.map_or(Ok(()), |resumed| consumer.resume(&resumed))
    }

    /// Sends `record` to `channel` with an in-flight permit. Returns the
    /// record if no permit is free or the channel is full.
    fn dispatch(
        &self,
        record: DecodedRecord<P>,
        channel: &Sender<ConsumerRecord<P>>,
    ) -> Result<(), DecodedRecord<P>> {
        let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            return Err(record);
        };

        match channel.try_reserve() {
            Ok(slot) => {
                slot.send(record.into_record(permit, |meta| create_receive_span(meta, self.spans)));
            }
            Err(TrySendError::Full(())) => return Err(record),
            // The partition is shutting down. Its next owner reads the record
            // again.
            Err(TrySendError::Closed(())) => {}
        }
        Ok(())
    }
}
