//! The client-local gauges that librdkafka statistics reports feed:
//! fetch-queue depth and size per assigned partition, and the age of the
//! client's topic metadata.
//!
//! Assignment and lag are left to broker-side exporters, which already report
//! them. A second series would compete with theirs for no gain.
//!
//! These are ordinary synchronous gauges, so a series keeps its last recorded
//! value until something records over it. A revoked partition is therefore
//! zeroed explicitly.
//!
//! Attribute values are owned by the `OTel` API, so the identity strings are
//! cloned once per partition per statistics report. There is no attribute cache
//! keyed by topic or partition.

#[cfg(test)]
mod tests;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Gauge, Meter};
use parking_lot::Mutex;
use rdkafka::Statistics;
use rdkafka::statistics::Partition as StatsPartition;
use std::sync::Arc;

/// librdkafka's internal partition entry, which belongs to no real partition.
const INTERNAL_PARTITION: i32 = -1;

/// The gauges of one Kafka client.
///
/// The client's context owns it, so it lives exactly as long as the client.
/// Dropping it zeroes the last report's series: a stopped consumer stops
/// contributing to `sum` aggregations. librdkafka delivers no statistics after
/// the client is destroyed, and the context drops only after that.
pub(super) struct KafkaMetrics {
    group: Arc<str>,
    fetch_queue_messages: Gauge<u64>,
    fetch_queue_size: Gauge<u64>,
    metadata_age: Gauge<u64>,
    /// The newest report. The next report or the drop retires its series.
    last: Mutex<Option<Statistics>>,
}

impl KafkaMetrics {
    /// Registers the three instruments on `meter`. Production passes the global
    /// meter; tests pass one from their own provider, never the process-global
    /// one.
    pub(super) fn new(meter: &Meter, group_id: &str) -> Self {
        Self {
            group: Arc::from(group_id),
            fetch_queue_messages: meter
                .u64_gauge("prosody.kafka.consumer.fetch_queue.messages")
                .with_description(
                    "Prefetched messages waiting in an assigned partition's fetch queue",
                )
                .with_unit("{message}")
                .build(),
            fetch_queue_size: meter
                .u64_gauge("prosody.kafka.consumer.fetch_queue.size")
                .with_description("Bytes waiting in an assigned partition's fetch queue")
                .with_unit("By")
                .build(),
            metadata_age: meter
                .u64_gauge("prosody.kafka.consumer.metadata.age")
                .with_description("Age of the client's metadata for its assigned topics")
                .with_unit("ms")
                .build(),
            last: Mutex::new(None),
        }
    }

    /// Records `incoming` and keeps it as the newest report.
    ///
    /// The statistics callback runs on the poll thread, so the lock is never
    /// contended.
    pub(super) fn observe(&self, incoming: Statistics) {
        let mut last = self.last.lock();
        self.record(last.as_ref(), &incoming);
        *last = Some(incoming);
    }

    /// Retires the series `previous` reported and no longer holds, then records
    /// the current values from `incoming`.
    ///
    /// Retirement compares the two raw statistics trees directly, so nothing is
    /// collected or allocated to decide what survived. A revoked partition
    /// normally stays in the tree with `desired` cleared, which counts as
    /// retired.
    ///
    /// Retired series are addressed with `incoming`'s identity attributes:
    /// librdkafka's handle name and the configured `client.id` are fixed for a
    /// client's lifetime, so they name the same series either way.
    fn record(&self, previous: Option<&Statistics>, incoming: &Statistics) {
        if let Some(previous) = previous {
            for (topic, id, _) in assigned_partitions(previous) {
                if retained(incoming, topic, id) {
                    continue;
                }
                self.zero_partition(incoming, topic, id);
            }
        }

        for (topic, id, partition) in assigned_partitions(incoming) {
            let attributes = self.partition_attributes(incoming, topic, id);
            // rdkafka declares the queue length as `i64` and the gauge is
            // `u64`; librdkafka only ever emits a non-negative length.
            self.fetch_queue_messages.record(
                u64::try_from(partition.fetchq_cnt).unwrap_or(0),
                &attributes,
            );
            self.fetch_queue_size
                .record(partition.fetchq_size, &attributes);
        }

        self.metadata_age.record(
            oldest_metadata_age(incoming),
            &self.base_attributes(incoming),
        );
    }

    /// Zeroes every series of `last`'s assignment.
    fn zero_assigned(&self, last: &Statistics) {
        for (topic, id, _) in assigned_partitions(last) {
            self.zero_partition(last, topic, id);
        }
        self.metadata_age.record(0, &self.base_attributes(last));
    }

    /// Records zero on both per-partition gauges for one `(topic, id)` series.
    fn zero_partition(&self, statistics: &Statistics, topic: &str, id: i32) {
        let attributes = self.partition_attributes(statistics, topic, id);
        self.fetch_queue_messages.record(0, &attributes);
        self.fetch_queue_size.record(0, &attributes);
    }

    /// The identity attributes every gauge carries.
    fn base_attributes(&self, statistics: &Statistics) -> [KeyValue; 4] {
        [
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.client.id", statistics.client_id.clone()),
            KeyValue::new("messaging.consumer.group.name", self.group.clone()),
            KeyValue::new("prosody.kafka.consumer.name", statistics.name.clone()),
        ]
    }

    /// The identity attributes plus the partition a per-partition gauge
    /// reports.
    fn partition_attributes(&self, statistics: &Statistics, topic: &str, id: i32) -> [KeyValue; 6] {
        let [system, client, group, name] = self.base_attributes(statistics);
        [
            system,
            client,
            group,
            name,
            KeyValue::new("messaging.destination.name", topic.to_owned()),
            KeyValue::new("messaging.destination.partition.id", i64::from(id)),
        ]
    }
}

impl Drop for KafkaMetrics {
    fn drop(&mut self) {
        if let Some(last) = self.last.get_mut().take() {
            self.zero_assigned(&last);
        }
    }
}

/// The topic, id, and statistics of every partition `statistics` reports as
/// assigned to this instance. The map key is the canonical partition id; the
/// duplicated `partition` field inside each entry is never trusted.
fn assigned_partitions(
    statistics: &Statistics,
) -> impl Iterator<Item = (&str, i32, &StatsPartition)> {
    statistics.topics.iter().flat_map(|(name, topic)| {
        topic.partitions.iter().filter_map(move |(&id, partition)| {
            is_assigned(id, partition).then_some((name.as_str(), id, partition))
        })
    })
}

/// Whether librdkafka reports this entry as a real partition assigned to this
/// instance. `desired` is what the rebalance sets.
///
/// `unknown` is deliberately not consulted: a partition missing from broker
/// metadata still has client-side queues worth reporting.
fn is_assigned(id: i32, partition: &StatsPartition) -> bool {
    id != INTERNAL_PARTITION && partition.desired
}

/// Whether `incoming` still reports `(topic, id)` as assigned to this instance.
fn retained(incoming: &Statistics, topic: &str, id: i32) -> bool {
    incoming
        .topics
        .get(topic)
        .and_then(|topic| topic.partitions.get(&id))
        .is_some_and(|partition| is_assigned(id, partition))
}

/// The oldest metadata age among topics with at least one assigned partition,
/// or zero when this instance holds no assignment.
///
/// librdkafka reports zero for a topic it has never refreshed, which is
/// indistinguishable from a topic refreshed just now.
fn oldest_metadata_age(statistics: &Statistics) -> u64 {
    statistics
        .topics
        .values()
        .filter(|topic| {
            topic
                .partitions
                .iter()
                .any(|(&id, partition)| is_assigned(id, partition))
        })
        .map(|topic| u64::try_from(topic.metadata_age).unwrap_or(0))
        .max()
        .unwrap_or(0)
}
