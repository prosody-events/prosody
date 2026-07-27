//! What the primary Kafka client knows about itself.
//!
//! One [`KafkaObserver`] per consumer holds the newest observation that client
//! produced: the metadata fetched once at startup, then each librdkafka
//! statistics report. Readers take a [`KafkaSnapshotGuard`] and borrow from
//! that single generation, so a traversal never mixes two observations and
//! never allocates.
//!
//! Every consumer mode builds exactly one observer and hands the same instance
//! (by clone) to its primary consumer's context. A second observer would split
//! the observation stream: statistics would land on one handle while readers
//! queried another.

mod metrics;
#[cfg(test)]
mod tests;

use crate::consumer::observer::metrics::KafkaMetrics;
use arc_swap::ArcSwapOption;
use opentelemetry::global::meter;
use rdkafka::Statistics;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use rdkafka::statistics::Partition as StatsPartition;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

// Test-only until first-write publication consumes the observed count. The
// gates come off together with the lookup below.
#[cfg(test)]
use crate::error::{ClassifyError, ErrorCategory};
#[cfg(test)]
use crate::state_reader::{PartitionCount, PartitionCountError};
#[cfg(test)]
use smallvec::SmallVec;
#[cfg(test)]
use thiserror::Error;

/// How often the primary consumer emits librdkafka statistics. Private on
/// purpose: no operational caller has asked for a tunable interval.
pub(in crate::consumer) const STATISTICS_INTERVAL: Duration = Duration::from_secs(5);

/// How long the startup metadata fetch may run before construction fails.
const STARTUP_METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// librdkafka's internal partition entry, which belongs to no real partition.
const INTERNAL_PARTITION: i32 = -1;

/// One whole observation generation. Stores rust-rdkafka's owned values by
/// move: the statistics callback already receives a decoded tree, and a second
/// hierarchy beside it would only go stale.
enum KafkaSnapshot {
    /// Broker metadata fetched through the primary consumer at startup. It
    /// predates partition assignment, so it reports no assigned partitions.
    InitialMetadata(Metadata),
    /// The newest librdkafka statistics report. It replaces startup metadata
    /// outright rather than merging with it. Boxed so both generations are the
    /// same size.
    ConsumerStatistics(Box<Statistics>),
}

/// A cheap-clone handle on one consumer's Kafka observation.
///
/// Clone it freely: every clone shares one observation, so the primary
/// consumer's statistics callback is visible to every reader.
#[derive(Clone)]
pub(crate) struct KafkaObserver {
    inner: Arc<KafkaObserverInner>,
}

/// The shared observation and its gauges.
///
/// Invariant: the snapshot may be absent only before `initialize_consumer`
/// completes its startup metadata fetch. No event handler is reachable during
/// that interval, so a running consumer always has an observation.
struct KafkaObserverInner {
    snapshot: ArcSwapOption<KafkaSnapshot>,
    metrics: KafkaMetrics,
    startup_timeout: Duration,
}

/// A coherent borrow of one observation generation. Every reference borrowed
/// through it comes from the same generation, however long the guard is held.
pub(crate) struct KafkaSnapshotGuard {
    snapshot: Arc<KafkaSnapshot>,
}

impl KafkaSnapshot {
    /// The statistics tree, or `None` for the startup metadata generation.
    fn statistics(&self) -> Option<&Statistics> {
        match self {
            Self::ConsumerStatistics(statistics) => Some(statistics),
            Self::InitialMetadata(_) => None,
        }
    }

    /// How many topics this generation observed. A cluster with no topics yet
    /// is a legal observation, so startup logs this count instead of rejecting
    /// an empty view.
    fn observed_topics(&self) -> usize {
        match self {
            Self::InitialMetadata(metadata) => metadata.topics().len(),
            Self::ConsumerStatistics(statistics) => statistics.topics.len(),
        }
    }

    /// The topic's partition count, or why this observation cannot supply one.
    ///
    /// Both generations answer through one rule: a topic has a usable count
    /// exactly when it reports a nonempty, error-free, contiguous set of real
    /// partition ids `0..count`.
    ///
    /// # Errors
    ///
    /// [`PartitionCountObservationError`] when the topic is absent from this
    /// observation, or present with an incomplete or non-contiguous topology.
    // Test-only until first-write publication consumes the observed count.
    #[cfg(test)]
    fn partition_count(
        &self,
        topic: &str,
    ) -> Result<PartitionCount, PartitionCountObservationError> {
        let mut ids: SmallVec<[i32; 16]> = SmallVec::new();
        match self {
            Self::InitialMetadata(metadata) => {
                let observed = metadata
                    .topics()
                    .iter()
                    .find(|entry| entry.name() == topic)
                    .ok_or_else(|| PartitionCountObservationError::unknown(topic))?;
                if observed.error().is_some() {
                    return Err(PartitionCountObservationError::incomplete(topic));
                }
                for partition in observed.partitions() {
                    if partition.error().is_some() {
                        return Err(PartitionCountObservationError::incomplete(topic));
                    }
                    ids.push(partition.id());
                }
            }
            Self::ConsumerStatistics(statistics) => {
                let observed = statistics
                    .topics
                    .get(topic)
                    .ok_or_else(|| PartitionCountObservationError::unknown(topic))?;
                for (&id, partition) in &observed.partitions {
                    if id == INTERNAL_PARTITION {
                        continue;
                    }
                    if partition.unknown {
                        return Err(PartitionCountObservationError::incomplete(topic));
                    }
                    ids.push(id);
                }
            }
        }
        contiguous_count(ids, topic)
    }
}

impl KafkaObserver {
    /// Creates the empty observer a consumer's construction threads everywhere.
    /// Gauges report under `group_id`, which never changes for this consumer.
    pub(in crate::consumer) fn new(group_id: &str) -> Self {
        Self::with_startup_timeout(group_id, STARTUP_METADATA_TIMEOUT)
    }

    /// [`Self::new`] with an explicit startup timeout. Tests use a short one so
    /// the real construction path's metadata-fetch failure is reachable in
    /// milliseconds.
    fn with_startup_timeout(group_id: &str, startup_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(KafkaObserverInner {
                snapshot: ArcSwapOption::empty(),
                metrics: KafkaMetrics::new(&meter("prosody"), group_id),
                startup_timeout,
            }),
        }
    }

    /// Fetches broker metadata through `consumer` and installs it as the first
    /// observation.
    ///
    /// Blocking, bounded by the observer's startup timeout. Runs once, after
    /// subscribe and before the poll loop starts, so a consumer never begins
    /// dispatching without an observation.
    ///
    /// # Errors
    ///
    /// [`KafkaError`] when the fetch fails or times out. Construction then
    /// fails rather than starting a blind consumer.
    pub(in crate::consumer) fn install_startup_metadata<Ctx: ConsumerContext>(
        &self,
        consumer: &BaseConsumer<Ctx>,
    ) -> Result<(), KafkaError> {
        let metadata = consumer.fetch_metadata(None, self.inner.startup_timeout)?;
        let snapshot = KafkaSnapshot::InitialMetadata(metadata);
        debug!(
            topics = snapshot.observed_topics(),
            "installed the consumer's initial Kafka observation"
        );
        self.inner.snapshot.store(Some(Arc::new(snapshot)));
        Ok(())
    }

    /// Records the gauges from `statistics`, then replaces the whole
    /// observation with it.
    ///
    /// Only two writers exist and they never overlap: the startup install runs
    /// before the poll loop starts, and this runs on the poll thread. So the
    /// load-record-store sequence needs no read-modify-write atomicity.
    /// Recording gauges is infallible, so nothing can leave the observation
    /// stale.
    pub(in crate::consumer) fn observe_statistics(&self, statistics: Statistics) {
        let previous = self.snapshot();
        self.inner.metrics.record(previous.as_ref(), &statistics);
        self.inner
            .snapshot
            .store(Some(Arc::new(KafkaSnapshot::ConsumerStatistics(Box::new(
                statistics,
            )))));
    }

    /// Borrows the current observation, or `None` before startup installs one.
    pub(crate) fn snapshot(&self) -> Option<KafkaSnapshotGuard> {
        self.inner
            .snapshot
            .load_full()
            .map(|snapshot| KafkaSnapshotGuard { snapshot })
    }

    /// The topic's partition count from the current observation.
    ///
    /// # Errors
    ///
    /// [`PartitionCountObservationError`] when no observation is installed yet,
    /// or the current one cannot supply a count for `topic`.
    // Test-only until first-write publication consumes the observed count.
    #[cfg(test)]
    pub(crate) fn partition_count(
        &self,
        topic: &str,
    ) -> Result<PartitionCount, PartitionCountObservationError> {
        self.snapshot()
            .ok_or(PartitionCountObservationError::NoSnapshot)?
            .snapshot
            .partition_count(topic)
    }

    /// Zeroes the gauge series of the last observed assignment.
    ///
    /// Called after the poll loop has exited, so no statistics callback can
    /// follow and re-record. Without it a stopped consumer would keep reporting
    /// its final fetch-queue values. `consumer::partition::metrics` zeroes its
    /// timer gauges before exit for the same reason.
    pub(in crate::consumer) fn shutdown(&self) {
        if let Some(guard) = self.snapshot() {
            self.inner.metrics.zero_assigned(&guard);
        }
    }
}

impl KafkaSnapshotGuard {
    /// The topic, id, and statistics of every partition assigned to this
    /// consumer instance. Yields nothing for the startup metadata generation,
    /// which predates assignment.
    pub(crate) fn assigned_partitions(&self) -> impl Iterator<Item = (&str, i32, &StatsPartition)> {
        self.snapshot
            .statistics()
            .into_iter()
            .flat_map(assigned_partitions)
    }

    /// The statistics of this generation, or `None` for startup metadata.
    fn statistics(&self) -> Option<&Statistics> {
        self.snapshot.statistics()
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
/// metadata still has client-side queues worth reporting. The partition-count
/// lookup rejects `unknown` separately, because a count must be complete.
fn is_assigned(id: i32, partition: &StatsPartition) -> bool {
    id != INTERNAL_PARTITION && partition.desired
}

/// Validates that `ids` is a nonempty contiguous range from zero and converts
/// its length into a [`PartitionCount`]. The one place both observation
/// generations agree on what a usable topology is.
// Test-only until first-write publication consumes the observed count.
#[cfg(test)]
fn contiguous_count(
    mut ids: SmallVec<[i32; 16]>,
    topic: &str,
) -> Result<PartitionCount, PartitionCountObservationError> {
    if ids.is_empty() {
        return Err(PartitionCountObservationError::incomplete(topic));
    }
    ids.sort_unstable();
    let count =
        i32::try_from(ids.len()).map_err(|_| PartitionCountObservationError::incomplete(topic))?;
    if !ids.iter().copied().eq(0_i32..count) {
        return Err(PartitionCountObservationError::incomplete(topic));
    }
    Ok(PartitionCount::try_from(count)?)
}

/// Why the current Kafka observation cannot supply a topic's partition count.
// Test-only until first-write publication consumes the observed count.
#[cfg(test)]
#[derive(Debug, Error)]
pub(crate) enum PartitionCountObservationError {
    /// No observation is installed yet, which can only happen before consumer
    /// startup completes.
    #[error("no Kafka observation is installed yet")]
    NoSnapshot,

    /// The topic is absent from the current observation.
    #[error("topic {0:?} is not in the current Kafka observation")]
    TopicUnknown(String),

    /// The topic is present but its topology is error-marked, incomplete, or
    /// not contiguous from zero.
    #[error("topic {0:?} has an incomplete or invalid observed topology")]
    TopicIncomplete(String),

    /// The observed topology yielded a count outside `[1, i32::MAX]`.
    #[error(transparent)]
    Count(#[from] PartitionCountError),
}

#[cfg(test)]
impl PartitionCountObservationError {
    fn unknown(topic: &str) -> Self {
        Self::TopicUnknown(topic.to_owned())
    }

    fn incomplete(topic: &str) -> Self {
        Self::TopicIncomplete(topic.to_owned())
    }
}

/// A missing or incomplete observation is transient: the next statistics report
/// may repair either. Only a structurally invalid count is permanent.
#[cfg(test)]
impl ClassifyError for PartitionCountObservationError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::NoSnapshot | Self::TopicUnknown(_) | Self::TopicIncomplete(_) => {
                ErrorCategory::Transient
            }
            Self::Count(error) => error.classify_error(),
        }
    }
}
