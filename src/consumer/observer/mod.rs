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
pub(crate) mod tests;

use crate::consumer::observer::metrics::KafkaMetrics;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state_reader::{PartitionCount, PartitionCountError};
use arc_swap::ArcSwapOption;
use opentelemetry::global::meter;
use opentelemetry::metrics::Meter;
use rdkafka::Statistics;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use rdkafka::statistics::Partition as StatsPartition;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::debug;

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
    /// outright rather than merging with it. It covers only topics this client
    /// holds, so once the first report lands the observation can no longer
    /// answer for a topic the consumer never consumed. Boxed because
    /// `Statistics` is hundreds of bytes: unboxed it would trip
    /// `clippy::large_enum_variant`.
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
/// Invariant: a running consumer always has an observation. The snapshot is
/// absent only before `initialize_consumer` installs the startup metadata, and
/// after a construction that failed discards it. No event handler is reachable
/// in either case.
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

    /// How many topics this generation observed. Startup logs it so a bare or
    /// empty first observation is visible in a trace.
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
        Self::with_instrumentation(group_id, STARTUP_METADATA_TIMEOUT, &meter("prosody"))
    }

    /// [`Self::new`] with an explicit startup timeout and meter. Tests use a
    /// short timeout so the real construction path's metadata-fetch failure is
    /// reachable in milliseconds, and their own meter so exported series are
    /// readable back.
    fn with_instrumentation(group_id: &str, startup_timeout: Duration, meter: &Meter) -> Self {
        Self {
            inner: Arc::new(KafkaObserverInner {
                snapshot: ArcSwapOption::empty(),
                metrics: KafkaMetrics::new(meter, group_id),
                startup_timeout,
            }),
        }
    }

    /// Fetches broker metadata through `consumer` and installs it as the first
    /// observation.
    ///
    /// Blocking, bounded by the observer's startup timeout. The caller runs it
    /// on a blocking thread. It runs once, after subscribe and before the poll
    /// loop starts, so a consumer never begins dispatching without an
    /// observation.
    ///
    /// The fetch asks for all topics: `fetch_metadata` accepts at most one
    /// topic and a consumer may subscribe to several.
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
        // A cluster with no topics yet is a legal observation, so this logs the
        // count instead of rejecting an empty view.
        debug!(
            topics = snapshot.observed_topics(),
            "installed the consumer's initial Kafka observation"
        );
        self.inner.snapshot.store(Some(Arc::new(snapshot)));
        Ok(())
    }

    /// Discards the observation, leaving the observer as constructed.
    ///
    /// Construction calls this when it fails, once the primary consumer has
    /// been dropped. That drop polls the client's queue one last time and can
    /// dispatch a statistics report into this observer, so clearing any earlier
    /// would not stick. A failed construction must leave nothing installed: a
    /// clone that outlives it would otherwise read a dead client's view.
    pub(in crate::consumer) fn clear(&self) {
        self.inner.snapshot.store(None);
    }

    /// Records the gauges from `statistics`, then replaces the whole
    /// observation with it.
    ///
    /// Three writers exist and none overlap: the startup install runs before
    /// the poll loop starts, this runs on the poll thread, and [`Self::clear`]
    /// runs only after the primary consumer has been dropped. So the
    /// load-record-store sequence needs no read-modify-write atomicity.
    /// Recording gauges is infallible, so nothing can leave the observation
    /// stale.
    ///
    /// A report librdkafka queued while the startup fetch was still running
    /// predates that fetch. Installing it moves the observation back to the
    /// older view, for at most one statistics interval after startup. A
    /// published durable write whose topic that older view lacks blocks until
    /// the next report.
    pub(in crate::consumer) fn observe_statistics(&self, statistics: Statistics) {
        let previous = self.snapshot();
        self.inner.metrics.record(
            previous
                .as_ref()
                .and_then(|guard| guard.snapshot.statistics()),
            &statistics,
        );
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
    pub(in crate::consumer) fn retire_gauges(&self) {
        let Some(guard) = self.snapshot() else {
            return;
        };
        if let Some(statistics) = guard.snapshot.statistics() {
            self.inner.metrics.zero_assigned(statistics);
        }
    }
}

impl KafkaSnapshotGuard {
    /// The topic, id, and statistics of every partition assigned to this
    /// consumer instance. Yields nothing for the startup metadata generation,
    /// which predates assignment.
    ///
    /// Test-only, and not dead: only a guard-based read can show that a guard
    /// taken before a replacement keeps answering from its own generation.
    /// Production borrows through the free [`assigned_partitions`] instead.
    #[cfg(test)]
    pub(crate) fn assigned_partitions(&self) -> impl Iterator<Item = (&str, i32, &StatsPartition)> {
        self.snapshot
            .statistics()
            .into_iter()
            .flat_map(assigned_partitions)
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
    ///
    /// Unreachable while [`contiguous_count`] rejects an empty id set first.
    /// The variant is kept so a count `PartitionCount` rejects stays permanent
    /// rather than being retried forever.
    #[error(transparent)]
    Count(#[from] PartitionCountError),
}

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
