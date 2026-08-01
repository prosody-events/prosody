//! Statistics fixtures: a partition-topology generator, the trees librdkafka
//! would report for it, the identity attributes every generated series carries,
//! and the observers the publication tests read counts from.

use super::super::{KafkaObserver, KafkaSnapshot, KafkaSnapshotGuard};
use quickcheck::{Arbitrary, Gen};
use rdkafka::Statistics;
use rdkafka::statistics::Partition as StatsPartition;
use rdkafka::statistics::Topic as StatsTopic;
use std::collections::HashMap;
use std::sync::Arc;

/// The topic every fixture that needs only one uses.
pub(super) const TOPIC: &str = "observed";
/// The topics a generated report draws from. Two names, so a topic can be
/// present in one report and gone from the next.
const TOPIC_POOL: [&str; 2] = [TOPIC, "observed-other"];
/// Fixture identity attributes. They are constant for a client's lifetime, so
/// one pair keeps every generated series comparable.
const CLIENT_NAME: &str = "rdkafka#consumer-1";
const CLIENT_ID: &str = "observer-tests";
pub(super) const GROUP: &str = "observer-tests-group";
/// The metadata ages a generated report draws from, in milliseconds.
const METADATA_AGES: [i64; 4] = [0, 1, 37, 4_000];

/// librdkafka's internal partition entry, which belongs to no real partition.
const INTERNAL: i32 = -1;

/// Deliberately wrong values for the fields librdkafka duplicates inside its
/// statistics tree. The map key is canonical, so code that trusted a duplicate
/// fails every fixture instead of being caught by prose.
const UNTRUSTED_TOPIC: &str = "not-the-map-key";
const UNTRUSTED_PARTITION: i32 = i32::MIN;

/// The fetch-queue depth and size every assigned `(topic, id)` series must
/// report.
pub(super) type Assignment = HashMap<(&'static str, i32), (u64, u64)>;

/// One partition entry in a fixture topology.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    pub(super) id: i32,
    desired: bool,
    unknown: bool,
    fetch_messages: i64,
    fetch_bytes: u64,
}

/// A generated single-topic topology: contiguous ids from zero with randomized
/// `desired` flags and counters derived from each id, then independent
/// mutations that introduce gaps, librdkafka's internal `-1` entry, and unknown
/// partitions.
///
/// No `shrink` override: a topology holds at most a few dozen entries, so its
/// `Debug` output is already the whole reproducer.
#[derive(Clone, Debug)]
pub(super) struct Topology {
    pub(super) entries: Vec<Entry>,
}

/// A generated statistics report over [`TOPIC_POOL`]: each topic is present
/// three times in four, with its own metadata age and topology.
#[derive(Clone, Debug)]
pub(super) struct Report {
    topics: Vec<ReportTopic>,
}

/// One topic inside a generated [`Report`].
#[derive(Clone, Debug)]
struct ReportTopic {
    name: &'static str,
    metadata_age: i64,
    entries: Vec<Entry>,
}

impl Entry {
    /// A real partition this instance holds.
    pub(super) const fn assigned(id: i32, fetch_messages: i64, fetch_bytes: u64) -> Self {
        Self {
            id,
            desired: true,
            unknown: false,
            fetch_messages,
            fetch_bytes,
        }
    }

    /// librdkafka's internal entry, marked desired so only the `-1` filter can
    /// exclude it.
    pub(super) const fn internal() -> Self {
        Self::assigned(INTERNAL, 0, 0)
    }

    /// A partition whose counters are derived from its id, so a series paired
    /// with the wrong partition cannot match by accident.
    fn generated(id: i32) -> Self {
        let ordinal = u64::from(id.unsigned_abs());
        Self::assigned(id, i64::from(id.unsigned_abs()) * 3 + 1, ordinal * 7 + 2)
    }

    /// Whether this entry is a real partition this instance holds. The oracle
    /// half of the observer's assignment filter.
    fn is_assigned(self) -> bool {
        self.id != INTERNAL && self.desired
    }

    /// What the fetch-queue gauges must report for this entry.
    fn queued(self) -> (u64, u64) {
        (
            u64::try_from(self.fetch_messages).unwrap_or(0),
            self.fetch_bytes,
        )
    }

    pub(super) fn statistics(self) -> StatsPartition {
        StatsPartition {
            partition: UNTRUSTED_PARTITION,
            desired: self.desired,
            unknown: self.unknown,
            fetchq_cnt: self.fetch_messages,
            fetchq_size: self.fetch_bytes,
            ..StatsPartition::default()
        }
    }
}

impl Report {
    /// The statistics tree this report describes.
    pub(super) fn into_statistics(self) -> Statistics {
        statistics_of(self.topics.into_iter().map(|topic| {
            (
                topic.name.to_owned(),
                stats_topic(topic.metadata_age, partition_map(&topic.entries)),
            )
        }))
    }

    /// The fetch-queue values every assigned series must report, keyed by
    /// `(topic, id)`.
    pub(super) fn assigned(&self) -> Assignment {
        self.topics
            .iter()
            .flat_map(|topic| {
                topic
                    .entries
                    .iter()
                    .filter(|entry| entry.is_assigned())
                    .map(move |entry| ((topic.name, entry.id), entry.queued()))
            })
            .collect()
    }

    /// The metadata age the gauge must report: the oldest among topics holding
    /// at least one assigned partition.
    pub(super) fn metadata_age(&self) -> u64 {
        self.topics
            .iter()
            .filter(|topic| topic.entries.iter().any(|entry| entry.is_assigned()))
            .map(|topic| u64::try_from(topic.metadata_age).unwrap_or(0))
            .max()
            .unwrap_or(0)
    }
}

impl Arbitrary for Topology {
    fn arbitrary(g: &mut Gen) -> Self {
        let size = *g
            .choose(&[
                0_i32, 1_i32, 2_i32, 3_i32, 4_i32, 5_i32, 6_i32, 16_i32, 17_i32, 32_i32,
            ])
            .unwrap_or(&3_i32);
        let mut entries: Vec<Entry> = (0_i32..size)
            .map(|id| Entry {
                desired: bool::arbitrary(g),
                ..Entry::generated(id)
            })
            .collect();
        // Each mutation lands roughly one time in four, independently, so one
        // topology can carry several defects at once.
        if one_in_four(g) && !entries.is_empty() {
            let index = index_within(g, entries.len());
            entries.remove(index);
        }
        if one_in_four(g) {
            entries.push(Entry {
                desired: bool::arbitrary(g),
                ..Entry::generated(size + 2)
            });
        }
        if one_in_four(g) {
            entries.push(Entry {
                desired: bool::arbitrary(g),
                ..Entry::internal()
            });
        }
        if one_in_four(g) && !entries.is_empty() {
            let index = index_within(g, entries.len());
            if let Some(entry) = entries.get_mut(index) {
                entry.unknown = true;
            }
        }
        Self { entries }
    }
}

impl Arbitrary for Report {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut topics = Vec::with_capacity(TOPIC_POOL.len());
        for name in TOPIC_POOL {
            if one_in_four(g) {
                continue;
            }
            topics.push(ReportTopic {
                name,
                metadata_age: *g.choose(&METADATA_AGES).unwrap_or(&0),
                entries: Topology::arbitrary(g).entries,
            });
        }
        Self { topics }
    }
}

fn one_in_four(g: &mut Gen) -> bool {
    *g.choose(&[true, false, false, false]).unwrap_or(&false)
}

fn index_within(g: &mut Gen, len: usize) -> usize {
    usize::arbitrary(g) % len.max(1)
}

fn partition_map(entries: &[Entry]) -> HashMap<i32, StatsPartition> {
    entries
        .iter()
        .map(|entry| (entry.id, entry.statistics()))
        .collect()
}

/// One topic's statistics entry. Its embedded `topic` field is deliberately
/// wrong; see [`UNTRUSTED_TOPIC`].
fn stats_topic(metadata_age: i64, partitions: HashMap<i32, StatsPartition>) -> StatsTopic {
    StatsTopic {
        topic: UNTRUSTED_TOPIC.to_owned(),
        metadata_age,
        partitions,
        ..StatsTopic::default()
    }
}

/// Wraps topic trees in a report carrying the fixture identity attributes.
fn statistics_of(topics: impl IntoIterator<Item = (String, StatsTopic)>) -> Statistics {
    Statistics {
        name: CLIENT_NAME.to_owned(),
        client_id: CLIENT_ID.to_owned(),
        topics: topics.into_iter().collect(),
        ..Statistics::default()
    }
}

/// Builds a statistics tree from `(topic, metadata_age, entries)` triples.
pub(super) fn statistics_with(topics: &[(&str, i64, &[Entry])]) -> Statistics {
    statistics_of(topics.iter().map(|&(name, metadata_age, entries)| {
        (
            name.to_owned(),
            stats_topic(metadata_age, partition_map(entries)),
        )
    }))
}

/// A single-topic tree over a prebuilt partition map, so a property can feed
/// one generated map to both its oracle and the subject.
pub(super) fn statistics_of_partitions(partitions: HashMap<i32, StatsPartition>) -> Statistics {
    statistics_of([(TOPIC.to_owned(), stats_topic(0, partitions))])
}

/// Wraps a statistics tree in the guard readers hold.
pub(super) fn guard_of(statistics: Statistics) -> KafkaSnapshotGuard {
    KafkaSnapshotGuard {
        snapshot: Arc::new(KafkaSnapshot::ConsumerStatistics(Box::new(statistics))),
    }
}

/// The identity attributes every gauge carries, in the form the gauge readback
/// matches on.
pub(super) fn identity() -> [(&'static str, String); 4] {
    [
        ("messaging.system", "kafka".to_owned()),
        ("messaging.client.id", CLIENT_ID.to_owned()),
        ("messaging.consumer.group.name", GROUP.to_owned()),
        ("prosody.kafka.consumer.name", CLIENT_NAME.to_owned()),
    ]
}

/// A contiguous assigned topology of `count` partitions.
pub(super) fn contiguous(count: i32) -> Vec<Entry> {
    (0..count).map(Entry::generated).collect()
}

/// The assigned partition ids a guard yields, sorted.
pub(super) fn assigned_ids(guard: &KafkaSnapshotGuard) -> Vec<i32> {
    let mut ids: Vec<i32> = guard.assigned_partitions().map(|(_, id, _)| id).collect();
    ids.sort_unstable();
    ids
}

/// The assigned `(topic, id, fetch-queue depth)` triples a guard yields,
/// sorted. Pairing each id with its own depth is what proves the iterator does
/// not mix entries.
pub(super) fn assigned_depths(guard: &KafkaSnapshotGuard) -> Vec<(&str, i32, i64)> {
    let mut yielded: Vec<(&str, i32, i64)> = guard
        .assigned_partitions()
        .map(|(topic, id, partition)| (topic, id, partition.fetchq_cnt))
        .collect();
    yielded.sort_unstable();
    yielded
}

/// An observer with no observation installed — the pre-startup state.
pub(crate) fn unobserved(group: &str) -> KafkaObserver {
    KafkaObserver::new(group)
}

/// An observer reporting each `(topic, partition count)` as a contiguous
/// assigned topology. Counts must be positive: `contiguous(0)` yields a topic
/// with no partitions, which the count lookup rejects as an incomplete
/// topology rather than reporting zero.
pub(crate) fn observing(group: &str, topics: &[(&str, i32)]) -> KafkaObserver {
    let observer = unobserved(group);
    observe(&observer, topics);
    observer
}

/// Replaces `observer`'s observation, as the next statistics report would.
pub(crate) fn observe(observer: &KafkaObserver, topics: &[(&str, i32)]) {
    observer.observe_statistics(statistics_of(topics.iter().map(|&(name, count)| {
        (
            name.to_owned(),
            stats_topic(0, partition_map(&contiguous(count))),
        )
    })));
}
