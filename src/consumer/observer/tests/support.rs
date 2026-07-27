//! Fixtures and harnesses the observer tests share: a partition-topology
//! generator, statistics-tree builders, in-memory metric capture, and a real
//! `initialize_consumer` run.

use super::super::{KafkaObserver, KafkaSnapshot, KafkaSnapshotGuard};
use crate::JsonCodec;
use crate::consumer::ProsodyConsumer;
use crate::consumer::config::ConsumerConfiguration;
use crate::consumer::error::ConsumerError;
use crate::consumer::event_context::EventContext;
use crate::consumer::handler::{DemandType, EventHandler, Uncommitted};
use crate::consumer::message::UncommittedMessage;
use crate::consumer::middleware::CloneProvider;
use crate::consumer::middleware::deduplication::DEFAULT_IDEMPOTENCE_VERSION;
use crate::consumer::storage::StorePair;
use crate::consumer::wiring::runtime::{StartupServices, initialize_consumer};
use crate::consumer::wiring::state::{KeyedStateInputs, memory_state_provider};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::loader::MemoryLoader;
use crate::state::config::KeyedStateConfiguration;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore};
use crate::telemetry::Telemetry;
use crate::timers::UncommittedTimer;
use color_eyre::Result;
use color_eyre::eyre::{bail, eyre};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use quickcheck::{Arbitrary, Gen};
use rdkafka::Statistics;
use rdkafka::statistics::Partition as StatsPartition;
use rdkafka::statistics::Topic as StatsTopic;
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

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
/// The meter scope every test provider registers its instruments under.
const SCOPE: &str = "observer-tests";
/// The metadata ages a generated report draws from, in milliseconds.
const METADATA_AGES: [i64; 4] = [0, 1, 37, 4_000];

/// librdkafka's internal partition entry, which belongs to no real partition.
const INTERNAL: i32 = -1;

/// Deliberately wrong values for the fields librdkafka duplicates inside its
/// statistics tree. The map key is canonical, so code that trusted a duplicate
/// fails every fixture instead of being caught by prose.
const UNTRUSTED_TOPIC: &str = "not-the-map-key";
const UNTRUSTED_PARTITION: i32 = i32::MIN;

pub(super) const FETCH_MESSAGES: &str = "prosody.kafka.consumer.fetch_queue.messages";
pub(super) const FETCH_BYTES: &str = "prosody.kafka.consumer.fetch_queue.size";
pub(super) const METADATA_AGE: &str = "prosody.kafka.consumer.metadata.age";

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

/// A no-op handler. The startup tests build a real consumer but deliver nothing
/// to it.
#[derive(Clone)]
pub(super) struct SilentHandler;

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

    /// A real partition the last rebalance took away. librdkafka keeps the
    /// entry and clears `desired`.
    pub(super) const fn revoked(id: i32) -> Self {
        Self {
            desired: false,
            ..Self::assigned(id, 0, 0)
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

impl EventHandler for SilentHandler {
    type Payload = Value;

    async fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<Value>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        let (_, uncommitted) = message.into_inner();
        uncommitted.commit().await;
    }

    async fn on_timer<C, T>(&self, _context: C, timer: T, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        T: UncommittedTimer,
    {
        timer.commit().await;
    }

    async fn shutdown(self) {}
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

/// A meter provider that exports into memory. Never installed globally, so
/// parallel tests stay isolated.
pub(super) fn test_meter() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    (provider, exporter)
}

/// An observer recording into `provider`'s meter, so its exported series are
/// readable back. Tests pass a short `startup_timeout` where a failing metadata
/// fetch is the subject.
pub(super) fn observer_with(
    group_id: &str,
    startup_timeout: Duration,
    provider: &SdkMeterProvider,
) -> KafkaObserver {
    KafkaObserver::with_instrumentation(group_id, startup_timeout, &provider.meter(SCOPE))
}

/// The identity attributes every gauge carries, in the form [`gauge_value`]
/// matches on.
pub(super) fn identity() -> [(&'static str, String); 4] {
    [
        ("messaging.system", "kafka".to_owned()),
        ("messaging.client.id", CLIENT_ID.to_owned()),
        ("messaging.consumer.group.name", GROUP.to_owned()),
        ("prosody.kafka.consumer.name", CLIENT_NAME.to_owned()),
    ]
}

/// The last exported value of `name` on the data point carrying every attribute
/// in `wanted`, or `None` when no such series exists.
///
/// A second matching data point is a test bug: the assertion would then depend
/// on export order, so this fails instead.
pub(super) fn gauge_value(
    exporter: &InMemoryMetricExporter,
    name: &str,
    wanted: &[(&str, String)],
) -> Result<Option<u64>> {
    let batches = exporter.get_finished_metrics()?;
    let last = batches
        .last()
        .ok_or_else(|| eyre!("no metric batch was exported"))?;
    let mut found = None;
    for scope in last.scope_metrics() {
        for metric in scope.metrics().filter(|metric| metric.name() == name) {
            let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                bail!("{name} was not exported as a u64 gauge");
            };
            for point in gauge.data_points() {
                let matched = wanted.iter().all(|(key, value)| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == *key && kv.value.to_string() == *value)
                });
                if matched {
                    if found.is_some() {
                        bail!("{name} exported more than one data point matching {wanted:?}");
                    }
                    found = Some(point.value());
                }
            }
        }
    }
    Ok(found)
}

/// The value of a per-partition gauge for one `(topic, id)` series.
pub(super) fn partition_gauge(
    exporter: &InMemoryMetricExporter,
    name: &str,
    topic: &str,
    id: i32,
) -> Result<Option<u64>> {
    gauge_value(
        exporter,
        name,
        &[
            ("messaging.destination.name", topic.to_owned()),
            ("messaging.destination.partition.id", id.to_string()),
        ],
    )
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

/// Runs the real `initialize_consumer` with `observer`, mirroring the memory
/// arm of the direct mode. The outer result is test setup; the inner one is
/// what construction returned.
pub(super) async fn initialize_with(
    config: &ConsumerConfiguration,
    observer: KafkaObserver,
) -> Result<Result<ProsodyConsumer<JsonCodec>, ConsumerError>> {
    let telemetry = Telemetry::new();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let stores = StorePair::new(
        &TriggerStoreConfiguration::InMemory,
        config.mock,
        Duration::default(),
        NonZeroUsize::MIN,
        config.timer_spans,
        None,
    )
    .await?;
    let StorePair::Memory {
        trigger_provider,
        dedup_provider,
        ..
    } = stores
    else {
        bail!("the in-memory trigger store configuration must yield the memory arm");
    };
    let keyed_state = KeyedStateInputs::new(
        KeyedStateConfiguration::builder().build()?,
        config,
        DEFAULT_IDEMPOTENCE_VERSION,
    )?;
    let state_provider = memory_state_provider::<JsonCodec>(
        &keyed_state,
        dedup_provider,
        MemoryCells::new(),
        MemoryDescriptorIdentityStore::new(),
        MemoryLoader::<Value>::new(),
        None,
    );
    Ok(initialize_consumer::<_, _, _, JsonCodec>(
        config,
        CloneProvider::new(SilentHandler),
        trigger_provider,
        state_provider,
        StartupServices {
            version: keyed_state.version.clone(),
            telemetry: &telemetry,
            heartbeats,
            observer,
        },
    )
    .await)
}
