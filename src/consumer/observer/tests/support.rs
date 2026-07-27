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
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use quickcheck::{Arbitrary, Gen};
use rdkafka::Statistics;
use rdkafka::statistics::Partition as StatsPartition;
use rdkafka::statistics::Topic as StatsTopic;
use serde_json::Value;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

/// The topic every fixture that needs only one uses.
pub(super) const TOPIC: &str = "observed";
/// Fixture identity attributes. They are constant for a client's lifetime, so
/// one pair keeps every generated series comparable.
const CLIENT_NAME: &str = "rdkafka#consumer-1";
const CLIENT_ID: &str = "observer-tests";
pub(super) const GROUP: &str = "observer-tests-group";

pub(super) const FETCH_MESSAGES: &str = "prosody.kafka.consumer.fetch_queue.messages";
pub(super) const FETCH_BYTES: &str = "prosody.kafka.consumer.fetch_queue.size";
pub(super) const METADATA_AGE: &str = "prosody.kafka.consumer.metadata.age";

/// One partition entry in a fixture topology.
#[derive(Clone, Copy, Debug)]
pub(super) struct Entry {
    pub(super) id: i32,
    desired: bool,
    unknown: bool,
    fetch_messages: i64,
    fetch_bytes: u64,
}

/// A generated single-topic topology: contiguous ids from zero, then
/// independent mutations that introduce gaps, librdkafka's internal `-1` entry,
/// unknown partitions, and undesired ones.
///
/// No `shrink` override: a topology holds at most a handful of entries, so its
/// `Debug` output is already the whole reproducer.
#[derive(Clone, Debug)]
pub(super) struct Topology {
    pub(super) entries: Vec<Entry>,
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
        Self::assigned(-1, 0, 0)
    }

    pub(super) fn statistics(self) -> StatsPartition {
        StatsPartition {
            partition: self.id,
            desired: self.desired,
            unknown: self.unknown,
            fetchq_cnt: self.fetch_messages,
            fetchq_size: self.fetch_bytes,
            ..StatsPartition::default()
        }
    }
}

impl Arbitrary for Topology {
    fn arbitrary(g: &mut Gen) -> Self {
        let size = *g
            .choose(&[0_i32, 1_i32, 2_i32, 3_i32, 4_i32, 5_i32, 6_i32])
            .unwrap_or(&3_i32);
        let mut entries: Vec<Entry> = (0_i32..size)
            .map(|id| Entry {
                desired: bool::arbitrary(g),
                ..Entry::assigned(id, 0, 0)
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
                ..Entry::assigned(size + 2, 0, 0)
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

/// Builds a statistics tree from `(topic, metadata_age, entries)` triples.
pub(super) fn statistics_with(topics: &[(&str, i64, &[Entry])]) -> Statistics {
    Statistics {
        name: CLIENT_NAME.to_owned(),
        client_id: CLIENT_ID.to_owned(),
        topics: topics
            .iter()
            .map(|&(name, metadata_age, entries)| {
                (
                    name.to_owned(),
                    StatsTopic {
                        topic: name.to_owned(),
                        metadata_age,
                        partitions: entries
                            .iter()
                            .map(|entry| (entry.id, entry.statistics()))
                            .collect(),
                        ..StatsTopic::default()
                    },
                )
            })
            .collect(),
        ..Statistics::default()
    }
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

/// The last exported value of `name` on the data point carrying every attribute
/// in `wanted`, or `None` when no such series exists.
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
    (0..count).map(|id| Entry::assigned(id, 0, 0)).collect()
}

/// The assigned partition ids a guard yields, sorted.
pub(super) fn assigned_ids(guard: &KafkaSnapshotGuard) -> Vec<i32> {
    let mut ids: Vec<i32> = guard.assigned_partitions().map(|(_, id, _)| id).collect();
    ids.sort_unstable();
    ids
}

/// Runs the real `initialize_consumer` with `observer`, mirroring the memory
/// arm of the low-level constructor. The outer result is test setup; the inner
/// one is what construction returned.
pub(super) async fn initialize_with(
    config: &ConsumerConfiguration,
    observer: KafkaObserver,
) -> Result<Result<ProsodyConsumer<JsonCodec>, ConsumerError>> {
    let telemetry = Telemetry::new();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
    let stores = StorePair::new(
        &TriggerStoreConfiguration::InMemory,
        config.mock,
        Duration::from_secs(30),
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
    ))
}
