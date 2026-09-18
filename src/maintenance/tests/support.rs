//! The backend-generic runner and the Cassandra fixture.
//!
//! One runner drives a [`CatalogTrace`] through a backend's production stores
//! and reads that backend's catalog, so both backends answer the same trace.

use super::trace::{
    CatalogTrace, Effect, Model, Snapshot, key_name, timer_segment, timer_segment_row, topic,
};
use crate::consumer::middleware::defer::memory_providers;
use crate::consumer::middleware::defer::message::store::cassandra::MessageQueries;
use crate::consumer::middleware::defer::message::store::{
    CassandraMessageDeferStoreProvider, MessageDeferStore, MessageDeferStoreProvider,
};
use crate::consumer::middleware::defer::segment::CassandraSegmentStore;
use crate::consumer::middleware::defer::timer::store::cassandra::queries::Queries as TimerQueries;
use crate::consumer::middleware::defer::timer::store::{
    CassandraTimerDeferStoreProvider, TimerDeferStore, TimerDeferStoreProvider,
};
use crate::maintenance::{CassandraCatalog, Catalog, GroupId, MemoryCatalog};
use crate::otel::SpanRelation;
use crate::test_util::{TEST_KEYSPACE, shared_cassandra_store};
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::CassandraTriggerStoreProvider;
use crate::timers::store::memory::InMemoryTriggerStoreProvider;
use crate::timers::store::{TriggerStore, TriggerStoreProvider};
use crate::timers::{TimerType, Trigger};
use crate::{Key, Offset, Partition};
use color_eyre::Result;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut, stream};
use quickcheck::TestResult;
use std::collections::BTreeSet;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::task::coop::cooperative;
use tracing::Span;

/// Write-through cache size for the Cassandra defer stores under test.
const CACHE: usize = 64;

/// Seed writes in flight at once. The seeded keys are independent, so they are
/// written concurrently under this bound.
const SEED_FANOUT: usize = 16;

/// The Cassandra providers and catalog, prepared once for the test process.
static CASSANDRA: OnceCell<CassandraFixture> = OnceCell::const_new();

/// The Cassandra providers and catalog a parity run drives.
pub(super) struct CassandraFixture {
    triggers: CassandraTriggerStoreProvider,
    messages: CassandraMessageDeferStoreProvider,
    timers: CassandraTimerDeferStoreProvider,
    catalog: CassandraCatalog,
}

impl CassandraFixture {
    async fn build() -> Result<Self> {
        let store = shared_cassandra_store().await?.clone();
        let keyspace = TEST_KEYSPACE;
        let segments = CassandraSegmentStore::new(store.clone(), keyspace).await?;

        Ok(Self {
            triggers: CassandraTriggerStoreProvider::with_store(store.clone(), keyspace).await?,
            messages: CassandraMessageDeferStoreProvider::new(
                store.clone(),
                Arc::new(MessageQueries::new(store.session(), keyspace).await?),
                segments.clone(),
            ),
            timers: CassandraTimerDeferStoreProvider::new(
                store.clone(),
                Arc::new(TimerQueries::new(store.session(), keyspace).await?),
                segments,
                SpanRelation::default(),
            ),
            catalog: CassandraCatalog::new(store).await?,
        })
    }

    /// Drives `trace` through the Cassandra production stores and reads the
    /// Cassandra catalog.
    pub(super) async fn run(&self, trace: &CatalogTrace, group: &GroupId) -> Result<Snapshot> {
        run_trace(
            trace,
            group,
            &self.triggers,
            &self.messages,
            &self.timers,
            &self.catalog,
        )
        .await
    }

    /// The prepared catalog, for the reads that need no trace.
    pub(super) fn catalog(&self) -> &CassandraCatalog {
        &self.catalog
    }

    /// Defers one message and one timer for each of `count` distinct keys in
    /// partition 0 of `group`. Returns the key names.
    pub(super) async fn seed_keys(
        &self,
        group: &GroupId,
        count: usize,
    ) -> Result<BTreeSet<String>> {
        let messages = self
            .messages
            .create_store(topic(), 0, group.as_str(), CACHE);
        let timers = self.timers.create_store(topic(), 0, group.as_str(), CACHE);
        let (messages, timers) = (&messages, &timers);

        stream::iter(0..count)
            .map(|index| {
                cooperative(async move {
                    let key = Key::from(key_name(index));
                    let offset = Offset::from(index as i64);
                    messages.defer_first_message(&key, offset).await?;

                    let time = CompactDateTime::from(index as u32);
                    let trigger =
                        Trigger::new(key.clone(), time, TimerType::Application, Span::none());
                    timers.defer_first_timer(&trigger).await?;

                    Ok::<_, color_eyre::Report>(key.to_string())
                })
            })
            .buffer_unordered(SEED_FANOUT)
            .try_collect()
            .await
    }
}

/// The Cassandra fixture, prepared once for the test process.
pub(super) async fn cassandra_fixture() -> Result<&'static CassandraFixture> {
    CASSANDRA.get_or_try_init(CassandraFixture::build).await
}

/// Drives `trace` through memory mode's production stores, then reads the
/// memory catalog.
///
/// The providers come from the production wiring, so the catalog reads the one
/// registry both providers write.
pub(super) async fn run_memory(trace: &CatalogTrace, group: &GroupId) -> Result<Snapshot> {
    let (segments, messages, timers) = memory_providers(SpanRelation::default());
    let triggers = InMemoryTriggerStoreProvider::new();
    let catalog = MemoryCatalog::new(segments, messages.clone(), timers.clone(), triggers.clone());

    run_trace(trace, group, &triggers, &messages, &timers, &catalog).await
}

/// Drives `trace` through one backend's production stores, then reads that
/// backend's catalog.
pub(super) async fn run_trace<T, M, R, C>(
    trace: &CatalogTrace,
    group: &GroupId,
    triggers: &T,
    messages: &M,
    timers: &R,
    catalog: &C,
) -> Result<Snapshot>
where
    T: TriggerStoreProvider,
    M: MessageDeferStoreProvider,
    R: TimerDeferStoreProvider,
    C: Catalog,
{
    // Every partition but the last gets a timer segment row, so an absent row
    // is exercised on both backends.
    for partition in 0..trace.partitions.saturating_sub(1) {
        triggers
            .create_store(timer_segment(group, partition))
            .insert_segment()
            .await?;
    }

    let keys: Vec<Key> = (0..trace.keys)
        .map(|index| Key::from(key_name(index)))
        .collect();
    let message_stores: Vec<M::Store> = (0..trace.partitions)
        .map(|partition| {
            messages.create_store(topic(), partition as Partition, group.as_str(), CACHE)
        })
        .collect();
    let timer_stores: Vec<R::Store> = (0..trace.partitions)
        .map(|partition| {
            timers.create_store(topic(), partition as Partition, group.as_str(), CACHE)
        })
        .collect();

    let mut model = Model::default();
    for &op in &trace.ops {
        let Some(effect) = model.apply(op) else {
            continue;
        };
        apply_effect(effect, &keys, &message_stores, &timer_stores).await?;
    }

    read_snapshot(catalog, group).await
}

/// Runs one resolved store call against the backend under test.
async fn apply_effect<M, R>(
    effect: Effect,
    keys: &[Key],
    message_stores: &[M],
    timer_stores: &[R],
) -> Result<()>
where
    M: MessageDeferStore,
    R: TimerDeferStore,
{
    let trigger =
        |key: &Key, time| Trigger::new(key.clone(), time, TimerType::Application, Span::none());

    match effect {
        Effect::DeferFirstMessage {
            partition,
            key,
            offset,
        } => {
            message_stores[partition]
                .defer_first_message(&keys[key], offset)
                .await?;
        }
        Effect::DeferAdditionalMessage {
            partition,
            key,
            offset,
        } => {
            message_stores[partition]
                .defer_additional_message(&keys[key], offset)
                .await?;
        }
        Effect::CompleteMessage {
            partition,
            key,
            offset,
        } => {
            message_stores[partition]
                .complete_retry_success(&keys[key], offset)
                .await?;
        }
        Effect::IncrementMessageRetry { partition, key } => {
            message_stores[partition]
                .increment_retry_count(&keys[key], 0)
                .await?;
        }
        Effect::DeferFirstTimer {
            partition,
            key,
            time,
        } => {
            timer_stores[partition]
                .defer_first_timer(&trigger(&keys[key], time))
                .await?;
        }
        Effect::DeferAdditionalTimer {
            partition,
            key,
            time,
        } => {
            timer_stores[partition]
                .defer_additional_timer(&trigger(&keys[key], time))
                .await?;
        }
        Effect::CompleteTimer {
            partition,
            key,
            time,
        } => {
            timer_stores[partition]
                .complete_retry_success(&keys[key], time)
                .await?;
        }
        Effect::IncrementTimerRetry { partition, key } => {
            timer_stores[partition]
                .increment_retry_count(&keys[key], 0)
                .await?;
        }
    }

    Ok(())
}

/// Reads every fact one catalog reports for `group`.
async fn read_snapshot<C: Catalog>(catalog: &C, group: &GroupId) -> Result<Snapshot> {
    let mut found = Vec::new();
    {
        let segments = catalog.segments();
        pin_mut!(segments);
        while let Some(segment) = segments.try_next().await? {
            if segment.group == *group {
                found.push(segment);
            }
        }
    }

    let mut out = Snapshot::default();
    for segment in found {
        let label = segment.to_string();
        out.segments.insert(label.clone());
        out.message_keys.insert(
            label.clone(),
            collect_keys(catalog.message_keys(segment.defer_id())).await?,
        );
        out.timer_keys.insert(
            label.clone(),
            collect_keys(catalog.timer_keys(segment.defer_id())).await?,
        );
        out.timer_segments.insert(
            label,
            catalog
                .timer_segment(segment.timer_id())
                .await?
                .map(timer_segment_row),
        );
    }

    Ok(out)
}

/// Drains one key scan into a set.
pub(super) async fn collect_keys<S, E>(stream: S) -> Result<BTreeSet<String>>
where
    S: Stream<Item = Result<Key, E>>,
    E: Error + Send + Sync + 'static,
{
    pin_mut!(stream);
    let mut out = BTreeSet::new();
    while let Some(key) = stream.try_next().await? {
        out.insert(key.to_string());
    }
    Ok(out)
}

/// Converts a property body's result into a test outcome. A store failure is
/// reported as a broken environment, with the error text attached.
pub(super) fn finish(result: Result<()>) -> TestResult {
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}
