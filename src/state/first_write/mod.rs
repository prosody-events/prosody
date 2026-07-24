//! First-write publication of keyed-state routing rows.
//!
//! A `Published` collection's committed state is only discoverable by a
//! cross-group reader if a routing row exists in `keyed_state_publication`.
//! This module owns the write-path barrier that guarantees the row exists
//! **before** any durable state write of a published `(collection, topic)`
//! becomes committed, and the startup reconciliation that retires a group's
//! rows once it stops publishing a collection.
//!
//! The barrier ([`FirstWritePublisher::ensure_one`]) is gated on
//! [`StateVisibility::Published`](crate::state::registry::StateVisibility): a
//! private collection never consults the memo and never upserts, which is what
//! makes reconciliation's removal final for an un-publishing group.

use std::sync::Arc;
use std::time::Duration;

use quick_cache::sync::Cache;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use thiserror::Error;
use tokio::task::{JoinError, spawn_blocking};
use tracing::error;

use crate::Topic;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cassandra::{CassandraPublicationError, CassandraPublicationStore};
use crate::state::memory::MemoryPublicationStore;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
#[cfg(test)]
use crate::state::tests::support::{ScriptedPublicationError, ScriptedPublicationStore};
use crate::state::{StateName, StateType};
use crate::state_reader::{PartitionCount, PartitionCountError};
use crate::subsystem::SubsystemName;

#[cfg(test)]
mod tests;

/// Fixed capacity of the per-provider publication memo. Regex subscriptions
/// make the live `(collection, topic)` set open-ended, so the memo is a
/// capacity-bounded `quick_cache` — never an insert-only map. Eviction only
/// costs one extra idempotent re-run of the barrier for the evicted entry.
const PUBLICATION_MEMO_CAPACITY: usize = 4096;

/// How long the Kafka metadata fetch for a partition count may run.
const METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Backend variance for the write-path publication store — a closed enum, so
/// the concrete publisher type carries no store type parameter and introduces
/// no `dyn`. Delegation methods forward to the matched backend.
pub(crate) enum PublicationBackend {
    /// Cassandra-backed routing table (production).
    Cassandra(CassandraPublicationStore),
    /// In-memory routing table (mock mode).
    Memory(MemoryPublicationStore),
    /// Scripted store for tests: call log, injectable errors, upsert barrier.
    #[cfg(test)]
    Scripted(ScriptedPublicationStore),
}

impl PublicationBackend {
    /// Idempotently records `row` under `(subsystem, state_type, name)`.
    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), PublicationBackendError> {
        match self {
            Self::Cassandra(store) => store.upsert(subsystem, state_type, name, row).await?,
            // Memory's error is `Infallible`; the empty match discharges it
            // without a fallible unwrap and without a `?` (which would need an
            // unwritable `From<!>`).
            Self::Memory(store) => match store.upsert(subsystem, state_type, name, row).await {
                Ok(()) => {}
                Err(e) => match e {},
            },
            #[cfg(test)]
            Self::Scripted(store) => store.upsert(subsystem, state_type, name, row).await?,
        }
        Ok(())
    }

    /// Removes the `(group_id, topic)` source of `(subsystem, state_type,
    /// name)`.
    async fn remove(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
        topic: Topic,
    ) -> Result<(), PublicationBackendError> {
        match self {
            Self::Cassandra(store) => {
                store
                    .remove(subsystem, state_type, name, group_id, topic)
                    .await?;
            }
            Self::Memory(store) => {
                match store
                    .remove(subsystem, state_type, name, group_id, topic)
                    .await
                {
                    Ok(()) => {}
                    Err(e) => match e {},
                }
            }
            #[cfg(test)]
            Self::Scripted(store) => {
                store
                    .remove(subsystem, state_type, name, group_id, topic)
                    .await?;
            }
        }
        Ok(())
    }

    /// All published sources of `(subsystem, state_type, name)` — one
    /// partition read.
    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<Vec<StatePublication>, PublicationBackendError> {
        match self {
            Self::Cassandra(store) => {
                Ok(store.read_publications(subsystem, state_type, name).await?)
            }
            Self::Memory(store) => {
                match store.read_publications(subsystem, state_type, name).await {
                    Ok(rows) => Ok(rows),
                    Err(e) => match e {},
                }
            }
            #[cfg(test)]
            Self::Scripted(store) => {
                Ok(store.read_publications(subsystem, state_type, name).await?)
            }
        }
    }
}

/// The topic's live partition-count source — a closed enum, so the count fetch
/// carries no type parameter. The `Kafka` arm builds a throwaway
/// [`BaseConsumer`] per fetch under [`spawn_blocking`]; it only ever runs on a
/// cold memo entry (the first write per `(collection, topic)`), never on the
/// steady-state path.
///
/// A cheap-clone handle: the `Kafka` arm holds an `Arc<[String]>` and `Memory`
/// a `Copy` count, so cloning the
/// [`SharedDeps`](crate::state_reader::SharedDeps) bundle that carries one
/// clones no resources.
#[derive(Clone)]
pub(crate) enum PartitionCounts {
    /// Fetch the count from a Kafka broker.
    Kafka {
        /// Bootstrap servers of the cluster to query.
        bootstrap: Arc<[String]>,
    },
    /// The mock topology's fixed count (mock mode).
    Memory(PartitionCount),
}

impl PartitionCounts {
    /// The topic's live partition count.
    ///
    /// # Errors
    ///
    /// A broker/metadata failure or a topic not yet in metadata classifies
    /// `Transient` (retry); a live topic reporting a degenerate count
    /// classifies `Permanent`. Never `Terminal`.
    async fn live_count(&self, topic: &str) -> Result<PartitionCount, PartitionCountFetchError> {
        match self {
            Self::Memory(count) => Ok(*count),
            Self::Kafka { bootstrap } => {
                let servers = bootstrap.join(",");
                let topic_owned = topic.to_owned();
                let count = spawn_blocking(move || {
                    let consumer: BaseConsumer = ClientConfig::new()
                        .set("bootstrap.servers", servers)
                        .create()?;
                    let metadata =
                        consumer.fetch_metadata(Some(&topic_owned), METADATA_FETCH_TIMEOUT)?;
                    // Count the partitions of the requested topic. A topic
                    // absent from metadata (or reporting zero partitions) is
                    // treated as "not ready yet" — Transient.
                    let partitions = metadata
                        .topics()
                        .iter()
                        .find(|t| t.name() == topic_owned)
                        .filter(|t| t.error().is_none())
                        .map_or(0, |t| t.partitions().len());
                    Ok::<usize, KafkaError>(partitions)
                })
                .await??;
                let count = i32::try_from(count).unwrap_or(i32::MAX);
                if count == 0_i32 {
                    return Err(PartitionCountFetchError::TopicNotReady(topic.to_owned()));
                }
                Ok(PartitionCount::try_from(count)?)
            }
        }
    }
}

/// Memo key: provider-scoped, so `(group, subsystem)` are fixed and the topic
/// distinguishes sources of the same collection.
#[derive(Clone, PartialEq, Eq, Hash)]
struct PublicationMemoKey {
    state_type: StateType,
    name: StateName,
    topic: Topic,
}

/// Everything the barrier needs except the per-partition topic — shared across
/// all sessions a state-manager provider mints. Cloning shares the `Arc`s (the
/// memo included), so the memo dedups across every session of one provider.
#[derive(Clone)]
pub(crate) struct PublisherTemplate {
    subsystem: SubsystemName,
    group: Arc<str>,
    store: Arc<PublicationBackend>,
    counts: Arc<PartitionCounts>,
    memo: Arc<Cache<PublicationMemoKey, ()>>,
    registry: Arc<CollectionDefRegistry>,
}

impl PublisherTemplate {
    /// Builds the template with a fresh capacity-bounded memo.
    pub(crate) fn new(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<PublicationBackend>,
        counts: Arc<PartitionCounts>,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self::with_memo_capacity(
            subsystem,
            group,
            store,
            counts,
            registry,
            PUBLICATION_MEMO_CAPACITY,
        )
    }

    /// Builds the template with an explicit memo capacity — the eviction test
    /// drives a tiny memo to prove a re-run after eviction (the RAM-bound
    /// guard); production always uses [`PUBLICATION_MEMO_CAPACITY`] via
    /// [`Self::new`].
    pub(crate) fn with_memo_capacity(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<PublicationBackend>,
        counts: Arc<PartitionCounts>,
        registry: Arc<CollectionDefRegistry>,
        capacity: usize,
    ) -> Self {
        Self {
            subsystem,
            group,
            store,
            counts,
            memo: Arc::new(Cache::new(capacity)),
            registry,
        }
    }

    /// Binds the template to one topic, yielding the per-session publisher.
    pub(crate) fn bind(&self, topic: Topic) -> FirstWritePublisher {
        FirstWritePublisher {
            template: self.clone(),
            topic,
        }
    }
}

/// The per-session first-write publisher: a [`PublisherTemplate`] pinned to the
/// session's topic. Cloned into each session; cheap (all `Arc`s).
#[derive(Clone)]
pub(crate) struct FirstWritePublisher {
    template: PublisherTemplate,
    topic: Topic,
}

impl FirstWritePublisher {
    /// Runs the publication barrier for one collection, the precondition of
    /// every session-owned durable write of a `Published` collection.
    ///
    /// The invariant: **a routing row exists before the collection's committed
    /// state does.** Every durable cell-write channel MUST run this barrier
    /// before its first durable write of a published collection, or committed
    /// state could exist with no routing row to advertise it. Today those
    /// channels are the settle boundary's publication step
    /// (`publish_first_writes` in `settle.rs`) and the mid-handler
    /// `commit()` path (`session/mod.rs`); any new durable-write channel
    /// added for published state must call the barrier first too.
    ///
    /// For a `Published`, cold-memo `(collection, topic)` the sequence is:
    /// 1. fetch the topic's live partition count (must ride the caller's retry
    ///    posture);
    /// 2. **best-effort** own-row read for the `StableRouting` tripwire — a
    ///    read failure skips it and never gates (the blind upsert in step 3 is
    ///    the real overwrite, so gating on a decode that a single corrupt
    ///    sibling row can poison is both unnecessary and dangerous). A stored
    ///    count that differs from the live count is logged at `error!` and the
    ///    row is overwritten with the live count (see below);
    /// 3. blind idempotent upsert of `{group, topic, live_count}` — this is the
    ///    barrier;
    /// 4. latch the memo, **only after** the upsert is acknowledged.
    ///
    /// A private collection returns `Ok(())` immediately: the visibility gate
    /// is what makes reconciliation's removal final.
    ///
    /// **Partition counts are assumed fixed for the topic's lifetime.** A key's
    /// routing partition is derived from the count in its routing row, so a
    /// changed count reroutes every key: keys written under the previous count
    /// become unreachable to owner and readers alike (the owner's own routing
    /// moves with the count too). The step-2 tripwire *detects* this divergence
    /// and overwrites the row so readers stay consistent with the owner's
    /// post-expansion view — but it cannot *repair* the stranded keys.
    /// Partition expansion on topics backing keyed state is unsupported.
    ///
    /// # Errors
    ///
    /// Any of the count fetch or the upsert failing. The error classifies
    /// `Permanent`/`Transient` only — never `Terminal` — so the settle path
    /// retries it forever and `commit()` re-runs the handler.
    pub(crate) async fn ensure_one(
        &self,
        state_type: StateType,
        name: &StateName,
    ) -> Result<(), PublicationError> {
        let t = &self.template;
        if !t.registry.is_published(state_type, name) {
            return Ok(());
        }
        let key = PublicationMemoKey {
            state_type,
            name: name.clone(),
            topic: self.topic,
        };
        if t.memo.get(&key).is_some() {
            return Ok(());
        }
        let live = t.counts.live_count(self.topic.as_ref()).await?;
        // StableRouting tripwire: a decoded own row whose stored count differs
        // from the live count means the topic's partition count changed since
        // the row was written — unsupported, because keys route by that count
        // (see the invariant on this method). Overwrite with the live count so
        // readers stay consistent with the owner's post-expansion view (the
        // owner's own old-count keys are unreachable too), and log the
        // divergence loudly: the tripwire detects the misconfiguration but
        // cannot recover the stranded keys. A read error skips the diagnostic;
        // the blind upsert below still overwrites.
        if let Ok(rows) = t
            .store
            .read_publications(&t.subsystem, state_type, name)
            .await
            && let Some(stored) = rows
                .iter()
                .find(|r| r.group_id.as_ref() == t.group.as_ref() && r.topic == self.topic)
            && stored.partition_count != live
        {
            error!(
                collection = %name.as_str(),
                topic = %self.topic.as_ref(),
                stored = i32::from(stored.partition_count),
                live = i32::from(live),
                "keyed-state publication partition count changed for a topic backing keyed state: \
                 keys written under the previous partition count are no longer reachable by owner \
                 or readers; partition expansion on such topics is unsupported. Overwriting the \
                 routing row with the live count to keep readers consistent with the owner."
            );
        }
        let row = StatePublication {
            group_id: t.group.clone(),
            topic: self.topic,
            partition_count: live,
        };
        t.store.upsert(&t.subsystem, state_type, name, &row).await?;
        t.memo.insert(key, ());
        Ok(())
    }
}

/// Startup reconciliation: removes this group's own routing rows for every
/// **registered-but-private** collection, so a collection un-published
/// (`.published(false)`) since the last run retires its routing.
///
/// Only private names are swept. A name un-published via `.published(false)`
/// becomes [`StateVisibility::Private`], so [`is_published`] returns false and
/// the retired name is still swept; a name that remains `Published` keeps its
/// row across restart so a reader never loses discoverability of that
/// collection's still-committed state (sweeping it would delete the row until
/// the next durable write to that `(collection, topic)`, an unbounded window
/// for a quiescent published collection). Rows of *other* groups are left
/// untouched.
///
/// [`StateVisibility::Private`]: crate::state::registry::StateVisibility::Private
/// [`is_published`]: CollectionDefRegistry::is_published
///
/// Convergence rests on the zero-or-one-instance-per-partition invariant plus
/// running at every startup: with stop-then-start deploy ordering the
/// last-started instance reconciles after the final old-generation write. A
/// corrupt sibling row that will not decode is logged and skipped (it cannot be
/// cleaned), never a startup wedge.
///
/// # Errors
///
/// A transient read/remove failure propagates so the caller's build-time retry
/// re-runs; the operation is idempotent.
pub(crate) async fn reconcile_publications(
    store: &PublicationBackend,
    registry: &CollectionDefRegistry,
    subsystem: &SubsystemName,
    group: &str,
) -> Result<(), PublicationError> {
    // Own the registered set up front: streaming borrowed registry items into
    // the awaits below would keep the registry borrowed across `.await`.
    // Routing rows are addressed by `(subsystem, state_type, name)` — the same
    // `(state_type, name)` namespacing as the registry — so the private-name
    // sweep addresses each collection exactly.
    let collections: Vec<(StateType, StateName)> = registry
        .collections()
        .filter(|(state_type, name)| !registry.is_published(*state_type, name))
        .map(|(state_type, name)| (state_type, name.clone()))
        .collect();
    for (state_type, name) in &collections {
        let rows = match store.read_publications(subsystem, *state_type, name).await {
            Ok(rows) => rows,
            Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                error!(
                    collection = %name.as_str(),
                    "keyed-state publication reconciliation skipped a collection whose rows will \
                     not decode: {error:#}"
                );
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        for row in rows {
            if row.group_id.as_ref() == group {
                store
                    .remove(subsystem, *state_type, name, &row.group_id, row.topic)
                    .await?;
            }
        }
    }
    Ok(())
}

/// Error surfaced by the publication barrier and reconciliation.
///
/// Classifies `Permanent`/`Transient` only — never `Terminal` — matching the
/// settle-path posture and the client-layer no-Terminal ruling.
#[derive(Debug, Error)]
pub(crate) enum PublicationError {
    /// The publication store failed.
    #[error(transparent)]
    Store(#[from] PublicationBackendError),

    /// The topic's live partition count could not be fetched.
    #[error(transparent)]
    Count(#[from] PartitionCountFetchError),
}

impl ClassifyError for PublicationError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Store(e) => e.classify_error(),
            Self::Count(e) => e.classify_error(),
        }
    }
}

/// Backend publication-store error, folded over the closed backend enum.
#[derive(Debug, Error)]
pub(crate) enum PublicationBackendError {
    /// Cassandra store failure.
    #[error(transparent)]
    Cassandra(#[from] CassandraPublicationError),

    /// Scripted store failure (tests).
    #[cfg(test)]
    #[error(transparent)]
    Scripted(#[from] ScriptedPublicationError),
}

impl ClassifyError for PublicationBackendError {
    fn classify_error(&self) -> ErrorCategory {
        let category = match self {
            Self::Cassandra(e) => e.classify_error(),
            #[cfg(test)]
            Self::Scripted(e) => e.classify_error(),
        };
        // Fold a backend `Terminal` (a Cassandra driver-fatal) to `Transient`:
        // the publication path never emits `Terminal`, and the settle loop
        // retries a broken store forever either way.
        match category {
            ErrorCategory::Permanent => ErrorCategory::Permanent,
            ErrorCategory::Transient | ErrorCategory::Terminal => ErrorCategory::Transient,
        }
    }
}

/// Error fetching a topic's live partition count. Never `Terminal`.
#[derive(Debug, Error)]
pub(crate) enum PartitionCountFetchError {
    /// A Kafka broker/metadata error — retryable.
    #[error("kafka metadata fetch failed: {0:#}")]
    Kafka(#[from] KafkaError),

    /// The blocking metadata fetch task failed to join — retryable.
    #[error("partition-count fetch task failed: {0}")]
    Join(#[from] JoinError),

    /// The topic is not yet visible in cluster metadata — retryable (it may
    /// not be created yet).
    #[error("topic {0:?} not yet visible in cluster metadata")]
    TopicNotReady(String),

    /// A live topic reported a degenerate partition count — a data problem.
    #[error(transparent)]
    Count(#[from] PartitionCountError),
}

impl ClassifyError for PartitionCountFetchError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Kafka(_) | Self::Join(_) | Self::TopicNotReady(_) => ErrorCategory::Transient,
            Self::Count(e) => e.classify_error(),
        }
    }
}
