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

use futures::{StreamExt, TryStreamExt, stream};
use quick_cache::sync::Cache;
use thiserror::Error;
use tokio::task::coop::cooperative;
use tracing::{error, warn};

use crate::Topic;
use crate::consumer::observer::{KafkaObserver, PartitionCountObservationError};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::cassandra::{CassandraPublicationError, CassandraPublicationStore};
use crate::state::memory::MemoryPublicationStore;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::CollectionDefRegistry;
#[cfg(test)]
use crate::state::tests::support::{ScriptedPublicationError, ScriptedPublicationStore};
use crate::state::{STATE_FANOUT_CONCURRENCY, StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;

#[cfg(test)]
mod tests;

/// Fixed capacity of the per-provider publication memo. Regex subscriptions
/// make the live `(collection, topic)` set open-ended, so the memo is a
/// capacity-bounded `quick_cache` — never an insert-only map. Eviction only
/// costs one extra idempotent re-run of the barrier for the evicted entry.
const PUBLICATION_MEMO_CAPACITY: usize = 4096;

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

    /// Removes every source of `(subsystem, state_type, name)` published by
    /// `group_id`.
    async fn remove_group(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> Result<(), PublicationBackendError> {
        match self {
            Self::Cassandra(store) => {
                store
                    .remove_group(subsystem, state_type, name, group_id)
                    .await?;
            }
            Self::Memory(store) => {
                match store
                    .remove_group(subsystem, state_type, name, group_id)
                    .await
                {
                    Ok(()) => {}
                    Err(e) => match e {},
                }
            }
            #[cfg(test)]
            Self::Scripted(store) => {
                store
                    .remove_group(subsystem, state_type, name, group_id)
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

/// Where the publication barrier reads a topic's partition count. A closed
/// enum, so the lookup carries no type parameter and no `dyn`.
///
/// Cloning is cheap: the observer is a shared handle and the fixed count is
/// `Copy`.
#[derive(Clone)]
pub(crate) enum PartitionCounts {
    /// The primary consumer's own Kafka observation. No extra client and no
    /// broker round trip — the count comes from the snapshot that consumer
    /// already keeps.
    Observed(KafkaObserver),
    /// The mock topology's fixed count (mock mode).
    Fixed(PartitionCount),
}

impl PartitionCounts {
    /// The topic's current partition count.
    ///
    /// # Errors
    ///
    /// [`PartitionCountObservationError`] when the observation cannot supply
    /// one. An absent or incomplete topic is `Transient`, so the caller retries
    /// until a later statistics report repairs it; only a structurally invalid
    /// count is `Permanent`. Never `Terminal`.
    fn live_count(&self, topic: &str) -> Result<PartitionCount, PartitionCountObservationError> {
        match self {
            Self::Fixed(count) => Ok(*count),
            Self::Observed(observer) => observer.partition_count(topic),
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
/// all sessions a state-manager provider opens. Cloning shares the `Arc`s (the
/// memo included), so the memo dedups across every session of one provider.
#[derive(Clone)]
pub(crate) struct PublisherTemplate {
    subsystem: SubsystemName,
    group: Arc<str>,
    store: Arc<PublicationBackend>,
    counts: PartitionCounts,
    memo: Arc<Cache<PublicationMemoKey, ()>>,
    registry: Arc<CollectionDefRegistry>,
}

impl PublisherTemplate {
    /// Builds the template with a fresh capacity-bounded memo.
    pub(crate) fn new(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<PublicationBackend>,
        counts: PartitionCounts,
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

    /// Builds the template with an explicit memo capacity. The eviction test
    /// uses a tiny memo to prove the barrier re-runs after an entry is evicted.
    /// Production always uses [`PUBLICATION_MEMO_CAPACITY`] via [`Self::new`].
    pub(crate) fn with_memo_capacity(
        subsystem: SubsystemName,
        group: Arc<str>,
        store: Arc<PublicationBackend>,
        counts: PartitionCounts,
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

/// The per-session first-write publisher: a [`PublisherTemplate`] bound to the
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
    /// `commit()` path in `session/mod.rs`. Any new durable-write channel
    /// added for published state must call the barrier first too.
    ///
    /// For a `Published`, cold-memo `(collection, topic)` the sequence is:
    /// 1. read the topic's partition count from the consumer's Kafka
    ///    observation (must ride the caller's retry posture);
    /// 2. **best-effort** own-row read to detect a changed partition count. A
    ///    read failure is logged at `warn!` and skips the check; it never
    ///    blocks the upsert, since step 3 overwrites the row regardless. A
    ///    stored count that differs from the live count is logged at `error!`
    ///    and the row is overwritten with the live count (see below);
    /// 3. blind idempotent upsert of `{group, topic, live_count}` — this is the
    ///    barrier;
    /// 4. latch the memo, **only after** the upsert is acknowledged.
    ///
    /// A private collection returns `Ok(())` immediately: the visibility gate
    /// is what makes reconciliation's removal final.
    ///
    /// **Partition counts are assumed fixed for the topic's lifetime.** A key's
    /// routing partition is derived from the count in its routing row. A
    /// changed count therefore reroutes every key, so keys written under
    /// the previous count become unreachable to both the owner and its
    /// readers. Step 2 detects this divergence and overwrites the row,
    /// keeping readers consistent with the owner's new view. It cannot
    /// recover the stranded keys. Partition expansion on topics backing
    /// keyed state is unsupported.
    ///
    /// # Errors
    ///
    /// An unavailable partition count or a failing upsert. The error classifies
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
        let live = t.counts.live_count(self.topic.as_ref())?;
        // Detect a changed partition count. A stored own-row count that differs
        // from the live count means the topic was repartitioned, which is
        // unsupported (see this method's doc). Log it as an error. The blind
        // upsert below overwrites the row with the live count either way, so a
        // read failure only costs the check.
        match t
            .store
            .read_publications(&t.subsystem, state_type, name)
            .await
        {
            Ok(rows) => {
                if let Some(stored) = rows
                    .iter()
                    .find(|r| r.group_id.as_ref() == t.group.as_ref() && r.topic == self.topic)
                    && stored.partition_count != live
                {
                    error!(
                        collection = %name.as_str(),
                        topic = %self.topic.as_ref(),
                        stored = i32::from(stored.partition_count),
                        live = i32::from(live),
                        "keyed-state publication partition count changed for a topic backing \
                         keyed state: keys written under the previous partition count are no \
                         longer reachable by owner or readers; partition expansion on such topics \
                         is unsupported. Overwriting the routing row with the live count to keep \
                         readers consistent with the owner."
                    );
                }
            }
            Err(error) => warn!(
                collection = %name.as_str(),
                topic = %self.topic.as_ref(),
                error = %error,
                "reading the routing row before the publication upsert failed; skipping the \
                 repartition check. The upsert proceeds with the live partition count."
            ),
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
/// the retired name is swept. A name that remains `Published` keeps its row
/// across restart, so a reader never loses discoverability of that collection's
/// still-committed state. Sweeping a published name would delete its row until
/// the next durable write to that `(collection, topic)`, an unbounded window
/// for a quiescent published collection. Rows of *other* groups are left
/// untouched.
///
/// [`StateVisibility::Private`]: crate::state::registry::StateVisibility::Private
/// [`is_published`]: CollectionDefRegistry::is_published
///
/// Convergence rests on the zero-or-one-instance-per-partition invariant plus
/// running at every startup: with stop-then-start deploy ordering the
/// last-started instance reconciles after the final old-generation write.
///
/// Each private name costs exactly one clustering-prefix removal of this
/// group's slice — no read, so a corrupt sibling row can never block the
/// sweep. The names are independent partitions, so the removals fan out.
///
/// # Errors
///
/// A transient removal failure propagates so the caller's build-time retry
/// re-runs; the operation is idempotent.
pub(crate) async fn reconcile_publications(
    store: &PublicationBackend,
    registry: &CollectionDefRegistry,
    subsystem: &SubsystemName,
    group: &str,
) -> Result<(), PublicationError> {
    // Routing rows use the same `(state_type, name)` namespacing as the
    // registry, so the private-name sweep addresses each collection exactly.
    // `cooperative` wraps each removal so the fan-out yields to the runtime
    // every ~128 collections rather than draining in one poll.
    stream::iter(
        registry
            .collections()
            .filter(|(state_type, name)| !registry.is_published(*state_type, name)),
    )
    .map(|(state_type, name)| {
        cooperative(async move {
            store
                .remove_group(subsystem, state_type, name, group)
                .await
                .map_err(PublicationError::from)
        })
    })
    .buffer_unordered(STATE_FANOUT_CONCURRENCY)
    .try_for_each(|()| async { Ok(()) })
    .await
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

    /// The topic's partition count was not available from the Kafka
    /// observation.
    #[error(transparent)]
    Count(#[from] PartitionCountObservationError),
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
