//! Production composition helpers for the keyed-state stack.
//!
//! This module provides small wiring utilities that the typical
//! production composition needs. It stays a thin layer over the concrete
//! types in [`crate::state::cassandra`], [`crate::state::fjall`],
//! [`crate::state::layered`], and [`crate::state::recovering`].
//!
//! Three exports, two of them a pair:
//!
//! * [`ProductionValueDurable`] names the canonical durable bundle shape,
//!   `Layered<FjallValueStore, Recovering<CassandraValueStore, O>>`. The
//!   `Recovering` layer is baked into the alias, so the production bundle
//!   cannot even be *named* without it.
//! * [`compose_value_durable`] is the only sanctioned way to build that bundle.
//!   The `Recovering` layer is load-bearing for crash safety — omitting it
//!   reopens the lost-commit data-loss window — so funneling construction
//!   through one composer keeps the layer from being silently dropped.
//! * [`capped_default_ttl`] applies the Cassandra `USING TTL` ceiling
//!   ([`MAX_CASSANDRA_TTL_SECS`]) to a configured default TTL. Producers wire
//!   this once when they read `CassandraStore::base_ttl()`; the result feeds
//!   every store and wrapper that takes a `default_ttl:
//!   Option<CompactDuration>`.
//!
//! See the [design summary][summary] for the full canonical composition:
//! `Layered<FjallValueStore, Recovering<CassandraValueStore,
//! CommitManager>>` as the Value durable bundle,
//! `CassandraValueStore` as the [`PendingIndexScanner`],
//! `CommitManager` as the [`CommitOracle`], and
//! `FjallDirtyValueStoreFactory` as the dirty-store factory.
//!
//! [summary]: ../../docs/keyed-state/design-summary.md
//! [`PendingIndexScanner`]: super::pending::PendingIndexScanner
//! [`CommitOracle`]: super::oracle::CommitOracle

use crate::ConsumerGroup;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::commit_manager::{CommitManager, StoreTagSource};
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::state::cassandra::CassandraValueStore;
use crate::state::fjall::{
    AssignmentEpoch, FjallClient, FjallDirtyValueStoreProvider, FjallFactoryError, FjallValueStore,
};
use crate::state::layered::LayeredValueStore;
use crate::state::memory::{MemoryDirtyValueStoreProvider, MemoryDurableValueStore};
use crate::state::recovering::RecoveringValueStore;
use crate::state::registry::CollectionDefRegistry;
use crate::state::{BackendOf, StateBackend, StateBackendFactory};
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, TriggerStoreProvider};
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;

/// The canonical production Value durable bundle:
/// `Layered<FjallValueStore, Recovering<CassandraValueStore, O>>`, with the
/// shared [`Arc<CollectionDefRegistry>`] as the recovery-write TTL resolver.
///
/// `O` is the [`CommitOracle`](crate::state::oracle::CommitOracle) — in
/// production [`CommitManager`].
pub type ProductionValueDurable<O> = LayeredValueStore<
    FjallValueStore,
    RecoveringValueStore<CassandraValueStore, O, Arc<CollectionDefRegistry>>,
>;

/// Composes the canonical production Value durable bundle.
///
/// Wraps the authoritative [`CassandraValueStore`] in
/// [`RecoveringValueStore`] — so both the read-before-use (`get`) and
/// recover-before-overwrite (`seal`) recovery paths are always present — and
/// fronts it with the [`FjallValueStore`] cache. This is the one sanctioned
/// constructor for the bundle, and its return type — [`ProductionValueDurable`]
/// — bakes `Recovering` into the type, so the production bundle cannot be named
/// without it. Routing every wiring through here keeps the `Recovering` layer
/// from being silently dropped, which would reopen the lost-commit data-loss
/// window.
///
/// `registry` must be the **same** [`Arc<CollectionDefRegistry>`] the
/// state manager's provider holds, so first-touch recovery (driven by the
/// wrapper's resolver) and the timer-sweep recovery (driven by the
/// manager's registry) bind identical per-collection TTLs.
#[must_use]
pub fn compose_value_durable<O>(
    cache: FjallValueStore,
    backing: CassandraValueStore,
    oracle: O,
    registry: Arc<CollectionDefRegistry>,
) -> ProductionValueDurable<O> {
    LayeredValueStore::new(cache, RecoveringValueStore::new(backing, oracle, registry))
}

/// Caps a configured default TTL at the Cassandra `USING TTL` ceiling.
///
/// Returns `Some(base)` when the duration fits within
/// [`MAX_CASSANDRA_TTL_SECS`]; returns `None` when the
/// duration would overflow the ceiling. `None` signals "do not bind a TTL
/// on writes", which the keyed-state stores route through the `*_no_ttl`
/// Cassandra queries (an indefinite-retention fallback that is preferable
/// to a silent rejection).
#[must_use]
pub fn capped_default_ttl(base: CompactDuration) -> Option<CompactDuration> {
    (i64::from(base.seconds()) <= MAX_CASSANDRA_TTL_SECS).then_some(base)
}

/// The oracle both production backend factories mint: a [`CommitManager`]
/// whose timer half reads tags straight from the partition's segment-scoped
/// trigger store.
///
/// The store-backed tag source is sound here because the oracle is only
/// consulted for *sealed* WALs during recovery, and per-key serialization
/// guarantees the sealing event fully completed — durability markers
/// included — before any later event on the key dispatches.
pub type ProductionOracle<DP, TP> = CommitManager<
    <DP as DeduplicationStoreProvider>::Store,
    StoreTagSource<<TP as TriggerStoreProvider>::Store>,
>;

/// [`StateBackendFactory`] for the Cassandra storage backend.
///
/// Per partition it opens one fjall workspace (committed cache + dirty
/// overlay), mints the segment-scoped commit oracle, and composes the
/// canonical durable bundle via [`compose_value_durable`] — so first-touch
/// recovery and the middleware sweep share one oracle, and the cache and
/// dirty workspaces share one fjall instance.
#[derive(Clone)]
pub struct CassandraStateBackendFactory<DP, TP> {
    client: Arc<FjallClient>,
    backing: CassandraValueStore,
    dedup: Option<DP>,
    triggers: TP,
    consumer_group: ConsumerGroup,
    timer_slab_size: CompactDuration,
    registry: Arc<CollectionDefRegistry>,
}

impl<DP, TP> CassandraStateBackendFactory<DP, TP> {
    /// Creates the factory.
    ///
    /// `dedup` may be `None` only when no keyed-state collections are
    /// registered (the consumer wiring enforces this) — message-event
    /// recovery reads commit state from the deduplication store.
    /// `registry` must be the same [`Arc<CollectionDefRegistry>`] handed to
    /// the keyed-state middleware so every recovery path binds identical
    /// per-collection TTLs.
    #[must_use]
    pub fn new(
        client: Arc<FjallClient>,
        backing: CassandraValueStore,
        dedup: Option<DP>,
        triggers: TP,
        consumer_group: ConsumerGroup,
        timer_slab_size: CompactDuration,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self {
            client,
            backing,
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
            registry,
        }
    }
}

impl<DP, TP> StateBackendFactory for CassandraStateBackendFactory<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    type DirtyProvider = FjallDirtyValueStoreProvider;
    type Durable = ProductionValueDurable<ProductionOracle<DP, TP>>;
    type Error = FjallFactoryError;
    type Oracle = ProductionOracle<DP, TP>;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<BackendOf<Self>, Self::Error> {
        let epoch = AssignmentEpoch::now().map_err(FjallFactoryError::Epoch)?;
        let workspace = Arc::new(
            self.client
                .workspace(topic, partition, epoch)
                .map_err(FjallFactoryError::Workspace)?,
        );
        let cache = FjallValueStore::new(workspace.cache_handle().clone());
        let oracle = self.oracle_for(topic, partition);
        let durable = compose_value_durable(
            cache,
            self.backing.clone(),
            oracle.clone(),
            Arc::clone(&self.registry),
        );
        Ok(StateBackend {
            durable,
            oracle,
            dirty: FjallDirtyValueStoreProvider::new(workspace),
        })
    }
}

impl<DP, TP> CassandraStateBackendFactory<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    fn oracle_for(&self, topic: Topic, partition: Partition) -> ProductionOracle<DP, TP> {
        mint_oracle(
            self.dedup.as_ref(),
            &self.triggers,
            &self.consumer_group,
            self.timer_slab_size,
            topic,
            partition,
        )
    }
}

/// [`StateBackendFactory`] for the in-memory storage backend (and mock
/// mode).
///
/// One process-wide [`MemoryDurableValueStore`] is shared across partitions
/// (state survives reassignment within the process); the recovery wrapper
/// and oracle are minted per partition, mirroring the Cassandra factory.
#[derive(Clone)]
pub struct MemoryStateBackendFactory<DP, TP> {
    durable: MemoryDurableValueStore,
    dedup: Option<DP>,
    triggers: TP,
    consumer_group: ConsumerGroup,
    timer_slab_size: CompactDuration,
    registry: Arc<CollectionDefRegistry>,
}

impl<DP, TP> MemoryStateBackendFactory<DP, TP> {
    /// Creates the factory over a shared memory durable store. See
    /// [`CassandraStateBackendFactory::new`] for the `dedup` and `registry`
    /// contracts.
    #[must_use]
    pub fn new(
        durable: MemoryDurableValueStore,
        dedup: Option<DP>,
        triggers: TP,
        consumer_group: ConsumerGroup,
        timer_slab_size: CompactDuration,
        registry: Arc<CollectionDefRegistry>,
    ) -> Self {
        Self {
            durable,
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
            registry,
        }
    }
}

impl<DP, TP> StateBackendFactory for MemoryStateBackendFactory<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    type DirtyProvider = MemoryDirtyValueStoreProvider;
    type Durable = RecoveringValueStore<
        MemoryDurableValueStore,
        ProductionOracle<DP, TP>,
        Arc<CollectionDefRegistry>,
    >;
    type Error = Infallible;
    type Oracle = ProductionOracle<DP, TP>;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<BackendOf<Self>, Self::Error> {
        let oracle = mint_oracle(
            self.dedup.as_ref(),
            &self.triggers,
            &self.consumer_group,
            self.timer_slab_size,
            topic,
            partition,
        );
        let durable = RecoveringValueStore::new(
            self.durable.clone(),
            oracle.clone(),
            Arc::clone(&self.registry),
        );
        Ok(StateBackend {
            durable,
            oracle,
            dirty: MemoryDirtyValueStoreProvider,
        })
    }
}

/// Mints the segment-scoped commit oracle shared by both factories.
///
/// The trigger store is created over [`Segment::for_partition`] — the same
/// formula the partition loop uses — so the oracle reads the exact timer
/// rows the partition writes.
fn mint_oracle<DP, TP>(
    dedup: Option<&DP>,
    triggers: &TP,
    consumer_group: &ConsumerGroup,
    timer_slab_size: CompactDuration,
    topic: Topic,
    partition: Partition,
) -> ProductionOracle<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    let dedup = dedup.map(|provider| provider.create_store(topic, partition, consumer_group));
    let triggers = StoreTagSource(triggers.create_store(Segment::for_partition(
        consumer_group,
        topic,
        partition,
        timer_slab_size,
    )));
    CommitManager::with_optional_dedup(dedup, triggers)
}

#[cfg(test)]
mod tests;
