//! Production composition helpers for the keyed-state stack.
//!
//! This module provides the per-partition [`StateBackendFactory`]
//! implementations the typical production composition needs:
//!
//! * [`CassandraStateBackendFactory`] mints, per partition, the
//!   [`CassandraStore`] (shared), a per-partition fjall committed-value cache,
//!   and the segment-scoped commit oracle.
//! * [`MemoryStateBackendFactory`] mints the in-memory equivalents over a
//!   process-wide shared [`MemoryCellStore`].
//! * [`ProductionOracle`] names the commit oracle ([`CommitManager`]) that
//!   answers "did this event commit?" while the recovery sweep resolves
//!   provisional cells.
//!
//! [`CommitOracle`]: super::oracle::CommitOracle

use crate::ConsumerGroup;
use crate::cassandra::CassandraStore as CassandraSession;
use crate::commit_manager::{CommitManager, StoreTagSource};
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::state::cached::Cached;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraStore, CellQueries,
};
use crate::state::fjall::{AssignmentEpoch, FjallCellCache, FjallClient, FjallValueStoreError};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{PartitionBackend, StateBackendFactory};
use crate::timers::duration::CompactDuration;
use crate::timers::store::{Segment, TriggerStoreProvider};
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;

/// The oracle both production backend factories mint: a [`CommitManager`]
/// whose timer half reads tags straight from the partition's segment-scoped
/// trigger store.
///
/// The store-backed tag source is sound here because the oracle is only
/// consulted for *provisional* cells during recovery, and per-key
/// serialization guarantees the staging event fully completed — durability
/// markers included — before any later event on the key dispatches.
pub type ProductionOracle<DP, TP> = CommitManager<
    <DP as DeduplicationStoreProvider>::Store,
    StoreTagSource<<TP as TriggerStoreProvider>::Store>,
>;

/// The per-partition commit-oracle ingredients both factories mint their
/// [`ProductionOracle`] from: the deduplication store provider (the message
/// commit half), the trigger store provider (the timer-tag half), and the
/// segment-deriving consumer group + timer slab size. Bundled so a factory
/// constructor stays readable.
#[derive(Clone)]
pub struct OracleProviders<DP, TP> {
    /// Deduplication store provider — the mandatory message-commit oracle half.
    pub dedup: DP,
    /// Trigger store provider — the timer-tag oracle half.
    pub triggers: TP,
    /// Consumer group, deriving the partition's segment id.
    pub consumer_group: ConsumerGroup,
    /// Timer slab size for the segment.
    pub timer_slab_size: CompactDuration,
}

/// [`StateBackendFactory`] for the Cassandra storage backend.
///
/// Per partition it opens one fjall workspace (the committed-value cache),
/// mints the segment-scoped commit oracle, and hands out clones of the shared
/// [`CassandraStore`] and [`CassandraDescriptorIdentityStore`] — so the
/// sessions stage and the recovery sweep resolve through one oracle while
/// identity validation runs against the one group-global identity store. The
/// cache store owns the workspace, so the workspace's `Drop` (which deletes the
/// fjall partition) fires only at partition revocation.
#[derive(Clone)]
pub struct CassandraStateBackendFactory<DP, TP> {
    client: Arc<FjallClient>,
    session: CassandraSession,
    queries: Arc<CellQueries>,
    identity: CassandraDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
    dedup: DP,
    triggers: TP,
    consumer_group: ConsumerGroup,
    timer_slab_size: CompactDuration,
}

impl<DP, TP> CassandraStateBackendFactory<DP, TP> {
    /// Creates the factory.
    ///
    /// Deduplication is mandatory — it is the commit oracle for message-event
    /// recovery. The `registry` is shared with the
    /// [`StateManagerProvider`](crate::state::manager::StateManagerProvider) so
    /// the per-partition cell store binds the same per-collection TTLs on its
    /// resolution write-backs as the session does at stage time.
    #[must_use]
    pub fn new(
        client: Arc<FjallClient>,
        cell: CassandraCellResources,
        identity: CassandraDescriptorIdentityStore,
        registry: Arc<CollectionDefRegistry>,
        oracle: OracleProviders<DP, TP>,
    ) -> Self {
        let CassandraCellResources { session, queries } = cell;
        let OracleProviders {
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
        } = oracle;
        Self {
            client,
            session,
            queries,
            identity,
            registry,
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
        }
    }
}

impl<DP, TP> StateBackendFactory for CassandraStateBackendFactory<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    type Backend = PartitionBackend<
        ProductionOracle<DP, TP>,
        CassandraDescriptorIdentityStore,
        Cached<CassandraStore<ProductionOracle<DP, TP>>>,
    >;
    type Error = FjallValueStoreError;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Backend, Self::Error> {
        let epoch = AssignmentEpoch::mint();
        let workspace = self.client.workspace(topic, partition, epoch)?;
        // The cache owns the workspace, holding it (and so its on-disk
        // partition) alive until the partition's state manager is dropped at
        // revocation.
        let fjall = FjallCellCache::for_workspace(workspace);
        let oracle = mint_oracle(
            &self.dedup,
            &self.triggers,
            &self.consumer_group,
            self.timer_slab_size,
            topic,
            partition,
        );
        // Production writer bottom: fjall write-through cache over the resolving
        // Cassandra cell store; the session wraps this in its per-event Overlay.
        let cassandra = CassandraStore::new(
            self.session.clone(),
            self.queries.clone(),
            oracle.clone(),
            self.registry.clone(),
        );
        let cell = Cached::new(fjall, cassandra);
        Ok(PartitionBackend::new(oracle, self.identity.clone(), cell))
    }
}

/// [`StateBackendFactory`] for the in-memory storage backend (and mock
/// mode).
///
/// One process-wide [`MemoryCellStore`] and one process-wide
/// [`MemoryDescriptorIdentityStore`] are shared across partitions (state and
/// identities survive reassignment within the process); the committed-value
/// cache and oracle are minted per partition, mirroring the Cassandra factory.
#[derive(Clone)]
pub struct MemoryStateBackendFactory<DP, TP> {
    cells: MemoryCells,
    identity: MemoryDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
    dedup: DP,
    triggers: TP,
    consumer_group: ConsumerGroup,
    timer_slab_size: CompactDuration,
}

impl<DP, TP> MemoryStateBackendFactory<DP, TP> {
    /// Creates the factory over a shared memory cell map and identity store.
    /// See [`CassandraStateBackendFactory::new`] for the `dedup`/`registry`
    /// contract.
    #[must_use]
    pub fn new(
        cells: MemoryCells,
        identity: MemoryDescriptorIdentityStore,
        registry: Arc<CollectionDefRegistry>,
        oracle: OracleProviders<DP, TP>,
    ) -> Self {
        let OracleProviders {
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
        } = oracle;
        Self {
            cells,
            identity,
            registry,
            dedup,
            triggers,
            consumer_group,
            timer_slab_size,
        }
    }
}

impl<DP, TP> StateBackendFactory for MemoryStateBackendFactory<DP, TP>
where
    DP: DeduplicationStoreProvider,
    TP: TriggerStoreProvider,
{
    type Backend = PartitionBackend<
        ProductionOracle<DP, TP>,
        MemoryDescriptorIdentityStore,
        MemoryCellStore<ProductionOracle<DP, TP>>,
    >;
    type Error = Infallible;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Backend, Self::Error> {
        let oracle = mint_oracle(
            &self.dedup,
            &self.triggers,
            &self.consumer_group,
            self.timer_slab_size,
            topic,
            partition,
        );
        let cell = MemoryCellStore::new(self.cells.clone(), oracle.clone(), self.registry.clone());
        Ok(PartitionBackend::new(oracle, self.identity.clone(), cell))
    }
}

/// Mints the segment-scoped commit oracle shared by both factories.
///
/// The trigger store is created over [`Segment::for_partition`] — the same
/// formula the partition loop uses — so the oracle reads the exact timer
/// rows the partition writes.
fn mint_oracle<DP, TP>(
    dedup: &DP,
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
    let dedup = dedup.create_store(topic, partition, consumer_group);
    let triggers = StoreTagSource(triggers.create_store(Segment::for_partition(
        consumer_group,
        topic,
        partition,
        timer_slab_size,
    )));
    CommitManager::new(dedup, triggers)
}
