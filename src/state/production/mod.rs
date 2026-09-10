//! Creates Cassandra and memory dependencies for partition assignments.

use crate::ConsumerGroup;
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::state::cached::Cached;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraStore,
};
use crate::state::fjall::{FjallCellCache, FjallCellCacheError, FjallClient, MarkerCheckSet};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{PartitionBackend, StateBackendFactory};
use crate::timers::store::TriggerStore;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;

/// Creates Cassandra stores and a disk workspace for each partition assignment.
/// The workspace holds the committed cache and admission checks until
/// revocation.
#[derive(Clone)]
pub(crate) struct CassandraStateBackendFactory<DP> {
    client: Arc<FjallClient>,
    cell: CassandraCellResources,
    identity: CassandraDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
    dedup: DP,
    consumer_group: ConsumerGroup,
}

impl<DP> CassandraStateBackendFactory<DP> {
    /// Creates the factory with shared cells, identities, registry, and dedup
    /// stores.
    #[must_use]
    pub(crate) fn new(
        client: Arc<FjallClient>,
        cell: CassandraCellResources,
        identity: CassandraDescriptorIdentityStore,
        registry: Arc<CollectionDefRegistry>,
        dedup: DP,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            client,
            cell,
            identity,
            registry,
            dedup,
            consumer_group,
        }
    }
}

impl<DP, S> StateBackendFactory<S> for CassandraStateBackendFactory<DP>
where
    DP: DeduplicationStoreProvider,
    S: TriggerStore,
{
    type Backend = PartitionBackend<
        DP::Store,
        CassandraDescriptorIdentityStore,
        Cached<CassandraStore>,
        MarkerCheckSet,
    >;
    type Error = FjallCellCacheError;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        _triggers: S,
    ) -> Result<Self::Backend, Self::Error> {
        let workspace = self.client.workspace(topic, partition)?;
        // The cache owns the workspace, holding it (and so its on-disk
        // partition) alive until the partition's state manager is dropped at
        // revocation.
        let fjall = FjallCellCache::for_workspace(workspace);
        let dedup = self
            .dedup
            .create_store(topic, partition, &self.consumer_group);
        let CassandraCellResources { session, queries } = &self.cell;
        let cassandra =
            CassandraStore::new(session.clone(), queries.clone(), self.registry.clone());
        let checks = fjall.marker_checks();
        let cell = Cached::new(fjall, cassandra);
        Ok(PartitionBackend::new(
            dedup,
            self.identity.clone(),
            cell,
            checks,
        ))
    }
}

/// Shares memory cells and identities across partitions.
/// Memory assignments have no disk workspace and admit each event.
#[derive(Clone)]
pub(crate) struct MemoryStateBackendFactory<DP> {
    cells: MemoryCells,
    identity: MemoryDescriptorIdentityStore,
    dedup: DP,
    consumer_group: ConsumerGroup,
}

impl<DP> MemoryStateBackendFactory<DP> {
    /// Creates the factory over a shared memory cell map and identity store.
    /// See [`CassandraStateBackendFactory::new`] for the `dedup`/`registry`
    /// contract.
    #[must_use]
    pub(crate) fn new(
        cells: MemoryCells,
        identity: MemoryDescriptorIdentityStore,
        dedup: DP,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            cells,
            identity,
            dedup,
            consumer_group,
        }
    }
}

impl<DP, S> StateBackendFactory<S> for MemoryStateBackendFactory<DP>
where
    DP: DeduplicationStoreProvider,
    S: TriggerStore,
{
    type Backend = PartitionBackend<DP::Store, MemoryDescriptorIdentityStore, MemoryCellStore, ()>;
    type Error = Infallible;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        _triggers: S,
    ) -> Result<Self::Backend, Self::Error> {
        let dedup = self
            .dedup
            .create_store(topic, partition, &self.consumer_group);
        let cell = MemoryCellStore::new(self.cells.clone());
        Ok(PartitionBackend::new(
            dedup,
            self.identity.clone(),
            cell,
            (),
        ))
    }
}
