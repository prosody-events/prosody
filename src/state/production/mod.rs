//! Production composition helpers for the keyed-state stack.
//!
//! This module provides the per-partition [`StateBackendFactory`]
//! implementations the typical production composition needs:
//!
//! * [`CassandraStateBackendFactory`] mints, per partition, the
//!   [`CassandraStore`] (shared), a per-partition fjall committed-value cache,
//!   and the partition's commit oracle.
//! * [`MemoryStateBackendFactory`] mints the in-memory equivalents over a
//!   process-wide shared [`MemoryCellStore`].
//! * [`ProductionOracle`] names the commit oracle (`CommitManager`) that
//!   answers "did this event commit?" while the recovery sweep resolves
//!   provisional cells.
//!
//! [`CommitOracle`]: super::oracle::CommitOracle

use crate::ConsumerGroup;
use crate::consumer::middleware::deduplication::DeduplicationStoreProvider;
use crate::state::cached::Cached;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraStore,
};
use crate::state::commit::{CommitManager, StoreTagSource};
use crate::state::fjall::{FjallCellCache, FjallCellCacheError, FjallClient};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::CollectionDefRegistry;
use crate::state::{PartitionBackend, StateBackendFactory};
use crate::timers::store::TriggerStore;
use crate::{Partition, Topic};
use std::convert::Infallible;
use std::sync::Arc;

/// The oracle both production backend factories mint: a `CommitManager`
/// whose timer half reads tags through the partition's own trigger store
/// handle `S` (`mint_oracle` documents why the handle must be shared) and
/// whose message half reads the dedup marker through a provider-minted
/// dedup store.
///
/// The oracle is only consulted for *provisional* cells during recovery, and
/// per-key serialization guarantees the staging event fully completed —
/// durability markers included — before any later event on the key
/// dispatches.
pub type ProductionOracle<DP, S> =
    CommitManager<<DP as DeduplicationStoreProvider>::Store, StoreTagSource<S>>;

/// [`StateBackendFactory`] for the Cassandra storage backend.
///
/// Per partition it opens one fjall workspace (the committed-value cache),
/// mints the commit oracle over the partition's trigger-store handle, and
/// hands out clones of the shared [`CassandraStore`] and
/// [`CassandraDescriptorIdentityStore`] — so the sessions stage and the
/// recovery sweep resolve through one oracle while identity validation runs
/// against the one group-global identity store. The cache store owns the
/// workspace, so the workspace's `Drop` (which deletes the fjall partition)
/// fires only at partition revocation.
#[derive(Clone)]
pub struct CassandraStateBackendFactory<DP> {
    client: Arc<FjallClient>,
    cell: CassandraCellResources,
    identity: CassandraDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
    dedup: DP,
    consumer_group: ConsumerGroup,
}

impl<DP> CassandraStateBackendFactory<DP> {
    /// Creates the factory.
    ///
    /// Deduplication is mandatory — it is the commit oracle for message-event
    /// recovery. The `registry` is shared with the
    /// [`StateManagerProvider`](crate::state::manager::StateManagerProvider) so
    /// the per-partition cell store binds the same per-collection TTLs on its
    /// resolution write-backs as the session does at stage time.
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
        ProductionOracle<DP, S>,
        CassandraDescriptorIdentityStore,
        Cached<CassandraStore<ProductionOracle<DP, S>>>,
    >;
    type Error = FjallCellCacheError;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: S,
    ) -> Result<Self::Backend, Self::Error> {
        let workspace = self.client.workspace(topic, partition)?;
        // The cache owns the workspace, holding it (and so its on-disk
        // partition) alive until the partition's state manager is dropped at
        // revocation.
        let fjall = FjallCellCache::for_workspace(workspace);
        let oracle = mint_oracle(
            &self.dedup,
            &self.consumer_group,
            topic,
            partition,
            triggers,
        );
        // Production writer bottom: fjall write-through cache over the resolving
        // Cassandra cell store; the session wraps this in its per-event Overlay.
        // `Cached` owns the fjall workspace, so its warm provisional-coordinate
        // cache and its scan coverage both spill to the one per-partition
        // `index` keyspace.
        let CassandraCellResources { session, queries } = &self.cell;
        let cassandra = CassandraStore::new(
            session.clone(),
            queries.clone(),
            oracle.clone(),
            self.registry.clone(),
            fjall.presence(),
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
pub struct MemoryStateBackendFactory<DP> {
    cells: MemoryCells,
    identity: MemoryDescriptorIdentityStore,
    registry: Arc<CollectionDefRegistry>,
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
        registry: Arc<CollectionDefRegistry>,
        dedup: DP,
        consumer_group: ConsumerGroup,
    ) -> Self {
        Self {
            cells,
            identity,
            registry,
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
    type Backend = PartitionBackend<
        ProductionOracle<DP, S>,
        MemoryDescriptorIdentityStore,
        MemoryCellStore<ProductionOracle<DP, S>>,
    >;
    type Error = Infallible;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
        triggers: S,
    ) -> Result<Self::Backend, Self::Error> {
        let oracle = mint_oracle(
            &self.dedup,
            &self.consumer_group,
            topic,
            partition,
            triggers,
        );
        let cell = MemoryCellStore::new(self.cells.clone(), oracle.clone(), self.registry.clone());
        Ok(PartitionBackend::new(oracle, self.identity.clone(), cell))
    }
}

/// Mints the partition's commit oracle, shared by both factories.
///
/// **Timer half — handle sharing is required.** `triggers` is a clone of the
/// very store the partition's timer manager writes through, threaded down
/// from the partition loop via
/// [`StateBackendFactory::for_partition`]. The Cassandra store answers
/// [`current_tag`](crate::timers::store::TriggerStore::current_tag)
/// cache-first from a per-instance state cache that only writes through the
/// instance (and its clones) keep current, so a sibling store minted from a
/// provider could serve a stale tag and roll back a committed write.
///
/// **Dedup half — provider minting is sound.** The marker *write* already
/// goes through this oracle's own store ([`CommitOracle::record_message`] is
/// the `settle` boundary's flush), so writer and recovery reader are one
/// instance by construction. The dedup middleware's separate filter
/// instance cannot diverge either: every store a
/// [`DeduplicationStoreProvider`] creates shares its state — the Cassandra
/// provider shares one session and one write-through marker cache across
/// all stores it mints (and the cache is presence-only over an insert-only
/// table, so it can never claim a marker that was not durably written); the
/// memory provider hands out clones of one shared set.
///
/// [`CommitOracle::record_message`]:
///     crate::state::oracle::CommitOracle::record_message
fn mint_oracle<DP, S>(
    dedup: &DP,
    consumer_group: &str,
    topic: Topic,
    partition: Partition,
    triggers: S,
) -> ProductionOracle<DP, S>
where
    DP: DeduplicationStoreProvider,
    S: TriggerStore,
{
    let dedup = dedup.create_store(topic, partition, consumer_group);
    CommitManager::new(dedup, StoreTagSource(triggers))
}
