//! The memory reader harness and the backend-generic seam the reader trace
//! runners drive, plus the control-plane publication seeding both need.

use super::owner::registry_of;
use crate::Topic;
use crate::codec::JsonCodec;
use crate::loader::MemoryLoader;
use crate::state::descriptor::StateDescriptor;
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::memory::{
    MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore,
};
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CellStore;
use crate::state::tests::support::FixedOracle;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::state_reader::deps::SharedDeps;
use crate::subsystem::SubsystemName;
use color_eyre::eyre::Result;
use std::sync::Arc;
use std::time::Duration;

// --- Control-plane seeding --------------------------------------------------

/// Upserts `publication` as a source of `name` and freezes `identity` against
/// the memory control-plane stores (both `Infallible`, so the swallow is
/// total). The shared core of [`publish_source`] and
/// [`MemoryReaderBackend::publish`].
async fn seed_memory_publication(
    publications: &MemoryPublicationStore,
    identities: &MemoryDescriptorIdentityStore,
    subsystem: &SubsystemName,
    name: &StateName,
    publication: &StatePublication,
    identity: &DurableDescriptorIdentity,
) {
    publications
        .upsert(subsystem, StateType::Application, name, publication)
        .await
        .unwrap_or_else(|e| match e {});
    identities
        .register_identity(&publication.group_id, identity)
        .await
        .unwrap_or_else(|e| match e {});
}

/// Advertises `(group, topic)` as a source of `name` and freezes its identity
/// to match `descriptor`, so the reader admits it. The identity row is derived
/// from the same descriptor the reader carries, so acquisition validates equal.
pub(crate) async fn publish_source<D: StateDescriptor>(
    stores: (&MemoryPublicationStore, &MemoryDescriptorIdentityStore),
    subsystem: &SubsystemName,
    name: &StateName,
    group: &str,
    topic: Topic,
    count: PartitionCount,
    descriptor: &D,
) {
    let (publications, identities) = stores;
    let publication = StatePublication {
        group_id: Arc::from(group),
        topic,
        partition_count: count,
    };
    let row = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    seed_memory_publication(
        publications,
        identities,
        subsystem,
        name,
        &publication,
        &row,
    )
    .await;
}

/// The shared handles the memory suites hold: the stores the owner writes into
/// and the reader reads from, plus the publication and identity control-plane
/// stores.
#[derive(Clone)]
pub(in crate::state_reader::tests) struct MemoryHarness {
    pub(in crate::state_reader::tests) cells: MemoryCells,
    pub(in crate::state_reader::tests) publications: MemoryPublicationStore,
    pub(in crate::state_reader::tests) identities: MemoryDescriptorIdentityStore,
}

impl MemoryHarness {
    pub(in crate::state_reader::tests) fn new() -> Self {
        Self {
            cells: MemoryCells::new(),
            publications: MemoryPublicationStore::new(),
            identities: MemoryDescriptorIdentityStore::new(),
        }
    }

    /// A shared-deps bundle over these handles with a wall-clock cache.
    pub(in crate::state_reader::tests) fn deps(&self, budget: u64) -> SharedDeps<JsonCodec> {
        SharedDeps::memory(
            "reader-test".to_owned(),
            Duration::from_secs(30),
            self.cells.clone(),
            self.publications.clone(),
            self.identities.clone(),
            MemoryLoader::new(),
            budget,
        )
    }
}

// --- Backend-generic reader seam --------------------------------------------

/// The seam that `reader_suite::run_reader_*_trace` drives. One runner body
/// proves that committed state matches the oracle for both the memory
/// reader and a live Cassandra reader. An implementation supplies the three
/// pieces that differ by backend: the owner-seed cell store, seeded through
/// the real [`KeyedStateSession`] via [`owner_commit_cell`]; the
/// control-plane seeding; and the reader's `deps` bundle.
pub(in crate::state_reader::tests) trait ReaderBackend {
    /// The owner-seed cell store: [`MemoryCellStore`] for memory, the shared
    /// `CassandraStore<FixedOracle>` for Cassandra.
    type OwnerCell: CellStore;

    /// The registry the sessions and the owner cell store share.
    fn registry(&self) -> Arc<CollectionDefRegistry>;

    /// A cell store to seed one event through. Cloning shares the committed
    /// backing, memory cells or Cassandra rows, across a trace's events. On
    /// Cassandra, cloning also shares the one `MarkerMemo`/`MarkerPresence`
    /// lifecycle the store owns.
    fn owner_cell(&self) -> Self::OwnerCell;

    /// Advertises `(group, topic)` as a source of `name` and freezes `identity`
    /// so the reader admits it.
    async fn publish(
        &self,
        subsystem: &SubsystemName,
        name: &StateName,
        group: &str,
        topic: Topic,
        count: PartitionCount,
        identity: &DurableDescriptorIdentity,
    ) -> Result<()>;

    /// A fresh reader bundle over this backend's stores. Each call gets a
    /// fresh cache, so a per-event reader observes current committed state
    /// instead of a stale one.
    fn deps(&self) -> SharedDeps<JsonCodec>;
}

/// The memory [`ReaderBackend`]: a fresh [`MemoryHarness`] plus a registry
/// carrying the trace's single per-kind def.
pub(in crate::state_reader::tests) struct MemoryReaderBackend {
    harness: MemoryHarness,
    registry: Arc<CollectionDefRegistry>,
}

impl MemoryReaderBackend {
    /// A backend registering `descriptor` under `def`.
    pub(in crate::state_reader::tests) fn new<D: StateDescriptor>(
        descriptor: &D,
        def: CollectionDef,
    ) -> Result<Self> {
        Ok(Self {
            harness: MemoryHarness::new(),
            registry: registry_of(descriptor, def)?,
        })
    }
}

impl ReaderBackend for MemoryReaderBackend {
    type OwnerCell = MemoryCellStore<FixedOracle>;

    fn registry(&self) -> Arc<CollectionDefRegistry> {
        self.registry.clone()
    }

    fn owner_cell(&self) -> Self::OwnerCell {
        MemoryCellStore::new(
            self.harness.cells.clone(),
            FixedOracle::committed(),
            self.registry.clone(),
        )
    }

    async fn publish(
        &self,
        subsystem: &SubsystemName,
        name: &StateName,
        group: &str,
        topic: Topic,
        count: PartitionCount,
        identity: &DurableDescriptorIdentity,
    ) -> Result<()> {
        let publication = StatePublication {
            group_id: Arc::from(group),
            topic,
            partition_count: count,
        };
        seed_memory_publication(
            &self.harness.publications,
            &self.harness.identities,
            subsystem,
            name,
            &publication,
            identity,
        )
        .await;
        Ok(())
    }

    fn deps(&self) -> SharedDeps<JsonCodec> {
        self.harness.deps(1 << 20)
    }
}
