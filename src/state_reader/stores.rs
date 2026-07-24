//! Backend carriage for the reader's stores — the **oracle-free** committed
//! carriers plus the publication and identity control-plane stores.
//!
//! A closed enum (no `dyn`) delegates each read to the active backend. The
//! read source is the oracle-free carriers only ([`CassandraCellResources`],
//! [`MemoryCells`]) — never the resolving stores, the owner's write-through
//! cache, or a dirty overlay.

use crate::state::access::StateAccessError;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::identity::CollectionId;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::store::{CellBuffer, CoordinateBatch};
use crate::state::{StateName, StateType};
use crate::state_reader::error::StateReaderError;
use crate::subsystem::SubsystemName;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};

#[cfg(test)]
use crate::state::tests::support::ScriptedPublicationStore;
#[cfg(test)]
use crate::state_reader::tests::support::{CountingIdentityStore, ScriptedCellSource};

/// The reader's stores, carried as a closed enum. Each arm bundles a
/// committed-cell carrier with the publication and identity stores of the same
/// backend.
#[derive(Clone)]
pub(crate) enum ReaderStores {
    /// Cassandra-backed reader stores.
    Cassandra {
        /// Oracle-free committed cell reads.
        cells: CassandraCellResources,
        /// Routing-row discovery.
        publications: CassandraPublicationStore,
        /// Frozen descriptor-identity validation.
        identities: CassandraDescriptorIdentityStore,
    },
    /// In-memory reader stores (mock/tests).
    Memory {
        /// Oracle-free committed cell reads.
        cells: MemoryCells,
        /// Routing-row discovery.
        publications: MemoryPublicationStore,
        /// Frozen descriptor-identity validation.
        identities: MemoryDescriptorIdentityStore,
    },
    /// Scripted faults for the probe/refresh property tests.
    #[cfg(test)]
    Scripted {
        /// Seeded committed cells with injectable fault points.
        cells: ScriptedCellSource,
        /// Staged publication-row edits with injectable read faults.
        publications: ScriptedPublicationStore,
        /// Identity store that counts reads (already-admitted no-re-read pin).
        identities: CountingIdentityStore,
    },
}

impl ReaderStores {
    /// The oracle-free committed point read for `cell`, erasing the backend
    /// error to [`StateAccessError`].
    pub(crate) async fn read_committed(
        &self,
        id: &CollectionId,
        cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        match self {
            Self::Cassandra { cells, .. } => cells
                .read_committed(id, cell)
                .await
                .map_err(|e| StateAccessError::store(&e)),
            Self::Memory { cells, .. } => Ok(cells.read_committed(id, cell)),
            #[cfg(test)]
            Self::Scripted { cells, .. } => cells.read_committed(id, cell),
        }
    }

    /// The oracle-free committed batch read, index-aligned to `batch`.
    pub(crate) async fn read_committed_many(
        &self,
        id: &CollectionId,
        section: Section,
        batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        match self {
            Self::Cassandra { cells, .. } => cells
                .read_committed_many(id, section, batch)
                .await
                .map_err(|e| StateAccessError::store(&e)),
            Self::Memory { cells, .. } => Ok(cells.read_committed_many(id, section, batch)),
            #[cfg(test)]
            Self::Scripted { cells, .. } => cells.read_committed_many(id, section, batch),
        }
    }

    /// The oracle-free committed section scan, unified to one stream type over
    /// [`StateAccessError`].
    pub(crate) fn scan_committed<'a>(
        &'a self,
        id: &'a CollectionId,
        scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        try_stream! {
            match self {
                Self::Cassandra { cells, .. } => {
                    let inner = cells.scan_committed(id, scan);
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().await {
                        yield item.map_err(|e| StateAccessError::store(&e))?;
                    }
                }
                Self::Memory { cells, .. } => {
                    let inner = cells.scan_committed(id, scan);
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().await {
                        // Memory scan errors are `Infallible`.
                        yield item.map_err(|e| StateAccessError::store(&e))?;
                    }
                }
                #[cfg(test)]
                Self::Scripted { cells, .. } => {
                    let inner = cells.scan_committed(id, scan);
                    futures::pin_mut!(inner);
                    while let Some(item) = inner.next().await {
                        yield item?;
                    }
                }
            }
        }
    }

    /// Reads all published sources of `(subsystem, state_type, name)` — one
    /// partition read.
    pub(crate) async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<Vec<StatePublication>, StateReaderError> {
        match self {
            Self::Cassandra { publications, .. } => publications
                .read_publications(subsystem, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
            Self::Memory { publications, .. } => publications
                .read_publications(subsystem, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
            #[cfg(test)]
            Self::Scripted { publications, .. } => publications
                .read_publications(subsystem, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
        }
    }

    /// Point-reads the frozen identity row for `(group, state_type, name)`.
    pub(crate) async fn read_identity(
        &self,
        group: &str,
        state_type: StateType,
        name: &str,
    ) -> Result<Option<DurableDescriptorIdentity>, StateReaderError> {
        match self {
            Self::Cassandra { identities, .. } => identities
                .read_identity(group, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
            Self::Memory { identities, .. } => identities
                .read_identity(group, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
            #[cfg(test)]
            Self::Scripted { identities, .. } => identities
                .read_identity(group, state_type, name)
                .await
                .map_err(|e| StateReaderError::store(&e)),
        }
    }
}
