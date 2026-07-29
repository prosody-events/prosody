//! The reader's stores for each backend. Every backend provides committed-cell
//! reads that bypass the commit oracle, plus its publication and identity
//! stores.
//!
//! A closed enum (no `dyn`) delegates each read to the active backend. Reads
//! come only from the committed-cell stores ([`CassandraCellResources`],
//! [`MemoryCells`]). Do not read from the resolving stores, the owner's
//! write-through cache, or a dirty overlay.

use crate::state::access::StateAccessError;
use crate::state::cassandra::{
    CassandraCellResources, CassandraDescriptorIdentityStore, CassandraPublicationStore,
};
use crate::state::cell_key::{CellKey, Scan, Section};
use crate::state::descriptor_identity::{DescriptorIdentityStore, DurableDescriptorIdentity};
use crate::state::identity::CollectionId;
use crate::state::memory::{MemoryCells, MemoryDescriptorIdentityStore, MemoryPublicationStore};
use crate::state::publication::{PublicationRows, PublicationStore};
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

/// The reader's stores as a closed enum. Each arm holds one backend's
/// committed-cell store together with its publication and identity stores.
#[derive(Clone)]
pub(crate) enum ReaderStores {
    /// Cassandra-backed reader stores.
    Cassandra {
        /// Committed cell reads that bypass the commit oracle.
        cells: CassandraCellResources,
        /// Finds the published sources for a collection.
        publications: CassandraPublicationStore,
        /// Reads the frozen descriptor identity for validation.
        identities: CassandraDescriptorIdentityStore,
    },
    /// In-memory reader stores (mock/tests).
    Memory {
        /// Committed cell reads that bypass the commit oracle.
        cells: MemoryCells,
        /// Finds the published sources for a collection.
        publications: MemoryPublicationStore,
        /// Reads the frozen descriptor identity for validation.
        identities: MemoryDescriptorIdentityStore,
    },
    /// Scripted faults for the probe/refresh property tests.
    #[cfg(test)]
    Scripted {
        /// Seeded committed cells with injectable fault points.
        cells: ScriptedCellSource,
        /// Staged publication-row edits with injectable read faults.
        publications: ScriptedPublicationStore,
        /// Identity store that counts reads, so a test can assert an
        /// already-validated identity is not read again.
        identities: CountingIdentityStore,
    },
}

impl ReaderStores {
    /// Committed point read for `cell` that bypasses the commit oracle. Maps
    /// the backend error to [`StateAccessError`].
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

    /// Committed batch read that bypasses the commit oracle. The result is
    /// index-aligned to `batch`.
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

    /// Committed section scan that bypasses the commit oracle. Every backend
    /// yields the same stream type over [`StateAccessError`].
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
    ) -> Result<PublicationRows, StateReaderError> {
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
