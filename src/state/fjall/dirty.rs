//! Fjall-backed dirty Value workspace.
//!
//! `FjallDirtyValueStore` implements [`ValueStore`] +
//! [`PendingOpSource<ValueKind>`] over a single named fjall partition:
//!
//! * `dirty_overlay` — one row per collection, holding the compacted final
//!   buffered op as the cache codec's tagged-cell (`0x01` = pending Set with
//!   payload, `0x00` = pending Clear, key-absent = no pending op).
//!
//! This mirrors
//! [`MemoryDirtyValueStore`](crate::state::memory::MemoryDirtyValueStore)
//! exactly: Value's last-writer-wins fold means only the final op matters, so
//! the store never keeps an op obviated by a later one. Each mutation is a
//! single atomic overlay insert (or, for
//! [`PendingOpSource::clear_pending_ops`], a single remove), making the
//! multi-partition desync class structurally impossible. The single overlay is
//! the K=Value specialization of the per-element keyed overlay that Map and
//! Deque-as-indexed-map will use at finer granularity.
//!
//! # Concurrency
//!
//! One `FjallDirtyValueStore` is minted per `(Kafka partition,
//! EventScopeId)`. The keyed-state middleware's per-key linearization
//! guarantees that at most one event handler is running for a given key
//! system-wide; concurrent events on different keys share the same Kafka
//! partition's workspace but cannot collide because the per-event
//! [`EventScopeId`] is baked into the overlay key.

use super::codec::{decode_cell, dirty_collection_key, encode_absent_cell, encode_present_cell};
use super::error::FjallValueStoreError;
use super::workspace::{AssignmentEpoch, FjallClient, FjallWorkspace};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::value::{PendingOpSource, StoredPayload, ValueKind, ValueOp, ValueStore};
use crate::state::{
    CollectionId, DirtyStoreFactory, DirtyStoreProvider, EventScopeId, PendingOps, Read,
};
use crate::timers::datetime::CompactDateTimeError;
use crate::{Partition, Topic};
use educe::Educe;
use fjall::PartitionHandle;
use std::num::NonZeroU64;
use std::option::IntoIter as OptionIntoIter;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::spawn_blocking;

/// Fjall-backed dirty Value workspace, scoped to one event.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallDirtyValueStore {
    #[educe(Debug(ignore))]
    overlay: PartitionHandle,
    scope: EventScopeId,
}

impl FjallDirtyValueStore {
    /// Creates a Fjall-backed dirty workspace bound to `scope`.
    #[must_use]
    pub fn new(overlay: PartitionHandle, scope: EventScopeId) -> Self {
        Self { overlay, scope }
    }

    /// Returns the event scope this store is bound to.
    #[must_use]
    pub fn scope(&self) -> EventScopeId {
        self.scope
    }
}

impl ValueStore for FjallDirtyValueStore {
    type Error = FjallValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<StoredPayload>, Self::Error> {
        let key = dirty_collection_key(self.scope, collection);
        let overlay = self.overlay.clone();
        let raw = spawn_blocking(move || overlay.get(key)).await??;
        decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        let key = dirty_collection_key(self.scope, collection);
        let cell = encode_present_cell(&payload)?;
        let overlay = self.overlay.clone();
        spawn_blocking(move || overlay.insert(key, cell.as_ref())).await??;
        Ok(())
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        let key = dirty_collection_key(self.scope, collection);
        let cell = encode_absent_cell();
        let overlay = self.overlay.clone();
        spawn_blocking(move || overlay.insert(key, cell.as_ref())).await??;
        Ok(())
    }
}

impl PendingOpSource<ValueKind> for FjallDirtyValueStore {
    type Error = FjallValueStoreError;
    type Ops<'a> = OptionIntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        let key = dirty_collection_key(self.scope, collection);
        let op = match decode_cell(self.overlay.get(key)?.as_deref())? {
            Read::Present(payload) => Some(ValueOp::Set { payload }),
            Read::Absent => Some(ValueOp::Clear),
            Read::Unknown => None,
        };
        Ok(op.map(|op| PendingOps {
            count: NonZeroU64::MIN,
            ops: Some(op).into_iter(),
        }))
    }

    fn clear_pending_ops(&self, collection: &CollectionId<ValueKind>) -> Result<(), Self::Error> {
        let key = dirty_collection_key(self.scope, collection);
        self.overlay.remove(key)?;
        Ok(())
    }
}

/// Per-partition provider for [`FjallDirtyValueStore`].
///
/// Owns the partition-scoped [`FjallWorkspace`]; cloning the provider
/// clones the `Arc<FjallWorkspace>`, so all clones share the same
/// per-partition state.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallDirtyValueStoreProvider {
    #[educe(Debug(ignore))]
    workspace: Arc<FjallWorkspace>,
}

impl FjallDirtyValueStoreProvider {
    /// Wraps an opened workspace as a provider.
    #[must_use]
    pub fn new(workspace: Arc<FjallWorkspace>) -> Self {
        Self { workspace }
    }
}

impl DirtyStoreProvider<ValueKind> for FjallDirtyValueStoreProvider {
    type Store = FjallDirtyValueStore;

    fn for_scope(&self, scope: EventScopeId) -> FjallDirtyValueStore {
        FjallDirtyValueStore::new(self.workspace.dirty_overlay_handle().clone(), scope)
    }
}

/// Process-wide factory that mints per-partition
/// [`FjallDirtyValueStoreProvider`]s by opening a fresh
/// [`FjallWorkspace`] for each Kafka partition assignment.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallDirtyValueStoreFactory {
    #[educe(Debug(ignore))]
    client: Arc<FjallClient>,
}

impl FjallDirtyValueStoreFactory {
    /// Creates a factory that mints workspaces from `client`.
    #[must_use]
    pub fn new(client: Arc<FjallClient>) -> Self {
        Self { client }
    }
}

impl DirtyStoreFactory<ValueKind> for FjallDirtyValueStoreFactory {
    type Error = FjallFactoryError;
    type Provider = FjallDirtyValueStoreProvider;

    fn for_partition(
        &self,
        topic: Topic,
        partition: Partition,
    ) -> Result<Self::Provider, Self::Error> {
        let epoch = AssignmentEpoch::now().map_err(FjallFactoryError::Epoch)?;
        let workspace = self
            .client
            .workspace(topic, partition, epoch)
            .map_err(FjallFactoryError::Workspace)?;
        Ok(FjallDirtyValueStoreProvider::new(Arc::new(workspace)))
    }
}

/// Errors raised by [`FjallDirtyValueStoreFactory::for_partition`].
#[derive(Debug, Error)]
pub enum FjallFactoryError {
    /// The wall-clock read for [`AssignmentEpoch::now`] failed.
    #[error("assignment epoch lookup failed")]
    Epoch(#[source] CompactDateTimeError),

    /// Opening the workspace partitions failed.
    #[error("fjall workspace open failed")]
    Workspace(#[source] FjallValueStoreError),
}

impl ClassifyError for FjallFactoryError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Epoch(e) => e.classify_error(),
            Self::Workspace(e) => e.classify_error(),
        }
    }
}
