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
//!
//! # Blocking I/O
//!
//! The async [`ValueStore`] methods (`get`/`set`/`clear`) dispatch their
//! fjall I/O through [`tokio::task::spawn_blocking`] via
//! [`cell_io`], the same path the cache uses. The
//! [`PendingOpSource`] methods (`pending_ops`/`clear_pending_ops`) cannot:
//! that trait is synchronous, so they call fjall directly off the caller's
//! thread. Each is a single point `get`/`remove` against the overlay cell
//! that was just written in the same event, so the synchronous call resolves
//! against the memtable on the hot path rather than blocking on disk.

use super::cell_io;
use super::codec::{decode_cell, dirty_collection_key, encode_absent_cell, encode_present_cell};
use super::error::FjallValueStoreError;
use super::workspace::FjallWorkspace;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::value::{PendingOpSource, ValueKind, ValueOp, ValueStore};
use crate::state::{CollectionId, DirtyStoreProvider, EventScopeId, PendingOps, Read};
use bytes::Bytes;
use educe::Educe;
use fjall::PartitionHandle;
use std::option::IntoIter as OptionIntoIter;
use std::sync::Arc;
use thiserror::Error;

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
}

impl ValueStore for FjallDirtyValueStore {
    type Error = FjallValueStoreError;

    async fn get<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Read<Bytes>, Self::Error> {
        let raw =
            cell_io::read_cell(&self.overlay, dirty_collection_key(self.scope, collection)).await?;
        decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: Bytes,
    ) -> Result<(), Self::Error> {
        cell_io::write_cell(
            &self.overlay,
            dirty_collection_key(self.scope, collection),
            encode_present_cell(&payload)?,
        )
        .await
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        cell_io::write_cell(
            &self.overlay,
            dirty_collection_key(self.scope, collection),
            encode_absent_cell(),
        )
        .await
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
        Ok(op.map(PendingOps::single))
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

/// Errors raised when minting a per-partition
/// [`FjallDirtyValueStoreProvider`] (see the production state-backend
/// factory in [`crate::state::production`]).
#[derive(Debug, Error)]
pub enum FjallFactoryError {
    /// Opening the workspace partitions failed.
    #[error("fjall workspace open failed")]
    Workspace(#[source] FjallValueStoreError),
}

impl ClassifyError for FjallFactoryError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Workspace(e) => e.classify_error(),
        }
    }
}
