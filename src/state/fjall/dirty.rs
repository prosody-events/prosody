//! Fjall-backed dirty Value workspace.
//!
//! `FjallDirtyValueStore` implements [`ValueStore`] +
//! [`PendingOpSource<ValueKind>`] over three named fjall partitions:
//!
//! * `dirty_ops`     — one row per buffered op, keyed by `[scope][seq]`.
//! * `dirty_overlay` — one row per collection, encodes the "next read"
//!   visibility using the cache codec's tagged-cell format.
//! * `dirty_meta`    — one row per collection, value is `[next_seq u64 LE]`.
//!
//! All three partitions share the same `[16-byte scope-collection prefix]`
//! key shape so prefix-scans, prefix-deletes, and overlay/meta point reads
//! are uniform.
//!
//! Every write batches the three partition updates inside a single
//! [`fjall::Batch`] so a crash mid-write cannot desync ops vs. overlay vs.
//! meta. Ops are MsgPack-encoded (see [`super::codec::DIRTY_OP_ENCODING`]).
//!
//! # Concurrency
//!
//! One `FjallDirtyValueStore` is minted per `(Kafka partition,
//! EventScopeId)`. The keyed-state middleware's per-key linearization
//! guarantees that at most one event handler is running for a given key
//! system-wide; concurrent events on different keys share the same Kafka
//! partition's workspace but cannot collide because the per-event
//! [`EventScopeId`] is baked into the prefix.

use super::codec::{
    DIRTY_OP_ENCODING, decode_dirty_meta, dirty_collection_key, dirty_ops_key, encode_absent_cell,
    encode_dirty_meta, encode_present_cell, scope_collection_prefix,
};
use super::error::FjallValueStoreError;
use super::workspace::{AssignmentEpoch, FjallClient, FjallWorkspace};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::encoding::{decode_payload, encode_payload};
use crate::state::value::{PendingOpSource, StoredPayload, ValueKind, ValueOp, ValueStore};
use crate::state::{
    CollectionId, DirtyStoreFactory, DirtyStoreProvider, EventScopeId, PendingOps, Read,
};
use crate::timers::datetime::CompactDateTimeError;
use crate::{Partition, Topic};
use educe::Educe;
use fjall::{Keyspace, PartitionHandle};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::vec::IntoIter;
use thiserror::Error;
use tokio::task::spawn_blocking;

/// Fjall-backed dirty Value workspace, scoped to one event.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct FjallDirtyValueStore {
    #[educe(Debug(ignore))]
    keyspace: Arc<Keyspace>,
    #[educe(Debug(ignore))]
    ops: PartitionHandle,
    #[educe(Debug(ignore))]
    overlay: PartitionHandle,
    #[educe(Debug(ignore))]
    meta: PartitionHandle,
    scope: EventScopeId,
}

impl FjallDirtyValueStore {
    /// Creates a Fjall-backed dirty workspace bound to `scope`.
    ///
    /// The three partition handles must all sit in the same `keyspace` so
    /// the batched three-partition write commits atomically.
    #[must_use]
    pub fn new(
        keyspace: Arc<Keyspace>,
        ops: PartitionHandle,
        overlay: PartitionHandle,
        meta: PartitionHandle,
        scope: EventScopeId,
    ) -> Self {
        Self {
            keyspace,
            ops,
            overlay,
            meta,
            scope,
        }
    }

    /// Returns the event scope this store is bound to.
    #[must_use]
    pub fn scope(&self) -> EventScopeId {
        self.scope
    }

    /// Reads the collection's `next_seq` sequence counter, defaulting to 0
    /// for an absent meta row.
    fn read_meta(meta: &PartitionHandle, key: &[u8; 16]) -> Result<u64, FjallValueStoreError> {
        match meta.get(key)? {
            None => Ok(0),
            Some(bytes) => decode_dirty_meta(bytes.as_ref()),
        }
    }

    fn append_op_sync(
        &self,
        collection: &CollectionId<ValueKind>,
        op: &ValueOp,
        overlay_cell: &bytes::Bytes,
    ) -> Result<(), FjallValueStoreError> {
        let collection_key = dirty_collection_key(self.scope, collection);
        let next_seq = Self::read_meta(&self.meta, &collection_key)?;
        let ops_key = dirty_ops_key(self.scope, collection, next_seq);
        let op_bytes = encode_payload(op, DIRTY_OP_ENCODING)?;

        let mut batch = self.keyspace.batch();
        batch.insert(&self.ops, ops_key.as_ref(), op_bytes.as_ref());
        batch.insert(
            &self.overlay,
            collection_key.as_ref(),
            overlay_cell.as_ref(),
        );
        let new_meta = encode_dirty_meta(next_seq.wrapping_add(1));
        batch.insert(&self.meta, collection_key.as_ref(), new_meta.as_ref());
        batch.commit()?;
        Ok(())
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
        super::codec::decode_cell(raw.as_deref())
    }

    async fn set<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
        payload: StoredPayload,
    ) -> Result<(), Self::Error> {
        let overlay_cell = encode_present_cell(&payload)?;
        let op = ValueOp::Set { payload };
        let store = self.clone();
        let collection = collection.clone();
        spawn_blocking(move || store.append_op_sync(&collection, &op, &overlay_cell)).await?
    }

    async fn clear<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<(), Self::Error> {
        let overlay_cell = encode_absent_cell();
        let store = self.clone();
        let collection = collection.clone();
        spawn_blocking(move || store.append_op_sync(&collection, &ValueOp::Clear, &overlay_cell))
            .await?
    }
}

impl PendingOpSource<ValueKind> for FjallDirtyValueStore {
    type Error = FjallValueStoreError;
    type Ops<'a> = IntoIter<ValueOp>;

    fn pending_ops<'a>(
        &'a self,
        collection: &'a CollectionId<ValueKind>,
    ) -> Result<Option<PendingOps<Self::Ops<'a>>>, Self::Error> {
        let prefix = scope_collection_prefix(self.scope, collection);
        let mut ops: Vec<ValueOp> = Vec::new();
        for entry in self.ops.prefix(prefix) {
            let (_key, value) = entry?;
            let op: ValueOp = decode_payload(value.as_ref(), DIRTY_OP_ENCODING)?;
            ops.push(op);
        }
        let Some(count) = NonZeroU64::new(ops.len() as u64) else {
            return Ok(None);
        };
        Ok(Some(PendingOps {
            count,
            ops: ops.into_iter(),
        }))
    }

    fn clear_pending_ops(&self, collection: &CollectionId<ValueKind>) -> Result<(), Self::Error> {
        let prefix = scope_collection_prefix(self.scope, collection);
        let collection_key = dirty_collection_key(self.scope, collection);
        let mut batch = self.keyspace.batch();
        for entry in self.ops.prefix(prefix) {
            let (key, _value) = entry?;
            batch.remove(&self.ops, key.as_ref());
        }
        batch.remove(&self.overlay, collection_key.as_ref());
        batch.remove(&self.meta, collection_key.as_ref());
        batch.commit()?;
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
        FjallDirtyValueStore::new(
            Arc::clone(self.workspace.keyspace()),
            self.workspace.dirty_ops_handle().clone(),
            self.workspace.dirty_overlay_handle().clone(),
            self.workspace.dirty_meta_handle().clone(),
            scope,
        )
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
    Workspace(#[source] super::error::FjallValueStoreError),
}

impl ClassifyError for FjallFactoryError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Epoch(e) => e.classify_error(),
            Self::Workspace(e) => e.classify_error(),
        }
    }
}
