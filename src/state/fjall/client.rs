//! The client's fjall database and its assignment keyspaces.
//!
//! One [`FjallClient`] owns a `fjall::Database` in a fresh directory under
//! `cache_dir`. Each partition assignment gets a [`CacheSlot`] from
//! [`FjallClient::slot`]. The lifecycle task creates the assignment's
//! keyspace in the background and fills the slot. When the assignment ends,
//! the last cache handle queues the keyspace for deletion. The task stops after
//! the client and every assignment cache are gone.
//!
//! Each keyspace takes a fresh v4 UUID name, so a new keyspace starts empty and
//! no assignment can open another's data. The keyspace holds the
//! committed-value cache and the admission markers.
//!
//! The cache has no durability guarantee. Cassandra provisional cells and
//! collection evidence are the recovery source.

use super::CacheSlot;
use super::lifecycle::{self, Pending, Retirement};
use crate::ByteSize;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::config::KeyedStateConfiguration;
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, KeyspaceCreateOptions};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
#[cfg(test)]
use tokio::sync::oneshot::{self, error::RecvError};
use tokio::task::{JoinError, spawn_blocking};
use uuid::Uuid;

/// A fjall database and the queues of its lifecycle task.
///
/// Clones share the database and the task.
#[derive(Clone)]
pub(crate) struct FjallClient {
    creates: UnboundedSender<Pending>,
    /// Keeps the delete queue open while the client lives. The lifecycle task
    /// gives each new keyspace a sender from it.
    _retires: UnboundedSender<Retirement>,
    #[cfg(test)]
    database: Database,
}

impl FjallClient {
    /// Opens an empty database in a fresh directory under `cache_dir` and
    /// starts the lifecycle task.
    ///
    /// The directory name is a new v4 UUID, so no other client can use it.
    /// fjall removes the directory when the last database handle drops.
    ///
    /// # Errors
    ///
    /// Returns [`FjallClientError`] when the database cannot be opened.
    pub async fn open(config: &KeyedStateConfiguration) -> Result<Self, FjallClientError> {
        let path = config.cache_dir.join(Uuid::new_v4().simple().to_string());
        let mut builder = Database::builder(path).temporary(true);
        if let Some(bytes) = config.owned_cache_size {
            builder = builder.cache_size(bytes.get());
        }
        let database = spawn_blocking(move || builder.open()).await??;

        let (creates, retires) =
            lifecycle::spawn(database.clone(), keyspace_options(config.memtable_size));
        Ok(Self {
            creates,
            _retires: retires,
            #[cfg(test)]
            database,
        })
    }

    /// Returns an empty slot for a new assignment and queues the creation of
    /// its keyspace. If the lifecycle task has stopped, the slot stays empty
    /// and the assignment uses durable storage.
    #[must_use]
    pub(crate) fn slot(&self) -> CacheSlot {
        let slot = CacheSlot::default();
        drop(self.creates.send(slot.pending()));
        slot
    }

    /// Returns the shared database.
    #[cfg(test)]
    pub(crate) fn database(&self) -> &Database {
        &self.database
    }

    /// Waits until the lifecycle task has no queued work.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] when the lifecycle task has stopped.
    #[cfg(test)]
    pub(crate) async fn settled(&self) -> Result<(), RecvError> {
        let Self {
            _retires: retires, ..
        } = self;
        let (done, settled) = oneshot::channel();
        drop(retires.send(Retirement::Barrier(done)));
        settled.await
    }
}

/// Creation options shared by every keyed-state fjall keyspace.
///
/// Cells are stored raw. fjall compresses data blocks at flush and
/// compaction, with LZ4 at every level. The explicit [`CompressionPolicy`]
/// keeps LZ4 if fjall changes its default.
///
/// `memtable_size` is the size at which fjall flushes the keyspace's memtable.
/// `None` keeps fjall's default.
pub(super) fn keyspace_options(memtable_size: Option<ByteSize>) -> KeyspaceCreateOptions {
    let mut options = KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4));
    if let Some(bytes) = memtable_size {
        options = options.max_memtable_size(bytes.get());
    }
    options
}

/// Internal errors raised while opening the local keyed-state cache.
#[derive(Debug, Error)]
pub(crate) enum FjallClientError {
    #[error(transparent)]
    Engine(#[from] fjall::Error),

    #[error("the blocking task that opened the cache failed: {0}")]
    BlockingTaskJoin(#[from] JoinError),
}

impl ClassifyError for FjallClientError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Engine(_) | Self::BlockingTaskJoin(_) => ErrorCategory::Transient,
        }
    }
}
