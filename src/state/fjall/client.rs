//! The process-wide fjall database and its assignment keyspaces.
//!
//! One [`FjallClient`] owns the `fjall::Database` at the configured
//! `cache_dir`. Each partition assignment gets a [`CacheSlot`] from
//! [`FjallClient::slot`]. The lifecycle task creates the assignment's
//! keyspace in the background and fills the slot. When the assignment ends,
//! the last cache handle queues the keyspace for deletion.
//!
//! Each keyspace takes a fresh v4 UUID name, so a new keyspace starts empty and
//! no assignment can open another's data. The keyspace holds the
//! committed-value cache and the admission markers.
//!
//! The cache has no durability guarantee. Cassandra provisional cells and
//! collection evidence are the recovery source. The process owns everything
//! in `cache_dir`, so [`FjallClient::open`] queues every existing keyspace for
//! deletion. A failed delete costs only disk until a later startup.

use super::CacheSlot;
use super::lifecycle::{self, Pending, Retirement};
use crate::ByteSize;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::config::KeyedStateConfiguration;
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, KeyspaceCreateOptions};
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::mpsc::UnboundedSender;
#[cfg(test)]
use tokio::sync::oneshot::{self, error::RecvError};
use tokio::task::{JoinError, spawn_blocking};

/// Process-wide Fjall instance and the queues of its lifecycle task.
///
/// One `FjallClient` per consumer process. Clones share the task.
#[derive(Clone)]
pub(crate) struct FjallClient {
    creates: UnboundedSender<Pending>,
    /// Keeps the delete queue open while the client lives. The lifecycle task
    /// gives each new keyspace a sender from it.
    retires: UnboundedSender<Retirement>,
    #[cfg(test)]
    database: Database,
}

impl FjallClient {
    /// Opens the shared database at the configured `cache_dir`, starts the
    /// lifecycle task, and queues every existing keyspace for deletion. Startup
    /// does not wait for the deletes.
    ///
    /// fjall locks `cache_dir` before it opens the database. So no other live
    /// client's keyspaces can be under this directory.
    ///
    /// # Errors
    ///
    /// Returns [`FjallClientError`] when another live client holds
    /// `cache_dir` or the database cannot be opened.
    pub async fn open(config: &KeyedStateConfiguration) -> Result<Self, FjallClientError> {
        let mut builder = Database::builder(&config.cache_dir);
        if let Some(bytes) = config.owned_cache_size {
            builder = builder.cache_size(bytes.get());
        }
        let options = keyspace_options(config.memtable_size);
        let path = config.cache_dir.clone();
        let opened = options.clone();
        let (database, stale) = spawn_blocking(move || {
            let database = builder.open().map_err(|error| match error {
                fjall::Error::Locked => FjallClientError::CacheDirInUse { path },
                other => FjallClientError::Engine(other),
            })?;
            let stale = database
                .list_keyspace_names()
                .iter()
                .map(|name| database.keyspace(name, || opened.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, FjallClientError>((database, stale))
        })
        .await??;

        let (creates, retires) = lifecycle::spawn(database.clone(), options);
        let client = Self {
            creates,
            retires,
            #[cfg(test)]
            database,
        };
        for keyspace in stale {
            drop(client.retires.send(Retirement::Keyspace(keyspace)));
        }
        Ok(client)
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
        let (done, settled) = oneshot::channel();
        drop(self.retires.send(Retirement::Barrier(done)));
        settled.await
    }
}

/// Creation options shared by every keyed-state fjall keyspace.
///
/// Cells are stored raw; fjall compresses data blocks at flush/compaction.
/// fjall 3.x configures compression with a per-level [`CompressionPolicy`]
/// rather than a single type; pinning every level to LZ4 preserves the prior
/// behavior, documents the intent, and guards against a future change to
/// fjall's default policy.
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

    #[error(
        "cache_dir {path:?} is already in use by another live prosody client; each consumer needs \
         its own cache_dir"
    )]
    CacheDirInUse { path: PathBuf },
}

impl ClassifyError for FjallClientError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Engine(_) | Self::BlockingTaskJoin(_) => ErrorCategory::Transient,
            // The same configuration will keep colliding with the other
            // client's lock; retrying cannot succeed.
            Self::CacheDirInUse { .. } => ErrorCategory::Permanent,
        }
    }
}
