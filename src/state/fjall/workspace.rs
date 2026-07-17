//! Per-process Fjall client + per-Kafka-partition workspace.
//!
//! `FjallClient` and `FjallWorkspace` are the process-local write-through
//! cache workspace backing keyed-state cells. One `FjallClient` owns a shared
//! `fjall::Database` rooted at the configured `cache_dir`; per Kafka partition
//! assignment the client mints a `FjallWorkspace` carrying one `cache` and one
//! `index` keyspace.
//!
//! # Keyspace naming
//!
//! Each workspace's keyspaces are named `value_cache_<uuid>` /
//! `value_index_<uuid>` from a fresh UUID (v4) minted at assignment,
//! hex-encoded to fit fjall's `[A-Za-z0-9_#$-]` name charset. Minting fresh per
//! assignment is what makes a workspace born cold — see
//! [`FjallClient::workspace`] for that invariant. Here it means a stale
//! keyspace, left behind by a crash or a failed delete, is unreachable garbage;
//! the `value_` prefix exists only so the startup sweep can find and reap it.
//!
//! `Uuid::new_v4()` sources OS entropy and can only fail by panicking inside
//! the dependency — treated like any other infallible-by-platform dependency.
//!
//! # Lifecycle
//!
//! On revocation, the workspace's `Drop` impl deletes both named keyspaces.
//! The database stays open for other Kafka partitions still owned by the
//! process. `Keyspace` is internally an `Arc`, so cloning a handle into
//! `delete_keyspace` is cheap and lets the Drop impl read fields by reference.
//!
//! On process startup, `FjallClient::open` deletes every existing `value_*`
//! keyspace: the cache carries no durability guarantee — Cassandra provisional
//! cells plus the commit oracle are the recovery source — so leftovers are
//! reclaimed disk, never recovered state. A failed sweep or drop-delete costs
//! only disk until the next successful sweep or the cache volume's teardown.

use super::error::FjallCellCacheError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::{Partition, Topic};
use educe::Educe;
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

const PARTITION_NAME_PREFIX: &str = "value_";
const CACHE_ROLE: &str = "cache";
const INDEX_ROLE: &str = "index";

/// Process-wide Fjall instance.
///
/// One `FjallClient` per consumer process. It owns the shared
/// `fjall::Database` at `cache_dir` and is responsible for sweeping
/// crash-leftover keyspaces at startup.
#[derive(Educe)]
#[educe(Debug)]
pub struct FjallClient {
    #[educe(Debug(ignore))]
    database: Database,
}

impl FjallClient {
    /// Opens the shared database at `cache_dir` and best-effort deletes any
    /// `value_*` keyspaces left over from a prior process. The sweep is disk
    /// reclamation, not a correctness gate: keyspace names are never
    /// re-derived (see [`Self::workspace`]), so a leftover that survives a
    /// failed delete is unreachable and simply retried at the next startup.
    ///
    /// fjall takes an exclusive lock on `cache_dir` before recovery runs.
    /// That lock is load-bearing: it is what makes the sweep safe to run at
    /// all — no other live client's keyspaces can be under this directory
    /// when the sweep deletes everything `value_*`.
    ///
    /// `cache_dir` arrives already resolved: the environment-variable
    /// resolution, defaulting, and validation live on
    /// [`KeyedStateConfiguration`](crate::state::config::KeyedStateConfiguration).
    /// Production deployments mount it at an emptyDir-type volume; on
    /// partition revocation the per-partition keyspaces are dropped.
    ///
    /// `cache_size_bytes` is fjall's block-cache capacity in bytes; `None`
    /// leaves fjall to pick its own default (the call is simply omitted).
    ///
    /// # Errors
    ///
    /// Returns [`FjallClientError::CacheDirInUse`] when another live client
    /// holds `cache_dir`, and [`FjallClientError::Engine`] when the database
    /// cannot be opened.
    pub fn open(
        cache_dir: &Path,
        cache_size_bytes: Option<NonZeroU64>,
    ) -> Result<Arc<Self>, FjallClientError> {
        let mut builder = Database::builder(cache_dir);
        if let Some(bytes) = cache_size_bytes {
            builder = builder.cache_size(bytes.get());
        }
        let database = builder.open().map_err(|error| match error {
            fjall::Error::Locked => FjallClientError::CacheDirInUse {
                path: cache_dir.to_path_buf(),
            },
            other => FjallClientError::Engine(other),
        })?;
        sweep_orphaned(&database)?;
        Ok(Arc::new(Self { database }))
    }

    /// Returns the shared database.
    #[must_use]
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Mints a fresh per-Kafka-partition workspace.
    ///
    /// Opens two named Fjall keyspaces — `cache` (committed-value mirror) and
    /// `index` (the warm provisional-coordinate index and latches) —
    /// sharing one fresh UUID (v4). **A workspace's keyspaces are born cold**:
    /// their names are minted fresh per assignment and never derived from
    /// anything, so no workspace can ever open another assignment's data.
    /// Both are dropped together at revocation.
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError::Engine`] when a keyspace cannot be
    /// opened.
    pub fn workspace(
        self: &Arc<Self>,
        topic: Topic,
        partition: Partition,
    ) -> Result<FjallWorkspace, FjallCellCacheError> {
        let uuid = Uuid::new_v4();
        let cache = self
            .database
            .keyspace(&partition_name(CACHE_ROLE, uuid), keyspace_options)?;
        let index = self
            .database
            .keyspace(&partition_name(INDEX_ROLE, uuid), keyspace_options)?;

        Ok(FjallWorkspace {
            client: Arc::clone(self),
            uuid,
            topic,
            partition,
            cache,
            index,
        })
    }
}

/// Per-Kafka-partition workspace.
///
/// Owns the named Fjall `cache` and `index` keyspaces of one partition
/// assignment. Drop deletes both; if a delete fails it logs and continues —
/// the leftover is reaped on next startup by [`FjallClient::open`].
#[derive(Educe)]
#[educe(Debug)]
pub struct FjallWorkspace {
    #[educe(Debug(ignore))]
    client: Arc<FjallClient>,
    uuid: Uuid,
    topic: Topic,
    partition: Partition,
    #[educe(Debug(ignore))]
    cache: Keyspace,
    #[educe(Debug(ignore))]
    index: Keyspace,
}

impl FjallWorkspace {
    /// Returns the shared database.
    #[must_use]
    pub fn database(&self) -> &Database {
        self.client.database()
    }

    /// Returns the committed-value cache keyspace handle.
    #[must_use]
    pub fn cache_handle(&self) -> &Keyspace {
        &self.cache
    }

    /// Returns the warm-index keyspace handle (provisional coordinates,
    /// and the cold-seed and marker-presence latches).
    #[must_use]
    pub fn index_handle(&self) -> &Keyspace {
        &self.index
    }
}

impl Drop for FjallWorkspace {
    fn drop(&mut self) {
        // `Keyspace` wraps an `Arc`, so cloning here is cheap and matches
        // fjall's expected ownership for `delete_keyspace`. Both keyspaces are
        // deleted; a failure on either logs and continues (startup sweep reaps).
        for (role, keyspace) in [(CACHE_ROLE, &self.cache), (INDEX_ROLE, &self.index)] {
            if let Err(err) = self.client.database.delete_keyspace(keyspace.clone()) {
                warn!(
                    role,
                    topic = ?self.topic,
                    partition = self.partition,
                    uuid = %self.uuid,
                    error = ?err,
                    "delete_keyspace failed on workspace drop; \
                     stale keyspace will be reaped on next startup"
                );
            }
        }
    }
}

/// Deletes every `value_*` keyspace currently in the database.
///
/// Called by [`FjallClient::open`] to clear crash-leftover keyspaces at
/// process startup. Operating directly on the database via
/// `keyspace` + `delete_keyspace` because fjall has no
/// "delete by name" shortcut.
fn sweep_orphaned(database: &Database) -> Result<(), FjallClientError> {
    let names = database.list_keyspace_names();
    for name in names {
        if !name.starts_with(PARTITION_NAME_PREFIX) {
            continue;
        }
        let handle = database.keyspace(&name, keyspace_options)?;
        if let Err(err) = database.delete_keyspace(handle) {
            warn!(
                keyspace = %name,
                error = ?err,
                "sweep_orphaned failed to delete stale workspace keyspace; \
                 continuing — fjall is best-effort here"
            );
        }
    }
    Ok(())
}

/// Creation options shared by every keyed-state fjall keyspace.
///
/// Cells are stored raw; fjall compresses data blocks at flush/compaction.
/// fjall 3.x configures compression with a per-level [`CompressionPolicy`]
/// rather than a single type; pinning every level to LZ4 preserves the prior
/// behavior, documents the intent, and guards against a future change to
/// fjall's default policy.
pub(super) fn keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
}

fn partition_name(role: &str, uuid: Uuid) -> String {
    format!("{PARTITION_NAME_PREFIX}{role}_{}", uuid.simple())
}

/// Errors raised by [`FjallClient`].
#[derive(Debug, Error)]
pub enum FjallClientError {
    /// Underlying fjall engine error.
    #[error(transparent)]
    Engine(#[from] fjall::Error),

    /// Another live prosody client owns this `cache_dir` — each consumer
    /// needs its own.
    #[error(
        "cache_dir {path:?} is already in use by another live prosody client; each consumer needs \
         its own cache_dir"
    )]
    CacheDirInUse {
        /// The contended cache directory.
        path: PathBuf,
    },
}

impl ClassifyError for FjallClientError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Engine(_) => ErrorCategory::Transient,
            // The same configuration will keep colliding with the other
            // client's lock; retrying cannot succeed.
            Self::CacheDirInUse { .. } => ErrorCategory::Permanent,
        }
    }
}
