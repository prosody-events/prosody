//! Per-process Fjall client + per-Kafka-partition workspace.
//!
//! `FjallClient` and `FjallWorkspace` are the local workspace described by
//! `docs/keyed-state/design-summary.md`. One
//! `FjallClient` owns a shared `fjall::Database` rooted at the configured
//! `cache_dir`; per Kafka partition assignment the client mints a
//! `FjallWorkspace` carrying one named Fjall keyspace (`cache`) tagged with
//! an `AssignmentEpoch` so a fast assign→revoke→assign cycle cannot collide
//! even if `delete_keyspace` is delayed.
//!
//! # Keyspace naming
//!
//! Fjall keyspace names are restricted to `[A-Za-z0-9_#$-]`, so Kafka
//! topic names (which may contain `.`) cannot be embedded verbatim. The
//! workspace hashes `(role, topic, partition, epoch)` with `xxh3_128`
//! and hex-encodes the result. The `value_<role>_` prefix lets the
//! startup sweep find and reap all `value_*` keyspaces regardless of
//! topic name.
//!
//! # Lifecycle
//!
//! On revocation, the workspace's `Drop` impl deletes its named
//! keyspace. The database stays open for other Kafka partitions still
//! owned by the process. `Keyspace` is internally an `Arc`, so
//! cloning a handle into `delete_keyspace` is cheap and lets the Drop
//! impl read fields by reference.
//!
//! On process startup, `FjallClient::open` walks every existing
//! `value_*` keyspace and deletes them — design §"Local Workspace"
//! §"Process restart": "Delete old workspaces; Cassandra recovers truth."

use super::config::FjallConfiguration;
use super::error::FjallCellCacheError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::{Partition, Topic};
use educe::Educe;
use fjall::config::CompressionPolicy;
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tracing::warn;
use xxhash_rust::xxh3::xxh3_128;

/// Source of process-monotonic [`AssignmentEpoch`] values.
static NEXT_EPOCH: AtomicU64 = AtomicU64::new(0);

const PARTITION_NAME_PREFIX: &str = "value_";
const CACHE_ROLE: &str = "cache";
const INDEX_ROLE: &str = "index";

/// Per-Kafka-partition workspace creation epoch.
///
/// A process-monotonic counter, minted by [`Self::mint`] from a global
/// [`AtomicU64`]. Tagging each workspace with a strictly increasing value
/// guarantees that a fast `assign → revoke → assign` cycle on the same Kafka
/// `(topic, partition)` produces *distinct* fjall keyspace names even when
/// the prior `delete_keyspace` has not yet landed on disk — so the new
/// workspace never aliases the live handle of the one being torn down.
///
/// Wall-clock granularity (the previous, one-second design) could not make
/// that guarantee: two assignments in the same second collided. A counter is
/// also deterministic, so the collision-avoidance invariant is testable.
///
/// Cross-restart collisions are irrelevant: the counter resets to `0` each
/// process, but [`FjallClient::open`] sweeps *every* `value_*` keyspace at
/// startup, so no stale keyspace from a prior process survives to alias a
/// fresh epoch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct AssignmentEpoch(u64);

impl AssignmentEpoch {
    /// Mints the next process-monotonic assignment epoch.
    #[must_use]
    pub fn mint() -> Self {
        Self(NEXT_EPOCH.fetch_add(1, Ordering::Relaxed))
    }
}

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
    /// Opens the shared database and wipes any `value_*` keyspaces left
    /// over from a prior process.
    ///
    /// Design §"Local Workspace" §"Process restart": "Delete old
    /// workspaces; Cassandra recovers truth."
    ///
    /// # Errors
    ///
    /// Returns [`FjallClientError::Engine`] when the database cannot be
    /// opened or stale keyspaces cannot be swept.
    pub fn open(config: &FjallConfiguration) -> Result<Arc<Self>, FjallClientError> {
        let database = Database::builder(&config.cache_dir).open()?;
        sweep_orphaned(&database)?;
        Ok(Arc::new(Self { database }))
    }

    /// Returns the shared database.
    #[must_use]
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Mints a fresh per-Kafka-partition workspace tagged with `epoch`.
    ///
    /// Opens two named Fjall keyspaces — `cache` (committed-value mirror) and
    /// `index` (the warm provisional-coordinate index + scan coverage) — each
    /// named via `xxh3_128(role, topic, partition, epoch)` so concurrent
    /// assign/revoke cycles cannot collide and arbitrary topic names cannot
    /// violate fjall's keyspace-name charset. Both are cold at a fresh epoch
    /// and dropped together at revocation.
    ///
    /// # Errors
    ///
    /// Returns [`FjallCellCacheError::Engine`] when a keyspace cannot be
    /// opened.
    pub fn workspace(
        self: &Arc<Self>,
        topic: Topic,
        partition: Partition,
        epoch: AssignmentEpoch,
    ) -> Result<FjallWorkspace, FjallCellCacheError> {
        let cache_name = partition_name(CACHE_ROLE, topic, partition, epoch);
        let index_name = partition_name(INDEX_ROLE, topic, partition, epoch);

        let cache = self.database.keyspace(&cache_name, keyspace_options)?;
        let index = self.database.keyspace(&index_name, keyspace_options)?;

        Ok(FjallWorkspace {
            client: Arc::clone(self),
            epoch,
            topic,
            partition,
            cache,
            index,
        })
    }
}

/// Per-Kafka-partition workspace.
///
/// Owns the named Fjall `cache` and `index` keyspaces for one `(topic,
/// partition, epoch)`. Drop deletes both; if a delete fails it logs and
/// continues — the next process startup will reap the leftover via
/// [`FjallClient::open`].
#[derive(Educe)]
#[educe(Debug)]
pub struct FjallWorkspace {
    #[educe(Debug(ignore))]
    client: Arc<FjallClient>,
    epoch: AssignmentEpoch,
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

    /// Returns the warm-index keyspace handle (provisional coordinates +
    /// scan coverage).
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
                    epoch = ?self.epoch,
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

/// Creation options shared by every keyed-state fjall keyspace (the `cache`
/// keyspace and the startup sweep's reopen).
///
/// Cells are stored raw; fjall compresses data blocks at flush/compaction.
/// fjall 3.x configures compression with a per-level [`CompressionPolicy`]
/// rather than a single type; pinning every level to LZ4 preserves the prior
/// behavior, documents the intent, and guards against a future change to
/// fjall's default policy.
fn keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
}

fn partition_name(
    role: &str,
    topic: Topic,
    partition: Partition,
    epoch: AssignmentEpoch,
) -> String {
    let mut buf = Vec::with_capacity(
        role.len() + topic.as_ref().len() + 2 * size_of::<i32>() + size_of::<u64>(),
    );
    buf.extend_from_slice(role.as_bytes());
    buf.push(0);
    buf.extend_from_slice(topic.as_ref().as_bytes());
    buf.push(0);
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(&epoch.0.to_be_bytes());

    let hash = xxh3_128(&buf);
    format!("{PARTITION_NAME_PREFIX}{role}_{hash:032x}")
}

/// Errors raised by [`FjallClient`].
#[derive(Debug, Error)]
pub enum FjallClientError {
    /// Underlying fjall engine error.
    #[error(transparent)]
    Engine(#[from] fjall::Error),
}

impl ClassifyError for FjallClientError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Engine(_) => ErrorCategory::Transient,
        }
    }
}
