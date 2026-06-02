//! Per-process Fjall client + per-Kafka-partition workspace.
//!
//! `FjallClient` and `FjallWorkspace` are the local workspace described by
//! `docs/keyed-state/design-summary.md`. One
//! `FjallClient` owns a shared `fjall::Keyspace` rooted at the configured
//! `cache_dir`; per Kafka partition assignment the client mints a
//! `FjallWorkspace` carrying two named Fjall partitions (`cache`,
//! `dirty_overlay`) tagged with an `AssignmentEpoch` so a fast
//! assign→revoke→assign cycle cannot collide even if `delete_partition` is
//! delayed.
//!
//! # Partition naming
//!
//! Fjall partition names are restricted to `[A-Za-z0-9_#$-]`, so Kafka
//! topic names (which may contain `.`) cannot be embedded verbatim. The
//! workspace hashes `(role, topic, partition, epoch)` with `xxh3_128`
//! and hex-encodes the result. The `value_<role>_` prefix lets the
//! startup sweep find and reap all `value_*` partitions regardless of
//! topic name.
//!
//! # Lifecycle
//!
//! On revocation, the workspace's `Drop` impl deletes its two named
//! partitions. The keyspace stays open for other Kafka partitions still
//! owned by the process. `PartitionHandle` is internally an `Arc`, so
//! cloning a handle into `delete_partition` is cheap and lets the Drop
//! impl read fields by reference.
//!
//! On process startup, `FjallClient::open` walks every existing
//! `value_*` partition and deletes them — design §"Local Workspace"
//! §"Process restart": "Delete old workspaces; Cassandra recovers truth."

use super::config::FjallConfiguration;
use super::error::FjallValueStoreError;
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::{CompactDateTime, CompactDateTimeError};
use crate::{Partition, Topic};
use educe::Educe;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;
use xxhash_rust::xxh3::xxh3_128;

const PARTITION_NAME_PREFIX: &str = "value_";
const CACHE_ROLE: &str = "cache";
const DIRTY_OVERLAY_ROLE: &str = "dirty_overlay";

/// Per-Kafka-partition workspace creation epoch.
///
/// Tagging a workspace with the moment it was created prevents a fast
/// `assign → revoke → assign` cycle from colliding on the same Kafka
/// `(topic, partition)` even when the prior `delete_partition` has not
/// yet landed on disk.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct AssignmentEpoch(CompactDateTime);

impl AssignmentEpoch {
    /// Creates an assignment epoch from a [`CompactDateTime`].
    #[must_use]
    pub fn new(time: CompactDateTime) -> Self {
        Self(time)
    }

    /// Returns the current wall-clock time as an [`AssignmentEpoch`].
    ///
    /// # Errors
    ///
    /// Returns [`CompactDateTimeError`] when the system clock is before
    /// [`CompactDateTime`]'s representable range.
    pub fn now() -> Result<Self, CompactDateTimeError> {
        Ok(Self(CompactDateTime::now()?))
    }

    /// Returns the wall-clock time the workspace was created at.
    #[must_use]
    pub fn as_datetime(self) -> CompactDateTime {
        self.0
    }

    /// Returns the raw epoch-seconds discriminator.
    #[must_use]
    pub fn epoch_seconds(self) -> u32 {
        self.0.epoch_seconds()
    }
}

/// Process-wide Fjall instance.
///
/// One `FjallClient` per consumer process. It owns the shared
/// `fjall::Keyspace` at `cache_dir` and is responsible for sweeping
/// crash-leftover partitions at startup.
#[derive(Educe)]
#[educe(Debug)]
pub struct FjallClient {
    #[educe(Debug(ignore))]
    keyspace: Arc<Keyspace>,
    cache_dir: PathBuf,
}

impl FjallClient {
    /// Opens the shared keyspace and wipes any `value_*` partitions left
    /// over from a prior process.
    ///
    /// Design §"Local Workspace" §"Process restart": "Delete old
    /// workspaces; Cassandra recovers truth."
    ///
    /// # Errors
    ///
    /// Returns [`FjallClientError::Engine`] when the keyspace cannot be
    /// opened or stale partitions cannot be swept.
    pub fn open(config: &FjallConfiguration) -> Result<Arc<Self>, FjallClientError> {
        let keyspace = Arc::new(Config::new(&config.cache_dir).open()?);
        sweep_orphaned(&keyspace)?;
        Ok(Arc::new(Self {
            keyspace,
            cache_dir: config.cache_dir.clone(),
        }))
    }

    /// Returns the shared keyspace.
    #[must_use]
    pub fn keyspace(&self) -> &Arc<Keyspace> {
        &self.keyspace
    }

    /// Returns the on-disk cache root.
    #[must_use]
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    /// Mints a fresh per-Kafka-partition workspace tagged with `epoch`.
    ///
    /// Opens two named Fjall partitions (`cache`, `dirty_overlay`) named via
    /// `xxh3_128(role, topic, partition, epoch)` so concurrent assign/revoke
    /// cycles cannot collide and arbitrary topic names cannot violate
    /// fjall's partition-name charset.
    ///
    /// # Errors
    ///
    /// Returns [`FjallValueStoreError::Engine`] when either partition cannot
    /// be opened.
    pub fn workspace(
        self: &Arc<Self>,
        topic: Topic,
        partition: Partition,
        epoch: AssignmentEpoch,
    ) -> Result<FjallWorkspace, FjallValueStoreError> {
        let cache_name = partition_name(CACHE_ROLE, topic, partition, epoch);
        let overlay_name = partition_name(DIRTY_OVERLAY_ROLE, topic, partition, epoch);

        let cache = self
            .keyspace
            .open_partition(&cache_name, PartitionCreateOptions::default())?;
        let overlay = self
            .keyspace
            .open_partition(&overlay_name, PartitionCreateOptions::default())?;

        Ok(FjallWorkspace {
            client: Arc::clone(self),
            epoch,
            topic,
            partition,
            cache,
            overlay,
        })
    }
}

/// Per-Kafka-partition workspace.
///
/// Owns the two named Fjall partitions for one `(topic, partition,
/// epoch)`. Drop deletes both; if delete fails it logs and continues
/// — the next process startup will reap the leftovers via
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
    cache: PartitionHandle,
    #[educe(Debug(ignore))]
    overlay: PartitionHandle,
}

impl FjallWorkspace {
    /// Returns the workspace creation epoch.
    #[must_use]
    pub fn epoch(&self) -> AssignmentEpoch {
        self.epoch
    }

    /// Returns the Kafka topic this workspace is bound to.
    #[must_use]
    pub fn topic(&self) -> Topic {
        self.topic
    }

    /// Returns the Kafka partition this workspace is bound to.
    #[must_use]
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Returns the shared keyspace.
    #[must_use]
    pub fn keyspace(&self) -> &Arc<Keyspace> {
        self.client.keyspace()
    }

    /// Returns the cache partition handle.
    #[must_use]
    pub fn cache_handle(&self) -> &PartitionHandle {
        &self.cache
    }

    /// Returns the dirty-overlay partition handle.
    #[must_use]
    pub fn dirty_overlay_handle(&self) -> &PartitionHandle {
        &self.overlay
    }
}

impl Drop for FjallWorkspace {
    fn drop(&mut self) {
        // `PartitionHandle` wraps an `Arc`, so cloning here is cheap and
        // matches fjall's expected ownership for `delete_partition`.
        for (role, handle) in [
            (CACHE_ROLE, &self.cache),
            (DIRTY_OVERLAY_ROLE, &self.overlay),
        ] {
            if let Err(err) = self.client.keyspace.delete_partition(handle.clone()) {
                warn!(
                    role,
                    topic = ?self.topic,
                    partition = self.partition,
                    epoch = ?self.epoch,
                    error = ?err,
                    "delete_partition failed on workspace drop; \
                     stale partition will be reaped on next startup"
                );
            }
        }
    }
}

/// Deletes every `value_*` partition currently in the keyspace.
///
/// Called by [`FjallClient::open`] to clear crash-leftover partitions at
/// process startup. Operating directly on the keyspace via
/// `open_partition` + `delete_partition` because fjall has no
/// "delete by name" shortcut.
fn sweep_orphaned(keyspace: &Arc<Keyspace>) -> Result<(), FjallClientError> {
    let names = keyspace.list_partitions();
    for name in names {
        if !name.starts_with(PARTITION_NAME_PREFIX) {
            continue;
        }
        let handle = keyspace.open_partition(&name, PartitionCreateOptions::default())?;
        if let Err(err) = keyspace.delete_partition(handle) {
            warn!(
                partition = %name,
                error = ?err,
                "sweep_orphaned failed to delete stale workspace partition; \
                 continuing — fjall is best-effort here"
            );
        }
    }
    Ok(())
}

fn partition_name(
    role: &str,
    topic: Topic,
    partition: Partition,
    epoch: AssignmentEpoch,
) -> String {
    let mut buf = Vec::with_capacity(
        role.len() + topic.as_ref().len() + 2 * size_of::<i32>() + size_of::<u32>(),
    );
    buf.extend_from_slice(role.as_bytes());
    buf.push(0);
    buf.extend_from_slice(topic.as_ref().as_bytes());
    buf.push(0);
    buf.extend_from_slice(&partition.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(&epoch.epoch_seconds().to_be_bytes());

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
