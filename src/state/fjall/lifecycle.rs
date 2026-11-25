//! Background creation and deletion of assignment keyspaces.
//!
//! fjall creates and deletes a keyspace under one database-wide lock, with many
//! `fsync` calls. One task runs these operations one at a time, each on a
//! blocking thread. Assignment, revocation, and the Tokio workers never wait
//! for them. Creates run before deletes: a create lets an assignment start
//! caching, and a delete only reclaims disk.
//!
//! The task also divides the memtable budget. It gives each new keyspace an
//! equal share for every partition that it holds or that waits for a create.
//! A keyspace keeps its share until it is deleted, because fjall cannot change
//! the memtable size of a live keyspace.
//!
//! The task holds only weak senders. It stops after the client and every
//! assignment cache are gone, then drops the database on a blocking thread.
//! fjall removes the whole database directory when that last handle drops.

use super::{FjallCellCache, io};
use crate::ByteSize;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::sync::{Arc, OnceLock, Weak};
use tokio::select;
use tokio::sync::mpsc::{
    UnboundedReceiver, UnboundedSender, WeakUnboundedSender, unbounded_channel,
};
use tokio::task::spawn_blocking;
use tracing::warn;
use uuid::Uuid;

/// A request to create the keyspace for one slot. It holds the slot weakly, so
/// an assignment that ends first cancels its create.
pub(super) type Pending = Weak<OnceLock<FjallCellCache>>;

/// One assignment's cell cache.
///
/// The slot starts empty. The lifecycle task fills it once, when the
/// assignment's keyspace exists, and it never empties again. A new keyspace
/// starts empty, so a slot filled partway through an assignment is a cold cache
/// for every key.
#[derive(Clone, Default)]
pub(crate) struct CacheSlot(Arc<OnceLock<FjallCellCache>>);

/// Queues its keyspace for deletion when dropped.
pub(super) struct Retire {
    /// `Some` until `drop` moves it into the queue.
    keyspace: Option<Keyspace>,
    queue: UnboundedSender<Keyspace>,
}

impl CacheSlot {
    /// Returns the attached cache, or `None` before the keyspace exists or
    /// after the cache is disabled. Each operation calls this once and uses
    /// that answer throughout.
    #[must_use]
    pub(crate) fn active(&self) -> Option<&FjallCellCache> {
        self.attached().filter(|cache| !cache.is_disabled())
    }

    /// Returns the attached cache, disabled or not.
    #[must_use]
    pub(crate) fn attached(&self) -> Option<&FjallCellCache> {
        self.0.get()
    }

    /// Fills an empty slot. A filled slot keeps its first cache.
    pub(crate) fn attach(&self, cache: FjallCellCache) {
        drop(self.0.set(cache));
    }

    /// Returns the create request for this slot.
    pub(super) fn pending(&self) -> Pending {
        Arc::downgrade(&self.0)
    }
}

impl From<FjallCellCache> for CacheSlot {
    fn from(cache: FjallCellCache) -> Self {
        Self(Arc::new(OnceLock::from(cache)))
    }
}

impl Drop for Retire {
    fn drop(&mut self) {
        // Moving the handle leaves the last one to the lifecycle task, so fjall
        // removes the keyspace directory on a blocking thread. A failed send
        // means the task has stopped. The database drop removes the keyspace.
        if let Some(keyspace) = self.keyspace.take() {
            drop(self.queue.send(keyspace));
        }
    }
}

/// Starts the lifecycle task for `database` and returns its create and delete
/// queues. The task creates each keyspace with `options` and a share of the
/// memtable `budget`. The caller runs inside a Tokio runtime.
pub(super) fn spawn(
    database: Database,
    options: KeyspaceCreateOptions,
    budget: ByteSize,
) -> (UnboundedSender<Pending>, UnboundedSender<Keyspace>) {
    let (creates, create_rx) = unbounded_channel();
    let (retires, retire_rx) = unbounded_channel();
    tokio::spawn(run(
        database,
        options,
        budget,
        create_rx,
        retire_rx,
        retires.downgrade(),
    ));
    (creates, retires)
}

/// Runs queued creates and deletes, one at a time and creates first. Returns
/// after the client and every assignment cache are gone and both queues are
/// empty. It then drops the last database handle on a blocking thread.
///
/// `live` counts the keyspaces that exist. A keyspace that waits for deletion
/// still counts, so the count never falls below the partitions held.
async fn run(
    database: Database,
    options: KeyspaceCreateOptions,
    budget: ByteSize,
    mut creates: UnboundedReceiver<Pending>,
    mut retires: UnboundedReceiver<Keyspace>,
    retire_queue: WeakUnboundedSender<Keyspace>,
) {
    let mut live: usize = 0;
    loop {
        select! {
            biased;
            Some(slot) = creates.recv() => {
                let partitions = live + creates.len() + 1;
                let options = options.clone().max_memtable_size(memtable_share(budget, partitions));
                if create(&database, options, slot, &retire_queue).await {
                    live += 1;
                }
            }
            Some(keyspace) = retires.recv() => {
                delete(&database, keyspace).await;
                live = live.saturating_sub(1);
            }
            else => break,
        }
    }
    // The last database handle removes the whole directory when it drops.
    spawn_blocking(move || drop(database));
}

/// Creates the keyspace for `slot` and fills the slot. A slot whose assignment
/// ended drops the new cache, which queues the keyspace's deletion. After a
/// failure the slot stays empty, so the assignment uses durable storage, as it
/// does after the cache is disabled.
///
/// Returns whether a keyspace was created. Each created keyspace later reaches
/// the delete queue.
pub(super) async fn create(
    database: &Database,
    options: KeyspaceCreateOptions,
    slot: Pending,
    retire_queue: &WeakUnboundedSender<Keyspace>,
) -> bool {
    let Some(queue) = retire_queue.upgrade().filter(|_| slot.strong_count() > 0) else {
        return false;
    };
    let opened = database.clone();
    let created = io::blocking(move || {
        let mut name = Uuid::encode_buffer();
        let name = Uuid::new_v4().simple().encode_lower(&mut name);
        opened.keyspace(name, || options)
    })
    .await;

    match created {
        Ok(keyspace) => {
            let retire = Retire {
                keyspace: Some(keyspace.clone()),
                queue,
            };
            let cache = FjallCellCache::for_keyspace(database.clone(), keyspace, retire);
            if let Some(slot) = slot.upgrade() {
                CacheSlot(slot).attach(cache);
            }
            true
        }
        Err(error) => {
            warn!(%error, "keyed-state cache keyspace creation failed; the assignment uses durable storage");
            false
        }
    }
}

/// Returns one keyspace's share of the memtable `budget` when `partitions`
/// keyspaces share it. The shares never sum to more than the budget.
pub(super) fn memtable_share(budget: ByteSize, partitions: usize) -> u64 {
    budget.get() / u64::try_from(partitions.max(1)).unwrap_or(u64::MAX)
}

/// Deletes `keyspace`. A failure leaves it for the database drop to remove.
pub(super) async fn delete(database: &Database, keyspace: Keyspace) {
    let database = database.clone();
    if let Err(error) = io::blocking(move || database.delete_keyspace(keyspace)).await {
        warn!(%error, "keyed-state cache keyspace deletion failed; the database drop removes it");
    }
}
