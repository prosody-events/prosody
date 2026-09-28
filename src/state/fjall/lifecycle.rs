//! Background creation and deletion of assignment keyspaces.
//!
//! fjall creates and deletes a keyspace under one process-wide lock, with many
//! `fsync` calls. One task runs these operations one at a time, each on a
//! blocking thread. Assignment, revocation, and the Tokio workers never wait
//! for them. Creates run before deletes: a create lets an assignment start
//! caching, and a delete only reclaims disk.
//!
//! The task stops when the client drops, and then [`Stopped`] resolves.
//! Consumer shutdown waits for it after the assignment caches are gone, so the
//! directory lock is free and a new client can open the same directory.
//! Deletes still in the queue stay on disk until that startup.

use super::{FjallCellCache, io};
use crate::state::manager::Stopped;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::sync::{Arc, OnceLock, Weak};
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
#[cfg(test)]
use tokio::sync::oneshot;
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

/// Work for the delete queue.
pub(super) enum Retirement {
    /// Deletes the keyspace.
    Keyspace(Keyspace),
    /// Replies when both queues are empty.
    #[cfg(test)]
    Barrier(oneshot::Sender<()>),
}

/// Queues its keyspace for deletion when dropped.
pub(super) struct Retire {
    /// `Some` until `drop` moves it into the queue.
    keyspace: Option<Keyspace>,
    queue: UnboundedSender<Retirement>,
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
        // means the task has stopped. The next startup deletes the keyspace.
        if let Some(keyspace) = self.keyspace.take() {
            drop(self.queue.send(Retirement::Keyspace(keyspace)));
        }
    }
}

/// Starts the lifecycle task for `database`, queues the deletion of `stale`,
/// and returns the create queue, the delete queue, and the task's stop signal.
/// The task creates each keyspace with `options`. The caller runs inside a
/// Tokio runtime.
///
/// The task stops when the returned create sender drops. The signal resolves
/// after the task has dropped every fjall handle it held.
pub(super) fn spawn(
    database: Database,
    options: KeyspaceCreateOptions,
    stale: Vec<Keyspace>,
) -> (
    UnboundedSender<Pending>,
    UnboundedSender<Retirement>,
    Stopped,
) {
    let (creates, create_rx) = unbounded_channel();
    let (retires, retire_rx) = unbounded_channel();
    for keyspace in stale {
        drop(retires.send(Retirement::Keyspace(keyspace)));
    }
    let (running, stopped) = Stopped::channel();
    let task = run(database, options, create_rx, retire_rx, retires.clone());
    tokio::spawn(async move {
        task.await;
        drop(running);
    });
    (creates, retires, stopped)
}

/// Runs queued creates and deletes, one at a time and creates first. Returns
/// when the create queue closes. The task holds a delete sender, so the delete
/// queue never closes first.
async fn run(
    database: Database,
    options: KeyspaceCreateOptions,
    mut creates: UnboundedReceiver<Pending>,
    mut retires: UnboundedReceiver<Retirement>,
    retire_queue: UnboundedSender<Retirement>,
) {
    loop {
        select! {
            biased;
            slot = creates.recv() => match slot {
                Some(slot) => create(&database, &options, slot, &retire_queue).await,
                None => return,
            },
            Some(work) = retires.recv() => match work {
                Retirement::Keyspace(keyspace) => delete(&database, keyspace).await,
                #[cfg(test)]
                Retirement::Barrier(done) if creates.is_empty() && retires.is_empty() => {
                    let _ = done.send(());
                }
                #[cfg(test)]
                barrier @ Retirement::Barrier(_) => drop(retire_queue.send(barrier)),
            },
        }
    }
}

/// Creates the keyspace for `slot` and fills the slot. A slot whose assignment
/// ended drops the new cache, which queues the keyspace's deletion. After a
/// failure the slot stays empty, so the assignment uses durable storage, as it
/// does after the cache is disabled.
async fn create(
    database: &Database,
    options: &KeyspaceCreateOptions,
    slot: Pending,
    retire_queue: &UnboundedSender<Retirement>,
) {
    if slot.strong_count() == 0 {
        return;
    }
    let opened = database.clone();
    let options = options.clone();
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
                queue: retire_queue.clone(),
            };
            let cache = FjallCellCache::for_keyspace(database.clone(), keyspace, retire);
            if let Some(slot) = slot.upgrade() {
                CacheSlot(slot).attach(cache);
            }
        }
        Err(error) => {
            warn!(%error, "keyed-state cache keyspace creation failed; the assignment uses durable storage");
        }
    }
}

/// Deletes `keyspace`. A failure leaves it for the next startup to delete.
async fn delete(database: &Database, keyspace: Keyspace) {
    let database = database.clone();
    if let Err(error) = io::blocking(move || database.delete_keyspace(keyspace)).await {
        warn!(%error, "keyed-state cache keyspace deletion failed; startup deletes it");
    }
}
