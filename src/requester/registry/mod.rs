//! Bounded storage where request waiters and response arrivals meet.

mod pending;

pub(in crate::requester) use self::pending::{
    Arrival, Awaited, Finished, PendingRequest, Status, Terminal,
};
use crate::requester::ValidatedRequest;
use crate::requester::config::RequesterConfiguration;
use crate::response::frame::ResponseFrame;
use crate::response::{RequestId, ResponseDisposition};
use crate::subsystem::SubsystemName;
use ahash::RandomState;
use parking_lot::Mutex;
use scc::HashMap;
use smallvec::SmallVec;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::{Arc, Weak};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval};
use tracing::warn;
use validator::{Validate, ValidationErrors};

/// Ids one sweep pass carries between its scan and removals.
pub(in crate::requester) const SWEEP_BATCH: usize = 64;

/// Where a waiting request and an arriving response meet.
///
/// One entry holds one request's results. Whether a position is filled is read
/// off that position, so no counter and no bitset can disagree with it.
///
/// # What bounds the memory
///
/// Admission bounds how many entries exist. The validated awaited limit bounds
/// how many positions one entry holds. The validated response ceiling bounds
/// each payload, and the peer transport applies it when it decodes a frame.
/// The configuration refuses the product of the three. So what an operator
/// commits to is checked at startup, rather than left to three numbers that are
/// each plausible alone.
///
/// # Removal paths, all of them
///
/// 1. resolution — [`Registration`]'s `Drop`, once every position is filled;
/// 2. the request's own deadline, through the same guard;
/// 3. cancellation — the same guard again, when the call is dropped unfinished;
/// 4. [`shutdown`](Self::shutdown), which closes every live request at once;
/// 5. [`sweep`](Self::sweep), the only path that does not need the waiting call
///    to run again.
///
/// A call that is never polled and never dropped still holds its own reference
/// to the request it registered. The sweep reclaims the map record and the
/// admission permit; whatever that call's frame retains is the caller's.
pub(crate) struct PendingRegistry {
    entries: HashMap<RequestId, Entry, RandomState>,
    admission: Arc<Semaphore>,
    caps: Caps,
    grace: Duration,
    closed: AtomicBool,
    /// A `OnceLock` cannot replace this: [`shutdown`](Self::shutdown) awaits
    /// the handle, and that needs ownership a shared reference cannot give.
    sweeper: Mutex<Option<JoinHandle<()>>>,
}

/// One map record owns both the request and its admission permit.
struct Entry {
    request: Arc<PendingRequest>,
    /// Held only for its `Drop`. Removing the record is therefore what returns
    /// the request's capacity, so a waiter that never runs again costs nothing
    /// once the sweep has removed it.
    _permit: OwnedSemaphorePermit,
}

/// Limits that every request validates before registration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Caps {
    /// Most subsystems one request may await.
    pub(crate) max_awaited: usize,
    /// Longest timeout one request may use.
    pub(crate) max_timeout: Duration,
}

/// Removes one request from the registry when its call ends, however it ends.
///
/// A call that returns, times out, or is dropped leaves no record behind,
/// because the removal is this value's `Drop`.
pub(crate) struct Registration {
    registry: Arc<PendingRegistry>,
    id: RequestId,
    request: Arc<PendingRequest>,
}

impl PendingRegistry {
    /// Builds a registry and starts its deadline sweep.
    ///
    /// Call this function inside a Tokio runtime. The sweep runs as a spawned
    /// task and holds only a weak registry reference.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::InvalidConfiguration`] when a limit is invalid.
    pub(crate) fn new(config: &RequesterConfiguration) -> Result<Arc<Self>, RegistryError> {
        config.validate()?;
        let registry = Arc::new(Self {
            entries: HashMap::with_capacity_and_hasher(
                config.max_in_flight,
                RandomState::default(),
            ),
            admission: Arc::new(Semaphore::new(config.max_in_flight)),
            caps: Caps {
                max_awaited: config.max_awaited,
                max_timeout: config.max_timeout,
            },
            grace: config.sweep_grace,
            closed: AtomicBool::new(false),
            sweeper: Mutex::new(None),
        });
        let sweep = spawn_sweep(Arc::downgrade(&registry), config.sweep_grace);
        *registry.sweeper.lock() = Some(sweep);
        Ok(registry)
    }

    /// Returns the request limits owned by this registry.
    pub(crate) const fn caps(&self) -> Caps {
        self.caps
    }

    /// Registers one request before its Kafka record is produced.
    ///
    /// The [`ValidatedRequest`] witness is the enforcement of the caps this
    /// registry documents: nothing else can mint one.
    ///
    /// # Errors
    ///
    /// Returns [`Admission`] when capacity is full, shutdown has started, or
    /// a generated id already exists.
    pub(in crate::requester) fn register(
        self: &Arc<Self>,
        request: ValidatedRequest<'_>,
        expects: &'static str,
    ) -> Result<Registration, Admission> {
        let ValidatedRequest {
            subsystems,
            timeout,
        } = request;
        let (id, request) = self.insert(subsystems, expects, timeout)?;
        Ok(Registration {
            registry: Arc::clone(self),
            id,
            request,
        })
    }

    /// Hands one arriving response to the call waiting for it.
    ///
    /// A frame written in another format fills its position with the refusal,
    /// so the caller learns at once instead of waiting out its deadline.
    ///
    /// The lookup clones the request out and releases the map guard, so the
    /// entry lock is never taken under it. That order is fixed: map guard
    /// released before the entry lock, entry lock released before map removal,
    /// and never a wakeup while the entry lock is held.
    pub(crate) fn accept(&self, frame: ResponseFrame) -> ResponseDisposition {
        let Some(request) = self
            .entries
            .read_sync(&frame.header.request, |_, entry| Arc::clone(&entry.request))
        else {
            return ResponseDisposition::UnknownRequest;
        };
        request.deposit(frame)
    }

    /// Removes every entry at least one grace period past its deadline.
    ///
    /// Each scan reads only immutable deadlines into a stack batch. It releases
    /// every map guard before it takes an entry lock. The deadline stays on the
    /// request rather than beside the map record: one scan per grace period
    /// does not pay for a second copy of it.
    pub(crate) fn sweep(&self, now: Instant) {
        loop {
            let mut expired = SmallVec::<[RequestId; SWEEP_BATCH]>::new();
            self.entries.iter_sync(|id, entry| {
                if now.saturating_duration_since(entry.request.deadline) >= self.grace {
                    expired.push(*id);
                }
                expired.len() < SWEEP_BATCH
            });
            if expired.is_empty() {
                return;
            }
            let full_batch = expired.len() == SWEEP_BATCH;
            for id in expired {
                if let Some(request) = self
                    .entries
                    .read_sync(&id, |_, entry| Arc::clone(&entry.request))
                {
                    self.close_and_remove(id, &request, Terminal::TimedOut);
                }
            }
            if !full_batch {
                return;
            }
        }
    }

    /// Refuses new requests, closes all entries, and stops the sweep task.
    pub(crate) async fn shutdown(&self) {
        self.closed.store(true, Release);
        loop {
            let mut batch = SmallVec::<[(RequestId, Arc<PendingRequest>); SWEEP_BATCH]>::new();
            self.entries.iter_sync(|id, entry| {
                batch.push((*id, Arc::clone(&entry.request)));
                batch.len() < SWEEP_BATCH
            });
            if batch.is_empty() {
                break;
            }
            for (id, request) in batch {
                self.close_and_remove(id, &request, Terminal::ShuttingDown);
            }
        }
        let sweeper = self.sweeper.lock().take();
        if let Some(sweeper) = sweeper {
            sweeper.abort();
            if let Err(error) = sweeper.await
                && !error.is_cancelled()
            {
                warn!(%error, "the requester deadline sweep did not stop cleanly");
            }
        }
    }

    /// Number of entries stored by this registry.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Number of requests that admission can accept now.
    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.admission.available_permits()
    }

    /// Reports whether one request still holds a map record.
    #[cfg(test)]
    pub(crate) fn contains(&self, id: RequestId) -> bool {
        self.entries.read_sync(&id, |_, _| ()).is_some()
    }

    /// Registers an entry without a waiter guard.
    ///
    /// Tests use this constructor to reproduce a waiter that never removes its
    /// entry. It does not leak a guard or shared request.
    #[cfg(test)]
    pub(crate) fn register_unguarded(
        self: &Arc<Self>,
        awaited: &[SubsystemName],
        expects: &'static str,
        timeout: Duration,
    ) -> Result<RequestId, Admission> {
        self.insert(awaited, expects, timeout).map(|(id, _)| id)
    }

    /// Inserts one map record and reverses an insert that races shutdown.
    fn insert(
        self: &Arc<Self>,
        awaited: &[SubsystemName],
        expects: &'static str,
        timeout: Duration,
    ) -> Result<(RequestId, Arc<PendingRequest>), Admission> {
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| Admission::Exhausted)?;
        if self.closed.load(Acquire) {
            return Err(Admission::ShuttingDown);
        }
        let id = RequestId::new();
        let request = Arc::new(PendingRequest::new(
            awaited,
            expects,
            Instant::now() + timeout,
        ));
        let entry = Entry {
            request: Arc::clone(&request),
            _permit: permit,
        };
        if self.entries.insert_sync(id, entry).is_err() {
            return Err(Admission::IdInUse);
        }
        // The check that closes the race: it reverses an insert that overlapped
        // a drain already in progress.
        if self.closed.load(Acquire) {
            self.close_and_remove(id, &request, Terminal::ShuttingDown);
            return Err(Admission::ShuttingDown);
        }
        Ok((id, request))
    }

    /// Closes one exact request and then removes its map record.
    ///
    /// The identity test is what makes the removal exact. Nothing is recycled,
    /// so it is not a fence against a stale reference: it keeps a late guard
    /// from removing a record another request owns under the same id.
    fn close_and_remove(&self, id: RequestId, request: &Arc<PendingRequest>, status: Terminal) {
        request.close(status);
        drop(
            self.entries
                .remove_if_sync(&id, |entry| Arc::ptr_eq(&entry.request, request)),
        );
    }
}

impl Registration {
    /// Returns this registration's request state.
    pub(in crate::requester) const fn request(&self) -> &Arc<PendingRequest> {
        &self.request
    }

    /// Returns the id written into the Kafka request headers.
    pub(in crate::requester) const fn id(&self) -> RequestId {
        self.id
    }

    /// Returns the deadline shared by the waiter and the sweep.
    pub(in crate::requester) fn deadline(&self) -> Instant {
        self.request.deadline
    }

    /// Ends the request and takes its positions in one operation.
    pub(in crate::requester) fn finish(&self) -> Finished {
        self.request.finish()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry
            .close_and_remove(self.id, &self.request, Terminal::Cancelled);
    }
}

/// Starts the task that removes entries after their grace period.
fn spawn_sweep(registry: Weak<PendingRegistry>, grace: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticks = interval(grace);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let Some(registry) = registry.upgrade() else {
                return;
            };
            if registry.closed.load(Acquire) {
                return;
            }
            registry.sweep(Instant::now());
        }
    })
}

/// Why the pending registry could not start.
#[derive(Debug, Error)]
pub(crate) enum RegistryError {
    /// A configured limit is outside its valid range.
    #[error("requester configuration is invalid: {0:#}")]
    InvalidConfiguration(#[from] ValidationErrors),
}

/// Why one request could not enter the registry.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum Admission {
    /// Every request permit is in use.
    #[error("request admission is exhausted")]
    Exhausted,
    /// Registry shutdown has started.
    #[error("the requester is shutting down")]
    ShuttingDown,
    /// The generated request id already names a live request.
    #[error("the request id is already in use")]
    IdInUse,
}
