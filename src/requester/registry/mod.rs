//! Response waiters keyed by request and subsystem.

use crate::requester::RequestError;
use crate::response::frame::ResponseFrame;
use crate::response::{RequestId, ResponseDisposition};
use crate::subsystem::SubsystemName;
use ahash::RandomState;
use opentelemetry::global::meter;
use opentelemetry::metrics::UpDownCounter;
use scc::HashMap;
use smallvec::SmallVec;
use std::error::Error;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

const INLINE_AWAITED: usize = 2;

#[cfg(test)]
pub(crate) mod tests;

type WaiterKey = (RequestId, SubsystemName);
type Waiters = HashMap<WaiterKey, oneshot::Sender<ResponseFrame>, RandomState>;

static PENDING: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    meter("prosody")
        .i64_up_down_counter("prosody.peer.requests.pending")
        .with_description("Requests this process is waiting for answers to")
        .with_unit("{request}")
        .build()
});

/// Response waiters for one process.
///
/// Each sender has two states. It is present and waiting, or it is absent and
/// consumed. Removal is the only transition.
pub(crate) struct PendingRegistry {
    waiters: Waiters,
    closed: AtomicBool,
}

type Waiter = oneshot::Receiver<ResponseFrame>;

/// Removes all response senders that remain when one request ends.
pub(crate) struct Registration {
    registry: Arc<PendingRegistry>,
    id: RequestId,
    keys: SmallVec<[WaiterKey; INLINE_AWAITED]>,
    receivers: SmallVec<[Waiter; INLINE_AWAITED]>,
    deadline: Instant,
}

impl PendingRegistry {
    /// Builds an empty waiter registry.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            waiters: HashMap::with_hasher(RandomState::default()),
            closed: AtomicBool::new(false),
        })
    }

    /// Registers one sender for each awaited subsystem.
    pub(in crate::requester) fn register<E: Error>(
        self: &Arc<Self>,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<Registration, RequestError<E>> {
        Self::validate_request(subsystems)?;
        if self.closed.load(Acquire) {
            return Err(RequestError::ShuttingDown);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(RequestError::DeadlineOutOfRange)?;

        let (id, keys, receivers) = loop {
            let id = RequestId::new();
            let mut keys = SmallVec::with_capacity(subsystems.len());
            let mut receivers = SmallVec::with_capacity(subsystems.len());
            let mut collision = false;
            for name in subsystems {
                let (sender, receiver) = oneshot::channel();
                let key = (id, name.clone());
                if self.waiters.insert_sync(key.clone(), sender).is_err() {
                    collision = true;
                    break;
                }
                keys.push(key);
                receivers.push(receiver);
            }
            if !collision {
                break (id, keys, receivers);
            }
            self.remove(&keys);
        };
        if self.closed.load(Acquire) {
            self.remove(&keys);
            return Err(RequestError::ShuttingDown);
        }
        PENDING.add(1, &[]);
        Ok(Registration {
            registry: Arc::clone(self),
            id,
            keys,
            receivers,
            deadline,
        })
    }

    /// Delivers one frame to its exact waiter.
    pub(crate) fn accept(&self, frame: ResponseFrame) -> ResponseDisposition {
        let key = (frame.header.request, frame.header.subsystem.clone());
        let Some((_, sender)) = self.waiters.remove_sync(&key) else {
            return ResponseDisposition::UnknownRequest;
        };
        if sender.send(frame).is_ok() {
            ResponseDisposition::Accepted
        } else {
            ResponseDisposition::ClosedRequest
        }
    }

    pub(crate) fn close_admission(&self) {
        self.closed.store(true, Release);
    }

    /// Closes admission and wakes all request tasks.
    pub(crate) fn terminate(&self) {
        self.close_admission();
        self.waiters.retain_sync(|_, _| false);
    }

    pub(in crate::requester) fn is_closed(&self) -> bool {
        self.closed.load(Acquire)
    }

    fn validate_request<E: Error>(subsystems: &[SubsystemName]) -> Result<(), RequestError<E>> {
        if subsystems.is_empty() {
            return Err(RequestError::NoSubsystems);
        }
        for i in 0..subsystems.len() {
            for j in (i + 1)..subsystems.len() {
                if subsystems[i] == subsystems[j] {
                    return Err(RequestError::DuplicateSubsystem {
                        name: subsystems[i].clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn remove(&self, keys: &[WaiterKey]) {
        for key in keys {
            drop(self.waiters.remove_sync(key));
        }
    }
}

impl Registration {
    pub(in crate::requester) const fn id(&self) -> RequestId {
        self.id
    }

    pub(in crate::requester) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(in crate::requester) fn take_waiters(&mut self) -> SmallVec<[Waiter; INLINE_AWAITED]> {
        mem::take(&mut self.receivers)
    }

    pub(in crate::requester) fn is_closed(&self) -> bool {
        self.registry.is_closed()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.remove(&self.keys);
        PENDING.add(-1, &[]);
    }
}
