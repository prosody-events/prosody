//! Pending responses keyed by request and subsystem.

use crate::peer::metrics::PeerMetrics;
use crate::peer::requester::RequestError;
use crate::peer::response::frame::ResponseFrame;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::{RequestId, ResponseDisposition};
use crate::subsystem::SubsystemName;
use ahash::RandomState;
use scc::HashMap;
use smallvec::SmallVec;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};
use tokio::sync::oneshot;
use tokio::time::Instant;

const INLINE_AWAITED: usize = 2;

#[cfg(test)]
pub(crate) mod tests;

type ResponseKey = (RequestId, SubsystemName);
type FrameSender = oneshot::Sender<ResponseFrame>;
type FrameReceiver = oneshot::Receiver<ResponseFrame>;
type FrameReceivers = SmallVec<[FrameReceiver; INLINE_AWAITED]>;
pub(super) type IndexedFrameReceivers = SmallVec<[(usize, FrameReceiver); INLINE_AWAITED]>;
type PendingSenders = HashMap<ResponseKey, FrameSender, RandomState>;

/// Pending response channels for one process.
///
/// Each key has at most one sender. Response delivery, registration drop, and
/// shutdown remove senders. No operation restores a removed sender.
pub(crate) struct PendingRegistry {
    senders: PendingSenders,
    closed: AtomicBool,
    metrics: PeerMetrics,
}

/// Owns one request's receivers and removes its remaining senders on drop.
pub(crate) struct Registration {
    pending: PendingRequest,
    receivers: FrameReceivers,
}

/// Removes one request's remaining response senders on drop.
pub(super) struct PendingRequest {
    registry: Arc<PendingRegistry>,
    id: RequestId,
    keys: SmallVec<[ResponseKey; INLINE_AWAITED]>,
    deadline: Instant,
}

impl PendingRegistry {
    /// Builds an empty pending response registry.
    pub(crate) fn new() -> Arc<Self> {
        Self::with_metrics(PeerMetrics::default())
    }

    /// Builds an empty registry with the specified peer instruments.
    pub(crate) fn with_metrics(metrics: PeerMetrics) -> Arc<Self> {
        Arc::new(Self {
            senders: HashMap::with_hasher(RandomState::default()),
            closed: AtomicBool::new(false),
            metrics,
        })
    }

    /// The peer instruments that this registry updates.
    pub(crate) const fn metrics(&self) -> &PeerMetrics {
        &self.metrics
    }

    /// Registers one sender for each awaited subsystem.
    pub(in crate::peer::requester) fn register<E: Error>(
        self: &Arc<Self>,
        subsystems: &[SubsystemName],
        deadline: RequestDeadline,
    ) -> Result<Registration, RequestError<E>> {
        if subsystems.is_empty() {
            return Err(RequestError::NoSubsystems);
        }
        if self.closed.load(Acquire) {
            return Err(RequestError::ShuttingDown);
        }
        let (id, keys, receivers) = loop {
            let id = RequestId::new();
            let mut keys = SmallVec::with_capacity(subsystems.len());
            let mut receivers = SmallVec::with_capacity(subsystems.len());
            let mut collision = false;
            for name in subsystems {
                let (sender, receiver) = oneshot::channel();
                let key = (id, name.clone());
                if self.senders.insert_sync(key.clone(), sender).is_err() {
                    if keys.iter().any(|(_, present)| present == name) {
                        self.remove(&keys);
                        return Err(RequestError::DuplicateSubsystem { name: name.clone() });
                    }
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
        self.metrics.requests_pending.add(1, &[]);
        Ok(Registration {
            pending: PendingRequest {
                registry: Arc::clone(self),
                id,
                keys,
                deadline: deadline.expires_at(),
            },
            receivers,
        })
    }

    /// Delivers one frame to its exact pending request.
    pub(crate) fn accept(&self, frame: ResponseFrame) -> ResponseDisposition {
        let key = (frame.header.request, frame.header.subsystem.clone());
        let Some((_, sender)) = self.senders.remove_sync(&key) else {
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
        self.senders.retain_sync(|_, _| false);
    }

    fn remove(&self, keys: &[ResponseKey]) {
        for key in keys {
            drop(self.senders.remove_sync(key));
        }
    }
}

impl Registration {
    pub(in crate::peer::requester) const fn id(&self) -> RequestId {
        self.pending.id
    }

    pub(super) fn into_parts(self) -> (PendingRequest, FrameReceivers) {
        (self.pending, self.receivers)
    }
}

impl PendingRequest {
    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.registry.remove(&self.keys);
        self.registry.metrics.requests_pending.add(-1, &[]);
    }
}
