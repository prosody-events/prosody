//! State owned by one waiting request.

use crate::response::frame::ResponseFrame;
use crate::response::{ResponseDisposition, ResponseStatus};
use crate::subsystem::SubsystemName;
use bytes::BytesMut;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::mem::take;
use std::pin::pin;
use tokio::sync::Notify;
use tokio::time::Instant;

/// Subsystems a request awaits before the list uses the heap.
const INLINE_AWAITED: usize = 1;

/// One in-flight request.
///
/// Each request gets a new allocation. A stale reference cannot refer to a
/// later request because this value is never recycled.
pub(in crate::requester) struct PendingRequest {
    state: Mutex<PendingState>,
    notify: Notify,
    /// The sweep reads this immutable deadline without the state lock.
    pub(super) deadline: Instant,
    /// The waiting codec's `FORMAT_ID`. A `&'static str` keeps the arrival
    /// check free of storage and allocation. It belongs to the request
    /// because one listener serves requesters whose codecs differ.
    expects: &'static str,
}

/// The terminal status and the position list for one request.
struct PendingState {
    status: Status,
    awaited: SmallVec<[Awaited; INLINE_AWAITED]>,
}

/// A subsystem and the response that arrived for it.
pub(in crate::requester) struct Awaited {
    /// The subsystem for this position.
    pub(in crate::requester) name: SubsystemName,
    /// The first response accepted for this position.
    pub(in crate::requester) arrival: Option<Arrival>,
}

/// What arrived for one awaited position.
pub(in crate::requester) enum Arrival {
    /// A response in the format this request expects.
    Response {
        /// How the responder classified the result.
        status: ResponseStatus,
        /// The encoded response payload.
        payload: BytesMut,
    },
    /// A response written in a format this request does not read.
    FormatMismatch,
}

/// The lifecycle state of one pending request.
///
/// [`Open`](Self::Open) is the only state a request can leave. A transition out
/// of it is one way, so a second terminal transition changes nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::requester) enum Status {
    /// The request can accept responses.
    Open,
    /// Every awaited position has a response.
    Complete,
    /// The request deadline passed.
    TimedOut,
    /// The caller or a failed produce operation cancelled the request.
    Cancelled,
    /// Registry shutdown ended the request.
    ShuttingDown,
}

/// The status and positions taken from one finished request.
pub(in crate::requester) struct Finished {
    /// The final request status.
    pub(in crate::requester) status: Status,
    /// The positions in the caller's original order.
    pub(in crate::requester) awaited: SmallVec<[Awaited; INLINE_AWAITED]>,
}

impl PendingRequest {
    /// Creates one open request with one position per subsystem.
    pub(super) fn new(awaited: &[SubsystemName], expects: &'static str, deadline: Instant) -> Self {
        let mut positions = SmallVec::with_capacity(awaited.len());
        positions.extend(awaited.iter().cloned().map(|name| Awaited {
            name,
            arrival: None,
        }));
        Self {
            state: Mutex::new(PendingState {
                status: Status::Open,
                awaited: positions,
            }),
            notify: Notify::new(),
            deadline,
            expects,
        }
    }

    /// Ends an open request and takes its positions in one lock scope.
    ///
    /// A wait that ends with the request still open ended at the deadline, so
    /// the status this function writes is always [`Status::TimedOut`]. The take
    /// is unconditional, so only the first call receives the positions; a later
    /// call reads the same status and an empty list.
    pub(in crate::requester) fn finish(&self) -> Finished {
        let (finished, transitioned) = {
            let mut state = self.state.lock();
            let transitioned = state.status == Status::Open;
            if transitioned {
                state.status = Status::TimedOut;
            }
            let finished = Finished {
                status: state.status,
                awaited: take(&mut state.awaited),
            };
            (finished, transitioned)
        };
        if transitioned {
            self.notify.notify_waiters();
        }
        finished
    }

    /// Ends an open request without taking its positions.
    pub(super) fn close(&self, status: Status) -> bool {
        let transitioned = {
            let mut state = self.state.lock();
            if state.status == Status::Open {
                state.status = status;
                true
            } else {
                false
            }
        };
        if transitioned {
            self.notify.notify_waiters();
        }
        transitioned
    }

    /// Ends an open request only when no response has arrived.
    ///
    /// The emptiness test and transition share one lock scope. An accepted
    /// response cannot fall between them and get discarded.
    pub(in crate::requester) fn abandon_if_empty(&self, status: Status) -> bool {
        let transitioned = {
            let mut state = self.state.lock();
            if state.status != Status::Open
                || state
                    .awaited
                    .iter()
                    .any(|awaited| awaited.arrival.is_some())
            {
                false
            } else {
                state.status = status;
                true
            }
        };
        if transitioned {
            self.notify.notify_waiters();
        }
        transitioned
    }

    /// Stores the first response for its named subsystem.
    ///
    /// A frame written in another format fills its position with the refusal.
    /// One responder answers per subsystem, so no later frame can repair that
    /// position. The refusal is still evidence that the record reached a
    /// responder, so [`abandon_if_empty`](Self::abandon_if_empty) counts it and
    /// a failed delivery report does not fail the call.
    ///
    /// The caller releases the map guard before this function takes the state
    /// lock. This function releases the state lock before it notifies.
    pub(super) fn deposit(&self, frame: ResponseFrame) -> ResponseDisposition {
        let (disposition, completed) = {
            let mut state = self.state.lock();
            if state.status != Status::Open {
                return ResponseDisposition::ClosedRequest;
            }
            let Some(position) = state
                .awaited
                .iter_mut()
                .find(|awaited| awaited.name == frame.header.subsystem)
            else {
                return ResponseDisposition::UnexpectedSubsystem;
            };
            if position.arrival.is_some() {
                return ResponseDisposition::DuplicateSubsystem;
            }
            let disposition = if frame.format.to_str() == self.expects {
                position.arrival = Some(Arrival::Response {
                    status: frame.header.status,
                    payload: frame.payload,
                });
                ResponseDisposition::Accepted
            } else {
                position.arrival = Some(Arrival::FormatMismatch);
                ResponseDisposition::FormatMismatch
            };
            let completed = state
                .awaited
                .iter()
                .all(|awaited| awaited.arrival.is_some());
            if completed {
                state.status = Status::Complete;
            }
            (disposition, completed)
        };
        if completed {
            self.notify.notify_waiters();
        }
        disposition
    }

    /// Waits until the request reaches a terminal state.
    ///
    /// This function enables each notification before it checks the state.
    /// Therefore, a transition between registration and the check cannot lose
    /// its wakeup.
    pub(in crate::requester) async fn park(&self) {
        loop {
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            if !self.is_open() {
                return;
            }
            notified.await;
        }
    }

    /// Reports whether the request can still accept a response.
    fn is_open(&self) -> bool {
        self.state.lock().status == Status::Open
    }
}
