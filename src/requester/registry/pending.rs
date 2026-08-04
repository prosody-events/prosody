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
    /// A response whose payload is over this process's response ceiling.
    TooLarge,
}

/// A state a request may be moved into from outside.
///
/// [`Status::Open`] and [`Status::Complete`] are absent by design: a request
/// never reopens, and only an arrival that fills the last position completes
/// one. So a caller cannot write either of them, and the one-way rule needs no
/// runtime check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::requester) enum Terminal {
    /// The request deadline passed.
    TimedOut,
    /// The caller or a failed produce operation cancelled the request.
    Cancelled,
    /// Registry shutdown ended the request.
    ShuttingDown,
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
    /// call reads the same status and an empty list. Only
    /// [`Registration::finish`](super::Registration::finish) reaches this, and
    /// one call owns one registration.
    pub(super) fn finish(&self) -> Finished {
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
    pub(super) fn close(&self, status: Terminal) -> bool {
        let transitioned = {
            let mut state = self.state.lock();
            if state.status == Status::Open {
                state.status = status.into();
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

    /// Reports whether the request is still unanswered, and ends it when it is
    /// and it is still open.
    ///
    /// The test and the transition share one lock scope. An accepted response
    /// cannot fall between them and get discarded.
    ///
    /// The answer describes the positions, not this call. A request another
    /// path already closed still reports the truth about its arrivals, so a
    /// failed delivery report is judged by the evidence rather than by which
    /// path closed the request first.
    pub(in crate::requester) fn abandon_unanswered(&self, status: Terminal) -> bool {
        let (unanswered, transitioned) = {
            let mut state = self.state.lock();
            let unanswered = state
                .awaited
                .iter()
                .all(|awaited| awaited.arrival.is_none());
            let transitioned = unanswered && state.status == Status::Open;
            if transitioned {
                state.status = status.into();
            }
            (unanswered, transitioned)
        };
        if transitioned {
            self.notify.notify_waiters();
        }
        unanswered
    }

    /// Stores the first response for its named subsystem.
    ///
    /// A frame over `max_payload`, and a frame written in another format, fill
    /// their position with the refusal rather than the response. So the caller
    /// learns at once instead of waiting out its deadline, and the position
    /// never holds more than the ceiling. The refusal is the position's first
    /// writer, and one duplicate delivery of the same response is absorbed by
    /// first-writer-wins, so a second frame does not replace it. That trade is
    /// deliberate: a responder that answers over the ceiling or in another
    /// format is misconfigured rather than late. A refusal is also evidence
    /// that the record reached a responder, so
    /// [`abandon_unanswered`](Self::abandon_unanswered) reads it as an answer
    /// and a failed delivery report does not fail the call.
    ///
    /// A response that arrives after the deadline but before the waiter wakes
    /// is still stored, because the only thing tested here is the record's
    /// status. Reading the clock instead would turn a delivered answer into a
    /// discard, and an answer is worth more to the caller than a timeout. The
    /// window is the gap between the deadline elapsing and the waiter task
    /// being scheduled.
    ///
    /// The caller releases the map guard before this function takes the state
    /// lock. This function releases the state lock before it notifies.
    pub(super) fn deposit(&self, frame: ResponseFrame, max_payload: usize) -> ResponseDisposition {
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
            let disposition = if frame.payload.len() > max_payload {
                position.arrival = Some(Arrival::TooLarge);
                ResponseDisposition::ResponseTooLarge
            } else if frame.format.to_str() == self.expects {
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

    /// The payload stored for one awaited position, when a response filled it.
    ///
    /// A position a refusal filled answers `None`: a mismatched format and an
    /// oversized payload both mark the position without storing bytes.
    #[cfg(test)]
    pub(super) fn stored_payload(&self, subsystem: &SubsystemName) -> Option<BytesMut> {
        let state = self.state.lock();
        let position = state
            .awaited
            .iter()
            .find(|awaited| &awaited.name == subsystem)?;
        match &position.arrival {
            Some(Arrival::Response { payload, .. }) => Some(payload.clone()),
            Some(Arrival::FormatMismatch | Arrival::TooLarge) | None => None,
        }
    }
}

impl From<Terminal> for Status {
    fn from(terminal: Terminal) -> Self {
        match terminal {
            Terminal::TimedOut => Self::TimedOut,
            Terminal::Cancelled => Self::Cancelled,
            Terminal::ShuttingDown => Self::ShuttingDown,
        }
    }
}
