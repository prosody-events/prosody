//! What one response's journey came to, as counters an operator can watch.
//!
//! **No identity is ever an attribute.** A node id and a claimed subsystem name
//! arrive in a Kafka header a topic writer controls, so a series keyed by one
//! is a cardinality attack on the metrics pipeline. Every attribute here is one
//! fixed `&'static str` a `const fn` chose. What the queues hold across the
//! process is still derivable: it is `stages{enqueued}` less
//! `stages{delivered}` less every reason a response can meet once it counts as
//! enqueued — `queue_closed`, `queue_full`, `deadline`, `encode_failed`,
//! `unresolvable_node`, `lookup_failed` and `send_failed`. The three refusals
//! before that point — `no_destination`, `no_slot` and `shutting_down` — reach
//! no queue, so a subtraction of the whole `dropped` total counts them twice
//! and reads negative. What the fleet itself holds is
//! `prosody.peer.fleet.destinations`.
//!
//! These counters are the operator's account of delivery.
//! [`SendCounters`](super::SendCounters) is the in-process one, and it is the
//! per-sender total the delivery suites assert on. The series here are asserted
//! through a meter provider installed in `tests/metrics.rs`, one per test
//! process.
//!
//! Each instrument binds to whatever meter provider is global when it is first
//! touched, so a process installs its provider before it queues a response.

use crate::router::Preference;
use crate::router::fleet::Refusal;
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::sync::LazyLock;
use tokio::sync::mpsc::error::TrySendError;

/// Responses that reached each stage of the send path, by fixed stage label.
static STAGES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.stages")
        .with_description("Responses that reached one stage of the send path")
        .with_unit("{response}")
        .build()
});

/// Responses the sender gave up on, by fixed reason label.
static DROPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.dropped")
        .with_description("Responses the sender gave up on")
        .with_unit("{response}")
        .build()
});

/// Responses a route's next candidate answered after the one before it failed.
static FALLBACKS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.fallback")
        .with_description(
            "Responses a route's next candidate answered after the one before it failed",
        )
        .with_unit("{response}")
        .build()
});

/// Send attempts that waited for a destination's next turn.
static RATE_LIMITED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.rate_limited")
        .with_description("Send attempts that waited for a destination's pacing")
        .with_unit("{attempt}")
        .build()
});

/// How far one response got.
///
/// Every offered response passes `Attempted`, and each later stage is reached
/// only from the one before it. So the differences between these counters are
/// where responses stop, and [`DropReason`] says why.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(super) enum Stage {
    /// A response was offered to the sender.
    Attempted,
    /// It took a slot and was offered to its destination's queue. Counted
    /// before the offer, so no worker can report a later stage first.
    Enqueued,
    /// A worker framed it into that worker's own scratch.
    Framed,
    /// The destination accepted the frame. A worker records this stage only
    /// where the transport answered `Ok`, so a job the deadline ended is never
    /// counted here — not even when the peer holds its frame.
    Delivered,
}

/// Why the sender gave up on one response.
///
/// One vocabulary for both halves of the path: the refusals the queueing side
/// gives, and the outcomes a worker reports. Every response that is not
/// delivered moves exactly one of these.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(super) enum DropReason {
    /// Every destination cell held a destination with sends in flight.
    NoDestination,
    /// This destination's slots were all taken.
    NoSlot,
    /// Fleet admission was closed.
    ShuttingDown,
    /// The destination's queue was full. This arm exists because the channel
    /// error is total, not because the send path can reach it: a job that holds
    /// a slot always has room. A non-zero `queue_full` series therefore reports
    /// that the slot accounting broke.
    QueueFull,
    /// The destination's worker had already exited.
    QueueClosed,
    /// The sender gave up before the delivery finished. This can cancel a
    /// delivery the transport already accepted, so it does not say the peer
    /// never got the response.
    Deadline,
    /// The codec could not frame the result inside the ceiling.
    EncodeFailed,
    /// No live registration names the node the response is addressed to.
    UnresolvableNode,
    /// The directory lookup itself failed.
    LookupFailed,
    /// No endpoint of the route answered `Ok`, which does not prove that none
    /// of them read the frame. A failure that is not a wrong endpoint also ends
    /// the walk, so the candidate behind it can stay undialed.
    SendFailed,
}

impl Stage {
    /// Counts one response reaching this stage.
    pub(super) fn record(self) {
        STAGES.add(1, &[KeyValue::new("stage", self.label())]);
    }

    /// The metric label for this stage.
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Attempted => "attempted",
            Self::Enqueued => "enqueued",
            Self::Framed => "framed",
            Self::Delivered => "delivered",
        }
    }
}

impl DropReason {
    /// Counts one response dropped for this reason.
    pub(super) fn record(self) {
        DROPPED.add(1, &[KeyValue::new("reason", self.label())]);
    }

    /// The metric label for this reason.
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::NoDestination => "no_destination",
            Self::NoSlot => "no_slot",
            Self::ShuttingDown => "shutting_down",
            Self::QueueFull => "queue_full",
            Self::QueueClosed => "queue_closed",
            Self::Deadline => "deadline",
            Self::EncodeFailed => "encode_failed",
            Self::UnresolvableNode => "unresolvable_node",
            Self::LookupFailed => "lookup_failed",
            Self::SendFailed => "send_failed",
        }
    }
}

/// Names a fleet refusal in the send path's own vocabulary, so the router never
/// has to know what a response is.
impl From<Refusal> for DropReason {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::ShuttingDown => Self::ShuttingDown,
            Refusal::NoDestination => Self::NoDestination,
            Refusal::NoSlot => Self::NoSlot,
        }
    }
}

/// Names a queue refusal the same way. The error carries the caller's payload,
/// so only the refusal matters here.
impl<T> From<&TrySendError<T>> for DropReason {
    fn from(error: &TrySendError<T>) -> Self {
        match error {
            TrySendError::Full(_) => Self::QueueFull,
            TrySendError::Closed(_) => Self::QueueClosed,
        }
    }
}

/// Counts one attempt that had to wait for its destination's next turn.
pub(super) fn record_rate_limited() {
    RATE_LIMITED.add(1, &[]);
}

/// Counts one response the route's next candidate answered after the one before
/// it failed.
///
/// A route offers a second candidate only where the dialer's network label and
/// the node's are equal. Each count therefore says the first candidate did not
/// serve the response from here. A network label put on the wrong process is
/// one cause of that. A dead direct endpoint, a node that moved and a node that
/// answered `UNAVAILABLE` are others, so read this series as a question, not as
/// a verdict.
///
/// It counts transitions, not responses, and a steady fault does not count once
/// per response. The destination remembers the candidate that answered, so the
/// responses behind the first one start there and count nothing. A further
/// count needs that remembered candidate to fail too, or the destination to be
/// evicted and the walk to start over. `from` and `to` are recorded together,
/// so one series names the whole transition. A reader does not need to know
/// which endpoints a route offers.
pub(super) fn record_fallback(from: Preference, to: Preference) {
    FALLBACKS.add(
        1,
        &[
            KeyValue::new("from", from.label()),
            KeyValue::new("to", to.label()),
        ],
    );
}
