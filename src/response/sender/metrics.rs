//! What one response's journey came to, as counters an operator can watch.
//!
//! **No identity is ever an attribute.** A node id and a claimed subsystem name
//! arrive in a Kafka header a topic writer controls, so a series keyed by one
//! is a cardinality attack on the metrics pipeline. One fixed label per outcome
//! is the whole attribute set here, which is why every label below is a
//! `&'static str` a `const fn` chose. Per-destination occupancy is derivable
//! without such a label: it is `stages{enqueued}` less `stages{delivered}` less
//! the drops.
//!
//! These counters are the operator's account of delivery.
//! [`SendCounters`](super::SendCounters) is the in-process one that the
//! sender's own suites read, and the two are kept apart on purpose: a test
//! asserts on a value it owns rather than on a global meter provider.
//!
//! Each instrument binds to whatever meter provider is global when it is first
//! touched, so a process installs its provider before it queues a response.

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

/// Responses that never reached their destination, by fixed reason label.
static DROPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.dropped")
        .with_description("Responses that never reached their destination")
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
/// Every queued response passes `Attempted`, and each later stage is reached
/// only from the one before it. So the differences between these counters are
/// where responses are lost, and [`DropReason`] says why.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub(super) enum Stage {
    /// A response was offered to the sender.
    Attempted,
    /// It took a slot and entered its destination's queue.
    Enqueued,
    /// A worker framed it into that worker's own scratch.
    Framed,
    /// The destination accepted the frame.
    Delivered,
}

/// Why one response never reached its destination.
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
    /// The destination's queue was full.
    QueueFull,
    /// The destination's worker had already exited.
    QueueClosed,
    /// The response ran out of its send deadline.
    Deadline,
    /// The codec could not frame the result inside the ceiling.
    EncodeFailed,
    /// No live registration names the node the response is addressed to.
    UnresolvableNode,
    /// The directory lookup itself failed.
    LookupFailed,
    /// Every endpoint of the route refused or never answered.
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

/// Names a queue refusal the same way. The payload the error carries is the
/// caller's again, so only which refusal it was matters here.
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
