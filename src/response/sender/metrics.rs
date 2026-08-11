//! What one response's journey came to, as counters an operator can watch.
//!
//! **No identity is ever an attribute.** A peer id and a claimed subsystem name
//! arrive in a Kafka header a topic writer controls, so a series keyed by one
//! is a cardinality attack on the metrics pipeline. Every attribute here is one
//! fixed `&'static str` a `const fn` chose.
//!
//! These counters are the operator's account of delivery.
//!
//! Each instrument binds to whatever meter provider is global when it is first
//! touched. Thus, install the process provider before the first response.

use crate::router::Preference;
use opentelemetry::KeyValue;
use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::sync::LazyLock;

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
/// Concurrent senders can count the same shared preference change once each.
static FALLBACKS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.response.fallback")
        .with_description(
            "Responses a route's next candidate answered after the one before it failed",
        )
        .with_unit("{response}")
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
    /// The sender framed the response.
    Framed,
    /// The destination accepted the frame. The sender records this stage only
    /// where the transport answered `Ok`, so an expired response is never
    /// counted here — not even when the peer holds its frame.
    Delivered,
}

/// Why the sender gave up on one response.
///
/// Each response that is not delivered moves exactly one reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::VariantArray))]
pub enum DropReason {
    /// The codec could not encode the result.
    EncodeFailed,
    /// No live registration names the peer the response is addressed to.
    UnresolvablePeer,
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
            Self::EncodeFailed => "encode_failed",
            Self::UnresolvablePeer => "unresolvable_peer",
            Self::LookupFailed => "lookup_failed",
            Self::SendFailed => "send_failed",
        }
    }
}

/// Counts one response the route's next candidate answered after the one before
/// it failed.
///
/// A route offers a second candidate only where the dialer's network label and
/// the peer's are equal. Each count therefore says the first candidate gave no
/// proof that it serves the peer. That is not proof it never read the frame:
/// the share it was given can simply have run out. A network label put on the
/// wrong process is one cause. A dead direct endpoint, a peer that moved and a
/// peer that answered `UNAVAILABLE` are others, so read this series as a
/// question, not as a verdict.
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
