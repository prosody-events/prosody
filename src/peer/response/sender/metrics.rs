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
    /// The selected endpoint did not accept the response.
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
