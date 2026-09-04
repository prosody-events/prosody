//! Metric instruments owned by one peer runtime.

use opentelemetry::global::meter;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

/// All metric instruments that one peer runtime records.
///
/// One runtime owns one value. Tests can supply a meter from a local provider.
#[derive(Clone)]
pub struct PeerMetrics {
    pub(crate) request_latency: Histogram<f64>,
    pub(crate) requests_pending: UpDownCounter<i64>,
    pub(crate) response_dispositions: Counter<u64>,
    pub(crate) response_stages: Counter<u64>,
    pub(crate) responses_dropped: Counter<u64>,
}

impl PeerMetrics {
    /// Builds the peer instruments on `meter`.
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            request_latency: meter
                .f64_histogram("prosody.request.duration")
                .with_description("Duration of requests that waited for answers")
                .with_unit("s")
                .build(),
            requests_pending: meter
                .i64_up_down_counter("prosody.request.pending")
                .with_description("Requests this process is waiting for answers to")
                .with_unit("{request}")
                .build(),
            response_dispositions: meter
                .u64_counter("prosody.response.dispositions")
                .with_description("Response delivery attempts this process answered")
                .with_unit("{response}")
                .build(),
            response_stages: meter
                .u64_counter("prosody.response.stages")
                .with_description("Responses that reached one stage of the send path")
                .with_unit("{response}")
                .build(),
            responses_dropped: meter
                .u64_counter("prosody.response.dropped")
                .with_description("Responses the sender gave up on")
                .with_unit("{response}")
                .build(),
        }
    }
}

impl Default for PeerMetrics {
    fn default() -> Self {
        Self::new(&meter("prosody"))
    }
}
