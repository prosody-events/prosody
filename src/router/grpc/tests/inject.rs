//! Trace context across the peer hop.
//!
//! The assertion spans the **real** call: the client injects into the outbound
//! metadata and the listener extracts from the inbound metadata, in separate
//! tasks. A same-process round trip through the injector and the extractor
//! would still pass with the injection deleted, so it would prove nothing.

use super::{ALPHA, Harness, header, payload, register};
use crate::codec::Codec;
use crate::response::frame::tests::CountingCodec;
use crate::test_util::{GlobalSpans, TEST_RUNTIME, named};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry_sdk::trace::SpanData;
use tonic::Code;
use tracing::{Instrument, info_span};

/// The span the listener opens for one delivered response.
const RECEIVED: &str = "peer.response.receive";

/// The span the instrumented call is made from.
const CALLER: &str = "peer.test.call";

/// A short payload; its size is not the subject.
const SHORT: usize = 8;

/// The call the client makes under a span reaches the listener as a child of
/// that span. A call made under no span reaches it as a trace of its own, so a
/// missing context stays visible rather than being invented.
#[test]
fn the_metadata_hop_carries_the_trace_context() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let traced = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let answered = harness
            .deliver(&header(harness.node, traced, ALPHA)?, payload(SHORT))
            .instrument(info_span!("peer.test.call"))
            .await?;
        ensure!(answered == Code::Ok, "the traced delivery must be accepted");

        let untraced = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let answered = harness
            .deliver(&header(harness.node, untraced, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            answered == Code::Ok,
            "the untraced delivery must be accepted"
        );

        let ended = spans.ended();
        let caller = named(&ended, CALLER)?;
        let received: Vec<&SpanData> = ended.iter().filter(|span| span.name == RECEIVED).collect();
        ensure!(
            received.len() == 2,
            "the listener must have opened one span per delivery, not {}",
            received.len()
        );
        let carried: Vec<&&SpanData> = received
            .iter()
            .filter(|span| span.span_context.trace_id() == caller.span_context.trace_id())
            .collect();
        ensure!(
            carried.len() == 1,
            "exactly one delivery must have carried the caller's trace, not {}",
            carried.len()
        );
        let carried = carried.first().ok_or_else(|| eyre!("no carried span"))?;
        ensure!(
            carried.parent_span_id == caller.span_context.span_id(),
            "the listener's span must be a child of the span the call was made from"
        );
        Ok(())
    })
}
