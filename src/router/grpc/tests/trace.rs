//! The return leg reads as one nested client call.
//!
//! The assertion spans the real delivery. The sender opens
//! `peer.response.send`, the client injects that span's context into the
//! outbound metadata, and the listener extracts it in another task. Asserting
//! the *immediate* parent is what makes it falsifiable — dropping the injection
//! re-parents `peer.response.receive`, and "it is not a root span" would not
//! notice.

use super::{ALPHA, Harness, header, reaching, register};
use crate::response::frame::tests::CountingCodec;
use crate::response::sender::TypedSender;
use crate::router::Router;
use crate::test_util::{GlobalSpans, TEST_RUNTIME, named};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Value;
use opentelemetry::trace::{SpanKind, TraceContextExt};
use opentelemetry_sdk::trace::SpanData;
use tracing::info_span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The span the sender opens for one outbound response.
const SENT: &str = "peer.response.send";

/// The span the listener opens for the response it receives.
const RECEIVED: &str = "peer.response.receive";

/// The response body this suite sends.
const PAYLOAD: &[u8] = b"traced";

/// The attribute naming what became of the response.
const DISPOSITION: &str = "peer.disposition";

/// The attribute naming the endpoint that answered.
const PREFERENCE: &str = "peer.preference";

/// One response delivered through the whole send path lands in the caller's
/// trace, with `peer.response.receive` directly under `peer.response.send`, the
/// send span is a client call rather than a consumer continuation, and that
/// span says what became of the response and which endpoint answered.
#[test]
fn the_return_leg_nests_under_the_call_that_asked_for_it() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let router = reaching(harness.cap, &harness.address)?;
        let sender =
            TypedSender::<CountingCodec, _>::new_route(router.clone(), router.fleet(), harness.cap);
        let request = register(&harness.registry, &[ALPHA])?;

        // The caller's span is opened, read, and closed here: the send carries
        // its context, not the span itself.
        let caller = info_span!("peer.test.call");
        let trace = caller.context();
        let caller_span = trace.span().span_context().clone();
        let delivered = sender
            .send(
                header(harness.node, request.id(), ALPHA)?,
                trace,
                PAYLOAD.to_vec(),
            )
            .await;
        drop(caller);
        drop(sender);
        ensure!(
            delivered,
            "the response must have reached the listener before its trace is read"
        );

        let ended = spans.ended();
        let sent = named(&ended, SENT)?;
        let received = named(&ended, RECEIVED)?;
        ensure!(
            sent.span_kind == SpanKind::Client,
            "{SENT} is an outbound call, not a consumer continuation: it is {:?}",
            sent.span_kind
        );
        ensure!(
            sent.parent_span_id == caller_span.span_id()
                && sent.span_context.trace_id() == caller_span.trace_id(),
            "{SENT} must be a child of the span the response was sent under"
        );
        ensure!(
            received.parent_span_id == sent.span_context.span_id()
                && received.span_context.trace_id() == caller_span.trace_id(),
            "{RECEIVED} must be a child of {SENT}, in the caller's trace"
        );
        let disposition = attribute(sent, DISPOSITION)?;
        ensure!(
            disposition.as_str() == "delivered",
            "{SENT} must say what became of the response, not {disposition}"
        );
        let preference = attribute(sent, PREFERENCE)?;
        ensure!(
            preference.as_str() == "direct",
            "{SENT} must name the endpoint that answered, not {preference}"
        );
        Ok(())
    })
}

/// One span attribute, by key.
fn attribute<'a>(span: &'a SpanData, key: &str) -> Result<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
        .ok_or_else(|| eyre!("the {} span carries no {key}", span.name))
}
