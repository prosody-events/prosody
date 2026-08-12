//! The return leg reads as one nested client call.
//!
//! The assertion spans the real delivery. The sender opens
//! `peer.response.send`, the client injects that span's context into the
//! outbound metadata, and the listener extracts it in another task. Asserting
//! the *immediate* parent is what makes it falsifiable — dropping the injection
//! re-parents `peer.response.receive`, and "it is not a root span" would not
//! notice.

use super::{ALPHA, Harness, header, reaching, register};
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{deliver_response, stage};
use crate::peer::router::directory::Endpoint;
use crate::test_util::{GlobalSpans, TEST_RUNTIME, named, span_attribute};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::trace::{SpanKind, Status, TraceContextExt};
use opentelemetry_sdk::trace::SpanData;
use std::convert::Infallible;
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
/// span says what became of the response and which endpoint answered. A
/// refused delivery marks both transport spans as OpenTelemetry errors.
#[test]
fn the_return_leg_nests_under_the_call_that_asked_for_it() -> Result<()> {
    let spans = GlobalSpans::install()?;
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let router = reaching(&harness.address)?;
        let request = register(&harness.registry, &[ALPHA])?;

        // The caller's span is opened, read, and closed here: the send carries
        // its context, not the span itself.
        let caller = info_span!("peer.test.call");
        let trace = caller.context();
        let caller_span = trace.span().span_context().clone();
        let payload = PAYLOAD.to_vec();
        let prepared = stage::<CountingCodec, Infallible>(
            header(harness.peer, request.id(), ALPHA)?,
            Ok(&payload),
        );
        deliver_response(
            &router,
            prepared,
            trace,
            RequestDeadline::from_unix_micros(4_102_444_800_000_000),
        )
        .await;
        drop(caller);

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
        let disposition = span_attribute(sent, DISPOSITION)?;
        ensure!(
            disposition.as_str() == "delivered",
            "{SENT} must say what became of the response, not {disposition}"
        );
        let preference = span_attribute(sent, PREFERENCE)?;
        ensure!(
            preference.as_str() == "direct",
            "{SENT} must name the endpoint that answered, not {preference}"
        );
        ensure_rpc_attributes(sent)?;
        ensure_rpc_attributes(received)?;
        ensure_server_attributes(sent, &harness.address)?;
        for (key, expected) in [
            ("peer.request", request.id().to_string()),
            ("peer.subsystem", ALPHA.to_owned()),
            ("peer.target", harness.peer.to_string()),
            (DISPOSITION, "accepted".to_owned()),
        ] {
            let value = span_attribute(received, key)?;
            ensure!(
                value.as_str() == expected,
                "{RECEIVED} {key} reads {value}, not {expected}"
            );
        }

        let mut refused = register(&harness.registry, &[ALPHA])?;
        drop(refused.receiver()?);
        let caller = info_span!("peer.test.refused");
        let payload = PAYLOAD.to_vec();
        let prepared = stage::<CountingCodec, Infallible>(
            header(harness.peer, refused.id(), ALPHA)?,
            Ok(&payload),
        );
        deliver_response(
            &router,
            prepared,
            caller.context(),
            RequestDeadline::from_unix_micros(4_102_444_800_000_000),
        )
        .await;
        drop(caller);

        let ended = spans.ended();
        for (name, expected) in [(SENT, "send_failed"), (RECEIVED, "closed_request")] {
            let span = ended
                .iter()
                .find(|span| span.name == name && matches!(span.status, Status::Error { .. }))
                .ok_or_else(|| eyre!("no failed {name} span was exported"))?;
            ensure!(
                matches!(&span.status, Status::Error { description } if description == expected),
                "{name} must report {expected} as an error, not {:?}",
                span.status
            );
            ensure!(
                span_attribute(span, "error.type")?.as_str() == "NOT_FOUND",
                "{name} must keep the exact gRPC error type"
            );
        }
        Ok(())
    })
}

fn ensure_rpc_attributes(span: &SpanData) -> Result<()> {
    for (key, expected) in [
        ("rpc.system.name", "grpc"),
        ("rpc.method", "prosody.peer.v1.PeerService/DeliverResult"),
        ("rpc.response.status_code", "OK"),
    ] {
        let value = span_attribute(span, key)?;
        ensure!(
            value.as_str() == expected,
            "{} {key} reads {value}, not {expected}",
            span.name
        );
    }
    Ok(())
}

fn ensure_server_attributes(span: &SpanData, endpoint: &Endpoint) -> Result<()> {
    let uri = endpoint.uri();
    ensure!(
        span_attribute(span, "server.address")?.as_str()
            == uri
                .host()
                .ok_or_else(|| eyre!("the endpoint has no host"))?,
        "{SENT} must name the selected server address"
    );
    ensure!(
        matches!(
            span_attribute(span, "server.port")?,
            opentelemetry::Value::I64(port) if Some(*port) == uri.port_u16().map(i64::from)
        ),
        "{SENT} must name the selected server port"
    );
    Ok(())
}
