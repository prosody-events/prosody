//! What one call leaves in a trace.
//!
//! The span is named for the function, exactly as the producer's `send` is, and
//! it covers the whole wait: there is deliberately no second span for the
//! answer arriving, because the answer is this call returning.

use super::{KEY, PEER, SUBSYSTEM, TOPIC, unanswered_call};
use crate::peer::router::loopback::paused;
use crate::test_util::{captured_spans, named, span_attribute};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Value;
use opentelemetry::trace::SpanKind;
use uuid::Uuid;

/// The span `request` opens. `#[instrument]` with no explicit name takes the
/// function's name, so anything naming it `peer.request` is wrong.
const CALL: &str = "request";

/// One unanswered call opens a client span carrying the request's identity, the
/// Kafka system it travelled over, and the count of answers it received.
///
/// The paused clock walks past the deadline, so the call returns all-`Timeout`
/// outcomes and `responses.received` is a value the body computed rather than a
/// constant.
#[test]
fn one_call_opens_a_client_span_naming_its_request_and_its_answers() -> Result<()> {
    let mut outcome: Result<()> = Err(eyre!("the call never ran"));
    let spans = captured_spans(|| outcome = run_unanswered_call());
    outcome?;

    let span = named(&spans, CALL)?;
    ensure!(
        span.span_kind == SpanKind::Client,
        "a call that awaits an answer is a client span, not {:?}",
        span.span_kind
    );
    for (key, expected) in [
        ("messaging.system", "kafka"),
        ("topic", TOPIC),
        ("key", KEY),
        ("response.peer", &PEER.to_string()),
        ("request.outcome", "none"),
        ("responses.missing", SUBSYSTEM),
    ] {
        let value = span_attribute(span, key)?;
        ensure!(
            value.as_str() == expected,
            "{key} reads {value}, not {expected}"
        );
    }
    let id = span_attribute(span, "request.id")?;
    Uuid::try_parse(&id.as_str())
        .map_err(|error| eyre!("request.id reads {id}, which is no UUID: {error}"))?;
    for (key, expected) in [("subsystems", 1_i64), ("responses.received", 0_i64)] {
        let value = span_attribute(span, key)?;
        ensure!(
            matches!(value, Value::I64(count) if *count == expected),
            "{key} reads {value:?}, not the integer {expected}"
        );
    }
    let latency = span_attribute(span, "request.latency_ms")?;
    ensure!(
        matches!(latency, Value::I64(milliseconds) if *milliseconds >= 0),
        "request.latency_ms reads {latency:?}, not a nonnegative integer"
    );
    Ok(())
}

/// Drives the shared unanswered call on a paused clock of its own, because
/// [`captured_spans`] scopes a synchronous closure.
fn run_unanswered_call() -> Result<()> {
    paused()?.block_on(unanswered_call())
}
