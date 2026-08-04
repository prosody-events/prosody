//! What one call leaves in a trace.
//!
//! The span is named for the function, exactly as the producer's `send` is, and
//! it covers the whole wait: there is deliberately no second span for the
//! answer arriving, because the answer is this call returning.

use super::{KEY, NODE, TOPIC, unanswered_call};
use crate::test_util::{captured_spans, named};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::Value;
use opentelemetry::trace::SpanKind;
use opentelemetry_sdk::trace::SpanData;
use std::io::Error as IoError;
use tokio::runtime::{Builder, Runtime};
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
        ("response.node", &NODE.to_string()),
    ] {
        let value = attribute(span, key)?;
        ensure!(
            value.as_str() == expected,
            "{key} reads {value}, not {expected}"
        );
    }
    let id = attribute(span, "request.id")?;
    Uuid::try_parse(&id.as_str())
        .map_err(|error| eyre!("request.id reads {id}, which is no UUID: {error}"))?;
    for (key, expected) in [("subsystems", 1_i64), ("responses.received", 0_i64)] {
        let value = attribute(span, key)?;
        ensure!(
            matches!(value, Value::I64(count) if *count == expected),
            "{key} reads {value:?}, not the integer {expected}"
        );
    }
    Ok(())
}

/// Drives the shared unanswered call on a paused clock of its own, because
/// [`captured_spans`] scopes a synchronous closure.
fn run_unanswered_call() -> Result<()> {
    paused()?.block_on(unanswered_call())
}

/// A current-thread runtime with paused time and the whole driver set, because
/// the producer this call uses has its own I/O.
fn paused() -> Result<Runtime, IoError> {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
}

/// One span attribute, by key.
fn attribute<'a>(span: &'a SpanData, key: &str) -> Result<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
        .ok_or_else(|| eyre!("the {} span carries no {key}", span.name))
}
