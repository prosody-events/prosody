//! What one call leaves in a trace.
//!
//! The span is named for the function, exactly as the producer's `send` is, and
//! it covers the whole wait: there is deliberately no second span for the
//! answer arriving, because the answer is this call returning.

use super::{MAX_TIMEOUT, NODE, RequestPayload, TestError, names, registry, requester};
use crate::Topic;
use crate::requester::{Outcome, ResponseFailure};
use crate::test_util::captured_spans;
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

/// Requests this suite's registry admits.
const IN_FLIGHT: usize = 2;

/// Most subsystems this suite's registry accepts.
const MAX_AWAITED: usize = 2;

/// The topic, key and subsystem the call names, asserted back off the span.
const TOPIC: &str = "requests";
const KEY: &str = "the-key";
const SUBSYSTEM: &str = "billing";

/// One unanswered call opens a client span carrying the request's identity, the
/// Kafka system it travelled over, and the count of answers it received.
///
/// The paused clock walks past the deadline, so the call returns all-`Timeout`
/// outcomes and `responses.received` is a value the body computed rather than a
/// constant.
#[test]
fn one_call_opens_a_client_span_naming_its_request_and_its_answers() -> Result<()> {
    let mut outcome: Result<()> = Err(eyre!("the call never ran"));
    let spans = captured_spans(|| outcome = unanswered_call());
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

/// Drives one call that nothing answers, on a paused clock that walks past its
/// deadline.
fn unanswered_call() -> Result<()> {
    paused()?.block_on(async {
        let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
        let requester = requester(registry)?;
        let awaited = names(&[SUBSYSTEM])?;
        let no_headers: Vec<(&'static str, &'static str)> = Vec::new();
        let outcomes = requester
            .request::<_, u32, TestError>(
                no_headers,
                Topic::from(TOPIC),
                KEY,
                RequestPayload,
                &awaited,
                MAX_TIMEOUT,
            )
            .await?;
        ensure!(
            outcomes == vec![Outcome::Failed(ResponseFailure::Timeout)],
            "nothing answered this call, so its one outcome must be a timeout"
        );
        Ok(())
    })
}

/// A current-thread runtime with paused time and the whole driver set, because
/// the producer this call uses has its own I/O.
fn paused() -> Result<Runtime, IoError> {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
}

/// The one exported span with `name`.
fn named<'a>(spans: &'a [SpanData], name: &str) -> Result<&'a SpanData> {
    spans
        .iter()
        .find(|span| span.name == name)
        .ok_or_else(|| eyre!("span {name} was not exported"))
}

/// One span attribute, by key.
fn attribute<'a>(span: &'a SpanData, key: &str) -> Result<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
        .ok_or_else(|| eyre!("the {} span carries no {key}", span.name))
}
