use super::*;
use crate::consumer::decode::DecodedMessage;
use crate::consumer::message::ConsumerMessageValue;
use crate::test_util::{assert_span_relation, captured_spans, sampled_remote_context};
use opentelemetry::Context;
use opentelemetry::trace::TraceContextExt as _;

/// A `DecodedMessage` carrying `context` as its `parent_context`, the
/// same shape `decode_message` produces after extracting headers via the
/// propagator.
fn decoded_message(context: Context) -> DecodedMessage<serde_json::Value> {
    DecodedMessage {
        value: Arc::new(ConsumerMessageValue::default()),
        parent_context: context,
    }
}

/// The "load" span parents on the producer-propagated context under
/// child-of and links back to it under follows-from.
#[test]
fn load_span_connects_to_producer_context() -> color_eyre::Result<()> {
    let context = sampled_remote_context();
    let target = context.span().span_context().clone();

    for relation in [SpanRelation::Child, SpanRelation::FollowsFrom] {
        let decoded = decoded_message(context.clone());
        let spans = captured_spans(move || {
            let _span = create_load_span(decoded.meta(), false, relation);
        });
        assert_span_relation(&spans, "load", relation, &target)?;
    }
    Ok(())
}
