use color_eyre::Result;
use opentelemetry::trace::TraceContextExt as _;

use crate::otel::SpanRelation;
use crate::test_util::{assert_span_relation, captured_spans, sampled_remote_context};

/// `related_span!` connects a new span to a propagated context under both
/// relations: child-of parents on it and shares its trace; follows-from starts
/// a fresh trace root that links back to it.
#[test]
fn related_span_connects_to_propagated_context() -> Result<()> {
    let context = sampled_remote_context();
    let target = context.span().span_context().clone();

    for relation in [SpanRelation::Child, SpanRelation::FollowsFrom] {
        let ctx = context.clone();
        let spans = captured_spans(move || {
            let _span = related_span!(relation, ctx, "related_op");
        });
        assert_span_relation(&spans, "related_op", relation, &target)?;
    }
    Ok(())
}
