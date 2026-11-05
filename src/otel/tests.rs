use color_eyre::Result;
use color_eyre::eyre::ensure;
use opentelemetry::trace::{Status, TraceContextExt as _};

use crate::otel::{SpanRelation, context_with_parent, mark_failed};
use crate::test_util::{
    assert_span_relation, captured_spans, captured_spans_filtered, sampled_remote_context,
};
use tracing::{debug_span, info_span, instrument};
use tracing_subscriber::filter::LevelFilter;

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

/// A filtered internal span leaves the carried context unchanged.
#[test]
fn a_disabled_span_preserves_its_carried_context() {
    let carried = sampled_remote_context();
    let expected = carried.span().span_context().clone();
    captured_spans_filtered(LevelFilter::INFO, || {
        let actual = context_with_parent(&debug_span!("filtered"), carried);
        assert_eq!(actual.span().span_context(), &expected);
    });
}

/// A failure logged at warn still marks its span failed. This holds for the
/// `err` event of `#[instrument]` and for [`mark_failed`].
#[test]
fn warn_failures_mark_their_span_failed() -> Result<()> {
    #[instrument(err(level = "warn"))]
    fn instrumented() -> Result<(), &'static str> {
        Err("failed")
    }

    let spans = captured_spans(|| {
        assert!(instrumented().is_err());
        mark_failed(&info_span!("manual"), "failed");
    });
    ensure!(
        spans.len() == 2
            && spans
                .iter()
                .all(|span| matches!(span.status, Status::Error { .. })),
        "expected two failed spans, got {spans:?}"
    );
    Ok(())
}
