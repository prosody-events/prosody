use super::*;
use crate::test_util::{assert_span_relation, captured_spans, sampled_remote_context};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use opentelemetry::trace::TraceContextExt as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// [`StoredTimer::to_trigger`] reconstructs a `"timer_defer.load"` span that
/// connects to the stored scheduling context under both span relations —
/// child-of parents on it, follows-from links back to it — and the trigger
/// carries that span live (reload time is dispatch time on the defer path;
/// nothing later mints a dispatch span for the handler).
#[test]
fn to_trigger_links_reconstructed_span_to_stored_context() -> Result<()> {
    let stored_context = sampled_remote_context();
    let target = stored_context.span().span_context().clone();

    for relation in [SpanRelation::Child, SpanRelation::FollowsFrom] {
        let stored = StoredTimer {
            key: Key::from("defer-load"),
            time: CompactDateTime::from(1_500_000_u32),
            context: stored_context.clone(),
        };

        let mut carried = None;
        let spans = captured_spans(|| {
            let trigger = stored.to_trigger(relation);
            carried = Some(trigger.span().context().span().span_context().clone());
        });

        assert_span_relation(&spans, "timer_defer.load", relation, &target)?;

        // The handler-visible span is the exported reload span itself.
        let carried = carried.ok_or_else(|| eyre!("capture closure did not run"))?;
        let reload = spans
            .iter()
            .find(|s| s.name.as_ref() == "timer_defer.load")
            .ok_or_else(|| eyre!("reload span was not exported"))?;
        assert_eq!(carried.span_id(), reload.span_context.span_id());
        assert_eq!(carried.trace_id(), reload.span_context.trace_id());
    }

    Ok(())
}
