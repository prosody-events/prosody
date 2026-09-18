use super::*;
use crate::test_util::{
    TEST_RUNTIME, assert_span_relation, captured_spans, sampled_remote_context,
};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use opentelemetry::trace::TraceContextExt as _;
use quickcheck::{QuickCheck, TestResult};
use std::collections::BTreeSet;
use tracing::Span;
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

/// Two stores minted by one provider for one segment share the durable
/// substrate, a store minted for another segment never sees those rows, and
/// two standalone stores share nothing. The twin of the message store's
/// substrate property.
///
/// A fresh map per `create_store` would make every durable row vanish with the
/// store that wrote it. A map keyed by the key alone would merge two
/// partitions. One shared default provider would merge every standalone
/// store.
#[test]
fn prop_provider_shares_one_substrate_per_segment() {
    fn property(name: String, times: Vec<u32>) -> TestResult {
        finish(TEST_RUNTIME.block_on(async move {
            let provider =
                MemoryTimerDeferStoreProvider::new(MemorySegmentStore::new(), SpanRelation::Child);
            let topic = Topic::from("substrate");
            let writer = provider.create_store(topic, 0, "group", 0);
            let reader = provider.create_store(topic, 0, "group", 0);
            let other = provider.create_store(topic, 1, "group", 0);

            let key: Key = Arc::from(format!("substrate-{name}"));
            // The queue always holds a head, so the read below is a real claim.
            let mut queue: BTreeSet<CompactDateTime> =
                times.into_iter().map(CompactDateTime::from).collect();
            queue.insert(CompactDateTime::from(0_u32));

            let mut queued = queue.iter().copied();
            let first = queued.next().ok_or_else(|| eyre!("queue is empty"))?;
            let trigger =
                |time| Trigger::new(key.clone(), time, TimerType::Application, Span::none());
            writer.defer_first_timer(&trigger(first)).await?;
            for time in queued {
                writer.defer_additional_timer(&trigger(time)).await?;
            }

            let head = reader.get_next_deferred_timer(&key).await?;
            ensure!(
                head.map(|(trigger, count)| (trigger.time, count)) == Some((first, 0)),
                "a second store on the same segment must read the writer's queue"
            );
            ensure!(
                other.get_next_deferred_timer(&key).await?.is_none(),
                "a store on another segment must not read the writer's queue"
            );

            let standalone = MemoryTimerDeferStore::new(SpanRelation::Child);
            let other_standalone = MemoryTimerDeferStore::new(SpanRelation::Child);
            standalone.defer_first_timer(&trigger(first)).await?;
            ensure!(
                other_standalone
                    .get_next_deferred_timer(&key)
                    .await?
                    .is_none(),
                "each standalone store must own its substrate"
            );
            Ok(())
        }))
    }

    QuickCheck::new().quickcheck(property as fn(String, Vec<u32>) -> TestResult);
}

/// A store or setup failure is a broken environment, never a shrinkable
/// property failure.
fn finish(result: Result<()>) -> TestResult {
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:?}")),
    }
}
