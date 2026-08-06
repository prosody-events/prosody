//! What the hook does when the transport cannot take a response.
//!
//! Every suite here keeps a response off the wire in one of four ways: a
//! barrier holds the transport open, the fleet refuses the response, the
//! encoder drops it for the frame cap, or the directory names no address for
//! its node. Each reads the outcome after an explicit drain.

use super::{
    Fixture, OversizedProbeCodec, ResultProbeCodec, offset_tracker, serialize_count, tagged,
};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, ScriptedHook,
};
use crate::consumer::{DemandType, EventHandler};
use crate::error::ErrorCategory;
use crate::response::frame::FrameCap;
use crate::router::loopback::{Script, UNPUBLISHED_NODE, paused};
use color_eyre::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// The apply hook returns while the transport still holds a response, so the
/// next event for the same key dispatches.
///
/// The two dispatches run one after the other on one key, exactly as the
/// partition serializes them. Two shapes of the defect are writable: a hook
/// that awaits the delivery outcome, and a sender that frames and delivers
/// inline instead of queuing. Either one holds the response over the hook, so
/// the counters read after both dispatches state the invariant positively: no
/// response had finished. The deadline only turns a full stall into a failure
/// instead of a hang.
///
/// A full queue is not one of them. A destination's queue is as deep as that
/// destination has slots, so a response that holds a slot always has room, and
/// the queue push cannot wait however it is written.
#[test]
fn the_hook_does_not_block_on_the_network() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new(1, 1)?;
        let barrier = Arc::new(Semaphore::new(0));
        fixture.router.script(1, Script::Hold(Arc::clone(&barrier)));

        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        for (offset, request) in [(0, 11), (1, 12)] {
            let tracker = offset_tracker();
            let message = tagged(1, request, "held")?.into_uncommitted(tracker.take(offset).await?);
            EventHandler::on_message(
                &handler,
                MockEventContext::new(),
                message,
                DemandType::Normal,
            )
            .await;
        }
        drop(handler);

        assert_eq!(
            leaf.call_count(),
            2,
            "the second event dispatched while the first response was held",
        );
        let counters = fixture.responder.counters();
        assert_eq!(
            (counters.sent(), counters.dropped()),
            (0, 0),
            "the queued response was still in flight when both hooks had returned",
        );
        // Releasing the held attempt lets the drain finish rather than wait out
        // the send deadline.
        barrier.add_permits(1);
        let drained = fixture.drain().await?;
        assert_eq!(
            drained.deliveries.len(),
            1,
            "only the held response ever reached the transport",
        );
        Ok(())
    })
}

/// A response with no slot is never encoded, and its result goes back to the
/// handler's own hook.
#[test]
fn a_refused_response_is_never_encoded() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new(1, 1)?;
        let barrier = Arc::new(Semaphore::new(0));
        fixture.router.script(1, Script::Hold(Arc::clone(&barrier)));

        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let queued = tagged(1, 21, "queued")?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            queued,
            DemandType::Normal,
        )
        .await;

        let refused_before = fixture.refused();
        let encodes_before = serialize_count();
        let tracker = offset_tracker();
        let refused = tagged(1, 22, "refused")?.into_uncommitted(tracker.take(1).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            refused,
            DemandType::Normal,
        )
        .await;
        let encodes_after = serialize_count();
        drop(handler);

        let hooks = leaf.hook_events();
        assert_eq!(
            hooks
                .iter()
                .filter(|hook| matches!(hook, ScriptedHook::AfterCommit(_)))
                .count(),
            1,
            "only the refused result reaches the handler's own hook: {hooks:?}",
        );
        assert_eq!(
            encodes_after, encodes_before,
            "a response with no slot is never handed to a codec",
        );
        assert_eq!(
            fixture.refused() - refused_before,
            1,
            "the fleet counted the refusal",
        );

        barrier.add_permits(1);
        let drained = fixture.drain().await?;
        assert_eq!(drained.deliveries.len(), 1, "only the queued response went");
        Ok(())
    })
}

/// A response the frame ceiling refuses sends nothing at all, and nothing
/// stands in for it.
///
/// The refusal itself is pinned at the encoder. This suite is the end-to-end
/// reading: the response is queued, then dropped, and the transport records no
/// attempt. The control response proves a broken worker cannot pass by
/// delivering nothing.
#[test]
fn an_over_limit_response_sends_nothing() -> Result<()> {
    paused()?.block_on(async {
        let cap = FrameCap::new(FrameCap::MIN_BYTES)?;
        let fixture = Fixture::<OversizedProbeCodec>::with_cap(1, 2, cap)?;

        let control = ScriptedHandler::success();
        let handler = fixture.stack(control, 0)?;
        let tracker = offset_tracker();
        let message = tagged(1, 31, "control")?.into_uncommitted(tracker.take(0).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let oversized = ScriptedHandler::always_failing(ErrorCategory::Permanent);
        let handler = fixture.stack(oversized, 0)?;
        let tracker = offset_tracker();
        let message = tagged(1, 32, "oversized")?.into_uncommitted(tracker.take(1).await?);
        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let drained = fixture.drain().await?;
        assert_eq!(
            drained.deliveries.len(),
            1,
            "the control response is delivered and the over-cap one is not",
        );
        assert_eq!(
            (drained.sent, drained.dropped),
            (1, 1),
            "the over-cap response is counted as dropped",
        );
        Ok(())
    })
}

/// A node the directory does not hold is never dialed.
///
/// A request tag holds two identifiers and no host and no port, and the header
/// it builds carries only the target node. The delivery worker resolves that
/// node through the router, so no edit can make an address originate from a
/// Kafka header without inventing a field. What stays testable is the
/// unresolvable node itself.
///
/// The handler's own hook does not fire here: the result moved into the queue
/// and the worker then dropped it. That is what "responding is the disposition
/// of the value" costs, and pinning it keeps the cost visible.
#[test]
fn an_unpublished_node_is_never_dialed() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new(1, 1)?;
        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let message =
            tagged(UNPUBLISHED_NODE, 41, "unpublished")?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let hooks = leaf.hook_events();
        let drained = fixture.drain().await?;
        assert!(
            drained.deliveries.is_empty(),
            "an unresolvable node is never dialed",
        );
        assert_eq!(drained.dropped, 1, "the sender counted the drop");
        assert!(
            !hooks
                .iter()
                .any(|hook| matches!(hook, ScriptedHook::AfterCommit(_))),
            "the queued result never returns to the handler's hook: {hooks:?}",
        );
        Ok(())
    })
}
