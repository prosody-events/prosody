//! What the hook does when the transport cannot take a response.
//!
//! A barrier holds the transport, the encoder rejects a large frame, or the
//! directory does not contain the destination.

use super::{Fixture, OversizedProbeCodec, ResultProbeCodec, offset_tracker, tagged};
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

/// The apply hook waits for response delivery.
#[test]
fn the_hook_applies_network_backpressure() -> Result<()> {
    paused()?.block_on(async {
        let mut fixture = Fixture::<ResultProbeCodec>::new()?;
        let barrier = Arc::new(Semaphore::new(0));
        fixture.router.script(1, Script::Hold(Arc::clone(&barrier)));

        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let message = tagged(1, 11, "held")?.into_uncommitted(tracker.take(0).await?);
        let mut dispatch = Box::pin(EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        ));
        tokio::select! {
            () = &mut dispatch => return Err(color_eyre::eyre::eyre!(
                "the apply hook returned before response delivery"
            )),
            delivery = fixture.deliveries.recv() => assert!(delivery.is_some()),
        }
        assert!(
            leaf.hook_events()
                .contains(&ScriptedHook::AfterCommit(Ok(()))),
            "response delivery started before the handler's commit hook"
        );
        barrier.add_permits(1);
        dispatch.await;
        drop(handler);
        assert!(fixture.drain().await?.is_empty());
        Ok(())
    })
}

/// A response the frame ceiling refuses sends nothing at all, and nothing
/// stands in for it.
///
/// The refusal itself is pinned at the encoder. This suite is the end-to-end
/// reading: the sender rejects the response, and the transport records no
/// attempt. The control response proves that the transport works.
#[test]
fn an_over_limit_response_sends_nothing() -> Result<()> {
    paused()?.block_on(async {
        let cap = FrameCap::new(FrameCap::MIN_BYTES)?;
        let fixture = Fixture::<OversizedProbeCodec>::with_cap(cap)?;

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
            drained.len(),
            1,
            "the control response is delivered and the over-cap one is not",
        );
        Ok(())
    })
}

/// A node the directory does not hold is never dialed.
///
/// A request tag holds two identifiers and no host and no port, and the header
/// it builds carries only the target node. The route resolves that
/// node through the router, so no edit can make an address originate from a
/// Kafka header without inventing a field. What stays testable is the
/// unresolvable node itself.
///
/// The handler's own hook fires even when the response route rejects the value.
#[test]
fn an_unpublished_node_is_never_dialed() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
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
        assert!(drained.is_empty(), "an unresolvable node is never dialed");
        assert!(
            hooks
                .iter()
                .any(|hook| matches!(hook, ScriptedHook::AfterCommit(Ok(())))),
            "the handler's commit hook did not receive the result: {hooks:?}",
        );
        Ok(())
    })
}
