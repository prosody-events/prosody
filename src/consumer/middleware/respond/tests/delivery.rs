//! What the hook does when the transport cannot take a response.
//!
//! A barrier holds the transport, or the directory does not contain the
//! destination.

use super::{Fixture, ResultProbeCodec, offset_tracker, requesting, requesting_at};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, ScriptedHandler, ScriptedHook,
};
use crate::consumer::{DemandType, EventHandler};
use crate::peer::router::loopback::{Script, UNPUBLISHED_PEER, paused};
use color_eyre::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// An expired header does not suppress work before the gRPC transport.
#[test]
fn the_request_deadline_only_reaches_the_transport() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let handler = fixture.stack(ScriptedHandler::success(), 0)?;
        let tracker = offset_tracker();
        let message = requesting_at(1, 10, "expired", 0)?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        assert_eq!(fixture.drain().await?.len(), 1);
        Ok(())
    })
}

/// The apply hook waits for response delivery.
#[test]
fn the_hook_applies_network_backpressure() -> Result<()> {
    paused()?.block_on(async {
        let mut fixture = Fixture::<ResultProbeCodec>::new()?;
        let barrier = Arc::new(Semaphore::new(0));
        fixture
            .router
            .script(1, Script::Hold(Arc::clone(&barrier)))?;

        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let message = requesting(1, 11, "held")?.into_uncommitted(tracker.take(0).await?);
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

/// A peer the directory does not hold is never dialed.
///
/// A result request holds two identifiers and no host and no port, and the
/// header it builds carries only the target peer. The route resolves that
/// peer through the router, so no edit can make an address originate from a
/// Kafka header without inventing a field. What stays testable is the
/// unresolvable peer itself.
///
/// The handler's own hook fires even when the response route rejects the value.
#[test]
fn an_unpublished_peer_is_never_dialed() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let message = requesting(UNPUBLISHED_PEER, 41, "unpublished")?
            .into_uncommitted(tracker.take(0).await?);

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
        assert!(drained.is_empty(), "an unresolvable peer is never dialed");
        assert!(
            hooks
                .iter()
                .any(|hook| matches!(hook, ScriptedHook::AfterCommit(Ok(())))),
            "the handler's commit hook did not receive the result: {hooks:?}",
        );
        Ok(())
    })
}
