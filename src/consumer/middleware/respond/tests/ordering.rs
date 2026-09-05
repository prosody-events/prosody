//! The durable commit order for requesting responses.

use super::{Fixture, ResultProbeCodec, requesting, serialize_count};
use crate::consumer::DemandType;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::respond::RespondHandler;
use crate::consumer::middleware::settle::settle;
use crate::consumer::middleware::tests::test_support::{
    GatedGuard, ScriptedHandler, buffered, committed_value, is_provisional,
};
use crate::peer::router::loopback::paused;
use color_eyre::eyre::bail;
use color_eyre::{Report, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;

/// The durable commit precedes the requesting response.
#[test]
fn the_response_leaves_only_after_the_durable_commit() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let (context, cell_store, cart_id) = buffered(|ctx| ctx).await?;
        let handler = RespondHandler::new(
            LeafHandler::new(ScriptedHandler::success()),
            Arc::clone(&fixture.responder),
        );
        let message = requesting(1, 9, "ordering")?;
        let result =
            FallibleHandler::on_message(&handler, context.clone(), message, DemandType::Normal)
                .await;

        let (guard, entered, release, committed, aborted) = GatedGuard::new();
        async {
            let settled = settle(&handler, context, guard, result);
            tokio::pin!(settled);
            tokio::select! {
                () = &mut settled => bail!("settle finished before the commit gate"),
                report = entered => if report.is_err() {
                    bail!("the guard aborted before the commit gate");
                },
            }

            assert!(
                !is_provisional(&cell_store, &cart_id).await?,
                "the sweep posture promotes before it retires the source"
            );
            assert_eq!(
                serialize_count(),
                0,
                "a response left before the durable commit"
            );

            if release.send(()).is_err() {
                bail!("the guard stopped waiting before the release");
            }
            settled.await;
            Ok::<(), Report>(())
        }
        .await?;

        assert_eq!(committed.load(SeqCst), 1, "the guard did not commit");
        assert_eq!(aborted.load(SeqCst), 0, "the guard aborted");
        assert!(
            committed_value(&cell_store, &cart_id).await?.is_some(),
            "the staged value was not promoted"
        );
        assert_eq!(
            serialize_count(),
            1,
            "the response did not reach the sender"
        );
        drop(handler);
        let drained = fixture.drain().await?;
        assert_eq!(drained.len(), 1, "the response did not reach the transport");
        Ok(())
    })
}
