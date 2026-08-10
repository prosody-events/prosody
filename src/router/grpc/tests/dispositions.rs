//! Delivery status parity across the gRPC boundary.

use super::{ALPHA, Harness, header, payload, register};
use crate::response::RequestId;
use crate::response::frame::{FrameResult, ResponseSuccess};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use tonic::Code;

const SHORT: usize = 8;

#[test]
fn the_wire_reports_exact_waiter_consumption() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let mut request = register(&harness.registry, &[ALPHA])?;
        let id = request.id();
        let receiver = request.receiver()?;
        let sent = payload(SHORT);

        let accepted = harness
            .deliver(&header(harness.node, id, ALPHA)?, sent.clone())
            .await?;
        ensure!(
            accepted == Code::Ok,
            "the matching waiter rejected its response"
        );

        let frame = receiver.await?;
        let FrameResult::Success(ResponseSuccess {
            payload: answer, ..
        }) = frame.result
        else {
            return Err(color_eyre::eyre::eyre!(
                "the waiter received a handler error"
            ));
        };
        ensure!(answer.as_ref() == sent, "the waiter received other bytes");

        let repeated = harness
            .deliver(&header(harness.node, id, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            repeated == Code::NotFound,
            "a consumed waiter accepted twice"
        );

        let unknown = harness
            .deliver(
                &header(harness.node, RequestId::new(), ALPHA)?,
                payload(SHORT),
            )
            .await?;
        ensure!(unknown == Code::NotFound, "an unknown request was accepted");
        Ok(())
    })
}

#[test]
fn a_closed_receiver_reports_not_found() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let mut request = register(&harness.registry, &[ALPHA])?;
        let id = request.id();
        drop(request.receiver()?);
        let status = harness
            .deliver(&header(harness.node, id, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            status == Code::NotFound,
            "a closed receiver reported another status"
        );
        Ok(())
    })
}
