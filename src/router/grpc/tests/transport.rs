//! What the frame decoder refuses after Tonic accepts a message.

use super::{ALPHA, Harness, header, payload, register};
use crate::response::RESPONSE_PROTOCOL_VERSION;
use crate::response::frame::tests::RawFrame;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use tonic::Code;

/// A short payload, for the cases whose size is not the subject.
const SHORT: usize = 8;

/// A frame with an unknown protocol version is refused by the reader.
#[test]
fn an_unknown_protocol_version_is_refused_by_the_decoder() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA])?;
        let target = harness.node.into_bytes();
        let id = request.id().into_bytes();
        let raw = RawFrame {
            version: Some(u64::from(RESPONSE_PROTOCOL_VERSION) + 1),
            target: Some(&target),
            request: Some(&id),
            subsystem: Some(ALPHA.as_bytes()),
            ..RawFrame::default()
        };
        let refused = harness.deliver_raw(&harness.sender, raw.encode()).await?;
        ensure!(
            refused == Code::InvalidArgument,
            "a version this build does not speak must be refused as invalid, not {refused:?}"
        );
        let accepted = harness
            .deliver(&header(harness.node, request.id(), ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            accepted == Code::Ok,
            "the refused frame must have left the request fillable, but it answered {accepted:?}"
        );
        Ok(())
    })
}
