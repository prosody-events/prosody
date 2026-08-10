//! What happens to a response between its hook and the wire.

use super::{Harness, PAYLOAD, config, node, paused};
use crate::Codec;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::tests::CountingCodec;
use crate::response::frame::{FrameResult, ResponseSuccess};
use crate::router::loopback::direct_uri;
use color_eyre::Result;

/// The destination these suites address.
const TARGET: u8 = 1;

/// A response reaches the wire with its header and payload intact.
#[test]
fn a_response_reaches_the_wire_intact() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::new(config())?;
        harness.send(TARGET).await?;
        let mut drained = harness.drain().await?;

        let mut delivery = drained
            .deliveries
            .pop()
            .ok_or_else(|| color_eyre::eyre::eyre!("the response made no delivery attempt"))?;
        assert_eq!(
            delivery.uri,
            direct_uri(TARGET)?,
            "the response must reach its target node"
        );

        let frame = decode_frame(&mut delivery.bytes)?;
        assert_eq!(
            frame.header.target,
            node(TARGET),
            "the frame must name its target node"
        );
        let FrameResult::Success(ResponseSuccess { payload, .. }) = frame.result else {
            return Err(color_eyre::eyre::eyre!("the response must succeed"));
        };
        assert_eq!(CountingCodec.deserialize_bytes(payload)?, PAYLOAD);

        assert_eq!(drained.sent, 1);
        Ok(())
    })
}
