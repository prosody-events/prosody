//! What happens to a response between its hook and the wire.

use super::{Harness, PAYLOAD, paused, peer};
use crate::Codec;
use crate::peer::response::frame::decode::decode_frame;
use crate::peer::response::frame::tests::CountingCodec;
use crate::peer::response::frame::{FrameResult, ResponseSuccess};
use crate::peer::router::loopback::direct_uri;
use color_eyre::Result;

/// The destination these suites address.
const TARGET: u8 = 1;

/// A response reaches the wire with its header and payload intact.
#[test]
fn a_response_reaches_the_wire_intact() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::new()?;
        harness.send(TARGET).await?;
        let mut drained = harness.drain().await?;

        let mut delivery = drained
            .deliveries
            .pop()
            .ok_or_else(|| color_eyre::eyre::eyre!("the response made no delivery attempt"))?;
        assert_eq!(
            delivery.uri,
            direct_uri(TARGET)?,
            "the response must reach its target peer"
        );

        let frame = decode_frame(&mut delivery.bytes)?;
        assert_eq!(
            frame.header.target,
            peer(TARGET),
            "the frame must name its target peer"
        );
        let FrameResult::Success(ResponseSuccess { payload, .. }) = frame.result else {
            return Err(color_eyre::eyre::eyre!("the response must succeed"));
        };
        assert_eq!(CountingCodec.deserialize_bytes(payload)?, PAYLOAD);

        assert_eq!(drained.sent, 1);
        Ok(())
    })
}
