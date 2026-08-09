//! What happens to a response between its hook and the wire.

use super::{Harness, PAYLOAD, config, node, paused, port};
use crate::Codec;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::tests::CountingCodec;
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
            delivery.port,
            port(TARGET),
            "the response must reach its target node"
        );

        let mut frame = decode_frame(&mut delivery.bytes)?;
        assert_eq!(
            frame.header.target,
            node(TARGET),
            "the frame must name its target node"
        );
        assert_eq!(
            CountingCodec.deserialize(&mut frame.payload)?,
            PAYLOAD,
            "the frame must carry the sent response"
        );

        assert_eq!(drained.sent, 1);
        Ok(())
    })
}
