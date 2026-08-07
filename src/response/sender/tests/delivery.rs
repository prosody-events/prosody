//! What happens to a response between its hook and the wire.

use super::{CAP_BYTES, Harness, PAYLOAD, attempts, config, node, paused, port};
use crate::Codec;
use crate::response::frame::FrameCap;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::tests::{CountingCodec, serialized_on_this_thread};
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

        let mut frame = decode_frame(&mut delivery.bytes, FrameCap::new(CAP_BYTES)?)?;
        assert_eq!(
            frame.header.target,
            node(TARGET),
            "the frame must name its target node"
        );
        assert_eq!(
            CountingCodec::default().deserialize(&mut frame.payload)?,
            PAYLOAD,
            "the frame must carry the sent response"
        );

        assert_eq!(drained.sent, 1);
        Ok(())
    })
}

/// A response whose deadline passed is dropped before it is encoded.
#[test]
fn an_expired_response_is_never_encoded() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::new(config())?;
        let serialized = serialized_on_this_thread();
        harness.run_expired(TARGET).await?;

        let drained = harness.drain().await?;
        assert_eq!(
            serialized_on_this_thread() - serialized,
            0,
            "the expired response must never be encoded"
        );
        assert_eq!(
            attempts(&drained.deliveries, TARGET),
            0,
            "the expired response must never reach the transport"
        );
        Ok(())
    })
}
