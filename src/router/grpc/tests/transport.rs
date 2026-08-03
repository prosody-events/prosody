//! What the transport refuses before the peer method can run.
//!
//! The invariant: a frame the transport refuses never reaches the registry, and
//! is reported as a transport failure rather than as a registry outcome. The
//! evidence is the service's own invocation counter — a refused frame leaves it
//! where it was — together with the request staying fillable afterwards.

use super::{
    ALPHA, FRAME_CAP, Harness, OVER_FRAME_BYTES, WIDE_FRAME_CAP, header, payload, register,
    transport,
};
use crate::codec::Codec;
use crate::response::RESPONSE_PROTOCOL_VERSION;
use crate::response::frame::FrameCap;
use crate::response::frame::tests::{CountingCodec, RawFrame};
use crate::router::grpc::TRANSPORT;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::ensure;
use tonic::Code;

/// A short payload, for the cases whose size is not the subject.
const SHORT: usize = 8;

/// A frame over the listener's ceiling is refused by the listener, and the peer
/// method never runs.
///
/// It is sent through the wide sender on purpose: that sender's own encoding
/// ceiling is above the frame, so the refusal cannot be the client's.
#[test]
fn an_oversized_frame_is_refused_by_the_server() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let served = TRANSPORT.served();
        let refused = harness
            .deliver_under(
                &harness.wide,
                FrameCap::new(WIDE_FRAME_CAP)?,
                &header(harness.node, request, ALPHA)?,
                payload(OVER_FRAME_BYTES),
            )
            .await?;
        ensure!(
            refused == Code::OutOfRange,
            "an over-cap frame must be refused as out of range, not {refused:?}"
        );
        ensure!(
            TRANSPORT.served() == served,
            "the peer method must not run for a frame the transport refused"
        );
        let accepted = harness
            .deliver(&header(harness.node, request, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            accepted == Code::Ok,
            "the refused frame must have left the request fillable, but it answered {accepted:?}"
        );
        Ok(())
    })
}

/// The client refuses a frame over its own encoding ceiling before it sends
/// one byte.
///
/// The listener here has a wider ceiling than the sender, so a frame that
/// leaves the process reaches the peer method and moves its counter. That
/// counter is the assertion: without the explicit encoding ceiling the client
/// would happily send, and the call would fail for a different reason.
#[test]
fn the_client_refuses_an_over_cap_frame_before_it_sends() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::with(transport(WIDE_FRAME_CAP)).await?;
        let outcome = async {
            let request = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
            let narrow = super::super::client::GrpcSender::new(FrameCap::new(FRAME_CAP)?, 2);
            let served = TRANSPORT.served();
            let refused = harness
                .deliver_under(
                    &narrow,
                    FrameCap::new(WIDE_FRAME_CAP)?,
                    &header(harness.node, request, ALPHA)?,
                    payload(OVER_FRAME_BYTES),
                )
                .await?;
            // A local encode refusal never reaches a peer, so there is no
            // peer status to report and tonic answers INTERNAL. What separates
            // it from a refusal by the listener is the counter below: this
            // listener's ceiling is wider than the frame, so a frame that left
            // the process would have been served.
            ensure!(
                refused == Code::Internal,
                "the client must refuse an over-cap frame before it sends, not answer {refused:?}"
            );
            ensure!(
                TRANSPORT.served() == served,
                "a frame the client refused must never have left the process"
            );
            Ok(())
        }
        .await;
        harness.stop().await;
        outcome
    })
}

/// A frame stating a protocol version this build does not speak is refused by
/// the reader, counted, and never reaches the peer method.
#[test]
fn an_unknown_protocol_version_is_refused_by_the_decoder() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let request = register(&harness.registry, &[ALPHA], CountingCodec::FORMAT_ID)?;
        let target = harness.node.into_bytes();
        let id = request.into_bytes();
        let raw = RawFrame {
            version: Some(u64::from(RESPONSE_PROTOCOL_VERSION) + 1),
            target: Some(&target),
            request: Some(&id),
            subsystem: Some(ALPHA.as_bytes()),
            ..RawFrame::default()
        };
        let served = TRANSPORT.served();
        let rejected = TRANSPORT.rejected_frames();
        let refused = harness.deliver_raw(&harness.sender, raw.encode()).await?;
        ensure!(
            refused == Code::InvalidArgument,
            "a version this build does not speak must be refused as invalid, not {refused:?}"
        );
        ensure!(
            TRANSPORT.rejected_frames() == rejected + 1,
            "a refused frame must be counted as a transport rejection"
        );
        ensure!(
            TRANSPORT.served() == served,
            "the peer method must not run for a frame the reader refused"
        );
        let accepted = harness
            .deliver(&header(harness.node, request, ALPHA)?, payload(SHORT))
            .await?;
        ensure!(
            accepted == Code::Ok,
            "the refused frame must have left the request fillable, but it answered {accepted:?}"
        );
        Ok(())
    })
}
