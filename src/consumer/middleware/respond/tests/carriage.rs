//! What the layer's result carrier holds, and what it tells the settlement
//! boundary.

use super::super::{RespondHandler, Responded};
use super::{Fixture, ResultProbeCodec, offset_tracker, requesting};
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, MockEventContext, ScriptedHandler, TestError,
};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::consumer::{DemandType, EventHandler};
use crate::error::ErrorCategory;
use crate::response::RequestId;
use crate::response::frame::decode::decode_frame;
use crate::response::frame::{FrameResult, HandlerError};
use crate::router::loopback::{TestRouter, paused, peer};
use color_eyre::Result;

/// The request reaches the wire from the error arm and the success arm.
///
/// A permanent rejection is the case where a failure answer helps a requester
/// most: nothing else will come, and only the responder knows why.
#[test]
fn metadata_rides_the_error_arm() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::always_failing(ErrorCategory::Permanent);
        let handler = fixture.stack(leaf, 0)?;
        let tracker = offset_tracker();
        let message = requesting(2, 9, "rejected")?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let mut drained = fixture.drain().await?;
        assert_eq!(drained.len(), 1);
        let mut delivery = drained.remove(0);
        let frame = decode_frame(&mut delivery.bytes)?;
        assert_eq!(
            frame.header.target,
            peer(2),
            "the request names the destination"
        );
        assert_eq!(frame.header.request, RequestId::from_bytes([9; 16]));
        assert_eq!(frame.header.subsystem.as_str(), super::SUBSYSTEM);
        let FrameResult::HandlerError(HandlerError { category, message }) = frame.result else {
            return Err(color_eyre::eyre::eyre!(
                "the failed handler returned success"
            ));
        };
        assert_eq!(category, ErrorCategory::Permanent);
        assert_eq!(message, b"test error (Permanent)".as_slice());
        Ok(())
    })
}

/// The layer answers the settlement boundary with its inner handler's own
/// classification, on both result arms.
///
/// Production always nests the layer around the chain's leaf adapter, whose
/// classification is final on both arms. The bypassed rows therefore pin this
/// implementation rather than a reachable production state — the layer stays
/// correct over any inner handler, which is why it delegates instead of
/// answering final.
#[test]
fn settlement_delegates_both_result_arms() {
    let output = Responded {
        inner: (),
        meta: None,
    };
    let failure = Responded {
        inner: TestError(ErrorCategory::Permanent),
        meta: None,
    };

    let bypassed = [
        RespondHandler::<BypassedHandler, ResultProbeCodec, TestRouter>::settlement(Ok(&output)),
        RespondHandler::<BypassedHandler, ResultProbeCodec, TestRouter>::settlement(Err(&failure)),
    ];
    assert_eq!(
        bypassed,
        [Settlement::Bypassed, Settlement::Bypassed],
        "a bypassing inner keeps its classification through the layer",
    );

    let settled = [
        RespondHandler::<ScriptedHandler, ResultProbeCodec, TestRouter>::settlement(Ok(&output)),
        RespondHandler::<ScriptedHandler, ResultProbeCodec, TestRouter>::settlement(Err(&failure)),
    ];
    assert_eq!(
        settled,
        [Settlement::Final, Settlement::Final],
        "a settling inner keeps its classification through the layer",
    );
}
