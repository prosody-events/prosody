//! What the layer's result carrier holds, and what it tells the settlement
//! boundary.

use super::super::{RespondHandler, Responded};
use super::{Fixture, ResultProbeCodec, cap, offset_tracker, tagged};
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, MockEventContext, ScriptedHandler, TestError,
};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::consumer::{DemandType, EventHandler};
use crate::error::ErrorCategory;
use crate::response::frame::decode::decode_frame;
use crate::response::{RequestId, ResponseStatus};
use crate::router::loopback::{node, paused};
use color_eyre::Result;

/// The tag reaches the wire from the error arm as well as the success arm.
///
/// A permanent rejection is the case where a failure answer helps a requester
/// most: nothing else will come, and only the responder knows why.
#[test]
fn metadata_rides_the_error_arm() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new(1, 1)?;
        let leaf = ScriptedHandler::always_failing(ErrorCategory::Permanent);
        let handler = fixture.stack(leaf, 0)?;
        let tracker = offset_tracker();
        let message = tagged(2, 9, "rejected")?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let mut drained = fixture.drain().await?;
        assert_eq!(
            (drained.deliveries.len(), drained.sent, drained.dropped),
            (1, 1, 0),
        );
        let mut delivery = drained.deliveries.remove(0);
        let frame = decode_frame(&mut delivery.bytes, cap()?)?;
        assert_eq!(
            frame.header.target,
            node(2),
            "the tag names the destination"
        );
        assert_eq!(frame.header.request, RequestId::from_bytes([9; 16]));
        assert_eq!(frame.header.subsystem.as_str(), super::SUBSYSTEM);
        assert_eq!(
            frame.header.status,
            ResponseStatus::Error(ErrorCategory::Permanent),
            "the category labels the frame",
        );
        assert_eq!(
            frame.payload.first().copied(),
            Some(i32::from(ErrorCategory::Permanent) as u8),
            "the error arm itself crossed the wire",
        );
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
        RespondHandler::<BypassedHandler, ResultProbeCodec>::settlement(Ok(&output)),
        RespondHandler::<BypassedHandler, ResultProbeCodec>::settlement(Err(&failure)),
    ];
    assert_eq!(
        bypassed,
        [Settlement::Bypassed, Settlement::Bypassed],
        "a bypassing inner keeps its classification through the layer",
    );

    let settled = [
        RespondHandler::<ScriptedHandler, ResultProbeCodec>::settlement(Ok(&output)),
        RespondHandler::<ScriptedHandler, ResultProbeCodec>::settlement(Err(&failure)),
    ];
    assert_eq!(
        settled,
        [Settlement::Final, Settlement::Final],
        "a settling inner keeps its classification through the layer",
    );
}
