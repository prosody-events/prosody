//! Settlement classification tables for the wrappers without their own
//! tests module: the pure pass-throughs (retry mid-stack, log, timeout,
//! telemetry) and the `LeafHandler` chain terminator. Delegation is proven
//! against [`BypassedHandler`], whose classification is `Bypassed` for every
//! result. A wrapper that always uses `Final` fails these rows.
use super::*;
use crate::consumer::middleware::log::LogHandler;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::retry::RetryHandler;
use crate::consumer::middleware::telemetry::TelemetryHandler;
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, ScriptedHandler, TestError as SupportError,
};
use crate::consumer::middleware::timeout::TimeoutHandler;
use crate::consumer::middleware::{Settlement, SettlementHandler};

/// The pure pass-throughs (retry mid-stack, log, timeout, telemetry, the
/// test pass-through) delegate both sides verbatim.
#[test]
fn passthrough_wrappers_delegate_settlement() {
    fn assert_delegates<W, P>(label: &str)
    where
        W: SettlementHandler<Output = (), Error = SupportError>,
        P: SettlementHandler<Output = (), Error = SupportError>,
    {
        let ok: Result<(), SupportError> = Ok(());
        let err: Result<(), SupportError> = Err(SupportError(ErrorCategory::Permanent));
        assert_eq!(W::settlement(ok.as_ref()), Settlement::Final, "{label} Ok");
        assert_eq!(
            W::settlement(err.as_ref()),
            Settlement::Final,
            "{label} Err"
        );
        // Over a Bypassed probe, both sides stay Bypassed — the wrapper
        // is delegating, not hardcoding Final.
        assert_eq!(
            P::settlement(ok.as_ref()),
            Settlement::Bypassed,
            "{label} probe Ok"
        );
        assert_eq!(
            P::settlement(err.as_ref()),
            Settlement::Bypassed,
            "{label} probe Err"
        );
    }

    assert_delegates::<RetryHandler<ScriptedHandler>, RetryHandler<BypassedHandler>>("retry");
    assert_delegates::<LogHandler<ScriptedHandler>, LogHandler<BypassedHandler>>("log");
    assert_delegates::<TimeoutHandler<ScriptedHandler>, TimeoutHandler<BypassedHandler>>("timeout");
    assert_delegates::<TelemetryHandler<ScriptedHandler>, TelemetryHandler<BypassedHandler>>(
        "telemetry",
    );
    assert_delegates::<
        PassThroughMiddleware<ScriptedHandler>,
        PassThroughMiddleware<BypassedHandler>,
    >("pass-through");
}

/// The chain terminator classifies `Final` on both sides — the leaf's
/// result is the event's own outcome, by definition.
#[test]
fn leaf_handler_is_final_on_both_sides() {
    type Subject = LeafHandler<ScriptedHandler>;
    let ok: Result<(), SupportError> = Ok(());
    let err: Result<(), SupportError> = Err(SupportError(ErrorCategory::Permanent));
    assert_eq!(Subject::settlement(ok.as_ref()), Settlement::Final);
    assert_eq!(Subject::settlement(err.as_ref()), Settlement::Final);
}

#[tokio::test]
async fn leaf_dispatches_excise_record_to_excise() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::success();
    let message = create_test_message_from(ConsumerMessageValue {
        record: Record::Excise,
        ..Default::default()
    })?;
    FallibleHandler::on_message(
        &LeafHandler::new(handler.clone()),
        MockEventContext::new(),
        message,
        DemandType::Normal,
    )
    .await?;
    assert_eq!(handler.excision_count(), 1);
    assert_eq!(handler.call_count(), 1);
    Ok(())
}
