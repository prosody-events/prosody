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
use crate::consumer::middleware::tests::test_support::settlement_name;
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
        assert_eq!(
            settlement_name(W::settlement(ok.as_ref())),
            "Final",
            "{label} Ok"
        );
        assert_eq!(
            settlement_name(W::settlement(err.as_ref())),
            "Rejected",
            "{label} Err"
        );
        // Over a Bypassed probe, both sides stay Bypassed — the wrapper
        // is delegating, not hardcoding Final.
        assert_eq!(
            settlement_name(P::settlement(ok.as_ref())),
            "Bypassed",
            "{label} probe Ok"
        );
        assert_eq!(
            settlement_name(P::settlement(err.as_ref())),
            "Bypassed",
            "{label} probe Err"
        );
    }

    assert_delegates::<RetryHandler<LeafHandler<ScriptedHandler>>, RetryHandler<BypassedHandler>>(
        "retry",
    );
    assert_delegates::<LogHandler<LeafHandler<ScriptedHandler>>, LogHandler<BypassedHandler>>(
        "log",
    );
    assert_delegates::<TimeoutHandler<LeafHandler<ScriptedHandler>>, TimeoutHandler<BypassedHandler>>(
        "timeout",
    );
    assert_delegates::<
        TelemetryHandler<LeafHandler<ScriptedHandler>>,
        TelemetryHandler<BypassedHandler>,
    >("telemetry");
    assert_delegates::<
        PassThroughMiddleware<LeafHandler<ScriptedHandler>>,
        PassThroughMiddleware<BypassedHandler>,
    >("pass-through");
}

/// The leaf maps success and all three error categories to distinct actions.
#[test]
fn leaf_handler_maps_error_categories() {
    type Subject = LeafHandler<ScriptedHandler>;
    assert!(matches!(Subject::settlement(Ok(&())), Settlement::Final(_)));
    assert!(matches!(
        Subject::settlement(Err(&SupportError(ErrorCategory::Permanent))),
        Settlement::Rejected(_)
    ));
    assert_eq!(
        Subject::settlement(Err(&SupportError(ErrorCategory::Transient))),
        Settlement::Bypassed
    );
    assert_eq!(
        Subject::settlement(Err(&SupportError(ErrorCategory::Terminal))),
        Settlement::Abandoned
    );
}

#[tokio::test]
async fn leaf_dispatches_excise_to_excise() -> color_eyre::Result<()> {
    let handler = ScriptedHandler::success();
    let message = create_test_message_from(ConsumerMessageValue {
        payload: (),
        ..Default::default()
    })?;
    FallibleHandler::on_excise(
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
