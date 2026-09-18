//! Scheduler settlement classification: the admission split between
//! delegated inner results and the pre-inner permit rejection.

use super::dispatch::DispatchError;
use super::{SchedulerError, SchedulerHandler};
use crate::consumer::middleware::SettlementHandler;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::tests::test_support::settlement_name;
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, ScriptedHandler, TestError,
};
use crate::error::ErrorCategory;

/// The settlement classification table: inner results delegate; a permit
/// rejection (pre-inner) is `Abandoned`. Delegation is proven against a
/// `Bypassed`-classifying probe.
#[test]
fn settlement_classification_table() {
    type Subject = SchedulerHandler<LeafHandler<ScriptedHandler>>;
    type Probe = SchedulerHandler<BypassedHandler>;
    type TableErr = SchedulerError<TestError>;

    let rows: Vec<(&str, Result<(), TableErr>, &str)> = vec![
        ("Ok delegates to the leaf's Final", Ok(()), "Final"),
        (
            "Handler delegates to the leaf's Rejected",
            Err(SchedulerError::Handler(TestError(ErrorCategory::Permanent))),
            "Rejected",
        ),
        (
            "Terminal inner is Abandoned",
            Err(SchedulerError::Handler(TestError(ErrorCategory::Terminal))),
            "Abandoned",
        ),
        (
            "PermitAcquisition (pre-inner admission) is Abandoned",
            Err(SchedulerError::PermitAcquisition(DispatchError::Shutdown)),
            "Abandoned",
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(
            settlement_name(Subject::settlement(result.as_ref())),
            expected,
            "{label}"
        );
    }

    // Delegation proof: over a Bypassed-classifying inner the delegating
    // rows stay Bypassed.
    let ok: Result<(), TableErr> = Ok(());
    assert_eq!(settlement_name(Probe::settlement(ok.as_ref())), "Bypassed");
    let inner_err: Result<(), TableErr> =
        Err(SchedulerError::Handler(TestError(ErrorCategory::Permanent)));
    assert_eq!(
        settlement_name(Probe::settlement(inner_err.as_ref())),
        "Bypassed"
    );
}
