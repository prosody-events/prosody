//! Scheduler settlement classification: the admission split between
//! delegated inner results and the pre-inner permit rejection.

use super::dispatch::DispatchError;
use super::{SchedulerError, SchedulerHandler};
use crate::consumer::middleware::tests::test_support::{
    BypassedHandler, ScriptedHandler, TestError,
};
use crate::consumer::middleware::{Settlement, SettlementHandler};
use crate::error::ErrorCategory;

/// The settlement classification table: inner results delegate; a permit
/// rejection (pre-inner) is `Bypassed`. Delegation is proven against a
/// `Bypassed`-classifying probe.
#[test]
fn settlement_classification_table() {
    type Subject = SchedulerHandler<ScriptedHandler>;
    type Probe = SchedulerHandler<BypassedHandler>;
    type TableErr = SchedulerError<TestError>;

    let rows: Vec<(&str, Result<(), TableErr>, Settlement)> = vec![
        (
            "Ok delegates to the leaf's Final",
            Ok(()),
            Settlement::Final,
        ),
        (
            "Handler delegates to the leaf's Final",
            Err(SchedulerError::Handler(TestError(ErrorCategory::Permanent))),
            Settlement::Final,
        ),
        (
            "PermitAcquisition (pre-inner admission) is Bypassed",
            Err(SchedulerError::PermitAcquisition(DispatchError::Shutdown)),
            Settlement::Bypassed,
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(Subject::settlement(result.as_ref()), expected, "{label}");
    }

    // Delegation proof: over a Bypassed-classifying inner the delegating
    // rows stay Bypassed.
    let ok: Result<(), TableErr> = Ok(());
    assert_eq!(Probe::settlement(ok.as_ref()), Settlement::Bypassed);
    let inner_err: Result<(), TableErr> =
        Err(SchedulerError::Handler(TestError(ErrorCategory::Permanent)));
    assert_eq!(Probe::settlement(inner_err.as_ref()), Settlement::Bypassed);
}
