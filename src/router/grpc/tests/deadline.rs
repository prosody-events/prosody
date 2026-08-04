//! What a caller's stated budget becomes on arrival.
//!
//! Every case reads the budget back through the same function the service uses,
//! from metadata tonic's own writer produced. A round trip through a parser of
//! this crate's own would prove only that it agrees with itself.

use crate::router::grpc::deadline::inbound_deadline;
use color_eyre::Result;
use std::time::Duration;
use tokio::time::Instant;
use tonic::Request;
use tonic::metadata::MetadataValue;

/// The ceiling every case here is read under. Far above every budget in the
/// table, so nothing in the table is clamped by it.
const CAP: Duration = Duration::from_hours(24);

/// How much wall clock one round trip may spend. It is a measurement tolerance
/// on two readings of the clock, never the assertion: every case also holds the
/// recovered budget to the one the caller stated.
const SLACK: Duration = Duration::from_secs(1);

/// Budgets spanning every unit tonic writes: nanoseconds under a tenth of a
/// second, microseconds up to a hundred seconds, milliseconds above that.
const BUDGETS: [Duration; 8] = [
    Duration::from_nanos(1),
    Duration::from_nanos(99_999_999),
    Duration::from_micros(1),
    Duration::from_millis(500),
    Duration::from_secs(2),
    Duration::from_secs(30),
    Duration::from_secs(90),
    Duration::from_hours(5),
];

/// Values no caller of this protocol can have meant.
const MALFORMED: [&str; 6] = ["", "S", "abc", "12X", "999999999S", "1.5S"];

/// A budget a caller stated is the budget this process reads back.
///
/// Each case is written by tonic and read by this crate, so a unit either side
/// reads differently is a real disagreement rather than a self-consistent
/// mistake. A parser that read one unit as another would recover a budget
/// larger than the caller stated, which the first assertion refuses.
#[test]
fn a_stated_budget_reads_back_as_the_budget_the_caller_stated() {
    for budget in BUDGETS {
        let mut request = Request::new(());
        request.set_timeout(budget);
        let recovered = remaining(inbound_deadline(request.metadata(), CAP));
        assert!(
            recovered <= budget,
            "a {budget:?} budget read back as {recovered:?}, which is more than the caller granted"
        );
        assert!(
            budget.saturating_sub(recovered) < SLACK,
            "a {budget:?} budget read back as {recovered:?}, far short of what the caller granted"
        );
    }
}

/// A caller that states no budget, or one this build cannot read, gets this
/// process's own ceiling. That is what bounds a forward for a caller that
/// bounded nothing.
#[test]
fn an_absent_or_unreadable_budget_becomes_this_process_ceiling() -> Result<()> {
    let plain: Request<()> = Request::new(());
    let recovered = remaining(inbound_deadline(plain.metadata(), CAP));
    assert!(
        CAP.saturating_sub(recovered) < SLACK,
        "an absent budget read back as {recovered:?} rather than the {CAP:?} ceiling"
    );

    for value in MALFORMED {
        let mut request = Request::new(());
        drop(
            request
                .metadata_mut()
                .insert("grpc-timeout", MetadataValue::try_from(value)?),
        );
        let recovered = remaining(inbound_deadline(request.metadata(), CAP));
        assert!(
            CAP.saturating_sub(recovered) < SLACK,
            "the unreadable budget {value:?} read back as {recovered:?} rather than the ceiling"
        );
    }
    Ok(())
}

/// A caller that asks for years gets this process's ceiling, and the instant it
/// asks for never overflows.
#[test]
fn a_budget_beyond_the_ceiling_is_held_to_the_ceiling() -> Result<()> {
    for value in ["99999999H", "99999999M", "99999999S"] {
        let mut request = Request::new(());
        drop(
            request
                .metadata_mut()
                .insert("grpc-timeout", MetadataValue::try_from(value)?),
        );
        let recovered = remaining(inbound_deadline(request.metadata(), CAP));
        assert!(
            recovered <= CAP,
            "the budget {value:?} read back as {recovered:?}, past the {CAP:?} ceiling"
        );
        assert!(
            CAP.saturating_sub(recovered) < SLACK,
            "the budget {value:?} read back as {recovered:?} rather than the ceiling"
        );
    }
    Ok(())
}

/// What is left of a deadline now.
fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}
