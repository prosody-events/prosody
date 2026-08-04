//! What a caller's stated budget becomes on arrival.
//!
//! Every case reads the budget back through the same function the service uses,
//! from metadata tonic's own writer produced. A round trip through a parser of
//! this crate's own would prove only that it agrees with itself.
//!
//! Time is paused, so the two clock readings inside one case are the same
//! instant and each recovered budget is exact. Every row of the table then
//! bites: under a wall-clock tolerance a parser that answered zero, or one that
//! read milliseconds as microseconds, would satisfy most of them.

use crate::router::grpc::deadline::inbound_deadline;
use color_eyre::Result;
use std::time::Duration;
use tokio::time::Instant;
use tonic::Request;
use tonic::metadata::MetadataValue;

/// The ceiling every case here is read under. Far above every budget in the
/// table, so nothing in the table is clamped by it.
const CAP: Duration = Duration::from_hours(24);

/// A budget under [`CAP`], and the header value a caller states it in. Only an
/// unclamped read gives this duration back.
const UNDER_CEILING: (&str, Duration) = ("86399S", Duration::from_secs(86_399));

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
/// mistake. A parser that read one unit as another would recover a different
/// budget, which the assertion refuses whichever way the two disagree.
#[tokio::test(start_paused = true)]
async fn a_stated_budget_reads_back_as_the_budget_the_caller_stated() {
    for budget in BUDGETS {
        let mut request = Request::new(());
        request.set_timeout(budget);
        let recovered = remaining(inbound_deadline(request.metadata(), CAP));
        assert_eq!(
            recovered, budget,
            "a {budget:?} budget read back as {recovered:?}"
        );
    }
}

/// A caller that states no budget, or one this build cannot read, gets this
/// process's own ceiling. That is what bounds a forward for a caller that
/// bounded nothing.
#[tokio::test(start_paused = true)]
async fn an_absent_or_unreadable_budget_becomes_this_process_ceiling() -> Result<()> {
    let plain: Request<()> = Request::new(());
    let recovered = remaining(inbound_deadline(plain.metadata(), CAP));
    assert_eq!(
        recovered, CAP,
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
        assert_eq!(
            recovered, CAP,
            "the unreadable budget {value:?} read back as {recovered:?} rather than the ceiling"
        );
    }
    Ok(())
}

/// A caller that asks for years gets this process's ceiling, and the instant it
/// asks for never overflows.
///
/// The last row is the one the ceiling alone can satisfy. Every other row here
/// reads back as the ceiling, which is also what a caller that stated nothing
/// gets, so those rows cannot tell a clamped budget from a rejected one. A
/// budget one second under the ceiling reads back unclamped, and a parser that
/// held it to the ceiling anyway — or that refused it for its size — is refused
/// here.
#[tokio::test(start_paused = true)]
async fn a_budget_is_held_to_the_ceiling_only_when_it_is_over_it() -> Result<()> {
    assert!(
        UNDER_CEILING.1 < CAP,
        "the row that proves the ceiling is a clamp must state a budget under it"
    );
    for (value, expected) in [
        ("99999999H", CAP),
        ("99999999M", CAP),
        ("99999999S", CAP),
        UNDER_CEILING,
    ] {
        let mut request = Request::new(());
        drop(
            request
                .metadata_mut()
                .insert("grpc-timeout", MetadataValue::try_from(value)?),
        );
        let recovered = remaining(inbound_deadline(request.metadata(), CAP));
        assert_eq!(
            recovered, expected,
            "the budget {value:?} read back as {recovered:?} rather than {expected:?}"
        );
    }
    Ok(())
}

/// What is left of a deadline now.
fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}
