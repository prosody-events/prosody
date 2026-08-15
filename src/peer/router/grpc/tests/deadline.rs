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

use crate::peer::router::grpc::deadline::inbound_deadline;
use color_eyre::Result;
use std::time::Duration;
use tokio::time::Instant;
use tonic::Request;
use tonic::metadata::MetadataValue;

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
        let recovered = inbound_deadline(request.metadata()).map(remaining);
        assert_eq!(
            recovered,
            Some(budget),
            "a {budget:?} budget read back as {recovered:?}"
        );
    }
}

/// A caller must state a readable budget.
#[tokio::test(start_paused = true)]
async fn an_absent_or_unreadable_budget_is_rejected() -> Result<()> {
    let plain: Request<()> = Request::new(());
    assert_eq!(inbound_deadline(plain.metadata()), None);

    for value in MALFORMED {
        let mut request = Request::new(());
        drop(
            request
                .metadata_mut()
                .insert("grpc-timeout", MetadataValue::try_from(value)?),
        );
        assert_eq!(
            inbound_deadline(request.metadata()),
            None,
            "the unreadable budget {value:?} was accepted"
        );
    }
    Ok(())
}

/// What is left of a deadline now.
fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}
