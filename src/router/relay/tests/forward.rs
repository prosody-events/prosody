//! When a process answers for a frame it sent on, and what that answer says.

use super::{BUDGET, Process, frame};
use crate::response::ResponseDisposition;
use crate::router::SendFailure;
use crate::router::loopback::{Script, config, direct_uri, paused, peer};
use color_eyre::Result;
use std::time::Duration;

/// The peer every case here sends a frame on to. It is not the process under
/// test, so every frame is forwarded rather than accepted.
const ELSEWHERE: u8 = 3;

/// The budget of a caller that left none at all.
const SPENT: Duration = Duration::ZERO;

/// A process answers for a forward only once that forward is over, and it makes
/// one attempt at it.
///
/// The target refuses every attempt for as long as anything asks, so an `OK`
/// here could only mean the process answered before it knew. Counting the
/// attempts separates an early answer from one failed forwarding attempt.
#[test]
fn a_failed_forward_is_never_answered_as_a_delivery() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut process = Process::new(config())?;
        let mut request = process.expects()?;
        process.router.script(
            ELSEWHERE,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        )?;

        let answered = process
            .deliver(frame(peer(ELSEWHERE), request.id(), None)?, BUDGET)
            .await?;
        assert_eq!(
            answered,
            ResponseDisposition::Unreachable.status(),
            "a forward that reached nothing must answer UNAVAILABLE, not {answered:?}"
        );
        let recorded = process.recorded();
        assert_eq!(
            recorded.map(|delivery| delivery.uri),
            Some(direct_uri(ELSEWHERE)?),
            "the process must have attempted the forward before it answered"
        );
        assert!(
            process.recorded().is_none(),
            "a relay must make one attempt"
        );
        assert!(
            !request.received(),
            "a frame for another process must never reach this one's registry"
        );
        Ok(())
    })
}

/// A frame that arrives with its budget already spent reserves nothing.
///
/// The caller states a budget of zero, so there is no time to reach anybody in.
/// The answer is the deadline rather than anything about capacity, and the
/// table is left as it was: a process that admitted the target first would let
/// whoever reaches it churn the table with frames that could never be
/// delivered, one evicted destination at a time.
#[test]
fn a_frame_that_arrives_with_no_budget_reserves_nothing() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut process = Process::new(config())?;
        let request = process.expects()?;

        let answered = process
            .deliver(frame(peer(ELSEWHERE), request.id(), None)?, SPENT)
            .await?;
        assert_eq!(
            answered,
            ResponseDisposition::RelayDeadlineExceeded.status(),
            "a frame with no budget left must answer DEADLINE_EXCEEDED, not {answered:?}"
        );
        assert!(
            process.recorded().is_none(),
            "a frame with no budget left must reach no transport"
        );
        Ok(())
    })
}
