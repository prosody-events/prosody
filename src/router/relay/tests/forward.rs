//! When a process answers for a frame it sent on, and what that answer says.

use super::{BUDGET, Process, frame};
use crate::response::ResponseDisposition;
use crate::router::RelayHop;
use crate::router::SendFailure;
use crate::router::loopback::{Script, config, node, paused, port};
use color_eyre::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// The node every case here sends a frame on to. It is not the process under
/// test, so every frame is forwarded rather than accepted.
const ELSEWHERE: u8 = 3;

/// Cells and slots the fleet behind each case holds.
const CELLS: usize = 4;
const SLOTS: usize = 2;

/// The budget one caller states in the deadline case. Any value works: the
/// target never answers, so the budget is what ends the forward.
const GRANTED: Duration = Duration::from_millis(500);

/// The budget of a caller that left none at all.
const SPENT: Duration = Duration::ZERO;

/// A process answers for a forward only once that forward is over, and it makes
/// one attempt at it.
///
/// The target refuses every attempt for as long as anything asks, so an `OK`
/// here could only mean the process answered before it knew. Counting the
/// attempts is what makes three cases separable: it answered without forwarding
/// at all, it forwarded once and the forward failed, or it forwarded again on
/// an attempt budget of its own.
#[test]
fn a_failed_forward_is_never_answered_as_a_delivery() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut process = Process::new(config(CELLS, SLOTS), BUDGET)?;
        let request = process.expects()?;
        process.router.script(
            ELSEWHERE,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );

        let answered = process
            .deliver(frame(node(ELSEWHERE), request, None)?, None)
            .await?;
        assert_eq!(
            answered,
            ResponseDisposition::Unreachable.status(),
            "a forward that reached nothing must answer UNAVAILABLE, not {answered:?}"
        );
        let recorded = process.recorded();
        assert_eq!(
            recorded.map(|delivery| delivery.port),
            Some(port(ELSEWHERE)),
            "the process must have attempted the forward before it answered"
        );
        assert!(
            process.recorded().is_none(),
            "a relay must make one attempt: the responder that sent this frame keeps its own \
             attempt budget, and a relay that retried would multiply it"
        );
        assert!(
            !process.stored(request)?,
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
        let mut process = Process::new(config(CELLS, SLOTS), BUDGET)?;
        let request = process.expects()?;
        let fleet = Arc::clone(process.router.fleet());

        let answered = process
            .deliver(frame(node(ELSEWHERE), request, None)?, Some(SPENT))
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
        assert_eq!(
            fleet.live_count(),
            0,
            "a frame with no budget left must admit no destination"
        );
        Ok(())
    })
}

/// A forward that cannot finish inside the caller's budget ends at that budget.
///
/// The target holds every attempt and nothing releases it, so the only thing
/// that can end this call is the deadline the caller's own budget set. Paused
/// time makes that deterministic: the runtime has one timer left and advances
/// to it.
#[test]
fn a_forward_with_no_time_left_answers_deadline_exceeded() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut process = Process::new(config(CELLS, SLOTS), BUDGET)?;
        let request = process.expects()?;
        process
            .router
            .script(ELSEWHERE, Script::Hold(Arc::new(Semaphore::new(0))));

        let answered = process
            .deliver(frame(node(ELSEWHERE), request, None)?, Some(GRANTED))
            .await?;
        assert_eq!(
            answered,
            ResponseDisposition::RelayDeadlineExceeded.status(),
            "a forward that outlived the caller's budget must answer DEADLINE_EXCEEDED, not \
             {answered:?}"
        );
        assert!(
            process.recorded().is_some(),
            "the held attempt must have reached the transport before the budget ended it"
        );
        Ok(())
    })
}
