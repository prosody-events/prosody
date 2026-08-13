//! Which apply hook fired decides whether a response happens.
//!
//! Every suite here drives the real composed stack — log outside retry, retry
//! outside respond — and reads the transport after an explicit drain.

use super::{Fixture, ResultProbeCodec, offset_tracker, plain, requesting};
use crate::consumer::middleware::tests::test_support::{
    MockEventContext, RecordingTimer, ScriptedHandler, ScriptedHook, create_test_trigger,
};
use crate::consumer::{DemandType, EventHandler};
use crate::error::ErrorCategory;
use crate::peer::response::frame::tests::decode_frame;
use crate::peer::response::frame::{FrameResult, HandlerError};
use crate::peer::router::loopback::{Delivery, paused};
use color_eyre::Report;
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::sync::atomic::Ordering;

/// One retry session: how many transient retries the middleware allows, and
/// what the leaf answers on each attempt before it starts succeeding.
///
/// No `shrink`: a session holds at most five outcomes and prints in full, so a
/// counterexample is readable without one.
#[derive(Clone, Debug)]
struct RetrySession {
    max_retries: u32,
    script: Vec<ErrorCategory>,
}

/// What the retry and settle rules say one session comes to.
#[derive(Debug, Eq, PartialEq)]
struct Expected {
    invocations: usize,
    responses: usize,
}

impl Arbitrary for RetrySession {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_retries = u32::from(u8::arbitrary(g) % 4);
        let length = usize::from(u8::arbitrary(g) % 6);
        let script = (0..length).map(|_| category(g)).collect();
        Self {
            max_retries,
            script,
        }
    }
}

/// A response happens exactly when the session commits, whatever the outcome
/// sequence and the retry ceiling.
#[test]
fn at_most_one_response_per_final_invocation() {
    fn property(session: RetrySession) -> Result<bool> {
        let want = expected(&session);
        let (drained, invocations) = run(session)?;
        let seen = Expected {
            invocations,
            responses: drained.len(),
        };
        Ok(seen == want)
    }

    QuickCheck::new().quickcheck(property as fn(RetrySession) -> Result<bool>);
}

/// A retried cascade answers once, and the answer states the outcome that
/// settled it.
///
/// Both settling directions run over the same retry ceiling: a first attempt
/// that fails and then succeeds, and a transient error that exhausts its
/// retries. Each direction must reach the wire, so an arm that drops its
/// response metadata loses its answer here.
///
/// The exhaustion direction is the row a category check would silently break:
/// the last attempt's error is still transient, so a responder that answered
/// only permanent rejections would leave the requester waiting for its own
/// timeout.
#[test]
fn a_retried_cascade_answers_once_with_the_settled_outcome() -> Result<()> {
    let directions = [
        (vec![ErrorCategory::Transient], None),
        (
            vec![ErrorCategory::Transient, ErrorCategory::Transient],
            Some(ErrorCategory::Transient),
        ),
    ];
    for (script, settled) in directions {
        let session = RetrySession {
            max_retries: 1,
            script,
        };
        let (mut drained, invocations) = run(session)?;

        assert_eq!(invocations, 2, "the leaf runs one attempt and one retry");
        assert_eq!(
            drained.len(),
            1,
            "only the attempt that settled answers: {settled:?}",
        );
        let mut delivery = drained.remove(0);
        let frame = decode_frame(&mut delivery.bytes)?;
        let actual = match frame.result {
            FrameResult::Success(_) => None,
            FrameResult::HandlerError(HandlerError { category, .. }) => Some(category),
        };
        assert_eq!(actual, settled, "the answer must state the settled outcome");
    }
    Ok(())
}

/// Ordinary traffic reaches the transport never, and the handler's own hook
/// always.
#[test]
fn a_plain_message_forwards_its_result() -> Result<()> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::success();
        let handler = fixture.stack(leaf.clone(), 0)?;
        let tracker = offset_tracker();
        let message = plain("plain")?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let hooks = leaf.hook_events();
        let drained = fixture.drain().await?;
        assert!(drained.is_empty(), "nothing was asked for");
        assert!(
            hooks.contains(&ScriptedHook::AfterCommit(Ok(()))),
            "the handler's own hook still receives the result: {hooks:?}",
        );
        Ok(())
    })
}

/// A timer dispatch never responds, and settles exactly as it does without the
/// layer.
///
/// A trigger has no headers, so the layer cannot wrap a timer result. A
/// deferred reload arrives as a message inside timer handling and does respond.
#[test]
fn a_timer_dispatch_never_responds() -> Result<()> {
    for failure in [None, Some(ErrorCategory::Permanent)] {
        paused()?.block_on(async {
            let fixture = Fixture::<ResultProbeCodec>::new()?;
            let leaf = failure.map_or_else(ScriptedHandler::success, |category| {
                ScriptedHandler::always_failing(category)
            });
            let handler = fixture.stack(leaf, 0)?;
            let (timer, committed, aborted) = RecordingTimer::new(create_test_trigger());

            EventHandler::on_timer(&handler, MockEventContext::new(), timer, DemandType::Normal)
                .await;
            drop(handler);

            let drained = fixture.drain().await?;
            assert!(
                drained.is_empty(),
                "a timer dispatch must not respond, whatever the leaf answered",
            );
            assert_eq!(
                (
                    committed.load(Ordering::SeqCst),
                    aborted.load(Ordering::SeqCst),
                ),
                (1, 0),
                "the trigger settles exactly as it does without the layer",
            );
            Ok::<(), Report>(())
        })?;
    }
    Ok(())
}

/// What one session comes to, derived from the retry and settle rules rather
/// than from the code under test.
///
/// A session commits on the first success, on the first permanent error, and on
/// a transient error that exceeds the retry ceiling. It aborts on a terminal
/// error. A commit reaches `after_commit`, which responds; an abort reaches
/// `after_abort`, which is silent.
fn expected(session: &RetrySession) -> Expected {
    let mut attempt = 0_u32;
    loop {
        attempt += 1;
        let invocations = attempt as usize;
        let answered = |responses| Expected {
            invocations,
            responses,
        };
        match session.script.get(invocations - 1) {
            None | Some(ErrorCategory::Permanent) => return answered(1),
            Some(ErrorCategory::Terminal) => return answered(0),
            Some(ErrorCategory::Transient) if attempt > session.max_retries => return answered(1),
            Some(ErrorCategory::Transient) => {}
        }
    }
}

fn category(g: &mut Gen) -> ErrorCategory {
    match u8::arbitrary(g) % 3 {
        0 => ErrorCategory::Transient,
        1 => ErrorCategory::Permanent,
        _ => ErrorCategory::Terminal,
    }
}

/// Runs one session against the real stack and reports what the transport saw,
/// with the number of leaf invocations it took.
fn run(session: RetrySession) -> Result<(Vec<Delivery>, usize)> {
    paused()?.block_on(async {
        let fixture = Fixture::<ResultProbeCodec>::new()?;
        let leaf = ScriptedHandler::failing_then_success(session.script);
        let handler = fixture.stack(leaf.clone(), session.max_retries)?;
        let tracker = offset_tracker();
        let message = requesting(1, 7, "session")?.into_uncommitted(tracker.take(0).await?);

        EventHandler::on_message(
            &handler,
            MockEventContext::new(),
            message,
            DemandType::Normal,
        )
        .await;
        drop(handler);

        let invocations = leaf.call_count();
        Ok((fixture.drain().await?, invocations))
    })
}
