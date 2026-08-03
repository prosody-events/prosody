//! Where each arrival is filed, and what the waiter reads back.

use super::{
    MAX_RESPONSE_BYTES, MAX_TIMEOUT, POOL, TestCodec, TestError, body, formatted_frame, frame,
    names, register, registry, success,
};
use crate::error::ErrorCategory;
use crate::requester::collect::decode;
use crate::requester::registry::Status;
use crate::requester::{Outcome, ResponseFailure};
use crate::response::{ResponseDisposition, ResponseStatus};
use crate::router::loopback::paused;
use bytes::BytesMut;
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::iter::{empty, once};
use std::time::Duration;

/// Timeout every case here asks for. No case reaches it: each one ends the
/// request itself.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Most subsystems one case may name.
const MAX_AWAITED: usize = 4;

/// One request's awaited names, plus the order and the content of its
/// arrivals.
///
/// The arrival order is an independent permutation of the answering positions.
/// Thus a response filed by arrival order lands in the wrong place, and the
/// value assertion sees it.
#[derive(Clone, Debug)]
struct PositionTrace {
    /// Distinct pool indices, in the order the caller names them.
    awaited: Vec<usize>,
    /// Positions that answer, in arrival order.
    order: Vec<usize>,
    /// What each position answers with.
    answers: Vec<Answer>,
}

/// One response a position answers with.
#[derive(Clone, Copy, Debug)]
struct Answer {
    /// The value the arm carries.
    value: u32,
    /// The category of a failing arm. `None` names the success arm.
    category: Option<ErrorCategory>,
}

impl Arbitrary for PositionTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let count = usize::arbitrary(g) % MAX_AWAITED + 1;
        let mut pool: Vec<usize> = (0..POOL.len()).collect();
        let mut awaited = Vec::with_capacity(count);
        for _ in 0..count {
            awaited.push(pool.swap_remove(usize::arbitrary(g) % pool.len()));
        }

        let answers = (0..count).map(|_| Answer::arbitrary(g)).collect();

        let mut unanswered: Vec<usize> = (0..count).collect();
        let answering = usize::arbitrary(g) % (count + 1);
        let mut order = Vec::with_capacity(answering);
        for _ in 0..answering {
            order.push(unanswered.swap_remove(usize::arbitrary(g) % unanswered.len()));
        }

        Self {
            awaited,
            order,
            answers,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let mut shorter = self.clone();
        if shorter.order.pop().is_none() {
            return Box::new(empty());
        }
        Box::new(once(shorter))
    }
}

impl Arbitrary for Answer {
    fn arbitrary(g: &mut Gen) -> Self {
        let category = match u8::arbitrary(g) % 4 {
            0 => None,
            1 => Some(ErrorCategory::Transient),
            2 => Some(ErrorCategory::Permanent),
            _ => Some(ErrorCategory::Terminal),
        };
        Self {
            value: u32::arbitrary(g),
            category,
        }
    }
}

impl Answer {
    /// The wire status a frame carrying this answer states.
    const fn status(self) -> ResponseStatus {
        match self.category {
            None => ResponseStatus::Success,
            Some(category) => ResponseStatus::Error(category),
        }
    }

    /// The encoded body of this answer.
    fn body(self) -> Result<BytesMut> {
        match self.category {
            None => body(Ok(self.value)),
            Some(category) => body(Err(TestError {
                value: self.value,
                category,
            })),
        }
    }

    /// What the waiter must decode this answer into.
    const fn outcome(self) -> Outcome<u32, TestError> {
        match self.category {
            None => Outcome::Ok(self.value),
            Some(category) => Outcome::Handler(TestError {
                value: self.value,
                category,
            }),
        }
    }
}

/// Every position holds the answer its own subsystem sent, whatever order the
/// answers arrived in, and an unanswered position reads as a timeout.
#[quickcheck]
fn positions_hold_the_answer_their_subsystem_sent(trace: PositionTrace) -> TestResult {
    match run_positional(trace) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// A second answer from one subsystem is refused, and it takes no other
/// subsystem's place.
///
/// This is the arrival order a capacity-based duplicate test loses: two
/// `billing` frames then one `ledger`.
#[test]
fn a_repeated_subsystem_never_takes_another_position() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(4, MAX_AWAITED)?;
        let awaited = names(&["billing", "ledger"])?;
        let registration = register(&registry, &awaited, TIMEOUT)?;
        let id = registration.id();

        let dispositions = [
            registry.accept(success(id, &awaited[0], 1)?),
            registry.accept(success(id, &awaited[0], 2)?),
            registry.accept(success(id, &awaited[1], 3)?),
        ];
        assert_eq!(
            dispositions,
            [
                ResponseDisposition::Accepted,
                ResponseDisposition::DuplicateSubsystem,
                ResponseDisposition::Accepted,
            ],
            "the second billing answer must be the only refused one"
        );

        let finished = registration.finish();
        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes,
            vec![Outcome::Ok(1), Outcome::Ok(3)],
            "billing keeps its first answer and ledger keeps its own"
        );
        Ok(())
    })
}

/// A frame in another format fills its position and ends the request, so the
/// caller learns at once instead of waiting out its deadline.
#[test]
fn a_frame_in_another_format_is_refused_and_reported() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(4, MAX_AWAITED)?;
        let awaited = names(&["billing"])?;
        let registration = register(&registry, &awaited, TIMEOUT)?;

        let disposition = registry.accept(formatted_frame(
            registration.id(),
            &awaited[0],
            ResponseStatus::Success,
            body(Ok(9))?,
            "some-other-format",
        ));
        assert_eq!(disposition, ResponseDisposition::FormatMismatch);

        let finished = registration.finish();
        assert_eq!(
            finished.status,
            Status::Complete,
            "the request must end on arrival, not at its deadline"
        );
        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes,
            vec![Outcome::Failed(ResponseFailure::FormatMismatch)]
        );
        Ok(())
    })
}

/// A payload over the configured response ceiling is refused rather than held,
/// and one exactly at the ceiling is held. The refusal ends the request, so the
/// caller reads it instead of waiting out its deadline.
#[test]
fn a_payload_over_the_response_ceiling_is_refused() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(4, MAX_AWAITED)?;
        let awaited = names(&["billing", "ledger"])?;
        let registration = register(&registry, &awaited, TIMEOUT)?;
        let id = registration.id();

        let dispositions = [
            registry.accept(frame(
                id,
                &awaited[0],
                ResponseStatus::Success,
                BytesMut::zeroed(MAX_RESPONSE_BYTES + 1),
            )),
            registry.accept(frame(
                id,
                &awaited[1],
                ResponseStatus::Success,
                BytesMut::zeroed(MAX_RESPONSE_BYTES),
            )),
        ];
        assert_eq!(
            dispositions,
            [
                ResponseDisposition::ResponseTooLarge,
                ResponseDisposition::Accepted,
            ],
            "the ceiling must refuse the payload above it and hold the one at it"
        );

        let finished = registration.finish();
        assert_eq!(
            finished.status,
            Status::Complete,
            "the request must end on the refusal, not at its deadline"
        );
        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes,
            vec![
                Outcome::Failed(ResponseFailure::TooLarge),
                // A thousand zero bytes are a payload the codec cannot read.
                Outcome::Failed(ResponseFailure::Malformed),
            ],
            "the refused payload reached the position it was too large for"
        );
        Ok(())
    })
}

/// A frame whose wire status disagrees with its decoded arm is malformed, and
/// one whose status agrees reaches the caller as a handler failure.
#[test]
fn a_status_that_disagrees_with_its_payload_is_malformed() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(4, MAX_AWAITED)?;
        let awaited = names(&["billing", "ledger", "audit"])?;
        let registration = register(&registry, &awaited, TIMEOUT)?;
        let id = registration.id();

        let failure = TestError {
            value: 4,
            category: ErrorCategory::Transient,
        };
        let dispositions = [
            // A success status over a failing arm.
            registry.accept(frame(
                id,
                &awaited[0],
                ResponseStatus::Success,
                body(Err(failure))?,
            )),
            // A failure status over a successful arm.
            registry.accept(frame(
                id,
                &awaited[1],
                ResponseStatus::Error(ErrorCategory::Permanent),
                body(Ok(5))?,
            )),
            // A failure status that names the category the arm classifies as.
            registry.accept(frame(
                id,
                &awaited[2],
                ResponseStatus::Error(ErrorCategory::Transient),
                body(Err(failure))?,
            )),
        ];
        assert_eq!(
            dispositions,
            [ResponseDisposition::Accepted; 3],
            "a frame the waiter's own codec reads must be filed, whatever its status says"
        );

        let finished = registration.finish();
        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes,
            vec![
                Outcome::Failed(ResponseFailure::Malformed),
                Outcome::Failed(ResponseFailure::Malformed),
                Outcome::Handler(failure),
            ]
        );
        Ok(())
    })
}

/// Drives one generated positional trace.
fn run_positional(trace: PositionTrace) -> Result<()> {
    let PositionTrace {
        awaited: chosen,
        order,
        answers,
    } = trace;
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(4, MAX_AWAITED)?;
        let pool: Vec<&str> = chosen.iter().map(|index| POOL[*index]).collect();
        let awaited = names(&pool)?;
        let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
        let id = registration.id();

        for position in &order {
            let answer = answers[*position];
            let disposition = registry.accept(frame(
                id,
                &awaited[*position],
                answer.status(),
                answer.body()?,
            ));
            assert_eq!(
                disposition,
                ResponseDisposition::Accepted,
                "the answer for position {position} was refused"
            );
        }

        let finished = registration.finish();
        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes.len(),
            awaited.len(),
            "one outcome must come back per named subsystem"
        );
        for (position, outcome) in outcomes.iter().enumerate() {
            let expected = if order.contains(&position) {
                answers[position].outcome()
            } else {
                Outcome::Failed(ResponseFailure::Timeout)
            };
            assert_eq!(
                *outcome, expected,
                "position {position} holds an answer another subsystem sent"
            );
        }
        Ok(())
    })
}
