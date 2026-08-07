//! What a responder does when the first endpoint it tries does not answer.

use super::{Harness, attempts_on, paused};
use crate::router::SendFailure;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Script, advertised_port, port};
use color_eyre::Result;
use tonic::Code;

/// The node the responses in this suite are addressed to.
const TARGET: u8 = 1;

/// Every candidate walk two responses to one destination can make.
///
/// Each row states what the two endpoints answer, and the four numbers the walk
/// must come to. Together they cover every arm of the walk:
///
/// - **A wrong endpoint moves on, and the endpoint that answered is
///   remembered.** `remembers` and `refusal_is_remembered`, whose second
///   response starts at the remembered endpoint and so touches the other one
///   fewer times.
/// - **A status stops the walk.** `refusal_is_remembered` and
///   `refused_entry_point`: the entry point is never tried in the first, and
///   the direct endpoint is tried once in the second.
/// - **A walk with no answer at all remembers nothing.** `forgets`, which
///   clears a preference the response before it set, and `nothing_answers`,
///   which never sets one.
const CASES: &[Case] = &[
    Case {
        name: "remembers",
        first: (Answer::Silent, Answer::Takes),
        attempts: 2,
        second: (Answer::Silent, Answer::Takes),
        direct: 1,
        advertised: 2,
        sent: 2,
    },
    Case {
        name: "refusal_is_remembered",
        first: (Answer::Refuses(Code::ResourceExhausted), Answer::Takes),
        attempts: 1,
        second: (Answer::Refuses(Code::ResourceExhausted), Answer::Takes),
        direct: 2,
        advertised: 0,
        sent: 0,
    },
    Case {
        name: "forgets",
        first: (Answer::Silent, Answer::Takes),
        attempts: 2,
        second: (Answer::Silent, Answer::Silent),
        direct: 2,
        advertised: 2,
        sent: 1,
    },
    Case {
        name: "nothing_answers",
        first: (Answer::Silent, Answer::Silent),
        attempts: 2,
        second: (Answer::Silent, Answer::Silent),
        direct: 2,
        advertised: 2,
        sent: 0,
    },
    Case {
        name: "refused_entry_point",
        first: (Answer::Silent, Answer::Refuses(Code::Internal)),
        attempts: 2,
        second: (Answer::Silent, Answer::Refuses(Code::Internal)),
        direct: 1,
        advertised: 2,
        sent: 0,
    },
];

/// What one endpoint answers.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// Accepts the response.
    Takes,
    /// Says nothing at all, which is a wrong endpoint.
    Silent,
    /// Answers this status, which is a process on the path speaking.
    Refuses(Code),
}

/// One candidate walk: two responses to the same destination, each answered by
/// the scripts the row states.
struct Case {
    name: &'static str,
    /// What the direct and the advertised endpoint answer the first response.
    first: (Answer, Answer),
    /// Attempts the first response makes. Waiting for exactly this many is what
    /// lets the test set the second response scripts after the first completes.
    attempts: usize,
    /// What the two endpoints answer the second response.
    second: (Answer, Answer),
    /// Attempts each endpoint took over both responses. The two endpoints are
    /// distinct ports, so these say which endpoint each attempt reached.
    direct: usize,
    advertised: usize,
    /// Responses delivered.
    sent: u64,
}

impl Answer {
    /// The transport script this answer is driven by.
    fn script(self) -> Script {
        match self {
            Self::Takes => Script::Fail {
                failure: SendFailure::Unreachable,
                times: 0,
            },
            Self::Silent => Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
            Self::Refuses(status) => Script::Fail {
                failure: SendFailure::Status(status),
                times: usize::MAX,
            },
        }
    }
}

/// A route is walked until something answers, and the destination remembers the
/// endpoint that spoke — a refusal included, because a status is a process on
/// the path speaking rather than an endpoint failing.
///
/// The next response starts at the remembered endpoint, and a walk that reaches
/// no answer at all leaves nothing remembered. That memory lives in the
/// destination's own cell, which
/// `an_evicted_destination_forgets_which_endpoint_answered` pins.
///
/// Which statuses count as a wrong endpoint is not this suite's subject:
/// `every_failure_answers_the_two_questions_the_send_path_asks` states that per
/// status, so one refusing status is enough here.
#[test]
fn a_route_is_walked_until_something_answers_and_that_endpoint_is_remembered() -> Result<()> {
    for case in CASES {
        let runtime = paused()?;
        runtime.block_on(async {
            let mut harness = Harness::dual_homed(settings())?;
            apply(&harness, case.first);
            harness.send(TARGET).await?;
            // Taken from the stream rather than left for the drain, because
            // reading them is what says the first response is over.
            let mut recorded = Vec::with_capacity(case.attempts);
            for _ in 0..case.attempts {
                recorded.push(harness.next_delivery().await?);
            }
            apply(&harness, case.second);
            harness.send(TARGET).await?;

            let drained = harness.drain().await?;
            recorded.extend(drained.deliveries);
            let name = case.name;
            assert_eq!(
                attempts_on(&recorded, port(TARGET)),
                case.direct,
                "{name}: wrong number of attempts on the direct endpoint"
            );
            assert_eq!(
                attempts_on(&recorded, advertised_port(TARGET)),
                case.advertised,
                "{name}: wrong number of attempts on the entry point"
            );
            assert_eq!(drained.sent, case.sent, "{name}: wrong number delivered");
            Ok::<(), color_eyre::Report>(())
        })?;
    }
    Ok(())
}

/// Scripts what each of the target's two endpoints answers next.
fn apply(harness: &Harness, (direct, advertised): (Answer, Answer)) {
    harness.script(TARGET, direct.script());
    harness.script_advertised(TARGET, advertised.script());
}

/// The fleet every case here runs against.
fn settings() -> FleetConfiguration {
    FleetConfiguration::default()
}
