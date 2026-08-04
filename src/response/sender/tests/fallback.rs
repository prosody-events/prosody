//! What a responder does when the first endpoint it tries does not answer.

use super::{Harness, attempts_on, paused};
use crate::router::SendFailure;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::{Script, advertised_port, node, port};
use color_eyre::Result;
use color_eyre::eyre::bail;
use std::time::Duration;
use tonic::Code;

/// The node the responses in this suite are addressed to.
const TARGET: u8 = 1;

/// Two other nodes, enough to take every cell of the table below.
const CROWD: [u8; 2] = [2, 3];

/// Cells and slots the fleet here holds.
const CELLS: usize = 2;
const SLOTS: usize = 2;

/// Attempts one response may make against one endpoint.
///
/// One, deliberately. A budget shared between the two endpoints would make the
/// fallback unreachable at exactly this setting, so this is the value that
/// proves the budget is per endpoint.
const ATTEMPTS: u32 = 1;

/// Sends per second in the pacing case, and the interval that follows from it.
/// One per second, so no rounding can hide a turn that was never claimed.
const PACED: u32 = 1;
const PERIOD: Duration = Duration::from_secs(1);

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
        remembered: 1,
    },
    Case {
        name: "refusal_is_remembered",
        first: (Answer::Refuses(Code::ResourceExhausted), Answer::Takes),
        attempts: 1,
        second: (Answer::Refuses(Code::ResourceExhausted), Answer::Takes),
        direct: 2,
        advertised: 0,
        sent: 0,
        remembered: 1,
    },
    Case {
        name: "forgets",
        first: (Answer::Silent, Answer::Takes),
        attempts: 2,
        second: (Answer::Silent, Answer::Silent),
        direct: 2,
        advertised: 2,
        sent: 1,
        remembered: 0,
    },
    Case {
        name: "nothing_answers",
        first: (Answer::Silent, Answer::Silent),
        attempts: 2,
        second: (Answer::Silent, Answer::Silent),
        direct: 2,
        advertised: 2,
        sent: 0,
        remembered: 0,
    },
    Case {
        name: "refused_entry_point",
        first: (Answer::Silent, Answer::Refuses(Code::Internal)),
        attempts: 2,
        second: (Answer::Silent, Answer::Refuses(Code::Internal)),
        direct: 1,
        advertised: 2,
        sent: 0,
        remembered: 1,
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
    /// lets the second response's scripts be set after the first has finished:
    /// one worker serves one destination, so nothing else can be in flight.
    attempts: usize,
    /// What the two endpoints answer the second response.
    second: (Answer, Answer),
    /// Attempts each endpoint took over both responses. The two endpoints are
    /// distinct ports, so these say which endpoint each attempt reached.
    direct: usize,
    advertised: usize,
    /// Responses delivered, and destinations left remembering an endpoint.
    sent: u64,
    remembered: usize,
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
            let fleet = harness.fleet();
            apply(&harness, case.first);
            harness.send(TARGET)?;
            // Taken from the stream rather than left for the drain, because
            // reading them is what says the first response is over.
            let mut recorded = Vec::with_capacity(case.attempts);
            for _ in 0..case.attempts {
                recorded.push(harness.next_delivery().await?);
            }
            apply(&harness, case.second);
            harness.send(TARGET)?;

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
            assert_eq!(
                fleet.remembered(),
                case.remembered,
                "{name}: wrong number of destinations remembering an endpoint"
            );
            assert!(
                fleet.live(node(TARGET)).is_some(),
                "{name}: the destination must still hold its cell"
            );
            Ok::<(), color_eyre::Report>(())
        })?;
    }
    Ok(())
}

/// A remembered endpoint dies with the cell that holds it.
///
/// The first response falls back and the destination remembers the endpoint
/// that answered; the second reaches that endpoint alone, which is the memory
/// working. Two other nodes then take every cell, so this destination is
/// evicted. The next response therefore walks the route from the start and
/// reaches the dead direct endpoint again. A verdict held anywhere the table
/// cannot evict — a map keyed by node id, say — would survive that eviction,
/// and the last response would never touch the dead endpoint.
#[test]
fn an_evicted_destination_forgets_which_endpoint_answered() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let mut harness = Harness::dual_homed(settings())?;
        let fleet = harness.fleet();
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );

        harness.send(TARGET)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            port(TARGET),
            "the first response must try the direct endpoint first"
        );
        assert_eq!(
            harness.next_delivery().await?.port,
            advertised_port(TARGET),
            "the first response must fall back to the endpoint that answers"
        );
        harness.send(TARGET)?;
        assert_eq!(
            harness.next_delivery().await?.port,
            advertised_port(TARGET),
            "the second response must start at the remembered endpoint"
        );

        // A recorded delivery means that response's worker finished it and gave
        // the slot back, so the next admission finds an idle cell to take
        // without waiting on a clock.
        for index in CROWD {
            harness.send(index)?;
            assert_eq!(
                harness.next_delivery().await?.port,
                port(index),
                "each crowding response must reach the node it was queued for"
            );
        }
        assert!(
            fleet.live(node(TARGET)).is_none(),
            "the crowd must have evicted the destination that remembered an endpoint"
        );

        harness.send(TARGET)?;
        let drained = harness.drain().await?;
        assert_eq!(
            attempts_on(&drained.deliveries, port(TARGET)),
            1,
            "the last response must try the dead direct endpoint again, because the verdict went \
             with the cell"
        );
        assert_eq!(
            attempts_on(&drained.deliveries, advertised_port(TARGET)),
            1,
            "the last response must fall back again"
        );
        assert_eq!(drained.sent, 5, "every response must be delivered");
        Ok(())
    })
}

/// Every attempt one response makes claims the destination's pacing, the
/// fallback included.
///
/// The direct endpoint never answers and the entry point does, so one response
/// makes exactly two attempts under an allowance of one attempt per endpoint.
/// The destination is paced at one send per second, so the second attempt must
/// go a whole second after the first. An attempt that claimed no turn would go
/// out at the same instant as the one before it, and this destination would be
/// asked for twice what its rate limit allows.
#[test]
fn every_attempt_of_one_response_claims_the_destination_pacing() -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let harness = Harness::dual_homed(FleetConfiguration {
            sends_per_second: PACED,
            ..settings()
        })?;
        harness.script(
            TARGET,
            Script::Fail {
                failure: SendFailure::Unreachable,
                times: usize::MAX,
            },
        );
        harness.send(TARGET)?;

        let drained = harness.drain().await?;
        let [first, second] = drained.deliveries.as_slice() else {
            bail!(
                "one response over two endpoints must make two attempts, not {}",
                drained.deliveries.len()
            );
        };
        assert_eq!(
            (first.port, second.port),
            (port(TARGET), advertised_port(TARGET)),
            "the response must try the direct endpoint and then the entry point"
        );
        assert_eq!(
            second.at.duration_since(first.at),
            PERIOD,
            "the fallback attempt must wait its own turn on the destination's rate limit"
        );
        Ok(())
    })
}

/// Scripts what each of the target's two endpoints answers next.
fn apply(harness: &Harness, (direct, advertised): (Answer, Answer)) {
    harness.script(TARGET, direct.script());
    harness.script_advertised(TARGET, advertised.script());
}

/// The fleet every case here runs against.
fn settings() -> FleetConfiguration {
    FleetConfiguration {
        max_destinations: CELLS,
        slots_each: SLOTS,
        max_send_attempts: ATTEMPTS,
        ..FleetConfiguration::default()
    }
}
