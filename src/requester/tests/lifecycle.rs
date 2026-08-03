//! What ends a request, what it costs, and what the map holds afterwards.

use super::{MAX_TIMEOUT, POOL, SWEEP_GRACE, TestCodec, TestError, names, registry, success};
use crate::Codec;
use crate::requester::collect::decode;
use crate::requester::registry::{PendingRegistry, Registration, Status};
use crate::requester::{Outcome, ResponseFailure};
use crate::response::ResponseDisposition;
use crate::router::loopback::paused;
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::iter::{empty, once};
use std::time::Duration;
use tokio::time::Instant;

/// Requests one registry in these suites admits.
const IN_FLIGHT: usize = 4;

/// Most subsystems one request here names.
const MAX_AWAITED: usize = 3;

/// Timeout every request here asks for.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The value each position answers with, so a misfiled answer is visible.
const VALUES: [u32; MAX_AWAITED] = [101, 202, 303];

/// Accepts before and after one terminal transition.
#[derive(Clone, Debug)]
struct TerminalTrace {
    /// Subsystems the request names.
    awaited: usize,
    /// Positions that answer, in arrival order.
    order: Vec<usize>,
    /// How many of those arrive before the transition.
    cut: usize,
}

/// One step against a single registered request.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// One response arrives.
    Respond,
    /// The deadline sweep runs a full grace period past the deadline.
    Sweep,
    /// The waiter's own timeout ends the request.
    Timeout,
    /// The whole registry shuts down.
    Shutdown,
}

/// One step against a registry holding several requests.
#[derive(Clone, Copy, Debug)]
enum DrainStep {
    /// One more request registers.
    Register,
    /// One live request is answered in full.
    Answer(u8),
    /// One live request's caller goes away.
    Cancel(u8),
    /// The sweep runs while every deadline is still ahead.
    Sweep,
}

impl Arbitrary for TerminalTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let awaited = usize::arbitrary(g) % MAX_AWAITED + 1;
        let mut unanswered: Vec<usize> = (0..awaited).collect();
        let answering = usize::arbitrary(g) % (awaited + 1);
        let mut order = Vec::with_capacity(answering);
        for _ in 0..answering {
            order.push(unanswered.swap_remove(usize::arbitrary(g) % unanswered.len()));
        }
        let cut = usize::arbitrary(g) % (order.len() + 1);
        Self {
            awaited,
            order,
            cut,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        if self.order.is_empty() {
            return Box::new(empty());
        }
        let mut shorter = self.clone();
        shorter.order.pop();
        shorter.cut = shorter.cut.min(shorter.order.len());
        Box::new(once(shorter))
    }
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        // Responses are the common step, so a trace usually has a request worth
        // ending rather than an empty one.
        match u8::arbitrary(g) % 6 {
            0..=2 => Self::Respond,
            3 => Self::Sweep,
            4 => Self::Timeout,
            _ => Self::Shutdown,
        }
    }
}

impl Arbitrary for DrainStep {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0..=1 => Self::Register,
            2..=3 => Self::Answer(u8::arbitrary(g)),
            4 => Self::Cancel(u8::arbitrary(g)),
            _ => Self::Sweep,
        }
    }
}

/// A response accepted before the request ends reaches the waiter, and no
/// response is accepted after it ends.
#[quickcheck]
fn a_terminal_transition_keeps_what_it_collected(trace: TerminalTrace) -> TestResult {
    match run_terminal(trace) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// One request makes one terminal transition, and its admission permit comes
/// back exactly once however the request ends.
#[quickcheck]
fn one_request_transitions_once_and_returns_one_permit(steps: Vec<Step>) -> TestResult {
    match run_steps(steps) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// The map never exceeds admission, always holds every live request, and
/// reaches empty once every caller has gone.
#[quickcheck]
fn the_registry_holds_live_requests_and_drains(steps: Vec<DrainStep>) -> TestResult {
    match run_drain(steps) {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}")),
    }
}

/// Drives one generated terminal-transition trace.
fn run_terminal(trace: TerminalTrace) -> Result<()> {
    let TerminalTrace {
        awaited: count,
        order,
        cut,
    } = trace;
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
        let awaited = names(&POOL[..count])?;
        let registration = registry.register(&awaited, TestCodec::FORMAT_ID, MAX_TIMEOUT)?;
        let id = registration.id();

        let before = &order[..cut];
        for position in before {
            let disposition = registry.accept(success(id, &awaited[*position], VALUES[*position])?);
            assert_eq!(
                disposition,
                ResponseDisposition::Accepted,
                "position {position} was refused before the transition"
            );
        }

        let finished = registration.finish(Status::TimedOut);

        for position in &order[cut..] {
            let disposition = registry.accept(success(id, &awaited[*position], VALUES[*position])?);
            assert_eq!(
                disposition,
                ResponseDisposition::ClosedRequest,
                "position {position} was accepted after the request ended"
            );
        }

        let outcomes = decode::<TestCodec, u32, TestError, _>(finished.awaited);
        assert_eq!(
            outcomes.len(),
            awaited.len(),
            "the transition dropped the positions it was collecting"
        );
        for (position, outcome) in outcomes.iter().enumerate() {
            let expected = if before.contains(&position) {
                Outcome::Ok(VALUES[position])
            } else {
                Outcome::Failed(ResponseFailure::Timeout)
            };
            assert_eq!(
                *outcome, expected,
                "position {position} lost the answer it accepted"
            );
        }
        Ok(())
    })
}

/// Drives one generated step sequence against a single request.
fn run_steps(steps: Vec<Step>) -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
        let awaited = names(&POOL[..1])?;
        let registration = registry.register(&awaited, TestCodec::FORMAT_ID, TIMEOUT)?;
        let id = registration.id();
        let expired = registration.deadline() + SWEEP_GRACE;
        let mut first = None;

        for step in steps {
            match step {
                Step::Respond => {
                    if registry.accept(success(id, &awaited[0], 1)?)
                        == ResponseDisposition::Accepted
                    {
                        first = first.or(Some(Status::Complete));
                    }
                }
                Step::Sweep => {
                    registry.sweep(expired);
                    first = first.or(Some(Status::TimedOut));
                }
                Step::Timeout => {
                    registration.finish(Status::TimedOut);
                    first = first.or(Some(Status::TimedOut));
                }
                Step::Shutdown => {
                    registry.shutdown().await;
                    first = first.or(Some(Status::ShuttingDown));
                }
            }
            assert!(
                registry.available_permits() <= IN_FLIGHT,
                "admission released more permits than it issued after {step:?}"
            );
        }

        assert_eq!(
            registration.finish(Status::TimedOut).status,
            first.unwrap_or(Status::TimedOut),
            "a later trigger overwrote the transition that ended the request"
        );

        drop(registration);
        assert_eq!(registry.len(), 0, "the request kept its map record");
        assert_eq!(
            registry.available_permits(),
            IN_FLIGHT,
            "the request did not return exactly one permit"
        );
        Ok(())
    })
}

/// Drives one generated step sequence against a registry holding many
/// requests.
fn run_drain(steps: Vec<DrainStep>) -> Result<()> {
    let runtime = paused()?;
    runtime.block_on(async {
        let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
        let awaited = names(&POOL[..1])?;
        let mut live: Vec<Registration> = Vec::with_capacity(IN_FLIGHT);

        for step in steps {
            match step {
                DrainStep::Register => {
                    if let Ok(registration) =
                        registry.register(&awaited, TestCodec::FORMAT_ID, TIMEOUT)
                    {
                        live.push(registration);
                    }
                }
                DrainStep::Answer(index) => {
                    if let Some(registration) = pick(&live, index) {
                        registry.accept(success(registration.id(), &awaited[0], 2)?);
                    }
                }
                DrainStep::Cancel(index) => {
                    if !live.is_empty() {
                        live.swap_remove(usize::from(index) % live.len());
                    }
                }
                DrainStep::Sweep => registry.sweep(Instant::now()),
            }
            check_live(&registry, &live, step);
        }

        // One request the trace cannot have ended, so the last check is never
        // vacuous: a sweep that ignores its grace period removes it here.
        if live.len() == IN_FLIGHT {
            live.pop();
        }
        live.push(registry.register(&awaited, TestCodec::FORMAT_ID, TIMEOUT)?);
        registry.sweep(Instant::now());
        check_live(&registry, &live, DrainStep::Sweep);

        live.clear();
        assert_eq!(registry.len(), 0, "a finished request kept its map record");
        assert_eq!(
            registry.available_permits(),
            IN_FLIGHT,
            "admission did not get every permit back"
        );
        Ok(())
    })
}

/// Holds the map to its bound and to every live request it must still hold.
fn check_live(registry: &PendingRegistry, live: &[Registration], step: DrainStep) {
    assert!(
        registry.len() <= IN_FLIGHT,
        "the map holds {} requests, over the {IN_FLIGHT} admission allows, after {step:?}",
        registry.len()
    );
    for registration in live {
        assert!(
            registry.contains(registration.id()),
            "a request whose caller is still waiting left the map after {step:?}"
        );
    }
}

/// The registration at `index`, when the registry holds any.
fn pick(live: &[Registration], index: u8) -> Option<&Registration> {
    if live.is_empty() {
        return None;
    }
    live.get(usize::from(index) % live.len())
}
