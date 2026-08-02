//! What one destination's pacing guarantees: turns one period apart, and never
//! a turn in the past.

use crate::router::fleet::rate::RateLimit;
use quickcheck::{Arbitrary, Gen, TestResult, empty_shrinker};
use quickcheck_macros::quickcheck;
use std::iter::once;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::time::{Instant, sleep, sleep_until};

/// Sends per second the generated schedules are paced at.
const RATE: u32 = 4;

/// The period `RATE` implies, spelled out rather than derived, so a wrong
/// division in the limiter cannot agree with the expectation.
const PERIOD: Duration = Duration::from_millis(250);

/// Longest idle stretch a step may leave, in periods.
const MAX_IDLE: u8 = 4;

/// Most turns one step claims in a row.
const MAX_CLAIMS: u8 = 3;

/// One step of a generated schedule: wait a while, then claim a few turns.
#[derive(Clone, Copy, Debug)]
struct Step {
    /// Periods to let pass before this step's claims. Zero keeps the
    /// destination saturated; more leaves it idle for longer than its period,
    /// which is where a burst would appear.
    idle: u8,
    /// Turns claimed one after another.
    claims: u8,
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            idle: u8::arbitrary(g) % (MAX_IDLE + 1),
            claims: 1 + u8::arbitrary(g) % MAX_CLAIMS,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self { idle, claims } = *self;
        if idle == 0 && claims == 1 {
            return empty_shrinker();
        }
        Box::new(
            once(Self { idle: 0, claims: 1 })
                .chain((idle > 0).then_some(Self { idle: 0, claims }))
                .chain((claims > 1).then_some(Self { idle, claims: 1 })),
        )
    }
}

/// A destination's pacing keeps its turns one period apart, and never hands out
/// a turn that has already passed.
///
/// The second half is the anti-burst rule. A limiter that resumed from the
/// schedule it stopped at would answer with instants in the past after an idle
/// stretch, and the caller would release all of them at once. Paused time makes
/// both claims exact, and the caller waits for each turn it is given, which is
/// what makes the next claim's present the schedule the limiter has to keep.
#[quickcheck]
fn prop_pacing_keeps_its_period_and_never_bursts(schedule: Vec<Step>) -> TestResult {
    let runtime = match Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("{error:?}")),
    };
    runtime.block_on(async {
        let limit = RateLimit::new(RATE);
        let mut previous: Option<Instant> = None;

        for step in schedule {
            sleep(PERIOD * u32::from(step.idle)).await;
            for _ in 0..step.claims {
                let now = Instant::now();
                let at = limit.claim();
                assert!(
                    at >= now,
                    "a claimed turn is {:?} in the past, so an idle destination would burst",
                    now - at
                );
                if let Some(previous) = previous {
                    assert!(
                        at.duration_since(previous) >= PERIOD,
                        "two turns are {:?} apart, less than the {PERIOD:?} period",
                        at - previous
                    );
                }
                previous = Some(at);
                sleep_until(at).await;
            }
        }
        TestResult::passed()
    })
}
