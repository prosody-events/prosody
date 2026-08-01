//! Retry pacing after a failed or absent refresh, including the exact
//! suppression of routing reads inside the window. See the parent module for
//! the rule these prove.

use super::{Expect, element, outcome_matches};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::StateType;
use crate::state::descriptor::value_state;
use crate::state::publication::PublicationStore;
use crate::state_reader::reader::acquisition::{ABSENT_BACKOFF, REFRESH_BACKOFF};
use crate::state_reader::tests::support::{GROUP_A, ScriptedEnv, mock_clock_cache, topic};
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::iter::{empty, once};
use std::time::Duration;

/// The refresh interval the pacing property's reader runs with.
///
/// Every [`Advance`] except `Long` lands inside it, so a completed refresh
/// stays fresh across the shorter steps. That is what makes the absent window
/// observable: an absence outlives its own deadline while still inside this
/// interval, and only excluding it from freshness lets the next read re-read.
const PACING_INTERVAL: Duration = REFRESH_BACKOFF;

/// How far one pacing step moves the mock clock before its read.
///
/// The two windows differ in length, so the steps are chosen to land on both
/// sides of each: `Short` stays inside either, `Absent` lands exactly on the
/// absent deadline while still inside a failure window, and `Long` clears both.
#[derive(Clone, Copy, Debug)]
enum Advance {
    /// Not at all, so the read lands inside any open window.
    None,
    /// Half the absent window, so two steps are needed to cross it.
    Short,
    /// Exactly the absent window, which a failure window outlives.
    Absent,
    /// The whole failure window, landing on the later of the two deadlines.
    Long,
}

impl Advance {
    /// The mock-clock step this advance takes.
    fn duration(self) -> Duration {
        match self {
            Self::None => Duration::ZERO,
            Self::Short => ABSENT_BACKOFF / 2,
            Self::Absent => ABSENT_BACKOFF,
            Self::Long => REFRESH_BACKOFF,
        }
    }
}

impl Arbitrary for Advance {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::None,
            1 => Self::Short,
            2 => Self::Absent,
            _ => Self::Long,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::None => Box::new(empty()),
            _ => Box::new(once(Self::None)),
        }
    }
}

/// One pacing step: how far the clock moves, whether the collection is
/// advertised, and whether the routing read fails.
#[derive(Clone, Copy, Debug)]
struct PacingStep {
    advance: Advance,
    published: bool,
    fail: bool,
}

impl Arbitrary for PacingStep {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            advance: Advance::arbitrary(g),
            published: bool::arbitrary(g),
            fail: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self {
            advance,
            published,
            fail,
        } = *self;
        // Shrink toward no outage, then toward a published collection, then
        // toward a clock that does not move.
        let healed = fail.then_some(Self {
            advance,
            published,
            fail: false,
        });
        let advertised = (!published).then_some(Self {
            advance,
            published: true,
            fail,
        });
        let slower = advance.shrink().map(move |advance| Self {
            advance,
            published,
            fail,
        });
        Box::new(healed.into_iter().chain(advertised).chain(slower))
    }
}

/// A shrinkable sequence of pacing steps.
#[derive(Clone, Debug)]
struct PacingScript {
    steps: Vec<PacingStep>,
}

impl Arbitrary for PacingScript {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: Vec::<PacingStep>::arbitrary(g)
                .into_iter()
                .take(16)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.steps.shrink().map(|steps| Self { steps }))
    }
}

/// What the reader's acquisition holds, mirroring `Acquired`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Held {
    /// Nothing acquired: a fresh reader, or one whose absence a failed refresh
    /// discarded.
    Nothing,
    /// A validated snapshot.
    Sources,
    /// A completed refresh that found no publication row.
    Absent,
}

/// Neither refresh outcome that yields no snapshot may re-read the routing
/// table on every read. A failed refresh paces the next attempt by
/// `REFRESH_BACKOFF`; one that finds no publisher paces by the shorter
/// `ABSENT_BACKOFF`. Inside either window a read serves the held outcome
/// without touching the store, and the first read at or past the deadline
/// re-reads. Proven over random outage, publish/withdraw, and clock-advance
/// scripts against a model of both windows.
///
/// The absence is deliberately **not** durable across an outage: a failed
/// refresh discards it, because "no publisher yet" is the one claim that read
/// could not confirm. Sources survive so reads keep working.
///
/// Pacing and the refresh interval are the only two things that suppress a
/// routing read, and both are modelled, so the expected read count is exact.
/// The interval is [`PACING_INTERVAL`], long enough that an absence's shorter
/// window expires well inside it.
///
/// FALSIFICATION: drop the `within_pacing` term from `StateReader::snapshot`'s
/// gate → a read inside either window re-reads the routing table → the
/// read-count assert reds. Stop pacing an absence (drop the `Acquired::Absent`
/// arm in `acquire`) → a script that stays unpublished re-reads every step →
/// red. Include an absence in `is_fresh` → the read after the absent deadline
/// is suppressed → red. Let `failed` keep an absence → a fail step after an
/// unpublished one serves `UnknownPublication` where the model expects the
/// store error → red. Fall through to the store instead of
/// `RefreshUnavailable` when paced with nothing held → red.
#[test]
fn prop_refresh_pacing() {
    fn property(script: PacingScript) -> Result<bool> {
        block_on(run_refresh_pacing(script))
    }
    QuickCheck::new().quickcheck(property as fn(PacingScript) -> Result<bool>);
}

async fn run_refresh_pacing(script: PacingScript) -> Result<bool> {
    let descriptor = value_state::<JsonCodec>("pacing-v");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let reader = env.reader_with_interval(cache, PACING_INTERVAL)?;

    // Model state: elapsed mock time, when pacing next permits an attempt, when
    // the last refresh completed, and what it acquired.
    let mut elapsed = Duration::ZERO;
    let mut retry_after: Option<Duration> = None;
    let mut refreshed_at: Option<Duration> = None;
    let mut held = Held::Nothing;

    for step in script.steps {
        let advance = step.advance.duration();
        mock.increment(advance);
        elapsed += advance;
        if step.published {
            env.publish(GROUP_A, tp).await;
        } else {
            env.publications
                .remove_group(&env.sub, StateType::Application, &env.name, GROUP_A)
                .await
                .map_err(|e| eyre!("remove_group: {e}"))?;
        }
        if step.fail {
            env.publications.fail_reads_with(ErrorCategory::Transient);
        } else {
            env.publications.heal_reads();
        }

        let paced = retry_after.is_some_and(|deadline| elapsed < deadline);
        // An absence never counts as fresh: only its own shorter window paces
        // it, so a publisher that appears is admitted within `ABSENT_BACKOFF`.
        let fresh = held != Held::Absent
            && refreshed_at.is_some_and(|at| elapsed.saturating_sub(at) < PACING_INTERVAL);

        let (expect, expected_reads) = if paced || fresh {
            // Inside a window: serve what is held, touching no store.
            let served = match held {
                Held::Sources => Expect::Value(0),
                Held::Absent => Expect::UnknownPublication,
                Held::Nothing => Expect::RefreshUnavailable,
            };
            (served, 0)
        } else if step.fail {
            retry_after = Some(elapsed + REFRESH_BACKOFF);
            // Sources survive the outage; an unconfirmed absence does not.
            if held == Held::Absent {
                held = Held::Nothing;
                refreshed_at = None;
            }
            let served = match held {
                Held::Sources => Expect::Value(0),
                _ => Expect::ReadError,
            };
            (served, 1)
        } else if step.published {
            retry_after = None;
            refreshed_at = Some(elapsed);
            held = Held::Sources;
            (Expect::Value(0), 1)
        } else {
            retry_after = Some(elapsed + ABSENT_BACKOFF);
            refreshed_at = Some(elapsed);
            held = Held::Absent;
            (Expect::UnknownPublication, 1)
        };

        let reads_before = env.publications.reads();
        let observed = reader.get(key.clone()).await;
        let reads = env.publications.reads() - reads_before;
        if !outcome_matches(&expect, &observed) {
            bail!("step {step:?} at {elapsed:?}: expected {expect:?}, read {observed:?}");
        }
        if reads != expected_reads {
            bail!("step {step:?} at {elapsed:?}: {reads} routing reads, expected {expected_reads}");
        }
    }
    Ok(true)
}
