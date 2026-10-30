//! Property test (`prop_scheduler_invariants`) for the scheduler actor.
//!
//! Drives random sequences of scheduler ops through the actor's commands and
//! checks these invariants after every op:
//!
//!   P1 (owned↔DB parity): memory holds every `(key, time, type)` in the
//!       loaded numeric range that is in the DB, except a timer whose row
//!       write failed, which memory still holds.
//!   P2 (pending = store-only): memory holds no timer in a slab outside the
//!       owned range.
//!   P3 (watermark I1): every slab clustering row sits strictly above
//!       `last_persisted_watermark`.
//!   P4 (cleanup safety): cleanup never deletes
//!       a slab that has active triggers or that contains `now`.
//!   P5 (restart preserves): the property holds across actor restart, so no
//!       timer rows can be lost when the in-memory state is discarded and
//!       rebuilt against the same store. This includes a restart over a stale
//!       watermark that an older version left.
//!   P6 (cleanup progress): a persisted slab at or below the load high-water
//!       that is neither active nor current-time is deleted by cleanup, even
//!       when it has no trigger rows.
//!   P7 (past-watermark schedule): scheduling an unregistered slab at or below
//!       the watermark lowers the watermark, preserves compact ownership, and
//!       activates the trigger.
//!   P8 (settled schedules): every stored trigger sits in a registered slab,
//!       and a schedule that stops early leaves no entry for a row that does
//!       not exist.

mod checks;
mod fixture;

use super::SLAB_SIZE_SECS;
use crate::Key;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::slab::SlabId;
use crate::timers::test_support::Memory;
use ahash::HashMap;
use checks::check_invariants;
use color_eyre::eyre::{Result, WrapErr};
use fixture::Fixture;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use strum::VariantArray;
use tokio::runtime::Builder as RuntimeBuilder;

/// Bounded domain so quickcheck explores meaningful interleavings
/// rather than spreading triggers across a unique slab per op.
const PROP_KEYS: u8 = 4;
/// Slab offset span (`-PROP_PAST_SLABS..=PROP_FUTURE_SLABS`) relative to
/// `now_slab_id`. Negative offsets eventually fall below the watermark
/// once cleanups have advanced it, exercising the past-time BATCH path.
const PROP_PAST_SLABS: i32 = 5;
const PROP_FUTURE_SLABS: i32 = 5;

/// One element of the random op sequence applied to the actor.
#[derive(Clone, Debug)]
enum Op {
    Schedule(TriggerSpec, Stop),
    Unschedule(TriggerSpec),
    /// Fires the attempt that leaves the queue next.
    Fire,
    Abort(TriggerSpec),
    LoadStep,
    CleanupStep,
    /// Restarts the actor. A stale restart first writes the watermark that
    /// an older version left after a change from 1-minute slabs.
    Restart {
        stale_watermark: bool,
    },
}

/// How a schedule ends. A call dropped before its row write leaves memory as
/// it was. One dropped during the write, or whose write fails, queues the
/// timer whether the row exists or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    Never,
    BeforeWrite,
    AfterWrite,
    FailedWrite,
}

impl Arbitrary for Stop {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => Self::BeforeWrite,
            1 => Self::AfterWrite,
            2 => Self::FailedWrite,
            _ => Self::Never,
        }
    }
}

/// Compact, hashable trigger identity used by the generator and the
/// expected-state model. Resolved to a concrete `(Key, time, type)` at
/// fixture time so all ops within one quickcheck iteration share a
/// consistent `now_slab_id` anchor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TriggerSpec {
    key_idx: u8,
    slab_offset: i32,
    timer_type: TimerType,
}

impl TriggerSpec {
    fn resolve(self, now_slab: SlabId) -> (Key, CompactDateTime, TimerType) {
        let signed = i64::from(now_slab) + i64::from(self.slab_offset);
        let slab_id: SlabId = signed.max(0).try_into().unwrap_or(0);
        let time = CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS));
        (
            Key::from(format!("k{}", self.key_idx)),
            time,
            self.timer_type,
        )
    }
}

fn timer_type_from_idx(idx: u8) -> TimerType {
    TimerType::VARIANTS[usize::from(idx) % TimerType::VARIANTS.len()]
}

impl Arbitrary for TriggerSpec {
    fn arbitrary(g: &mut Gen) -> Self {
        let slab_span: i32 = PROP_PAST_SLABS + PROP_FUTURE_SLABS + 1_i32;
        let raw = i32::from(u8::arbitrary(g)) % slab_span;
        let slab_offset = raw - PROP_PAST_SLABS;
        Self {
            key_idx: u8::arbitrary(g) % PROP_KEYS,
            slab_offset,
            timer_type: timer_type_from_idx(u8::arbitrary(g)),
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        // Bias toward Schedule/Load so the actor accumulates state to
        // exercise rather than thrashing through cleanups and restarts.
        match u8::arbitrary(g) % 12 {
            0..=3 => Self::Schedule(TriggerSpec::arbitrary(g), Stop::arbitrary(g)),
            4..=5 => Self::Unschedule(TriggerSpec::arbitrary(g)),
            6 => Self::Fire,
            7 => Self::Abort(TriggerSpec::arbitrary(g)),
            8..=9 => Self::LoadStep,
            10 => Self::CleanupStep,
            _ => Self::Restart {
                stale_watermark: bool::arbitrary(g),
            },
        }
    }
}

/// Vec of ops with a small minimum length so degenerate empty cases
/// don't dominate the run.
#[derive(Clone, Debug)]
struct OpSequence(Vec<Op>);

impl Arbitrary for OpSequence {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = (u8::arbitrary(g) % 40 + 10) as usize;
        Self((0..n).map(|_| Op::arbitrary(g)).collect())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TriggerModel {
    in_store: bool,
    memory: Option<Memory>,
    /// The tag of the attempt whose delivery runs.
    delivery: Option<i32>,
}

/// Per-trigger model of the stored row and of memory.
type TriggerModels = HashMap<(Key, CompactDateTime, TimerType), TriggerModel>;

/// Enumerates every `(key, time, type)` that the generator can produce
/// given `now_slab`. Used to drive an exhaustive per-triple check after
/// every op.
fn build_universe(now_slab: SlabId) -> Vec<(Key, CompactDateTime, TimerType)> {
    let mut out = Vec::with_capacity(usize::from(PROP_KEYS) * 11 * TimerType::VARIANTS.len());
    for key_idx in 0..PROP_KEYS {
        for offset in -PROP_PAST_SLABS..=PROP_FUTURE_SLABS {
            let signed = i64::from(now_slab) + i64::from(offset);
            let slab_id: SlabId = signed.max(0).try_into().unwrap_or(0);
            let time = CompactDateTime::from(slab_id.saturating_mul(SLAB_SIZE_SECS));
            for &timer_type in TimerType::VARIANTS {
                out.push((Key::from(format!("k{key_idx}")), time, timer_type));
            }
        }
    }
    out
}

async fn run_property(ops: Vec<Op>) -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let same = TriggerSpec {
        key_idx: 0,
        slab_offset: -1,
        timer_type: TimerType::DeferredMessage,
    };
    let prefix = [
        Op::Schedule(same, Stop::Never),
        Op::LoadStep,
        Op::Fire,
        Op::Schedule(same, Stop::Never),
        Op::Abort(same),
        Op::Schedule(same, Stop::BeforeWrite),
    ];
    let history: Vec<_> = prefix.into_iter().chain(ops).collect();
    for (i, op) in history.iter().cloned().enumerate() {
        fixture
            .apply(op.clone())
            .await
            .wrap_err_with(|| format!("op #{i} {op:?}\nhistory: {history:#?}"))?;
        check_invariants(&fixture)
            .await
            .wrap_err_with(|| format!("after op #{i} {op:?}\nhistory: {history:#?}"))?;
    }
    Ok(())
}

#[test]
fn prop_scheduler_invariants() {
    fn property(seq: OpSequence) -> Result<()> {
        RuntimeBuilder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()?
            .block_on(run_property(seq.0))
    }
    QuickCheck::new().quickcheck(property as fn(OpSequence) -> Result<()>);
}
