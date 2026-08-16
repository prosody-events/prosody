//! `arm_backstop` is arm-if-sooner: it (re-)arms the per-key `StateRecovery`
//! backstop only when a newly-staged commit's fire is strictly sooner than the
//! standing one. A per-collection `recovery_within` can thereby *tighten* the
//! single timer, while a later, looser commit keeps the tighter one — so every
//! staged cell is swept no later than its own bound and the amortized single
//! timer is preserved.
use super::*;
use crate::Key;
use crate::codec::JsonCodec;
use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
use crate::consumer::middleware::tests::test_support::TimerOperation;
use crate::loader::MemoryLoader;
use crate::state::StateKey;
use crate::state::descriptor::tests::{TestSession, test_session_with_armed};
use crate::state::descriptor::{Registered, value_state};
use crate::state::manager::ArmedKeys;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Finalized;
use crate::state::session::sealed::StateLifecycle;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor;
use quickcheck::{QuickCheck, TestResult};
use serde_json::json;
use uuid::Uuid;

const FLOOR_SECS: u32 = 30;

/// The fire time of the most recent `StateRecovery` `clear_and_schedule`,
/// or `None` if the arm did not (re-)schedule one.
fn scheduled_recovery_fire(ops: &[TimerOperation]) -> Option<CompactDateTime> {
    ops.iter().rev().find_map(|op| match op {
        TimerOperation::ClearAndSchedule(fire, TimerType::StateRecovery) => Some(*fire),
        _ => None,
    })
}

/// Stages a cell in one `ReadCommitted` value collection per entry in
/// `bounds` (each carrying that `recovery_within`), sharing `armed`/`key`
/// with prior events, then runs `arm_backstop`. `durable` seeds a
/// `StateRecovery` timer standing in the mock's durable store (a prior
/// epoch's backstop). Brackets the arm with `now` so the caller can bound
/// the scheduled fire time; returns the context for op/durable inspection.
async fn run_arm(
    bounds: &[Option<u32>],
    key: &StateKey,
    armed: &ArmedKeys,
    durable: Option<CompactDateTime>,
) -> Result<(
    CompactDateTime,
    CompactDateTime,
    MockEventContext<serde_json::Value, TestSession>,
)> {
    let mut registry = CollectionDefRegistry::default();
    for (i, within) in bounds.iter().enumerate() {
        registry.register(
            &value_state::<JsonCodec>(&format!("c{i}")),
            CollectionDef {
                recovery_within: within.map(CompactDuration::new),
                ..CollectionDef::new(None)
            },
        )?;
    }
    let (session, _store) =
        test_session_with_armed(MemoryLoader::new(), registry, key.clone(), armed.clone());
    let mut context = MockEventContext::new()
        .with_session(session)
        .with_timer_tracking();
    if let Some(time) = durable {
        context = context.with_durable_timer(time, TimerType::StateRecovery);
    }
    for i in 0..bounds.len() {
        let handle = context
            .state(Registered::new(value_state::<JsonCodec>(&format!("c{i}"))))
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.set(json!({ "v": i as i32 })).await?;
    }
    let lifecycle = context
        .test_lifecycle()
        .map_err(|e| eyre!("lifecycle: {e}"))?;
    let Finalized::Staged(staged) = lifecycle
        .finalize()
        .await
        .map_err(|e| eyre!("finalize: {e}"))?
    else {
        bail!("expected a staged receipt (the props stage at least one collection)");
    };
    let before = CompactDateTime::now()?;
    arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
    let after = CompactDateTime::now()?;
    Ok((before, after, context))
}

/// Arm-if-sooner **and** the convergence bound, as one property over
/// `(bounds, standing)`:
///
/// - **Fire time** — a staged event schedules its `StateRecovery` sweep at now
///   plus the minimum recovery bound. Thus, no provisional cell outlives its
///   collection's bound and the fire never exceeds the floor (hence stays below
///   every collection's TTL, the `TtlBelowRecoveryDelay` invariant — no
///   dedup-margin regression).
/// - **Arm-if-sooner** — a standing backstop is re-armed **iff** the new fire
///   is strictly sooner; on re-arm `ArmedKeys` holds the scheduled fire, and
///   when kept the standing fire is left untouched.
///
/// `standing`: `None` = unarmed → arm; `Some(true)` = a far-future standing
/// fire (looser) → must tighten; `Some(false)` = a far-past one (tighter) →
/// must keep. The far-future/past extremes make the strict-`<` decision
/// immune to sub-second wall-clock drift across the bracketing `now` reads.
#[test]
fn prop_arm_backstop_arms_iff_new_fire_is_sooner() {
    async fn check(bounds: &[Option<u32>], standing: Option<bool>) -> Result<()> {
        let armed: ArmedKeys = Arc::default();
        let raw_key: Key = Arc::from("k");
        let key = StateKey::new(Uuid::from_u128(0xC0), raw_key.clone());

        // Seed the standing backstop at an extreme so the decision cannot
        // flip on sub-second drift: MAX is always later than the new fire
        // (must tighten), MIN always earlier-or-equal (must keep).
        let seed = standing.map(|future| {
            if future {
                CompactDateTime::MAX
            } else {
                CompactDateTime::MIN
            }
        });
        if let Some(seed) = seed {
            let _ = armed.insert_async(raw_key.clone(), seed).await;
        }

        let (before, after, context) = run_arm(bounds, &key, &armed, None).await?;
        let ops = context.timer_operations();
        let delay = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);
        let stored = armed.read_async(&raw_key, |_, &f| f).await;
        let scheduled = scheduled_recovery_fire(&ops);

        // Re-arm unless a sooner-or-equal backstop (the far-past seed)
        // already stands.
        if standing == Some(false) {
            if scheduled.is_some() {
                return Err(eyre!("a sooner-or-equal standing backstop must not re-arm"));
            }
            if stored != seed {
                return Err(eyre!(
                    "the kept path must leave the standing fire untouched"
                ));
            }
            return Ok(());
        }

        let fire = scheduled.ok_or_else(|| eyre!("expected an arm, none scheduled"))?;
        let (lo, hi) = (
            before.epoch_seconds() + delay,
            after.epoch_seconds() + delay,
        );
        if !(lo..=hi).contains(&fire.epoch_seconds()) {
            return Err(eyre!(
                "fire {} not in [{lo},{hi}] (delay {delay}s ≤ floor {FLOOR_SECS}s)",
                fire.epoch_seconds(),
            ));
        }
        if stored != Some(fire) {
            return Err(eyre!("stored fire {stored:?} != scheduled {fire:?}"));
        }
        Ok(())
    }

    fn prop(raw: Vec<Option<u16>>, standing: Option<bool>) -> TestResult {
        // ≥1 collection (so something stages); bounded count keeps the
        // interned-name set small.
        if raw.is_empty() || raw.len() > 6 {
            return TestResult::discard();
        }
        let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
        match executor::block_on(check(&bounds, standing)) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(e.to_string()),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>, Option<bool>) -> TestResult);
}

/// Never-loosen across reacquisition: `ArmedKeys` is minted empty per
/// acquisition while a prior epoch's backstop survives in the durable
/// trigger store, so the first arm on a key must consult the durable
/// store. A sooner standing durable fire is kept (no singleton overwrite)
/// and seeded into `ArmedKeys`; a later one is tightened. Either way the
/// standing durable fire never moves later.
///
/// `standing`: `None` = no durable backstop (plain first arm);
/// `Some(false)` = far-past durable fire (sooner) → must keep;
/// `Some(true)` = far-future durable fire (later) → must tighten. The
/// extremes make the decision immune to sub-second wall-clock drift, as in
/// `prop_arm_backstop_arms_iff_new_fire_is_sooner`.
#[test]
fn prop_reacquisition_never_loosens_standing_backstop() {
    async fn check(bounds: &[Option<u32>], standing: Option<bool>) -> Result<()> {
        // Fresh per-acquisition RAM: the durable seed is the only record
        // of the prior epoch's backstop.
        let armed: ArmedKeys = Arc::default();
        let raw_key: Key = Arc::from("k");
        let key = StateKey::new(Uuid::from_u128(0xC1), raw_key.clone());
        let durable = standing.map(|future| {
            if future {
                CompactDateTime::MAX
            } else {
                CompactDateTime::MIN
            }
        });

        let (before, after, context) = run_arm(bounds, &key, &armed, durable).await?;
        let ops = context.timer_operations();
        let now_durable = context.durable_scheduled(TimerType::StateRecovery);
        let stored = armed.read_async(&raw_key, |_, &f| f).await;
        let scheduled = scheduled_recovery_fire(&ops);
        let delay = bounds.iter().filter_map(|o| *o).fold(FLOOR_SECS, u32::min);

        if standing == Some(false) {
            if scheduled.is_some() {
                return Err(eyre!(
                    "a sooner durable backstop must not be overwritten (loosened)"
                ));
            }
            if now_durable != vec![CompactDateTime::MIN] {
                return Err(eyre!(
                    "the sooner durable fire must be left standing, got {now_durable:?}"
                ));
            }
            if stored != Some(CompactDateTime::MIN) {
                return Err(eyre!(
                    "the durable fire must seed the fresh ArmedKeys, got {stored:?}"
                ));
            }
            return Ok(());
        }

        // No durable backstop, or a later (far-future) one: arm/tighten.
        let fire = scheduled.ok_or_else(|| eyre!("expected an arm, none scheduled"))?;
        let (lo, hi) = (
            before.epoch_seconds() + delay,
            after.epoch_seconds() + delay,
        );
        if !(lo..=hi).contains(&fire.epoch_seconds()) {
            return Err(eyre!("fire {} not in [{lo},{hi}]", fire.epoch_seconds()));
        }
        if now_durable != vec![fire] {
            return Err(eyre!(
                "the singleton overwrite must leave exactly the new fire standing, got \
                 {now_durable:?}"
            ));
        }
        if stored != Some(fire) {
            return Err(eyre!("stored fire {stored:?} != scheduled {fire:?}"));
        }
        Ok(())
    }

    fn prop(raw: Vec<Option<u16>>, standing: Option<bool>) -> TestResult {
        if raw.is_empty() || raw.len() > 6 {
            return TestResult::discard();
        }
        let bounds: Vec<Option<u32>> = raw.into_iter().map(|o| o.map(u32::from)).collect();
        match executor::block_on(check(&bounds, standing)) {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(e.to_string()),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<Option<u16>>, Option<bool>) -> TestResult);
}
