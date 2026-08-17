//! `ArmState` amortization: while a `StateRecovery` backstop stands for a key,
//! later stateful commits on that key skip re-arming, so a burst issues at most
//! one timer-store write per backstop generation.
use super::*;
use crate::consumer::middleware::tests::test_support::TestLifecycleAccess;
use crate::loader::MemoryLoader;
use crate::state::StateKey;
use crate::state::descriptor::tests::test_session_with_armed;
use crate::state::descriptor::{Registered, ValueDescriptor, value_state};
use crate::state::manager::ArmedKeys;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::Finalized;
use crate::state::session::sealed::StateLifecycle;
use color_eyre::eyre::{Result, bail, eyre};
use serde_json::json;
use uuid::Uuid;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Five commits on one key, all sharing the partition's `armed` set, arm
/// the backstop exactly once: the first commit schedules, the rest skip
/// while it stands.
#[tokio::test]
async fn commits_while_armed_schedule_at_most_once() -> Result<()> {
    const COMMITS: usize = 5;
    let armed: ArmedKeys = Arc::default();
    let state_key = StateKey::new(Uuid::from_u128(0x9), Arc::from("hot-key"));
    let mut total_scheduled = 0;

    for i in 0..COMMITS {
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        // A fresh session per event, all sharing the one `armed` set and key
        // — exactly how the manager mints sessions for a partition.
        let (session, _store) = test_session_with_armed(
            MemoryLoader::new(),
            registry,
            state_key.clone(),
            armed.clone(),
        );
        let context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();

        // Stage a cell so arming is warranted.
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.set(json!({ "i": i as i32 })).await?;
        let lifecycle = context
            .test_lifecycle()
            .map_err(|e| eyre!("lifecycle: {e}"))?;
        let Finalized::Staged(staged) = lifecycle
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?
        else {
            bail!("expected a staged receipt");
        };

        let outcome = arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
        assert!(
            matches!(outcome, ArmOutcome::Armed),
            "arm must succeed every commit"
        );
        total_scheduled += context.count_scheduled(TimerType::StateRecovery);
    }

    assert_eq!(
        total_scheduled, 1,
        "only the first commit of the armed generation schedules a backstop"
    );
    Ok(())
}

/// The amortization is per key: a commit on a different key arms its own
/// backstop even while the first key's stands.
#[tokio::test]
async fn a_different_key_arms_independently() -> Result<()> {
    let armed: ArmedKeys = Arc::default();
    let mut scheduled = 0;

    for raw_key in ["key-a", "key-b"] {
        let state_key = StateKey::new(Uuid::from_u128(0xA), Arc::from(raw_key));
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        let (session, _store) =
            test_session_with_armed(MemoryLoader::new(), registry, state_key, armed.clone());
        let context = MockEventContext::new()
            .with_session(session)
            .with_timer_tracking();
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.set(json!({ "x": 1_i32 })).await?;
        let lifecycle = context
            .test_lifecycle()
            .map_err(|e| eyre!("lifecycle: {e}"))?;
        let Finalized::Staged(staged) = lifecycle
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?
        else {
            bail!("expected a staged receipt");
        };
        arm_backstop(&context, &lifecycle, staged.recovery_delay()).await;
        scheduled += context.count_scheduled(TimerType::StateRecovery);
    }

    assert_eq!(scheduled, 2, "each distinct key arms its own backstop");
    Ok(())
}
