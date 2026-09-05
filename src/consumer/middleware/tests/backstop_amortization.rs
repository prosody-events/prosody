//! Sweep-posture commits arm only when promotion is incomplete.
use super::*;
use crate::consumer::middleware::tests::test_support::{TestLifecycleAccess, buffered_with};
use crate::error::ErrorCategory;
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

/// Five resolved message commits do not arm a safety timer.
#[tokio::test]
async fn resolved_message_commits_do_not_arm() -> Result<()> {
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
        let handler = ProbeHandler::ok(i as u64);
        let (guard, committed, aborted) = RecordingGuard::new();
        settle(&handler, context.clone(), guard, Ok(i as u64)).await;
        assert_eq!(committed.load(Ordering::SeqCst), 1);
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        total_scheduled += context.count_scheduled(TimerType::StateRecovery);
    }

    assert_eq!(
        total_scheduled, 0,
        "resolved sweep-posture commits need no safety timer"
    );
    Ok(())
}

/// Two incomplete message commits share one standing safety timer.
#[tokio::test]
async fn incomplete_commits_arm_once_while_timer_stands() -> Result<()> {
    let armed: ArmedKeys = Arc::default();
    let mut scheduled = 0;

    for sentinel in 0..2 {
        let context = buffered_with(
            armed.clone(),
            Some((ErrorCategory::Permanent, 1)),
            None,
            MockEventContext::with_timer_tracking,
        )
        .await?
        .0;
        let handler = ProbeHandler::ok(sentinel);
        let (guard, committed, aborted) = RecordingGuard::new();

        settle(&handler, context.clone(), guard, Ok(sentinel)).await;

        assert_eq!(committed.load(Ordering::SeqCst), 1);
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
        scheduled += context.count_scheduled(TimerType::StateRecovery);
    }

    assert_eq!(scheduled, 1, "the standing timer covers the later commit");
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
