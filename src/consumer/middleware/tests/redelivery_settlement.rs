use super::*;
use crate::consumer::middleware::tests::test_support::{
    DuplicateHandler, TestLifecycleAccess, buffered, buffered_failing_sweep, committed_value,
    is_provisional,
};
use crate::consumer::receipted_sealed;
use crate::state::CollectionId;
use crate::state::memory::MemoryCellStore;
use crate::state::session::Finalized;
use crate::state::session::sealed::StateLifecycle;
use crate::state::tests::support::FixedOracle;
use color_eyre::eyre::{Result, bail, eyre};
use std::future::{Future, ready};
use std::sync::atomic::AtomicBool;

struct OrderGuard {
    store: MemoryCellStore<FixedOracle>,
    id: CollectionId,
    receipt_saw_provisional: Arc<AtomicBool>,
    retire_saw_resolved: Arc<AtomicBool>,
}

impl Uncommitted for OrderGuard {
    async fn commit(self) {
        let resolved = is_provisional(&self.store, &self.id)
            .await
            .is_ok_and(|provisional| !provisional);
        self.retire_saw_resolved.store(resolved, Ordering::SeqCst);
    }

    async fn abort(self) {}
}

impl receipted_sealed::Sealed for OrderGuard {}

impl Receipted for OrderGuard {
    fn redelivery(&self) -> impl Future<Output = Redelivery> + Send {
        ready(Redelivery::Sweeps)
    }

    async fn receipt(&mut self) {
        let provisional = is_provisional(&self.store, &self.id)
            .await
            .is_ok_and(|provisional| provisional);
        self.receipt_saw_provisional
            .store(provisional, Ordering::SeqCst);
    }
}

/// A duplicate sweeps its committed provisional cell before source retirement.
#[tokio::test]
async fn duplicate_sweeps_then_commits() -> Result<()> {
    let (context, store, cart_id) = buffered(|context| context).await?;
    let lifecycle = context
        .test_lifecycle()
        .map_err(|error| eyre!("lifecycle: {error}"))?;
    if !matches!(lifecycle.finalize().await?, Finalized::Staged(_)) {
        bail!("expected staged state");
    }
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&DuplicateHandler, context, guard, Ok(())).await;

    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(aborted.load(Ordering::SeqCst), 0);
    assert!(!is_provisional(&store, &cart_id).await?);
    assert!(committed_value(&store, &cart_id).await?.is_some());
    Ok(())
}

/// Sweep posture records its receipt before promotion and retires after it.
#[tokio::test]
async fn sweep_posture_orders_receipt_promote_retire() -> Result<()> {
    let (context, store, id) = buffered(|context| context).await?;
    let receipt_saw_provisional = Arc::new(AtomicBool::new(false));
    let retire_saw_resolved = Arc::new(AtomicBool::new(false));
    let guard = OrderGuard {
        store,
        id,
        receipt_saw_provisional: receipt_saw_provisional.clone(),
        retire_saw_resolved: retire_saw_resolved.clone(),
    };

    settle(&ProbeHandler::ok(1), context, guard, Ok(1)).await;

    assert!(receipt_saw_provisional.load(Ordering::SeqCst));
    assert!(retire_saw_resolved.load(Ordering::SeqCst));
    Ok(())
}

/// Shutdown after a duplicate sweep failure preserves its redelivery source.
#[tokio::test]
async fn duplicate_shutdown_mid_sweep_abandons() -> Result<()> {
    let context = buffered_failing_sweep(
        ErrorCategory::Transient,
        Arc::default(),
        MockEventContext::with_shutdown,
    )
    .await?;
    let lifecycle = context
        .test_lifecycle()
        .map_err(|error| eyre!("lifecycle: {error}"))?;
    if !matches!(lifecycle.finalize().await?, Finalized::Staged(_)) {
        bail!("expected staged state");
    }
    let (guard, committed, aborted) = RecordingGuard::new();

    settle(&DuplicateHandler, context, guard, Ok(())).await;

    assert_eq!(committed.load(Ordering::SeqCst), 0);
    assert_eq!(aborted.load(Ordering::SeqCst), 1);
    Ok(())
}
