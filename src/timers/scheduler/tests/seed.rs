//! Checks how the actor seeds its slab watermark at start.

use crate::Key;
use crate::consumer::partition::ShutdownPhase;
use crate::heartbeat::HeartbeatRegistry;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::scheduler::TriggerScheduler;
use crate::timers::slab::Slab;
use crate::timers::store::TriggerStore;
use crate::timers::store::memory::memory_store;
use crate::timers::test_support::test_segment;
use crate::timers::{TimerType, Trigger};
use color_eyre::eyre::{Result, eyre};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::Span;

/// An older version kept a 1-minute watermark across a change to 5-minute
/// slabs. That watermark is above the current slab and hides the slab row of
/// a due timer. The actor must discard it, then load and fire the timer.
#[tokio::test(start_paused = true)]
async fn test_stale_watermark_does_not_hide_timers() -> Result<()> {
    let store = memory_store(test_segment("stale-watermark", 300_u32));
    let trigger = Trigger::new(
        Key::from("stale-watermark"),
        CompactDateTime::now()?,
        TimerType::Application,
        Span::current(),
    );
    let old_slab = Slab::from_time(CompactDuration::new(60), trigger.time);
    store.insert_segment().await?;
    store.add_trigger(trigger.clone()).await?;
    store
        .insert_slab(Slab::from_time(store.slab_size(), trigger.time))
        .await?;
    store
        .set_slab_watermark(old_slab.id().checked_sub(1))
        .await?;

    let segment = store
        .get_segment()
        .await?
        .ok_or_else(|| eyre!("segment missing"))?;
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (mut fired, _scheduler) =
        TriggerScheduler::new(store, segment, &HeartbeatRegistry::test(), shutdown_rx);

    let observed = timeout(Duration::from_secs(5), fired.recv())
        .await?
        .ok_or_else(|| eyre!("scheduler stopped"))?;
    assert_eq!(observed, trigger, "the actor must fire the hidden timer");
    Ok(())
}
