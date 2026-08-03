//! What reaches a request whose caller never runs again.
//!
//! The two directions are separate on purpose. Each removal path has to work
//! without the other: the caller's own guard for an ordinary call, the sweep
//! for a handle a foreign runtime released late or never.

use super::{
    MAX_TIMEOUT, POOL, SWEEP_GRACE, TestCodec, TestCodecError, TestError, names, poll_once,
    register, registry,
};
use crate::Codec;
use crate::producer::ProducerError;
use crate::requester::collect::collect;
use crate::requester::registry::SWEEP_BATCH;
use color_eyre::Result;
use std::pin::pin;
use tokio::task::yield_now;
use tokio::time::{Instant, advance};

/// Requests one registry in these suites admits.
///
/// One more than a sweep batch, so the drain here fills two batches and the
/// batch loop's continuation is not dead under test.
const IN_FLIGHT: usize = SWEEP_BATCH + 1;

/// Most subsystems one request here names.
const MAX_AWAITED: usize = 2;

/// A call that is never polled again keeps its record and its permit inside the
/// grace period, and loses both once the period has passed.
#[tokio::test(start_paused = true)]
async fn the_sweep_reclaims_a_call_that_stopped() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&POOL[..1])?;
    let registration = register(&registry, &awaited, MAX_TIMEOUT)?;
    let deadline = registration.deadline();

    let produce = async { Ok::<(), ProducerError<TestCodecError>>(()) };
    let mut call = pin!(collect::<TestCodec, u32, TestError, _, TestCodecError>(
        &registration,
        produce,
        deadline,
    ));
    assert!(
        poll_once(call.as_mut()).await.is_pending(),
        "the call must park with no response and no elapsed deadline"
    );
    // Nothing polls `call` again from here on.

    registry.sweep(deadline + SWEEP_GRACE / 2);
    assert_eq!(
        registry.len(),
        1,
        "the sweep removed a request still inside its grace period"
    );
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT - 1,
        "the permit came back while the request was still the caller's"
    );

    registry.sweep(deadline + SWEEP_GRACE);
    assert_eq!(
        registry.len(),
        0,
        "the sweep left a request nothing will ever finish"
    );
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT,
        "the sweep reclaimed the record but not the capacity"
    );
    Ok(())
}

/// The map still empties and every permit still returns when no caller guard
/// exists at all.
#[tokio::test(start_paused = true)]
async fn the_map_empties_without_a_caller_guard() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&POOL[..1])?;
    for _ in 0..IN_FLIGHT {
        registry.register_unguarded(&awaited, TestCodec::FORMAT_ID, MAX_TIMEOUT)?;
    }
    assert_eq!(registry.len(), IN_FLIGHT);
    assert_eq!(registry.available_permits(), 0);

    registry.sweep(Instant::now() + MAX_TIMEOUT + SWEEP_GRACE);
    assert_eq!(registry.len(), 0, "the sweep left records behind");
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT,
        "the sweep did not return every permit"
    );
    Ok(())
}

/// The registry's own sweep task reclaims an expired record, so a caller that
/// never runs again needs nothing else to run either.
///
/// Nothing here calls `sweep`. The task the registry spawned is the only thing
/// that can empty the map.
#[tokio::test(start_paused = true)]
async fn the_spawned_sweep_reclaims_an_expired_record() -> Result<()> {
    let registry = registry(IN_FLIGHT, MAX_AWAITED)?;
    let awaited = names(&POOL[..1])?;
    registry.register_unguarded(&awaited, TestCodec::FORMAT_ID, MAX_TIMEOUT)?;

    advance(MAX_TIMEOUT + 2 * SWEEP_GRACE).await;
    yield_now().await;

    assert_eq!(
        registry.len(),
        0,
        "the spawned sweep left a record a full grace period past its deadline"
    );
    assert_eq!(
        registry.available_permits(),
        IN_FLIGHT,
        "the spawned sweep removed the record but not its permit"
    );
    Ok(())
}
