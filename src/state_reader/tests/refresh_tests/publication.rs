//! Deterministic publication schedules: what two callers observe when they
//! race one generation of a shared snapshot. See the parent module.

use super::element;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::StateType;
use crate::state::descriptor::{ValueDescriptor, value_state};
use crate::state::publication::PublicationStore;
use crate::state::tests::support::{ParkedRead, ScriptedPublicationStore};
use crate::state_reader::StateReaderError;
use crate::state_reader::reader::acquisition::{ABSENT_BACKOFF, REFRESH_BACKOFF};
use crate::state_reader::tests::support::{GROUP_A, GROUP_B, ScriptedEnv, mock_clock_cache, topic};
use crate::{Key, Topic};
use color_eyre::eyre::{Result, bail, eyre};
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// The scripted environment every schedule here drives.
type JsonEnv = ScriptedEnv<ValueDescriptor<JsonCodec>>;

/// The refresh interval a schedule uses when it needs a warm, still-fresh
/// snapshot between steps.
const REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// How long a schedule waits for an operation that must not block. It is a
/// hang guard, never the assertion: the reads these schedules run resolve
/// against in-memory stores and complete immediately.
const HANG_GUARD: Duration = Duration::from_secs(1);

/// A first acquisition must never wait for another caller's refresh.
///
/// Two readers share one collection snapshot and neither has refreshed, so
/// neither has anything to serve. One is parked inside its routing read. The
/// other must still complete: it refreshes speculatively and publishes. The
/// parked caller then loses the publication and adopts the winner's state.
/// Both callers issue one routing read, which is the accepted cost of having
/// no refresh leader.
///
/// Falsify: take an awaited refresh gate before refreshing in
/// `StateReader::snapshot` → the second caller blocks behind the parked one and
/// the hang guard fires.
#[tokio::test(start_paused = true)]
async fn a_first_acquisition_never_waits_for_another_refresh() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-first");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp).await;

    let (cache, _mock) = mock_clock_cache(1 << 20);
    let (parked, other) = env.shared_readers(cache, Duration::ZERO)?;

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    let served = timeout(HANG_GUARD, other.get(key))
        .await
        .map_err(|_| eyre!("a first acquisition waited for another caller's refresh"))??;
    assert_eq!(
        served,
        Some(element(0)),
        "the winner must serve its sources"
    );

    held.release();
    match join(task).await? {
        Ok(Some(value)) if value == element(0) => {}
        other => bail!("the parked caller must adopt the winner's sources, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads(),
        2,
        "each stale caller issues exactly one routing read"
    );
    Ok(())
}

/// A refresh that observed a superseded generation must never publish.
///
/// The parked caller reads the routing table after a withdrawal the winner
/// never saw, so its acquisition is newer in wall-clock terms and older in
/// generation terms. It must lose the publication and serve the winner's
/// sources, and the published state must stay the winner's.
///
/// Falsify: replace the compare-and-swap in `publish` with an unconditional
/// store, or retry the publication after losing → the parked caller serves its
/// own `GROUP_B` element and the follow-up read serves it too.
#[tokio::test(start_paused = true)]
async fn a_parked_refresher_cannot_overwrite_a_newer_publication() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-superseded");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let (tp_a, tp_b) = seed_two_sources(&env, &key).await?;
    env.publish(GROUP_A, tp_a).await;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let (parked, winner) = env.shared_readers(cache, REFRESH_INTERVAL)?;

    assert_eq!(
        winner.get(key.clone()).await?,
        Some(element(0)),
        "the warm-up read must acquire GROUP_A"
    );
    mock.increment(2 * REFRESH_INTERVAL);

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    let served = timeout(HANG_GUARD, winner.get(key.clone()))
        .await
        .map_err(|_| eyre!("the winner waited for the parked caller's refresh"))??;
    assert_eq!(
        served,
        Some(element(0)),
        "the winner republishes GROUP_A from the generation it observed"
    );

    // The parked caller's own read now returns a routing table the winner never
    // saw. Its acquisition still derives from the superseded generation.
    withdraw(&env, GROUP_A).await?;
    env.publish(GROUP_B, tp_b).await;
    held.release();
    match join(task).await? {
        Ok(Some(value)) if value == element(0) => {}
        other => bail!("the losing caller must serve the winner's sources, got {other:?}"),
    }

    let before = env.publications.reads();
    assert_eq!(
        winner.get(key).await?,
        Some(element(0)),
        "the published state must still be the winner's"
    );
    assert_eq!(
        env.publications.reads() - before,
        0,
        "the winner's publication must still be fresh"
    );
    Ok(())
}

/// A failed refresh that loses the publication must consume the winner's
/// outcome, never roll it back.
///
/// The parked caller holds `GROUP_A` from a superseded generation. Its own read
/// fails, so its candidate keeps `GROUP_A` and paces the retry. The winner has
/// already published `GROUP_B`, so the failure must be discarded whole: neither
/// its sources nor its pacing may reach the published state.
///
/// Falsify: replace the compare-and-swap in `publish` with an unconditional
/// store, or retry the publication after losing → the losing caller serves its
/// retained `GROUP_A` element.
#[tokio::test(start_paused = true)]
async fn a_failed_refresh_consumes_a_publication_that_won() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-failed-loser");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let (tp_a, tp_b) = seed_two_sources(&env, &key).await?;
    env.publish(GROUP_A, tp_a).await;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let (parked, winner) = env.shared_readers(cache, REFRESH_INTERVAL)?;

    assert_eq!(
        winner.get(key.clone()).await?,
        Some(element(0)),
        "the warm-up read must acquire GROUP_A"
    );
    mock.increment(2 * REFRESH_INTERVAL);

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    withdraw(&env, GROUP_A).await?;
    env.publish(GROUP_B, tp_b).await;
    let served = timeout(HANG_GUARD, winner.get(key.clone()))
        .await
        .map_err(|_| eyre!("the winner waited for the parked caller's refresh"))??;
    assert_eq!(served, Some(element(1)), "the winner must publish GROUP_B");

    env.publications.fail_reads_with(ErrorCategory::Transient);
    held.release();
    match join(task).await? {
        Ok(Some(value)) if value == element(1) => {}
        other => bail!("the failed caller must serve the winner's sources, got {other:?}"),
    }

    env.publications.heal_reads();
    let before = env.publications.reads();
    assert_eq!(
        winner.get(key).await?,
        Some(element(1)),
        "the failure must not roll the published sources back"
    );
    assert_eq!(
        env.publications.reads() - before,
        0,
        "the failure must not install pacing over the winner's publication"
    );
    Ok(())
}

/// A successful refresh that loses the publication also consumes the winner,
/// even when the winner published a failure.
///
/// This is the accepted consequence of the rule that a loser never republishes:
/// a completed successful read can answer `RefreshUnavailable`. The window is
/// bounded by [`REFRESH_BACKOFF`], and the alternative needs a generation
/// marker and a second compare-and-swap (see `publish`).
///
/// Falsify: replace the compare-and-swap in `publish` with an unconditional
/// store, or retry the publication after losing → the parked caller answers
/// with its own sources instead of the published failure.
#[tokio::test(start_paused = true)]
async fn a_successful_refresh_consumes_a_paced_failure_that_won() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-paced-winner");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.publish(GROUP_A, tp).await;

    let (cache, _mock) = mock_clock_cache(1 << 20);
    let (parked, other) = env.shared_readers(cache, Duration::ZERO)?;

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    env.publications.fail_reads_with(ErrorCategory::Transient);
    match timeout(HANG_GUARD, other.get(key.clone()))
        .await
        .map_err(|_| eyre!("the failing caller waited for the parked caller's refresh"))?
    {
        Err(StateReaderError::Store { .. }) => {}
        other => bail!("a failure with nothing held must propagate the store error, got {other:?}"),
    }

    env.publications.heal_reads();
    held.release();
    match join(task).await? {
        Err(StateReaderError::RefreshUnavailable { .. }) => {}
        other => bail!("the losing caller must consume the paced failure, got {other:?}"),
    }

    let before = env.publications.reads();
    match other.get(key).await {
        Err(StateReaderError::RefreshUnavailable { .. }) => {}
        other => bail!("the pacing window must still be open, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads() - before,
        0,
        "a read inside the pacing window must not re-read the routing table"
    );
    Ok(())
}

/// A failed refresh paces from the instant its read returned, not from the
/// instant the caller decided to refresh.
///
/// The clock advances while the routing read is parked, so a deadline sampled
/// before the read would already be half spent when the read returns. The
/// schedule proves a full [`REFRESH_BACKOFF`] window remains, and that the very
/// first read at the deadline re-reads the routing table.
///
/// Falsify: sample the deadline from the pre-refresh instant in `failed` → the
/// read halfway through the window re-reads and answers with the store error
/// instead of `RefreshUnavailable`.
#[tokio::test(start_paused = true)]
async fn a_failure_deadline_is_sampled_after_the_read_returns() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-failure-deadline");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let (parked, after) = env.shared_readers(cache, Duration::ZERO)?;

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    // A slow store read: the whole backoff elapses before the read returns.
    mock.increment(REFRESH_BACKOFF);
    env.publications.fail_reads_with(ErrorCategory::Transient);
    held.release();
    match join(task).await? {
        Err(StateReaderError::Store { .. }) => {}
        other => bail!("the parked read must fail with the store error, got {other:?}"),
    }

    mock.increment(REFRESH_BACKOFF / 2);
    let before = env.publications.reads();
    match after.get(key.clone()).await {
        Err(StateReaderError::RefreshUnavailable { .. }) => {}
        other => bail!("halfway through the window the read must be paced, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads() - before,
        0,
        "a paced read must not re-read the routing table"
    );

    env.publications.heal_reads();
    env.publish(GROUP_A, tp).await;
    mock.increment(REFRESH_BACKOFF / 2);
    let before = env.publications.reads();
    assert_eq!(
        after.get(key).await?,
        Some(element(0)),
        "the read at the deadline must refresh"
    );
    assert_eq!(
        env.publications.reads() - before,
        1,
        "the window ends exactly one backoff after the read returned"
    );
    Ok(())
}

/// The absence deadline is sampled the same way as the failure deadline: after
/// the refresh's read returned.
///
/// This is the twin of [`a_failure_deadline_is_sampled_after_the_read_returns`]
/// for [`ABSENT_BACKOFF`]. The served error is `UnknownPublication` whether the
/// read is paced or re-reads an empty routing table, so the routing-read count
/// is the detector.
///
/// Falsify: sample the deadline from the pre-refresh instant in `acquire` → the
/// read halfway through the window re-reads the routing table and the
/// read-count assert reds.
#[tokio::test(start_paused = true)]
async fn an_absence_deadline_is_sampled_after_the_read_returns() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("publication-absence-deadline");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let (parked, after) = env.shared_readers(cache, Duration::ZERO)?;

    let parked_key = key.clone();
    let (task, held) = park(
        &env.publications,
        async move { parked.get(parked_key).await },
    )
    .await?;

    // A slow store read: the whole absent backoff elapses before the empty
    // routing table comes back.
    mock.increment(ABSENT_BACKOFF);
    held.release();
    match join(task).await? {
        Err(StateReaderError::UnknownPublication { .. }) => {}
        other => bail!("an empty routing table must answer UnknownPublication, got {other:?}"),
    }

    mock.increment(ABSENT_BACKOFF / 2);
    let before = env.publications.reads();
    match after.get(key.clone()).await {
        Err(StateReaderError::UnknownPublication { .. }) => {}
        other => bail!("the absence must still be held, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads() - before,
        0,
        "a paced read must not re-read the routing table"
    );

    env.publish(GROUP_A, tp).await;
    mock.increment(ABSENT_BACKOFF / 2);
    let before = env.publications.reads();
    assert_eq!(
        after.get(key).await?,
        Some(element(0)),
        "the read at the deadline must admit the publisher that appeared"
    );
    assert_eq!(
        env.publications.reads() - before,
        1,
        "the window ends exactly one absent backoff after the read returned"
    );
    Ok(())
}

/// Commits `element(0)` under `GROUP_A` and `element(1)` under `GROUP_B`,
/// returning their topics. Neither is advertised yet.
async fn seed_two_sources(env: &JsonEnv, key: &Key) -> Result<(Topic, Topic)> {
    let tp_a = topic(GROUP_A);
    let tp_b = topic(GROUP_B);
    env.commit(GROUP_A, tp_a, key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    env.commit(GROUP_B, tp_b, key, 2, |handle| async move {
        handle.set(element(1)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;
    Ok((tp_a, tp_b))
}

/// Withdraws `group`'s publication row.
async fn withdraw(env: &JsonEnv, group: &str) -> Result<()> {
    env.publications
        .remove_group(&env.sub, StateType::Application, &env.name, group)
        .await
        .map_err(|e| eyre!("remove_group: {e}"))
}

/// Spawns `op` and parks its routing read at the publication store's gate, then
/// stops gating so every later read runs to completion while `op` stays parked.
/// Returns the parked task and the handle that releases it.
async fn park<F>(
    publications: &ScriptedPublicationStore,
    op: F,
) -> Result<(JoinHandle<F::Output>, ParkedRead)>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    publications.gate_reads();
    let task = tokio::spawn(op);
    publications.wait_read_entered().await;
    let parked = publications
        .stop_gating_reads()
        .ok_or_else(|| eyre!("the read gate must still be installed"))?;
    Ok((task, parked))
}

/// Joins a parked task under the hang guard.
async fn join<T>(task: JoinHandle<T>) -> Result<T> {
    timeout(HANG_GUARD, task)
        .await
        .map_err(|_| eyre!("hang guard: the parked read never resumed"))?
        .map_err(|error| eyre!("parked task: {error}"))
}
