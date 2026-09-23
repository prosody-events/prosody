//! Invariants of the read-through TTL cache.
//!
//! [`prop_cache_staleness`] proves the staleness rules together over random
//! clock and get schedules, checked against a plain `HashMap` model: the
//! issue-time age, expiry, batch refresh, negative caching, and cache-key
//! isolation. The focused tests below pin invariants that schedule cannot
//! express: concurrent single-flight, failed presence upgrades, slow fills, and
//! the byte-budget bound. Its key pool includes two namespaces with the same
//! collection name, proving `StateType` participates in cache identity.
//!
//! Every test drives a mocked monotonic clock instead of sleeping, so timing
//! stays deterministic. The cache is exercised directly, with no
//! stores underneath, so each invariant is isolated.
//! Each fill closure supplies the concrete future type that the cache requires.

use super::support::{mock_clock_cache, topic};
use crate::Key;
use crate::state::access::StateAccessError;
use crate::state::cell::{Presence, Values};
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::store::CellBuffer;
use crate::state::{StateName, StateType};
use crate::state_reader::cache::{CacheKey, CacheLookup};
use crate::state_reader::{PartitionCount, source::SourceId};
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::executor::block_on;
use quanta::Instant;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use smallvec::smallvec;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::yield_now;

mod staleness;

/// A cache key for the given collection name at cell coordinate `coord`.
fn key_at(
    state_type: StateType,
    name: &str,
    partition_count: i32,
    coord: Vec<u8>,
) -> Result<CacheKey> {
    Ok((
        SourceId {
            group_id: Arc::from("group-aaa"),
            topic: topic("t"),
            partition_count: PartitionCount::try_from(partition_count)?,
        },
        state_type,
        StateName::try_new(name)?,
        Key::from("user-1"),
        CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(coord),
        },
    ))
}

/// A cache key for the given collection name and one fixed cell.
fn lookup(key: &CacheKey) -> CacheLookup<'_> {
    CacheLookup((&key.0, key.1, &key.2, &key.3, key.4.as_ref()))
}

fn key(name: &str) -> Result<CacheKey> {
    key_at(StateType::Application, name, 1, vec![0])
}

// --- Staleness property -----------------------------------------------------

// --- Focused survivors (invariants the serial schedule cannot express) ------

/// A slow fill enters already-aged, so it cannot launder an old value into a
/// fresh window for a later reader. A failed value fill retains fresh presence.
///
/// Falsify: record the entry at fill completion instead of issue. The second
/// read then sees age zero and `fills` stays one.
#[tokio::test]
async fn slow_fill_cannot_launder() -> Result<()> {
    let (cache, mock) = mock_clock_cache(1 << 20);
    let k = key("slow")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let ttl = Duration::from_secs(5);
    // The fill advances the clock past the ttl before returning — a "slow"
    // store read. The issue time was recorded at t=0, not here.
    let fill = || {
        let fills = fills.clone();
        let mock = mock.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            mock.increment(Duration::from_secs(10));
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    // Issued at t=0, completes at t=10s; the fill serves its own result.
    let got = cache
        .get_cached::<Values, _, _>(lookup(&k), ttl, fill)
        .await?;
    assert_eq!(got, Some(Bytes::from_static(b"v")));
    assert_eq!(fills.load(Ordering::Relaxed), 1);

    // A later reader at t=10s: age 10s >= ttl → miss → refill. The refill
    // advances the clock again, which only ages it further.
    cache
        .get_cached::<Values, _, _>(lookup(&k), ttl, fill)
        .await?;
    assert_eq!(
        fills.load(Ordering::Relaxed),
        2,
        "the slow fill was timed from issue, so it expired for the next reader"
    );
    // The value-only schedule cannot represent a failed upgrade of presence.
    let k = key("failed-upgrade")?;
    cache
        .get_cached::<Presence, _, _>(lookup(&k), ttl, || async { Ok(Some(())) })
        .await?;
    let failed = cache
        .get_cached::<Values, _, _>(lookup(&k), ttl, || async {
            Err(StateAccessError::Terminated)
        })
        .await;
    assert!(matches!(failed, Err(StateAccessError::Terminated)));
    let before = fills.load(Ordering::Relaxed);
    let presence = cache
        .get_cached::<Presence, _, _>(lookup(&k), ttl, || async {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        })
        .await?;
    assert_eq!(presence, Some(()));
    assert_eq!(fills.load(Ordering::Relaxed), before);
    Ok(())
}

/// Two concurrent value reads share one fill for a cold key or fresh presence.
///
/// Falsify: let `Read::Unknown` call `fill` without a guard.
/// The presence upgrade then adds two fills instead of one.
#[tokio::test]
async fn cold_miss_is_single_flight() -> Result<()> {
    let (cache, _mock) = mock_clock_cache(1 << 20);
    let k = key("single-flight")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    let ttl = Duration::from_secs(1);
    let (a, b) = tokio::join!(
        cache.get_cached::<Values, _, _>(lookup(&k), ttl, fill),
        cache.get_cached::<Values, _, _>(lookup(&k), ttl, fill),
    );
    assert_eq!(a?, Some(Bytes::from_static(b"v")));
    assert_eq!(b?, Some(Bytes::from_static(b"v")));
    assert_eq!(
        fills.load(Ordering::Relaxed),
        1,
        "single-flight: one fill serves both"
    );

    let k = key("single-flight-upgrade")?;
    cache
        .get_cached::<Presence, _, _>(lookup(&k), ttl, || async { Ok(Some(())) })
        .await?;
    let before = fills.load(Ordering::Relaxed);
    let upgrade = || async {
        fills.fetch_add(1, Ordering::Relaxed);
        yield_now().await;
        Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
    };
    let (a, b) = tokio::join!(
        cache.get_cached::<Values, _, _>(lookup(&k), ttl, upgrade),
        cache.get_cached::<Values, _, _>(lookup(&k), ttl, upgrade),
    );
    assert_eq!(a?, Some(Bytes::from_static(b"v")));
    assert_eq!(b?, Some(Bytes::from_static(b"v")));
    assert_eq!(
        fills.load(Ordering::Relaxed) - before,
        1,
        "one fill upgrades presence for both value readers"
    );
    Ok(())
}

/// The batch read serves entirely from the cache when every key is a fresh
/// hit, firing zero fills. A single stale key triggers exactly ONE
/// whole-batch refill, never a per-key fill.
///
/// Falsify: drop the `hits.len() == keys.len()` all-hits shortcut in
/// `get_many_cached` so it always refetches. The all-fresh second call then
/// fills, and the count reaches 2 before the clock ever advances.
#[tokio::test]
async fn get_many_cached_shortcuts_when_all_fresh() -> Result<()> {
    let (cache, mock) = mock_clock_cache(1 << 20);
    let ttl = Duration::from_secs(5);
    let keys = [key("batch-0")?, key("batch-1")?];
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(smallvec![
                Some(Bytes::from_static(b"a")),
                Some(Bytes::from_static(b"b")),
            ])
        }
    };

    // A newer presence fill finishes after an older value fill.
    let presence_started = Notify::new();
    let value_done = Notify::new();
    let values = async {
        let result = cache
            .get_many_cached::<Values, _, _>(keys.iter().map(lookup), ttl, || async {
                mock.increment(Duration::from_nanos(1));
                presence_started.notified().await;
                fill().await
            })
            .await;
        value_done.notify_one();
        result
    };
    let presence =
        cache.get_many_cached::<Presence, _, _>(keys.iter().map(lookup), ttl, || async {
            presence_started.notify_one();
            value_done.notified().await;
            Ok(smallvec![Some(()), Some(())])
        });
    let (values, presence) = tokio::join!(biased; values, presence);
    values?;
    assert_eq!(presence?.as_slice(), [Some(()), Some(())]);
    assert_eq!(fills.load(Ordering::Relaxed), 1, "cold batch fills once");

    // Both entries remain fresh. The cache answers without a fill.
    let served = cache
        .get_many_cached::<Values, _, _>(keys.iter().map(lookup), ttl, fill)
        .await?;
    let expected: CellBuffer<Option<Bytes>> = smallvec![
        Some(Bytes::from_static(b"a")),
        Some(Bytes::from_static(b"b"))
    ];
    assert_eq!(served, expected);
    assert_eq!(
        fills.load(Ordering::Relaxed),
        1,
        "an all-fresh batch is served without a fill"
    );

    // Advance to the ttl: age == ttl is stale (the strict-`<` boundary), so
    // exactly one whole-batch refill fires (a single fill, not one per key).
    mock.increment(ttl);
    cache
        .get_many_cached::<Values, _, _>(keys.iter().map(lookup), ttl, fill)
        .await?;
    assert_eq!(
        fills.load(Ordering::Relaxed),
        2,
        "one stale key refetches the whole batch exactly once"
    );
    Ok(())
}

/// Declared weight never exceeds the byte budget across a fill trace.
#[tokio::test]
async fn declared_weight_bounded_by_budget() -> Result<()> {
    let budget = 4096u64;
    let (cache, _mock) = mock_clock_cache(budget);
    let value = Bytes::from(vec![0u8; 256]);
    for i in 0..200u32 {
        let k = key_at(
            StateType::Application,
            "weighted",
            1,
            i.to_be_bytes().to_vec(),
        )?;
        let value = value.clone();
        cache
            .get_cached::<Values, _, _>(lookup(&k), Duration::from_secs(1000), || {
                let value = value.clone();
                async move { Ok::<_, StateAccessError>(Some(value)) }
            })
            .await?;
        assert!(
            cache.weight() <= budget,
            "declared weight {} exceeded budget {budget}",
            cache.weight()
        );
    }
    Ok(())
}
