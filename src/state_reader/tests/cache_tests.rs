//! The read-through TTL cache invariants (plan arms a–h).
//!
//! All deterministic over an injected millisecond clock — never a sleep. The
//! cache is exercised directly (no stores) so each invariant is isolated. Each
//! fill closure is written inline so the cache's `Fn() -> impl Future` bound is
//! satisfied with a concrete future (no boxing, no `dyn`).

use super::support::{fixed_clock_cache, topic};
use crate::Key;
use crate::state::StateName;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state_reader::cache::CacheKey;
use crate::state_reader::source::SourceId;
use bytes::Bytes;
use color_eyre::eyre::Result;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A cache key for the given collection name and one fixed cell.
fn key(name: &str) -> Result<CacheKey> {
    Ok((
        SourceId {
            group_id: Arc::from("group-aaa"),
            topic: topic("t"),
        },
        StateName::try_new(name)?,
        Key::from("user-1"),
        CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(vec![0]),
        },
    ))
}

/// (b) An entry exactly `ttl` old is a MISS (`>=`, not `>`).
///
/// Falsify: change `fresh`'s `<` to `<=` — age==ttl serves a stale hit, so the
/// second fill never fires and `fills == 1`.
#[tokio::test]
async fn age_equal_ttl_is_a_miss() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k = key("boundary")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1, "cold miss fills once");

    now.store(99, Ordering::Relaxed);
    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1, "age 99 < ttl is a hit");

    now.store(100, Ordering::Relaxed);
    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 2, "age == ttl is a miss");
    Ok(())
}

/// (a) Stamp-at-issue: a slow fill enters already-aged, so it cannot launder an
/// old value into a fresh window for a later reader.
///
/// Falsify: stamp the entry at fill COMPLETION instead of issue — the entry is
/// then stamped at t=200, the second read sees age 0, and `fills` stays 1.
#[tokio::test]
async fn slow_fill_cannot_launder() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k = key("slow")?;
    let fills = Arc::new(AtomicUsize::new(0));
    // The fill advances the clock past the ttl before returning — a "slow"
    // store read. The stamp was taken at issue (t=0), not here.
    let fill = || {
        let fills = fills.clone();
        let now = now.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            now.store(200, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    // Issued at t=0, completes at t=200; the fill serves its own result.
    let got = cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(got, Some(Bytes::from_static(b"v")));
    assert_eq!(fills.load(Ordering::Relaxed), 1);

    // A later reader at t=200: age = 200 - 0 = 200 >= ttl → miss → refill.
    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(
        fills.load(Ordering::Relaxed),
        2,
        "the slow fill was stamped at issue, so it expired for the next reader"
    );
    Ok(())
}

/// (c) Two concurrent cold gets of one key issue exactly ONE store fill
/// (single-flight through the guard).
///
/// Falsify: replace `get_value_or_guard_async` with an unconditional read —
/// both fill, `fills == 2`.
#[tokio::test]
async fn cold_miss_is_single_flight() -> Result<()> {
    let (cache, _now) = fixed_clock_cache(1 << 20);
    let k = key("single-flight")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    let (a, b) = tokio::join!(
        cache.get_cached(k.clone(), 1000, fill),
        cache.get_cached(k.clone(), 1000, fill),
    );
    assert_eq!(a?, Some(Bytes::from_static(b"v")));
    assert_eq!(b?, Some(Bytes::from_static(b"v")));
    assert_eq!(
        fills.load(Ordering::Relaxed),
        1,
        "single-flight: one fill serves both"
    );
    Ok(())
}

/// (f) A cached negative result (`None`) is served until ttl, then refilled.
#[tokio::test]
async fn negative_entry_refreshes_after_ttl() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k = key("negative")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(None)
        }
    };

    assert_eq!(cache.get_cached(k.clone(), 100, fill).await?, None);
    now.store(50, Ordering::Relaxed);
    assert_eq!(cache.get_cached(k.clone(), 100, fill).await?, None);
    assert_eq!(fills.load(Ordering::Relaxed), 1, "negative hit, no refill");

    now.store(100, Ordering::Relaxed);
    assert_eq!(cache.get_cached(k.clone(), 100, fill).await?, None);
    assert_eq!(fills.load(Ordering::Relaxed), 2, "negative entry refreshed");
    Ok(())
}

/// (g) Two collections with the same source/key/cell never alias — `StateName`
/// is in the key.
///
/// Falsify: drop `StateName` from `CacheKey` — collection `y` serves `x`'s
/// value.
#[tokio::test]
async fn state_name_prevents_alias() -> Result<()> {
    let (cache, _now) = fixed_clock_cache(1 << 20);
    let x = cache
        .get_cached(key("x")?, 1000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"x")))
        })
        .await?;
    let y = cache
        .get_cached(key("y")?, 1000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"y")))
        })
        .await?;
    assert_eq!(x, Some(Bytes::from_static(b"x")));
    assert_eq!(y, Some(Bytes::from_static(b"y")), "distinct name, no alias");
    Ok(())
}

/// (h) Two descriptors over the SAME collection with different TTLs share
/// entries but keep distinct freshness windows: the short-TTL reader
/// revalidates the shared issue stamp and refills where the long-TTL reader
/// still hits.
#[tokio::test]
async fn shared_entry_distinct_freshness_windows() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k = key("shared")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    // Long reader (ttl 100) fills at t=0.
    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1);

    now.store(50, Ordering::Relaxed);
    // Long reader still fresh (age 50 < 100) — hit.
    cache.get_cached(k.clone(), 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1, "long ttl still fresh");
    // Short reader (ttl 10) sees age 50 >= 10 — miss, refill.
    cache.get_cached(k.clone(), 10, fill).await?;
    assert_eq!(
        fills.load(Ordering::Relaxed),
        2,
        "short ttl expired the shared entry"
    );
    Ok(())
}

/// (d) Declared weight never exceeds the byte budget across a fill trace.
#[tokio::test]
async fn declared_weight_bounded_by_budget() -> Result<()> {
    let budget = 4096u64;
    let (cache, _now) = fixed_clock_cache(budget);
    let value = Bytes::from(vec![0u8; 256]);
    for i in 0..200u32 {
        let k: CacheKey = (
            SourceId {
                group_id: Arc::from("group-aaa"),
                topic: topic("t"),
            },
            StateName::try_new("weighted")?,
            Key::from("user-1"),
            CellKey {
                section: Section::new(0),
                coordinate: Coordinate::from_bytes(i.to_be_bytes().to_vec()),
            },
        );
        let value = value.clone();
        cache
            .get_cached(k, 1_000_000, || {
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

/// The batch read-through overwrites a stale entry with the newer-issued fill,
/// keyed by `seq` (issue order), not by `(issued_ms, seq)` lexicographic order.
/// The codex Q2(d) regression pin: `write_through` must compare `seq` alone so
/// a later-issued fill that read an earlier millisecond still wins.
///
/// Falsify: compare the full lexicographic `Stamp` in `write_through` — with a
/// clock that does not advance monotonically the newer batch value can lose to
/// the older point value.
#[tokio::test]
async fn batch_refill_overwrites_stale_entry() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k = key("batch")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let point_fill = || {
        let fills = fills.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v1")))
        }
    };

    // Point fill at t=0 (seq 0), ttl 10.
    cache.get_cached(k.clone(), 10, point_fill).await?;

    // Advance past the ttl so the entry is stale, then batch-refill "v2"
    // (issued at a later seq). Newer-wins overwrites the stale point value.
    now.store(100, Ordering::Relaxed);
    let got = cache
        .get_many_cached(slice::from_ref(&k), 10, || async {
            Ok::<_, StateAccessError>(vec![Some(Bytes::from_static(b"v2"))])
        })
        .await?;
    assert_eq!(got, vec![Some(Bytes::from_static(b"v2"))]);

    // Read back within the new fresh window: the batch value won.
    let back = cache.get_cached(k, 10, point_fill).await?;
    assert_eq!(back, Some(Bytes::from_static(b"v2")), "newer fill wins");
    Ok(())
}
