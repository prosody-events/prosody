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

/// Regression pin: `write_through`'s newer-wins compares `seq` (issue order)
/// alone, never the lexicographic `(issued_ms, seq)`. A later-issued fill that
/// read an *earlier* millisecond must still win — the two orders disagree only
/// when the clock does not advance monotonically between issues, so the test
/// drives it that way.
///
/// The point fill is issued at a HIGH millisecond (stamp `ms 1000, seq 0`); the
/// batch fill is issued later at a LOW millisecond (stamp `ms 1, seq 1`). A
/// second, absent decoy key forces the batch fill to fire so its write-through
/// runs against the occupied entry. Under seq-only the batch value (seq 1)
/// wins; a lexicographic compare would rank `(1, 1) < (1000, 0)` and leave
/// "v1".
///
/// Falsify: compare the full `Stamp` lexicographically in `write_through` — the
/// read-back returns "v1" and the assert goes red.
#[tokio::test]
async fn batch_refill_overwrites_stale_entry() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k1 = key("batch")?;
    let k2 = key("batch-decoy")?;

    // Point-fill k1 at a HIGH millisecond (issue stamp: ms 1000, seq 0); the ttl
    // is large so k1 stays a fresh hit — the test turns on newer-wins, not on
    // expiry.
    now.store(1000, Ordering::Relaxed);
    cache
        .get_cached(k1.clone(), 1_000_000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v1")))
        })
        .await?;

    // Batch-fill [k1, k2] issued later at an EARLIER millisecond (stamp: ms 1,
    // seq 1). k1 is still a fresh hit, but k2 is absent, so the fill fires and
    // write-throughs k1 with the later-seq / earlier-ms stamp.
    now.store(1, Ordering::Relaxed);
    let got = cache
        .get_many_cached(&[k1.clone(), k2], 1_000_000, || async {
            Ok::<_, StateAccessError>(vec![
                Some(Bytes::from_static(b"v2")),
                Some(Bytes::from_static(b"decoy")),
            ])
        })
        .await?;
    assert_eq!(got[0], Some(Bytes::from_static(b"v2")));

    // Read k1 back: seq-only newer-wins kept the batch value (seq 1 > 0) despite
    // its earlier millisecond; the fill closure here must not run (k1 is a hit).
    let back = cache
        .get_cached(k1, 1_000_000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"unused")))
        })
        .await?;
    assert_eq!(back, Some(Bytes::from_static(b"v2")), "later seq wins");
    Ok(())
}
