//! Invariants of the read-through TTL cache.
//!
//! [`prop_cache_staleness`] proves the staleness rules together over random
//! clock and get schedules, checked against a plain `HashMap` model: the
//! stamp-at-issue age, the `age == ttl` miss boundary, negative caching,
//! `StateName` no-aliasing, and per-ttl freshness windows over a shared
//! entry. The focused tests below pin invariants that schedule cannot
//! express: concurrent single-flight, a fill that mutates the clock while it
//! runs, the newer-wins comparison on a batch write-through, and the
//! byte-budget bound.
//!
//! Every test drives an injected millisecond clock instead of sleeping, so
//! timing stays deterministic. The cache is exercised directly, with no
//! stores underneath, so each invariant is isolated. Each fill closure is
//! written inline because the cache's `Fn() -> impl Future` bound needs a
//! concrete future, not a boxed `dyn`.

use super::support::{fixed_clock_cache, topic};
use crate::Key;
use crate::state::StateName;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::store::CellBuffer;
use crate::state_reader::cache::CacheKey;
use crate::state_reader::source::SourceId;
use bytes::Bytes;
use color_eyre::eyre::Result;
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use smallvec::smallvec;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A cache key for the given collection name at cell coordinate `coord`.
fn key_at(name: &str, coord: Vec<u8>) -> Result<CacheKey> {
    Ok((
        SourceId {
            group_id: Arc::from("group-aaa"),
            topic: topic("t"),
        },
        StateName::try_new(name)?,
        Key::from("user-1"),
        CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(coord),
        },
    ))
}

/// A cache key for the given collection name and one fixed cell.
fn key(name: &str) -> Result<CacheKey> {
    key_at(name, vec![0])
}

// --- Staleness property -----------------------------------------------------

/// The distinct collection names the schedule's key pool spans. They differ
/// **only** by `StateName`: same source, partition key, and cell. A cache key
/// that dropped `StateName` would collapse them, aliasing one collection's
/// entry onto another's.
const CACHE_NAMES: [&str; 3] = ["cache-n0", "cache-n1", "cache-n2"];

/// The per-get TTL pool the schedule draws from: the degenerate `0` (every
/// read is born stale), the minimum meaningful `1`, mid-range values, and one
/// large enough to never expire within a bounded schedule. Reusing one key
/// across different TTLs from this pool is how the property test exercises a
/// freshness window that depends on the read, not just the key.
const TTL_POOL: [u64; 5] = [0, 1, 10, 100, 1_000_000];

/// The clock-advance pool (milliseconds). Sharing magnitudes with [`TTL_POOL`]
/// makes the `age == ttl` boundary (the strict-`<` miss) recur.
const ADVANCE_POOL: [u64; 5] = [0, 1, 10, 100, 1000];

/// Upper bound on schedule length.
const MAX_CACHE_STEPS: usize = 24;

/// One step: advance the injected clock, or issue a cached get for a pooled key
/// at a pooled TTL, filling `Some`/`None`.
#[derive(Clone, Copy, Debug)]
enum CacheStep {
    /// Advance the clock by `ADVANCE_POOL[idx]` ms.
    Advance(u8),
    /// Get pooled key `key` at `TTL_POOL[ttl]`; a fill returns `Some` when
    /// `present`, else the negative `None`.
    Get { key: u8, ttl: u8, present: bool },
}

impl Arbitrary for CacheStep {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Advance(u8::arbitrary(g) % ADVANCE_POOL.len() as u8)
        } else {
            Self::Get {
                key: u8::arbitrary(g) % CACHE_NAMES.len() as u8,
                ttl: u8::arbitrary(g) % TTL_POOL.len() as u8,
                present: bool::arbitrary(g),
            }
        }
    }
}

/// A shrinkable schedule of cache steps.
#[derive(Clone, Debug)]
struct CacheSchedule {
    steps: Vec<CacheStep>,
}

impl Arbitrary for CacheSchedule {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: Vec::<CacheStep>::arbitrary(g)
                .into_iter()
                .take(MAX_CACHE_STEPS)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.steps.shrink().map(|steps| Self { steps }))
    }
}

/// The deterministic fill value for a pooled key. `Some` carries a one-byte
/// value derived from `key`, distinct per key so an alias would serve the
/// wrong bytes. `present = false` yields the negative `None`.
fn fill_value(key: u8, present: bool) -> Option<Bytes> {
    present.then(|| Bytes::from(vec![key]))
}

/// One property proves five staleness rules at once: stamp-at-issue age, the
/// `age == ttl` miss boundary, negative-entry refresh, `StateName`
/// no-aliasing, and per-ttl freshness windows over a shared entry.
///
/// A plain `HashMap<key, (issued_ms, value)>` model predicts, for every get,
/// both the served value and whether a fill fires. A get is a hit served
/// from the model's entry when `age < ttl`; otherwise it is a miss that
/// refills. Asserting the served value and the running fill count after
/// every step catches a stale hit as a wrong count, an alias as wrong bytes,
/// and a laundered stamp as a wrong count.
///
/// Falsify: change `ReaderCache::fresh`'s `<` to `<=`. An `age == ttl` step
/// then serves a stale hit, so the fill count trails the model. Or drop
/// `StateName` from `CacheKey`: a later get of a different name then hits
/// the first name's entry, serving the wrong bytes with no fill.
#[test]
fn prop_cache_staleness() {
    fn property(schedule: CacheSchedule) -> Result<bool> {
        block_on(run_cache_schedule(schedule))
    }
    QuickCheck::new().quickcheck(property as fn(CacheSchedule) -> Result<bool>);
}

async fn run_cache_schedule(schedule: CacheSchedule) -> Result<bool> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let keys: Vec<CacheKey> = CACHE_NAMES
        .iter()
        .map(|name| key(name))
        .collect::<Result<_>>()?;
    let fills = Arc::new(AtomicUsize::new(0));
    // key idx -> (issue stamp ms, cached value).
    let mut model: HashMap<u8, (u64, Option<Bytes>)> = HashMap::new();
    let mut expected_fills = 0usize;

    for step in schedule.steps {
        match step {
            CacheStep::Advance(idx) => {
                let delta = ADVANCE_POOL[idx as usize];
                now.store(now.load(Ordering::Relaxed) + delta, Ordering::Relaxed);
            }
            CacheStep::Get { key, ttl, present } => {
                let ttl_ms = TTL_POOL[ttl as usize];
                let cur = now.load(Ordering::Relaxed);
                let filled = fill_value(key, present);

                let hit = model
                    .get(&key)
                    .is_some_and(|(issued, _)| cur.saturating_sub(*issued) < ttl_ms);
                let expected_value = if hit {
                    model[&key].1.clone()
                } else {
                    expected_fills += 1;
                    model.insert(key, (cur, filled.clone()));
                    filled.clone()
                };

                let counter = fills.clone();
                let served = cache
                    .get_cached(keys[key as usize].clone(), ttl_ms, move || {
                        let counter = counter.clone();
                        let filled = filled.clone();
                        async move {
                            counter.fetch_add(1, Ordering::Relaxed);
                            Ok::<_, StateAccessError>(filled)
                        }
                    })
                    .await?;

                if served != expected_value {
                    return Ok(false);
                }
                if fills.load(Ordering::Relaxed) != expected_fills {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

// --- Focused survivors (invariants the serial schedule cannot express) ------

/// Stamp-at-issue: a slow fill enters already-aged, so it cannot launder an
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

/// Two concurrent cold gets of one key issue exactly ONE store fill
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

/// Regression pin: `write_through`'s newer-wins compares `seq` (issue order)
/// alone, never the lexicographic `(issued_ms, seq)`. A later-issued fill that
/// read an *earlier* millisecond must still win. The two orders disagree only
/// when the clock does not advance monotonically between issues, so the test
/// drives the clock that way.
///
/// The point fill is issued at a HIGH millisecond, stamp `ms 1000, seq 0`.
/// The batch fill is issued later at a LOW millisecond, stamp `ms 1, seq 1`.
/// A second, absent decoy key forces the batch fill to fire, so its
/// write-through runs against the occupied entry. Under seq-only ordering the
/// batch value, seq 1, wins. A lexicographic compare would instead rank
/// `(1, 1) < (1000, 0)` and leave "v1".
///
/// Falsify: compare the full `Stamp` lexicographically in `write_through`.
/// The read-back then returns "v1" and the assert goes red.
#[tokio::test]
async fn batch_refill_overwrites_stale_entry() -> Result<()> {
    let (cache, now) = fixed_clock_cache(1 << 20);
    let k1 = key("batch")?;
    let k2 = key("batch-decoy")?;

    // Point-fill k1 at a HIGH millisecond: issue stamp ms 1000, seq 0. The ttl
    // is large so k1 stays a fresh hit. The test turns on newer-wins, not on
    // expiry.
    now.store(1000, Ordering::Relaxed);
    cache
        .get_cached(k1.clone(), 1_000_000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v1")))
        })
        .await?;

    // Batch-fill [k1, k2] issued later at an EARLIER millisecond (stamp: ms 1,
    // seq 1). k1 is still a fresh hit, but k2 is absent, so the fill fires and
    // writes k1 through with the later seq and the earlier millisecond.
    now.store(1, Ordering::Relaxed);
    let got = cache
        .get_many_cached(&[k1.clone(), k2], 1_000_000, || async {
            Ok::<_, StateAccessError>(smallvec![
                Some(Bytes::from_static(b"v2")),
                Some(Bytes::from_static(b"decoy")),
            ])
        })
        .await?;
    assert_eq!(got[0], Some(Bytes::from_static(b"v2")));

    // Read k1 back. Seq-only newer-wins kept the batch value (seq 1 > 0)
    // despite its earlier millisecond. The fill closure here must not run:
    // k1 is a hit.
    let back = cache
        .get_cached(k1, 1_000_000, || async {
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"unused")))
        })
        .await?;
    assert_eq!(back, Some(Bytes::from_static(b"v2")), "later seq wins");
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
    let (cache, now) = fixed_clock_cache(1 << 20);
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

    // Cold: one batch fill seeds both keys at t=0.
    cache.get_many_cached(&keys, 100, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1, "cold batch fills once");

    // Both still fresh at t=0: the all-hits shortcut serves from cache.
    let served = cache.get_many_cached(&keys, 100, fill).await?;
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

    // Advance past the ttl: the first probed key is stale, so exactly one
    // whole-batch refill fires (a single fill, not one per key).
    now.store(100, Ordering::Relaxed);
    cache.get_many_cached(&keys, 100, fill).await?;
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
    let (cache, _now) = fixed_clock_cache(budget);
    let value = Bytes::from(vec![0u8; 256]);
    for i in 0..200u32 {
        let k = key_at("weighted", i.to_be_bytes().to_vec())?;
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
