//! Invariants of the read-through TTL cache.
//!
//! [`prop_cache_staleness`] proves the staleness rules together over random
//! clock and get schedules, checked against a plain `HashMap` model: the
//! stamp-at-issue age, the `age == ttl` miss boundary, negative caching,
//! `StateName` no-aliasing, and per-ttl freshness windows over a shared
//! entry. The focused tests below pin invariants that schedule cannot
//! express: concurrent single-flight, a fill that advances the clock while it
//! runs, the newer-wins comparison in `write_through`, and the byte-budget
//! bound. Its key pool includes two namespaces with the same collection name,
//! proving `StateType` participates in cache identity.
//!
//! Every test drives a mocked monotonic clock instead of sleeping, so timing
//! stays deterministic. The cache is exercised directly, with no
//! stores underneath, so each invariant is isolated. Each fill closure is
//! written inline because the cache's `Fn() -> impl Future` bound needs a
//! concrete future, not a boxed `dyn`.

use super::support::{mock_clock_cache, topic};
use crate::Key;
use crate::state::access::StateAccessError;
use crate::state::cell_key::{CellKey, Coordinate, Section};
use crate::state::store::CellBuffer;
use crate::state::{StateName, StateType};
use crate::state_reader::cache::CacheKey;
use crate::state_reader::source::SourceId;
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

/// A cache key for the given collection name at cell coordinate `coord`.
fn key_at(state_type: StateType, name: &str, coord: Vec<u8>) -> Result<CacheKey> {
    Ok((
        SourceId {
            group_id: Arc::from("group-aaa"),
            topic: topic("t"),
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
fn key(name: &str) -> Result<CacheKey> {
    key_at(StateType::Application, name, vec![0])
}

// --- Staleness property -----------------------------------------------------

/// The distinct collection names the schedule's key pool spans. They differ
/// **only** by `StateName`: same source, partition key, and cell. A cache key
/// that dropped `StateName` would collapse them, aliasing one collection's
/// entry onto another's.
const CACHE_KEYS: [(StateType, &str); 4] = [
    (StateType::Application, "cache-n0"),
    (StateType::Application, "cache-n1"),
    (StateType::Application, "cache-n2"),
    (StateType::Framework, "cache-n0"),
];

/// The per-get TTL pool the schedule draws from: the degenerate zero (every
/// read is born stale), a sub-millisecond TTL the old millisecond clock could
/// not express, mid-range values, and one large enough to never expire within a
/// bounded schedule. Reusing one key across different TTLs from this pool is
/// how the property test exercises a freshness window that depends on the read,
/// not just the key.
const TTL_POOL: [Duration; 5] = [
    Duration::ZERO,
    Duration::from_micros(500),
    Duration::from_millis(10),
    Duration::from_millis(100),
    Duration::from_secs(1000),
];

/// The clock-advance pool. Sharing magnitudes with [`TTL_POOL`] makes the
/// `age == ttl` boundary (the strict-`<` miss) recur.
const ADVANCE_POOL: [Duration; 5] = [
    Duration::ZERO,
    Duration::from_micros(500),
    Duration::from_millis(10),
    Duration::from_millis(100),
    Duration::from_secs(1),
];

/// Upper bound on schedule length.
const MAX_CACHE_STEPS: usize = 24;

/// One step: advance the injected clock, or issue a cached get for a pooled key
/// at a pooled TTL, filling `Some`/`None`.
#[derive(Clone, Copy, Debug)]
enum CacheStep {
    /// Advance the clock by `ADVANCE_POOL[idx]`.
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
                key: u8::arbitrary(g) % CACHE_KEYS.len() as u8,
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

/// One property proves the staleness rules and cache-key isolation together:
/// stamp-at-issue age, the `age == ttl` miss boundary, negative-entry refresh,
/// namespace and name isolation, and per-ttl freshness windows.
///
/// A plain `HashMap<key, (issued, value)>` model predicts, for every get,
/// both the served value and whether a fill fires. A get is a hit served
/// from the model's entry when `age < ttl`; otherwise it is a miss that
/// refills. Asserting the served value and the running fill count after
/// every step catches a stale hit as a wrong count, an alias as wrong bytes,
/// and a laundered stamp as a wrong count.
///
/// Falsify: change `ReaderCache::fresh`'s `<` to `<=`. An `age == ttl` step
/// then serves a stale hit, so the fill count trails the model. Or drop
/// `StateName` or `StateType` from `CacheKey`: a later distinct collection
/// then hits the first entry, serving the wrong bytes with no fill.
#[test]
fn prop_cache_staleness() {
    fn property(schedule: CacheSchedule) -> Result<bool> {
        block_on(run_cache_schedule(schedule))
    }
    QuickCheck::new().quickcheck(property as fn(CacheSchedule) -> Result<bool>);
}

async fn run_cache_schedule(schedule: CacheSchedule) -> Result<bool> {
    let (cache, mock) = mock_clock_cache(1 << 20);
    let clock = cache.clock();
    let keys: Vec<CacheKey> = CACHE_KEYS
        .iter()
        .map(|(state_type, name)| key_at(*state_type, name, vec![0]))
        .collect::<Result<_>>()?;
    let fills = Arc::new(AtomicUsize::new(0));
    // key idx -> (issue instant, cached value).
    let mut model: HashMap<u8, (Instant, Option<Bytes>)> = HashMap::new();
    let mut expected_fills = 0usize;

    for step in schedule.steps {
        match step {
            CacheStep::Advance(idx) => mock.increment(ADVANCE_POOL[idx as usize]),
            CacheStep::Get { key, ttl, present } => {
                let ttl = TTL_POOL[ttl as usize];
                let cur = clock.now();
                let filled = fill_value(key, present);

                let hit = model
                    .get(&key)
                    .is_some_and(|(issued, _)| cur.duration_since(*issued) < ttl);
                let expected_value = if hit {
                    model[&key].1.clone()
                } else {
                    expected_fills += 1;
                    model.insert(key, (cur, filled.clone()));
                    filled.clone()
                };

                let counter = fills.clone();
                let served = cache
                    .get_cached(keys[key as usize].clone(), ttl, move || {
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
    let (cache, mock) = mock_clock_cache(1 << 20);
    let k = key("slow")?;
    let fills = Arc::new(AtomicUsize::new(0));
    let ttl = Duration::from_millis(100);
    // The fill advances the clock past the ttl before returning — a "slow"
    // store read. The stamp was taken at issue (t=0), not here.
    let fill = || {
        let fills = fills.clone();
        let mock = mock.clone();
        async move {
            fills.fetch_add(1, Ordering::Relaxed);
            mock.increment(Duration::from_millis(200));
            Ok::<_, StateAccessError>(Some(Bytes::from_static(b"v")))
        }
    };

    // Issued at t=0, completes at t=200ms; the fill serves its own result.
    let got = cache.get_cached(k.clone(), ttl, fill).await?;
    assert_eq!(got, Some(Bytes::from_static(b"v")));
    assert_eq!(fills.load(Ordering::Relaxed), 1);

    // A later reader at t=200ms: age 200ms >= ttl → miss → refill. The refill
    // advances the clock again, which only ages it further.
    cache.get_cached(k.clone(), ttl, fill).await?;
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
        cache.get_cached(k.clone(), ttl, fill),
        cache.get_cached(k.clone(), ttl, fill),
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
/// alone, never the lexicographic `(issued, seq)`. A later-issued fill that
/// read an *earlier* instant must still win.
///
/// The two stamps are built through `write_through_at` because no schedule can
/// produce them: in production the pair arises when two fills interleave
/// between the clock read and the `seq` increment, and a monotonic clock never
/// hands out a falling instant in issue order. The first write is issued at the
/// LATE instant and so takes `seq` 0; the second at the EARLY instant and takes
/// `seq` 1. Seq-only ordering keeps the second value. A lexicographic compare
/// would rank `(early, 1) < (late, 0)` and keep the first.
///
/// Falsify: compare the full `Stamp` lexicographically in `write_through`.
/// The read-back then returns "v1" and the assert goes red.
#[tokio::test]
async fn later_issued_fill_wins_despite_earlier_instant() -> Result<()> {
    let (cache, mock) = mock_clock_cache(1 << 20);
    let clock = cache.clock();
    let k = key("newer-wins")?;

    let early = clock.now();
    mock.increment(Duration::from_secs(1));
    let late = clock.now();

    cache
        .write_through_at(&k, late, Some(Bytes::from_static(b"v1")))
        .await;
    cache
        .write_through_at(&k, early, Some(Bytes::from_static(b"v2")))
        .await;

    // A ttl far above the one-second spread, so the read-back turns on newer-wins
    // and not on expiry. The fill must not run: the entry is a fresh hit.
    let back = cache
        .get_cached(k, Duration::from_secs(1000), || async {
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
    let (cache, mock) = mock_clock_cache(1 << 20);
    let ttl = Duration::from_millis(100);
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
    cache.get_many_cached(&keys, ttl, fill).await?;
    assert_eq!(fills.load(Ordering::Relaxed), 1, "cold batch fills once");

    // Both still fresh at t=0: the all-hits shortcut serves from cache.
    let served = cache.get_many_cached(&keys, ttl, fill).await?;
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
    cache.get_many_cached(&keys, ttl, fill).await?;
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
        let k = key_at(StateType::Application, "weighted", i.to_be_bytes().to_vec())?;
        let value = value.clone();
        cache
            .get_cached(k, Duration::from_secs(1000), || {
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
