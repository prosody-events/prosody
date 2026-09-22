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

/// The distinct collection names the schedule's key pool spans. They differ
/// **only** by `StateName`: same source, partition key, and cell. A cache key
/// that dropped `StateName` would collapse them, aliasing one collection's
/// entry onto another's.
const CACHE_KEYS: [(StateType, &str, i32); 5] = [
    (StateType::Application, "cache-n0", 1),
    (StateType::Application, "cache-n0", 2),
    (StateType::Application, "cache-n1", 1),
    (StateType::Application, "cache-n2", 1),
    (StateType::Framework, "cache-n0", 1),
];

/// One coarse freshness window for the cache model.
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Clock advances spanning fresh, boundary, and expired observations.
const ADVANCE_POOL: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_secs(1),
    CACHE_TTL,
    Duration::from_secs(10),
];

/// Upper bound on random steps before the mixed batch sequence.
const MAX_CACHE_STEPS: usize = 24;

/// One step advances the clock or reads one key or a batch.
#[derive(Clone, Copy, Debug)]
enum CacheStep {
    /// Advance the clock by `ADVANCE_POOL[idx]`.
    Advance(u8),
    /// Get pooled key `key`; a fill returns `Some` when `present`, else the
    /// negative `None`.
    Get { key: u8, present: bool },
    /// Read three positions. One miss refills every position.
    Batch { keys: [u8; 3], present: bool },
}

impl Arbitrary for CacheStep {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = |g: &mut Gen| u8::arbitrary(g) % CACHE_KEYS.len() as u8;
        match u8::arbitrary(g) % 3 {
            0 => Self::Advance(u8::arbitrary(g) % ADVANCE_POOL.len() as u8),
            1 => Self::Get {
                key: key(g),
                present: bool::arbitrary(g),
            },
            _ => Self::Batch {
                keys: [key(g), key(g), key(g)],
                present: bool::arbitrary(g),
            },
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
        let mut steps: Vec<_> = Vec::<CacheStep>::arbitrary(g)
            .into_iter()
            .take(MAX_CACHE_STEPS)
            .collect();
        let first = u8::arbitrary(g) % CACHE_KEYS.len() as u8;
        let second = (first + 1) % CACHE_KEYS.len() as u8;
        let present = bool::arbitrary(g);
        // A mixed batch must refresh its fresh position after it evicts a stale
        // position.
        steps.extend([
            CacheStep::Advance(2),
            CacheStep::Get {
                key: second,
                present,
            },
            CacheStep::Advance(2),
            CacheStep::Get {
                key: first,
                present: !present,
            },
            CacheStep::Batch {
                keys: [first, second, first],
                present,
            },
            CacheStep::Get {
                key: first,
                present,
            },
        ]);
        Self { steps }
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
/// issue-time age, expiry, batch refresh, negative-entry refresh, source
/// topology, namespace, and name isolation.
/// A batch miss updates every modeled position at one issue time.
///
/// A plain `HashMap<key, (issued, value)>` model predicts, for every get,
/// both the served value and whether a fill fires. A get is a hit served
/// from the model's entry when `age < ttl`; otherwise it is a miss that
/// refills. Asserting the served value and the running fill count after
/// every step catches a stale hit as a wrong count, an alias as wrong bytes,
/// and a laundered issue time as a wrong count.
///
/// Falsify: change `ReaderCache::fresh`'s `<` to `<=`. An `age == ttl` step
/// then serves a stale hit, so the fill count trails the model. Or drop
/// `PartitionCount`, `StateName`, or `StateType` from `CacheKey`: a later
/// distinct source or collection then hits the first entry, serving the wrong
/// bytes with no fill.
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
        .map(|(state_type, name, count)| key_at(*state_type, name, *count, vec![0]))
        .collect::<Result<_>>()?;
    let fills = Arc::new(AtomicUsize::new(0));
    // key idx -> (issue instant, cached value).
    let mut model: HashMap<u8, (Instant, Option<Bytes>)> = HashMap::new();
    let mut expected_fills = 0usize;

    for step in schedule.steps {
        match step {
            CacheStep::Advance(idx) => mock.increment(ADVANCE_POOL[idx as usize]),
            CacheStep::Batch {
                keys: indices,
                present,
            } => {
                mock.increment(Duration::from_nanos(1));
                let issued = clock.now();
                let all_fresh = indices.iter().all(|key| {
                    model
                        .get(key)
                        .is_some_and(|(time, _)| issued.duration_since(*time) < CACHE_TTL)
                });
                let batch: CellBuffer<_> =
                    indices.iter().map(|i| keys[*i as usize].clone()).collect();
                let filled: CellBuffer<_> =
                    indices.iter().map(|i| fill_value(*i, present)).collect();
                if !all_fresh {
                    expected_fills += 1;
                    for (key, value) in indices.iter().zip(&filled) {
                        model.insert(*key, (issued, value.clone()));
                    }
                }
                let expected: CellBuffer<_> =
                    indices.iter().map(|key| model[key].1.clone()).collect();
                let served = cache
                    .get_many_cached::<Values, _, _>(
                        batch.iter().map(lookup),
                        CACHE_TTL,
                        || async {
                            fills.fetch_add(1, Ordering::Relaxed);
                            Ok(filled)
                        },
                    )
                    .await?;
                assert_eq!(served, expected);
                assert_eq!(fills.load(Ordering::Relaxed), expected_fills);
            }
            CacheStep::Get { key, present } => {
                let cur = clock.now();
                let filled = fill_value(key, present);

                let hit = model
                    .get(&key)
                    .is_some_and(|(issued, _)| cur.duration_since(*issued) < CACHE_TTL);
                let expected_value = if hit {
                    model[&key].1.clone()
                } else {
                    expected_fills += 1;
                    model.insert(key, (cur, filled.clone()));
                    filled.clone()
                };

                let counter = fills.clone();
                let served = cache
                    .get_cached::<Values, _, _>(lookup(&keys[key as usize]), CACHE_TTL, move || {
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
