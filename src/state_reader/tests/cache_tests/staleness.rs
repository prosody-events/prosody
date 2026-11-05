//! Generated schedules never serve a value older than the cache TTL.

use super::*;

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
                let coordinates: Vec<[u8; 1]> = indices.iter().map(|&i| [i]).collect();
                let Ok(batch) = pool_batch(&coordinates) else {
                    continue;
                };
                let filled = batch.map(|coordinate| fill_value(coordinate[0], present));
                if !all_fresh {
                    expected_fills += 1;
                    for (key, value) in indices.iter().zip(filled.iter()) {
                        model.insert(*key, (issued, value.clone()));
                    }
                }
                let expected: CellBuffer<_> =
                    indices.iter().map(|key| model[key].1.clone()).collect();
                let served = cache
                    .get_many_cached::<Values, _, _>(
                        &batch,
                        |coordinate| lookup(&keys[usize::from(coordinate[0])]),
                        CACHE_TTL,
                        || async {
                            fills.fetch_add(1, Ordering::Relaxed);
                            Ok(filled)
                        },
                    )
                    .await?;
                assert_eq!(*served, *expected);
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
