//! Cached entries expire no later than their durable rows.

use super::*;

/// One mutation in a co-expiry-anchor trace. The clock advances explicitly via
/// [`Advance`](TtlMut::Advance), so a stage→commit gap is part of the input
/// space rather than a single fixed scenario.
#[derive(Clone, Debug)]
enum TtlMut {
    /// `write_resolved` — re-stamps the row's TTL at the current clock.
    Set(u8, u8),
    /// `write_provisional` — stages `data`+`prev`, re-stamping at the clock.
    Stage(u8, u8),
    /// `commit_provisional` — promotes; the settle transform keeps the stage
    /// TTL.
    Commit(u8),
    /// `abort_provisional` — rolls back to `prev`, re-stamping at the clock.
    Abort(u8),
    /// Advances the test clock by the specified milliseconds.
    Advance(u16),
}

/// A random mutator trace with a per-trace collection TTL. Generated with a
/// clean per-key lifecycle (a key is idle or staged; `Commit`/`Abort` target
/// only staged keys, `Set`/`Stage` only idle ones), so any prefix is itself
/// valid — which is how `shrink` minimises.
#[derive(Clone, Debug)]
struct TtlMutTrace {
    ttl: Option<u32>,
    ops: Vec<TtlMut>,
}

/// The keys a co-expiry trace addresses.
const TTL_KEYS: u8 = 5;

impl Arbitrary for TtlMutTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let ttl = if bool::arbitrary(g) {
            None
        } else {
            Some(1 + u32::from(u8::arbitrary(g) % 8))
        };
        let len = usize::arbitrary(g) % 24;
        let mut staged: HashSet<u8> = HashSet::new();
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            // A quarter of ops advance the clock; the rest mutate one key.
            if u8::arbitrary(g) % 4 == 0 {
                // Arbitrary millisecond advances (0–~12 s) cover both sub-second
                // remainders and multi-second gaps relative to the 1–8 s TTL.
                ops.push(TtlMut::Advance(u16::arbitrary(g) % 12_000));
                continue;
            }
            let key = u8::arbitrary(g) % TTL_KEYS;
            let value = u8::arbitrary(g);
            if staged.remove(&key) {
                ops.push(if bool::arbitrary(g) {
                    TtlMut::Commit(key)
                } else {
                    TtlMut::Abort(key)
                });
            } else if bool::arbitrary(g) {
                ops.push(TtlMut::Set(key, value));
            } else {
                staged.insert(key);
                ops.push(TtlMut::Stage(key, value));
            }
        }
        Self { ttl, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ttl = self.ttl;
        let ops = self.ops.clone();
        // A prefix of a clean-lifecycle trace is itself clean, so truncation is a
        // safe shrink; also try dropping the TTL entirely.
        let prefixes = (0..ops.len()).map(move |n| Self {
            ttl,
            ops: ops[..n].to_vec(),
        });
        let drop_ttl = self
            .ttl
            .map(|_| Self {
                ttl: None,
                ops: self.ops.clone(),
            })
            .into_iter();
        Box::new(drop_ttl.chain(prefixes))
    }
}

/// **The co-expiry anchor (the generalising property).** For any TTL, any
/// interleaving of the four write paths, and any **sub-second** clock movement,
/// the expiry stamped on each cell's fjall entry must equal its durable row's
/// modeled death — `floor(write_clock) + ttl`, mirroring Cassandra's
/// whole-second TTL resolution: `Set`/`Stage`/`Abort` re-stamp at the floored
/// write clock, while `Commit` (the settle transform keeps the stage TTL) must
/// REUSE the stage-time stamp — never a fresh `commit + ttl`, which would let
/// the entry outlive the row. Because the clock advances by arbitrary
/// milliseconds, the model's floor discriminates the sub-second overhang across
/// the full 0–999 ms remainder. Asserted after **every** op (`0` = never).
#[test]
fn prop_cached_ttl_expiry_matches_durable_death() {
    fn property(trace: TtlMutTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            let now = Arc::new(AtomicU64::new(START));
            let cells = MemoryCells::new();
            let lower = MemoryCellStore::new(cells);
            let cached = Cached::new(
                test_db::cache_with_clock("ttl-anchor", Clock::Fixed(now.clone()))?,
                lower,
            );
            let id = collection("ttl-anchor")?;
            let cref = CollectionRef::new(id.clone(), trace.ttl.map(CompactDuration::new));
            let ttl_ms = trace.ttl.map(|s| u64::from(s) * 1_000);
            // The durable death stamped by a write at `clock` (`0` = never).
            // Cassandra anchors at whole-second resolution, so the fjall stamp
            // floors `clock` DOWN to the second before adding the TTL — this is
            // what discriminates the sub-second overhang.
            let death_at = |clock: u64| ttl_ms.map_or(0, |ttl| (clock - clock % 1_000) + ttl);

            // Model: the death stamped on each present key's fjall entry.
            let mut death: HashMap<u8, u64> = HashMap::new();
            let mut committed: HashMap<u8, Committed> = HashMap::new();
            let mut staged: HashMap<u8, u8> = HashMap::new();
            let mut clock = START;
            let event = probe(1);
            let prev_of = |committed: &HashMap<u8, Committed>, key: u8| {
                committed
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| Committed::new(None))
            };

            for (index, op) in trace.ops.iter().enumerate() {
                match *op {
                    TtlMut::Set(key, value) => {
                        cached
                            .write_resolved(&cref, &[(cell_at(key), Some(bytes(value)))], &[])
                            .await?;
                        committed.insert(key, Committed::new(Some(bytes(value))));
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Stage(key, value) => {
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        let writes = [(cell_at(key), write)];
                        let marker = ttl_marker(event, &writes);
                        cached
                            .write_provisional(&cref, listed(&marker, &writes)?)
                            .await?;
                        staged.insert(key, value);
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Commit(key) => {
                        let Some(value) = staged.remove(&key) else {
                            return Err(eyre!("op {index}: commit without a prior stage"));
                        };
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        let writes = [(cell_at(key), write)];
                        let marker = ttl_marker(event, &writes);
                        cached.commit_provisional(&cref, &marker, &writes).await?;
                        committed.insert(key, Committed::new(Some(bytes(value))));
                        // The settle transform reuses the stage stamp → death
                        // unchanged.
                    }
                    TtlMut::Abort(key) => {
                        let Some(value) = staged.remove(&key) else {
                            return Err(eyre!("op {index}: abort without a prior stage"));
                        };
                        let write = ProvisionalWrite::new(
                            Some(bytes(value)),
                            prev_of(&committed, key),
                            event,
                        );
                        cached
                            .abort_provisional(&cref, &[(cell_at(key), write)])
                            .await?;
                        death.insert(key, death_at(clock));
                    }
                    TtlMut::Advance(ms) => {
                        clock += u64::from(ms);
                        now.store(clock, Ordering::Relaxed);
                    }
                }

                // After every op: each touched cell's stamp equals its modeled
                // death; an untouched key has no entry.
                for key in 0..TTL_KEYS {
                    let got = cached.stored_expiry(&id, &cell_at(key)).await?;
                    let want = death.get(&key).copied();
                    if got != want {
                        return Err(eyre!(
                            "op {index} ({op:?}): key {key} fjall expiry {got:?} != modeled \
                             durable death {want:?}"
                        ));
                    }
                }
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(TtlMutTrace) -> Result<bool>);
}

fn ttl_marker(event: EventRef, writes: &[(CellKey, ProvisionalWrite)]) -> EventMarker {
    EventMarker::frozen(event, writes, Vec::new(), &evidence([].into(), None))
}
