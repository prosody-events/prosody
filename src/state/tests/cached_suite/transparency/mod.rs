//! The transparency property: a cached store answers as the uncached store.

use super::*;

mod replay;
mod trace;
use replay::{Replay, WarmModel};
use trace::{CacheOp, CacheTrace, POOL};

/// Failed point and batch probes preserve every warm value frame.
async fn check_failed_probes(replay: &Replay, fjall: &FjallCellCache) -> Result<()> {
    let cells: Vec<_> = (0..POOL)
        .map(|key| (cell_at(key), Some(bytes(key))))
        .collect();
    replay
        .subject
        .write_resolved(&replay.cref, &cells, &[])
        .await?;
    let batch = batch_of((0..POOL).rev().chain([0]))?;
    for fail_puts in [false, true] {
        fjall
            .faults()
            .fail_puts()
            .store(fail_puts, Ordering::Relaxed);
        fjall.faults().fail_reads().store(true, Ordering::Relaxed);
        replay.counting.reset();
        let presence =
            CellRead::<Presence>::read_many(&replay.subject, &replay.id, SECTION, &batch.as_ref())
                .await?;
        assert!(presence.iter().all(|(value, _)| value.get().is_some()));
        assert_eq!(replay.counting.presence_reads(), 1);
        fjall.faults().fail_reads().store(false, Ordering::Relaxed);
        replay.counting.reset();
        let values =
            CellRead::<Values>::read_many(&replay.subject, &replay.id, SECTION, &batch.as_ref())
                .await?;
        assert!(values.iter().all(|(value, _)| value.get().is_some()));
        assert_eq!(
            replay.counting.batch_cache_reads(),
            0,
            "failed batch probes preserve values"
        );
        assert_eq!(replay.counting.lower_reads(), 0);

        for key in 0..POOL {
            fjall.faults().fail_reads().store(true, Ordering::Relaxed);
            replay.counting.reset();
            assert!(
                CellRead::<Presence>::read(&replay.subject, &replay.id, cell_at(key).as_ref())
                    .await?
                    .0
                    .get()
                    .is_some()
            );
            assert_eq!(replay.counting.presence_reads(), 1);
            fjall.faults().fail_reads().store(false, Ordering::Relaxed);
            replay.counting.reset();
            assert_eq!(
                CellRead::<Values>::read(&replay.subject, &replay.id, cell_at(key).as_ref())
                    .await?
                    .0
                    .get(),
                Some(&bytes(key))
            );
            assert_eq!(
                replay.counting.lower_reads(),
                0,
                "failed point probes preserve values"
            );
        }
    }
    Ok(())
}

/// **The transparency property** — the whole contract of a transparent cache
/// in one differential: one generated cell-op trace (writes, provisional
/// stage/commit/abort, raw promotes, clears, gets, scans, clock movement,
/// TTL'd and not) through `Cached` over a memory store AND through a bare
/// memory twin with equivalent commit evidence; after **every** op, every pool
/// cell's value, batch presence, and section scan answer identically. Bounded
/// fjall fault injection (`fail_puts`, an in-budget `fail_deletes` countdown)
/// runs degraded-cache paths inside the property, not beside it.
///
/// The second arm is the **KV5 budget**: after the trace (fault seams healed
/// and one model-updating verification pass run), re-getting every pool cell
/// issues zero lower reads for every cell the warm-set model holds — the
/// exceptions (expired, cleared, fault-path, marker-resolution evictions) are
/// excluded by construction as the model's removals.
#[test]
pub(super) fn prop_cached_is_transparent() {
    fn property(trace: CacheTrace) -> Result<bool> {
        TEST_RUNTIME.block_on(async move {
            const START: u64 = 1_000;
            /// Far beyond any bounded trace's clock: fills never expire.
            const DEATH: u64 = u64::MAX / 2;

            let now = Arc::new(AtomicU64::new(START));
            let counting = CountingCellStore::new(MemoryCellStore::new(MemoryCells::new()));
            let ttl_lower =
                TtlAwareCellStore::new(counting.clone(), Clock::Fixed(now.clone()), DEATH);
            let fjall = test_db::cache_with_clock("transparent", Clock::Fixed(now.clone()))?;
            let fail_puts = fjall.faults().fail_puts();
            let fail_deletes = fjall.faults().fail_deletes();
            let subject = Cached::new(fjall.clone(), ttl_lower.clone());
            let twin = MemoryCellStore::new(MemoryCells::new());
            let id = collection("transparent")?;
            let ttl = trace.ttl.map(CompactDuration::new);
            let mut replay = Replay {
                subject,
                twin,
                counting,
                cref: CollectionRef::new(id.clone(), ttl),
                twin_ref: CollectionRef::new(id.clone(), ttl),
                id,
                now,
                ttl_ms: trace.ttl.map(|s| u64::from(s) * 1_000),
                clock: START,
                stage_seq: 0,
                staged: None,
                fault_puts: false,
                warm: WarmModel::new(),
                fail_puts,
                fail_deletes,
            };

            for (index, op) in trace.ops.iter().enumerate() {
                replay
                    .step(op)
                    .await
                    .map_err(|e| eyre!("op {index} ({op:?}): {e}"))?;
                replay
                    .verify()
                    .await
                    .map_err(|e| eyre!("after op {index} ({op:?}): {e}"))?;
            }

            // The KV5 budget arm: heal the seams, run one model-updating
            // verification pass to refill cold entries, then assert every warm-model cell
            // re-gets with zero lower reads.
            replay.fail_puts.store(false, Ordering::Relaxed);
            replay.fail_deletes.store(0, Ordering::Relaxed);
            replay.fault_puts = false;
            replay.verify().await.map_err(|e| eyre!("heal pass: {e}"))?;
            for key in 0..POOL {
                if !replay.is_warm::<Values>(key) {
                    continue;
                }
                ttl_lower.reset();
                let _ =
                    CellRead::<Values>::read(&replay.subject, &replay.id, cell_at(key).as_ref())
                        .await
                        .map(|(committed, _)| committed)
                        .map_err(|e| eyre!("budget get({key}): {e:?}"))?;
                if ttl_lower.lower_reads() != 0 {
                    return Err(eyre!(
                        "KV5 violated: warm cell {key} paid {} lower read(s)",
                        ttl_lower.lower_reads()
                    ));
                }
            }
            check_failed_probes(&replay, &fjall).await?;
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(CacheTrace) -> Result<bool>);
}
