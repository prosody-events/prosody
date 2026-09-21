//! Read plans and batches hold the required admission.

use super::*;

/// An absent keyset produces no entry reads or scans.
/// Only the keyset read reaches storage.
/// A scan in the membership plan's absent-keyset arm fails this test.
#[test]
pub(super) fn map_absent_keyset_streams_zero_reads() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_absent_keyset")?;
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");
        let session = fx.session(1);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        fx.counting.reset();
        let mut yielded = 0usize;
        {
            let stream = handle.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                item.map_err(|e| eyre!("stream: {e}"))?;
                yielded += 1;
            }
        }
        assert_eq!(yielded, 0, "an absent keyset yields nothing");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "Absent → Empty issues no scan"
        );
        assert_eq!(
            fx.counting.lower_reads(),
            1,
            "the only lower read is the single keyset get; no entry point-gets"
        );
        Ok(())
    })
}

/// The set-racing-stream pin (the chunked-stream contract): a mutator racing a
/// live stream serializes against the CURRENT chunk fetch and lands **between
/// chunks**, never mid-fetch. The stream snapshots key membership at its init
/// keyset read, then releases the gate before it fetches the entry chunk. A
/// `set` parked on the gate during the init read therefore lands first (FIFO)
/// and buffers `1→99` into the shared overlay. The entry chunk is a fresh gate
/// acquire, so it reads key 1 through the overlay and sees 99. Values are read
/// live, chunk by chunk — the point-get arm's per-arm consistency contract.
/// The interleaving property `run_map_stream_interleave` is the stronger
/// successor (named in the commit). Red-proven by making
/// `OwnerEngine::begin_write` (or `OwnerEngine::resume`, the stream's per-chunk
/// acquire) hand back a witness over an already-released permit: without
/// serialization the yield is nondeterministic and the stream can observe a
/// torn state.
#[test]
pub(super) fn gate_excludes_set_during_keyset_stream() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_stream")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed {1: 10, 2: 20} with a two-key keyset beneath the cache (cold).
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(10)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(20)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("ks")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // The stream's FIRST cold read is the keyset cell — park it there,
        // holding the gate.
        fx.holds.read().arm(1);
        let stream_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                let mut out = Vec::new();
                let stream = handle.entries(KeyQuery::new(Direction::Forward));
                futures::pin_mut!(stream);
                while let Some(item) = stream.next().await {
                    out.push(item?);
                }
                Ok::<_, MapStateError<JsonCodecError>>(out)
            }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the stream never reached its keyset hold"))?;

        // The racing set of a listed key parks on the gate.
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&1, Value::from(99_i64)).await }
        });
        let_task_park().await;
        fx.holds.read().release();

        let yielded = timeout(HANG_GUARD, stream_task)
            .await
            .map_err(|_| eyre!("stream hung"))??
            .map_err(|e| eyre!("stream: {e}"))?;
        assert_eq!(
            yielded,
            vec![(1, Value::from(99_i64)), (2, Value::from(20_i64))],
            "the racing set landed between the init keyset read and the entry chunk fetch \
             (chunk-scoped hold, not whole-stream): the chunk read key 1 through the overlay"
        );
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        assert_eq!(
            handle.get(&1).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(99_i64)),
            "the set is durable in the overlay"
        );
        Ok(())
    })
}

/// The single-hold isolation pin for `Map::get_many`: a `> CELL_BATCH` call
/// holds the session gate ONCE across its two internal sub-batches, so a
/// concurrent mutator queued on the gate serializes entirely AFTER the whole
/// read and the read observes no intermediate state. The first sub-batch is
/// cold, so `get_many` parks in its cache-fill holding the gate; a `set` on a
/// sub-batch-2 key is queued (FIFO); on release `get_many` finishes both
/// sub-batches — reading the committed pre-set value for that key — before the
/// set runs. Red-proven by rewriting `Map::get_many` to acquire the read permit
/// PER `CELL_BATCH` sub-batch (drop + reacquire between them): the queued
/// set then wins the gate at the boundary and buffers its dirty write, so
/// sub-batch 2's overlay read answers the NEW value.
#[test]
pub(super) fn map_get_many_holds_gate_across_sub_batches() -> Result<()> {
    /// Two sub-batches: the first full `CELL_BATCH` chunk, then the remainder.
    const N: i64 = CELL_BATCH.get() as i64 + 2;
    /// First key of sub-batch 2 — the one the concurrent set targets.
    const TARGET: i64 = CELL_BATCH.get() as i64;

    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_get_many_isolation")?;
        let cref = CollectionRef::new(fx.id("m")?, None);

        // Seed N keys committed BENEATH the cache (cold), value == key.
        let mut seeded = Vec::new();
        for k in 0..N {
            seeded.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seeded, &[]).await?;

        let session = fx.session(1);
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        let keys: Vec<i64> = (0..N).collect();

        // Park get_many in sub-batch 1's cold cache-fill (its first lower read),
        // holding the gate.
        fx.holds.read().arm(1);
        let reader = tokio::spawn({
            let map = map.clone();
            let keys = keys.clone();
            async move { Box::pin(map.get_many(&keys)).await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("get_many never reached the sub-batch-1 hold"))?;

        // A set on the sub-batch-2 target parks on the gate (get_many holds it).
        let writer = tokio::spawn({
            let map = map.clone();
            async move { map.set(&TARGET, Value::from(999_i64)).await }
        });
        let_task_park().await;

        // Release the hold; correct code keeps the gate across the boundary.
        fx.holds.read().release();
        let out = timeout(HANG_GUARD, reader)
            .await
            .map_err(|_| eyre!("get_many hung"))??
            .map_err(|e| eyre!("get_many: {e}"))?;
        timeout(HANG_GUARD, writer)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;

        assert_eq!(out.len(), keys.len(), "every position answered");
        assert_eq!(
            out[TARGET as usize],
            Some(Value::from(TARGET)),
            "the sub-batch-2 read observed the committed pre-set value; the queued set serialized \
             after the whole get_many"
        );
        Ok(())
    })
}
