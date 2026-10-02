//! Session gates serialize writes and commits.

use super::*;

/// KV4 — a get-fill suspended across a `commit()` of the same cell.
/// The harness FORCES the round-2 schedule — the fill's lower read completes
/// (linearizing before the commit's durable write), its publish is withheld,
/// the commit's write-through would land, then the fill resumes. With the
/// gate, the commit parks until the whole get (read + publish) completes, so
/// the re-get answers the committed value WARM (zero lower reads). Red-proven
/// by making `OwnerEngine::begin_read` hand back a witness over an
/// already-released permit: the stale fill publish overwrites the commit's
/// write-through and the warm re-get answers the pre-commit value.
#[test]
pub(super) fn gate_serializes_fill_against_commit() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_fill_commit")?;
        let cref = CollectionRef::new(fx.id("v")?, None);
        // Committed base "A", seeded beneath the cache so the fill is cold.
        fx.counting
            .write_resolved(
                &cref,
                &[(
                    value_cell(),
                    Some(Bytes::from(serde_json::to_vec(&Value::from("A"))?)),
                )],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Suspend the fill after its lower read, before its publish.
        fx.holds.read().arm(1);
        let get_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.get().await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the fill never reached its hold"))?;

        // The racing set+commit parks on the gate the get holds.
        let commit_task = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle.set(Value::from("B")).await?;
                handle.commit().await?;
                Ok::<_, CellStateError<JsonCodecError>>(())
            }
        });
        let_task_park().await;
        fx.holds.read().release();

        let got = timeout(HANG_GUARD, get_task)
            .await
            .map_err(|_| eyre!("get hung"))??
            .map_err(|e| eyre!("get: {e}"))?;
        assert_eq!(
            got,
            Some(Value::from("A")),
            "the fill read the pre-commit value"
        );
        timeout(HANG_GUARD, commit_task)
            .await
            .map_err(|_| eyre!("commit hung"))??
            .map_err(|e| eyre!("commit: {e}"))?;

        // The serial-order assert, on the BUDGET (a value-only assert could be
        // healed by a fall-through): the re-get answers the committed B warm.
        fx.counting.reset();
        let after = handle.get().await.map_err(|e| eyre!("re-get: {e}"))?;
        assert_eq!(
            after,
            Some(Value::from("B")),
            "the fill never overwrote the newer write-through"
        );
        assert_eq!(
            fx.counting.lower_reads(),
            0,
            "the re-get is warm — the commit's write-through survived the suspended fill"
        );
        Ok(())
    })
}

/// KV4 — a `set` racing `commit()`'s snapshot→drain window. The
/// commit's lower write is withheld after it lands; the racing set parks on
/// the gate, so its cell is buffered strictly after the drain and survives to
/// the settle — nothing is lost. Red-proven by making
/// `OwnerEngine::begin_write` hand back a witness over an already-released
/// permit: the set buffers into the snapshot→drain window and the drain
/// silently drops it (the pre-existing lost-update this gate closes).
#[test]
pub(super) fn gate_serializes_set_against_commit_drain() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_set_drain")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        handle
            .set(&1, Value::from(10_i64))
            .await
            .map_err(|e| eyre!("{e}"))?;

        // Withhold the commit's durable write (it lands; the response parks).
        fx.holds.write_resolved().arm(1);
        let commit_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.commit().await }
        });
        timeout(HANG_GUARD, fx.holds.write_resolved().entered())
            .await
            .map_err(|_| eyre!("the commit never reached its hold"))?;

        // The racing set parks on the gate the commit holds.
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&2, Value::from(20_i64)).await }
        });
        let_task_park().await;
        fx.holds.write_resolved().release();
        timeout(HANG_GUARD, commit_task)
            .await
            .map_err(|_| eyre!("commit hung"))??
            .map_err(|e| eyre!("commit: {e}"))?;
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;

        // Settle the event; BOTH cells must be durable — the parked set was
        // buffered after the drain, not swallowed by it.
        finalize_and_promote(
            &session,
            &fx.dedup,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("m")?,
        )
        .await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert_eq!(
            fresh.get(&1).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(10_i64)),
            "the committed cell survived"
        );
        assert_eq!(
            fresh.get(&2).await.map_err(|e| eyre!("{e}"))?,
            Some(Value::from(20_i64)),
            "the racing set was never dropped by the drain"
        );
        Ok(())
    })
}

/// KV4 — a `set` racing `clear()`, proving the map's core invariant —
/// **every live entry is covered by a present keyset** (`KeysetPresence`) —
/// survives the race. The teeth need a *non-empty* map: on an empty map both
/// serial orders leave a valid state, so the invariant can't be violated.
/// Seeded cold with `{0,1,2}` and keyset `Tracked{0,1,2}`, then `set(1)` (a key
/// already tracked, so on a non-TTL map its keyset write is suppressed) parks
/// at its cold keyset read HOLDING the gate. `clear()` is polled exactly once:
/// with the gate it parks (a single deterministic `Poll::Pending`, no scheduler
/// heuristic) and runs only after the set completes — set-then-clear leaves the
/// map empty. Red-proven by making `OwnerEngine::begin_write` return a witness
/// over an already-released permit (so `set` and `clear` no longer exclude each
/// other): the first poll runs `clear` to completion, then the resumed `set`
/// writes ONLY the entry (its keyset write suppressed), stranding a live entry
/// with an absent keyset — the invariant the gate protects.
#[test]
pub(super) fn gate_serializes_set_against_clear() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_set_clear")?;
        let id = fx.id("m")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a valid, cold, non-TTL map: entries {0,1,2} and a three-key
        // Tracked keyset. Cold so the event's meta read lands on the armed hold.
        fx.counting
            .write_resolved(
                &cref,
                &[
                    (
                        map::keyset_cell(),
                        Some(Bytes::from(tracked_frame(&[0, 1, 2]))),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&0)),
                        Some(json_entry(0)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&1)),
                        Some(json_entry(1)?),
                    ),
                    (
                        map::entry_cell_for(&I64KeyCodec::encode(&2)),
                        Some(json_entry(2)?),
                    ),
                ],
                &[],
            )
            .await?;

        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(1) parks at its cold keyset read (a held lower read) while HOLDING
        // the gate. 1 is already tracked, so once it resumes its only write is
        // the entry.
        fx.holds.read().arm(1);
        let set_task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&1, Value::from(99_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the set never reached its hold"))?;

        // Poll clear ONCE. With the gate it hits the held permit and returns
        // Pending deterministically; without it, clear runs to completion in
        // this single poll (every op is Ready).
        let clear = handle.clear();
        futures::pin_mut!(clear);
        let first_clear_poll = futures::poll!(clear.as_mut());

        fx.holds.read().release();
        timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??
            .map_err(|e| eyre!("set: {e}"))?;
        match first_clear_poll {
            Poll::Ready(result) => result.map_err(|e| eyre!("clear: {e}"))?,
            Poll::Pending => timeout(HANG_GUARD, clear)
                .await
                .map_err(|_| eyre!("clear hung"))?
                .map_err(|e| eyre!("clear: {e}"))?,
        }

        // Settle, then probe the physical state: no live entry may survive with
        // an absent keyset. With the gate the outcome is set-then-clear (empty);
        // the injected race strands entry 1 with a cleared keyset.
        finalize_and_promote(&session, &fx.dedup, Uuid::from_u128(1), &fx.cells, &id).await?;
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        let entry = fresh.get(&1).await.map_err(|e| eyre!("{e}"))?;
        let keyset = CellRead::<Values>::read(&fx.counting, &id, map::keyset_cell().as_ref())
            .await?
            .0
            .into_inner();
        assert!(
            entry.is_none() || keyset.is_some(),
            "a live entry must be covered by a present keyset (entry={:?}, keyset={:?})",
            entry.is_some(),
            keyset.is_some(),
        );
        Ok(())
    })
}
