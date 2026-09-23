//! Attempt resets fence stale handles and preserve current writes.

use super::*;

/// The closure pin (the mutator fence): after the settle boundary closes the
/// gate, a detached mutator errors [`StateAccessError::SessionClosed`] while
/// a read still answers — the post-settle apply-hook read contract, made
/// explicit at the session level (`hook_visibility` is its unmodified
/// middleware-level witness).
#[test]
pub(super) fn closed_session_fences_mutators_but_serves_hook_reads() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_closed")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle
            .set(Value::from(7_i64))
            .await
            .map_err(|e| eyre!("set: {e}"))?;
        finalize_and_promote(
            &session,
            &fx.dedup,
            Uuid::from_u128(1),
            &fx.cells,
            &fx.id("v")?,
        )
        .await?;

        // The settle boundary's close: acquire once, mark Closed, drop the
        // permit before the hooks fire.
        let permit = session.close_gate().await;
        drop(permit);

        // A detached mutator errors SessionClosed.
        let denied = handle.set(Value::from(8_i64)).await;
        match denied {
            Err(CellStateError::Access(StateAccessError::SessionClosed)) => {}
            other => bail!("a closed session must fence mutators, got {other:?}"),
        }
        // rollback answers NoOp (its infallible containment posture).
        assert_eq!(
            handle.rollback().await,
            StoreOutcome::NoOp,
            "rollback on a closed session discards nothing"
        );
        // A read still answers — the apply hooks read state through it.
        assert_eq!(
            handle.get().await.map_err(|e| eyre!("hook read: {e}"))?,
            Some(Value::from(7_i64)),
            "post-settle reads serve the settled state"
        );
        Ok(())
    })
}

// ==========================================================================
// Attempt-epoch fence pins
// ==========================================================================
//
// The mechanics of the per-event attempt epoch (`AttemptEpoch`): a handle,
// stream, or session clone pins the epoch that was live when it was minted, and
// every cell op fails `Terminated` once a later attempt boundary (`reset`)
// bumped it. These pins drive `reset`/`repin` directly through
// `RepinProof::for_test()` at the typed layer — no retry loop — so each fence
// rule is isolated.

/// Whether a fenced cell op returned the `Terminated` access error.
pub(super) fn is_terminated(err: &CellStateError<JsonCodecError>) -> bool {
    matches!(err, CellStateError::Access(StateAccessError::Terminated))
}

/// Seeds `value` as the committed base of collection `v` beneath the cache, so
/// a later cold `get` fills through the lower store.
pub(super) async fn seed_committed_v(fx: &GateFixture, value: &Value) -> Result<()> {
    let cref = CollectionRef::new(fx.id("v")?, None);
    fx.counting
        .write_resolved(
            &cref,
            &[(value_cell(), Some(Bytes::from(serde_json::to_vec(value)?)))],
            &[],
        )
        .await?;
    Ok(())
}

/// Settles `session`, then asserts a fresh event's read of `v` answers
/// `expected` — the zero-store-effect verification every fence pin ends with.
pub(super) async fn settle_and_verify(
    fx: &GateFixture,
    session: &KeyedStateSession<GateBackend, MemoryLoader<Value>>,
    expected: Option<Value>,
    msg: &str,
) -> Result<()> {
    finalize_and_promote(
        session,
        &fx.dedup,
        Uuid::from_u128(1),
        &fx.cells,
        &fx.id("v")?,
    )
    .await?;
    let fresh = value_state::<JsonCodec>("v")
        .bind(&fx.session(2))
        .map_err(|e| eyre!("bind: {e}"))?;
    assert_eq!(
        fresh.get().await.map_err(|e| eyre!("verify: {e}"))?,
        expected,
        "{msg}"
    );
    Ok(())
}

/// A typed handle op after `reset()` errors `Terminated`, with zero store
/// effect — the fenced `set` never reaches the committed cell. Red-proven by
/// deleting the `!session.attempt_current()` pin compare in `ensure_live`: the
/// stale-pinned `get`/`set` then answer live instead of `Terminated`.
#[test]
pub(super) fn handle_op_after_reset_is_terminated() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_reset_op")?;
        seed_committed_v(&fx, &Value::from("A")).await?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary: discard + bump. `handle` keeps the stale pin.
        session.reset(RepinProof::for_test()).await;

        match handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("get after reset must be Terminated, got {other:?}"),
        }
        match handle.set(Value::from("B")).await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("set after reset must be Terminated, got {other:?}"),
        }

        // Zero store effect: settle the (now attempt-N+1) session — the fenced
        // set never buffered, so nothing stages — and a fresh event still reads
        // the seeded "A".
        settle_and_verify(
            &fx,
            &session,
            Some(Value::from("A")),
            "the fenced set left the committed cell unchanged",
        )
        .await?;
        Ok(())
    })
}

/// `Map::get_many` after a `reset()` epoch bump errors `Terminated` as a whole
/// — never a partial `Vec`. The batch twin of
/// `handle_op_after_reset_is_terminated`: `ensure_live` fences at the first
/// `raw_get_many` before any cell is read, and the `Result<Vec<_>>` shape makes
/// a partial answer unrepresentable. Red-proven by deleting the
/// `!session.attempt_current()` pin compare in `ensure_live`: the stale-pinned
/// `get_many` then answers live.
#[test]
pub(super) fn map_get_many_after_reset_is_terminated() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_reset_get_many")?;
        let session = fx.session(1);
        let map = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary: discard + bump. `map` keeps the stale pin.
        session.reset(RepinProof::for_test()).await;

        match Box::pin(map.get_many(&[0, 1, 2])).await {
            Err(ref e) if map_item_terminated(e) => {}
            other => bail!("get_many after reset must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A leaked attempt-N session clone (and a clone of a clone) stays fenced after
/// `reset()`, while a `repin`-ed clone is live. Red-proven by deleting the
/// `!session.attempt_current()` pin compare in `ensure_live`: the leaked
/// clones' reads then answer live instead of `Terminated`.
#[test]
pub(super) fn leaked_clone_is_fenced_repin_is_live() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_leaked_clone")?;
        let session = fx.session(1);
        session.reset(RepinProof::for_test()).await; // epoch N -> N+1

        // A leaked attempt-N clone vends a previously-unbound collection; its
        // first op errors.
        let leaked = session.clone();
        let leaked_handle = value_state::<JsonCodec>("v")
            .bind(&leaked)
            .map_err(|e| eyre!("bind: {e}"))?;
        match leaked_handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a leaked attempt-N clone op must be Terminated, got {other:?}"),
        }

        // A clone of a clone carries the same stale pin.
        let leaked2 = session.clone().clone();
        let handle2 = value_state::<JsonCodec>("v")
            .bind(&leaked2)
            .map_err(|e| eyre!("bind: {e}"))?;
        match handle2.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a clone-of-a-clone op must be Terminated, got {other:?}"),
        }

        // A `repin`-ed clone is pinned to the live epoch and reads normally.
        let live = session.repin(RepinProof::for_test());
        let live_handle = value_state::<JsonCodec>("v")
            .bind(&live)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert_eq!(
            live_handle
                .get()
                .await
                .map_err(|e| eyre!("live get: {e}"))?,
            None,
            "the live attempt reads normally"
        );
        Ok(())
    })
}

/// The epoch bump sticks even with NOTHING vended before the boundary — the
/// first handle bound from a stale-pinned clone after `reset()` errors on its
/// first op. Red-proven by deleting the `!session.attempt_current()` pin
/// compare in `ensure_live`: the first op then answers live instead of
/// `Terminated`.
#[test]
pub(super) fn reset_bump_sticks_with_nothing_vended() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_bump_sticks")?;
        let session = fx.session(1);
        // No handle/stream vended before the boundary.
        session.reset(RepinProof::for_test()).await;
        // The FIRST handle, bound from a stale-pinned clone, errors on first op.
        let stale = session.clone();
        let handle = value_state::<JsonCodec>("v")
            .bind(&stale)
            .map_err(|e| eyre!("bind: {e}"))?;
        match handle.get().await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("first op after a vend-free bump must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A stale `rollback()` (pinned N) after `reset()` returns `NoOp` and leaves
/// attempt N+1's dirty buffer untouched. Red-proven by deleting the
/// `!self.attempt_current()` pin term from the session `rollback`'s
/// self-admission: the stale rollback then drains attempt N+1's live buffer and
/// the fresh event reads `None`.
#[test]
pub(super) fn stale_rollback_is_noop_and_spares_next_attempt() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_rollback")?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Attempt boundary, then seed N+1's dirty through a live (repin-ed)
        // handle over the SAME collection.
        session.reset(RepinProof::for_test()).await;
        let live = session.repin(RepinProof::for_test());
        let live_handle = value_state::<JsonCodec>("v")
            .bind(&live)
            .map_err(|e| eyre!("bind: {e}"))?;
        live_handle
            .set(Value::from("keep"))
            .await
            .map_err(|e| eyre!("set: {e}"))?;

        // Without the pin check the stale rollback would drain the live buffer.
        assert_eq!(
            stale.rollback().await,
            StoreOutcome::NoOp,
            "a stale rollback discards nothing"
        );

        // N+1 settles with the seeded value intact.
        settle_and_verify(
            &fx,
            &live,
            Some(Value::from("keep")),
            "attempt N+1's dirty survived the stale rollback",
        )
        .await?;
        Ok(())
    })
}

/// A stale queued write (pinned N) issued after the whole one-hold reset
/// transition errors instead of buffering, and attempt N+1 settles with no
/// residue of it — the `mutate_permit` pin fence, distinct from the queued-set
/// interleaving race (`racing_set_never_joins_next_attempt`). Red-proven only
/// by deleting BOTH `mutate_permit`'s pin check AND `ensure_live`'s pin
/// compare: either alone leaves the other to fence this stale `set`, so both
/// must go for the write to buffer and its residue to surface.
#[test]
pub(super) fn stale_write_after_reset_errors_and_leaves_no_residue() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_write")?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        session.reset(RepinProof::for_test()).await; // discard + bump, one hold

        match stale.set(Value::from("leak")).await {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("a stale write after reset must be Terminated, got {other:?}"),
        }

        // Settle N+1: nothing staged from the fenced write.
        settle_and_verify(
            &fx,
            &session,
            None,
            "the fenced stale write left no committed residue",
        )
        .await?;
        Ok(())
    })
}

/// A `set` forced to queue on the gate behind the reset transition never joins
/// attempt N+1's committed transaction (paused-time-free but deterministic via
/// FIFO gate ordering). A parked fill holds the gate; reset queues first, the
/// stale set second; releasing lets reset bump, then the set's admission pin
/// check fences it — the interleaving case where the check-then-mint race is
/// resolved by the held permit. Red-proven only by deleting BOTH
/// `mutate_permit`'s pin check AND `ensure_live`'s pin compare: either alone
/// still fences the racing set, so both must go for it to join attempt N+1.
#[test]
pub(super) fn racing_set_never_joins_next_attempt() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_race_set")?;
        // A committed base so the gate-holding get triggers a cold fill.
        seed_committed_v(&fx, &Value::from("A")).await?;
        let session = fx.session(1);
        let stale = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Park a fill holding the gate (epoch still N, so its own `ensure_live`
        // admitted it before the bump).
        fx.holds.read().arm(1);
        let get_task = tokio::spawn({
            let stale = stale.clone();
            async move { stale.get().await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("the fill never parked on the gate"))?;

        // Reset queues on the gate FIRST (behind the parked fill).
        let reset_task = tokio::spawn({
            let session = session.clone();
            async move { session.reset(RepinProof::for_test()).await }
        });
        let_task_park().await;
        // The stale set queues on the gate SECOND (behind reset).
        let set_task = tokio::spawn({
            let stale = stale.clone();
            async move { stale.set(Value::from("B")).await }
        });
        let_task_park().await;

        // Release the fill: reset acquires (discard+bump), then the set.
        fx.holds.read().release();
        timeout(HANG_GUARD, get_task)
            .await
            .map_err(|_| eyre!("get hung"))??
            .map_err(|e| eyre!("get: {e}"))?;
        timeout(HANG_GUARD, reset_task)
            .await
            .map_err(|_| eyre!("reset hung"))??;
        let set_result = timeout(HANG_GUARD, set_task)
            .await
            .map_err(|_| eyre!("set hung"))??;
        match set_result {
            Err(ref e) if is_terminated(e) => {}
            other => bail!("the set that lost the gate to reset must be Terminated, got {other:?}"),
        }

        // N+1 settles with no trace of "B".
        settle_and_verify(
            &fx,
            &session,
            Some(Value::from("A")),
            "the racing set never joined attempt N+1's transaction",
        )
        .await?;
        Ok(())
    })
}
