//! Streams reject results from a stale attempt.

use super::*;
use crate::state::query::tests::query_buffer;

/// Whether a fenced map outcome — a stream item or a call's error — is the
/// `Terminated` access error.
pub(super) fn map_item_terminated(item: &MapStateError<JsonCodecError>) -> bool {
    matches!(
        item,
        MapStateError::Cell(CellStateError::Access(StateAccessError::Terminated))
    )
}

/// Scan-shell fence, RANGE source: the map degrade arm streams through the
/// gate-free range source, and an emission after an observed attempt bump
/// errors `Terminated` — the managed stream's per-emission fence catches it,
/// not the source (which keeps producing). The first item crosses pre-bump; the
/// range source holds no admission, so the `reset` between pulls bumps
/// immediately. Red proven by dropping the `fenced(...)` wrapper in
/// `Plan::projected` (return the raw source): the post-bump pull then
/// yields a second `Ok` item.
#[test]
pub(super) fn range_scan_stream_fences_after_bump() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_range_scan")?;
        let cref = CollectionRef::new(fx.id("ks")?, None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // An oversized 4-key Tracked frame (limit 3) degrades the stream to the
        // full-section range source; ≥ 2 entries so a second emission exists.
        let mut seed = vec![(
            map::keyset_cell(),
            Some(Bytes::from(tracked_frame(&[1, 2, 3, 4]))),
        )];
        for k in 1..=4_i64 {
            seed.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seed, &[]).await?;

        let session = fx.session(1);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let stream = handle.entries(query_buffer()).stream();
        futures::pin_mut!(stream);

        match stream.next().await {
            Some(Ok(_)) => {}
            other => bail!("the first range item must cross pre-bump, got {other:?}"),
        }
        // Attempt boundary: the range source holds no permit, so reset bumps now.
        session.reset(RepinProof::for_test()).await;
        match stream.next().await {
            Some(Err(ref e)) if map_item_terminated(e) => {}
            other => bail!("the post-bump range emission must be Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// Scan-shell fence, COORDINATE source: the map tracked arm point-gets a chunk,
/// collects it into a bounded buffer, and releases the permit before the first
/// yield; a buffered entry never crosses the fence after an observed bump. Both
/// keys land in one chunk (`CELL_BATCH >= 2`), so the first entry's fence
/// check passes pre-bump and the second's runs post-bump. Red proven by
/// dropping the `fenced(...)` wrapper in `Plan::projected`: the
/// buffered second entry then crosses as an `Ok`.
#[test]
pub(super) fn coordinate_stream_fences_buffered_entries_after_bump() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_coord_scan")?;
        let cref = CollectionRef::new(fx.id("m")?, None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("m");

        // Exactly two tracked keys: one chunk, both entries buffered together.
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
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let stream = handle.entries(query_buffer()).stream();
        futures::pin_mut!(stream);

        // The chunk is fetched and the permit dropped before this first yield.
        match stream.next().await {
            Some(Ok((1, _))) => {}
            other => bail!("the first buffered entry must cross pre-bump, got {other:?}"),
        }
        // Attempt boundary: the chunk permit is already dropped, so reset bumps.
        session.reset(RepinProof::for_test()).await;
        // The SECOND buffered entry's emission check runs post-bump.
        match stream.next().await {
            Some(Err(ref e)) if map_item_terminated(e) => {}
            other => bail!("the buffered second entry must be fenced Terminated, got {other:?}"),
        }
        Ok(())
    })
}

/// A stale-pinned mutation on an already-closed session classifies
/// `Terminated`, never `SessionClosed` — `mutate_permit` checks the pin before
/// the closed flag, so a dead-attempt op is fenced uniformly regardless of the
/// close. Isolates the admission ORDER: `ensure_live` never runs when the
/// permit errors `SessionClosed` first, so the order flip is observable only
/// here. Red-proven by swapping the pin and closed checks in `mutate_permit`:
/// the stale `set` then returns `SessionClosed`.
#[test]
pub(super) fn stale_mutator_on_closed_session_is_terminated_not_closed() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("fence_stale_closed")?;
        let session = fx.session(1);
        let handle = value_state::<JsonCodec>("v")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // Stale pin, then close the gate as the settle boundary would.
        session.reset(RepinProof::for_test()).await;
        drop(session.close_gate().await);

        match handle.set(Value::from(8_i64)).await {
            Err(ref e) if is_terminated(e) => {}
            other => {
                bail!("a stale mutator on a closed session must be Terminated, got {other:?}")
            }
        }
        Ok(())
    })
}
