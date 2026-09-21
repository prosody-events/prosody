//! Session gates preserve tracked keyset updates.

use super::*;

/// The keyset read-modify-write race pin (the pre-existing lost-update the gate
/// closes): two racing fresh-key sets serialize under the gate, so the keyset
/// is the UNION of both keys (not a last-wins singleton) and a stream yields
/// both entries. Red-proven by making `OwnerEngine::begin_write` return a
/// witness over an already-released permit: the parked
/// set's stale keyset read overwrites the other's update, the keyset loses a
/// key, and the current-membership invariant breaks.
#[test]
pub(super) fn gate_serializes_racing_keyset_rmw() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_rmw")?;
        let session = fx.session(1);
        let handle = map_state::<I64KeyCodec, JsonCodec>("m")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(1) parks at its keyset read while holding the gate; set(9) parks
        // on the gate behind it.
        fx.holds.read().arm(1);
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&1, Value::from(1_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("set(1) never reached its hold"))?;
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&9, Value::from(9_i64)).await }
        });
        let_task_park().await;
        fx.holds.read().release();
        timeout(HANG_GUARD, first)
            .await
            .map_err(|_| eyre!("set(1) hung"))??
            .map_err(|e| eyre!("set(1): {e}"))?;
        timeout(HANG_GUARD, second)
            .await
            .map_err(|_| eyre!("set(9) hung"))??
            .map_err(|e| eyre!("set(9): {e}"))?;

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
        let mut keys = Vec::new();
        {
            let stream = fresh.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(
            keys,
            vec![1, 9],
            "serialized keyset updates track both keys — no lost membership"
        );

        // The keyset is the UNION {1, 9}, not a last-wins singleton.
        let id = fx.id("m")?;
        let keyset = CellRead::<Values>::read(&fx.counting, &id, &map::keyset_cell())
            .await?
            .0
            .into_inner()
            .ok_or_else(|| eyre!("missing keyset cell"))?;
        assert_eq!(
            keyset[..],
            tracked_frame(&[1, 9]),
            "the serialized keyset updates union both keys"
        );
        Ok(())
    })
}

/// The exact `Tracked` frame over ascending `i64` `keys`, built from the real
/// codec: tag `0`, `u32` BE count, then per key a `u32` BE length and its
/// coordinate bytes.
pub(super) fn tracked_frame(keys: &[i64]) -> Vec<u8> {
    let mut frame = vec![0u8];
    frame.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for k in keys {
        let coordinate = I64KeyCodec::encode(k);
        frame.extend_from_slice(&(coordinate.as_bytes().len() as u32).to_be_bytes());
        frame.extend_from_slice(coordinate.as_bytes());
    }
    frame
}

/// A JSON-encoded map entry payload for the seed writes.
pub(super) fn json_entry(v: i64) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(&Value::from(v))?))
}

/// The set/set-nearly-full keyset pin: two racing sets on a map already at
/// `limit - 1` keys serialize under the gate, so the second observes the
/// first's insert and overflows — the raw keyset is the `Overflowed` sentinel
/// and the map holds all four entries. Red without the gate: both sets read the
/// same two-key keyset, both compute a fitting three-key list, and a last-wins
/// keyset write silently under-tracks (a three-of-four Tracked frame survives).
#[test]
pub(super) fn gate_overflows_keyset_at_the_limit() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_keyset_overflow")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);

        // Seed a two-key keyset (limit 3) beneath the cache, so event ops read
        // cold and land on the armed hold.
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
        let handle = map_state::<I64KeyCodec, JsonCodec>("ks")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;

        // set(3) parks in its cold meta read while holding the gate; set(4)
        // parks on the gate behind it.
        fx.holds.read().arm(1);
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&3, Value::from(3_i64)).await }
        });
        timeout(HANG_GUARD, fx.holds.read().entered())
            .await
            .map_err(|_| eyre!("set(3) never reached its hold"))?;
        let second = tokio::spawn({
            let handle = handle.clone();
            async move { handle.set(&4, Value::from(4_i64)).await }
        });
        let_task_park().await;
        fx.holds.read().release();
        timeout(HANG_GUARD, first)
            .await
            .map_err(|_| eyre!("set(3) hung"))??
            .map_err(|e| eyre!("set(3): {e}"))?;
        timeout(HANG_GUARD, second)
            .await
            .map_err(|_| eyre!("set(4) hung"))??
            .map_err(|e| eyre!("set(4): {e}"))?;

        finalize_and_promote(&session, &fx.dedup, Uuid::from_u128(1), &fx.cells, &id).await?;

        // The serial second set exceeds the limit → Overflowed.
        let keyset = CellRead::<Values>::read(&fx.counting, &id, &map::keyset_cell())
            .await?
            .0
            .into_inner()
            .ok_or_else(|| eyre!("missing keyset cell"))?;
        assert_eq!(
            keyset[..],
            [1],
            "the second set over the limit collapses the keyset to Overflowed"
        );

        // No entry was lost: the map holds all four keys (via the scan path).
        let verify = fx.session(2);
        let fresh = map_state::<I64KeyCodec, JsonCodec>("ks")
            .bind(&verify)
            .map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(
            keys,
            vec![1, 2, 3, 4],
            "no entry was dropped by the overflow"
        );
        Ok(())
    })
}

/// Rotating map stays `Tracked` (the live-size bound): a map churned across
/// more distinct keys than the limit — each event removing the oldest key
/// before setting a new one, so live size never exceeds the limit — keeps a
/// `Tracked` keyset forever, because `remove` subtracts. A fresh stream then
/// takes the point-get arm and issues **zero** scans. Red-proven by making
/// `remove`'s subtract a no-write: the keyset never shrinks, the fourth
/// distinct key overflows, and the stream degrades to a full-section scan
/// (`lower_scans() == 1`).
#[test]
pub(super) fn map_keyset_rotating_stays_tracked() -> Result<()> {
    /// Distinct keys churned — strictly greater than the limit (3), so a
    /// keyset that never subtracts would overflow.
    const STEPS: i64 = 6;

    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_rotating")?;
        let id = fx.id("ks")?;
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // Each committed event removes the oldest key (once the window is full)
        // then sets a new one, so live size stays ≤ 3 while total distinct = 6.
        for step in 0..STEPS {
            let event = Uuid::from_u128(u128::try_from(step)? + 1);
            let session = fx.session(u128::try_from(step)? + 1);
            let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
            if step >= 3 {
                handle.remove(&(step - 3)).await.map_err(|e| eyre!("{e}"))?;
            }
            handle
                .set(&step, Value::from(step))
                .await
                .map_err(|e| eyre!("{e}"))?;
            finalize_and_promote(&session, &fx.dedup, event, &fx.cells, &id).await?;
        }

        // A fresh stream over the live window {3,4,5} takes the Tracked arm.
        fx.counting.reset();
        let verify = fx.session(100);
        let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(keys, vec![3, 4, 5], "the stream yields the live window");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "a rotating map that never overflows streams by point gets, not a scan"
        );
        Ok(())
    })
}

/// Removal heals an oversized frame (the remove-side twin of
/// `map_keyset_oversized_frame_collapses_before_fast_path`): a stored oversized
/// `Tracked` frame degrades the stream to a full-section scan, but once
/// `remove` subtracts enough keys to bring the frame back under the limit, a
/// fresh stream takes the point-get arm again. Red-proven by making
/// `PriorKeyset::remove` write `Overflowed` instead of the shrunk frame:
/// removal never heals, so the post-remove stream still degrades
/// (`lower_scans() == 1`).
#[test]
pub(super) fn map_keyset_removal_heals_oversized() -> Result<()> {
    runtime()?.block_on(async {
        let fx = GateFixture::new("gate_heal_oversized")?;
        let id = fx.id("ks")?;
        let cref = CollectionRef::new(id.clone(), None);
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("ks");

        // Seed a valid but oversized 5-key Tracked frame (limit 3) + entries.
        let mut seed = vec![(
            map::keyset_cell(),
            Some(Bytes::from(tracked_frame(&[1, 2, 3, 4, 5]))),
        )];
        for k in 1..=5_i64 {
            seed.push((
                map::entry_cell_for(&I64KeyCodec::encode(&k)),
                Some(json_entry(k)?),
            ));
        }
        fx.counting.write_resolved(&cref, &seed, &[]).await?;

        // The oversized frame degrades to a full-section scan.
        fx.counting.reset();
        {
            let verify = fx.session(1);
            let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
            let stream = fresh.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while (stream.next().await).is_some() {}
        }
        assert_eq!(
            fx.counting.lower_scans(),
            1,
            "an oversized keyset degrades to the full-section scan"
        );

        // A committed event removes keys 1 and 2, bringing the tracked set to
        // {3,4,5} (≤ limit) — remove subtracts, rewriting a smaller Tracked.
        let session = fx.session(2);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle.remove(&1).await.map_err(|e| eyre!("{e}"))?;
        handle.remove(&2).await.map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&session, &fx.dedup, Uuid::from_u128(2), &fx.cells, &id).await?;

        // The healed frame ({3,4,5}) takes the point-get arm — no scan.
        fx.counting.reset();
        let verify = fx.session(3);
        let fresh = descriptor.bind(&verify).map_err(|e| eyre!("bind: {e}"))?;
        let mut keys = Vec::new();
        {
            let stream = fresh.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let (key, _) = item.map_err(|e| eyre!("stream: {e}"))?;
                keys.push(key);
            }
        }
        assert_eq!(keys, vec![3, 4, 5], "the stream yields the remaining keys");
        assert_eq!(
            fx.counting.lower_scans(),
            0,
            "removal healed the frame back under the bound: the fast arm is restored"
        );
        Ok(())
    })
}
