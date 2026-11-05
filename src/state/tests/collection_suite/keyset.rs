//! Tracked keyset limits, repair, and removal.

use super::*;

/// On a **TTL'd** map every set rewrites the keyset with its current contents
/// (not `Overflowed`): two committed sets under the limit leave the exact
/// two-key `Tracked` frame, and re-setting an already-tracked key still
/// refreshes that same `Tracked` frame rather than collapsing it. Guards
/// against a TTL-refresh that writes `Overflowed` — invisible to a
/// presence-only snapshot assert. The re-set of an already-tracked key is what
/// exercises the `Ok(_)` TTL fast-path arm; two distinct fresh keys never reach
/// it.
#[test]
pub(super) fn map_keyset_stays_tracked_under_ttl() -> Result<()> {
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(Some(CompactDuration::new(3_600)))
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&1, Value::from(1_u8)).await?;
        handle.set(&2, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner()
        .map(|b| b.to_vec()),
        Some(tracked_frame(&[1, 2])),
        "a TTL'd map keeps a Tracked keyset, never collapses to Overflowed"
    );

    // Re-set an ALREADY-tracked key on the TTL'd map: the already-tracked fast
    // path must still rewrite the keyset to refresh its TTL, and with the SAME
    // Tracked contents — never `Overflowed`. This is the `Ok(_)` TTL arm the two
    // fresh keys above never reach.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&1, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner()
        .map(|b| b.to_vec()),
        Some(tracked_frame(&[1, 2])),
        "re-setting a tracked key on a TTL'd map keeps the Tracked frame, never Overflowed"
    );
    Ok(())
}

/// A malformed keyset frame degrades iteration to the full-section scan (never
/// errors) and is healed by the next set (which writes `Overflowed`).
#[test]
pub(super) fn map_keyset_malformed_frame_degrades_and_heals() -> Result<()> {
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    // Event 1: a committed two-key map (writes a valid Tracked keyset).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&1, Value::from(1_u8)).await?;
        handle.set(&2, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Corrupt the keyset cell raw (unknown tag).
    block_on(store.write_resolved(
        &collection_ref,
        &[(keyset_cell(), Some(Bytes::from(vec![9_u8])))],
        &[],
    ))?;

    // A fresh stream degrades to the full-section scan and yields both entries
    // (no error).
    let read = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    let items = block_on(collect_map(&read_handle, Direction::Forward))?;
    assert_eq!(
        items,
        vec![(1, Value::from(1_u8)), (2, Value::from(2_u8))],
        "a malformed keyset degrades to the full-section scan, not an error"
    );

    // Event 2: a committed set heals the cell (malformed → Overflowed).
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&1, Value::from(9_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("the healing set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "the next set heals the malformed frame to Overflowed"
    );
    Ok(())
}

/// An oversized (but valid) stored `Tracked` frame — more keys than the
/// registered limit — degrades iteration to the full-section scan, and the next
/// set of an **already-listed** key collapses it to `Overflowed`: the size
/// check runs before the already-present fast path. The set-side twin of the
/// remove-side heal (`map_keyset_removal_heals_oversized`): they pin opposite
/// directions of the same `is_oversized` boundary.
#[test]
pub(super) fn map_keyset_oversized_frame_collapses_before_fast_path() -> Result<()> {
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    // Seed a valid 5-key Tracked keyset (over limit 3) plus its 5 entries.
    let mut seed = vec![(
        keyset_cell(),
        Some(Bytes::from(tracked_frame(&[1, 2, 3, 4, 5]))),
    )];
    for k in 1..=5_i64 {
        seed.push((
            entry_cell_for(&I64KeyCodec::encode(&k)),
            Some(Bytes::from(serde_json::to_vec(&Value::from(
                u8::try_from(k)?,
            ))?)),
        ));
    }
    block_on(store.write_resolved(&collection_ref, &seed, &[]))?;

    // The oversized Tracked keyset degrades to the full-section scan (yields
    // all 5).
    let read = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    let items = block_on(collect_map(&read_handle, Direction::Forward))?;
    assert_eq!(
        items.len(),
        5,
        "the oversized keyset degrades to the full-section scan"
    );

    // A set of an ALREADY-LISTED key collapses to Overflowed — the size check
    // ran before the already-present fast path.
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&1, Value::from(11_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("the set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "an oversized Tracked collapses to Overflowed even when the key is already listed"
    );
    Ok(())
}

/// The encoded-byte ceiling overflows the keyset even when the key count is far
/// under the limit: two committed sets of ~40 KiB string keys exceed the 64 KiB
/// frame ceiling on the second, writing `Overflowed`.
#[test]
pub(super) fn map_keyset_byte_ceiling_overflows() -> Result<()> {
    use crate::state::order_codec::Utf8KeyCodec;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<Utf8KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    let big_a = "a".repeat(40 * 1024);
    let big_b = "b".repeat(40 * 1024);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&big_a, Value::from(1_u8)).await?;
        handle.set(&big_b, Value::from(2_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("the sets must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "two ~40 KiB keys exceed the 64 KiB frame ceiling (count far under the limit)"
    );
    Ok(())
}

/// On a non-TTL'd map a set of an already-tracked key writes **no** keyset cell
/// (a no-op content change), while `remove` **subtracts** — rewriting the
/// `Tracked` frame without the removed coordinate (here down to the empty
/// `Tracked([])`). Both are invisible to a value-only assert, so probed on the
/// dirty write set itself.
#[test]
pub(super) fn map_keyset_subtracts_on_remove() -> Result<()> {
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&descriptor, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );

    // Event 1: set key 7 committed (a fresh single-key Tracked keyset).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event1), &cells, &id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Event 2 over a caller-owned dirty workspace: re-set 7, then remove it.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let dirty = Arc::new(DirtyStore::new());
    let session =
        make_session_with_dirty(&cells, &dedup, &registry, &state_key, event2, dirty.clone());
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&7, Value::from(2_u8)).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let after_reset = dirty.collection_snapshot(&id);
    assert!(
        !after_reset.iter().any(|(c, _)| *c == keyset_cell()),
        "a non-TTL re-set of a tracked key writes no keyset cell"
    );
    block_on(async {
        handle.remove(&7).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let after_remove = dirty.collection_snapshot(&id);
    let keyset_write = after_remove
        .iter()
        .find(|(c, _)| *c == keyset_cell())
        .ok_or_else(|| eyre!("remove must rewrite the keyset cell (subtracting the key)"))?;
    assert_eq!(
        keyset_write.1.as_deref(),
        Some(&tracked_frame(&[])[..]),
        "remove subtracts the last key, leaving an empty Tracked frame"
    );
    Ok(())
}
