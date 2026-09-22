//! Initial map keysets, clear operations, and frozen bytes.

use super::*;

/// Map clear, pinned at the physical grain the `BTreeMap` model cannot reach. A
/// committed `clear()` erases the keyset cell with the entries. The
/// absent-keyset ⇒ empty-map reading (`KeysetPresence`) therefore survives a
/// cleared map. A later set repopulates a fresh single-key `Tracked` keyset:
/// clear resets the tracking, not just the entries, so no stale pre-clear list
/// remains.
///
/// It also pins the reset's **scope**. Directly-seeded cells at BOTH retired
/// meta coordinates `[0]` and `[1]` are erased too. Those are legacy rows from
/// the removed min/max bounds design, and no handle method reaches them.
/// `clear()` erases them because it is one whole-layout reset over the declared
/// sections, not a point clear of the keyset cell.
#[test]
pub(super) fn map_clear_erases_keyset_and_repopulates() -> Result<()> {
    use crate::state::cell_key::{CellKey, Coordinate};
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    // Event 1: one committed set stamps the keyset cell.
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Legacy artifacts at BOTH retired meta coordinates, seeded straight
    // through the store because no handle method can address them.
    let legacy: Vec<CellKey> = [0u8, 1]
        .into_iter()
        .map(|byte| CellKey {
            section: keyset_cell().section,
            coordinate: Coordinate::from_bytes(vec![byte]),
        })
        .collect();
    let seeded: Vec<_> = legacy
        .iter()
        .map(|cell| (cell.clone(), Some(bytes::Bytes::from_static(&[0xAB]))))
        .collect();
    block_on(store.write_resolved(&collection_ref, &seeded, &[]))?;

    // Event 2: committed clear — one whole-layout reset over both sections.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.clear().await?;
        finalize_and_promote(&session, &dedup, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner(),
        None,
        "the committed clear must erase the keyset cell"
    );
    for cell in &legacy {
        assert_eq!(
            block_on(async {
                CellRead::<Values>::read(&store, id, cell.as_ref())
                    .await
                    .map(|(committed, _)| committed)
            })?
            .into_inner(),
            None,
            "the whole-layout reset must erase every retired meta coordinate too"
        );
    }

    // Event 3: one committed set repopulates a fresh single-key Tracked keyset
    // (keyset absent after clear ⇒ the empty map ⇒ a fresh singleton, not a
    // stale pre-clear list).
    let event3 = EventRef::Message {
        dedup_id: Uuid::from_u128(3),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event3);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event3), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner(),
        Some(bytes::Bytes::from(tracked_frame(&[7]))),
        "a set after clear writes a fresh single-key Tracked keyset"
    );
    Ok(())
}

/// The first map write must persist its keyset and entry together.
/// A fresh collection prevents an earlier keyset from satisfying the check.
#[test]
pub(super) fn map_first_set_writes_keyset() -> Result<()> {
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();

    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&7, Value::from(1_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    assert!(
        cells.stored_coordinates(id).contains(&keyset_cell()),
        "the first committed set writes a physical keyset cell"
    );

    let read = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let read_handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    assert_eq!(
        block_on(read_handle.get(&7))?,
        Some(Value::from(1_u8)),
        "the entry is live"
    );
    Ok(())
}

/// Key enumeration hides a live entry without its keyset.
/// The direct emptiness check still finds the entry.
#[test]
pub(super) fn map_missing_keyset_hides_a_live_entry() -> Result<()> {
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone());
    let coordinate = I64KeyCodec::encode(&7);
    let value = Bytes::from(serde_json::to_vec(&Value::from(1_u8))?);
    block_on(store.write_resolved(
        &collection_ref,
        &[(entry_cell_for(&coordinate), Some(value))],
        &[],
    ))?;

    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        assert!(!handle.is_empty().await?);
        assert!(
            collect_map_keys(&handle, Direction::Forward)
                .await?
                .is_empty()
        );
        Ok::<_, color_eyre::Report>(())
    })
}

/// The exact `Tracked` frame bytes over `i64` keys (assumed ascending): tag
/// `0`, `u32` BE count, then per key a `u32` BE length and the 8-byte
/// sign-flipped BE coordinate — built from the real codec so a
/// coordinate-encoding change moves with it.
pub(super) fn tracked_frame(keys: &[i64]) -> Vec<u8> {
    let mut frame = vec![TRACKED_TAG_BYTE];
    frame.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for k in keys {
        let coordinate = I64KeyCodec::encode(k);
        frame.extend_from_slice(&(coordinate.as_bytes().len() as u32).to_be_bytes());
        frame.extend_from_slice(coordinate.as_bytes());
    }
    frame
}

/// The frozen `Tracked` tag byte (`MapKeysetCodec` — mirrored here so the
/// suite's golden frames pin the same value the codec writes).
pub(super) const TRACKED_TAG_BYTE: u8 = 0;

/// The frozen `Overflowed` sentinel frame (`[1]`).
pub(super) const OVERFLOWED_FRAME: [u8; 1] = [1];

/// Durable keyset frame golden: after committed sets the raw keyset cell holds
/// the exact `Tracked` frame — tag, `u32` count, per-key `u32` length +
/// sign-flipped BE coordinate, sort order, and the `[2]` address, all in one
/// probe — and the first set past the limit collapses it to the exact
/// `Overflowed` sentinel.
#[test]
pub(super) fn map_keyset_cell_bytes_are_frozen() -> Result<()> {
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
            keyset_limit: 2,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());

    // Event 1: keys 1 and 2 fill the limit-2 keyset — a two-key Tracked frame.
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
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("the committed sets must have written a keyset cell");
    };
    // Golden literal on purpose — independent of `tracked_frame`, so a helper
    // bug and a codec bug can't drift together.
    assert_eq!(
        &bytes[..],
        [
            0, 0, 0, 0, 2, 0, 0, 0, 8, 0x80, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0x80, 0, 0, 0, 0, 0,
            0, 2
        ],
        "the two-key Tracked frame is frozen (tag, count, lengths, coordinates, order, address)"
    );

    // Event 2: key 3 exceeds the limit → the Overflowed sentinel.
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.set(&3, Value::from(3_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, keyset_cell().as_ref())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("the overflowing set must have written a keyset cell");
    };
    assert_eq!(
        &bytes[..],
        OVERFLOWED_FRAME,
        "the first set past the limit writes the Overflowed sentinel"
    );
    Ok(())
}
