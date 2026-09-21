//! Map membership and result limits with absent values.

use super::*;

/// Map key-scan presence: over a directly-seeded map whose keyset frame
/// over-reports a TTL-expired coordinate, `keys()` yields exactly the present
/// keys in order across BOTH arms. The **tracked** arm (a `Tracked` keyset
/// within the limit) lists every key `0..n` — including the holes — yet its
/// presence check skips a coordinate the store no longer holds; the
/// **degrade** arm (an `Overflowed` keyset) never sees an expired row in
/// `raw_scan`. `keys()` and `stream()` agree on the live key set in both.
/// Seeded directly — the only way to reach an over-reporting keyset the handle
/// never produces.
pub(crate) async fn run_map_key_scan_holes(shape: MapKeyHoles) -> Result<bool> {
    use bytes::Bytes;

    let n = shape.cells.len();
    let all_keys: Vec<i64> = (0..n as i64).collect();
    let present: Vec<i64> = shape
        .cells
        .iter()
        .enumerate()
        .filter_map(|(i, cell)| cell.map(|_| i as i64))
        .collect();
    let reversed: Vec<i64> = present.iter().rev().copied().collect();

    // Tracked lists every key (0..n) — the over-report; Overflowed degrades to
    // the full-section scan. Both seed the same present entries.
    let tracked = Bytes::from(tracked_frame(&all_keys));
    let overflowed = Bytes::from(OVERFLOWED_FRAME.to_vec());
    for keyset_frame in [tracked, overflowed] {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
        let (registry, collection_ref) = registry_and_ref(
            &descriptor,
            "mp",
            &state_key,
            CollectionDef {
                keyset_limit: 4096,
                ..CollectionDef::new(None)
            },
        )?;
        let store = MemoryCellStore::new(cells.clone());

        let mut seed = vec![(keyset_cell(), Some(keyset_frame))];
        for (i, cell) in shape.cells.iter().enumerate() {
            if let Some(value) = cell {
                let coordinate = I64KeyCodec::encode(&(i as i64));
                let bytes = Bytes::from(serde_json::to_vec(&Value::from(*value))?);
                seed.push((entry_cell_for(&coordinate), Some(bytes)));
            }
        }
        store.write_resolved(&collection_ref, &seed, &[]).await?;

        let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        // Presence-only: holed (absent) keys never appear, in either direction.
        if collect_map_keys(&handle, Direction::Forward).await? != present {
            return Ok(false);
        }
        if collect_map_keys(&handle, Direction::Backward).await? != reversed {
            return Ok(false);
        }
        // keys() ↔ stream() parity on the live key set.
        let stream_keys: Vec<i64> = collect_map(&handle, Direction::Forward)
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        if stream_keys != present {
            return Ok(false);
        }
        for dir in [Direction::Forward, Direction::Backward] {
            let all = collect_map(&handle, dir).await?;
            let limit = NonZeroUsize::new(all.len() / 2 + 1).unwrap_or(NonZeroUsize::MIN);
            let expected: Vec<_> = all.into_iter().take(limit.get()).collect();
            if drain(handle.entries(KeyQuery::new(dir).limit(limit))).await? != expected
                || drain(handle.keys(KeyQuery::new(dir).limit(limit))).await?
                    != expected.iter().map(|(key, _)| *key).collect::<Vec<_>>()
            {
                return Ok(false);
            }
        }
        if handle.is_empty().await? != present.is_empty() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A directly-seeded map with a possibly-stale keyset: keys `0..cells.len()`,
/// each `Some(v)` a present entry and `None` a hole — a TTL-expired entry the
/// keyset frame still over-reports (a later `set` refreshes the frame without
/// pruning expired coordinates, so staleness persists). Seeded straight into
/// the store (never produced by the handle, which keeps the frame ≡ its
/// entries) to pin that `keys()` is presence-only across BOTH arms.
#[derive(Clone, Debug)]
pub(crate) struct MapKeyHoles {
    cells: Vec<Option<u8>>,
}

impl Arbitrary for MapKeyHoles {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            cells: capped_vec(g, MAX_MAP_KEY_WINDOW),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.cells.shrink().map(|cells| Self { cells }))
    }
}
