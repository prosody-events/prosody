//! Key projections, corrupt values, and bounded fetches.

use super::*;
use crate::state::tests::counting_session;

/// Corrupt-coordinate classification: a stored entry whose coordinate does not
/// decode as the collection's key codec (here 3 bytes where `I64KeyCodec`
/// requires 8) surfaces from `entries` as [`CellStateError::Key`] classified
/// `Permanent` — one skippable row, never Terminal — the only error arm no
/// well-formed trace can reach.
#[test]
pub(super) fn map_stream_classifies_corrupt_coordinate_permanent() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};
    use crate::state::cell_key::Coordinate;
    use crate::state::descriptor::CellStateError;
    use crate::state::descriptor::map::MapStateError;
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone());

    // An `Overflowed` keyset forces the full-section scan, which reaches the
    // corrupt entry — a 3-byte coordinate that cannot decode as `I64KeyCodec`
    // (which needs 8 bytes).
    let corrupt = entry_cell_for(&Coordinate::from_bytes(vec![0x80, 0x00, 0x00]));
    block_on(store.write_resolved(
        &collection_ref,
        &[
            (keyset_cell(), Some(Bytes::from(OVERFLOWED_FRAME.to_vec()))),
            (
                corrupt,
                Some(Bytes::from(serde_json::to_vec(&Value::from(0_u8))?)),
            ),
        ],
        &[],
    ))?;

    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let error = block_on(async {
        let stream = handle.entries(KeyQuery::new(Direction::Forward));
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            if let Err(error) = item {
                return Ok(error);
            }
        }
        bail!("a corrupt coordinate must end the stream with an error")
    })?;
    assert!(matches!(error, MapStateError::Cell(CellStateError::Key(_))));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Presence and value reads diverge on a present-but-undecodable value: the
/// documented contract that `contains_key` and `keys()` answer about the
/// *cell*, while `get` and `entries` answer about the *value* and surface its
/// decode failure. A cell at a valid coordinate holds bytes that are not valid
/// JSON, so `contains_key` is `true` and `keys()` yields the key, yet `get`
/// errors `Permanent` and `entries` ends on that same error — across BOTH
/// keyset arms (tracked point-get and degrade full-section scan), since both
/// drop the value before any decode. The property generators only ever write
/// decodable values, so this divergence is unreachable there and needs a direct
/// seed.
#[test]
pub(super) fn map_presence_survives_an_undecodable_value() -> Result<()> {
    use crate::error::{ClassifyError, ErrorCategory};
    use bytes::Bytes;
    use futures::executor::block_on;

    // A valid `I64KeyCodec` coordinate (8 bytes) whose value bytes are not
    // valid JSON — present to a presence read, undecodable to a value read.
    let key = 0_i64;
    let coordinates = [key, key + 1, key + 2].map(|key| I64KeyCodec::encode(&key));
    let bad_value = Bytes::from(vec![0xFF, 0xFF]);

    // Tracked lists the key; Overflowed degrades to the full-section scan. Both
    // reach the same present-but-undecodable cell.
    let tracked = Bytes::from(tracked_frame(&[key - 1, key, key + 1, key + 2]));
    let overflowed = Bytes::from(OVERFLOWED_FRAME.to_vec());
    for (tracked_route, keyset_frame) in [(true, tracked), (false, overflowed)] {
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
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        block_on(counting.write_resolved(
            &collection_ref,
            &[
                (keyset_cell(), Some(keyset_frame)),
                (entry_cell_for(&coordinates[0]), Some(bad_value.clone())),
                (entry_cell_for(&coordinates[1]), Some(bad_value.clone())),
                (entry_cell_for(&coordinates[2]), Some(bad_value.clone())),
            ],
            &[],
        ))?;

        counting.reset();
        let session = counting_session(&counting, &dedup, &registry, &state_key, read_event(0));
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        block_on(async {
            assert!(!handle.is_empty().await?);
            assert_eq!(counting.scan_hint(), CELL_BATCH.get());
            assert_eq!(counting.visible_point_reads(), 0);
            assert_eq!(counting.batch_reads(), 0);
            assert_eq!(counting.presence_reads(), 0);
            assert_eq!(counting.presence_scans(), 1);
            counting.reset();
            assert!(
                handle.contains_key(&key).await.map_err(|e| eyre!("{e}"))?,
                "contains_key answers about the cell, not the value"
            );
            assert_eq!(counting.presence_reads(), 1);
            assert_eq!(counting.batch_reads(), 0);
            assert_eq!(counting.presence_scans(), 0, "contains_key does not scan");
            counting.reset();
            assert_eq!(
                collect_map_keys(&handle, Direction::Forward).await?,
                vec![key, key + 1, key + 2],
                "keys() yields the keys of undecodable-value cells"
            );
            assert_presence_route_calls(&counting, tracked_route);
            counting.reset();
            let keys = handle
                .keys(KeyQuery::new(Direction::Forward).limit(NonZeroUsize::MIN.saturating_add(1)));
            assert_eq!(drain(keys).await?, vec![key, key + 1]);
            assert_limited_fetch(&counting, tracked_route, &[4], CELL_BATCH.get());
            if !tracked_route {
                assert_eq!(counting.scan_rows(), 2);
            }

            counting.reset();
            let entries =
                handle.entries(KeyQuery::new(Direction::Forward).limit(NonZeroUsize::MIN));
            assert!(drain(entries).await.is_err());
            // Four keys: the second chunk `[key, key + 1]` satisfies the limit while
            // `key + 2` remains unread, so `[1, 2]` proves the schedule stops at the
            // limit and not at exhaustion.
            assert_limited_fetch(&counting, tracked_route, &[1, 2], 1);

            for dir in [Direction::Forward, Direction::Backward] {
                counting.reset();
                let keys = handle.keys(KeyQuery::new(dir).from(&(key + 1)).to(&(key + 1)));
                assert_eq!(drain(keys).await?, vec![key + 1]);
                assert_limited_fetch(&counting, tracked_route, &[1], 0);
                if !tracked_route {
                    assert_eq!(counting.scan_rows(), 1);
                }
            }

            // Value reads surface the decode failure as `Permanent`.
            let got = handle.get(&key).await;
            assert!(got.is_err(), "get must surface the value decode failure");
            if let Err(error) = got {
                assert_eq!(error.classify_error(), ErrorCategory::Permanent);
            }
            assert!(
                drain(handle.entries(KeyQuery::new(Direction::Forward)))
                    .await
                    .is_err()
            );
            Ok::<_, color_eyre::Report>(())
        })?;
    }
    Ok(())
}

pub(super) fn assert_limited_fetch(
    counting: &CountingCellStore<MemoryCellStore>,
    tracked: bool,
    widths: &[usize],
    hint: usize,
) {
    if tracked {
        assert_eq!(counting.batch_widths(), widths);
    } else {
        assert_eq!(counting.scan_hint(), hint);
    }
}

pub(super) fn assert_presence_route_calls(
    counting: &CountingCellStore<MemoryCellStore>,
    tracked_route: bool,
) {
    assert_eq!(counting.presence_reads(), usize::from(tracked_route));
    assert_eq!(counting.presence_scans(), usize::from(!tracked_route));
    assert_eq!(counting.batch_reads(), 0);
    assert_eq!(counting.lower_scans(), 0);
}
