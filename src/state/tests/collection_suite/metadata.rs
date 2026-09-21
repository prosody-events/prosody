//! Deque metadata and index window invariants.

use super::*;

/// Durable meta-frame golden (Deque): after real pushes `commit()`, the raw
/// bounds cell sits at its frozen address — `Meta` section, *empty*
/// coordinate — and
/// stores exactly `head ‖ tail` as two plain big-endian `i64`s. The pair
/// codec's own goldens pin that frame in isolation; this pins the deque's
/// *binding* to it: the codec choice, the head-first tuple order, and the unit
/// address (a swapped tuple, a different meta codec, or a moved address all go
/// red here while every self-consistent trace stays green).
#[test]
pub(super) fn deque_meta_cell_bytes_are_frozen() -> Result<()> {
    use crate::state::descriptor::deque::meta_cell;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, _) = registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let event = read_event(0);
    let session = make_session(&cells, &dedup, &registry, &state_key, event);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    // `push_back` then `push_front`: the window becomes `head = -1, tail = 1`,
    // so the frame crosses the sign boundary the plain-BE encoding must keep.
    block_on(async {
        handle.push_back(Value::from(7_u8)).await?;
        handle.push_front(Value::from(9_u8)).await?;
        handle.commit().await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    let store = MemoryCellStore::new(cells.clone());
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("dq")?,
    );
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, &id, &meta_cell())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("bounds cell missing at the frozen address");
    };
    assert_eq!(
        &bytes[..],
        [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 1
        ],
        "meta frame must be head ‖ tail as plain big-endian i64s"
    );
    Ok(())
}

/// Deque clear, pinned at the physical grain a `VecDeque` model cannot reach
/// (the reset window makes stale rows unreachable through the handle, so only
/// raw reads can observe them):
///
/// * **Index-space reset** — `clear()` erases the window cell, so the next push
///   starts a fresh window at index 0. A preserved (non-reset) index space
///   would read `head = 2 ‖ tail = 3` here; the reset reads `head = 0 ‖ tail =
///   1`.
/// * **Committed erasure** — a committed `clear()` physically erases the
///   pre-clear entry rows. The row outside the reused window must read absent;
///   a lost clear leg would leave it standing as an orphan the window never
///   addresses (unbounded leaked storage).
#[test]
pub(super) fn deque_clear_resets_the_index_space() -> Result<()> {
    use crate::state::descriptor::deque::{entry_cell_for, meta_cell};
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();

    // Event 1: two pushes, committed — the window becomes [0, 2).
    let event1 = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event1);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.push_back(Value::from(1_u8)).await?;
        handle.push_back(Value::from(2_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event1), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    // Event 2: clear then push, committed — the window resets to [0, 1).
    let event2 = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = make_session(&cells, &dedup, &registry, &state_key, event2);
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    block_on(async {
        handle.clear().await?;
        handle.push_back(Value::from(9_u8)).await?;
        finalize_and_promote(&session, &dedup, event_dedup(event2), &cells, id).await?;
        Ok::<_, color_eyre::Report>(())
    })?;

    let store = MemoryCellStore::new(cells.clone());
    let Some(bytes) = block_on(async {
        CellRead::<Values>::read(&store, id, &meta_cell())
            .await
            .map(|(committed, _)| committed)
    })?
    .into_inner() else {
        bail!("bounds cell missing after the committed clear-then-push");
    };
    assert_eq!(
        &bytes[..],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        "clear resets the index space: the reused window is head = 0 ‖ tail = 1"
    );

    // The physical erasure half: index 1 sat outside the reused window, so
    // only the clear's gap erase removes it — a stale committed row here is
    // the leak the API can never surface.
    let stale = entry_cell_for(&I64KeyCodec::encode(&1));
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, &stale)
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner(),
        None,
        "the committed clear must physically erase the out-of-window row"
    );
    let reused = entry_cell_for(&I64KeyCodec::encode(&0));
    assert_eq!(
        block_on(async {
            CellRead::<Values>::read(&store, id, &reused)
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner(),
        Some(Bytes::from(serde_json::to_vec(&Value::from(9_u8))?)),
        "the reused index holds exactly the post-clear push"
    );
    Ok(())
}

/// Endpoint-peek parity holds even at the over-wide window the operations can
/// never reach: `[i64::MIN, 0)` is ordered (so `Window::new` admits it) yet its
/// span exceeds `usize`, so `get`'s length check errors `IndexOverflow`. A peek
/// reads the endpoint slot directly and must take the same overflow path —
/// `peek == get` is total, not "except at a degenerate window". Seeded directly
/// because reaching `head = i64::MIN` would need 2^63 pushes.
#[test]
pub(super) fn deque_peeks_match_get_on_an_over_wide_window() -> Result<()> {
    use crate::state::descriptor::deque::{DequeStateError, MetaDecodeError, meta_cell};
    use bytes::Bytes;
    use futures::executor::block_on;

    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone());
    block_on(store.write_resolved(
        &collection_ref,
        &[(
            meta_cell(),
            Some(Bytes::from(deque::seed_frame(i64::MIN, 0))),
        )],
        &[],
    ))?;

    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    let overflows = |result| {
        matches!(
            result,
            Err(DequeStateError::Meta(MetaDecodeError::IndexOverflow))
        )
    };
    block_on(async {
        assert!(overflows(handle.get(0).await), "get(0) must overflow");
        assert!(
            overflows(handle.peek_front().await),
            "peek_front must match get"
        );
        assert!(
            overflows(handle.peek_back().await),
            "peek_back must match get"
        );
        Ok::<_, color_eyre::Report>(())
    })
}

/// Regression guard for the over-wide push paths: `push_back` on the
/// `[i64::MIN, 0)` window — whose span `Window::len` cannot measure — must
/// succeed in BOTH modes, not error. Unbounded, the push never reads the
/// length (evict 0) and extends the window to `[i64::MIN, 1)`. Bounded, the
/// unmeasurable span trims `TRIM_MAX` toward the cap rather than failing,
/// advancing `head` by `TRIM_MAX` to `[i64::MIN + TRIM_MAX, 1)` — the first
/// convergence push a cap exists to drive. A capacity as large as `usize::MAX`
/// sits so far above the window's `i64::MAX`-lower-bounded length that the
/// eviction arithmetic yields zero, so the window extends to `[i64::MIN, 1)`
/// — live in-capacity slots must not be erased. Restoring a
/// fallible `window.len()?` before the capacity check (the bug this fixes)
/// reddens the unbounded case; failing instead of trimming reddens the bounded
/// case; evicting `TRIM_MAX` unconditionally on the unmeasurable span reddens
/// the huge-cap case. Seeded directly because reaching `head = i64::MIN` would
/// need 2^63 pushes.
#[test]
pub(super) fn deque_push_on_an_over_wide_window_succeeds() -> Result<()> {
    use crate::state::descriptor::deque::meta_cell;
    use bytes::Bytes;
    use futures::executor::block_on;

    /// Seeds `[i64::MIN, 0)`, runs one `push_back` under `cap`, commits, and
    /// asserts the committed bounds are exactly `(want_head, want_tail)`.
    fn push_and_expect_bounds(
        cap: Option<NonZeroUsize>,
        want_head: i64,
        want_tail: i64,
    ) -> Result<()> {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor = deque_state::<JsonCodec>("dq");
        let (registry, collection_ref) = registry_and_ref(
            &descriptor,
            "dq",
            &state_key,
            CollectionDef {
                capacity: cap,
                ..CollectionDef::new(None)
            },
        )?;
        let store = MemoryCellStore::new(cells.clone());
        let id = collection_ref.id();
        block_on(store.write_resolved(
            &collection_ref,
            &[(
                meta_cell(),
                Some(Bytes::from(deque::seed_frame(i64::MIN, 0))),
            )],
            &[],
        ))?;

        let event = read_event(0);
        let session = make_session(&cells, &dedup, &registry, &state_key, event);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        block_on(async {
            handle.push_back(Value::from(1_u8)).await?;
            handle.commit().await?;
            Ok::<_, color_eyre::Report>(())
        })?;

        let Some(bounds) = block_on(async {
            CellRead::<Values>::read(&store, id, &meta_cell())
                .await
                .map(|(committed, _)| committed)
        })?
        .into_inner() else {
            bail!("bounds cell missing after the over-wide push");
        };
        let head = i64::from_be_bytes(bounds[0..8].try_into()?);
        let tail = i64::from_be_bytes(bounds[8..16].try_into()?);
        assert_eq!(
            (head, tail),
            (want_head, want_tail),
            "over-wide push must move the window as expected"
        );
        Ok(())
    }

    // Unbounded: evict 0, so the window just extends to `[i64::MIN, 1)`.
    push_and_expect_bounds(None, i64::MIN, 1)?;
    // Bounded: the unmeasurable span trims `TRIM_MAX`, advancing `head`.
    let cap = NonZeroUsize::new(2).ok_or_else(|| eyre!("2 is nonzero"))?;
    let trim = i64::try_from(deque::TRIM_MAX)?;
    push_and_expect_bounds(Some(cap), i64::MIN + trim, 1)?;
    // Capacity `usize::MAX`: the lower-bounded length (`i64::MAX as usize`,
    // 2^63 − 1 on a 64-bit target) sits below the cap, so the eviction
    // arithmetic yields zero and the window just extends to `[i64::MIN, 1)`.
    // Skipped on a 32-bit target: there `i64::MAX as usize` truncates to
    // `usize::MAX`, so the lower bound equals any cap and the arithmetic can
    // never reach zero — the zero-eviction outcome this case checks is
    // unreachable there, not wrong.
    if cfg!(target_pointer_width = "64") {
        let huge = NonZeroUsize::new(usize::MAX).ok_or_else(|| eyre!("MAX is nonzero"))?;
        push_and_expect_bounds(Some(huge), i64::MIN, 1)?;
    }
    Ok(())
}
