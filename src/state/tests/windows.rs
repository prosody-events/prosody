//! Deque stream plans respect the stored index window.

use super::*;

/// Binds `deque_state(name)` on `session` and fully drains its `stream(dir)`,
/// returning the yielded values. Called with a fresh (clean-overlay) session so
/// every read falls through to the underlying store.
pub(super) async fn drain_deque_stream(
    session: &KeyedStateSession<CountingBackend, MemoryLoader<Value>>,
    name: &str,
    dir: Direction,
) -> Result<Vec<Value>> {
    let handle = deque_state::<JsonCodec>(name)
        .bind(session)
        .map_err(|e| eyre!("bind: {e}"))?;
    let mut out = Vec::new();
    let stream = handle.values(DequeQuery::new(dir));
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Seeds a committed deque window of `width` entries named `name` directly into
/// `counting`'s lower store, valued by its own index so a stream-order
/// assertion is possible. It also seeds **one entry at index `width`, which
/// sits deliberately outside the seeded `[0, width)` bounds**.
///
/// That extra row makes the wide arm's range bound falsifiable. The entries
/// section then holds a row that the window does not. A scan over the whole
/// section, instead of exactly `[head, tail − 1]`, would yield that row.
pub(super) async fn seed_wide_deque(
    counting: &CountingCellStore<MemoryCellStore>,
    state_key: &StateKey,
    name: &str,
    width: usize,
) -> Result<()> {
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new(name)?,
    );
    let wide_ref = CollectionRef::new(id, None);
    let mut seeded = vec![(
        deque::meta_cell(),
        Some(Bytes::from(deque::seed_frame(0, i64::try_from(width)?))),
    )];
    // `0..=width`: the last entry sits past `tail`, outside the window.
    for i in 0..=width {
        let index = i64::try_from(i)?;
        seeded.push((
            deque::entry_cell_for(&I64KeyCodec::encode(&index)),
            Some(Bytes::from(serde_json::to_vec(&Value::from(index))?)),
        ));
    }
    counting.write_resolved(&wide_ref, &seeded, &[]).await?;
    Ok(())
}

/// Sub-threshold deque iteration streams through the batch verb: a small
/// committed deque issues **zero** lower-store scans, one bounds point-get, and
/// one batch read for the entries, in both directions. The test then hands the
/// same fixture to [`assert_wide_deque_scan_is_window_bounded`], which tests
/// the fallback arm.
#[test]
pub(super) fn deque_stream_issues_no_scans() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(&deque_state::<JsonCodec>("dq"), CollectionDef::new(None))?;
        registry.register(
            &deque_state::<JsonCodec>("dq-wide"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        // One committed event of pushes and pops: the deque reads [0, 1, 2].
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
        let handle = deque_state::<JsonCodec>("dq")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.push_back(Value::from(1_u8)).await?;
        handle.push_back(Value::from(2_u8)).await?;
        handle.push_back(Value::from(9_u8)).await?;
        handle.push_front(Value::from(0_u8)).await?;
        assert_eq!(handle.pop_back().await?, Some(Value::from(9_u8)));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("dq")?,
        );
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Stream in both directions; each is a pure sequence of point gets.
        for (n, (dir, expected)) in [
            (Direction::Forward, [0_u8, 1, 2]),
            (Direction::Backward, [2_u8, 1, 0]),
        ]
        .into_iter()
        .enumerate()
        {
            counting.reset();
            let event = EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - n as u128),
            };
            let session = counting_session(&counting, &dedup, &registry, &state_key, event);
            let out = drain_deque_stream(&session, "dq", dir).await?;
            let expected: Vec<Value> = expected.into_iter().map(Value::from).collect();
            assert_eq!(out, expected, "{dir:?} stream yields the committed window");
            assert_eq!(
                counting.lower_scans(),
                0,
                "a sub-threshold stream must issue no lower scan"
            );
            assert_eq!(
                counting.lower_reads(),
                1,
                "the bounds cell — entries flow through the batch verb, not point get"
            );
            assert_eq!(
                counting.batch_reads(),
                1,
                "the window's entries ride one lower batch read"
            );
        }

        assert_wide_deque_scan_is_window_bounded(&counting, &dedup, &registry, &state_key).await
    })
}

/// The wide-window companion of [`deque_stream_issues_no_scans`]. A
/// directly-seeded window one entry wider than
/// [`deque::DEQUE_POINT_ITERATION_MAX`] falls back to exactly one lower scan.
/// That count proves the sub-threshold zero is a live counter. The window
/// bounds that scan, not the section.
///
/// [`seed_wide_deque`] plants one row past `tail`. A scan over the whole
/// section, rather than `[head, tail − 1]`, would yield that row. This helper
/// drains both directions, because a regression that keeps the limit but drops
/// the edges hides forward and shows backward. Forward, the limit stops the
/// walk short of the extra row. Backward, the extra row becomes the first item.
pub(super) async fn assert_wide_deque_scan_is_window_bounded(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
) -> Result<()> {
    let width = deque::DEQUE_POINT_ITERATION_MAX + 1;
    seed_wide_deque(counting, state_key, "dq-wide", width).await?;
    let ascending: Vec<Value> = (0..width)
        .map(i64::try_from)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(Value::from)
        .collect();

    for (n, dir) in [Direction::Forward, Direction::Backward]
        .into_iter()
        .enumerate()
    {
        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 2 - n as u128),
        };
        let session = counting_session(counting, dedup, registry, state_key, event);
        let drained = drain_deque_stream(&session, "dq-wide", dir).await?;
        let mut expected = ascending.clone();
        if dir == Direction::Backward {
            expected.reverse();
        }
        assert_eq!(
            drained, expected,
            "the wide {dir:?} scan streams exactly the window's entries, in order"
        );
        assert_eq!(
            counting.lower_scans(),
            1,
            "a wide window pays exactly one lower scan"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the wide arm reads only the bounds cell"
        );
    }
    Ok(())
}
