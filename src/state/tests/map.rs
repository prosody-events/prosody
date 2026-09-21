//! Map stream plans select bounded coordinate and range reads.

use super::*;

/// Binds `map_state(name)` on `session` and fully drains its `stream(dir)`.
/// Called with a fresh (clean-overlay) session so every read falls through to
/// the underlying store.
pub(super) async fn drain_map_stream(
    session: &KeyedStateSession<CountingBackend, MemoryLoader<Value>>,
    name: &str,
    dir: Direction,
) -> Result<Vec<(i64, Value)>> {
    let handle = map_state::<I64KeyCodec, JsonCodec>(name)
        .bind(session)
        .map_err(|e| eyre!("bind: {e}"))?;
    let mut out = Vec::new();
    let stream = handle.entries(KeyQuery::new(dir));
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// The Map keyset budget tests for the point-get arms (parity of
/// `deque_stream_issues_no_scans`): a never-written map yields nothing and
/// issues zero scans, and a small committed map streams through the batch verb
/// — zero scans, one keyset point-get, and one batch read for the entries, in
/// both directions. The liveness of these zeros is proved by
/// `map_overflowed_stream_issues_one_scan`.
#[test]
pub(super) fn map_stream_issues_no_scans() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("mp"),
            CollectionDef::new(None),
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        // Empty-map arm: absent keyset ⇒ Empty ⇒ no scan (KeysetPresence).
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
        let drained = drain_map_stream(&session, "mp", Direction::Forward).await?;
        assert!(drained.is_empty(), "an unwritten map yields no entries");
        assert_eq!(
            counting.lower_scans(),
            0,
            "streaming an empty map must issue no lower-store scan"
        );

        // Commit a three-entry map — its keyset tracks all three keys.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
        let handle = map_state::<I64KeyCodec, JsonCodec>("mp")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        handle.set(&0, Value::from(10_i64)).await?;
        handle.set(&1, Value::from(11_i64)).await?;
        handle.set(&2, Value::from(12_i64)).await?;
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("mp")?,
        );
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Warm-Tracked arm: pure point gets in key order, both directions.
        for (n, (dir, expected)) in [
            (Direction::Forward, vec![(0_i64, 10_i64), (1, 11), (2, 12)]),
            (Direction::Backward, vec![(2_i64, 12_i64), (1, 11), (0, 10)]),
        ]
        .into_iter()
        .enumerate()
        {
            counting.reset();
            let event = EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - n as u128),
            };
            let session = counting_session(&counting, &dedup, &registry, &state_key, event);
            let out = drain_map_stream(&session, "mp", dir).await?;
            let want: Vec<(i64, Value)> = expected
                .into_iter()
                .map(|(k, v)| (k, Value::from(v)))
                .collect();
            assert_eq!(
                out, want,
                "{dir:?} Tracked stream yields the committed entries"
            );
            assert_eq!(
                counting.lower_scans(),
                0,
                "a Tracked stream must issue no lower scan"
            );
            assert_eq!(
                counting.lower_reads(),
                1,
                "the keyset cell — entries flow through the batch verb, not point get"
            );
            assert_eq!(
                counting.batch_reads(),
                1,
                "the three entries ride one lower batch read"
            );
        }
        Ok(())
    })
}

/// The overflowed-map budget test (the liveness proof for
/// `map_stream_issues_no_scans`): a keyset-disabled map (`keyset_limit = 0`)
/// overflows on its first set and streams through **exactly one** full-section
/// scan (plus the single keyset get — bounds are gone).
#[test]
pub(super) fn map_overflowed_stream_issues_one_scan() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &map_state::<I64KeyCodec, JsonCodec>("mp-of"),
            CollectionDef {
                keyset_limit: 0,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));

        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(0),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
        map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?
            .set(&0, Value::from(99_i64))
            .await?;
        let of_id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("mp-of")?,
        );
        finalize_and_promote(&session, &dedup, Uuid::from_u128(0), &cells, &of_id).await?;

        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(u128::MAX - 100),
        };
        let session = counting_session(&counting, &dedup, &registry, &state_key, event);
        let handle = map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&session)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert!(
            !handle.is_empty().await?,
            "a live overflowed map is not empty"
        );
        counting.reset();
        let out = drain_map_stream(&session, "mp-of", Direction::Forward).await?;
        assert_eq!(
            out,
            vec![(0_i64, Value::from(99_i64))],
            "the overflowed map streams its entry via the scan"
        );
        assert_eq!(
            counting.lower_scans(),
            1,
            "an overflowed map issues exactly one lower scan"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "the single keyset get — no bound reads"
        );

        handle.remove(&0).await?;
        finalize_and_promote(
            &session,
            &dedup,
            Uuid::from_u128(u128::MAX - 100),
            &cells,
            &of_id,
        )
        .await?;
        counting.reset();
        let empty_session = counting_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            EventRef::Message {
                dedup_id: Uuid::from_u128(u128::MAX - 99),
            },
        );
        let empty = map_state::<I64KeyCodec, JsonCodec>("mp-of")
            .bind(&empty_session)
            .map_err(|e| eyre!("bind: {e}"))?;
        assert!(
            empty.is_empty().await?,
            "a removed overflowed entry leaves an empty map"
        );
        assert_eq!(
            counting.presence_scans(),
            1,
            "the empty overflowed map scans once"
        );
        Ok(())
    })
}
