//! Collection projections preserve batched reads and presence.

use super::*;

/// A cold, dense `CELL_BATCH`-entry `Tracked` map streamed to exhaustion issues
/// exactly ONE lower batch read for its entries — a full-width scan chunk is
/// one [`CoordinateBatch`], one lower `get_many`; only the keyset meta cell
/// stays a point read.
/// Falsification: Replace coordinate batch reads with per-key `get`
/// calls. Then `batch_reads` becomes zero and both read-count asserts fail.
#[test]
pub(super) fn map_cold_chunk_is_one_batch_read() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor =
            map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("chunk1");
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &descriptor,
            CollectionDef {
                keyset_limit: 4096,
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("chunk1")?,
        );

        // Seed a dense committed map of exactly one full chunk.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for i in 0..CELL_BATCH.get() as i64 {
            seed.set(&i, Value::from(i))
                .await
                .map_err(|e| eyre!("{e}"))?;
        }
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh cold session, zeroed counters; drain the complete stream.
        counting.reset();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            event,
            ResolveCounter::default(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        let drained: Vec<_> = {
            let stream = handle.entries(KeyQuery::new(Direction::Forward));
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.map_err(|e| eyre!("stream: {e}"))?);
            }
            out
        };
        assert_eq!(drained.len(), CELL_BATCH.get(), "all entries drained");
        assert_eq!(
            counting.batch_reads(),
            1,
            "a cold full-width chunk is ONE lower batch read"
        );
        assert_eq!(
            counting.lower_reads(),
            1,
            "only the keyset meta read is a point read (lower_reads={})",
            counting.lower_reads()
        );
        Ok(())
    })
}

/// `contains_key` answers presence through the dirty overlay
/// (read-your-writes: committed/absent/uncommitted-set/uncommitted-remove/
/// set-after-clear) while never running the resolver — contrasted against
/// `get`, which resolves on the very same collection.
///
/// Falsification: Make `contains_key` always return `Ok(true)`.
/// Then absent-key checks return true and the presence asserts fail.
#[test]
pub(super) fn map_contains_key_presence_without_resolving() -> Result<()> {
    const K1: i64 = 1;
    const K2: i64 = 2;
    const K3: i64 = 3;
    const K_ABSENT: i64 = 99;

    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor =
            map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("presence");
        let mut registry = CollectionDefRegistry::default();
        registry.register(&descriptor, CollectionDef::new(None))?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("presence")?,
        );

        // Seed one committed present key.
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let seed_session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            event,
            ResolveCounter::default(),
        );
        let seed = descriptor
            .bind(&seed_session)
            .map_err(|e| eyre!("bind: {e}"))?;
        seed.set(&K1, Value::from(K1))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&seed_session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh cold session, fresh resolve counter, one live dirty overlay.
        counting.reset();
        let resolves = ResolveCounter::default();
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(2),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            event,
            resolves.clone(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        assert!(
            handle.contains_key(&K1).await.map_err(|e| eyre!("{e}"))?,
            "committed key is present"
        );
        assert!(
            !handle
                .contains_key(&K_ABSENT)
                .await
                .map_err(|e| eyre!("{e}"))?,
            "never-set key is absent"
        );
        handle
            .set(&K2, Value::from(K2))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert!(
            handle.contains_key(&K2).await.map_err(|e| eyre!("{e}"))?,
            "uncommitted set -> true"
        );
        handle.remove(&K1).await.map_err(|e| eyre!("{e}"))?;
        assert!(
            !handle.contains_key(&K1).await.map_err(|e| eyre!("{e}"))?,
            "uncommitted remove -> false (was committed)"
        );
        handle.clear().await.map_err(|e| eyre!("{e}"))?;
        handle
            .set(&K3, Value::from(K3))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert!(handle.contains_key(&K3).await.map_err(|e| eyre!("{e}"))?);
        assert_eq!(resolves.resolves(), 0);
        assert_eq!(counting.presence_reads(), 2);
        assert_eq!(counting.batch_reads(), 0);

        // Contrast: the K3 cell IS resolvable, so the zero above is a real skip.
        assert!(handle.get(&K3).await.map_err(|e| eyre!("{e}"))?.is_some());
        assert!(
            resolves.resolves() >= 1,
            "get resolves; contains_key did not"
        );
        Ok(())
    })
}

/// Proves that key scans do not resolve cell values.
///
/// Falsification: Route `MapHandle::keys` through the resolving stream.
/// Then the resolver count becomes nonzero and the count assert fails.
#[test]
pub(super) fn map_keys_no_resolve() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        // Tracked arm: keyset_limit >= n keeps the map Tracked; contrast get().
        map_keys_drain_resolves(4096, 6, true).await?;
        // Degrade arm: keyset_limit < n overflows → the full-section scan.
        map_keys_drain_resolves(2, 6, false).await?;
        Ok(())
    })
}

/// Seeds a dense `n`-entry committed map at `keyset_limit`, then over a fresh
/// cold session drains `keys()` in both directions and asserts the resolver ran
/// zero times. With `get_contrast`, also asserts a `get()` on a present key
/// resolves — so the zero above is a real skip on a resolvable cell.
pub(super) async fn map_keys_drain_resolves(
    keyset_limit: usize,
    n: usize,
    get_contrast: bool,
) -> Result<()> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("kz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            keyset_limit,
            ..CollectionDef::new(None)
        },
    )?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("kz")?,
    );

    // Seed a dense committed map (a blind `set` never resolves).
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let session = resolve_session(
        &counting,
        &dedup,
        &registry,
        &state_key,
        event,
        ResolveCounter::default(),
    );
    let seed = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..n {
        let key = i64::try_from(i)?;
        seed.set(&key, Value::from(key))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

    // Fresh cold session, zeroed resolve counter; drain keys() both directions.
    counting.reset();
    let resolves = ResolveCounter::default();
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(2),
    };
    let session = resolve_session(
        &counting,
        &dedup,
        &registry,
        &state_key,
        event,
        resolves.clone(),
    );
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    assert!(!handle.is_empty().await.map_err(|e| eyre!("{e}"))?);

    for dir in [Direction::Forward, Direction::Backward] {
        let drained: Vec<i64> = {
            let stream = handle.keys(KeyQuery::new(dir));
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.map_err(|e| eyre!("keys: {e}"))?);
            }
            out
        };
        let mut expected: Vec<i64> = (0..i64::try_from(n)?).collect();
        if dir == Direction::Backward {
            expected.reverse();
        }
        assert_eq!(
            drained, expected,
            "keys() enumerates every present key in order"
        );
    }
    assert_eq!(
        resolves.resolves(),
        0,
        "is_empty and keys resolve nothing on either arm"
    );

    if get_contrast {
        assert!(handle.get(&0).await.map_err(|e| eyre!("{e}"))?.is_some());
        assert!(resolves.resolves() >= 1, "get resolves; keys() did not");
    }
    Ok(())
}
