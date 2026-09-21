//! Stream demand bounds reads and message resolution.

use super::*;

/// A dense stream-laziness case: a collection of `n` entries drained
/// `stream(..).take(k)`, with `n` on the deque's point-get arm (`≤ 128`) and
/// far above `k`, so "fetch/resolve only the consumed prefix" is a strictly
/// stronger claim than "fetch everything".
#[derive(Clone, Copy, Debug)]
pub(super) struct StreamPrefix {
    n: usize,
    k: usize,
}

impl Arbitrary for StreamPrefix {
    fn arbitrary(g: &mut Gen) -> Self {
        // 48..=127: dense, ≤ DEQUE_POINT_ITERATION_MAX (128) so the deque stays
        // on the chunked point-get arm, and always > k + one chunk width (16) so the
        // "materialize everything" defect is observable.
        let n = 48 + usize::arbitrary(g) % 80;
        // 1..=12: well under one chunk width and far under n.
        let k = 1 + usize::arbitrary(g) % 12;
        Self { n, k }
    }
}

/// Mints a session over `counting` carrying a [`ResolveCounter`] loader, so a
/// stream-laziness test can bound resolutions independently of fetches.
pub(super) fn resolve_session(
    counting: &CountingCellStore<MemoryCellStore>,
    dedup: &MemoryDeduplicationStore,
    registry: &Arc<CollectionDefRegistry>,
    state_key: &StateKey,
    event: EventRef,
    loader: ResolveCounter,
) -> KeyedStateSession<CountingBackend, ResolveCounter> {
    session_with_loader(counting, dedup, registry, state_key, event, loader)
}

/// The stream-laziness property (map): a `stream(dir).take(k)` over a **dense**
/// `n`-entry `Tracked` map is genuinely incremental. It issues at most one
/// batch read beyond `k`, because entries flow through the batch verb and only
/// the keyset meta cell is a point read. It resolves at most `k + CELL_BATCH`
/// values, never the whole `n`-entry collection. The counting store bounds the
/// fetches and the counting resolver bounds the resolutions. Both counters sit
/// at the lowest layer, so nothing masks a materialization.
///
/// Falsification: Make the coordinate source consume all tracked keys.
/// Then the read and resolver counts exceed their bounds, and both asserts
/// fail. A larger `CELL_BATCH` cannot falsify: the bound moves with it.
pub(super) async fn run_map_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            // ≥ n so the map stays Tracked (the dense point-get arm).
            keyset_limit: 4096,
            ..CollectionDef::new(None)
        },
    )?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("lz")?,
    );

    // Seed a dense committed map of n entries (a blind `set` never resolves).
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

    // Fresh cold session, zeroed counters; drain only the k-prefix.
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
    let taken: Vec<_> = {
        let stream = handle.entries(KeyQuery::new(dir)).take(k);
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| eyre!("stream: {e}"))?);
        }
        out
    };
    assert_eq!(
        taken.len(),
        k.min(n),
        "take(k) yields exactly k.min(n) entries"
    );
    assert!(
        counting.batch_reads() <= k.div_ceil(CELL_BATCH.get()) + 1,
        "a lazy map take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert_eq!(
        counting.lower_reads(),
        1,
        "entries flow through the batch verb, not point get; only the keyset meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + CELL_BATCH.get(),
        "a lazy map take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
}

/// The stream-laziness property (deque): the structural twin of
/// [`run_map_stream_prefix_lazy`] over a dense `n`-entry window on the
/// point-get arm — at most one batch read beyond `k` (entries flow through the
/// batch verb; only the bounds meta cell is a point read) and at most
/// `k + CELL_BATCH` resolved.
///
/// Falsification: Make the coordinate source consume all tracked keys.
/// Then the read and resolver counts exceed their bounds, and both asserts
/// fail. A larger `CELL_BATCH` cannot falsify: the bound moves with it.
pub(super) async fn run_deque_stream_prefix_lazy(n: usize, k: usize, dir: Direction) -> Result<()> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("lz");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&descriptor, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("lz")?,
    );

    // Seed a dense committed window of n entries (a blind `push_back` never
    // resolves).
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
        seed.push_back(Value::from(i64::try_from(i)?))
            .await
            .map_err(|e| eyre!("{e}"))?;
    }
    finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

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
    let taken: Vec<_> = {
        let stream = handle.values(DequeQuery::new(dir)).take(k);
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| eyre!("stream: {e}"))?);
        }
        out
    };
    assert_eq!(
        taken.len(),
        k.min(n),
        "take(k) yields exactly k.min(n) elements"
    );
    assert!(
        counting.batch_reads() <= k.div_ceil(CELL_BATCH.get()) + 1,
        "a lazy deque take(k) issues at most one batch read beyond k (batches={}, k={k}, n={n})",
        counting.batch_reads()
    );
    assert_eq!(
        counting.lower_reads(),
        1,
        "entries flow through the batch verb, not point get; only the bounds meta read remains a \
         point read (lower_reads={})",
        counting.lower_reads()
    );
    assert!(
        resolves.resolves() <= k + CELL_BATCH.get(),
        "a lazy deque take(k) resolves at most k + one chunk (resolves={}, k={k}, n={n})",
        resolves.resolves()
    );
    Ok(())
}

/// A bounded push evicts the opposite end **decode-free**: no value decode, no
/// resolver run for the discarded slot. Over a message-backed
/// (resolver-carrying) deque capped at one slot, a `push_back` that evicts the
/// front resolves **nothing** — a blind push never resolves, so a nonzero count
/// could come only from the eviction path reading the evicted slot.
///
/// Falsification: Replace the eviction clear with a get followed by a clear.
/// Then the evicted slot resolves and the zero-count assert fails.
#[test]
pub(super) fn deque_bounded_eviction_does_not_resolve() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let dedup = MemoryDeduplicationStore::default();
        let cells = MemoryCells::new();
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
        let descriptor = deque_state::<WithResolver<JsonCodec, CountingResolver>>("cap");
        let mut registry = CollectionDefRegistry::default();
        registry.register(
            &descriptor,
            CollectionDef {
                capacity: Some(NonZeroUsize::MIN),
                ..CollectionDef::new(None)
            },
        )?;
        let registry = Arc::new(registry);
        let counting = CountingCellStore::new(MemoryCellStore::new(cells.clone()));
        let id = CollectionId::new(
            state_key.clone(),
            StateType::Application,
            StateName::try_new("cap")?,
        );

        // Seed a committed one-element window (a blind `push_back` never resolves).
        let seed_event = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session = resolve_session(
            &counting,
            &dedup,
            &registry,
            &state_key,
            seed_event,
            ResolveCounter::default(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        handle
            .push_back(Value::from(1_u8))
            .await
            .map_err(|e| eyre!("{e}"))?;
        finalize_and_promote(&session, &dedup, Uuid::from_u128(1), &cells, &id).await?;

        // Fresh event: push a second value, evicting the front (the capacity is 1).
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
        handle
            .push_back(Value::from(2_u8))
            .await
            .map_err(|e| eyre!("{e}"))?;
        assert_eq!(
            resolves.resolves(),
            0,
            "a bounded push resolves nothing — the eviction is decode/resolver-free"
        );

        // Only now (after the assert) read the survivor, which does resolve.
        let survivor = handle.peek_front().await.map_err(|e| eyre!("{e}"))?;
        assert_eq!(
            survivor,
            Some(Value::from(2_u8)),
            "the newest element survives"
        );
        Ok(())
    })
}

/// The stream-laziness property: both collections' `stream(dir).take(k)` are
/// genuinely incremental — the fetch/resolve budget tracks the consumed prefix,
/// not the collection size. A `QuickCheck` property over dense `(n, k)` in both
/// directions.
#[test]
pub(super) fn stream_take_is_lazy() {
    fn property(input: StreamPrefix) -> Result<bool> {
        let StreamPrefix { n, k } = input;
        TEST_RUNTIME.block_on(async move {
            for dir in [Direction::Forward, Direction::Backward] {
                run_map_stream_prefix_lazy(n, k, dir).await?;
                run_deque_stream_prefix_lazy(n, k, dir).await?;
            }
            Ok(true)
        })
    }
    QuickCheck::new().quickcheck(property as fn(StreamPrefix) -> Result<bool>);
}

/// The `StreamYieldFree` interleaving property (map): random
/// `next()`/mutator interleavings on one live session never deadlock or error
/// and stay weakly consistent with the init snapshot. A fresh `current_thread`
/// runtime with a time driver per iteration powers the hang-guard; no sleeps.
#[test]
pub(super) fn map_stream_interleave_is_yield_free() {
    fn property(input: MapInterleave) -> Result<bool> {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| eyre!("runtime: {e}"))?
            .block_on(run_map_stream_interleave(input))
    }
    QuickCheck::new().quickcheck(property as fn(MapInterleave) -> Result<bool>);
}

/// The `StreamYieldFree` interleaving property (deque): the structural twin.
#[test]
pub(super) fn deque_stream_interleave_is_yield_free() {
    fn property(input: DequeInterleave) -> Result<bool> {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|e| eyre!("runtime: {e}"))?
            .block_on(run_deque_stream_interleave(input))
    }
    QuickCheck::new().quickcheck(property as fn(DequeInterleave) -> Result<bool>);
}
