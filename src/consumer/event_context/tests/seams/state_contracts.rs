use super::*;

/// A synthetic error classifying `Terminal`, to pin the boundary fold.
#[derive(Debug, Error)]
#[error("synthetic terminal error")]
struct SyntheticTerminal;

impl ClassifyError for SyntheticTerminal {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

/// The single boundary fold maps a lower-layer `Terminal` to `Transient` and
/// preserves `Permanent`/`Transient` — the state layer never surfaces
/// `Terminal`. `ErasedCategory` has no `Terminal` variant, so the folded error
/// is structurally never `Terminal`. Falsify: map `Terminal => Permanent` in
/// [`ErasedStateError::from_classified`] and this observes Permanent.
#[test]
fn never_terminal_fold() {
    let folded = ErasedStateError::from_classified(&SyntheticTerminal);
    assert_eq!(folded.category(), ErasedCategory::Transient);
    assert_eq!(folded.classify_error(), ErrorCategory::Transient);
}

// --- Cursor laziness (against a counting store) -----------------------------

/// The read-counting cell store the cursor-laziness pin drives.
type CountingStore = CountingCellStore<MemoryCellStore<FixedOracle>>;

/// The backend the cursor-laziness pin drives: a memory cell store wrapped in a
/// read-counting decorator, so a single `next()`'s durable reads are bounded.
type CountingBackend = PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, CountingStore>;

/// The context the cursor-laziness pin drives.
type CountingContext =
    MockEventContext<Value, KeyedStateSession<CountingBackend, MemoryLoader<Value>>>;

/// Builds a counting-store-backed context (map `MAP_NAME` registered),
/// returning the context and a clone of the counting store to read its `get`
/// counter.
fn counting_context(registry: CollectionDefRegistry) -> (CountingContext, CountingStore) {
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(
        MemoryCells::new(),
        FixedOracle::committed(),
        registry.clone(),
    ));
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let parts = SessionParts::<CountingBackend, _> {
        cell: counting.clone(),
        dirty: Arc::new(DirtyStore::new()),
        oracle: FixedOracle::committed(),
        loader: MemoryLoader::<Value>::new(),
        registry,
        state_key: StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    };
    let ctx = MockEventContext::<Value>::new().with_session(KeyedStateSession::new(parts));
    (ctx, counting)
}

/// A map cursor is demand-driven: one `next()` issues at most one chunk's worth
/// of durable point reads (plus the single keyset read), never the whole
/// collection. Falsify: eagerly drain the whole typed stream inside the scan
/// generator before yielding — the first `next()`'s durable reads then equal
/// the full seeded size.
#[tokio::test]
async fn map_cursor_is_lazy() -> Result<()> {
    // Seed enough entries that a full drain far exceeds one chunk.
    let entries = CELL_BATCH * 3;
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &map_state::<Utf8KeyCodec, JsonCodec>(MAP_NAME),
        CollectionDef::new(None),
    )?;
    let (ctx, counting) = counting_context(registry);

    let map = ctx
        .map_state(MAP_NAME)
        .map_err(|e| eyre!("vend map: {e}"))?;
    for i in 0..entries {
        map.set(format!("k{i:04}"), json!(i))
            .await
            .map_err(|e| eyre!("seed set: {e}"))?;
    }
    // Commit so the entries are durable committed cells the scan re-reads.
    map.commit().await.map_err(|e| eyre!("commit: {e}"))?;

    counting.reset();
    let cursor = ctx
        .map_state(MAP_NAME)
        .map_err(|e| eyre!("vend map: {e}"))?
        .scan(MapScanConfig::default());
    let first = cursor.next().await.map_err(|e| eyre!("first next: {e}"))?;
    assert!(first.is_some(), "the seeded map must yield a first entry");

    let reads = counting.lower_reads();
    // One keyset read plus at most one chunk of point reads.
    assert!(
        reads <= CELL_BATCH + 1,
        "one next() read {reads} cells; expected <= one chunk ({}) plus the keyset",
        CELL_BATCH + 1
    );
    assert!(
        reads < entries,
        "one next() must not drain all {entries} entries ({reads} reads)"
    );
    Ok(())
}

// --- Registration fluent-option survival ------------------------------------

/// The fluent options a binding sets on a descriptor (`ttl`,
/// `read_uncommitted`, map `keyset_limit`) thread through registration
/// unchanged — the erased seam adds no new registration surface. Confirms the
/// operational def a client registers is the one the registry holds.
#[test]
fn erased_registration_options_thread_through() -> Result<()> {
    use crate::state::registry::CommitMode;
    use crate::state::{StateName, StateType};

    let ttl = CompactDuration::new(3_600);
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &value_state::<JsonCodec>("v").ttl(ttl).read_uncommitted(),
        // The config layer derives the def from the descriptor's fluent
        // settings; here the descriptor carries them directly.
        value_state::<JsonCodec>("v")
            .ttl(ttl)
            .read_uncommitted()
            .collection_def(),
    )?;
    registry.register(
        &map_state::<Utf8KeyCodec, JsonCodec>("m").keyset_limit(7),
        map_state::<Utf8KeyCodec, JsonCodec>("m")
            .keyset_limit(7)
            .collection_def(),
    )?;

    let v = StateName::try_new("v")?;
    let m = StateName::try_new("m")?;
    assert_eq!(registry.ttl_for(StateType::Application, &v), Some(ttl));
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &v),
        CommitMode::ReadUncommitted
    );
    assert_eq!(registry.def_for(StateType::Application, &m).keyset_limit, 7);
    Ok(())
}
