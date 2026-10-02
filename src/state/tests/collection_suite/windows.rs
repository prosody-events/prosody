//! Seeded deque windows and position visibility properties.

use super::*;

/// Seeds a deque window directly into `store`: the `head ‖ tail` meta frame for
/// `[head, head + cells.len())` plus a present entry cell for each `Some` slot,
/// leaving `None` slots as holes (a TTL-expired entry not yet swept). The only
/// way to reach a window the handle never produces — a sparse window, or one
/// wider than the current capacity.
pub(super) async fn seed_deque_window<S: CellStore>(
    store: &S,
    collection_ref: &CollectionRef,
    head: i64,
    cells: &[Option<u8>],
) -> Result<()> {
    use bytes::Bytes;

    let tail = head + cells.len() as i64;
    store
        .write_resolved(
            collection_ref,
            &[(
                deque::meta_cell(),
                Some(Bytes::from(deque::seed_frame(head, tail))),
            )],
            &[],
        )
        .await?;
    for (i, cell) in cells.iter().enumerate() {
        if let Some(value) = cell {
            let coordinate = I64KeyCodec::encode(&(head + i as i64));
            let bytes = Bytes::from(serde_json::to_vec(&Value::from(*value))?);
            store
                .write_resolved(
                    collection_ref,
                    &[(deque::entry_cell_for(&coordinate), Some(bytes))],
                    &[],
                )
                .await?;
        }
    }
    Ok(())
}

/// Deque TTL-hole read contract: over a directly-seeded sparse window, `len`
/// is the full span `tail − head` (an upper bound on the live count), `get`
/// returns `None` at a hole and past the span, both stream directions yield
/// exactly the present values in index order (ascending forward, reversed
/// backward) without error, and the endpoint peeks share `get`'s slot
/// semantics under holes (an expired endpoint yields `None` even with a live
/// interior). Seeded directly — never via wall-clock TTL — the only way to
/// reach a holed window the handle itself never produces.
pub(crate) async fn run_deque_holes(shape: DequeHoles) -> Result<bool> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "dq", &state_key, CollectionDef::new(None))?;
    let store = MemoryCellStore::new(cells.clone());

    seed_deque_window(&store, &collection_ref, shape.head, &shape.cells).await?;

    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

    let len = shape.cells.len();
    if handle.len().await? != len {
        return Ok(false);
    }
    for p in 0..len + 2 {
        let expected = shape.cells.get(p).copied().flatten().map(Value::from);
        if handle.get(p).await? != expected {
            return Ok(false);
        }
    }
    let present: Vec<Value> = shape
        .cells
        .iter()
        .copied()
        .filter_map(|c| c.map(Value::from))
        .collect();
    if collect_deque(&handle, Direction::Forward).await? != present {
        return Ok(false);
    }
    let reversed: Vec<Value> = present.iter().rev().cloned().collect();
    if collect_deque(&handle, Direction::Backward).await? != reversed {
        return Ok(false);
    }
    assert_peeks(&handle).await
}

/// A directly-seeded sparse deque window: a `head` index and a run of per-index
/// cells — `Some(v)` present, `None` a hole (a TTL-expired entry not yet
/// swept). `tail = head + cells.len()`. Seeded straight into the store (never
/// produced by the handle, which keeps the window dense) to pin the TTL'd-hole
/// read contract: `len` an upper bound, `get`/`values` skip holes without
/// error.
#[derive(Clone, Debug)]
pub(crate) struct DequeHoles {
    head: i64,
    cells: Vec<Option<u8>>,
}

impl Arbitrary for DequeHoles {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            head: g.choose(&HEAD_POOL).copied().unwrap_or(0),
            cells: capped_vec(g, MAX_DEQUE_WINDOW),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let head = self.head;
        Box::new(self.cells.shrink().map(move |cells| Self { head, cells }))
    }
}
