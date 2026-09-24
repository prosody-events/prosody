//! Bounded deque eviction and durable orphan checks.

use super::*;

/// Evicts the deque's capped-trim prelude to a push on a plain window model —
/// at most `TRIM_MAX` slots from the far end (front for a back push, back for a
/// front push) toward `cap`. Deliberately **not** a call to the production
/// `evictions`, keeping the dedup store an independent check.
pub(super) fn evict_for_push<T>(model: &mut VecDeque<T>, cap: usize, from_back: bool) {
    let mut evicted = 0;
    while model.len() + 1 > cap && evicted < deque::TRIM_MAX {
        if from_back {
            model.pop_front();
        } else {
            model.pop_back();
        }
        evicted += 1;
    }
}

/// Applies the deque's capped-trim rule to a window model op-for-op: evict the
/// capped-trim prelude (see [`evict_for_push`]), then append.
pub(super) fn apply_capped_push(
    model: &mut VecDeque<Option<u8>>,
    cap: usize,
    from_back: bool,
    value: u8,
) {
    evict_for_push(model, cap, from_back);
    if from_back {
        model.push_back(Some(value));
    } else {
        model.push_front(Some(value));
    }
}

/// Deque runtime-capacity convergence: over a directly-seeded window that may
/// start **wider than** the current cap (the redeploy case) and may hold TTL
/// holes, lazy push-only eviction converges to `len <= cap` within
/// `⌈D / (TRIM_MAX − 1)⌉` catch-up pushes, evicting **at most `TRIM_MAX` slots
/// per push** and surviving values equal the opposite-end suffix/prefix. Proves
/// in one property: a within-cap excess lands exactly `cap` on the first push;
/// convergence holds while reads never enforce; a hole eviction is a no-op
/// clear that never errors; and the per-push physical eviction cap holds. The
/// physical eviction count is read from the buffered dirty
/// overlay — the buffered entry-section deletes — not a net `len` delta, since
/// a net delta alone cannot bound the physical clears an impl issues.
pub(crate) async fn run_deque_capacity_convergence(shape: DequeCapacityShape) -> Result<bool> {
    let DequeCapacityShape {
        cap,
        cells,
        from_back,
    } = shape;
    let dedup = MemoryDeduplicationStore::default();
    let store_cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("dq");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "dq",
        &state_key,
        CollectionDef {
            capacity: Some(cap),
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(store_cells.clone());
    let read_session =
        |idx: usize| make_session(&store_cells, &dedup, &registry, &state_key, read_event(idx));

    // Seed the (possibly over-wide, possibly holed) window directly at index 0 —
    // the handle never produces a window wider than the cap, so it must be seeded.
    let span = cells.len();
    seed_deque_window(&store, &collection_ref, 0, &cells).await?;

    // Reads never enforce: before any push, the whole seeded window is visible.
    let read = read_session(0);
    let handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    if handle.len().await? != span {
        return Ok(false);
    }

    // The model of the window slots (holes as `None`), applying the identical
    // capped-trim rule per push — a plain loop, never a call to `evictions`.
    let mut model: VecDeque<Option<u8>> = cells.iter().copied().collect();
    let excess = span.saturating_sub(cap.get());
    let step = deque::TRIM_MAX.saturating_sub(1).max(1);
    let budget = excess.div_ceil(step).max(1);

    for i in 0..budget {
        let value = 100u8.wrapping_add(i as u8);
        let len_before = model.len();
        apply_capped_push(&mut model, cap.get(), from_back, value);
        let expected_evictions = len_before + 1 - model.len();

        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(i as u128 + 1),
        };
        let dirty = Arc::new(DirtyStore::new());
        let session = make_session_with_dirty(
            &store_cells,
            &dedup,
            &registry,
            &state_key,
            event,
            dirty.clone(),
        );
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        if from_back {
            handle.push_back(Value::from(value)).await?;
        } else {
            handle.push_front(Value::from(value)).await?;
        }

        // Physical buffered eviction count: entry-section deletes in the dirty
        // overlay (the meta bounds cell and the appended entry are `Set`s, so a
        // buffered `None` is exactly an evicted slot — including a holed one).
        let physical_evictions = dirty
            .collection_snapshot(id)
            .iter()
            .filter(|(_, val)| val.is_none())
            .count();
        // G: at most `TRIM_MAX` clears per push, and the physical clears track
        // the model's net convergence exactly.
        if physical_evictions > deque::TRIM_MAX || physical_evictions != expected_evictions {
            return Ok(false);
        }
        // B: a within-cap excess lands exactly `cap` on the very first push.
        if i == 0 && (1..deque::TRIM_MAX).contains(&excess) && model.len() != cap.get() {
            return Ok(false);
        }

        finalize_and_promote(&session, &dedup, event_dedup(event), &store_cells, id).await?;

        // Committed read-back: the span equals the model (holes included).
        let read = read_session(i + 1);
        let handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
        if handle.len().await? != model.len() {
            return Ok(false);
        }
    }

    // Converged: within the cap, and the surviving values equal the model's
    // opposite-end suffix/prefix (holes skipped by `values`, never an error).
    let read = read_session(budget + 1);
    let handle = descriptor.bind(&read).map_err(|e| eyre!("bind: {e}"))?;
    if handle.len().await? > cap.get() {
        return Ok(false);
    }
    let survivors: Vec<Value> = model.iter().filter_map(|c| c.map(Value::from)).collect();
    if collect_deque(&handle, Direction::Forward).await? != survivors {
        return Ok(false);
    }
    let reversed: Vec<Value> = survivors.iter().rev().cloned().collect();
    if collect_deque(&handle, Direction::Backward).await? != reversed {
        return Ok(false);
    }

    deque_no_committed_orphans(&store, id, span).await
}

/// Physical-erasure half of the capacity property: a bounded push must clear
/// the **correct** committed slots and leave no orphan below `head` or at/above
/// `tail`. Windowed reads skip such an orphan, so a count-matching mutant that
/// cleared a wrong coordinate passes every windowed assert — this reads the
/// committed bounds and requires every seeded index outside `[head, tail)` to
/// be erased. Pushed cells land inside the window, so only the seed range
/// `0..span` can orphan. Mirrors `deque_clear_resets_the_index_space`'s leak
/// guard.
pub(super) async fn deque_no_committed_orphans(
    store: &MemoryCellStore,
    id: &CollectionId,
    span: usize,
) -> Result<bool> {
    let Some(bounds) = CellRead::<Values>::read(store, id, deque::meta_cell().as_ref())
        .await?
        .0
        .into_inner()
    else {
        bail!("bounds cell missing after the convergence pushes");
    };
    let head = i64::from_be_bytes(bounds[0..8].try_into()?);
    let tail = i64::from_be_bytes(bounds[8..16].try_into()?);
    for i in 0..span as i64 {
        let outside = i < head || i >= tail;
        if outside
            && CellRead::<Values>::read(
                store,
                id,
                (deque::entry_cell_for(&I64KeyCodec::encode(&i))).as_ref(),
            )
            .await?
            .0
            .into_inner()
            .is_some()
        {
            return Ok(false); // a committed orphan outside the converged window
        }
    }
    Ok(true)
}

/// A directly-seeded over-wide (possibly holed) deque window plus a bounded
/// capacity and a push direction, for the capacity-convergence property. The
/// seeded span is `cells.len()`; the excess over `cap` is trimmed lazily by the
/// catch-up pushes. `from_back` pushes at the back (evicting the front) when
/// `true`, else at the front (evicting the back). Seeded straight into the
/// store (never produced by the handle) so the window can start **wider than**
/// the current capacity — the redeploy case lazy enforcement must converge.
#[derive(Clone, Debug)]
pub(crate) struct DequeCapacityShape {
    cap: NonZeroUsize,
    cells: Vec<Option<u8>>,
    from_back: bool,
}

impl Arbitrary for DequeCapacityShape {
    fn arbitrary(g: &mut Gen) -> Self {
        let cap = g.choose(&CAP_POOL).copied().unwrap_or(1);
        Self {
            cap: NonZeroUsize::new(cap).unwrap_or(NonZeroUsize::MIN),
            cells: capped_vec(g, MAX_DEQUE_WINDOW),
            from_back: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let cap = self.cap;
        let from_back = self.from_back;
        Box::new(self.cells.shrink().map(move |cells| Self {
            cap,
            cells,
            from_back,
        }))
    }
}
