//! Map and deque traces across event settlement.

use super::*;

/// Drives a deque trace, asserting the handle equals a `VecDeque` model after
/// every event and that each `pop` returns the model's value. A
/// `Some(capacity)` registers a bounded deque and applies the **identical**
/// capped-trim rule to the model (a plain loop, never a call to `evictions`),
/// so the dedup store tracks the handle's lazy push-only eviction op-for-op —
/// the abort/crash arms then exercise rollback of those evictions. A dense
/// no-TTL window keeps `VecDeque::len` equal to the handle's window span, so
/// the model stays exact.
pub(crate) async fn run_deque_trace(
    trace: DequeTrace,
    commit_mode: CommitMode,
    capacity: Option<NonZeroUsize>,
) -> Result<bool> {
    run_collection_trace(
        trace,
        deque_state::<JsonCodec>("dq"),
        "dq",
        CollectionDef {
            commit_mode,
            capacity,
            ..CollectionDef::new(None)
        },
        async move |handle, op, scratch: &mut VecDeque<Value>| match op {
            DequeOp::PushBack(b) => {
                let v = Value::from(b);
                handle.push_back(v.clone()).await?;
                if let Some(cap) = capacity {
                    evict_for_push(scratch, cap.get(), true);
                }
                scratch.push_back(v);
                Ok(OpOutcome::Continue)
            }
            DequeOp::PushFront(b) => {
                let v = Value::from(b);
                handle.push_front(v.clone()).await?;
                if let Some(cap) = capacity {
                    evict_for_push(scratch, cap.get(), false);
                }
                scratch.push_front(v);
                Ok(OpOutcome::Continue)
            }
            DequeOp::PopBack => Ok(mismatch_unless(
                handle.pop_back().await? == scratch.pop_back(),
            )),
            DequeOp::PopFront => Ok(mismatch_unless(
                handle.pop_front().await? == scratch.pop_front(),
            )),
            DequeOp::Clear => {
                handle.clear().await?;
                scratch.clear();
                Ok(OpOutcome::Continue)
            }
            DequeOp::Commit => {
                handle.commit().await?;
                Ok(OpOutcome::Committed)
            }
        },
        async |handle, model, _backing: &Backing<'_>| assert_deque(handle, model).await,
    )
    .await
}

/// Checks both query outputs on a range-only plan and on a plan that crosses
/// from tracked to overflowed.
pub(crate) async fn run_map_query_trace(
    trace: MapTrace,
    constraints: StreamConstraints,
) -> Result<bool> {
    for mode in [CommitMode::ReadCommitted, CommitMode::ReadUncommitted] {
        for keyset_limit in [0, 3] {
            if !run_map_trace_inner(trace.clone(), mode, keyset_limit, constraints).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub(super) async fn run_map_trace_inner(
    trace: MapTrace,
    commit_mode: CommitMode,
    keyset_limit: usize,
    constraints: StreamConstraints,
) -> Result<bool> {
    run_collection_trace(
        trace,
        map_state::<I64KeyCodec, JsonCodec>("mp"),
        "mp",
        // The keyset limit selects one source or permits a transition between sources.
        CollectionDef {
            commit_mode,
            keyset_limit,
            ..CollectionDef::new(None)
        },
        async |handle, op, scratch: &mut BTreeMap<i64, Value>| match op {
            MapOp::Set(k, b) => {
                let v = Value::from(b);
                handle.set(&k, v.clone()).await?;
                scratch.insert(k, v);
                Ok(OpOutcome::Continue)
            }
            MapOp::Remove(k) => {
                handle.remove(&k).await?;
                scratch.remove(&k);
                Ok(OpOutcome::Continue)
            }
            MapOp::Get(k) => {
                let got = handle.get(&k).await?;
                let present = handle.contains_key(&k).await?;
                Ok(mismatch_unless(
                    got == scratch.get(&k).cloned() && present == got.is_some(),
                ))
            }
            MapOp::IsEmpty => Ok(mismatch_unless(
                handle.is_empty().await? == scratch.is_empty(),
            )),
            MapOp::Clear => {
                handle.clear().await?;
                scratch.clear();
                Ok(OpOutcome::Continue)
            }
            MapOp::Commit => {
                handle.commit().await?;
                Ok(OpOutcome::Committed)
            }
        },
        async |handle, model, backing: &Backing<'_>| {
            Ok(assert_map(handle, model, constraints).await?
                && assert_keyset_present(backing.cells, backing.state_key, model)?)
        },
    )
    .await
}

/// Map TTL keyset-refresh (what `finalize` stages): on a collection **with a
/// TTL**, every `set` buffers the keyset cell — even a re-set of an
/// already-tracked key, and even once the map has overflowed — so its TTL is
/// refreshed and the keyset outlives every entry. Runs multiple committed
/// events over a fresh per-event dirty workspace so a later set lands over a
/// *committed* keyset: the case a single-event snapshot cannot reach, because
/// the first set always seeds the keyset into the dirty overlay, masking a
/// suppressed-refresh regression on the no-write fast paths (already-tracked
/// and `Overflowed`, both reached with pool 5 / limit 3).
pub(crate) async fn run_map_ttl_keyset_refresh_trace(trace: MapTrace) -> Result<bool> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &descriptor,
        CollectionDef {
            keyset_limit: 3,
            ..CollectionDef::new(Some(CompactDuration::new(3_600)))
        },
    )?;
    let registry = Arc::new(registry);
    let id = CollectionId::new(
        state_key.clone(),
        StateType::Application,
        StateName::try_new("mp")?,
    );
    let keyset_cell = keyset_cell();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let dirty = Arc::new(DirtyStore::new());
        let session =
            make_session_with_dirty(&cells, &dedup, &registry, &state_key, event, dirty.clone());
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for op in &ev.ops {
            match *op {
                MapOp::Set(k, b) => {
                    handle.set(&k, Value::from(b)).await?;
                    // Snapshot immediately, before any later Commit drains
                    // dirty: a TTL'd set always buffers the keyset cell.
                    let snapshot = dirty.collection_snapshot(&id);
                    if !snapshot.iter().any(|(c, _)| *c == keyset_cell) {
                        return Ok(false);
                    }
                }
                MapOp::Remove(k) => handle.remove(&k).await?,
                MapOp::Get(k) => {
                    handle.get(&k).await?;
                }
                MapOp::IsEmpty => {
                    handle.is_empty().await?;
                }
                MapOp::Clear => handle.clear().await?,
                MapOp::Commit => {
                    handle.commit().await?;
                }
            }
        }
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, &id).await?;
    }
    Ok(true)
}

/// Keyset exactness (no TTL): over an arbitrary committed
/// set/remove/get/clear/commit trace on a map whose `keyset_limit` (8) exceeds
/// the 5-key pool — so the frame never overflows on count, and i64 coordinates
/// (12 bytes each) stay far under the 64 KiB ceiling — the stored keyset
/// decodes to **exactly** the live key set after every settled event. Because
/// `remove` subtracts, a superset can never survive a removal (the pre-keyset
/// design's loose superset would fail here). An absent keyset counts as the
/// empty set (a fresh or `clear`ed map); a removed-to-empty map instead holds
/// the empty `Tracked` frame — both are the live-empty case.
pub(crate) async fn run_map_keyset_exact_trace(trace: MapTrace) -> Result<bool> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "mp",
        &state_key,
        CollectionDef {
            keyset_limit: 8,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();
    let store = MemoryCellStore::new(cells.clone());
    let mut model: BTreeMap<i64, Value> = BTreeMap::new();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let session = make_session(&cells, &dedup, &registry, &state_key, event);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
        for op in &ev.ops {
            match *op {
                MapOp::Set(k, b) => {
                    let v = Value::from(b);
                    handle.set(&k, v.clone()).await?;
                    model.insert(k, v);
                }
                MapOp::Remove(k) => {
                    handle.remove(&k).await?;
                    model.remove(&k);
                }
                MapOp::Get(k) => {
                    handle.get(&k).await?;
                }
                MapOp::IsEmpty => {
                    handle.is_empty().await?;
                }
                MapOp::Clear => {
                    handle.clear().await?;
                    model.clear();
                }
                MapOp::Commit => {
                    handle.commit().await?;
                }
            }
        }
        finalize_and_promote(&session, &dedup, event_dedup(event), &cells, id).await?;

        // The committed keyset must be exactly the live key set: a present
        // frame must equal `tracked_frame(live)`; an absent keyset is the
        // live-empty case (a fresh or `clear`ed map).
        let live: Vec<i64> = model.keys().copied().collect();
        let stored = CellRead::<Values>::read(&store, id, &keyset_cell())
            .await?
            .0
            .into_inner();
        let exact = match stored {
            Some(bytes) => bytes[..] == tracked_frame(&live)[..],
            None => model.is_empty(),
        };
        if !exact {
            return Ok(false);
        }
    }
    Ok(true)
}
