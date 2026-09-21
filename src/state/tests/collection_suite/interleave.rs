//! Stream observations across interleaved collection operations.

use super::*;
use crate::state::query::tests::query_buffer;

/// One step of a map stream-interleave trace: advance the live stream one item,
/// or run a mutator on the same session between items.
#[derive(Clone, Debug)]
pub(crate) enum MapStreamStep {
    /// Pull the next stream item.
    Advance,
    /// `set(key, val)`.
    Set(i64, i64),
    /// `remove(key)`.
    Remove(i64),
    /// `clear()`.
    Clear,
    /// Mid-handler `commit()`.
    Commit,
    /// Mid-handler `rollback()`.
    Rollback,
}

impl Arbitrary for MapStreamStep {
    fn arbitrary(g: &mut Gen) -> Self {
        // Keys 0..24 span the 20-key seed plus a few added-after-init keys;
        // values 0..8 so re-sets collide. `Advance` is weighted heavily so the
        // stream actually drains between mutations.
        let key = i64::from(u8::arbitrary(g) % 24);
        let val = i64::from(u8::arbitrary(g) % 8);
        match g
            .choose(&[0_u8, 0, 0, 1, 1, 2, 3, 4, 5])
            .copied()
            .unwrap_or(0)
        {
            1 => Self::Set(key, val),
            2 => Self::Remove(key),
            3 => Self::Commit,
            4 => Self::Rollback,
            5 => Self::Clear,
            _ => Self::Advance,
        }
    }
}

/// A map stream-interleave trace plus the stream direction.
#[derive(Clone, Debug)]
pub(crate) struct MapInterleave {
    steps: Vec<MapStreamStep>,
    backward: bool,
}

impl Arbitrary for MapInterleave {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
            backward: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let backward = self.backward;
        Box::new(
            self.steps
                .shrink()
                .map(move |steps| Self { steps, backward }),
        )
    }
}

/// One step of a deque stream-interleave trace.
#[derive(Clone, Debug)]
pub(crate) enum DequeStreamStep {
    /// Pull the next stream item.
    Advance,
    /// `push_back(val)`.
    PushBack(i64),
    /// `push_front(val)`.
    PushFront(i64),
    /// `pop_back()`.
    PopBack,
    /// `pop_front()`.
    PopFront,
    /// `clear()`.
    Clear,
    /// Mid-handler `commit()`.
    Commit,
    /// Mid-handler `rollback()`.
    Rollback,
}

impl Arbitrary for DequeStreamStep {
    fn arbitrary(g: &mut Gen) -> Self {
        let val = i64::from(u8::arbitrary(g));
        match g
            .choose(&[0_u8, 0, 0, 1, 2, 3, 4, 5, 6, 7])
            .copied()
            .unwrap_or(0)
        {
            1 => Self::PushBack(val),
            2 => Self::PushFront(val),
            3 => Self::PopBack,
            4 => Self::PopFront,
            5 => Self::Commit,
            6 => Self::Rollback,
            7 => Self::Clear,
            _ => Self::Advance,
        }
    }
}

/// A deque stream-interleave trace plus the stream direction.
#[derive(Clone, Debug)]
pub(crate) struct DequeInterleave {
    steps: Vec<DequeStreamStep>,
    backward: bool,
}

impl Arbitrary for DequeInterleave {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: capped_vec(g, MAX_TRACE_OPS),
            backward: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let backward = self.backward;
        Box::new(
            self.steps
                .shrink()
                .map(move |steps| Self { steps, backward }),
        )
    }
}

/// Runs a fallible handle op under the interleave hang-guard, tagging both the
/// timeout and the op error with `label` — the mutator boilerplate the
/// interleave runners share.
pub(super) async fn guarded<T, E: Display>(
    label: &str,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T> {
    timeout(INTERLEAVE_HANG_GUARD, fut)
        .await
        .map_err(|_| eyre!("{label} hung"))?
        .map_err(|e| eyre!("{label}: {e}"))
}

/// Weak-consistency check for one yielded map entry: its key was in the init
/// snapshot, is yielded at most once, and its value was held at that key at
/// some point (the per-arm consistency contract — a paged live read, not a
/// snapshot).
pub(super) fn check_map_yield(
    key: i64,
    value: &Value,
    init_keys: &BTreeSet<i64>,
    yielded: &mut BTreeSet<i64>,
    ever_held: &BTreeMap<i64, BTreeSet<i64>>,
) -> Result<()> {
    if !init_keys.contains(&key) {
        bail!("yielded key {key} was not in the init-snapshot membership");
    }
    if !yielded.insert(key) {
        bail!("key {key} was yielded twice");
    }
    let value = value
        .as_i64()
        .ok_or_else(|| eyre!("non-integer value yielded for key {key}"))?;
    if !ever_held
        .get(&key)
        .is_some_and(|held| held.contains(&value))
    {
        bail!("value {value} yielded for key {key} was never held there");
    }
    Ok(())
}

/// The `StreamYieldFree` interleaving property (map): random `next()`/mutator
/// interleavings on ONE live session against a live map stream never deadlock
/// and never error, and
/// every yielded entry is weakly consistent with the init snapshot. A forced
/// first `Advance` locks the key-membership snapshot to the committed seed
/// before any mutator runs, so a yielded key must be a seed key and its value
/// one held there at some point (values are read live, chunk by chunk). Every
/// op is bounded by [`INTERLEAVE_HANG_GUARD`] — the only deadline, never the
/// assertion. To falsify this test, hold chunk admission across a yield.
/// The next mutation then blocks until the test deadline expires.
pub(crate) async fn run_map_stream_interleave(input: MapInterleave) -> Result<bool> {
    let MapInterleave { steps, backward } = input;
    let dir = if backward {
        Direction::Backward
    } else {
        Direction::Forward
    };
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("iv");
    let (registry, collection_ref) = registry_and_ref(
        &descriptor,
        "iv",
        &state_key,
        CollectionDef {
            // ≥ seed so the map stays Tracked (the chunked point-get arm).
            keyset_limit: 4096,
            ..CollectionDef::new(None)
        },
    )?;
    let id = collection_ref.id();

    // Seed a committed map of INTERLEAVE_SEED keys; `ever_held` records each.
    let mut ever_held: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    let seed_event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let seed_session = make_session(&cells, &dedup, &registry, &state_key, seed_event);
    let seed = descriptor
        .bind(&seed_session)
        .map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..INTERLEAVE_SEED {
        let key = i64::try_from(i)?;
        seed.set(&key, Value::from(key)).await?;
        ever_held.entry(key).or_default().insert(key);
    }
    finalize_and_promote(&seed_session, &dedup, Uuid::from_u128(1), &cells, id).await?;
    let init_keys: BTreeSet<i64> = (0..i64::try_from(INTERLEAVE_SEED)?).collect();

    // A fresh live session; the stream and its racing mutators share it.
    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let stream = handle.entries(query_buffer()).direction(dir).stream();
    futures::pin_mut!(stream);

    let mut yielded: BTreeSet<i64> = BTreeSet::new();
    // A forced first `Advance` locks the snapshot to the committed seed before
    // any mutator, then the generated steps interleave freely.
    for step in once(MapStreamStep::Advance).chain(steps) {
        match step {
            MapStreamStep::Advance => {
                if let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
                    .await
                    .map_err(|_| eyre!("Advance hung: the stream held the gate across a yield"))?
                {
                    let (key, value) =
                        item.map_err(|e| eyre!("stream yielded Err on a legal interleaving: {e}"))?;
                    check_map_yield(key, &value, &init_keys, &mut yielded, &ever_held)?;
                }
            }
            MapStreamStep::Set(key, val) => {
                guarded("set", handle.set(&key, Value::from(val))).await?;
                ever_held.entry(key).or_default().insert(val);
            }
            MapStreamStep::Remove(key) => {
                guarded("remove", handle.remove(&key)).await?;
            }
            MapStreamStep::Clear => {
                guarded("clear", handle.clear()).await?;
            }
            MapStreamStep::Commit => {
                guarded("commit", handle.commit()).await?;
            }
            MapStreamStep::Rollback => {
                timeout(INTERLEAVE_HANG_GUARD, handle.rollback())
                    .await
                    .map_err(|_| eyre!("rollback hung"))?;
            }
        }
    }
    // Drain the remainder under the hang-guard.
    while let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
        .await
        .map_err(|_| eyre!("drain hung"))?
    {
        let (key, value) = item.map_err(|e| eyre!("stream yielded Err on drain: {e}"))?;
        check_map_yield(key, &value, &init_keys, &mut yielded, &ever_held)?;
    }
    Ok(true)
}

/// The `StreamYieldFree` interleaving property (deque): the structural twin of
/// [`run_map_stream_interleave`]. A forced first `Advance` locks the **position
/// window** snapshot to the committed seed; thereafter random push/pop/clear/
/// commit/rollback mutators interleave with `next()`. No op deadlocks, no
/// `Advance` errors, the yielded count never exceeds the init window length,
/// and every yielded value was pushed at some point (position identity — a
/// popped position reads absent and is skipped). Same falsification as the map
/// twin.
pub(crate) async fn run_deque_stream_interleave(input: DequeInterleave) -> Result<bool> {
    let DequeInterleave { steps, backward } = input;
    let dir = if backward {
        Direction::Backward
    } else {
        Direction::Forward
    };
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = deque_state::<JsonCodec>("iv");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "iv", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();

    // Seed a committed window of INTERLEAVE_SEED elements; `ever_pushed` records
    // every value the deque ever held.
    let mut ever_pushed: BTreeSet<i64> = BTreeSet::new();
    let seed_event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };
    let seed_session = make_session(&cells, &dedup, &registry, &state_key, seed_event);
    let seed = descriptor
        .bind(&seed_session)
        .map_err(|e| eyre!("bind: {e}"))?;
    for i in 0..INTERLEAVE_SEED {
        let value = i64::try_from(i)?;
        seed.push_back(Value::from(value)).await?;
        ever_pushed.insert(value);
    }
    finalize_and_promote(&seed_session, &dedup, Uuid::from_u128(1), &cells, id).await?;

    let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
    let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;
    let stream = handle.values().direction(dir).stream();
    futures::pin_mut!(stream);

    let mut yielded = 0_usize;

    for step in once(DequeStreamStep::Advance).chain(steps) {
        match step {
            DequeStreamStep::Advance => {
                if let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
                    .await
                    .map_err(|_| eyre!("Advance hung: the stream held the gate across a yield"))?
                {
                    let value =
                        item.map_err(|e| eyre!("stream yielded Err on a legal interleaving: {e}"))?;
                    check_deque_yield(&value, &ever_pushed, &mut yielded)?;
                }
            }
            DequeStreamStep::PushBack(val) => {
                guarded("push_back", handle.push_back(Value::from(val))).await?;
                ever_pushed.insert(val);
            }
            DequeStreamStep::PushFront(val) => {
                guarded("push_front", handle.push_front(Value::from(val))).await?;
                ever_pushed.insert(val);
            }
            DequeStreamStep::PopBack => {
                guarded("pop_back", handle.pop_back()).await?;
            }
            DequeStreamStep::PopFront => {
                guarded("pop_front", handle.pop_front()).await?;
            }
            DequeStreamStep::Clear => {
                guarded("clear", handle.clear()).await?;
            }
            DequeStreamStep::Commit => {
                guarded("commit", handle.commit()).await?;
            }
            DequeStreamStep::Rollback => {
                timeout(INTERLEAVE_HANG_GUARD, handle.rollback())
                    .await
                    .map_err(|_| eyre!("rollback hung"))?;
            }
        }
    }
    while let Some(item) = timeout(INTERLEAVE_HANG_GUARD, stream.next())
        .await
        .map_err(|_| eyre!("drain hung"))?
    {
        let value = item.map_err(|e| eyre!("stream yielded Err on drain: {e}"))?;
        check_deque_yield(&value, &ever_pushed, &mut yielded)?;
    }
    Ok(true)
}

/// Weak-consistency check for one yielded deque element: the yielded count
/// stays within the init window length and the value was pushed at some point
/// (position identity — a popped position reads absent and is skipped).
pub(super) fn check_deque_yield(
    value: &Value,
    ever_pushed: &BTreeSet<i64>,
    yielded: &mut usize,
) -> Result<()> {
    *yielded += 1;
    if *yielded > INTERLEAVE_SEED {
        bail!("yielded more items than the init window length");
    }
    let value = value
        .as_i64()
        .ok_or_else(|| eyre!("non-integer value yielded"))?;
    if !ever_pushed.contains(&value) {
        bail!("value {value} was never pushed");
    }
    Ok(())
}
