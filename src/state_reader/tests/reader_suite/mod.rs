//! Committed reads must match the model after each event.
//!
//! Each runner commits a generated trace through the real owner session.
//! The same operations update an independent model for each collection kind.
//! Readers check point reads and queries in both directions after every event.
//! The memory and Cassandra suites share these generic runners.
//!
//! [`collection_suite`](crate::state::tests::collection_suite) owns the
//! collection traces. [`ValueOp`] supplies operations for value cells.
//! The runner ignores mid-handler commits and reads because it commits every
//! event.
//!
//! Every query compares its results with the model, including empty results.
//! Trace generators favor inserts. Trace shrinking preserves event structure.

use super::support::{
    OwnerSession, ReaderBackend, all_match, collect_stream, owner_commit_cell, source_state_key,
    state_name,
};
use crate::Key;
use crate::Topic;
use crate::codec::JsonCodec;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    DequeDescriptor, DequeHandle, DescriptorIdentity, MapDescriptor, MapHandle, ValueDescriptor,
};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::identity::StateKey;
use crate::state::order_codec::I64KeyCodec;
use crate::state::query::tests::query_buffer;
use crate::state::tests::collection_suite::{DequeOp, KEY_POOL, MapOp, Trace};
use crate::state::tests::support::reader_residue;
use crate::state_reader::backend::ReaderBackend as CoreReaderBackend;
use crate::state_reader::{PartitionCount, StateReader};
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, eyre};
use futures::{join, try_join};
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;

/// The fixed routing coordinates one trace runs under. The owner writes
/// through them and the reader independently recomputes them. Bundled so a
/// runner takes one argument instead of five.
pub(crate) struct ReaderCase<'a> {
    /// The subsystem the collection routes under.
    pub(crate) sub: &'a SubsystemName,
    /// The single publishing group.
    pub(crate) group: &'a str,
    /// The topic whose messages wrote the state.
    pub(crate) topic: Topic,
    /// The partition key every op addresses.
    pub(crate) key: &'a Key,
    /// The topic's partition count.
    pub(crate) count: PartitionCount,
}

/// Publishes `descriptor`'s routing and freezes its identity so the reader
/// will admit this case's source. Returns the segment-qualified state key the
/// owner writes to, the same key the reader independently recomputes.
async fn seed_source<B, D>(backend: &B, descriptor: D, case: &ReaderCase<'_>) -> Result<StateKey>
where
    B: ReaderBackend,
    D: DescriptorIdentity + Copy,
{
    let name = state_name(descriptor.name())?;
    let identity = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );
    backend
        .publish(
            case.sub, &name, case.group, case.topic, case.count, &identity,
        )
        .await?;
    source_state_key(case.topic, case.group, case.key, case.count)
}

/// The concrete Map handle the owner session binds.
type OwnerMapHandle<B> =
    MapHandle<OwnerSession<<B as ReaderBackend>::OwnerCell>, I64KeyCodec, JsonCodec>;

/// The concrete Deque handle the owner session binds.
type OwnerDequeHandle<B> = DequeHandle<OwnerSession<<B as ReaderBackend>::OwnerCell>, JsonCodec>;

/// Applies one Map event's ops to the owner `handle` (ignoring the generators'
/// mid-handler `Get`/`Commit`, which are no-ops for a committed read).
async fn apply_map_ops<B: ReaderBackend>(
    handle: &OwnerMapHandle<B>,
    ops: Vec<MapOp>,
) -> Result<()> {
    for op in ops {
        match op {
            MapOp::Set(k, b) => handle
                .set(&k, Value::from(b))
                .await
                .map_err(|e| eyre!("set: {e}"))?,
            MapOp::Remove(k) => handle.remove(&k).await.map_err(|e| eyre!("remove: {e}"))?,
            MapOp::Clear => handle.clear().await.map_err(|e| eyre!("clear: {e}"))?,
            MapOp::Get(_) | MapOp::IsEmpty | MapOp::Commit => {}
        }
    }
    Ok(())
}

/// Mirrors one Map event's ops into the `BTreeMap` model.
fn model_map_ops(model: &mut BTreeMap<i64, Value>, ops: &[MapOp]) {
    for op in ops {
        match *op {
            MapOp::Set(k, b) => {
                model.insert(k, Value::from(b));
            }
            MapOp::Remove(k) => {
                model.remove(&k);
            }
            MapOp::Clear => model.clear(),
            MapOp::Get(_) | MapOp::IsEmpty | MapOp::Commit => {}
        }
    }
}

/// Asserts every map read operation against the `BTreeMap` model.
async fn assert_map<B: ReaderBackend>(
    backend: &B,
    descriptor: MapDescriptor<I64KeyCodec, JsonCodec>,
    case: &ReaderCase<'_>,
    model: &BTreeMap<i64, Value>,
) -> Result<bool> {
    let deps = backend.deps();
    let reader = &StateReader::new(&deps, case.sub.clone(), descriptor)?;
    // The first read warms the publication snapshot. The rest share it.
    let empty = reader.is_empty(case.key.clone()).await?;
    let (points, many, forward, keys, constrained_entries, constrained_keys, backward) = join!(
        all_match(KEY_POOL.iter(), |k| async move {
            let (value, present) = try_join!(
                reader.get(case.key.clone(), k),
                reader.contains_key(case.key.clone(), k)
            )?;
            Ok(value == model.get(k).cloned() && present == model.contains_key(k))
        }),
        reader.get_many(case.key.clone(), &KEY_POOL),
        collect_stream(reader.entries(case.key.clone(), query_buffer()).stream()),
        collect_stream(reader.keys(case.key.clone(), query_buffer()).stream()),
        collect_stream(
            reader
                .entries(case.key.clone(), query_buffer())
                .from(&-1)
                .before(&2)
                .limit(NonZeroUsize::MIN)
                .stream()
        ),
        collect_stream(
            reader
                .keys(case.key.clone(), query_buffer())
                .after(&-2)
                .to(&1)
                .stream()
        ),
        collect_stream(
            reader
                .entries(case.key.clone(), query_buffer())
                .reverse()
                .stream()
        ),
    );
    let points = points?;
    let many = many?;
    let keys = keys?;
    let constrained_entries = constrained_entries?;
    let constrained_keys = constrained_keys?;
    let expect_many: Vec<_> = KEY_POOL.iter().map(|k| model.get(k).cloned()).collect();
    let expect_forward: Vec<_> = model.iter().map(|(k, v)| (*k, v.clone())).collect();
    let expected_entries: Vec<_> = model
        .range(&-1..&2)
        .take(1)
        .map(|(key, value)| (*key, value.clone()))
        .collect();
    let forward = forward?;
    let backward = backward?;
    if empty != model.is_empty()
        || !points
        || many != expect_many
        || forward != expect_forward
        || keys != model.keys().copied().collect::<Vec<_>>()
        || constrained_entries != expected_entries
        || constrained_keys != model.range(-1..=1).map(|(key, _)| *key).collect::<Vec<_>>()
    {
        return Ok(false);
    }
    let mut expect_backward = expect_forward;
    expect_backward.reverse();
    if backward != expect_backward {
        return Ok(false);
    }
    all_match(
        [
            (Direction::Forward, &forward),
            (Direction::Backward, &backward),
        ],
        |(dir, expected)| async move {
            let limit = NonZeroUsize::new(expected.len() / 2 + 1).unwrap_or(NonZeroUsize::MIN);
            let (entries, keys) = try_join!(
                collect_stream(
                    reader
                        .entries(case.key.clone(), query_buffer())
                        .direction(dir)
                        .limit(limit)
                        .stream()
                ),
                collect_stream(
                    reader
                        .keys(case.key.clone(), query_buffer())
                        .direction(dir)
                        .limit(limit)
                        .stream()
                ),
            )?;
            let expected: Vec<_> = expected.iter().take(limit.get()).cloned().collect();
            Ok(entries == expected
                && keys == expected.iter().map(|(key, _)| *key).collect::<Vec<_>>())
        },
    )
    .await
}

/// Drives a Map trace: commit each event's `Set`/`Remove`/`Clear`, mirror into
/// a `BTreeMap`, and after every event assert the reader matches the model.
///
/// FALSIFICATION: perturb the reader's committed point read
/// (`CommittedCellSource::read_committed`/`read_committed_many`) to drop or
/// misorder an entry → the keyset-backed `entries`/`get_many` diverges from the
/// model on the first non-empty event. This property never reaches the wide
/// committed-scan arm that keyset overflow falls back to, since `KEY_POOL`
/// stays under the keyset limit. That fallback is covered separately: by
/// [`scan_reads_only_pinned_source`](super::probe) for memory, and by
/// [`reader_deque_scan_committed`](super::cassandra_tests) for Cassandra. Do
/// not re-add a scan case here.
pub(super) async fn run_reader_map_trace<B: ReaderBackend>(
    backend: &B,
    descriptor: MapDescriptor<I64KeyCodec, JsonCodec>,
    case: &ReaderCase<'_>,
    trace: Trace<MapOp>,
) -> Result<bool> {
    let registry = backend.registry();
    let state_key = seed_source(backend, descriptor, case).await?;

    let mut model: BTreeMap<i64, Value> = BTreeMap::new();
    for (index, ops) in trace.events_ops().enumerate() {
        let staged: Vec<MapOp> = ops.to_vec();
        let for_handle = staged.clone();
        let commit = owner_commit_cell(
            backend.owner_cell(),
            &registry,
            &state_key,
            descriptor,
            index as u128,
            move |handle| async move { apply_map_ops::<B>(&handle, for_handle).await },
        );
        Box::pin(commit).await?;
        model_map_ops(&mut model, &staged);
        if !Box::pin(assert_map(backend, descriptor, case, &model)).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Applies one Deque event's ops to the owner `handle` (ignoring `Commit`).
async fn apply_deque_ops<B: ReaderBackend>(
    handle: &OwnerDequeHandle<B>,
    ops: Vec<DequeOp>,
) -> Result<()> {
    for op in ops {
        match op {
            DequeOp::PushBack(b) => {
                handle
                    .push_back(Value::from(b))
                    .await
                    .map_err(|e| eyre!("push: {e}"))?;
            }
            DequeOp::PushFront(b) => {
                handle
                    .push_front(Value::from(b))
                    .await
                    .map_err(|e| eyre!("push: {e}"))?;
            }
            DequeOp::PopBack => {
                handle.pop_back().await.map_err(|e| eyre!("pop: {e}"))?;
            }
            DequeOp::PopFront => {
                handle.pop_front().await.map_err(|e| eyre!("pop: {e}"))?;
            }
            DequeOp::Clear => handle.clear().await.map_err(|e| eyre!("clear: {e}"))?,
            DequeOp::Commit => {}
        }
    }
    Ok(())
}

/// Mirrors one Deque event's ops into the `VecDeque` model.
fn model_deque_ops(model: &mut VecDeque<Value>, ops: &[DequeOp]) {
    for op in ops {
        match *op {
            DequeOp::PushBack(b) => model.push_back(Value::from(b)),
            DequeOp::PushFront(b) => model.push_front(Value::from(b)),
            DequeOp::PopBack => {
                model.pop_back();
            }
            DequeOp::PopFront => {
                model.pop_front();
            }
            DequeOp::Clear => model.clear(),
            DequeOp::Commit => {}
        }
    }
}

/// Asserts every deque read operation against the `VecDeque` model.
async fn assert_deque<B: ReaderBackend>(
    backend: &B,
    descriptor: DequeDescriptor<JsonCodec>,
    case: &ReaderCase<'_>,
    model: &VecDeque<Value>,
) -> Result<bool> {
    let deps = backend.deps();
    let reader = &StateReader::new(&deps, case.sub.clone(), descriptor)?;
    // The first read warms the publication snapshot. The rest share it.
    let len = reader.len(case.key.clone()).await?;
    let (empty, front, back, points, forward, backward, constrained, constrained_backward) = join!(
        reader.is_empty(case.key.clone()),
        reader.peek_front(case.key.clone()),
        reader.peek_back(case.key.clone()),
        all_match(0..=model.len(), |i| async move {
            Ok(reader.get(case.key.clone(), i).await? == model.get(i).cloned())
        }),
        collect_stream(reader.values(case.key.clone()).stream()),
        collect_stream(reader.values(case.key.clone()).reverse().stream()),
        collect_stream(
            reader
                .values(case.key.clone())
                .range(1..=3)
                .limit(NonZeroUsize::MIN)
                .stream()
        ),
        collect_stream(
            reader
                .values(case.key.clone())
                .reverse()
                .range(1..=3)
                .stream()
        ),
    );
    let empty = empty?;
    let front = front?;
    let back = back?;
    let points = points?;
    let forward = forward?;
    let backward = backward?;
    let constrained = constrained?;
    let constrained_backward = constrained_backward?;
    Ok(len == model.len()
        && empty == model.is_empty()
        && front == model.front().cloned()
        && back == model.back().cloned()
        && points
        && forward == model.iter().cloned().collect::<Vec<_>>()
        && backward == model.iter().rev().cloned().collect::<Vec<_>>()
        && constrained == model.iter().skip(1).take(1).cloned().collect::<Vec<_>>()
        && constrained_backward
            == model
                .iter()
                .take(4)
                .skip(1)
                .rev()
                .cloned()
                .collect::<Vec<_>>())
}

/// Drives a Deque trace: commit each event's push/pop/clear, mirror into a
/// `VecDeque`, and after every event assert the reader matches the model.
///
/// FALSIFICATION: shift the front-relative index in the reader's deque `get`
/// → element 0 diverges. Drop the first entry of the deque stream's batch
/// point read → the forward stream loses its front. This property never
/// reaches the wide committed-scan fallback, since trace deques stay under
/// `DEQUE_POINT_ITERATION_MAX`. That fallback is covered by the
/// live-Cassandra witness
/// [`reader_deque_scan_committed`](super::cassandra_tests). Do not re-add a
/// scan case here.
pub(super) async fn run_reader_deque_trace<B: ReaderBackend>(
    backend: &B,
    descriptor: DequeDescriptor<JsonCodec>,
    case: &ReaderCase<'_>,
    trace: Trace<DequeOp>,
) -> Result<bool> {
    let registry = backend.registry();
    let state_key = seed_source(backend, descriptor, case).await?;

    let mut model: VecDeque<Value> = VecDeque::new();
    for (index, ops) in trace.events_ops().enumerate() {
        let staged: Vec<DequeOp> = ops.to_vec();
        let for_handle = staged.clone();
        let commit = owner_commit_cell(
            backend.owner_cell(),
            &registry,
            &state_key,
            descriptor,
            index as u128,
            move |handle| async move { apply_deque_ops::<B>(&handle, for_handle).await },
        );
        Box::pin(commit).await?;
        model_deque_ops(&mut model, &staged);
        if !Box::pin(assert_deque(backend, descriptor, case, &model)).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

mod set;
pub(super) use set::run_reader_set_trace;

mod value;
pub(crate) use value::ValueOp;
pub(super) use value::run_reader_value_trace;
