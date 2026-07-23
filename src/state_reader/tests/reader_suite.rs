//! The backend-generic committed-read trace runner — the reader analogue of
//! [`cell_suite`](crate::state::tests::cell_suite)'s `run_*_trace` family.
//!
//! Each runner drives a generated multi-event trace: every event is committed
//! through the **real** owner
//! [`KeyedStateSession`](crate::state::session::KeyedStateSession)
//! (via [`owner_commit_cell`]), the same ops advance a plain
//! `Option`/`BTreeMap`/`VecDeque` model, and after **every** event a freshly
//! minted [`StateReader`] must answer point `get`, `get_many`, `stream`
//! (forward and backward), and `len` exactly as the model — the
//! committed==oracle invariant. Written once over a generic
//! [`ReaderBackend`] and instantiated for both the memory reader
//! (`reader_tests`) and a **live-Cassandra** reader (`cassandra_tests`).
//!
//! The trace generators are reused wholesale from
//! [`collection_suite`](crate::state::tests::collection_suite) (`MapOp`,
//! `DequeOp`, `Trace`, `KEY_POOL`); only the degenerate [`ValueOp`] is new,
//! since a Value has no removal. The runner ignores the generators' mid-handler
//! `Commit`/`Get` ops — a reader observes committed state, and the runner
//! promotes every event — so no new outcome path is generated.
//!
//! Witness guarantee (why the "drop first scan yield" falsification cannot
//! false-pass by shrinking to empty): the ordered `stream` is asserted against
//! the ordered model after **every** event, empty or not. Map/Deque generators
//! are weighted toward `Set`/`Push`, so a non-empty, ordered, multi-entry state
//! recurs; a counterexample keeps its witness because `Trace` shrink preserves
//! event structure. An empty read is itself a real assertion (`stream` yields
//! nothing, `get` is `None`).

use super::support::{
    OwnerSession, ReaderBackend, owner_commit_cell, source_state_key, state_name,
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
use crate::state::tests::collection_suite::{DequeOp, KEY_POOL, MapOp, Trace};
use crate::state_reader::{PartitionCount, StateReader, StateReaderError};
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, eyre};
use futures::{Stream, StreamExt};
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

/// The fixed namespace one trace runs under: the routing coordinates the owner
/// writes and the reader recomputes. Bundled so a runner takes one argument
/// instead of five.
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

/// A degenerate value mutation: overwrite with a JSON number. A Value has no
/// removal, so `Set` is the whole alphabet — enough for the committed
/// round-trip (the reader either observes the last committed value or `None`
/// before the first commit).
#[derive(Clone, Copy, Debug)]
pub(crate) enum ValueOp {
    /// Overwrite the committed value with `Value::from(b)`.
    Set(u8),
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self::Set(u8::arbitrary(g))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self::Set(b) = *self;
        Box::new(b.shrink().map(Self::Set))
    }
}

/// Seeds the publication + frozen identity for `descriptor` so the reader
/// admits the case's source, then returns the segment-qualified state key the
/// owner writes under (and the reader recomputes).
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

/// Collects the reader stream for `key`/`dir` into a `Vec`, surfacing the first
/// error.
async fn collect_stream<T>(
    stream: impl Stream<Item = Result<T, StateReaderError>>,
) -> Result<Vec<T>> {
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

/// Drives a Value trace: commit each event, mirror it into an `Option<Value>`
/// model, and after every event assert `reader.get(key)` equals the model.
///
/// FALSIFICATION: perturb `ReadSession::collection_id_for` (session.rs) to bind
/// the wrong partition/state-type → the point `get` reads an empty/foreign
/// collection → mismatch on the first committed event.
pub(crate) async fn run_reader_value_trace<B: ReaderBackend>(
    backend: &B,
    descriptor: ValueDescriptor<JsonCodec>,
    case: &ReaderCase<'_>,
    trace: Trace<ValueOp>,
) -> Result<bool> {
    let registry = backend.registry();
    let state_key = seed_source(backend, descriptor, case).await?;

    let mut model: Option<Value> = None;
    for (index, ops) in trace.events_ops().enumerate() {
        let staged: Vec<ValueOp> = ops.to_vec();
        let for_handle = staged.clone();
        owner_commit_cell(
            backend.owner_cell(),
            &registry,
            &state_key,
            descriptor,
            index as u128,
            move |handle| async move {
                for ValueOp::Set(b) in for_handle {
                    handle
                        .set(Value::from(b))
                        .await
                        .map_err(|e| eyre!("set: {e}"))?;
                }
                Ok(())
            },
        )
        .await?;
        for ValueOp::Set(b) in staged {
            model = Some(Value::from(b));
        }

        let deps = backend.deps();
        let reader = StateReader::new(&deps, case.sub.clone(), descriptor)?;
        if reader.get(case.key.clone()).await? != model {
            return Ok(false);
        }
    }
    Ok(true)
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
                .set(k, Value::from(b))
                .await
                .map_err(|e| eyre!("set: {e}"))?,
            MapOp::Remove(k) => handle.remove(&k).await.map_err(|e| eyre!("remove: {e}"))?,
            MapOp::Clear => handle.clear().await.map_err(|e| eyre!("clear: {e}"))?,
            MapOp::Get(_) | MapOp::Commit => {}
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
            MapOp::Get(_) | MapOp::Commit => {}
        }
    }
}

/// Asserts the reader's point `get`, `get_many`, and ordered `stream` (forward
/// and backward) over the whole key pool equal the `BTreeMap` model.
async fn assert_map<B: ReaderBackend>(
    backend: &B,
    descriptor: MapDescriptor<I64KeyCodec, JsonCodec>,
    case: &ReaderCase<'_>,
    model: &BTreeMap<i64, Value>,
) -> Result<bool> {
    let deps = backend.deps();
    let reader = StateReader::new(&deps, case.sub.clone(), descriptor)?;
    for &k in &KEY_POOL {
        if reader.get(case.key.clone(), &k).await? != model.get(&k).cloned() {
            return Ok(false);
        }
    }
    let expect_many: Vec<Option<Value>> = KEY_POOL.iter().map(|k| model.get(k).cloned()).collect();
    if reader.get_many(case.key.clone(), &KEY_POOL).await? != expect_many {
        return Ok(false);
    }
    let expect_forward: Vec<(i64, Value)> = model.iter().map(|(k, v)| (*k, v.clone())).collect();
    let forward = Box::pin(collect_stream(
        reader.stream(case.key.clone(), Direction::Forward),
    ))
    .await?;
    if forward != expect_forward {
        return Ok(false);
    }
    let backward = Box::pin(collect_stream(
        reader.stream(case.key.clone(), Direction::Backward),
    ))
    .await?;
    let mut expect_backward = expect_forward;
    expect_backward.reverse();
    Ok(backward == expect_backward)
}

/// Drives a Map trace: commit each event's `Set`/`Remove`/`Clear`, mirror into
/// a `BTreeMap`, and after every event assert the reader matches the model.
///
/// FALSIFICATION: in `ReaderStores::scan_committed`/`CassandraCellResources::
/// scan_committed`, skip the first present yield → the ordered `stream` loses
/// its front element → the `Vec` comparison reds on the first non-empty event.
pub(crate) async fn run_reader_map_trace<B: ReaderBackend>(
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

/// Asserts the reader's `len`, front-relative `get` (incl. the one-past-the-end
/// `None`), and ordered `stream` (forward and backward) equal the `VecDeque`.
async fn assert_deque<B: ReaderBackend>(
    backend: &B,
    descriptor: DequeDescriptor<JsonCodec>,
    case: &ReaderCase<'_>,
    model: &VecDeque<Value>,
) -> Result<bool> {
    let deps = backend.deps();
    let reader = StateReader::new(&deps, case.sub.clone(), descriptor)?;
    if reader.len(case.key.clone()).await? != model.len() {
        return Ok(false);
    }
    for i in 0..=model.len() {
        if reader.get(case.key.clone(), i).await? != model.get(i).cloned() {
            return Ok(false);
        }
    }
    let forward = Box::pin(collect_stream(
        reader.stream(case.key.clone(), Direction::Forward),
    ))
    .await?;
    if forward != model.iter().cloned().collect::<Vec<_>>() {
        return Ok(false);
    }
    let backward = Box::pin(collect_stream(
        reader.stream(case.key.clone(), Direction::Backward),
    ))
    .await?;
    Ok(backward == model.iter().rev().cloned().collect::<Vec<_>>())
}

/// Drives a Deque trace: commit each event's push/pop/clear, mirror into a
/// `VecDeque`, and after every event assert the reader matches the model.
///
/// FALSIFICATION: shifting the front-relative index in the reader's deque `get`
/// → element 0 diverges; dropping the first scan yield → the forward stream
/// loses its front.
pub(crate) async fn run_reader_deque_trace<B: ReaderBackend>(
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
