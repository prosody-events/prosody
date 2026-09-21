//! The shared collection lifecycle property runner.

use super::*;

/// The warm backing shared across a trace's events, handed to the read-back
/// assertion so a kind whose invariant needs the raw cells (Map's
/// `KeysetPresence` check) can reach them.
pub(super) struct Backing<'a> {
    pub(super) cells: &'a MemoryCells,
    pub(super) state_key: &'a StateKey,
}

/// Drives a generated trace through the real [`KeyedStateSession`] lifecycle
/// for any collection kind — the Deque and Map suites differ only in the op
/// alphabet, the model, and the assertions, so everything else lives here once:
/// bind a fresh session per event, apply each op (`apply_op`, which also
/// asserts mid-trace `pop`/`get` returns and reports mid-handler commits),
/// `finalize`, resolve along the event's outcome (promote / rollback / crash →
/// admission), advance the model — the full scratch on a commit (or always, for
/// a `ReadUncommitted` collection, whose `finalize` commits everything), the
/// last `commit()`-landed snapshot otherwise — then assert the committed
/// collection through a fresh read-back session (`assert`, which absorbs any
/// kind-specific check such as Map's `KeysetPresence`).
pub(super) async fn run_collection_trace<D, O, M, Apply, Assert>(
    trace: Trace<O>,
    descriptor: D,
    name: &str,
    def: CollectionDef,
    apply_op: Apply,
    assert: Assert,
) -> Result<bool>
where
    D: StateDescriptor,
    O: Copy,
    M: Clone + Default,
    Apply: AsyncFn(&D::Handle<SuiteSession>, O, &mut M) -> Result<OpOutcome>,
    Assert: AsyncFn(&D::Handle<SuiteSession>, &M, &Backing<'_>) -> Result<bool>,
{
    let commit_mode = def.commit_mode;
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let (registry, collection_ref) = registry_and_ref(&descriptor, name, &state_key, def)?;
    let backing = Backing {
        cells: &cells,
        state_key: &state_key,
    };
    let mut model = M::default();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let event = EventRef::Message {
            dedup_id: Uuid::from_u128(index as u128),
        };
        let session = make_session(&cells, &dedup, &registry, &state_key, event);
        let handle = descriptor.bind(&session).map_err(|e| eyre!("bind: {e}"))?;

        let mut scratch = model.clone();
        // The scratch as of the last mid-handler `commit()` — durable
        // regardless of the event's outcome (the at-least-once `commit()`
        // contract).
        let mut commit_floor: Option<M> = None;
        for op in &ev.ops {
            match apply_op(&handle, *op, &mut scratch).await? {
                OpOutcome::Continue => {}
                OpOutcome::Committed => commit_floor = Some(scratch.clone()),
                OpOutcome::Mismatch => return Ok(false),
            }
        }

        let finalized = session
            .finalize()
            .await
            .map_err(|e| eyre!("finalize: {e}"))?;
        if ev.outcome.commits() {
            dedup
                .insert(event_dedup(event))
                .await
                .map_err(|e| eyre!("marker: {e}"))?;
        }
        if !resolve_event(
            session,
            finalized,
            ev.outcome,
            &cells,
            &dedup,
            &registry,
            &collection_ref,
        )
        .await?
        {
            return Ok(false);
        }
        model = if commit_mode == CommitMode::ReadUncommitted || ev.outcome.commits() {
            // ReadUncommitted commits everything at `finalize` — any outcome
            // that reached it takes the full scratch (nothing provisional
            // exists for admission to resolve).
            scratch
        } else {
            // Abort / crash-rollback revert only the post-commit
            // provisionals (their `prev` was captured after the `commit()`
            // landed); the `commit()`-landed snapshot is already committed.
            commit_floor.unwrap_or(model)
        };

        // Read back through a fresh session (clean overlay) — pure committed.
        let read = make_session(&cells, &dedup, &registry, &state_key, read_event(index));
        let read_handle = descriptor
            .bind(&read)
            .map_err(|e| eyre!("bind read: {e}"))?;
        if !assert(&read_handle, &model, &backing).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// How an event resolved. Weighted toward `Commit` so state accumulates, with
/// real coverage of the rollback and crash-recovery arms.
#[derive(Clone, Copy, Debug)]
pub(super) enum Outcome {
    /// Promoted inline.
    Commit,
    /// Admission rolls back the stage.
    Abort,
    /// Committed evidence survives a crash; admission promotes the residue.
    CrashCommitted,
    /// No committed evidence survives a crash; admission rolls back the stage.
    CrashAborted,
}

impl Outcome {
    /// Whether the event's writes become committed.
    pub(super) fn commits(self) -> bool {
        matches!(self, Self::Commit | Self::CrashCommitted)
    }
}

impl Arbitrary for Outcome {
    fn arbitrary(g: &mut Gen) -> Self {
        g.choose(&[
            Self::Commit,
            Self::Commit,
            Self::Commit,
            Self::Abort,
            Self::Abort,
            Self::CrashCommitted,
            Self::CrashAborted,
        ])
        .copied()
        .unwrap_or(Self::Commit)
    }
}

/// What one applied op observed: keep going, a mid-handler `commit()` landed
/// (the runner snapshots the scratch model as immediately durable), or a
/// return value diverged from the model (property failure).
pub(super) enum OpOutcome {
    Continue,
    Committed,
    Mismatch,
}

/// One deque mutation. Payloads are single `u8`s wrapped as JSON numbers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DequeOp {
    PushBack(u8),
    PushFront(u8),
    PopBack,
    PopFront,
    Clear,
    Commit,
}

impl Arbitrary for DequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => Self::PushBack(u8::arbitrary(g)),
            1 => Self::PushFront(u8::arbitrary(g)),
            2 => Self::PopBack,
            3 => Self::PopFront,
            4 => Self::Clear,
            _ => Self::Commit,
        }
    }
}

/// One map mutation, mid-trace read, whole-map clear, or mid-handler
/// `commit()` over the bounded key pool.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MapOp {
    Set(i64, u8),
    Remove(i64),
    Get(i64),
    IsEmpty,
    Clear,
    Commit,
}

impl Arbitrary for MapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = g.choose(&KEY_POOL).copied().unwrap_or(0);
        match u8::arbitrary(g) % 7 {
            0 | 1 => Self::Set(key, u8::arbitrary(g)),
            2 => Self::Remove(key),
            3 => Self::Get(key),
            4 => Self::IsEmpty,
            5 => Self::Clear,
            _ => Self::Commit,
        }
    }
}

/// One event: a batch of ops and its resolution.
#[derive(Clone, Debug)]
pub(super) struct Event<O> {
    pub(super) ops: Vec<O>,
    pub(super) outcome: Outcome,
}

impl<O: Arbitrary> Arbitrary for Event<O> {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_EVENT_OPS),
            outcome: Outcome::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        Box::new(self.ops.shrink().map(move |ops| Self { ops, outcome }))
    }
}

/// A shrinkable trace of events.
#[derive(Clone, Debug)]
pub(crate) struct Trace<O> {
    pub(super) events: Vec<Event<O>>,
}

impl<O: Arbitrary> Arbitrary for Trace<O> {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

impl<O> Trace<O> {
    /// The per-event op slices, in order.
    ///
    /// The `state_reader` test suite replays these ops but always promotes
    /// every event. A `StateReader` only observes committed state, so the
    /// per-event outcome does not matter there.
    pub(crate) fn events_ops(&self) -> impl Iterator<Item = &[O]> + '_ {
        self.events.iter().map(|event| event.ops.as_slice())
    }
}

/// A deque trace.
pub(crate) type DequeTrace = Trace<DequeOp>;

/// A map trace.
pub(crate) type MapTrace = Trace<MapOp>;
