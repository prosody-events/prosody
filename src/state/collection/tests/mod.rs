//! Properties and pins for the scoped collection operations.
//!
//! The subject is the invocation, not any one collection: a probe layout and a
//! probe handle exercise the generated expansions and the write journal
//! directly, so the invariants hold for every collection that runs through the
//! same scope.
//!
//! The flagship is [`prop_write_invocations_are_atomic`], a trace/model
//! property over generated invocations. Its model is a plain map: the journal's
//! reverse-order fold must answer every in-invocation read, a successful merge
//! must leave the event overlay exactly at the model, and every other exit must
//! leave the overlay exactly as the invocation found it.
//!
//! The sibling [`plans`] module pins the managed stream drivers that a plan
//! feeds. It covers order, error termination, the per-emission fence, and the
//! resolve fan-out.

mod plans;

use super::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE,
    StateSession, collection_layout, collection_methods, decode_cell,
};
use crate::codec::{I64Codec, I64CodecError};
use crate::consumer::middleware::RepinProof;
use crate::loader::MemoryLoader;
use crate::state::cached::Cached;
use crate::state::cell_key::CellKey;
use crate::state::descriptor::tests::{session_over, session_with_dirty, value_registry};
use crate::state::descriptor::{
    CellStateError, Keyed, StateDescriptor, StructuralIdentity, ValueDescriptor, value_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::fjall::test_db;
use crate::state::identity::CollectionId;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::registry::CollectionDefRegistry;
use crate::state::session::sealed::StateLifecycle;
use crate::state::store::CELL_BATCH;
use crate::state::tests::support::{CountingCellStore, FixedOracle};
use crate::state::{CollectionKindId, StateAccessError, StateKey, StateType};
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use educe::Educe;
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeMap;
use std::iter::{empty, once};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

/// The probe collection's registered name.
const PROBE: &str = "pair-probe";

/// The probe collection's cell type: an `i64` value addressed by an `i64` key.
type ProbeCell = Keyed<I64KeyCodec, I64Codec>;

/// The probe collection's error type — the shape a real collection's methods
/// return, so the write scope's `From<StateAccessError>` requirement is
/// exercised rather than assumed.
type ProbeError = CellStateError<I64CodecError>;

collection_layout! {
    /// A two-family probe layout with a deliberate gap between its ids and a
    /// deliberate mismatch between declaration order and id order.
    struct PairLayout {
        /// The higher id, declared first.
        #[id(3)]
        RIGHT: ProbeCell,
        /// The lower id, declared second.
        #[id(0)]
        LEFT: ProbeCell,
    }
}

/// A handle over the probe collection, shaped exactly like a real one.
#[derive(Educe)]
#[educe(Clone(bound = "S: Clone"))]
struct PairHandle<S> {
    cells: Collection<S, PairLayout>,
}

/// A stateful helper in the author-facing form: it takes the operation, never a
/// handle, so it cannot acquire admission of its own.
async fn read_family<C>(
    op: &mut C,
    family: CellFamily<C::Layout, ProbeCell>,
    key: i64,
) -> Result<Option<i64>, ProbeError>
where
    C: CollectionRead,
{
    op.get(family, &key).await
}

/// The mutating twin of [`read_family`].
fn stage_pair<C>(op: &mut C, key: i64, left: i64, right: i64) -> Result<(), ProbeError>
where
    C: CollectionWrite<Layout = PairLayout>,
{
    op.set(PairLayout::LEFT, &key, left)?;
    op.set(PairLayout::RIGHT, &key, right)
}

#[collection_methods(field = cells, session = S)]
impl<S> PairHandle<S>
where
    S: StateSession,
{
    /// One read command, issued through a free helper.
    #[read(op)]
    async fn left(&self, key: i64) -> Result<Option<i64>, ProbeError> {
        read_family(op, PairLayout::LEFT, key).await
    }

    /// Several commands in one invocation, including a read of a staged write.
    #[write(op)]
    async fn swap(&self, key: i64, left: i64, right: i64) -> Result<Option<i64>, ProbeError> {
        let previous = read_family(op, PairLayout::LEFT, key).await?;
        stage_pair(op, key, left, right)?;
        let staged = read_family(op, PairLayout::LEFT, key).await?;
        assert_eq!(staged, Some(left), "a staged write must read back");
        assert!(!op.journal_spilled(), "two mutations stay inline");
        Ok(previous)
    }

    /// An invocation that stages and then fails.
    #[write(op)]
    async fn stage_then_fail(&self, key: i64, left: i64, right: i64) -> Result<(), ProbeError> {
        stage_pair(op, key, left, right)?;
        Err(CellStateError::Access(StateAccessError::Unavailable))
    }

    /// Takes `LEFT[key]`, **swallows** the result, stages an unrelated write,
    /// and reports whether the take succeeded. The invocation therefore merges
    /// on both paths, and the overlay shows what the take staged.
    ///
    /// This is the only shape that exposes a *failed* command's journal
    /// contribution. An invocation that propagates the error drops the whole
    /// journal.
    #[write(op)]
    async fn take_swallowing(&self, key: i64, marker: i64) -> Result<bool, ProbeError> {
        let took = op.take(PairLayout::LEFT, &key).await.is_ok();
        op.set(PairLayout::RIGHT, &key, marker)?;
        Ok(took)
    }
}

/// The registered descriptor the probe binds against. A layout brand is
/// independent of the durable identity, so a Value-kind registration admits a
/// two-family probe.
fn probe_descriptor() -> ValueDescriptor<I64Codec> {
    value_state(PROBE)
}

/// Binds the probe handle over `session`.
fn bind_probe<S: StateSession>(session: &S) -> Result<PairHandle<S>> {
    let collection = Collection::bind(
        session,
        PROBE,
        StateType::Application,
        &StructuralIdentity::of::<I64Codec>(CollectionKindId::Value),
    )
    .map_err(|e| eyre!("probe bind failed: {e}"))?;
    Ok(PairHandle { cells: collection })
}

/// The overlay's staged cells for the probe collection, decoded back into the
/// model's shape. Read straight off the dirty store: nothing resolves, caches,
/// or repairs between a merge and this observation, so nothing can heal a
/// mutation that should not be there.
fn staged_state(dirty: &DirtyStore, id: &CollectionId) -> Result<BTreeMap<(i8, i64), Option<i64>>> {
    let mut state = BTreeMap::new();
    for (cell, data) in dirty.collection_snapshot(id) {
        let key = I64KeyCodec::decode(cell.coordinate.as_bytes())
            .map_err(|e| eyre!("staged coordinate did not decode: {e}"))?;
        let value = match data {
            Some(bytes) => Some(
                decode_cell::<I64Codec>(bytes)
                    .map_err(|e| eyre!("staged cell did not decode: {e}"))?,
            ),
            None => None,
        };
        state.insert((i8::from(cell.section), key), value);
    }
    Ok(state)
}

/// Which probe family a generated command addresses.
#[derive(Clone, Copy, Debug)]
enum Family {
    Left,
    Right,
}

impl Family {
    fn token(self) -> CellFamily<PairLayout, ProbeCell> {
        match self {
            Self::Left => PairLayout::LEFT,
            Self::Right => PairLayout::RIGHT,
        }
    }

    fn section(self) -> i8 {
        i8::from(self.token().section())
    }
}

impl Arbitrary for Family {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Left
        } else {
            Self::Right
        }
    }
}

/// One command inside a generated invocation.
#[derive(Clone, Debug)]
enum Command {
    Set(Family, i64, i64),
    Clear(Family, i64),
    Get(Family, i64),
    GetMany(Family, Vec<i64>),
    Contains(Family, i64),
    ContainsMany(Family, Vec<i64>),
    Take(Family, i64),
    ClearCollection,
}

impl Arbitrary for Command {
    fn arbitrary(g: &mut Gen) -> Self {
        // A tiny key pool, so overwrites, clear-then-read, and read-your-writes
        // actually occur inside one invocation.
        let key = i64::from(u8::arbitrary(g) % 3);
        let family = Family::arbitrary(g);
        match u8::arbitrary(g) % 8 {
            0 => Self::Set(family, key, i64::from(u8::arbitrary(g))),
            1 => Self::Clear(family, key),
            2 => Self::Get(family, key),
            3 => Self::GetMany(
                family,
                (0..u8::arbitrary(g) % 4)
                    .map(|_| i64::from(u8::arbitrary(g) % 3))
                    .collect(),
            ),
            4 => Self::Contains(family, key),
            5 => Self::ContainsMany(
                family,
                (0..u8::arbitrary(g) % 4)
                    .map(|_| i64::from(u8::arbitrary(g) % 3))
                    .collect(),
            ),
            6 => Self::Take(family, key),
            _ => Self::ClearCollection,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set(family, key, value) => Box::new(
                value
                    .shrink()
                    .map(move |value| Self::Set(family, key, value)),
            ),
            _ => Box::new(empty()),
        }
    }
}

/// How a generated invocation ends.
#[derive(Clone, Copy, Debug)]
enum Exit {
    /// The authored body returns `Ok`: the journal merges.
    Ok,
    /// The authored body returns `Err`: the journal is dropped.
    Err,
    /// The session is terminated before the body returns: the final fence
    /// refuses and the journal is dropped.
    Terminated,
}

impl Arbitrary for Exit {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::Err,
            1 => Self::Terminated,
            _ => Self::Ok,
        }
    }
}

/// One generated case: some already-staged state, one invocation's commands,
/// and how that invocation ends.
#[derive(Clone, Debug)]
struct Invocation {
    seeded: Vec<(Family, i64, i64)>,
    commands: Vec<Command>,
    exit: Exit,
}

impl Arbitrary for Invocation {
    fn arbitrary(g: &mut Gen) -> Self {
        let seeded = (0..u8::arbitrary(g) % 3)
            .map(|_| {
                (
                    Family::arbitrary(g),
                    i64::from(u8::arbitrary(g) % 3),
                    i64::from(u8::arbitrary(g)),
                )
            })
            .collect();
        // Zero to six commands: zero exercises the empty-journal invocation
        // (admission plus a no-op merge), six is past `JOURNAL_INLINE`, so the
        // spill path is generated as well as the inline one.
        let count = u8::arbitrary(g) % 7;
        let commands = (0..count).map(|_| Command::arbitrary(g)).collect();
        Self {
            seeded,
            commands,
            exit: Exit::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let seeded = self.seeded.clone();
        let exit = self.exit;
        Box::new(self.commands.shrink().map(move |commands| Self {
            seeded: seeded.clone(),
            commands,
            exit,
        }))
    }
}

/// What the invocation's journal fold should answer, and whether a whole-layout
/// reset merged with it.
#[derive(Clone, Default)]
struct Model {
    cells: BTreeMap<(i8, i64), Option<i64>>,
    reset: bool,
}

impl Model {
    /// The model's answer for one cell: the fold's last write, or absent when
    /// the model says nothing (the probe collection starts empty, so every
    /// value it can hold went through the model).
    fn visible(&self, family: Family, key: i64) -> Option<i64> {
        self.cells
            .get(&(family.section(), key))
            .copied()
            .unwrap_or_default()
    }

    /// The sections a merge of this model marks cleared.
    fn cleared(&self) -> Vec<i8> {
        if self.reset {
            <PairLayout as CollectionLayout>::SECTIONS
                .iter()
                .map(|section| i8::from(*section))
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Runs one invocation's commands against `op`, asserting every in-invocation
/// read against the model as it happens — so no later command can heal an
/// earlier divergence.
async fn run_commands<C>(
    op: &mut C,
    commands: &[Command],
    model: &mut Model,
) -> Result<(), ProbeError>
where
    C: CollectionWrite<Layout = PairLayout>,
{
    for command in commands {
        match command {
            &Command::Set(family, key, value) => {
                op.set(family.token(), &key, value)?;
                model.cells.insert((family.section(), key), Some(value));
            }
            &Command::Clear(family, key) => {
                op.clear(family.token(), &key);
                model.cells.insert((family.section(), key), None);
            }
            &Command::Get(family, key) => {
                assert_eq!(
                    op.get(family.token(), &key).await?,
                    model.visible(family, key),
                    "an in-invocation read folds the journal last-write-wins"
                );
            }
            Command::GetMany(family, keys) => {
                let expected: Vec<Option<i64>> = keys
                    .iter()
                    .map(|key| model.visible(*family, *key))
                    .collect();
                assert_eq!(
                    op.get_many(family.token(), keys).await?.into_vec(),
                    expected,
                    "a batch read answers every position from the same journal fold"
                );
            }
            &Command::Contains(family, key) => {
                assert_eq!(
                    op.contains(family.token(), &key).await?,
                    model.visible(family, key).is_some(),
                    "presence agrees with the journal fold, without resolving"
                );
            }
            Command::ContainsMany(family, keys) => {
                let expected: Vec<bool> = keys
                    .iter()
                    .map(|key| model.visible(*family, *key).is_some())
                    .collect();
                assert_eq!(
                    op.contains_many(family.token(), keys).await?.into_vec(),
                    expected,
                    "batch presence agrees with each journal-fold position"
                );
            }
            &Command::Take(family, key) => {
                assert_eq!(
                    op.take(family.token(), &key).await?,
                    model.visible(family, key),
                    "take answers from the journal fold, then clears"
                );
                model.cells.insert((family.section(), key), None);
            }
            Command::ClearCollection => {
                op.clear_collection();
                // A reset hides every section of the layout — including the
                // probe's reserved id gap — and the merge discards the
                // sections' already-staged cells.
                model.cells.clear();
                model.reset = true;
            }
        }
    }
    Ok(())
}

/// Drives one generated invocation against the real scope and a plain-map
/// model, asserting the in-invocation reads after every command and the
/// overlay's exact contents at exit.
async fn run_invocation(case: Invocation) -> Result<()> {
    let registry = value_registry(&probe_descriptor())?;
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
    let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
    let handle = bind_probe(&session)?;
    // The session's own key and the collection's own canonical name — never a
    // value the test invented for its bookkeeping.
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        handle.cells.name().clone(),
    );

    // Seed through real invocations, so the pre-state is exactly what the
    // production path leaves behind.
    let mut seeded = Model::default();
    for &(family, key, value) in &case.seeded {
        handle
            .cells
            .write(async move |op| op.set(family.token(), &key, value))
            .await?;
        seeded.cells.insert((family.section(), key), Some(value));
    }
    let before = staged_state(&dirty, &id)?;

    let commands = case.commands.clone();
    let exit = case.exit;
    let terminator = session.clone();
    let outcome: Result<Model, ProbeError> = handle
        .cells
        .write(async move |op| {
            let mut model = seeded;
            op.set(PairLayout::LEFT, &2, 1)?;
            model.cells.insert((Family::Left.section(), 2), Some(1));
            op.clear(PairLayout::LEFT, &2);
            model.cells.insert((Family::Left.section(), 2), None);
            assert_eq!(
                op.contains_many(PairLayout::LEFT, &[2, 3, 2])
                    .await?
                    .into_vec(),
                vec![false, false, false],
                "batch presence sees staged clears, absent keys, and duplicates"
            );
            run_commands(op, &commands, &mut model).await?;
            assert_eq!(
                op.journal_spilled(),
                op.journal_len() > JOURNAL_INLINE,
                "the journal leaves its inline capacity only when it must"
            );
            match exit {
                Exit::Ok => Ok(model),
                Exit::Err => Err(CellStateError::Access(StateAccessError::Unavailable)),
                Exit::Terminated => {
                    terminator.terminate();
                    Ok(model)
                }
            }
        })
        .await;

    let after = staged_state(&dirty, &id)?;
    let cleared: Vec<i8> = dirty
        .cleared_sections(&id)
        .into_iter()
        .map(i8::from)
        .collect();
    match (case.exit, outcome) {
        (Exit::Ok, Ok(model)) => {
            assert_eq!(
                after, model.cells,
                "a successful merge replays the journal onto the overlay exactly"
            );
            assert_eq!(
                cleared,
                model.cleared(),
                "a merged reset marks every declared section, and nothing else marks any"
            );
        }
        (Exit::Err | Exit::Terminated, Err(_)) => {
            assert_eq!(
                after, before,
                "a failed or fenced invocation leaves the overlay untouched"
            );
            assert!(
                cleared.is_empty(),
                "a failed or fenced invocation stages no section clear"
            );
        }
        (exit, outcome) => {
            return Err(eyre!(
                "invocation ended as {exit:?} but returned ok={}",
                outcome.is_ok()
            ));
        }
    }
    Ok(())
}

/// Invariant: a write invocation is atomic. Every in-invocation read folds the
/// journal in reverse order; a successful merge replays it forward onto the
/// event overlay exactly; and an authored error or a fenced final validation
/// leaves the overlay exactly as the invocation found it.
#[test]
fn prop_write_invocations_are_atomic() {
    fn property(case: Invocation) -> TestResult {
        let described = format!("{case:?}");
        match TEST_RUNTIME.block_on(run_invocation(case)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{described}: {error}")),
        }
    }
    QuickCheck::new().quickcheck(property as fn(Invocation) -> TestResult);
}

/// The generated read and write expansions run, not merely typecheck: a
/// multi-command write stages both families and a later invocation sees them.
#[test]
fn generated_expansions_stage_and_read_real_values() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;

        assert_eq!(handle.left(7).await?, None, "an unset family reads absent");
        assert_eq!(
            handle.swap(7, 11, 22).await?,
            None,
            "the first swap reports no previous value"
        );
        assert_eq!(handle.left(7).await?, Some(11), "the staged left value");
        assert_eq!(
            handle.swap(7, 33, 44).await?,
            Some(11),
            "the second swap reports the first swap's value"
        );
        Ok(())
    })
}

/// A failed invocation leaves the overlay exactly as it was — observed on the
/// raw dirty store, with nothing in between that could heal it.
#[test]
fn failed_write_leaves_the_overlay_unchanged() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        let id = CollectionId::new(
            state_key,
            StateType::Application,
            handle.cells.name().clone(),
        );

        handle.swap(1, 10, 20).await?;
        let before = staged_state(&dirty, &id)?;
        assert!(
            handle.stage_then_fail(1, 99, 98).await.is_err(),
            "the invocation must surface its authored error"
        );
        assert_eq!(
            staged_state(&dirty, &id)?,
            before,
            "the failed invocation's mutations must not reach the overlay"
        );
        Ok(())
    })
}

/// A `take` whose read fails stages nothing. The addressed cell keeps the bytes
/// that did not decode. A staged clear would instead have replayed an absence.
///
/// The probe **swallows** the take's error and returns `Ok`. That is the only
/// shape that exposes a failed command's journal contribution, because an
/// invocation that propagates the error drops the whole journal. The test seeds
/// the bytes straight into the overlay, then reads the overlay back raw. No
/// layer between them can heal a clear that must not be there.
#[test]
fn take_error_does_not_clear() -> Result<()> {
    /// Bytes no `I64Codec` cell can decode.
    const BAD: &[u8] = b"not an i64";
    const KEY: i64 = 5;

    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        let name = handle.cells.name().clone();
        let id = CollectionId::new(state_key, StateType::Application, name.clone());
        let left = CellKey {
            section: PairLayout::LEFT.section(),
            coordinate: I64KeyCodec::encode(&KEY),
        };
        session
            .seed(StateType::Application, &name, &left, Some(BAD))
            .await;

        assert!(
            !handle.take_swallowing(KEY, 42).await?,
            "the seeded bytes must not decode, so the take must have failed"
        );

        let staged: BTreeMap<CellKey, Option<Bytes>> =
            dirty.collection_snapshot(&id).into_iter().collect();
        assert_eq!(
            staged
                .get(&left)
                .and_then(Option::as_ref)
                .map(Bytes::as_ref),
            Some(BAD),
            "a failed take must leave the addressed cell exactly as it found it"
        );
        assert!(
            staged.contains_key(&CellKey {
                section: PairLayout::RIGHT.section(),
                coordinate: I64KeyCodec::encode(&KEY),
            }),
            "the invocation still merged the write staged after the failed take"
        );
        Ok(())
    })
}

/// A cancelled invocation drops its journal and releases admission: the overlay
/// is unchanged and the next invocation acquires the gate immediately.
///
/// The invocation future is polled to `Pending` inside its scope and then
/// dropped — no clock and no sleep.
#[test]
fn cancelled_write_drops_the_journal_and_releases_admission() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key.clone());
        let handle = bind_probe(&session)?;
        handle.swap(2, 1, 2).await?;
        let id = CollectionId::new(
            state_key,
            StateType::Application,
            handle.cells.name().clone(),
        );
        let before = staged_state(&dirty, &id)?;

        let parked = Notify::new();
        let mut invocation = Box::pin(handle.cells.write(async |op| {
            op.set(PairLayout::LEFT, &2, 77)?;
            parked.notified().await;
            Ok::<(), ProbeError>(())
        }));
        assert!(
            futures::poll!(invocation.as_mut()).is_pending(),
            "the invocation must park inside its scope"
        );
        drop(invocation);

        assert_eq!(
            staged_state(&dirty, &id)?,
            before,
            "a cancelled invocation stages nothing"
        );
        // Admission is RAII: this call would hang if the dropped invocation had
        // leaked its permit, so completing it is the assertion.
        assert_eq!(
            handle.left(2).await?,
            Some(1),
            "the pre-cancel value stands"
        );
        Ok(())
    })
}

/// Steady-state I/O budget: a warm value read performs zero lower-store reads,
/// and opening a fresh operation does not change that — admission is not a
/// cache boundary.
#[test]
fn warm_reads_perform_no_additional_lower_reads() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let descriptor: ValueDescriptor<I64Codec> = value_state("warm-value");
        let registry = value_registry(&descriptor)?;
        let lower = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let cached = Cached::new(test_db::cache("collection-warm")?, lower.clone());
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("warm-key"));
        let session = session_over(MemoryLoader::new(), registry, state_key, cached);

        let handle = descriptor
            .bind(&session)
            .map_err(|e| eyre!("bind failed: {e}"))?;
        handle.set(7).await?;
        handle.commit().await?;

        assert_eq!(
            handle.get().await?,
            Some(7),
            "the committed value reads back"
        );
        let warm = lower.lower_reads();
        assert_eq!(handle.get().await?, Some(7), "the warm re-read");
        assert_eq!(
            lower.lower_reads(),
            warm,
            "a warm re-read performs no lower-store read"
        );

        let fresh = descriptor
            .bind(&session)
            .map_err(|e| eyre!("re-bind failed: {e}"))?;
        assert_eq!(fresh.get().await?, Some(7), "the fresh operation's read");
        assert_eq!(
            lower.lower_reads(),
            warm,
            "opening a new operation is not a cache boundary"
        );
        Ok(())
    })
}

/// A batch read stays index-aligned **across** the lower store's batch
/// boundary: a `CELL_BATCH`-crossing query answers every position, in input
/// order, with duplicates answered per position.
///
/// Deterministic because the generated property's key pool is tiny and its
/// queries never reach `CELL_BATCH`, so no random trace can cross the split
/// the sub-batching performs.
#[test]
fn batch_reads_stay_aligned_across_the_store_batch_boundary() -> Result<()> {
    // One past a full batch, so the query spans exactly two sub-batches and
    // lands on the 127/128/129 boundary.
    let populated = CELL_BATCH as i64 + 1;
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;
        handle
            .cells
            .write(async |op| {
                for key in 0..populated {
                    op.set(PairLayout::LEFT, &key, key * 10)?;
                }
                Ok::<(), ProbeError>(())
            })
            .await?;

        // The boundary key at both ends, so a dropped or reordered sub-batch
        // cannot be masked by a palindromic query.
        let queries: Vec<i64> = once(CELL_BATCH as i64)
            .chain(0..populated)
            .chain(once(CELL_BATCH as i64))
            .collect();
        let answers = handle
            .cells
            .read(async |op| op.get_many(PairLayout::LEFT, &queries).await)
            .await?;

        let expected: Vec<Option<i64>> = queries.iter().map(|key| Some(key * 10)).collect();
        assert_eq!(
            answers.into_vec(),
            expected,
            "every position of a batch-crossing read answers its own key"
        );
        Ok(())
    })
}

/// A managed stream leaked past its attempt fences on **exhaustion**: an empty
/// coordinate plan errors `Terminated` at its first pull rather than reporting
/// a clean end. The plan is captured before the bump, so the error can only
/// come from the driver's per-emission fence.
#[test]
fn empty_coordinate_plan_fences_on_exhaustion() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let registry = value_registry(&probe_descriptor())?;
        let state_key = StateKey::new(Uuid::new_v4(), Arc::from("probe-key"));
        let (session, _dirty) = session_with_dirty(MemoryLoader::new(), registry, state_key);
        let handle = bind_probe(&session)?;

        let plan = handle
            .cells
            .read(async |op| op.coordinates(PairLayout::LEFT, Vec::new()))
            .await;
        session.reset(RepinProof::for_test()).await;

        let stream = plan.entries();
        futures::pin_mut!(stream);
        match stream.next().await {
            Some(Err(CellStateError::Access(StateAccessError::Terminated))) => Ok(()),
            other => Err(eyre!(
                "a leaked empty plan must fence Terminated on exhaustion, got ok={}",
                other.is_some()
            )),
        }
    })
}

/// The generated section set follows the declared ids, not the declaration
/// order, and a family token addresses exactly its declared id.
#[test]
fn generated_layout_is_id_sorted() {
    assert_eq!(
        <PairLayout as CollectionLayout>::SECTIONS
            .iter()
            .map(|section| i8::from(*section))
            .collect::<Vec<_>>(),
        vec![0, 3],
        "the canonical section set is id-sorted"
    );
    assert!(
        <PairLayout as CollectionLayout>::RESERVED.is_empty(),
        "the probe layout has removed no family"
    );
    assert_eq!(
        i8::from(PairLayout::LEFT.section()),
        0,
        "a family token addresses its declared id"
    );
}
