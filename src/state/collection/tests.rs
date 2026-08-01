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

use super::{
    CellFamily, Collection, CollectionLayout, CollectionRead, CollectionWrite, JOURNAL_INLINE,
    StateSession, collection_layout, collection_methods, decode_cell,
};
use crate::codec::{I64Codec, I64CodecError};
use crate::loader::MemoryLoader;
use crate::state::cached::Cached;
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
use crate::state::session::CellRead;
use crate::state::session::sealed::StateLifecycle;
use crate::state::tests::support::{CountingCellStore, FixedOracle};
use crate::state::{CollectionKindId, StateAccessError, StateKey, StateType};
use crate::test_util::TEST_RUNTIME;
use color_eyre::eyre::{Result, eyre};
use educe::Educe;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeMap;
use std::iter::empty;
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
}

/// The registered descriptor the probe binds against. A layout brand is
/// independent of the durable identity, so a Value-kind registration admits a
/// two-family probe.
fn probe_descriptor() -> ValueDescriptor<I64Codec> {
    value_state(PROBE)
}

/// Binds the probe handle over `session`.
fn bind_probe<S: CellRead>(session: &S) -> Result<PairHandle<S>> {
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
}

impl Arbitrary for Command {
    fn arbitrary(g: &mut Gen) -> Self {
        // A tiny key pool, so overwrites, clear-then-read, and read-your-writes
        // actually occur inside one invocation.
        let key = i64::from(u8::arbitrary(g) % 3);
        let family = Family::arbitrary(g);
        match u8::arbitrary(g) % 3 {
            0 => Self::Set(family, key, i64::from(u8::arbitrary(g))),
            1 => Self::Clear(family, key),
            _ => Self::Get(family, key),
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
    let mut model: BTreeMap<(i8, i64), Option<i64>> = BTreeMap::new();
    for &(family, key, value) in &case.seeded {
        handle
            .cells
            .write(async move |op| op.set(family.token(), &key, value))
            .await?;
        model.insert((family.section(), key), Some(value));
    }
    let before = staged_state(&dirty, &id)?;

    let commands = case.commands.clone();
    let expected = model;
    let exit = case.exit;
    let terminator = session.clone();
    let outcome: Result<BTreeMap<(i8, i64), Option<i64>>, ProbeError> = handle
        .cells
        .write(async move |op| {
            let mut model = expected;
            for command in &commands {
                match *command {
                    Command::Set(family, key, value) => {
                        op.set(family.token(), &key, value)?;
                        model.insert((family.section(), key), Some(value));
                    }
                    Command::Clear(family, key) => {
                        op.clear(family.token(), &key);
                        model.insert((family.section(), key), None);
                    }
                    Command::Get(family, key) => {
                        let seen = op.get(family.token(), &key).await?;
                        assert_eq!(
                            seen,
                            model
                                .get(&(family.section(), key))
                                .copied()
                                .unwrap_or_default(),
                            "an in-invocation read folds the journal last-write-wins"
                        );
                    }
                }
            }
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
    match (case.exit, outcome) {
        (Exit::Ok, Ok(model)) => assert_eq!(
            after, model,
            "a successful merge replays the journal onto the overlay exactly"
        ),
        (Exit::Err | Exit::Terminated, Err(_)) => assert_eq!(
            after, before,
            "a failed or fenced invocation leaves the overlay untouched"
        ),
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
