//! Generated write invocations are atomic against a model.

use super::*;

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
                op.set(family.token().at(&key), value)?;
                model.cells.insert((family.section(), key), Some(value));
            }
            &Command::Clear(family, key) => {
                op.clear(family.token().at(&key));
                model.cells.insert((family.section(), key), None);
            }
            &Command::Get(family, key) => {
                assert_eq!(
                    read_family(op, family.token(), key).await?,
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
                let read = {
                    let bound = key;
                    op.contains(family.token(), &bound)
                };
                assert_eq!(
                    read.await?,
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
                let read = {
                    let bound = key;
                    op.take(family.token(), &bound)
                };
                assert_eq!(
                    read.await?,
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
            .write(async move |op| op.set(family.token().at(&key), value))
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
            op.set(PairLayout::LEFT.at(&2), 1)?;
            model.cells.insert((Family::Left.section(), 2), Some(1));
            op.clear(PairLayout::LEFT.at(&2));
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
