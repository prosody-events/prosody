//! The multi-section property: read-committed stages over many sections match
//! an independent model.

use super::stage::{StageOp, cell_in};
use super::{Fixture, Session, message, promote_receipt};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::state::cell::Values;
use crate::state::cell_key::{CellKey, Section};
use crate::state::session::Finalized;
use crate::state::session::Promoted;
use crate::state::session::sealed::StateLifecycle;
use crate::state::store::CellRead;
use crate::state::tests::support::admit_collection;
use crate::state::{CollectionRef, StateName, StateType};
use crate::test_util::TEST_RUNTIME;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::{HashMap, HashSet};

/// One event's outcome in the multi-section equivalence trace.
#[derive(Clone, Copy, Debug)]
enum MultiOutcome {
    /// Commit and promote; `fail_promote` poisons the promote so it reports
    /// `Incomplete` and the committed projection heals through the resolving
    /// read window.
    Commit { fail_promote: bool },
    /// Stage then roll back — the committed projection is unchanged.
    Abort,
    /// Drop the receipt, discard the dirty buffer, then re-apply and commit —
    /// the retry re-stages over its own unsettled provisional cells' base.
    Retry,
}

impl Arbitrary for MultiOutcome {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Abort,
            1 => Self::Retry,
            2 => Self::Commit { fail_promote: true },
            _ => Self::Commit {
                fail_promote: false,
            },
        }
    }
}

/// One event: a short op list and its outcome.
#[derive(Clone, Debug)]
struct MultiEvent {
    ops: Vec<StageOp>,
    outcome: MultiOutcome,
}

impl Arbitrary for MultiEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: Vec::<StageOp>::arbitrary(g).into_iter().take(6).collect(),
            outcome: MultiOutcome::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        Box::new(self.ops.shrink().map(move |ops| Self { ops, outcome }))
    }
}

/// A trace of multi-section RC events over one key.
#[derive(Clone, Debug)]
struct MultiTrace {
    events: Vec<MultiEvent>,
}

impl Arbitrary for MultiTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: Vec::<MultiEvent>::arbitrary(g)
                .into_iter()
                .take(6)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// A staging op with its byte resolved to a trace-unique value, so every cell
/// with an unsettled committed base has a DISTINCT base — the condition that
/// makes a reversed base pairing observable.
#[derive(Clone, Debug)]
enum ConcreteOp {
    Set(CellKey, u8),
    Clear(CellKey),
    ClearSection(Section),
}

/// Assigns each `Set` a trace-unique byte (from `next`), so re-applying the
/// same list on a retry reproduces the identical bases.
fn concrete_ops(ops: &[StageOp], next: &mut u8) -> Vec<ConcreteOp> {
    ops.iter()
        .map(|op| match *op {
            StageOp::Set { section, coord, .. } => {
                let byte = *next;
                *next = next.wrapping_add(1);
                ConcreteOp::Set(cell_in(section as i8, coord), byte)
            }
            StageOp::Clear { section, coord } => ConcreteOp::Clear(cell_in(section as i8, coord)),
            StageOp::ClearSection { section } => {
                ConcreteOp::ClearSection(Section::new(section as i8))
            }
        })
        .collect()
}

/// Applies concrete ops to the session.
async fn apply_concrete(session: &Session, name: &StateName, ops: &[ConcreteOp]) {
    for op in ops {
        match op {
            ConcreteOp::Set(cell, byte) => {
                session
                    .seed(StateType::Application, name, cell, Some(&[*byte]))
                    .await;
            }
            ConcreteOp::Clear(cell) => {
                session.seed(StateType::Application, name, cell, None).await;
            }
            ConcreteOp::ClearSection(section) => {
                session
                    .seed_section_clear(StateType::Application, name, *section)
                    .await;
            }
        }
    }
}

/// The event's net surviving `Set` cells and cleared sections (mirroring
/// [`replay_dirty`] over concrete ops): a `Clear` removes its cell, a
/// `ClearSection` drops the whole section's buffered cells.
fn replay_concrete(ops: &[ConcreteOp]) -> (HashMap<CellKey, Bytes>, HashSet<Section>) {
    let mut cells: HashMap<CellKey, Bytes> = HashMap::new();
    let mut cleared: HashSet<Section> = HashSet::new();
    for op in ops {
        match op {
            ConcreteOp::Set(cell, byte) => {
                cells.insert(cell.clone(), Bytes::copy_from_slice(&[*byte]));
            }
            ConcreteOp::Clear(cell) => {
                cells.remove(cell);
            }
            ConcreteOp::ClearSection(section) => {
                cleared.insert(*section);
                cells.retain(|c, _| c.section != *section);
            }
        }
    }
    (cells, cleared)
}

/// Advances the committed-projection model by one committed event: erase every
/// cleared section's committed rows, delete each surviving `Clear`'s committed
/// row, then write each surviving `Set`'s bytes.
fn commit_into_model(
    model: &mut HashMap<CellKey, Bytes>,
    cells: &HashMap<CellKey, Bytes>,
    cleared: &HashSet<Section>,
    surviving_clears: &HashSet<CellKey>,
) {
    for section in cleared {
        model.retain(|c, _| c.section != *section);
    }
    for cell in surviving_clears {
        model.remove(cell);
    }
    for (cell, bytes) in cells {
        model.insert(cell.clone(), bytes.clone());
    }
}

/// Drives a multi-section RC trace through the real lifecycle, asserting the
/// committed projection tracks the model after every event across commit /
/// abort / retry outcomes and multiple sections. Distinct per-cell bases make a
/// reversed base pairing diverge here.
async fn run_multi_section(trace: MultiTrace) -> Result<()> {
    let fx = Fixture::new()?;
    let name = fx.value_name.clone();
    let mut model: HashMap<CellKey, Bytes> = HashMap::new();
    let mut all: HashSet<CellKey> = HashSet::new();
    let mut next_byte: u8 = 1;

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup) = message(index as u128 + 1);
        let concrete = concrete_ops(&ev.ops, &mut next_byte);
        let (cells, cleared) = replay_concrete(&concrete);
        // Cells a surviving `Clear` deletes (present as an explicit absence in
        // the net state: named by a clear, not re-set, in a non-cleared
        // section). Reconstruct from the concrete ops for the commit model.
        let mut surviving_clears: HashSet<CellKey> = HashSet::new();
        for op in &concrete {
            match op {
                ConcreteOp::Clear(cell) if !cleared.contains(&cell.section) => {
                    surviving_clears.insert(cell.clone());
                }
                ConcreteOp::Set(cell, _) => {
                    surviving_clears.remove(cell);
                }
                _ => {}
            }
        }
        for op in &concrete {
            if let ConcreteOp::Set(cell, _) | ConcreteOp::Clear(cell) = op {
                all.insert(cell.clone());
            }
        }

        {
            let scope = fx.session(event);
            let session = scope.handle();
            apply_concrete(&session, &name, &concrete).await;

            match ev.outcome {
                MultiOutcome::Commit { fail_promote } => {
                    let finalized = session.finalize().await?;
                    fx.dedup.insert(dedup).await?;
                    if let Finalized::Staged(staged) = finalized {
                        promote_receipt(&fx, staged, fail_promote).await?;
                    }
                    if !fail_promote {
                        commit_into_model(&mut model, &cells, &cleared, &surviving_clears);
                    }
                }
                MultiOutcome::Abort => {
                    if let Finalized::Staged(staged) = session.finalize().await? {
                        drop(staged);
                        admit_collection(
                            &fx.cell_store(),
                            &fx.dedup,
                            &CollectionRef::new(fx.value_id(), None),
                        )
                        .await?;
                    }
                }
                MultiOutcome::Retry => {
                    drop(session.finalize().await?);
                    session.discard_dirty();
                    admit_collection(
                        &fx.cell_store(),
                        &fx.dedup,
                        &CollectionRef::new(fx.value_id(), None),
                    )
                    .await?;
                    apply_concrete(&session, &name, &concrete).await;
                    let finalized = session.finalize().await?;
                    fx.dedup.insert(dedup).await?;
                    if let Finalized::Staged(staged) = finalized {
                        assert!(matches!(staged.promote(|| false).await, Promoted::Complete));
                    }
                    commit_into_model(&mut model, &cells, &cleared, &surviving_clears);
                }
            }
        }

        admit_collection(
            &fx.cell_store(),
            &fx.dedup,
            &CollectionRef::new(fx.value_id(), None),
        )
        .await?;
        for cell in &all {
            let committed =
                CellRead::<Values>::read(&fx.cell_store(), &fx.value_id(), cell.as_ref())
                    .await?
                    .0
                    .into_inner();
            let expected = model.get(cell).cloned();
            if committed != expected {
                bail!(
                    "after event {index}, committed {cell:?} = {committed:?}, expected \
                     {expected:?}"
                );
            }
        }
    }
    Ok(())
}

/// The multi-section RC equivalence property: the committed projection
/// converges to the model across commit / abort / retry over several sections,
/// with distinct per-cell bases so a reversed base pairing diverges.
#[test]
fn prop_multi_section_rc_equivalence() {
    fn prop(trace: MultiTrace) -> TestResult {
        match TEST_RUNTIME.block_on(run_multi_section(trace)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MultiTrace) -> TestResult);
}
