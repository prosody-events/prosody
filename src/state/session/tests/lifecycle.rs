//! The value lifecycle property: session operations across successive
//! events match an independent value model.

use super::{Fixture, Session, TestStore, message, promote_receipt};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::state::cell::Values;
use crate::state::cell_key::Section;
use crate::state::marker::{EventEvidence, EventMarker};
use crate::state::session::Finalized;
use crate::state::session::sealed::StateLifecycle;
use crate::state::tests::cell_suite::value_cell;
use crate::state::tests::support::{admit_collection, assert_no_settlement_residue};
use crate::state::{CollectionRef, EventRef, StateName, StateType, StoreOutcome};
use crate::test_util::TEST_RUNTIME;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail};
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

/// Cap on ops per event: enough for commit/rollback/mutate interleavings,
/// small enough that a failing trace stays readable.
const MAX_EVENT_OPS: usize = 4;

/// One event in the Value lifecycle trace: a short op list and a terminal
/// outcome. An empty op list is the skip event.
#[derive(Clone, Debug)]
struct ValueEvent {
    ops: Vec<ValueOp>,
    outcome: Outcome,
}

#[derive(Clone, Copy, Debug)]
enum ValueOp {
    Set(u8),
    Clear,
    /// The section-clear marker leg: buffers a dirty clear of the value's
    /// section, so the stage carries a durable marker with clears — a lone
    /// `ClearSection` produces a clears-only stage with an empty write set.
    ClearSection,
    /// The mid-handler write-through: everything buffered so far becomes
    /// durable immediately and survives every non-commit outcome.
    Commit,
    /// The mid-handler discard: everything buffered since the last `Commit`
    /// (or event start) vanishes; reads revert to the commit floor.
    Rollback,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// The success path. `fail_promote` schedules a transient promote failure
    /// for the event's settle, so it yields [`false`] and
    /// durable recovery must converge through the loop-tail resolving reads.
    Commit {
        fail_promote: bool,
    },
    Abort,
    /// The attempt-boundary discard (retry between attempts): the receipt is
    /// dropped and `discard_dirty` clears the buffer.
    Reset,
    /// The final-error path: the event ends with no `finalize` and no
    /// discard (settle's error arms never finalize). The buffered write must
    /// neither commit nor linger — only the scope's `Drop` clears it.
    Failed,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // Sets weighted up so state actually accumulates between commits.
        match u8::arbitrary(g) % 7 {
            0..=2 => Self::Set(u8::arbitrary(g)),
            3 => Self::Clear,
            4 => Self::ClearSection,
            5 => Self::Commit,
            _ => Self::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match *self {
            Self::Set(b) => Box::new(b.shrink().map(Self::Set)),
            Self::Clear | Self::ClearSection | Self::Commit | Self::Rollback => {
                quickcheck::empty_shrinker()
            }
        }
    }
}

impl Arbitrary for ValueEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut ops: Vec<ValueOp> = Vec::<ValueOp>::arbitrary(g)
            .into_iter()
            .take(MAX_EVENT_OPS)
            .collect();
        // Precondition steering: roughly a quarter of events open with a
        // Rollback on a provably empty buffer (event start), pinning the NoOp
        // arm; the unconditioned draws above place Rollback after Set/Clear for
        // the Applied arm.
        if ops.len() < MAX_EVENT_OPS && u8::arbitrary(g) % 4 == 0 {
            ops.insert(0, ValueOp::Rollback);
        }
        let outcome = match u8::arbitrary(g) % 5 {
            0 => Outcome::Reset,
            1 => Outcome::Abort,
            2 => Outcome::Failed,
            _ => Outcome::Commit {
                fail_promote: u8::arbitrary(g) % 4 == 0,
            },
        };
        Self { ops, outcome }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let outcome = self.outcome;
        // A scheduled promote failure shrinks toward the calmer schedule
        // first, then the op list shrinks as usual.
        let calmed = matches!(outcome, Outcome::Commit { fail_promote: true }).then(|| Self {
            ops: self.ops.clone(),
            outcome: Outcome::Commit {
                fail_promote: false,
            },
        });
        Box::new(
            calmed
                .into_iter()
                .chain(self.ops.shrink().map(move |ops| Self { ops, outcome })),
        )
    }
}

/// A shrinkable trace of Value events over one key.
#[derive(Clone, Debug)]
struct Trace {
    events: Vec<ValueEvent>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            events: Vec::<ValueEvent>::arbitrary(g)
                .into_iter()
                .take(40)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.events.shrink().map(|events| Self { events }))
    }
}

/// The scratch as of the last mid-handler `commit()` this event — durable
/// regardless of the event's outcome.
#[derive(Clone)]
enum Floor {
    /// No `commit()` landed this event; a rollback or non-commit outcome falls
    /// through to the pre-event committed value.
    Unset,
    /// The value the last `commit()` made durable.
    Committed(Option<Bytes>),
}

impl Floor {
    /// The durable value this floor tests, or `pre_event` when no `commit()`
    /// landed this event.
    fn resolve(&self, pre_event: Option<Bytes>) -> Option<Bytes> {
        match self {
            Self::Unset => pre_event,
            Self::Committed(value) => value.clone(),
        }
    }
}

/// The per-event projection the property tracks in lockstep with the session:
/// the running read (`scratch`), the last `commit()` snapshot (`floor`), and
/// whether anything is buffered since the last drain.
struct EventModel {
    scratch: Option<Bytes>,
    floor: Floor,
    buffered: bool,
}

/// The mid-handler drain's `Applied`/`NoOp` contract: `Applied` iff the buffer
/// held anything since the last drain.
fn expected_outcome(buffered: bool) -> StoreOutcome {
    if buffered {
        StoreOutcome::Applied
    } else {
        StoreOutcome::NoOp
    }
}

/// Applies one op to the session and `model`, returning `false` on a divergence
/// (a wrong `Applied`/`NoOp` outcome, or a session read that no longer tracks
/// the scratch model). `pre_event` is the committed value the event opened on —
/// the rollback fallback when no `commit()` landed this event.
async fn apply_value_op(
    session: &Session,
    name: &StateName,
    op: ValueOp,
    pre_event: Option<Bytes>,
    model: &mut EventModel,
) -> Result<bool> {
    match op {
        ValueOp::Set(byte) => {
            session
                .seed(StateType::Application, name, &value_cell(), Some(&[byte]))
                .await;
            model.scratch = Some(Bytes::copy_from_slice(&[byte]));
            model.buffered = true;
        }
        ValueOp::Clear => {
            session
                .seed(StateType::Application, name, &value_cell(), None)
                .await;
            model.scratch = None;
            model.buffered = true;
        }
        ValueOp::ClearSection => {
            // The value cell lives in section 0, so the dirty clear marker
            // masks it to absent within the event.
            session
                .seed_section_clear(StateType::Application, name, Section::new(0))
                .await;
            model.scratch = None;
            model.buffered = true;
        }
        ValueOp::Commit => {
            let outcome = {
                let permit = session.permit().await;
                session
                    .commit(&permit, StateType::Application, name)
                    .await?
            };
            if outcome != expected_outcome(model.buffered) {
                return Ok(false);
            }
            model.floor = Floor::Committed(model.scratch.clone());
            model.buffered = false;
        }
        ValueOp::Rollback => {
            let outcome = session.rollback(StateType::Application, name).await;
            if outcome != expected_outcome(model.buffered) {
                return Ok(false);
            }
            // Reads revert to the commit floor, or the pre-event committed
            // value if no commit() landed this event.
            model.scratch = model.floor.resolve(pre_event);
            model.buffered = false;
        }
    }
    // Equivalence after every operation: the session's own overlay read tracks
    // the scratch model, so a missed rollback discard or a lost buffered write
    // surfaces at the op that caused it.
    let read = session
        .get::<Values>(StateType::Application, name, value_cell().as_ref())
        .await?;
    Ok(read == model.scratch)
}

/// Clause of the central property: `finalize` returns `Clean` iff nothing was
/// buffered since the event's last drain, and a `Staged` receipt's frozen
/// records equal the durable event marker the stage wrote. This is
/// receipt/durable-marker CONSISTENCY (both sides freeze through
/// [`EventMarker::frozen`]); survivor semantics are owned by the
/// crash-equivalence suite's clears dimension in `state::tests::cell_suite`.
fn finalize_matches_model(
    fx: &Fixture,
    event: EventRef,
    finalized: &Finalized<TestStore, ()>,
    buffered: bool,
) -> Option<&'static str> {
    match finalized {
        Finalized::Clean if buffered => Some("finalize returned Clean over a buffered op"),
        Finalized::Staged(_) if !buffered => Some("finalize staged a receipt for a drained buffer"),
        Finalized::Clean => None,
        Finalized::Staged(staged) => {
            let [collection] = staged.collections.as_slice() else {
                return Some("the single-collection trace staged more than one record");
            };
            let expected = EventMarker::frozen(
                event,
                &collection.writes,
                collection.marker.clears(),
                &EventEvidence {
                    touched: vec![(StateType::Application, fx.value_id().name().clone())].into(),
                    evidence_ttl: CompactDuration::new(30),
                    dedup: collection.marker.dedup(),
                    attempt: collection.marker.attempt(),
                },
            );
            (fx.cells.unsettled_marker_of(&fx.value_id()) != Some(expected))
                .then_some("the receipt's frozen records diverge from the durable event marker")
        }
    }
}

/// Finalizes the event and checks the Clean-iff / receipt-consistency clause
/// ([`finalize_matches_model`]) plus drain-on-success — the stage consumed the
/// receipt's mint source, so a second finalize finds an empty buffer and
/// returns `Clean`. Yields the receipt; a divergence is an error.
async fn checked_finalize(
    fx: &Fixture,
    session: &Session,
    event: EventRef,
    buffered: bool,
) -> Result<Finalized<TestStore, ()>> {
    let finalized = session.finalize().await?;
    if let Some(reason) = finalize_matches_model(fx, event, &finalized, buffered) {
        bail!("{reason}");
    }
    if !matches!(session.finalize().await?, Finalized::Clean) {
        bail!("a second finalize after success was not Clean");
    }
    Ok(finalized)
}

/// Drives the trace through the real session lifecycle, checking the central
/// property's clauses (the module doc lists them): the session's own read
/// equals a plain `Option<Bytes>` model after every operation, `finalize`
/// answers `Clean` iff nothing was buffered (with the receipt's frozen records
/// matching the durable marker, and a second finalize `Clean`), the consumed
/// receipt converges, and the overlay + committed projections equal the model
/// after every event. Errors carry the divergence reason; `Ok(())` means the
/// trace upholds the property.
///
/// A mid-event `commit()` snapshots the scratch model as immediately durable:
/// on a commit the full scratch wins, on every other outcome the durable state
/// must equal the last `commit()`-landed snapshot (post-commit ops roll back;
/// `commit()`-landed ops survive) — the at-least-once `commit()` contract. A
/// `Rollback` reverts the scratch to the commit floor (or the pre-event
/// committed value) and must report `Applied` iff anything was buffered.
async fn run(trace: Trace) -> Result<()> {
    let fx = Fixture::new()?;
    let mut model: Option<Bytes> = None;
    let key = fx.state_key.key.clone();

    for (index, ev) in trace.events.into_iter().enumerate() {
        let (event, dedup_id) = message(index as u128 + 1);
        // The per-event projection, tracked in lockstep with the session.
        let mut ev_model = EventModel {
            scratch: model.clone(),
            floor: Floor::Unset,
            buffered: false,
        };
        // The scope drops at the end of this block — the production per-event
        // lifetime that clears the shared dirty buffer.
        {
            let scope = fx.session(event);
            let session = scope.handle();

            for op in &ev.ops {
                if !apply_value_op(&session, &fx.value_name, *op, model.clone(), &mut ev_model)
                    .await?
                {
                    bail!("an op outcome or read diverged from the model");
                }
            }

            match ev.outcome {
                Outcome::Commit { fail_promote } => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    // The driver simulates the settle boundary's marker
                    // record — a direct dedup write, strictly after the
                    // stage (the session exposes no marker write; the real
                    // one is settlement-module-private).
                    fx.dedup.insert(dedup_id).await?;
                    if let Finalized::Staged(staged) = finalized {
                        promote_receipt(&fx, staged, fail_promote).await?;
                    }
                    // Commit advances the model (last-writer-wins).
                    model = if fail_promote {
                        ev_model.floor.resolve(model)
                    } else {
                        ev_model.scratch
                    };
                }
                Outcome::Abort => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    if let Finalized::Staged(staged) = finalized {
                        drop(staged);
                        admit_collection(
                            &fx.cell_store(),
                            &fx.dedup,
                            &CollectionRef::new(fx.value_id(), None),
                        )
                        .await?;
                        // Same raw probe as `promote_receipt`'s healthy arm: a
                        // rollback that skipped its store call would be healed
                        // to identical bytes by the loop-tail resolving reads
                        // and masked.
                        assert_no_settlement_residue(&fx.cells, &fx.value_id())?;
                    }
                    // Post-commit ops roll back to their `prev`, which
                    // finalize captured *after* the `commit()` landed — the
                    // `commit()`-landed snapshot.
                    model = ev_model.floor.resolve(model);
                }
                Outcome::Reset => {
                    let finalized =
                        checked_finalize(&fx, &session, event, ev_model.buffered).await?;
                    // Dropping the receipt leaves any provisional written by
                    // `finalize` unsettled, projecting its `prev` (the
                    // `commit()`-landed snapshot, or the unchanged committed
                    // base) — exactly the discarded-stage behavior the
                    // attempt-boundary `discard_dirty` pairs with.
                    drop(finalized);
                    session.discard_dirty();
                    model = ev_model.floor.resolve(model);
                }
                // Final-error path: no `finalize`, no `reset`. Only the
                // scope's `Drop` clears the buffered write — but a `commit()`
                // already wrote its snapshot through, and it must survive.
                Outcome::Failed => {
                    model = ev_model.floor.resolve(model);
                }
            }
        }

        admit_collection(
            &fx.cell_store(),
            &fx.dedup,
            &CollectionRef::new(fx.value_id(), None),
        )
        .await?;

        // The shared dirty buffer is empty for the key — no per-event leak.
        if !fx.dirty.touched(&key).is_empty() {
            bail!("the shared dirty buffer leaked past the event");
        }
        // A fresh overlay read (the dirty short-circuit path) tracks the model:
        // a leaked dirty cell would surface here as a read of uncommitted state.
        if fx.overlay_value().await? != model {
            bail!("the overlay read diverged from the model");
        }
        // The committed projection still tracks the model.
        if fx.committed_value().await? != model {
            bail!("the committed projection diverged from the model");
        }
    }
    Ok(())
}

/// The central settlement property: the Value session lifecycle is sound over
/// random mixed-outcome traces — the module doc lists the clauses.
#[test]
fn prop_value_lifecycle_equivalence() {
    fn prop(trace: Trace) -> TestResult {
        match TEST_RUNTIME.block_on(run(trace)) {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}
