//! An exhaustive search of the states that a small universe reaches.

use super::*;
use std::collections::{HashSet, VecDeque};

/// What a trace holds: whether a load read the unowned slab, and for each
/// coordinate its row, whether a delivery kept it, and its entry. The entry
/// is the tag of the waiting attempt, whether that attempt is queued, and
/// whether a delivery runs with its row deleted.
type State = (
    bool,
    Vec<(Stored, bool, Option<(Option<(i32, bool)>, Option<bool>)>)>,
);

/// Checks every operation from every state that operations on three
/// coordinates reach from empty memory. The first set interacts in owned
/// slabs and one unowned slab; the second set adds a second timer type.
#[test]
fn every_reachable_state_keeps_the_invariants() -> Result<()> {
    explore(&[At(0, 0), At(1, 0), At(UNOWNED, 0)])?;
    explore(&[At(0, 0), At(UNOWNED, 0), At(0, 1)])
}

/// Searches the states breadth first. Each step rebuilds memory from the
/// state, because a delay queue cannot be cloned.
fn explore(coordinates: &[At]) -> Result<()> {
    let runtime = runtime()?;
    let _runtime = runtime.enter();
    let ops = alphabet(coordinates);
    let start = state(&Trace::default());
    let mut seen = HashSet::from([start.clone()]);
    let mut frontier = VecDeque::from([(start, Vec::new())]);
    while let Some((from, path)) = frontier.pop_front() {
        for &op in &ops {
            let mut trace = rebuild(&from);
            trace
                .step(op)
                .map_err(|error| error.wrap_err(format!("path {path:?}, then {op:?}")))?;
            let next = state(&trace);
            if seen.insert(next.clone()) {
                let mut path = path.clone();
                path.push(op);
                frontier.push_back((next, path));
            }
        }
    }
    Ok(())
}

/// Every operation on `coordinates` with two tags. A pop during a row change
/// matters only to an insert, whose coordinate may hold an attempt.
fn alphabet(coordinates: &[At]) -> Vec<Op> {
    const OUTCOMES: [Outcome; 3] = [Outcome::Unchanged, Outcome::Unknown, Outcome::Done];
    const LOADS: [During; 2] = [During::Nothing, During::Load];
    let types: BTreeSet<_> = coordinates.iter().map(|at| at.1).collect();
    let mut ops = Vec::new();
    for ty in types {
        for outcome in OUTCOMES {
            ops.extend(LOADS.map(|during| Op::UnscheduleAll(ty, outcome, during)));
        }
    }
    for &at in coordinates {
        ops.extend([Op::Pop(at), Op::Load(at)]);
        for tag in [0_i32, 1_i32] {
            ops.extend([Op::Abort(at, tag), Op::Fire(at, tag)]);
        }
        for outcome in OUTCOMES {
            ops.push(Op::Complete(at, outcome));
            ops.extend(LOADS.map(|during| Op::Unschedule(at, outcome, during)));
            for during in [During::Nothing, During::Load, During::Pop] {
                for tag in [0_i32, 1_i32] {
                    for insert in [Insert::Schedule, Insert::Clear, Insert::Repair] {
                        ops.push(Op::Insert(at, insert, tag, outcome, during));
                    }
                }
            }
        }
    }
    ops
}

/// What `trace` holds, for the search to compare states.
fn state(trace: &Trace) -> State {
    let held = coordinates().map(|(time, ty)| {
        let entry = trace.entry(At(time, ty)).map(|entry| {
            let next = entry.next.as_ref();
            let firing = entry.firing.map(|delivery| delivery == Delivery::Deleted);
            (
                next.map(|next| (next.trigger.tag, next.queued.is_some())),
                firing,
            )
        });
        (trace.rows[time][ty], trace.kept[time][ty], entry)
    });
    (trace.loaded, held.collect())
}

/// Builds the trace that `state` describes.
fn rebuild((loaded, held): &State) -> Trace {
    let mut trace = Trace {
        loaded: *loaded,
        ..Trace::default()
    };
    let Timers { entries, queue } = &mut trace.timers;
    for ((time, ty), &(row, kept, entry)) in coordinates().zip(held) {
        trace.rows[time][ty] = row;
        trace.kept[time][ty] = kept;
        let Some((next, firing)) = entry else {
            continue;
        };
        let (_, slot) = get_or_insert(entries, &key(), time_of(time), TYPES[ty]);
        slot.next = next.map(|(tag, queued)| {
            let mut next = Next::queue(queue, trigger(At(time, ty), tag));
            if !queued && let Some(item) = next.queued.take() {
                queue.try_remove(&item);
            }
            next
        });
        slot.firing = firing.map(|deleted| {
            if deleted {
                Delivery::Deleted
            } else {
                Delivery::Stored
            }
        });
    }
    trace
}
