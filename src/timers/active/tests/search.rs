//! An exhaustive search of the states that a small universe reaches.

use super::*;
use color_eyre::eyre::eyre;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::thread::{available_parallelism, scope};

/// What a trace holds: whether a load read the unowned slab, and for each
/// coordinate its row, whether a delivery kept it, and its entry. The entry
/// is the tag of the waiting attempt and whether that attempt is queued, and
/// the tag of a running delivery and whether its row is deleted.
type State = (
    bool,
    Vec<(
        Stored,
        bool,
        Option<(Option<(i32, bool)>, Option<(i32, bool)>)>,
    )>,
);

/// Checks every operation from every state that operations on three
/// coordinates reach from empty memory. The first set interacts in owned
/// slabs and one unowned slab; the second set adds a second timer type.
#[test]
fn every_reachable_state_keeps_the_invariants() -> Result<()> {
    explore(&[At(0, 0), At(1, 0), At(UNOWNED, 0)])?;
    explore(&[At(0, 0), At(UNOWNED, 0), At(0, 1)])
}

/// Searches the states breadth first, and the workers share each level.
///
/// Memory compares tags only for equality, and it treats the two owned times
/// alike. So a swap of the tags, or of the owned times when the universe holds
/// both, maps the reachable states and the alphabet onto themselves. One state
/// of each orbit stands for the others. A failure names the state, which
/// [`rebuild`] makes again.
fn explore(coordinates: &[At]) -> Result<()> {
    let ops = alphabet(coordinates);
    let times = coordinates
        .iter()
        .all(|&At(time, ty)| coordinates.contains(&At(swap_time(time), ty)));
    let workers = available_parallelism().map_or(1, NonZeroUsize::get);
    let start = orbit(state(&Trace::default()), times);
    let mut seen = HashSet::from([start.clone()]);
    let mut frontier = vec![start];
    while !frontier.is_empty() {
        let part = frontier.len().div_ceil(workers);
        let found = scope(|scope| {
            let workers: Vec<_> = frontier
                .chunks(part)
                .map(|states| scope.spawn(|| successors(states, &ops, times, &seen)))
                .collect();
            workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .map_err(|_| eyre!("a search worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        frontier = found
            .into_iter()
            .flatten()
            .filter(|next| seen.insert(next.clone()))
            .collect();
    }
    Ok(())
}

/// The orbits that one operation takes `states` to, except those in `seen`.
/// Each step rebuilds memory, because a delay queue cannot be cloned.
fn successors(
    states: &[State],
    ops: &[Op],
    times: bool,
    seen: &HashSet<State>,
) -> Result<HashSet<State>> {
    let runtime = runtime()?;
    let _runtime = runtime.enter();
    let mut found = HashSet::new();
    let mut trace = Trace::default();
    for from in states {
        for &op in ops.iter().filter(|&&op| !repeats(op, from)) {
            rebuild(&mut trace, from);
            trace
                .step(op)
                .map_err(|error| error.wrap_err(format!("from {from:?}, then {op:?}")))?;
            let next = orbit(state(&trace), times);
            if !seen.contains(&next) {
                found.insert(next);
            }
        }
    }
    Ok(found)
}

/// Whether `op` from `state` does what another operation of the alphabet does.
/// A load after the load of the unowned slab does nothing, and so does a pop
/// when no attempt is queued.
fn repeats(op: Op, (loaded, held): &State) -> bool {
    let during = match op {
        Op::Insert(.., during) | Op::Unschedule(.., during) | Op::UnscheduleAll(.., during) => {
            during
        }
        Op::Load(At(UNOWNED, _)) => During::Load,
        _ => return false,
    };
    match during {
        During::Nothing => false,
        During::Load => *loaded,
        During::Pop => !held
            .iter()
            .any(|&(.., entry)| matches!(entry, Some((Some((_, true)), _)))),
    }
}

/// The least state in the orbit of `state`.
fn orbit(state: State, times: bool) -> State {
    let least = swap_tags(&state).min(state);
    if !times {
        return least;
    }
    let swapped = swap_times(&least);
    least.min(swap_tags(&swapped)).min(swapped)
}

/// `state` with the two tags swapped.
fn swap_tags((loaded, held): &State) -> State {
    let swap = |tag: i32| 1_i32 - tag;
    let held = held.iter().map(|&(row, kept, entry)| {
        let row = match row {
            Stored::Present(tag) => Stored::Present(swap(tag)),
            row => row,
        };
        let entry = entry.map(|(next, firing)| {
            (
                next.map(|(tag, queued)| (swap(tag), queued)),
                firing.map(|(tag, deleted)| (swap(tag), deleted)),
            )
        });
        (row, kept, entry)
    });
    (*loaded, held.collect())
}

/// `state` with the two owned times swapped.
fn swap_times((loaded, held): &State) -> State {
    let mut held = held.clone();
    for ty in 0..TYPES.len() {
        held.swap(ty, TYPES.len() + ty);
    }
    (*loaded, held)
}

/// The owned time that a swap of the owned times maps `time` to.
fn swap_time(time: usize) -> usize {
    match time {
        0 => 1,
        1 => 0,
        time => time,
    }
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
            ops.extend(OUTCOMES.map(|outcome| Op::Complete(at, tag, outcome)));
        }
        for outcome in OUTCOMES {
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
            let firing = entry
                .firing
                .map(|delivery| (delivery.tag, delivery.row == Row::Deleted));
            (
                next.map(|next| (next.trigger.tag, next.queued.is_some())),
                firing,
            )
        });
        (trace.rows[time][ty], trace.kept[time][ty], entry)
    });
    (trace.loaded, held.collect())
}

/// Makes `trace` the trace that `state` describes. It reuses the memory of
/// `trace`, because a new delay queue costs more than a cleared one.
fn rebuild(trace: &mut Trace, (loaded, held): &State) {
    trace.loaded = *loaded;
    let Timers { entries, queue } = &mut trace.timers;
    entries.clear();
    queue.clear();
    for ((time, ty), &(row, kept, entry)) in coordinates().zip(held) {
        trace.rows[time][ty] = row;
        trace.kept[time][ty] = kept;
        trace.delivering[time][ty] = None;
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
        trace.delivering[time][ty] = firing.map(|(tag, _)| tag);
        slot.firing = firing.map(|(tag, deleted)| Delivery {
            tag,
            row: if deleted { Row::Deleted } else { Row::Stored },
        });
    }
}
