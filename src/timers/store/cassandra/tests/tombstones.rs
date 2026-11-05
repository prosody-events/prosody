//! Key-index state and tombstone bound.
//!
//! The property drives one key through random timer histories. After each
//! step it checks the stored state, the cached state and every read path of
//! every type against a model. It also traces the store's own state read and
//! the peeks of the next fire, and bounds the tombstone cells that Cassandra
//! counts. This proves the tombstone invariant in `mutators`.

use super::{collect_all_types_times, collect_key_times, collect_trigger_times, setup_test_store};
use crate::Key;
use crate::test_util::{ModelProperty, TEST_RUNTIME, integration_test_count};
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::state::{PeekedTrigger, TimerState};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use color_eyre::eyre::{Result, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use scylla::response::query_result::QueryRowsResult;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::collections::{BTreeSet, HashMap};
use strum::VariantArray;
use uuid::Uuid;

/// Tombstone cells that a read may count for each gap between live rows that
/// it crosses: the markers of the gap's range tombstone, and its boundaries
/// with the type's last clear.
const GAP_CELLS: u64 = 4;

/// One step of a key's timer history. Times are seconds in a narrow window,
/// so deletes often hit live timers.
#[derive(Clone, Copy, Debug)]
struct Step(TimerType, Action);

#[derive(Clone, Copy, Debug)]
enum Action {
    Schedule(u32),
    /// Deletes the earliest timer, as a fire does.
    Fire,
    /// Deletes any time, as an unschedule does.
    Unschedule(u32),
    /// Replaces every timer with one, as `clear_and_schedule_key` does.
    Reschedule(u32),
    Clear,
    /// Clears every type of the key.
    ClearAll,
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        let timer_type =
            TimerType::VARIANTS[usize::from(u8::arbitrary(g)) % TimerType::VARIANTS.len()];
        let time = u32::from(u8::arbitrary(g) % 48);

        let action = match u8::arbitrary(g) % 20 {
            0..=7 => Action::Schedule(time),
            8..=13 => Action::Fire,
            14..=15 => Action::Unschedule(time),
            16..=17 => Action::Reschedule(time),
            18 => Action::Clear,
            _ => Action::ClearAll,
        };
        Self(timer_type, action)
    }
}

/// Every timer history keeps the key index equal to the model, and no read
/// counts more than a few tombstone cells.
#[test]
fn prop_key_index_matches_model_with_bounded_tombstones() {
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ModelProperty(|trace| TEST_RUNTIME.block_on(run(trace))));
}

/// Shrunk counterexamples of an earlier design, which wrote row and prefix
/// tombstones. Each deletes below an older delete.
#[test]
fn deletes_below_an_older_delete_stay_bounded() -> Result<()> {
    use Action::{Fire, Schedule, Unschedule};
    use TimerType::DeferredMessage as T;

    init_test_logging();
    let traces = [
        // A schedule below the latest delete, then fired.
        [
            Schedule(27),
            Schedule(36),
            Fire,
            Schedule(5),
            Unschedule(3),
            Schedule(2),
            Fire,
        ],
        [
            Schedule(5),
            Schedule(3),
            Fire,
            Schedule(23),
            Unschedule(31),
            Unschedule(17),
            Fire,
        ],
        // An unschedule below the latest delete.
        [
            Schedule(27),
            Schedule(39),
            Fire,
            Schedule(12),
            Schedule(42),
            Fire,
            Unschedule(6),
        ],
    ];
    for trace in traces {
        let trace = trace.map(|action| Step(T, action)).to_vec();
        assert!(TEST_RUNTIME.block_on(run(trace))?);
    }
    Ok(())
}

/// Runs a trace on a new segment, then deletes the segment on every path.
async fn run(trace: Vec<Step>) -> Result<bool> {
    let (store, _) = setup_test_store("key_index_tombstones").await?;
    let outcome = replay(&store, trace).await;
    store.delete_segment().await?;
    outcome
}

/// Applies each step to the store and the model, and checks every type after
/// each step.
async fn replay(store: &CassandraTriggerStore, trace: Vec<Step>) -> Result<bool> {
    let key = Key::from(Uuid::new_v4().to_string());
    let mut model = HashMap::<TimerType, BTreeSet<CompactDateTime>>::new();

    for Step(timer_type, action) in trace {
        let times = model.entry(timer_type).or_default();
        let trigger = |at: u32| Trigger::for_testing(key.clone(), at.into(), timer_type);

        match action {
            Action::Schedule(at) => {
                store.upsert_key_trigger(trigger(at)).await?;
                times.insert(at.into());
            }
            Action::Fire => {
                let Some(time) = times.pop_first() else {
                    continue;
                };
                store.delete_key_trigger(timer_type, &key, time).await?;
            }
            Action::Unschedule(at) => {
                store
                    .delete_key_trigger(timer_type, &key, at.into())
                    .await?;
                times.remove(&at.into());
            }
            Action::Reschedule(at) => {
                store.clear_and_schedule_key(trigger(at)).await?;
                *times = BTreeSet::from([at.into()]);
            }
            Action::Clear => {
                store.clear_key_triggers(timer_type, &key).await?;
                times.clear();
            }
            Action::ClearAll => {
                store.clear_key_triggers_all_types(&key).await?;
                model.clear();
            }
        }

        for &timer_type in TimerType::VARIANTS {
            let times = model.get(&timer_type).cloned().unwrap_or_default();
            if !check(store, &key, timer_type, &times).await? {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

/// Checks one type against its model: the stored and cached state and every
/// read path. Also bounds the tombstone cells that the state read and the
/// peeks of the next fire count.
async fn check(
    store: &CassandraTriggerStore,
    key: &Key,
    timer_type: TimerType,
    times: &BTreeSet<CompactDateTime>,
) -> Result<bool> {
    let queries = store.queries();
    let segment_id = store.segment.id;
    let times: Vec<_> = times.iter().copied().collect();

    let (rows, state_tombstones) = traced(
        store,
        &queries.get_state_entry,
        (timer_type, segment_id, key.as_ref()),
    )
    .await?;
    let stored = rows
        .maybe_first_row::<(Option<TimerState>,)>()?
        .and_then(|(state,)| state)
        .unwrap_or(TimerState::Absent);

    let (cached, _) = store.resolve_state(&segment_id, key, timer_type).await?;
    let cached = cached.lock().await.clone();

    // The static read crosses one gap of every type, from its end to its
    // latest row, and one static cell tombstone.
    let static_bound = (GAP_CELLS + 1) * TimerType::VARIANTS.len() as u64;
    if !(describes(&stored, &times)
        && describes(&cached, &times)
        && collect_key_times(store, timer_type, key).await? == times
        && collect_trigger_times(store, timer_type, key).await? == times
        && collect_all_types_times(store, timer_type, key).await? == times
        && state_tombstones <= static_bound)
    {
        return Ok(false);
    }

    // An Overflow fire peeks both sides of the earliest time. The peek below
    // crosses one gap, and the peek above crosses two.
    let [first, rest @ ..] = &*times else {
        return Ok(true);
    };
    if rest.is_empty() {
        return Ok(true);
    }
    let values = (segment_id, key.as_ref(), timer_type, *first);
    let (below, below_tombstones) =
        traced(store, &queries.peek_key_triggers_before, values).await?;
    let (above, above_tombstones) = traced(store, &queries.peek_key_triggers_after, values).await?;

    Ok(peeked_times(&below)?.is_empty()
        && peeked_times(&above)? == rest[..rest.len().min(2)]
        && below_tombstones <= GAP_CELLS
        && above_tombstones <= 2 * GAP_CELLS)
}

/// Whether a timer state describes exactly these times.
fn describes(state: &TimerState, times: &[CompactDateTime]) -> bool {
    match (state, times) {
        (TimerState::Absent, []) | (TimerState::Overflow, [_, _, ..]) => true,
        (TimerState::Inline(inline), [only]) => inline.time == *only,
        _ => false,
    }
}

/// The times of peeked rows, in the peek's order.
fn peeked_times(rows: &QueryRowsResult) -> Result<Vec<CompactDateTime>> {
    Ok(rows
        .rows::<PeekedTrigger>()?
        .map(|row| row.map(|(time, ..)| time))
        .collect::<Result<_, _>>()?)
}

/// Runs one of the store's statements with request tracing. Returns the rows
/// and the tombstone cells that Cassandra counted for the read.
async fn traced(
    store: &CassandraTriggerStore,
    statement: &PreparedStatement,
    values: impl SerializeRow,
) -> Result<(QueryRowsResult, u64)> {
    let mut statement = statement.clone();
    statement.set_tracing(true);
    let result = store.session().execute_unpaged(&statement, values).await?;
    let id = result
        .tracing_id()
        .ok_or_else(|| eyre!("traced read returned no tracing id"))?;

    // The test cluster sets `cassandra.wait_for_tracing_events_timeout_secs`,
    // so a finished session already holds every event.
    let info = store.session().get_tracing_info(&id).await?;
    let tombstones = info
        .events
        .iter()
        .find_map(|event| tombstone_cells(event.activity.as_deref()?))
        .ok_or_else(|| eyre!("trace {id} has no read summary"))?
        .parse()?;

    Ok((result.into_rows_result()?, tombstones))
}

/// Extracts `M` from Cassandra's `Read N live rows and M tombstone cells`
/// trace event.
fn tombstone_cells(activity: &str) -> Option<&str> {
    let (_, rest) = activity
        .strip_prefix("Read ")?
        .split_once(" live rows and ")?;
    Some(rest.split_once(" tombstone cells")?.0)
}
