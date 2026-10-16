//! Key-index state and tombstone bound.
//!
//! The property drives one key through random timer histories. After each
//! step it checks the stored state, the cached state and every read path
//! against a model. It also traces the store's own state read and peek, and
//! bounds the tombstone cells that Cassandra counts. This proves the
//! tombstone invariant in `mutators`.

use super::{collect_all_types_times, collect_key_times, collect_trigger_times, setup_test_store};
use crate::Key;
use crate::test_util::{ModelProperty, TEST_RUNTIME, integration_test_count};
use crate::timers::datetime::CompactDateTime;
use crate::timers::store::cassandra::CassandraTriggerStore;
use crate::timers::store::cassandra::state::{PeekedTrigger, TimerState};
use crate::timers::store::operations::TriggerOperations;
use crate::timers::{TimerType, Trigger};
use crate::tracing::init_test_logging;
use color_eyre::eyre::{Report, Result, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use scylla::response::query_result::QueryRowsResult;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use strum::VariantArray;
use tokio::time::{interval, timeout};
use uuid::Uuid;

/// Tombstone cells that one type-scoped read may count with no strays: the
/// two markers of a type-wide range tombstone and the boundary that a newer
/// prefix delete adds.
const RANGE_MARKERS: u64 = 3;

/// Interval between polls for a trace's read summary.
const TRACE_POLL: Duration = Duration::from_millis(5);

/// Hang guard for a trace's read summary.
const TRACE_DEADLINE: Duration = Duration::from_secs(10);

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
}

/// Live times of one timer type, and its strays since it last held at most
/// one timer.
///
/// A stray is an operation out of time order: an unschedule after the
/// earliest live time, or a schedule or delete before the latest deleted
/// time. Each one may cost a read one more tombstone cell.
#[derive(Debug, Default)]
struct TypeModel {
    times: BTreeSet<CompactDateTime>,
    deleted_through: Option<CompactDateTime>,
    strays: u64,
}

impl Arbitrary for Step {
    fn arbitrary(g: &mut Gen) -> Self {
        let timer_type =
            TimerType::VARIANTS[usize::from(u8::arbitrary(g)) % TimerType::VARIANTS.len()];
        let time = u32::from(u8::arbitrary(g) % 48);

        let action = match u8::arbitrary(g) % 10 {
            0..=3 => Action::Schedule(time),
            4..=6 => Action::Fire,
            7 => Action::Unschedule(time),
            8 => Action::Reschedule(time),
            _ => Action::Clear,
        };
        Self(timer_type, action)
    }
}

impl TypeModel {
    fn schedule(&mut self, time: CompactDateTime) {
        self.strays += u64::from(self.is_behind(time));
        self.times.insert(time);
    }

    fn delete(&mut self, time: CompactDateTime) {
        let after_earliest = self.times.first().is_some_and(|first| time > *first);
        self.strays += u64::from(after_earliest || self.is_behind(time));
        self.times.remove(&time);
        self.deleted_through = self.deleted_through.max(Some(time));
    }

    /// Forgets the strays once at most one timer remains. The store has then
    /// range-deleted the whole type, which covers every earlier tombstone.
    fn settle(&mut self) {
        if self.times.len() <= 1 {
            self.deleted_through = None;
            self.strays = 0;
        }
    }

    fn is_behind(&self, time: CompactDateTime) -> bool {
        self.deleted_through.is_some_and(|through| time < through)
    }
}

/// Every timer history keeps the key index equal to the model, and no read
/// counts the tombstones of in-order fires.
#[test]
fn prop_key_index_matches_model_with_bounded_tombstones() {
    init_test_logging();
    QuickCheck::new()
        .tests(integration_test_count(25))
        .quickcheck(ModelProperty(|trace| TEST_RUNTIME.block_on(run(trace))));
}

/// Shrunk property counterexamples. In each, a newer prefix delete ends below
/// an older one, which adds one boundary marker that the model counts as a
/// stray.
#[test]
fn prefix_delete_below_an_older_one_is_a_stray() -> Result<()> {
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

async fn run(trace: Vec<Step>) -> Result<bool> {
    let (store, _) = setup_test_store("key_index_tombstones").await?;
    let key = Key::from(Uuid::new_v4().to_string());
    let mut model = HashMap::<TimerType, TypeModel>::new();

    for Step(timer_type, action) in trace {
        let ty = model.entry(timer_type).or_default();
        let trigger = |at: u32| Trigger::for_testing(key.clone(), at.into(), timer_type);

        match action {
            Action::Schedule(at) => {
                store.upsert_key_trigger(trigger(at)).await?;
                ty.schedule(at.into());
            }
            Action::Fire => {
                let Some(&time) = ty.times.first() else {
                    continue;
                };
                store.delete_key_trigger(timer_type, &key, time).await?;
                ty.delete(time);
            }
            Action::Unschedule(at) => {
                store
                    .delete_key_trigger(timer_type, &key, at.into())
                    .await?;
                ty.delete(at.into());
            }
            Action::Reschedule(at) => {
                store.clear_and_schedule_key(trigger(at)).await?;
                ty.times = BTreeSet::from([at.into()]);
            }
            Action::Clear => {
                store.clear_key_triggers(timer_type, &key).await?;
                ty.times.clear();
            }
        }
        ty.settle();

        let quiescent = model.values().all(|ty| ty.times.len() <= 1);
        if !check(&store, &key, timer_type, &model[&timer_type], quiescent).await? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Checks one type against its model: the stored and cached state, the first
/// stored rows and every read path. Also bounds the tombstone cells that the
/// state read and the peek count.
async fn check(
    store: &CassandraTriggerStore,
    key: &Key,
    timer_type: TimerType,
    ty: &TypeModel,
    quiescent: bool,
) -> Result<bool> {
    let queries = store.queries();
    let segment_id = store.segment.id;
    let times: Vec<_> = ty.times.iter().copied().collect();

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

    let (rows, peek_tombstones) = traced(
        store,
        &queries.peek_three_key_triggers,
        (segment_id, key.as_ref(), timer_type),
    )
    .await?;
    let peeked = rows
        .rows::<PeekedTrigger>()?
        .map(|row| row.map(|(time, ..)| time))
        .collect::<Result<Vec<_>, _>>()?;

    let (cached, _) = store.resolve_state(&segment_id, key, timer_type).await?;
    let cached = cached.lock().await.clone();

    let overflow_rows = if times.len() > 1 {
        &times[..times.len().min(3)]
    } else {
        &[]
    };
    // A quiescent static read scans every type: its range markers and one
    // static cell tombstone each.
    let static_bound = (RANGE_MARKERS + 1) * TimerType::VARIANTS.len() as u64;

    Ok(describes(&stored, &times)
        && describes(&cached, &times)
        && peeked == overflow_rows
        && collect_key_times(store, timer_type, key).await? == times
        && collect_trigger_times(store, timer_type, key).await? == times
        && collect_all_types_times(store, timer_type, key).await? == times
        && peek_tombstones <= RANGE_MARKERS + ty.strays
        && (!quiescent || state_tombstones <= static_bound))
}

/// Whether a timer state describes exactly these times.
fn describes(state: &TimerState, times: &[CompactDateTime]) -> bool {
    match (state, times) {
        (TimerState::Absent, []) | (TimerState::Overflow, [_, _, ..]) => true,
        (TimerState::Inline(inline), [only]) => inline.time == *only,
        _ => false,
    }
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

    // Cassandra writes trace events asynchronously and gives no signal when
    // they land, so the read summary can trail the finished session. Poll for
    // it, as the driver's own `get_tracing_info` does.
    let summary = async {
        let mut ticks = interval(TRACE_POLL);
        loop {
            ticks.tick().await;
            let info = store.session().get_tracing_info(&id).await?;
            let cells = info
                .events
                .iter()
                .find_map(|event| tombstone_cells(event.activity.as_deref()?));
            if let Some(cells) = cells {
                return Ok::<_, Report>(cells);
            }
        }
    };
    let tombstones = timeout(TRACE_DEADLINE, summary)
        .await
        .map_err(|_| eyre!("trace {id} has no read summary"))??;

    Ok((result.into_rows_result()?, tombstones))
}

/// Parses Cassandra's `Read N live rows and M tombstone cells` trace event.
fn tombstone_cells(activity: &str) -> Option<u64> {
    let (_, rest) = activity
        .strip_prefix("Read ")?
        .split_once(" live rows and ")?;
    rest.split_once(" tombstone cells")?.0.parse().ok()
}
