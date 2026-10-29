//! `scheduled_times` against a set model, for timers that never fire.

use crate::Key;
use crate::timers::TimerRequest;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::tests::common::KEY_POOL;
use crate::timers::test_support::setup_timer_manager;
use color_eyre::eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use std::collections::BTreeSet;
use tokio::runtime::Builder;
use tokio::time;
use tracing::Span;

/// Second offsets the CRUD trace draws from: the current instant, two short
/// offsets that collide across keys, and ~1 year out — the boundary values
/// production sends.
const CRUD_OFFSETS: [u32; 4] = [0, 60, 120, 86_400 * 365];

/// Timer types that share keys and times, so an operation on one type must
/// leave the other alone.
const TYPES: [TimerType; 2] = [TimerType::Application, TimerType::DeferredMessage];

/// Invariant: for timers that never fire, `scheduled_times` reports exactly
/// the set a trivial model predicts — `schedule` inserts, `unschedule` and
/// `complete` remove, `unschedule_all` clears the key and type, and `abort`
/// preserves visibility (the row is kept for recovery). All five ops are
/// idempotent no-ops on absent coordinates. Parity is asserted for every key
/// and type after every op, over random interleavings on a small key, type
/// and time pool.
#[test]
fn prop_crud_scheduled_times_oracle() {
    QuickCheck::new()
        .quickcheck(prop_crud_scheduled_times_oracle_inner as fn(CrudTrace) -> TestResult);
}

fn prop_crud_scheduled_times_oracle_inner(trace: CrudTrace) -> TestResult {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return TestResult::error(format!("runtime build failed: {error}")),
    };

    runtime.block_on(async {
        match run_crud_trace(trace).await {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    })
}

async fn run_crud_trace(trace: CrudTrace) -> Result<()> {
    time::pause();

    let (_stream, manager, _shutdown_tx) = setup_timer_manager().await?;
    let now = CompactDateTime::now()?;
    let times = CRUD_OFFSETS
        .iter()
        .map(|&offset| now.add_duration(CompactDuration::new(offset)))
        .collect::<Result<Vec<_>, _>>()?;
    let keys: Vec<Key> = KEY_POOL.iter().copied().map(Key::from).collect();

    // Model: the set of times `scheduled_times` must report, per key and type.
    let mut model = vec![<[BTreeSet<CompactDateTime>; TYPES.len()]>::default(); keys.len()];

    for (step, &(key_idx, ty, time_idx, op)) in trace.0.iter().enumerate() {
        let key = &keys[key_idx];
        let time = times[time_idx];
        let timer_type = TYPES[ty];
        let model_set = &mut model[key_idx][ty];
        match op {
            CrudOp::Schedule => {
                manager
                    .schedule(TimerRequest::new(
                        key.clone(),
                        time,
                        timer_type,
                        Span::current(),
                    ))
                    .await?;
                model_set.insert(time);
            }
            CrudOp::Unschedule => {
                manager.unschedule(key, time, timer_type).await?;
                model_set.remove(&time);
            }
            CrudOp::UnscheduleAll => {
                manager.unschedule_all(key, timer_type).await?;
                model_set.clear();
            }
            CrudOp::Complete => {
                manager.complete(key, time, timer_type).await?;
                model_set.remove(&time);
            }
            // Abort never removes the durable row (it is preserved for
            // recovery), so `scheduled_times` visibility is unchanged.
            CrudOp::Abort => manager.abort(key, time, timer_type, 0),
        }

        for (idx, key) in keys.iter().enumerate() {
            for (checked, &timer_type) in TYPES.iter().enumerate() {
                let actual: BTreeSet<CompactDateTime> = manager
                    .scheduled_times(key, timer_type)
                    .await?
                    .into_iter()
                    .collect();
                assert_eq!(
                    actual, model[idx][checked],
                    "scheduled_times diverged from the CRUD model at step {step} ({op:?} on key \
                     {key_idx}, type {ty}, time {time_idx}) for key index {idx}, type {checked}"
                );
            }
        }
    }

    Ok(())
}

/// A trace of `(key index, type index, time index, op)` over the pools above.
#[derive(Clone, Debug)]
struct CrudTrace(Vec<(usize, usize, usize, CrudOp)>);

#[derive(Clone, Copy, Debug)]
enum CrudOp {
    Schedule,
    Unschedule,
    UnscheduleAll,
    Complete,
    Abort,
}

impl Arbitrary for CrudTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::from(u8::arbitrary(g) % 40);
        let ops = (0..len)
            .map(|_| {
                let key_idx = usize::from(u8::arbitrary(g)) % KEY_POOL.len();
                let ty = usize::from(u8::arbitrary(g)) % TYPES.len();
                let time_idx = usize::from(u8::arbitrary(g)) % CRUD_OFFSETS.len();
                // Schedule is weighted at half so traces keep timers around
                // for the removal ops to hit.
                let op = match u8::arbitrary(g) % 8 {
                    0..=3 => CrudOp::Schedule,
                    4 => CrudOp::Unschedule,
                    5 => CrudOp::UnscheduleAll,
                    6 => CrudOp::Complete,
                    _ => CrudOp::Abort,
                };
                (key_idx, ty, time_idx, op)
            })
            .collect();
        Self(ops)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Strictly shorter prefixes only, so shrinking always terminates.
        let ops = self.0.clone();
        Box::new(
            (0..ops.len())
                .rev()
                .map(move |len| Self(ops[..len].to_vec())),
        )
    }
}
