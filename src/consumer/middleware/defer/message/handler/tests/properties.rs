//! Checks defer invariants against real state before and after each dispatch.

use super::harness::TestHarness;
use super::loader::LoaderFailureType;
use super::types::{TimerOutcome, Trace, TraceEvent};
use super::{TEST_BASE_BACKOFF_SECS, TEST_MAX_BACKOFF_SECS, TEST_RUNTIME};
use crate::consumer::DemandType;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::tracing::init_test_logging;
use color_eyre::eyre::{ensure, eyre};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

/// A key has a live timer exactly when its deferred queue has a head.
#[quickcheck]
fn prop_timer_coverage(trace: Trace) -> TestResult {
    init_test_logging();
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        for step in trace.steps {
            events.push(harness.execute_step(&step).await?);
            harness.verify_invariants().await?;
        }
        Ok(())
    });
    finish(result, &events)
}

/// Each reload reads the current head and reports its cumulative retry count.
/// A committed pass advances the head or increments its retry count.
#[quickcheck]
fn prop_reload_contract(trace: Trace, demand: DemandType) -> TestResult {
    init_test_logging();
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        harness.demand = demand;
        for step in trace.steps {
            let event = harness.execute_step(&step).await?;
            events.push(event.clone());
            let TraceEvent::Timer(timer) = event else {
                continue;
            };
            for pass in &harness.passes {
                let outcome = match pass.loader_failure {
                    Some(LoaderFailureType::Transient) => TimerOutcome::LoaderTransient,
                    Some(LoaderFailureType::Permanent) => TimerOutcome::LoaderPermanent,
                    None => timer.outcome.clone(),
                };
                let Some((head, count)) = pass.before else {
                    ensure!(
                        pass.calls.is_empty() && pass.committed && pass.after.is_none(),
                        "Orphan fire changed state: {pass:?}"
                    );
                    continue;
                };
                ensure!(pass.calls.len() <= 1, "Multiple reload calls: {pass:?}");
                for call in &pass.calls {
                    ensure!(call.offset == head, "Reload used the wrong head: {pass:?}");
                    ensure!(
                        call.demand.retry()
                            == count.saturating_add(1).saturating_add(demand.retry()),
                        "Reload used the wrong retry count: {pass:?}"
                    );
                }
                if matches!(
                    outcome,
                    TimerOutcome::LoaderPermanent | TimerOutcome::LoaderTransient
                ) {
                    ensure!(
                        pass.calls.is_empty(),
                        "Failed load called the handler: {pass:?}"
                    );
                }
                if pass.committed {
                    match outcome {
                        TimerOutcome::Transient | TimerOutcome::LoaderTransient => {
                            ensure!(
                                pass.after == Some((head, count + 1)),
                                "Retry did not increment: {pass:?}"
                            );
                        }
                        TimerOutcome::Success
                        | TimerOutcome::Permanent
                        | TimerOutcome::LoaderPermanent => {
                            ensure!(
                                pass.after.map(|(offset, _)| offset) != Some(head),
                                "Head did not advance: {pass:?}"
                            );
                            ensure!(
                                pass.after.is_none_or(|(_, count)| count == 0),
                                "New head retained retries: {pass:?}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    });
    finish(result, &events)
}

/// Each schedule respects the backoff bound from the count after the event.
/// Every schedule in an event uses that final count.
#[quickcheck]
fn prop_backoff_bounds(trace: Trace) -> TestResult {
    init_test_logging();
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        for step in trace.steps {
            let before_time = CompactDateTime::now()?;
            let event = harness.execute_step(&step).await?;
            let key_idx = match &event {
                TraceEvent::Message(message) => message.key_idx,
                TraceEvent::Timer(timer) => timer.key_idx,
            };
            events.push(event);
            let count = harness.store().is_deferred(harness.key(key_idx)).await?;
            for pass in &harness.passes {
                for time in &pass.scheduled {
                    let count = count.ok_or_else(|| eyre!("Schedule has no queue: {pass:?}"))?;
                    let maximum = before_time
                        .add_duration(expected_max_backoff(count) + CompactDuration::new(1))?;
                    ensure!(
                        before_time <= *time && *time <= maximum,
                        "Backoff exceeds bounds: {before_time:?} <= {time:?} <= {maximum:?}; \
                         {pass:?}"
                    );
                }
            }
        }
        Ok(())
    });
    finish(result, &events)
}

/// Calls for each key have nondecreasing offsets. Redelivery can repeat an
/// offset.
#[quickcheck]
fn prop_processing_order(trace: Trace) -> TestResult {
    init_test_logging();
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        let mut previous = vec![None; trace.key_count];
        for step in trace.steps {
            events.push(harness.execute_step(&step).await?);
            for pass in &harness.passes {
                for call in &pass.calls {
                    let key_idx = (0..trace.key_count)
                        .find(|idx| harness.key(*idx) == &call.key)
                        .ok_or_else(|| eyre!("Unknown key: {:?}", call.key))?;
                    ensure!(
                        previous[key_idx].is_none_or(|offset| offset <= call.offset),
                        "Offset decreased from {:?} to {call:?}",
                        previous[key_idx]
                    );
                    previous[key_idx] = Some(call.offset);
                }
            }
        }
        Ok(())
    });
    finish(result, &events)
}

/// Each consumed store or timer fault abandons the source once.
#[quickcheck]
fn prop_fault_abandons_source(trace: Trace) -> TestResult {
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        for step in trace.steps {
            events.push(harness.execute_step(&step).await?);
            let first = &harness.passes[0];
            ensure!(
                first.committed != first.consumed,
                "Wrong source decision: {:?}",
                harness.passes
            );
            ensure!(
                harness.passes.len() == 1 + usize::from(first.consumed),
                "Wrong pass count: {:?}",
                harness.passes
            );
            if let Some(second) = harness.passes.get(1) {
                ensure!(
                    !second.consumed && second.committed,
                    "Redelivery failed: {second:?}"
                );
            }
        }
        Ok(())
    });
    finish(result, &events)
}

/// Timer loss remains visible when the key receives no further event.
#[quickcheck]
fn prop_lost_timer_without_traffic_stays_lost(trace: Trace) -> TestResult {
    let mut events = Vec::with_capacity(trace.steps.len());
    let result = TEST_RUNTIME.block_on(async {
        let mut harness = TestHarness::new(trace.key_count)?;
        for step in trace.steps {
            let event = harness.interpret(&step).await;
            events.push(event.clone());
            if let TraceEvent::Timer(timer) = event {
                let key = harness.key(timer.key_idx);
                let before = harness.store().get_next_deferred_message(key).await?;
                ensure!(before.is_some(), "The pending retry has no queue");
                harness.capture().drop_timers(key);
                ensure!(
                    !harness.capture().has_active_timer(key),
                    "Lost timer remains active"
                );
                ensure!(
                    harness.verify_invariants().await.is_err(),
                    "Coverage missed timer loss"
                );
                ensure!(
                    harness.store().get_next_deferred_message(key).await? == before,
                    "Timer loss changed the head"
                );
                return Ok(TestResult::passed());
            }
            if let TraceEvent::Message(message) = event {
                harness.execute_message(&message).await?;
            }
        }
        Ok(TestResult::discard())
    });
    result.unwrap_or_else(|error: color_eyre::Report| {
        TestResult::error(format!("{error:#}\nInterpreted events: {events:?}"))
    })
}

fn expected_max_backoff(retry_count: u32) -> CompactDuration {
    let multiplier = 1_u32.checked_shl(retry_count).unwrap_or(u32::MAX);
    CompactDuration::new(
        TEST_BASE_BACKOFF_SECS
            .saturating_mul(multiplier)
            .min(TEST_MAX_BACKOFF_SECS),
    )
}

fn finish(result: color_eyre::Result<()>, events: &[TraceEvent]) -> TestResult {
    match result {
        Ok(()) => TestResult::passed(),
        Err(error) => TestResult::error(format!("{error:#}\nInterpreted events: {events:?}")),
    }
}
