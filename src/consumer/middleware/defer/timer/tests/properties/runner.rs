//! Dispatches timer trace events and checks their results.

use super::super::HandlerOutcome;
use super::super::types::{ApplicationTimerEvent, ApplicationTimerOutcome, DeferredTimerEvent};
use super::*;
use crate::consumer::middleware::FallibleHandler;
use crate::timers::{TimerType, Trigger};
use tracing::Span;

// ============================================================================
// Helper Functions
// ============================================================================

pub(super) async fn execute_event(
    harness: &TestHarness,
    event: &TimerTraceEvent,
) -> color_eyre::Result<()> {
    match event {
        TimerTraceEvent::ApplicationTimer(app_event) => {
            execute_application_timer(harness, app_event).await
        }
        TimerTraceEvent::DeferredTimer(def_event) => {
            execute_deferred_timer(harness, def_event, DemandType::Normal).await
        }
    }
}

async fn execute_application_timer(
    harness: &TestHarness,
    event: &ApplicationTimerEvent,
) -> color_eyre::Result<()> {
    let key = test_key(event.key_idx);
    let context = &harness.contexts[event.key_idx];
    if matches!(event.outcome, ApplicationTimerOutcome::Queued) && event.lost_timer {
        context.drop_deferred_timers();
    }
    let trigger = Trigger::new(key, event.time, TimerType::Application, Span::current());

    // Configure handler based on expected outcome
    match &event.outcome {
        ApplicationTimerOutcome::Success => {
            harness.inner_handler.set_outcome(HandlerOutcome::Success);
        }
        ApplicationTimerOutcome::Permanent => {
            harness.inner_handler.set_outcome(HandlerOutcome::Permanent);
        }
        ApplicationTimerOutcome::Transient { defer } => {
            harness.inner_handler.set_outcome(HandlerOutcome::Transient);
            harness.decider.set_next(*defer);
        }
        ApplicationTimerOutcome::Queued => {
            // Handler won't be called - timer is queued
        }
    }

    let result = harness
        .handler
        .on_timer(context.clone(), trigger, DemandType::Normal)
        .await;

    // Verify result matches expectation
    verify_application_timer_result(&event.outcome, result.is_ok())
}

fn verify_application_timer_result(
    outcome: &ApplicationTimerOutcome,
    succeeded: bool,
) -> color_eyre::Result<()> {
    match outcome {
        ApplicationTimerOutcome::Success | ApplicationTimerOutcome::Queued => {
            if !succeeded {
                return Err(color_eyre::eyre::eyre!(
                    "Expected success/queued but got error"
                ));
            }
        }
        ApplicationTimerOutcome::Permanent => {
            if succeeded {
                return Err(color_eyre::eyre::eyre!(
                    "Expected permanent error but got success"
                ));
            }
        }
        ApplicationTimerOutcome::Transient { defer } => {
            if *defer {
                // Should absorb error
                if !succeeded {
                    return Err(color_eyre::eyre::eyre!(
                        "Expected deferral to absorb error but got error"
                    ));
                }
            } else {
                // Should propagate error
                if succeeded {
                    return Err(color_eyre::eyre::eyre!(
                        "Expected transient error to propagate but got success"
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn execute_deferred_timer(
    harness: &TestHarness,
    event: &DeferredTimerEvent,
    demand: DemandType,
) -> color_eyre::Result<()> {
    let key = test_key(event.key_idx);
    // DeferredTimer fires - time is the scheduled retry time, not original time
    let trigger = Trigger::new(
        key,
        CompactDateTime::now()?,
        TimerType::DeferredTimer,
        Span::current(),
    );

    // Configure handler based on expected outcome
    match &event.outcome {
        DeferredTimerOutcome::Success => {
            harness.inner_handler.set_outcome(HandlerOutcome::Success);
        }
        DeferredTimerOutcome::Permanent => {
            harness.inner_handler.set_outcome(HandlerOutcome::Permanent);
        }
        DeferredTimerOutcome::Transient => {
            harness.inner_handler.set_outcome(HandlerOutcome::Transient);
        }
    }

    let result = harness
        .handler
        .on_timer(harness.contexts[event.key_idx].clone(), trigger, demand)
        .await;

    // Verify result matches expectation
    verify_deferred_timer_result(&event.outcome, result.is_ok())
}

fn verify_deferred_timer_result(
    outcome: &DeferredTimerOutcome,
    succeeded: bool,
) -> color_eyre::Result<()> {
    match outcome {
        DeferredTimerOutcome::Success | DeferredTimerOutcome::Transient => {
            if !succeeded {
                return Err(color_eyre::eyre::eyre!(
                    "Expected success/re-defer but got error"
                ));
            }
        }
        DeferredTimerOutcome::Permanent => {
            // Permanent errors propagate
            if succeeded {
                return Err(color_eyre::eyre::eyre!(
                    "Expected permanent error but got success"
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn is_expected_error(error: &color_eyre::Report) -> bool {
    // Permanent errors propagate - this is expected
    let msg = format!("{error:?}");
    msg.contains("permanent") || msg.contains("Permanent")
}
