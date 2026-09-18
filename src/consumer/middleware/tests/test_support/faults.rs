//! Shared fault slots and error categories for middleware tests.

use super::super::RecordingGuard;
use crate::Key;
use crate::consumer::middleware::retry::{RetryConfiguration, RetryHandler, RetryMiddleware};
use crate::consumer::middleware::{
    FallibleCloneProvider, FallibleHandler, FallibleHandlerProvider, HandlerMiddleware,
};
use crate::consumer::{Keyed, Uncommitted};
use crate::error::{ClassifyError, ErrorCategory};
use crate::timers::datetime::CompactDateTime;
use crate::timers::{TimerType, Trigger, UncommittedTimer};
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;

/// Selects an injected error category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// Reports a transient error.
    Transient,
    /// Reports a permanent error.
    Permanent,
    /// Reports a terminal error.
    Terminal,
}

/// Shares revocation and the fired retry timer between the store and the
/// timer context.
#[derive(Default)]
pub struct Phase {
    revoked: AtomicBool,
    calls_until_revoke: AtomicUsize,
    consumed: Mutex<Option<FaultKind>>,
    /// The retry timer that already fired. It does not fire again, so a queue
    /// write cannot count it. A schedule call for the same time clears it.
    fired: Mutex<Option<CompactDateTime>>,
}

impl Phase {
    /// Revokes after the selected call. Zero revokes before dispatch.
    pub fn arm(&self, calls: u8) {
        self.calls_until_revoke
            .store(usize::from(calls), Ordering::SeqCst);
        self.revoked.store(calls == 0, Ordering::SeqCst);
    }

    /// Starts a new assignment without a pending revocation.
    pub fn reassign(&self) {
        self.calls_until_revoke.store(0, Ordering::SeqCst);
        self.revoked.store(false, Ordering::SeqCst);
    }

    /// Reports whether the assignment has ended.
    pub fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::SeqCst)
    }

    /// Takes the error category consumed by the last dispatch.
    pub fn take_consumed(&self) -> Option<FaultKind> {
        self.consumed.lock().take()
    }

    /// Names the retry timer under dispatch. Pass `None` for an event that no
    /// retry timer delivered.
    pub fn fire(&self, time: Option<CompactDateTime>) {
        *self.fired.lock() = time;
    }

    /// Clears the fired timer when a schedule call repeats its time.
    pub fn rearm(&self, time: CompactDateTime) {
        let mut fired = self.fired.lock();
        if *fired == Some(time) {
            *fired = None;
        }
    }

    /// Returns the fired timer that no longer fires.
    pub fn fired(&self) -> Option<CompactDateTime> {
        *self.fired.lock()
    }

    fn call(&self) {
        if self
            .calls_until_revoke
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |calls| {
                calls.checked_sub(1)
            })
            == Ok(1)
        {
            self.revoked.store(true, Ordering::SeqCst);
        }
    }
}

/// Holds one fault until its selected operation consumes it.
#[derive(Clone)]
pub struct FaultSlot<Op> {
    next: Arc<Mutex<Option<(Op, FaultKind)>>>,
    uncovered: Arc<Mutex<Option<Op>>>,
    phase: Arc<Phase>,
}

impl<Op> Default for FaultSlot<Op> {
    fn default() -> Self {
        Self {
            next: Arc::default(),
            uncovered: Arc::default(),
            phase: Arc::default(),
        }
    }
}

impl<Op> FaultSlot<Op> {
    /// Returns a slot that shares `phase` with another slot. The store and the
    /// timer context must share one phase, or the fired trigger is invisible
    /// to the rule check.
    pub fn sharing(phase: &Arc<Phase>) -> Self {
        Self {
            phase: Arc::clone(phase),
            ..Default::default()
        }
    }

    /// Returns the shared phase.
    pub fn phase(&self) -> &Arc<Phase> {
        &self.phase
    }
}

impl<Op: Copy + PartialEq> FaultSlot<Op> {
    /// Replaces the pending fault.
    pub fn set(&self, fault: Option<(Op, FaultKind)>) {
        *self.next.lock() = fault;
    }

    /// Records the first queue write that left a queue without a retry timer.
    pub fn record_uncovered(&self, op: Op) {
        self.uncovered.lock().get_or_insert(op);
    }

    /// Takes the recorded rule violation.
    pub fn take_uncovered(&self) -> Option<Op> {
        self.uncovered.lock().take()
    }

    /// Consumes the fault only when its operation matches.
    pub fn check(&self, op: Op) -> Option<FaultKind> {
        self.phase.call();
        let mut slot = self.next.lock();
        if let Some((target, kind)) = *slot
            && target == op
        {
            *slot = None;
            *self.phase.consumed.lock() = Some(kind);
            Some(kind)
        } else {
            None
        }
    }

    /// Rejects timer calls after revocation. Stores still accept calls.
    pub fn check_timer(&self, op: Op) -> Option<FaultKind> {
        if self.phase.is_revoked() {
            *self.phase.consumed.lock() = Some(FaultKind::Terminal);
            Some(FaultKind::Terminal)
        } else {
            self.check(op)
        }
    }
}

impl Arbitrary for FaultKind {
    fn arbitrary(g: &mut Gen) -> Self {
        let kinds = [Self::Transient, Self::Permanent, Self::Terminal];
        g.choose(&kinds).copied().unwrap_or(Self::Transient)
    }
}

/// Selects the timer call that receives a fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerOp {
    /// Adds a timer.
    Schedule,
    /// Replaces the key timers.
    ClearAndSchedule,
    /// Removes the key timers.
    ClearScheduled,
    /// Reads the key timers.
    Scheduled,
    /// Removes one timer.
    Unschedule,
}

/// A fault before one dispatch. `Op` is the twin's store operation.
#[derive(Clone, Copy, Debug)]
pub enum Fault<Op> {
    /// Fails one store call.
    Store(Op, FaultKind),
    /// Fails one timer call.
    Timer(TimerOp, FaultKind),
    /// The timer vanished outside the handler.
    LostTimer,
    /// Revokes after the selected store or timer call.
    Revoke(u8),
}

impl<Op: Arbitrary> Arbitrary for Fault<Op> {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = FaultKind::arbitrary(g);
        match u8::arbitrary(g) % 4 {
            0 => Self::Store(Op::arbitrary(g), kind),
            1 => {
                let ops = [
                    TimerOp::Schedule,
                    TimerOp::ClearAndSchedule,
                    TimerOp::ClearScheduled,
                    TimerOp::Scheduled,
                ];
                Self::Timer(ops[usize::arbitrary(g) % ops.len()], kind)
            }
            2 => Self::LostTimer,
            _ => Self::Revoke(u8::arbitrary(g) % 8),
        }
    }
}

/// Reports a timer fault before the capture changes.
#[derive(Debug, Error)]
#[error("injected timer failure: {0:?}")]
pub struct TimerError(pub FaultKind);

impl ClassifyError for TimerError {
    fn classify_error(&self) -> ErrorCategory {
        match self.0 {
            FaultKind::Transient => ErrorCategory::Transient,
            FaultKind::Permanent => ErrorCategory::Permanent,
            FaultKind::Terminal => ErrorCategory::Terminal,
        }
    }
}

/// Reports an injected error or the inner store's error.
#[derive(Debug, Error)]
pub enum FailableStoreError<E> {
    /// Reports an injected transient store error.
    #[error("injected transient store failure")]
    Transient,
    /// Reports an injected permanent store error.
    #[error("injected permanent store failure")]
    Permanent,
    /// Reports an injected terminal store error.
    #[error("injected terminal store failure")]
    Terminal,
    /// Retains the inner store error.
    #[error("inner store error: {0}")]
    Inner(E),
}

impl<E: ClassifyError> ClassifyError for FailableStoreError<E> {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            Self::Transient => ErrorCategory::Transient,
            Self::Permanent => ErrorCategory::Permanent,
            Self::Terminal => ErrorCategory::Terminal,
            Self::Inner(error) => error.classify_error(),
        }
    }
}

/// Records the source decision, the queue head, and coverage at the
/// settlement boundary. An aborted pass must leave `head` unchanged.
#[derive(Debug)]
pub struct Pass {
    pub consumed: Option<FaultKind>,
    pub committed: bool,
    pub covered_at_settle: bool,
    pub head: Option<i64>,
}

/// Checks each settlement and the required redelivery.
pub fn verify_passes(passes: &[Pass]) -> color_eyre::Result<()> {
    use color_eyre::eyre::ensure;
    for (index, pass) in passes.iter().enumerate() {
        ensure!(
            !pass.committed
                || pass.covered_at_settle
                || pass.consumed == Some(FaultKind::Permanent),
            "A committed source must be covered_at_settle: {passes:?}"
        );
        ensure!(
            pass.consumed != Some(FaultKind::Permanent) || pass.committed,
            "A consumed permanent fault commits: {passes:?}"
        );
        ensure!(
            pass.committed || passes.get(index + 1).is_some_and(|next| next.committed),
            "An aborted source must commit on redelivery: {passes:?}"
        );
    }
    Ok(())
}

/// Constructs the real retry handler through its provider.
pub fn retry<H>(handler: H) -> color_eyre::Result<RetryHandler<H>>
where
    H: FallibleHandler + Clone,
{
    let config = RetryConfiguration::builder()
        .base(Duration::from_millis(1))
        .max_delay(Duration::from_millis(10))
        .build()?;
    Ok(FallibleHandlerProvider::handler_for_partition(
        &RetryMiddleware::new(config)?.with_provider(FallibleCloneProvider::new(handler)),
        "test-topic".into(),
        0,
    ))
}

impl Keyed for (Trigger, RecordingGuard) {
    type Key = Key;

    fn key(&self) -> &Self::Key {
        &self.0.key
    }
}

impl Uncommitted for (Trigger, RecordingGuard) {
    async fn commit(self) {
        self.1.commit().await;
    }

    async fn abort(self) {
        self.1.abort().await;
    }
}

impl UncommittedTimer for (Trigger, RecordingGuard) {
    type CommitGuard = RecordingGuard;

    fn time(&self) -> CompactDateTime {
        self.0.time
    }

    fn timer_type(&self) -> TimerType {
        self.0.timer_type
    }

    fn span(&self) -> tracing::Span {
        self.0.span()
    }

    fn into_inner(self) -> (Trigger, Self::CommitGuard) {
        self
    }
}
