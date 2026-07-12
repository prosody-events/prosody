//! The keyed-state durability sequence — the single owner of
//! stage → arm → marker-flush → commit → promote, run once per event after
//! the middleware stack returns its final result.
//!
//! Both durability boundaries — the blanket [`EventHandler`] impl in the
//! parent module and [`RetryHandler`](super::retry::RetryHandler) — route
//! their final outcome through [`settle`] / [`abandon`] here, so the
//! sequence's ordering contracts (marker strictly after stage, promote
//! strictly after commit) live in one straight-line function and cannot be
//! written in the wrong order elsewhere.
//!
//! [`EventHandler`]: crate::consumer::EventHandler

use std::error::Error as StdError;
use std::future::Future;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{error, warn};

use super::FallibleHandler;
use crate::consumer::Uncommitted;
use crate::consumer::event_context::EventContext;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::session::sealed::{ApplyOutcome, StateLifecycle};
use crate::state::session::{Finalized, LifecycleAccessExt};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

/// Delay between retries of a durability step (stage / arm / marker flush)
/// that failed transiently. Mirrors the timer commit retry cadence
/// ([`crate::timers::uncommitted`]) and the state-manager init loop.
const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Outcome of one durability step driven by [`retry_step`].
enum StepOutcome<R> {
    /// The step succeeded, carrying its result.
    Done(R),

    /// The step failed permanently. Only a genuine data rejection is skipped
    /// (the `finalize` stage): the sequence continues defensively, and it
    /// never flushes a marker over an uncertain stage. Steps whose permanent
    /// failure is *not* a data rejection — the backstop arm and the
    /// success-path marker flush, both pure framework bookkeeping — retry
    /// past `Skip` in their own loops instead.
    Skip,

    /// Shutdown: abandon the event — abort the marker and let redelivery
    /// re-run from clean state. Reached **only** via
    /// [`EventContext::is_shutdown`], so every downstream `abandon` is, by
    /// construction, a shutdown abort — a transient or terminal store failure
    /// retries forever instead (see [`retry_step`]).
    ///
    /// [`EventContext::is_shutdown`]: crate::consumer::event_context::EventContext::is_shutdown
    Abandon,
}

/// Outcome of arming the `StateRecovery` backstop.
///
/// Arming is durability-critical (invariant 8: never certify a stage whose
/// backstop we could not arm), so [`arm_backstop`] retries **every**
/// non-shutdown failure forever — transient, terminal, *and* permanent
/// timer-store errors, and a fire-time computation error alike. It can
/// therefore end only one of two ways, which makes "abort in normal operation"
/// unrepresentable for the arm.
pub(super) enum ArmOutcome {
    /// The backstop is armed, or a standing one already covers this commit.
    Armed,

    /// Shutdown intervened before the backstop could be armed. The caller
    /// aborts the marker (rolling the un-certified receipt's staged cells
    /// back) so redelivery re-runs and re-arms.
    ShuttingDown,
}

/// The durability sequence: the single owner of stage → arm → marker-flush →
/// commit → promote, run once per event after the stack returns its final
/// `result`. Both the blanket [`EventHandler`] impl and
/// [`RetryHandler`](super::retry::RetryHandler) route their final outcome
/// here, so the wrong ordering (marker before stage) is structurally
/// unwritable.
///
/// Fires exactly one apply hook (`after_commit` / `after_abort`) carrying
/// `result`, preserving the per-invocation apply-hook invariant.
///
/// [`EventHandler`]: crate::consumer::EventHandler
pub(crate) async fn settle<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
) where
    T: FallibleHandler,
    T::Error: ClassifyError,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    // The inner work is done; clear any stale message-level cancel flag so
    // the durability steps' cancel-guarded timer ops aren't short-circuited
    // (mirrors the timeout middleware uncancelling after the inner returns).
    context.uncancel();

    // Reach the event's lifecycle handle. Every live context carries one —
    // `LifecycleAccess` binds unconditionally — so `None` means only an
    // invalidated context, which cannot stage anyway.
    let lifecycle = context.lifecycle().ok();

    match result.as_ref().err().map(ClassifyError::classify_error) {
        // Terminal: the marker aborts; the event redelivers and re-runs.
        // Nothing staged (finalize runs only on Ok), and abandon touches no
        // state.
        Some(ErrorCategory::Terminal) => {
            abandon(handler, context, guard, result).await;
        }
        // Final error (Transient/Permanent): nothing staged (finalize runs
        // only on Ok). On a Permanent error the dedup middleware registered
        // the marker; flush it so the failed-but-final message deduplicates.
        // The flush is gated to Permanent: in the shipped stacks a Transient
        // final has no registered marker anyway, but a custom stack whose
        // middleware swallows a post-success failure into a Transient error
        // (without resetting the session) must not have that attempt's
        // marker certify never-staged state. Then commit and fire the hook.
        Some(category) => {
            if category == ErrorCategory::Permanent
                && let Some(lifecycle) = &lifecycle
            {
                flush_marker_best_effort(&context, lifecycle).await;
            }
            guard.commit().await;
            handler.after_commit(context, result).await;
        }
        // Success: run the full durability sequence.
        None => settle_committed(handler, context, guard, result, lifecycle).await,
    }
}

/// The success arm of [`settle`]: stage, arm the backstop, flush the marker
/// strictly after the stage, commit, then promote the staged cells through
/// the receipt.
///
/// # Crash windows
///
/// The step order — stage → **arm** → marker flush → **commit** → promote —
/// closes every crash window without any acquisition-time sweep (there is
/// none):
///
/// * Crash after the stage, before the arm: the offset never commits, so the
///   event **redelivers**, re-stages, and re-arms; the redelivered handler's
///   own reads first-touch-resolve the orphan to its committed base.
/// * Crash after the arm, before the commit: still uncommitted → redelivery,
///   *and* the backstop is armed → the sweep resolves it either way.
/// * Crash after the commit, before the promote: committed (no redelivery), but
///   the backstop is armed → the sweep resolves; the flushed marker also
///   dedup-filters any redelivery.
///
/// So every durable provisional cell is covered by redelivery
/// (arm-precedes-commit) or an armed backstop. The lone first-touch-only
/// residual is the permanent-partial-stage path below (a `finalize` `Skip`
/// committed unarmed), an accepted edge bounded by first-touch and the cell
/// TTL.
async fn settle_committed<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
    lifecycle: Option<C::State>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    let Some(lifecycle) = lifecycle else {
        // Invalidated / stateless context: just commit and fire the hook.
        guard.commit().await;
        handler.after_commit(context, result).await;
        return;
    };

    // 1. Stage provisional cells / write resolved, retrying transient
    // failures.
    let finalized =
        match retry_step(&context, "keyed-state finalize", || lifecycle.finalize()).await {
            StepOutcome::Done(finalized) => finalized,
            StepOutcome::Skip => {
                // Permanent stage failure: a partial stage may be durable. Arm
                // the backstop defensively so the sweep resolves it, skip the
                // marker flush (invariant: marker present ⇒ stage durable),
                // and commit. A shutdown `ShuttingDown` from the arm is
                // deliberately ignored: committing a permanently-unstageable
                // event beats livelocking, and first-touch covers the unarmed
                // cell (the sole first-touch-only recovery residual —
                // everything else is redelivery or an armed sweep). No receipt
                // exists to carry a finalize-folded delay, so the defensive
                // arm uses the plain floor.
                let _ = arm_backstop(&context, &lifecycle, lifecycle.recovery_floor()).await;
                guard.commit().await;
                handler.after_commit(context, result).await;
                return;
            }
            StepOutcome::Abandon => {
                // Shutdown before a receipt exists: nothing is recorded to
                // roll back (finalize mints the receipt only on full success);
                // redelivery re-runs from clean state, and recovery owns any
                // partial durable stage.
                abandon(handler, context, guard, result).await;
                return;
            }
        };

    // 2. Arm the StateRecovery backstop iff something staged —
    // possession-driven: the receipt is the capability. The backstop is an
    // amortized per-key singleton: the first commit of a generation arms it,
    // later commits skip while it stands, and the boundary never clears it
    // (the sweep does, on fire), so this event cannot disturb another's
    // backstop (F2).
    //
    // Arm-gates-marker (invariant 8): a backstop is the only guarantee that a
    // staged provisional cell resolves before its TTL, so we must NOT certify
    // the stage until it is armed. `arm_backstop` is must-succeed — it retries
    // every non-shutdown failure forever — so the only non-`Armed` outcome is
    // a shutdown, which aborts *before* the marker flush: the receipt rolls
    // the staged cells back to their committed base (no lingering provisional,
    // nothing to TTL out) and the offset aborts so the event redelivers,
    // re-runs, and re-arms.
    let promotable = match finalized {
        Finalized::Clean => None,
        Finalized::Staged(staged) => {
            match arm_backstop(&context, &lifecycle, staged.recovery_delay()).await {
                ArmOutcome::Armed => Some(staged.certify()),
                ArmOutcome::ShuttingDown => {
                    // The ONE reachable rollback site — before any marker-flush
                    // attempt, so restoring the committed base is sound; past
                    // `certify` a rollback no longer compiles.
                    guard.abort().await;
                    staged.rollback().await;
                    handler.after_abort(context, result).await;
                    return;
                }
            }
        }
    };

    // 3. Flush the registered dedup marker — STRICTLY after the stage, so a
    // present marker always certifies a durable stage. Like the arm, the
    // flush is must-succeed: the marker is framework data (a bare dedup id),
    // so no failure here is a data rejection the sequence may skip.
    // Committing with the stage uncertified would have the armed sweep
    // silently roll a successful handler's writes back — with the offset
    // committed, nothing ever replays them. A permanently-failing store
    // therefore retries until it heals (or the liveness probe restarts the
    // process, the visible last resort); only shutdown abandons.
    loop {
        match retry_step(&context, "keyed-state marker flush", || {
            lifecycle.flush_marker()
        })
        .await
        {
            StepOutcome::Done(()) => break,
            StepOutcome::Skip => sleep(DURABILITY_RETRY_DELAY).await,
            StepOutcome::Abandon => {
                // A marker-flush attempt was made before shutdown: its
                // durability is ambiguous, so the staged cells must NOT roll
                // back — and cannot: `certify` consumed the receipt, and
                // `Promotable` has no rollback. They stay provisional and the
                // armed sweep resolves them through the oracle, which reads
                // whether the marker landed. Dropping the receipt here is
                // safe: recovery never depends on the in-memory record.
                abandon(handler, context, guard, result).await;
                return;
            }
        }
    }

    // 4. Commit the durability marker (offset / trigger).
    guard.commit().await;

    // 5. Promote the staged cells (null `event`/`prev`, O(1) per cell). This
    // is correct only here, strictly after the commit: promoting a timer
    // write before its trigger commit would resurrect it on a crash-refire.
    // The backstop stays armed regardless: a `Resolved` key's
    // sweep finds nothing provisional and clears itself when the key goes
    // quiet, while an `Incomplete` promote leaves real work for that same
    // sweep to retry. No point-clear means no cross-event race.
    if let Some(promotable) = promotable
        && promotable.promote().await == ApplyOutcome::Incomplete
    {
        warn!("keyed-state promote incomplete; the StateRecovery sweep will retry");
    }

    // 6. After-commit hook (telemetry, dedup forwarding, ...).
    handler.after_commit(context, result).await;
}

/// Abandons the event: abort the marker (offset → redelivery, timer →
/// reloadable) and fire `after_abort`. Reached on a terminal error or a
/// shutdown mid-sequence.
///
/// Touches no keyed state: staged cells (if any exist) stay provisional for
/// redelivery, first-touch, or the armed sweep to resolve through the oracle
/// — the one inline rollback lives at the arm-shutdown arm of
/// `settle_committed`, where possession of the un-certified receipt proves it
/// is sound.
pub(crate) async fn abandon<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Uncommitted + Send,
{
    guard.abort().await;
    handler.after_abort(context, result).await;
}

/// Arms the per-key `StateRecovery` backstop as an arm-if-sooner singleton.
///
/// Arming is **must-succeed** (invariant 8: a backstop is the only guarantee a
/// staged provisional cell resolves before its TTL, so we must not certify the
/// stage without one). Every non-shutdown failure — a transient, terminal, or
/// permanent timer-store error, or a fire-time computation error — therefore
/// retries forever; the arm returns [`ArmOutcome::ShuttingDown`] only when
/// shutdown interrupts it, never a swallow or an abort.
///
/// The first stateful commit on a quiet key issues one `clear_and_schedule`
/// (a type-scoped singleton overwrite — only the key's `StateRecovery` timers
/// move; user timers of other types are untouched) and records the standing
/// fire. A later commit re-arms **only** when its fire is strictly sooner than
/// the standing one — the tightening a per-collection `recovery_within` bound
/// needs — and otherwise skips: the standing timer already sweeps its staged
/// cells no later than its own bound. A commit that only loosens keeps the
/// tighter timer. So a burst of same-delay commits on one key still issues a
/// *single* timer-store write — the amortization the redesign's
/// tombstone-accounting depends on — while a tighter commit pulls the one timer
/// sooner.
///
/// The recorded fire lives in the per-acquisition in-RAM `ArmedKeys`, but a
/// durable backstop outlives an acquisition. So a key's first arm after
/// reacquisition seeds the map from the durable trigger store before deciding:
/// a prior epoch's sooner still-standing fire is kept, never overwritten with
/// a later one. Never-loosen therefore holds across reacquisition, not just
/// within one epoch.
///
/// The standing fire is cleared only when the sweep fires (the manager's
/// `recover`), so the durability boundary never unschedules and one event can
/// never clear another's still-needed backstop (finding F2). Per-key
/// serialization makes the decision race-free: the sweep that consumes a
/// backstop cannot run while a commit on the same key decides whether to
/// re-arm.
///
/// Cost and healing: at most one `clear_and_schedule` per backstop generation
/// (plus one per tightening), and the sweep fires by the tightest bound of a
/// generation (on a sustained hot key, periodically). Any read of a provisional
/// collection heals it immediately via first-touch (the cell store's resolving
/// `get`). Accepted residual: an
/// `Incomplete` leftover on a hot key whose collection is never read again
/// waits for the next sweep to resolve it — bounded by first-touch on any
/// access and by the cell's TTL.
///
/// `delay` is the caller's fire delay: the receipt's finalize-folded
/// `recovery_delay()` on the staged path, `recovery_floor()` on the defensive
/// permanent-failure arm.
pub(super) async fn arm_backstop<C>(
    context: &C,
    lifecycle: &C::State,
    delay: CompactDuration,
) -> ArmOutcome
where
    C: EventContext,
{
    loop {
        if context.is_shutdown() {
            return ArmOutcome::ShuttingDown;
        }
        // Compute the fire time. A failure here — the clock is unavailable, or a
        // misconfigured recovery delay overflows the representable range — is
        // not a shutdown signal, so retry rather than skip: arming is
        // must-succeed (invariant 8), never a swallow.
        let fire = match CompactDateTime::now().and_then(|now| now.add_duration(delay)) {
            Ok(fire) => fire,
            Err(error) => {
                error!(error = %error, "failed to compute StateRecovery fire time; retrying");
                sleep(DURABILITY_RETRY_DELAY).await;
                continue;
            }
        };
        // Arm-if-sooner: a standing backstop that fires no later than this one
        // already covers this commit's staged cells, so skip re-arming. Per-key
        // serialization makes the standing fire reliable — the sweep that
        // consumes it cannot run while this commit decides. `ArmedKeys` is
        // minted empty per acquisition while a prior epoch's backstop survives
        // in the durable trigger store, so a RAM miss consults the store and
        // seeds the map: assuming "unarmed" there would let the singleton
        // overwrite replace a sooner still-standing fire with a later one — a
        // loosening the boundary must never perform.
        let standing = match lifecycle.backstop_armed().await {
            Some(standing) => Some(standing),
            None => match retry_step(context, "read standing StateRecovery backstop", || {
                context.scheduled(TimerType::StateRecovery)
            })
            .await
            {
                StepOutcome::Done(times) => {
                    let standing = times.into_iter().min();
                    if let Some(standing) = standing {
                        lifecycle.mark_backstop_armed(standing).await;
                    }
                    standing
                }
                // Arming is must-succeed: a permanent read failure retries
                // (with a recomputed fire) rather than guessing "unarmed".
                StepOutcome::Skip => {
                    sleep(DURABILITY_RETRY_DELAY).await;
                    continue;
                }
                StepOutcome::Abandon => return ArmOutcome::ShuttingDown,
            },
        };
        if standing.is_some_and(|standing| standing <= fire) {
            return ArmOutcome::Armed;
        }
        match retry_step(context, "arm StateRecovery backstop", || {
            // Singleton overwrite: tightening replaces the one standing timer.
            context.clear_and_schedule(fire, TimerType::StateRecovery)
        })
        .await
        {
            StepOutcome::Done(()) => {
                // Record the standing fire only after a successful arm, so a
                // failed arm leaves the prior fire (or none) standing.
                lifecycle.mark_backstop_armed(fire).await;
                return ArmOutcome::Armed;
            }
            // A permanent timer-store/manager failure (a stale `InvalidContext`
            // or a past fire time — both unreachable for a future-dated arm on a
            // live context) is nonetheless retried: arming is must-succeed, so we
            // recompute the fire and try again rather than certify a stage with
            // no backstop.
            StepOutcome::Skip => {
                sleep(DURABILITY_RETRY_DELAY).await;
            }
            StepOutcome::Abandon => return ArmOutcome::ShuttingDown,
        }
    }
}

/// Flushes the registered marker, retrying transient failures; a permanent
/// failure or shutdown is tolerated (the failed-but-final message simply
/// isn't deduplicated and re-runs, re-failing the same way).
async fn flush_marker_best_effort<C>(context: &C, lifecycle: &C::State)
where
    C: EventContext,
{
    let _ = retry_step(context, "keyed-state marker flush", || {
        lifecycle.flush_marker()
    })
    .await;
}

/// Retries one durability step until it succeeds or shutdown intervenes.
/// **Transient and terminal store failures both retry forever** — a terminal
/// store error is a broken dependency, not a process-shutdown signal, and
/// retrying self-heals when the store recovers (a store that stays broken
/// stalls the offset until the liveness probe restarts the process, the
/// visible last resort). Only a **permanent** (data-rejection) failure is
/// skipped, so the straight-line sequence can continue defensively; only
/// shutdown abandons. Mirrors the retry-until-shutdown idiom of the timer
/// commit loop and state-manager initialization.
async fn retry_step<C, R, E, F, Fut>(context: &C, label: &str, mut step: F) -> StepOutcome<R>
where
    C: EventContext,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
    E: ClassifyError + StdError,
{
    loop {
        if context.is_shutdown() {
            return StepOutcome::Abandon;
        }
        match step().await {
            Ok(value) => return StepOutcome::Done(value),
            Err(error) => match error.classify_error() {
                // Retry forever, not just on Transient: a Terminal store error
                // is a broken dependency, not a process-shutdown signal.
                // Retrying self-heals the instant the store recovers; a store
                // that stays broken stalls the offset until the liveness probe
                // restarts the process — a visible last resort, strictly better
                // than silently abandoning the event here. `abandon` is
                // reserved for genuine shutdown, caught at the top of the loop.
                ErrorCategory::Transient | ErrorCategory::Terminal => {
                    error!(label, error = %error, "durability step failed; retrying");
                    sleep(DURABILITY_RETRY_DELAY).await;
                }
                ErrorCategory::Permanent => {
                    error!(label, error = %error, "durability step failed permanently; skipping");
                    return StepOutcome::Skip;
                }
            },
        }
    }
}
