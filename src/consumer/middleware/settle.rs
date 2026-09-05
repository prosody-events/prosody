//! Owns keyed-state durability after the middleware stack returns.
//!
//! Sweep posture uses stage → marker → receipt → promote → retire.
//!
//! Both durability boundaries — the blanket [`EventHandler`] impl in the
//! parent module and [`RetryHandler`](super::retry::RetryHandler) — route
//! their final outcome through [`settle`] / [`abandon`] here, so the
//! sequence's ordering contracts live in one function. A marker follows its
//! stage. A sweep receipt precedes promotion. The commit decision is a pure
//! function of the stack's final result: [`SettlementHandler::settlement`]
//! classifies it as [`Settlement::Final`], [`Settlement::Duplicate`], or
//! [`Settlement::Bypassed`]. The
//! message commit marker is read from the session's event identity
//! (`message_marker`), never deposited by middleware.
//!
//! The boundary also closes the session operation gate and holds its permit
//! across the whole sequence, dropping it just before the apply hooks fire (the
//! closure/permit contract is owned by [`SessionGate`](crate::state::session)).
//!
//! [`EventHandler`]: crate::consumer::EventHandler

use crate::util::crash_point;
use std::error::Error as StdError;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{error, warn};

use super::FallibleHandler;
use crate::consumer::event_context::EventContext;
use crate::consumer::{Receipted, ReceiptedSource};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::state::descriptor::Registered;
use crate::state::session::sealed::{ApplyOutcome, MarkerIdentity, Promotable, StateLifecycle};
use crate::state::session::{Finalized, LifecycleAccess, MessageMarker, OpPermit};
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;

/// Delay between retries of a durability step (stage / arm / marker record)
/// that failed transiently. Mirrors the timer commit retry cadence
/// ([`crate::timers::uncommitted`]) and the state-manager init loop.
const DURABILITY_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Settle-module-private access to the full settlement surface: binds the
/// event's session through [`LifecycleAccess`] and returns it so the boundary
/// drives the sealed [`StateLifecycle`] (`close_gate` / `finalize` /
/// `record_marker` / `discard_dirty` / the backstop accessors). Private to
/// this module — the crate-wide `lifecycle()` accessor is gone, so only settle
/// reaches this surface conveniently; dedup / defer-reload get the narrow
/// [`MarkerHandle`](crate::state::session::MarkerHandle) instead.
trait SettlementAccess: EventContext {
    /// Binds the event's session through the settlement tunnel. Fails only
    /// when the context is terminated; [`LifecycleAccess`] is otherwise
    /// registration-independent.
    fn settle_lifecycle(&self) -> Result<Self::State, StateAccessError> {
        self.state(Registered::new(LifecycleAccess))
    }
}

impl<C: EventContext> SettlementAccess for C {}

/// How the settlement boundary treats the stack's final result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// The result is the event's own outcome (the dispatch reached the
    /// handler layer). An `Ok` result uses the durability sequence.
    /// Err: record the marker iff the category is Permanent; commit.
    Final,

    /// The outcome lives elsewhere (defer queue, failure topic, an earlier
    /// commit of the same message) or the error belongs to a middleware
    /// layer, not the event (a rescue/admission failure where the marker
    /// must not certify anything). Nothing stages, no marker records; the
    /// offset/trigger commits. The boundary discards any uncommitted dirty
    /// overlay under the held permit before the hook fires (the scope drop
    /// stays the panic/drop backstop), and skipping `finalize` is exact parity
    /// with finalizing an empty buffer: an empty finalize yields
    /// [`Finalized::Clean`], and the Clean arm never arms the recovery backstop
    /// (nothing staged, so nothing needs protection).
    Bypassed,

    /// A committed message returned through its unretired redelivery source.
    Duplicate,
}

/// Crate-internal middleware-chain surface: classifies the final result for
/// the settlement boundary.
///
/// Required and non-defaulted so a future swallowing middleware cannot
/// inherit [`Settlement::Final`] by omission — a swallow classified `Final`
/// records the swallowed message's marker and dedup-filters its own retry,
/// the lost-write bug class this trait exists to close. For the same reason
/// there is deliberately **no blanket impl** over all [`FallibleHandler`]s:
/// exactly one concrete leaf adapter
/// ([`LeafHandler`](super::providers::LeafHandler), minted at
/// `into_provider`) hardcodes `Final`, and every framework wrapper writes one
/// explicit impl classifying its own Output and error variants (delegating on
/// pass-through shapes).
pub(crate) trait SettlementHandler: FallibleHandler {
    /// Classifies the stack's final result — both sides — for [`settle`].
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement;
}

/// Capability witnessing the settlement boundary: the only constructor is in
/// this module, so the oracle marker write does not compile anywhere else.
/// (Nominally `pub` inside this private module — the sealed-item idiom — so
/// the sealed lifecycle trait can carry it in a signature; its effective
/// visibility stays crate-internal.)
///
/// This is module privacy, NOT a proof-of-stage token — a `Clean` success and
/// a Permanent-no-stage final both legitimately record a marker with no
/// stage; stage-before-marker ordering stays enforced by the one
/// straight-line [`settle_committed`].
pub struct MarkerWrite(());

/// The attempt-boundary re-pin privilege. Opaque (the `MarkerWrite` idiom):
/// its tuple field is private to this module, so `RepinProof(())` is
/// constructible only here — the two production mint sites are the
/// `next_attempt` verb and the `fire_apply_hook` settle stamp.
/// A partial reset (a lone epoch bump with no matching re-pin, or a re-pin with
/// no reset) is therefore unwritable anywhere else, and a leaked stale context
/// clone can never re-pin itself back to life.
///
/// Nominally `pub` — and re-exported publicly — because
/// [`EventContext::redispatch`]
/// names it in a public signature; its effective visibility stays
/// crate-internal because no one outside this module can construct one.
pub struct RepinProof(());

impl RepinProof {
    /// Mints a proof for in-crate typed-layer tests that drive
    /// `reset`/`repin`/`redispatch` directly (the production mint sites are the
    /// two above). Test-only, so the privilege stays unforgeable in shipping
    /// code.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self(())
    }
}

/// The single middleware-facing attempt-boundary verb. Consumes the stale
/// dispatch context, runs the gate-held [`reset`](crate::state::session)
/// transition (discard the dirty overlay + bump the epoch, one gate hold), and
/// returns the re-pinned attempt-N+1 dispatch context. This is the epoch's ONLY
/// bump site.
///
/// The re-pin runs strictly after the reset, so a partial reset is
/// unrepresentable, and the cancellation flag is deliberately NOT cleared here
/// — outer cancellers own the sticky flag.
pub(crate) trait NextAttempt: EventContext {
    /// Advances this dispatch to its next attempt: reset then re-pin.
    fn next_attempt(self) -> impl Future<Output = Self> + Send;
}

impl<C: EventContext> NextAttempt for C {
    async fn next_attempt(self) -> Self {
        // A stateless / invalidated context has no lifecycle to reset; the
        // re-pin below is then a no-op rebuild. Mint site 1a (reset/bump).
        if let Ok(session) = self.settle_lifecycle() {
            session.reset(RepinProof(())).await;
        }
        // Mint site 1b (re-pin to the just-bumped epoch).
        self.redispatch(RepinProof(()))
    }
}

/// Outcome of one durability step driven by [`retry_step`].
enum StepOutcome<R> {
    /// The step succeeded, carrying its result.
    Done(R),

    /// The step failed permanently. Only a genuine data rejection is skipped
    /// (the `finalize` stage): the sequence continues defensively, and it
    /// never records a marker over an uncertain stage. Steps whose permanent
    /// failure is *not* a data rejection — the backstop arm and the
    /// success-path marker record, both pure framework bookkeeping — retry
    /// past `Skip` in their own loops instead.
    Skip,

    /// Shutdown: abandon the event — abort the marker and let redelivery
    /// re-run from clean state. Reached **only** via
    /// [`EventContext::is_shutdown`], so every downstream `abandon` is, by
    /// construction, a shutdown abort — a transient or terminal store failure
    /// retries forever instead (see [`retry_step`]).
    ///
    /// [`EventContext::is_shutdown`]: crate::consumer::event_context::TerminationSignals::is_shutdown
    Abandon,
}

/// Outcome of arming the `StateRecovery` backstop.
///
/// Some recovery paths require a durable safety timer before source retirement.
/// [`arm_backstop`] retries every non-shutdown failure. These paths include an
/// incomplete promotion, duplicate sweep failure, and permanent stage failure.
pub(super) enum ArmOutcome {
    /// The backstop is armed, or a standing one already guards this commit.
    Armed,

    /// Shutdown intervened before the safety timer became durable.
    /// The caller keeps or aborts the redelivery source for another attempt.
    ShuttingDown,
}

/// Owns the durability sequence after the stack returns its final result.
/// Both the blanket [`EventHandler`] impl and
/// [`RetryHandler`](super::retry::RetryHandler) route their final outcome
/// here, so the wrong ordering (marker before stage) is structurally
/// unwritable.
///
/// Branches on the typed [`Settlement`] classification first, the error
/// category second: a [`Settlement::Bypassed`] result stages nothing and
/// records no marker (see the variant's parity argument); a
/// [`Settlement::Duplicate`] result sweeps before source retirement. A
/// [`Settlement::Final`] result runs the sequence on `Ok`, records the
/// marker without a stage on a Permanent error, and commits bare on a
/// Transient one. A Terminal error always abandons, before the
/// classification is even consulted — commit-on-Terminal is unwritable
/// regardless of any wrapper's `settlement()`.
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
    T: SettlementHandler,
    C: EventContext<Payload = T::Payload>,
    G: Receipted + Send,
{
    // The inner work is done; clear any stale message-level cancel flag so
    // the durability steps' cancel-guarded timer ops aren't short-circuited
    // (mirrors the timeout middleware uncancelling after the inner returns).
    context.uncancel();

    let category = result.as_ref().err().map(ClassifyError::classify_error);
    // Terminal: the marker aborts; the event redelivers and re-runs. Nothing
    // staged (finalize runs only on a Final Ok), and abandon touches no
    // state. Checked before the settlement classification so a Terminal
    // error abandons even when a wrapper classifies it Bypassed.
    if category == Some(ErrorCategory::Terminal) {
        abandon(handler, context, guard, result).await;
        return;
    }

    // Reach the event's lifecycle handle. Every live context carries one —
    // `LifecycleAccess` binds unconditionally — so `None` means only an
    // invalidated context, which cannot stage anyway.
    let lifecycle = context.settle_lifecycle().ok();

    // Close the session operation gate and HOLD the permit across the whole
    // durability sequence: closure fences mutators (a detached op errors
    // `SessionClosed` instead of mutating a session this boundary already
    // snapshotted) while the held permit keeps any queued read serialized
    // behind the settle. Dropped just before the apply hooks fire, so the
    // hooks' post-settle state READS proceed and observe fully-settled state.
    let permit = match &lifecycle {
        Some(lifecycle) => Some(lifecycle.close_gate().await),
        None => None,
    };

    match T::settlement(result.as_ref()) {
        // The outcome lives elsewhere: no stage, no marker; commit the
        // offset/trigger and fire the hook. Skipping `finalize` here is
        // equivalent to finalizing an emptied buffer: an empty finalize
        // yields `Finalized::Clean`, and Clean never arms the backstop.
        Settlement::Bypassed => {
            guard.commit().await;
            discard_uncommitted(lifecycle.as_ref());
            drop(permit);
            fire_apply_hook(handler, context, true, result).await;
        }
        Settlement::Duplicate => {
            // Keep this tail local because the sweep can transfer guard ownership to
            // abandon.
            if let Some(lifecycle) = &lifecycle {
                match lifecycle.sweep().await {
                    Ok(()) => {}
                    Err(error) if error.classify_error() == ErrorCategory::Permanent => {
                        error!("keyed-state duplicate sweep failed permanently: {error:#}");
                    }
                    Err(error) => {
                        error!("keyed-state duplicate sweep failed: {error:#}");
                        if context.is_shutdown()
                            || matches!(
                                arm_backstop(
                                    &context,
                                    lifecycle,
                                    lifecycle.fallback_recovery_delay()
                                )
                                .await,
                                ArmOutcome::ShuttingDown
                            )
                        {
                            drop(permit);
                            abandon(handler, context, guard, result).await;
                            return;
                        }
                    }
                }
            }
            guard.commit().await;
            discard_uncommitted(lifecycle.as_ref());
            drop(permit);
            fire_apply_hook(handler, context, true, result).await;
        }
        Settlement::Final => match category {
            // A failed-but-final message: record its marker best-effort (no
            // stage exists — finalize runs only on Ok) so redelivery
            // dedup-filters the known-permanent failure, then commit.
            Some(ErrorCategory::Permanent) => {
                if let Some(lifecycle) = &lifecycle
                    && let Some(marker) = lifecycle.message_marker()
                {
                    record_marker_best_effort(&context, lifecycle, marker).await;
                }
                guard.commit().await;
                discard_uncommitted(lifecycle.as_ref());
                drop(permit);
                fire_apply_hook(handler, context, true, result).await;
            }
            // Transient final (no retry layer below took it): no marker —
            // the event is not handled — just commit and fire the hook.
            // (Terminal returned above.)
            Some(_) => {
                guard.commit().await;
                discard_uncommitted(lifecycle.as_ref());
                drop(permit);
                fire_apply_hook(handler, context, true, result).await;
            }
            // Success: run the full durability sequence.
            None => {
                settle_committed(handler, context, guard, result, lifecycle.as_ref(), permit).await;
            }
        },
    }
}

/// Discards this event's uncommitted dirty overlay, on every settle path that
/// did **not** successfully finalize (final permanent/transient, Bypassed,
/// permanent finalize-failure, finalize / marker-record shutdown, and the
/// direct [`abandon`]). Defined by the *absence* of successful finalization,
/// not an error-category list: a successful
/// [`finalize`](StateLifecycle::finalize) drains the buffer as part of the
/// stage, so the success path never reaches here.
///
/// Called under the still-held closed-gate permit, before the permit drops and
/// the apply hooks fire, so an apply hook or a leaked hook-window read observes
/// fully-settled committed truth with no aborted-attempt residue. The
/// commit-now floor survives untouched: an explicit mid-handler `commit()`
/// durably applies **and** drains its cells at commit time, so this clears only
/// the remaining uncommitted ops. Staged provisional cells and the recovery
/// backstop live in the durable store, not the dirty buffer, so this never
/// touches them. A stateless / invalidated context (`None`) has no overlay.
fn discard_uncommitted<S: StateLifecycle>(lifecycle: Option<&S>) {
    if let Some(lifecycle) = lifecycle {
        lifecycle.discard_dirty();
    }
}

/// Records the receipt before promotion, then retires the source.
///
/// The marker is read from the session's event identity
/// (`message_marker()`: the message `EventRef`'s dedup id, or the
/// deferred-reload override) and written through the settlement-private
/// [`MarkerWrite`] capability — no middleware deposits it, so
/// stage-before-marker is enforced by this one straight-line function.
///
/// # Crash windows
///
/// Message sweep posture closes these crash windows:
///
/// * A crash before the marker leaves the offset active. The next delivery runs
///   the handler.
/// * A crash after the marker leaves the offset active. Deduplication
///   classifies redelivery as `Duplicate`, which sweeps state.
/// * A crash after promotion leaves the offset active. Duplicate redelivery
///   retires it.
/// * A crash after offset commit leaves no pending state.
///
/// A timer receipt is its commit marker. Before receipt, the next delivery
/// runs the handler. First touch resolves cells from the abandoned attempt.
/// After receipt, recovery sweeps the key before it retires the slab row.
/// A rescheduled coordinate keeps its slab tag. Receipt rotates its key tag,
/// so recovery sweeps the old attempt before it starts the next live attempt.
///
/// A permanent stage failure needs first touch if shutdown prevents its
/// backstop arm. First touch uses the oracle to resolve the correct value.
async fn settle_committed<'a, T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
    lifecycle: Option<&'a C::State>,
    permit: Option<OpPermit<'a>>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Receipted + Send,
{
    let Some(lifecycle) = lifecycle else {
        // Invalidated / stateless context: just commit and fire the hook.
        guard.commit().await;
        drop(permit);
        fire_apply_hook(handler, context, true, result).await;
        return;
    };

    // 0. Stage provisional cells / write resolved, retrying transient
    // failures.
    let finalized = match retry_step(&context, "keyed-state finalize", || lifecycle.finalize())
        .await
    {
        StepOutcome::Done(finalized) => finalized,
        StepOutcome::Skip => {
            // A partial stage may exist. Arm recovery, omit the marker, and commit.
            let _ = arm_backstop(&context, lifecycle, lifecycle.fallback_recovery_delay()).await;
            guard.commit().await;
            // Not a successful finalize (`finalize`'s failure paths leave
            // the buffer whole); `discard_uncommitted` owns the
            // permit-held / commit-now-floor contract.
            discard_uncommitted(Some(lifecycle));
            drop(permit);
            fire_apply_hook(handler, context, true, result).await;
            return;
        }
        StepOutcome::Abandon => {
            // Shutdown before a receipt exists: nothing is recorded to
            // roll back (finalize mints the receipt only on full success);
            // redelivery re-runs from clean state, and recovery owns any
            // partial durable stage. Discard the uncommitted overlay under
            // the still-held permit (finalize did not drain it) before
            // dropping — closes the drop→`abandon` reacquire gap where a
            // leaked read could observe the residue in the open-gate
            // window. `abandon` performs its own idempotent gate close.
            discard_uncommitted(Some(lifecycle));
            drop(permit);
            abandon(handler, context, guard, result).await;
            return;
        }
    };

    crash_point().await;

    // 2. Capture the delay and certify before the marker write.
    let prepared = match finalized {
        Finalized::Clean => None,
        Finalized::Staged(staged) => Some((staged.recovery_delay(), staged.certify())),
    };

    // 3. Record the message marker after the stage. Timer events have no
    // message marker. Retry all non-shutdown failures.
    if let Some(marker) = lifecycle.message_marker() {
        loop {
            match retry_step(&context, "keyed-state marker record", || {
                lifecycle.record_marker(marker, MarkerWrite(()))
            })
            .await
            {
                StepOutcome::Done(()) => break,
                StepOutcome::Skip => sleep(DURABILITY_RETRY_DELAY).await,
                StepOutcome::Abandon => {
                    // The marker result is ambiguous. Certified state cannot
                    // roll back. Recovery resolves it through the oracle.
                    discard_uncommitted(Some(lifecycle));
                    drop(permit);
                    abandon(handler, context, guard, result).await;
                    return;
                }
            }
        }
    }

    crash_point().await;

    // 4. Receipt, promote, and retire staged state.
    match prepared {
        None => guard.commit().await,
        Some((recovery_delay, promotable)) => {
            settle_sweep_posture(&context, lifecycle, guard, promotable, recovery_delay).await;
        }
    }

    // 5. The permit drops before the apply hook, so hook reads can proceed.
    drop(permit);
    fire_apply_hook(handler, context, true, result).await;
}

/// Settles staged state when committed redelivery can sweep the key.
async fn settle_sweep_posture<C, G>(
    context: &C,
    lifecycle: &C::State,
    guard: G,
    promotable: Promotable<<C::State as StateLifecycle>::Cell>,
    recovery_delay: CompactDuration,
) where
    C: EventContext,
    G: Receipted + Send,
{
    let source = guard.receipt().await;
    crash_point().await;

    if promotable.promote().await == ApplyOutcome::Incomplete {
        warn!("keyed-state promote incomplete; the StateRecovery sweep will retry");
        if matches!(
            arm_backstop(context, lifecycle, recovery_delay).await,
            ArmOutcome::ShuttingDown
        ) {
            source.keep().await;
            return;
        }
    }

    crash_point().await;

    source.retire().await;
}

/// Abandons the event: abort the marker (offset → redelivery, timer →
/// reloadable) and fire `after_abort`. Reached on a terminal error or a
/// shutdown mid-sequence.
///
/// Never *promotes* keyed state: certified staged cells (if any exist) stay
/// provisional for redelivery, first-touch, or the armed sweep to resolve
/// through the oracle. It discards the uncommitted dirty overlay under the held
/// permit (abandon is never a successful finalize), so a leaked hook-window
/// read observes committed truth, not aborted-attempt residue; the commit-now
/// floor survives (its cells drained at `commit()` time).
pub(crate) async fn abandon<T, C, G>(
    handler: &T,
    context: C,
    guard: G,
    result: Result<T::Output, T::Error>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
    G: Receipted + Send,
{
    // Close the session gate here too — retry calls `abandon` directly, so
    // the fence must not depend on routing through `settle`. Idempotent: a
    // second close after `settle`'s own (its arms drop their permit before
    // delegating here) merely re-acquires and re-marks `Closed`.
    let lifecycle = context.settle_lifecycle().ok();
    let permit = match &lifecycle {
        Some(lifecycle) => Some(lifecycle.close_gate().await),
        None => None,
    };
    guard.abort().await;
    discard_uncommitted(lifecycle.as_ref());
    drop(permit);
    fire_apply_hook(handler, context, false, result).await;
}

/// The single site both apply hooks fire through. Stamps the hook's context
/// view **current** — one bump-free re-pin (the second [`RepinProof`] mint
/// site) — before invoking, so a final hook's reads see settled state
/// regardless of how many attempts ran or how deeply retry was nested. Inner
/// resets advance the shared epoch during the outer attempt, leaving the
/// boundary-owned final context pinned at a stale epoch; threading that context
/// through unchanged would fail every hook read `Terminated`. The stamp writes
/// **no** epoch — settlement has closed the gate and no further attempt can
/// begin, so re-pinning to the live epoch only re-enables the boundary's own
/// context, never a genuinely-leaked stale clone (which keeps its old pin).
async fn fire_apply_hook<T, C>(
    handler: &T,
    context: C,
    commit: bool,
    result: Result<T::Output, T::Error>,
) where
    T: FallibleHandler,
    C: EventContext<Payload = T::Payload>,
{
    let stamped = context.redispatch(RepinProof(()));
    if commit {
        handler.after_commit(stamped, result).await;
    } else {
        handler.after_abort(stamped, result).await;
    }
}

/// Arms a `StateRecovery` timer only if it fires before the standing timer.
/// Retries each error until the arm succeeds or shutdown starts (invariant 8).
///
/// [`ArmedKeys`](crate::state::manager::ArmedKeys) records the standing fire.
/// A missing entry requires a store read because a timer can survive
/// acquisition. The state manager's `reschedule_backstop` applies the same
/// arm-if-sooner rule. Per-key serialization prevents a sweep during this
/// decision. Only the fired sweep clears the entry (finding F2).
///
/// Incomplete promotions and failed duplicate sweeps arm before retirement.
/// Permanent stage failures also request an arm.
/// `delay` comes from the stage, or from `fallback_recovery_delay` when no
/// stage exists.
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

/// Records `marker` best-effort, retrying transient failures; a permanent
/// failure or shutdown is tolerated (the failed-but-final message simply
/// isn't deduplicated and re-runs, re-failing the same way).
async fn record_marker_best_effort<C>(context: &C, lifecycle: &C::State, marker: MessageMarker)
where
    C: EventContext,
{
    let _ = retry_step(context, "keyed-state marker record", || {
        lifecycle.record_marker(marker, MarkerWrite(()))
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
