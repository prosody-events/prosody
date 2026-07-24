//! The keyed-state durability sequence — the single owner of
//! publish → stage → arm → marker-record → commit → promote, run once per event
//! after the middleware stack returns its final result.
//!
//! Both durability boundaries — the blanket [`EventHandler`] impl in the
//! parent module and [`RetryHandler`](super::retry::RetryHandler) — route
//! their final outcome through [`settle`] / [`abandon`] here, so the
//! sequence's ordering contracts (marker strictly after stage, promote
//! strictly after commit) live in one straight-line function and cannot be
//! written in the wrong order elsewhere. The commit decision itself is a pure
//! function of the stack's final result: [`SettlementHandler::settlement`]
//! classifies it as [`Settlement::Final`] or [`Settlement::Bypassed`], and the
//! message commit marker is read from the session's event identity
//! (`message_marker`), never deposited by middleware.
//!
//! The boundary also closes the session operation gate and holds its permit
//! across the whole sequence, dropping it just before the apply hooks fire (the
//! closure/permit contract is owned by [`SessionGate`](crate::state::session)).
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
use crate::state::access::StateAccessError;
use crate::state::descriptor::Registered;
use crate::state::session::sealed::{ApplyOutcome, MarkerIdentity, StateLifecycle};
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
    /// handler layer). Ok: stage, record the marker, commit, promote.
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
    /// (arming is possession-driven, gated on `Finalized::Staged`).
    Bypassed,
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
/// Arming is durability-critical (invariant 8: never certify a stage whose
/// backstop we could not arm), so [`arm_backstop`] retries **every**
/// non-shutdown failure forever — transient, terminal, *and* permanent
/// timer-store errors, and a fire-time computation error alike. It can
/// therefore end only one of two ways, which makes "abort in normal operation"
/// unrepresentable for the arm.
pub(super) enum ArmOutcome {
    /// The backstop is armed, or a standing one already guards this commit.
    Armed,

    /// Shutdown intervened before the backstop could be armed. The caller
    /// aborts the marker (rolling the un-certified receipt's staged cells
    /// back) so redelivery re-runs and re-arms.
    ShuttingDown,
}

/// Outcome of the first-write publication barrier.
///
/// Publishing must succeed, for the same reason arming the backstop must
/// (invariant 8). A `Published` collection's committed state is undiscoverable
/// by a cross-group reader until its routing row exists. The row must precede
/// the stage. [`publish_first_writes`] retries every non-shutdown failure
/// forever, regardless of category, matching `arm_backstop`.
///
/// A dropped or misconfigured publication table surfaces
/// `CassandraPublicationError::Database`, which classifies `Terminal` or
/// `Transient`. The barrier retries it forever anyway, so a schema-level fault
/// blocks dispatch until an operator repairs the schema. Blocking is
/// deliberate: committing published state with no routing row to advertise it
/// would leave that state permanently unreachable. The only non-`Published`
/// outcome is a shutdown, which abandons before anything stages.
enum PublishOutcome {
    /// Every touched `Published` collection has a routing row (or there was
    /// nothing to publish).
    Published,

    /// Shutdown intervened before publication completed. The caller abandons
    /// before staging, so redelivery re-runs from clean state.
    ShuttingDown,
}

/// The durability sequence: the single owner of stage → arm → marker-record →
/// commit → promote, run once per event after the stack returns its final
/// `result`. Both the blanket [`EventHandler`] impl and
/// [`RetryHandler`](super::retry::RetryHandler) route their final outcome
/// here, so the wrong ordering (marker before stage) is structurally
/// unwritable.
///
/// Branches on the typed [`Settlement`] classification first, the error
/// category second: a [`Settlement::Bypassed`] result stages nothing and
/// records no marker (see the variant's parity argument); a
/// [`Settlement::Final`] result runs the full sequence on `Ok`, records the
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
    G: Uncommitted + Send,
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

/// The success arm of [`settle`]: publish, stage, arm the backstop, record the
/// marker strictly after the stage, commit, then promote the staged cells
/// through the receipt.
///
/// The marker is read from the session's event identity
/// (`message_marker()`: the message `EventRef`'s dedup id, or the
/// deferred-reload override) and written through the settlement-private
/// [`MarkerWrite`] capability — no middleware deposits it, so
/// stage-before-marker is enforced by this one straight-line function.
///
/// # Crash windows
///
/// The step order — publish → stage → **arm** → marker record → **commit** →
/// promote — closes every crash window without any acquisition-time sweep
/// (there is none):
///
/// * Crash after the publication upsert, before the stage: a routing row stands
///   over empty state, which a reader observes as a harmless absent value;
///   redelivery re-stages.
/// * Crash after the stage, before the arm: the offset never commits, so the
///   event **redelivers**, re-stages, and re-arms; the redelivered handler's
///   own reads first-touch-resolve the orphan to its committed base.
/// * Crash after the arm, before the commit: still uncommitted → redelivery,
///   *and* the backstop is armed → the sweep resolves it either way.
/// * Crash after the commit, before the promote: committed (no redelivery), but
///   the backstop is armed → the sweep resolves; the recorded marker also
///   dedup-filters any redelivery.
///
/// So every durable provisional cell is reached by redelivery
/// (arm-precedes-commit) or an armed backstop. The lone first-touch-only
/// residual is the permanent-partial-stage path below (a `finalize` `Skip`
/// committed unarmed), an accepted edge bounded by first-touch and the cell
/// TTL.
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
    G: Uncommitted + Send,
{
    let Some(lifecycle) = lifecycle else {
        // Invalidated / stateless context: just commit and fire the hook.
        guard.commit().await;
        drop(permit);
        fire_apply_hook(handler, context, true, result).await;
        return;
    };

    // 0. First-write publication barrier. A `Published` collection's routing
    // row must exist before its committed state does, so publish before the
    // stage. This must succeed: it retries until shutdown. A shutdown here
    // abandons before anything stages, so the marker is untouched and
    // redelivery re-runs from a clean state.
    if let PublishOutcome::ShuttingDown = publish_first_writes(&context, lifecycle).await {
        discard_uncommitted(Some(lifecycle));
        drop(permit);
        abandon(handler, context, guard, result).await;
        return;
    }

    // 1. Stage provisional cells / write resolved, retrying transient
    // failures.
    let finalized =
        match retry_step(&context, "keyed-state finalize", || lifecycle.finalize()).await {
            StepOutcome::Done(finalized) => finalized,
            StepOutcome::Skip => {
                // Permanent stage failure: a partial stage may be durable. Arm
                // the backstop defensively so the sweep resolves it, skip the
                // marker record (invariant: marker present ⇒ stage durable),
                // and commit. A shutdown `ShuttingDown` from the arm is
                // deliberately ignored: committing a permanently-unstageable
                // event beats livelocking, and first-touch heals the unarmed
                // cell (the sole first-touch-only recovery residual —
                // everything else is redelivery or an armed sweep). No receipt
                // exists to carry a finalize-folded delay, so the defensive
                // arm uses the plain floor.
                let _ = arm_backstop(&context, lifecycle, lifecycle.recovery_floor()).await;
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
    // a shutdown, which aborts *before* the marker record: the receipt rolls
    // the staged cells back to their committed base (no lingering provisional,
    // nothing to TTL out) and the offset aborts so the event redelivers,
    // re-runs, and re-arms.
    let promotable = match finalized {
        Finalized::Clean => None,
        Finalized::Staged(staged) => {
            match arm_backstop(&context, lifecycle, staged.recovery_delay()).await {
                ArmOutcome::Armed => Some(staged.certify()),
                ArmOutcome::ShuttingDown => {
                    // The ONE reachable rollback site — before any
                    // marker-record attempt, so restoring the committed base
                    // is sound; past `certify` a rollback no longer compiles.
                    guard.abort().await;
                    staged.rollback().await;
                    drop(permit);
                    fire_apply_hook(handler, context, false, result).await;
                    return;
                }
            }
        }
    };

    // 3. Record the message commit marker — STRICTLY after the stage, so a
    // present marker always certifies a durable stage. Timer events carry no
    // message marker (`message_marker()` is `None` on a timer session with no
    // reload override); the trigger commit is their dedup. Like the arm, the
    // record is must-succeed: the marker is framework data (a bare dedup id),
    // so no failure here is a data rejection the sequence may skip.
    // Committing with the stage uncertified would have the armed sweep
    // silently roll a successful handler's writes back — with the offset
    // committed, nothing ever replays them. A permanently-failing store
    // therefore retries until it heals (or the liveness probe restarts the
    // process, the visible last resort); only shutdown abandons.
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
                    // A record attempt was made: marker durability is
                    // ambiguous, so the staged cells must not (and
                    // structurally cannot) roll back — see
                    // [`StagedState::certify`]; the armed sweep resolves them.
                    // The buffer is already drained (finalize succeeded to
                    // reach here), so this discard is a provable no-op included
                    // only for a local, gap-free argument at the drop→`abandon`
                    // reacquire.
                    discard_uncommitted(Some(lifecycle));
                    drop(permit);
                    abandon(handler, context, guard, result).await;
                    return;
                }
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

    // 6. After-commit hook (telemetry, dedup forwarding, ...). The permit
    // drops first, so the hooks' post-settle state reads proceed.
    drop(permit);
    fire_apply_hook(handler, context, true, result).await;
}

/// Abandons the event: abort the marker (offset → redelivery, timer →
/// reloadable) and fire `after_abort`. Reached on a terminal error or a
/// shutdown mid-sequence.
///
/// Never *promotes* keyed state: certified staged cells (if any exist) stay
/// provisional for redelivery, first-touch, or the armed sweep to resolve
/// through the oracle — the one inline rollback lives at the arm-shutdown arm
/// of `settle_committed`, where possession of the un-certified receipt proves
/// it is sound. It does discard the uncommitted dirty overlay under the held
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
    G: Uncommitted + Send,
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

/// Runs the first-write publication barrier for every `Published` collection
/// this event touched, retrying until it succeeds or shutdown intervenes.
///
/// Must-succeed (see [`PublishOutcome`]): every non-shutdown failure retries
/// forever, so this never emits a `Terminal` and never abandons in normal
/// operation. Called as settle step 0 — before any stage — so a published
/// collection's committed state can never exist without its routing row. The
/// barrier is idempotent and memoized, so retries and a prior `commit()`'s
/// publication cost nothing.
async fn publish_first_writes<C>(context: &C, lifecycle: &C::State) -> PublishOutcome
where
    C: EventContext,
{
    loop {
        if context.is_shutdown() {
            return PublishOutcome::ShuttingDown;
        }
        match lifecycle.publish_first_writes().await {
            Ok(()) => return PublishOutcome::Published,
            Err(error) => {
                error!(error = %error, "keyed-state publication failed; retrying");
                sleep(DURABILITY_RETRY_DELAY).await;
            }
        }
    }
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
        // already sweeps this commit's staged cells, so skip re-arming. Per-key
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
