//! The event settlement boundary.
//!
//! The boundary closes the session gate after the middleware stack returns.
//! A successful final result stages state, promotes it, records the message
//! dedup id, and commits the source. The final or bypassed classification
//! determines whether the boundary stages state. The blanket `EventHandler`
//! implementation and `RetryHandler` use this same boundary.
//!
//! Apply hooks run after the permit drops. The boundary re-pins their context
//! so reads observe the settled state.

use std::future::Future;

use crate::state::retry::{DURABILITY_RETRY_DELAY, StepOutcome, retry_step};
use tokio::time::sleep;
use tracing::Level;

use super::FallibleHandler;
use crate::consumer::Uncommitted;
use crate::consumer::event_context::EventContext;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::state::descriptor::Registered;
use crate::state::session::sealed::{MarkerIdentity, StateLifecycle};
use crate::state::session::{Finalized, LifecycleAccess, MessageMarker, OpPermit};

/// Gives the settlement boundary access to the sealed session lifecycle.
/// Other middleware uses the narrower message-marker interface.
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
    /// The event's final result. Success stages, promotes, records dedup, and
    /// commits the source. A permanent rejection records dedup and commits
    /// the source without state changes.
    Final,

    /// Another operation owns the outcome. Commit the source and discard the
    /// dirty overlay. This dispatch stages no state and records no dedup
    /// id.
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

/// Restricts message dedup writes to the settlement boundary.
/// Only this module can construct the capability. The sealed lifecycle carries
/// it in its write signature.
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

/// Settles one final result and calls one apply hook.
///
/// A final success stages state, promotes it, records dedup, and commits the
/// source. A permanent rejection records dedup without state changes. A
/// bypassed or transient result commits the source without state or dedup.
/// A terminal result abandons the source before classification.
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
        // yields `Finalized::Clean`, which has no provisional work.
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
/// the remaining uncommitted ops. Provisional cells live in the durable store,
/// so this never touches them. A stateless / invalidated context (`None`) has
/// no overlay.
fn discard_uncommitted<S: StateLifecycle>(lifecycle: Option<&S>) {
    if let Some(lifecycle) = lifecycle {
        lifecycle.discard_dirty();
    }
}

/// Settles a successful final handler result while the session gate stays
/// closed. Stage all cells before promotion. Promotion writes positive evidence
/// before destructive cell changes. Record the payload's message identity after
/// promotion, then commit the source. A crash before evidence leaves
/// uncommitted residue. Admission rolls that residue back. A crash after
/// evidence leaves committed residue. Admission promotes it and retires its
/// source before dispatch.
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

    // Stage provisional cells and write resolved cells.
    let finalized = match retry_step(
        || context.is_shutdown(),
        "keyed-state finalize",
        Level::ERROR,
        || lifecycle.finalize(),
    )
    .await
    {
        StepOutcome::Done(finalized) => finalized,
        StepOutcome::Skip => {
            if let Some(marker) = lifecycle.message_marker() {
                record_marker_best_effort(&context, lifecycle, marker).await;
            }
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
            // Admission resolves the partial stage. Discard the overlay
            // before the permit drops so a leaked read sees no dirty data.
            discard_uncommitted(Some(lifecycle));
            drop(permit);
            abandon(handler, context, guard, result).await;
            return;
        }
    };

    // The first successful promote is the commit point.
    if let Finalized::Staged(staged) = finalized
        && !staged.promote(|| context.is_shutdown()).await
    {
        drop(permit);
        abandon(handler, context, guard, result).await;
        return;
    }

    // Record the message identity after all promote attempts finish.
    if let Some(marker) = lifecycle.message_marker() {
        loop {
            match retry_step(
                || context.is_shutdown(),
                "keyed-state marker record",
                Level::ERROR,
                || lifecycle.record_marker(marker, MarkerWrite(())),
            )
            .await
            {
                StepOutcome::Done(()) => break,
                StepOutcome::Skip => sleep(DURABILITY_RETRY_DELAY).await,
                StepOutcome::Abandon => {
                    // Admission completes any residue after source redelivery.
                    discard_uncommitted(Some(lifecycle));
                    drop(permit);
                    abandon(handler, context, guard, result).await;
                    return;
                }
            }
        }
    }

    // Commit the source offset or trigger.
    guard.commit().await;

    drop(permit);
    fire_apply_hook(handler, context, true, result).await;
}

/// Abandons the source and calls `after_abort`.
///
/// The closed gate protects the dirty overlay discard. Admission resolves
/// durable residue before the next dispatch. Hook reads project committed
/// values, including any successful mid-handler `commit()`.
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

/// Commits an event rejected by admission, without a handler or an apply hook.
pub(crate) async fn reject_admission<C: EventContext, G: Uncommitted + Send>(
    context: &C,
    guard: G,
) {
    if let Ok(lifecycle) = context.settle_lifecycle()
        && let Some(marker) = lifecycle.message_marker()
    {
        record_marker_best_effort(context, &lifecycle, marker).await;
    }
    guard.commit().await;
}

/// Records `marker` best-effort, retrying transient failures; a permanent
/// failure or shutdown is tolerated (the failed-but-final message simply
/// isn't deduplicated and re-runs, re-failing the same way).
async fn record_marker_best_effort<C>(context: &C, lifecycle: &C::State, marker: MessageMarker)
where
    C: EventContext,
{
    let _ = retry_step(
        || context.is_shutdown(),
        "keyed-state marker record",
        Level::ERROR,
        || lifecycle.record_marker(marker, MarkerWrite(())),
    )
    .await;
}
