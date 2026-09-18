//! The event settlement boundary.
//!
//! The leaf adapter maps the handler's error category to one [`Settlement`]
//! action. Wrappers forward that action or name an action for their own
//! outcomes. `Final` stages state, promotes it, records the marker, and commits
//! the source. `Rejected` discards state, records the marker best effort, and
//! commits the source. `Bypassed` discards state and commits the source without
//! a marker. `Abandoned` discards state and aborts the source without a marker.
//!
//! The source verb selects `after_commit` or `after_abort`.
//! Apply hooks run after the permit drops. [`stamp`] re-pins their context.
//! State rejection records no marker. The source commits only after state
//! resolution.

use opentelemetry::global::meter;
use opentelemetry::metrics::Counter;
use std::future::Future;
use std::sync::LazyLock;

use crate::state::retry::{DURABILITY_RETRY_DELAY, StepOutcome, retry_step};
use tokio::time::sleep;

use super::FallibleHandler;
use crate::consumer::Uncommitted;
use crate::consumer::event_context::EventContext;
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::state::descriptor::Registered;
use crate::state::session::sealed::{MarkerIdentity, StateLifecycle};
use crate::state::session::{Finalized, LifecycleAccess, MessageMarker, OpPermit, Promoted};

/// Counts each event whose promote succeeds for some collections and rejects
/// others.
static PROMOTE_TORN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter("prosody")
        .u64_counter("prosody.state.promote.torn")
        .with_description("Events whose promote succeeds for some collections and rejects others")
        .with_unit("{promote}")
        .build()
});

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

/// Only the leaf adapter creates this proof. Wrappers forward it unchanged.
/// The private field prevents wrappers from constructing it directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LeafProof(());

/// The action the settlement boundary takes on an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// Stage and promote state. Record the marker, then commit the source.
    Final(LeafProof),
    /// Discard state. Record the marker best effort, then commit the source.
    Rejected(LeafProof),
    /// Discard state and commit the source without a marker.
    Bypassed,
    /// Discard state and abort the source without a marker.
    Abandoned,
}

/// Selects `Final`, `Rejected`, `Bypassed`, or `Abandoned` for the boundary.
///
/// The leaf adapter maps the handler's error category. Wrappers forward inner
/// results or name an action for their own outcomes.
/// Plain wrapper error arms keep public signatures simpler than a generic error
/// enum.
pub(crate) trait SettlementHandler: FallibleHandler {
    /// Selects the action for the stack's final result.
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement;
}

/// Restricts message dedup writes to the settlement boundary.
/// Only this module can construct the capability. The sealed lifecycle carries
/// it in its write signature.
pub struct MarkerWrite(());

/// Permits a context re-pin after reset or settlement.
/// Only this module creates production proofs, through `next_attempt` and
/// `stamp`. A leaked stale context cannot re-pin itself.
/// The public `EventContext::redispatch` signature requires public visibility.
pub struct RepinProof(());

impl RepinProof {
    /// Creates a proof for tests that drive reset, re-pin, or redispatch
    /// directly.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self(())
    }
}

/// Maps the leaf handler's own result to its action. Only the leaf
/// adapter calls this. Wrappers forward the result.
pub(in crate::consumer::middleware) fn leaf_settlement<O, E: ClassifyError>(
    result: Result<&O, &E>,
) -> Settlement {
    match result {
        Ok(_) => Settlement::Final(LeafProof(())),
        Err(error) => match error.classify_error() {
            ErrorCategory::Permanent => Settlement::Rejected(LeafProof(())),
            // The retry layer stopped and logged the discard.
            ErrorCategory::Transient => Settlement::Bypassed,
            ErrorCategory::Terminal => Settlement::Abandoned,
        },
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
        // re-pin below is then a no-op rebuild. Create the reset proof here.
        if let Ok(session) = self.settle_lifecycle() {
            session.reset(RepinProof(())).await;
        }
        // Re-pin to the new epoch.
        self.redispatch(RepinProof(()))
    }
}

/// Applies the selected [`Settlement`] action and calls one source hook.
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
        Settlement::Final(_) => {
            settle_committed(handler, context, guard, result, lifecycle.as_ref(), permit).await;
        }
        Settlement::Rejected(_) => {
            if let Some(lifecycle) = &lifecycle
                && let Some(marker) = lifecycle.message_marker()
            {
                record_marker_best_effort(&context, lifecycle, marker).await;
            }
            commit_and_finish(handler, context, guard, result, lifecycle.as_ref(), permit).await;
        }
        Settlement::Bypassed => {
            commit_and_finish(handler, context, guard, result, lifecycle.as_ref(), permit).await;
        }
        Settlement::Abandoned => {
            drop(permit);
            abandon(handler, context, guard, result).await;
        }
    }
}

/// Commits the source and discards dirty state before the permit drops and the
/// apply hook runs. A successful finalize already drains the overlay; the
/// repeated discard is harmless.
async fn commit_and_finish<'a, T, C, G>(
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
    guard.commit().await;
    discard_uncommitted(lifecycle);
    drop(permit);
    handler.after_commit(stamp(&context), result).await;
}

/// Discards the uncommitted overlay under the closed gate before hook reads can
/// proceed. A mid-handler `commit()` already applies and drains its cells; this
/// discard preserves those values and all durable provisional cells.
/// An invalidated context has no overlay.
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
        commit_and_finish(handler, context, guard, result, None, permit).await;
        return;
    };

    // Stage provisional cells and write resolved cells.
    let finalized = match retry_step(
        || context.is_shutdown(),
        "keyed-state finalize",
        || lifecycle.finalize(),
    )
    .await
    {
        StepOutcome::Done(finalized) => finalized,
        StepOutcome::Skip => {
            if context.is_shutdown() {
                drop(permit);
                abandon(handler, context, guard, result).await;
                return;
            }
            // Finalize failed and left the dirty overlay intact.
            commit_and_finish(handler, context, guard, result, Some(lifecycle), permit).await;
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
    if let Finalized::Staged(staged) = finalized {
        let outcome = staged.promote(|| context.is_shutdown()).await;
        let complete = match outcome {
            Promoted::Complete => true,
            Promoted::Abandoned => false,
            Promoted::Rejected(rejected) => {
                if rejected.abort(|| context.is_shutdown()).await {
                    commit_and_finish(handler, context, guard, result, Some(lifecycle), permit)
                        .await;
                    return;
                }
                false
            }
            Promoted::Torn(rejected) => {
                PROMOTE_TORN.add(1, &[]);
                rejected.abort(|| context.is_shutdown()).await
            }
        };
        if !complete {
            drop(permit);
            abandon(handler, context, guard, result).await;
            return;
        }
    }

    // Record the message identity after all promote attempts finish.
    if let Some(marker) = lifecycle.message_marker() {
        // The record must succeed, so the outer loop retries permanent errors too.
        loop {
            match retry_step(
                || context.is_shutdown(),
                "keyed-state marker record",
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
    commit_and_finish(handler, context, guard, result, Some(lifecycle), permit).await;
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
    handler.after_abort(stamp(&context), result).await;
}

/// Re-pins the hook context so reads observe settled state.
///
/// Inner retries advance the shared epoch and leave the boundary context stale.
/// The gate is closed, so this re-pin changes no epoch and permits no mutation.
/// Leaked context clones keep their old epoch and remain fenced.
fn stamp<C: EventContext>(context: &C) -> C {
    context.redispatch(RepinProof(()))
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
        || lifecycle.record_marker(marker, MarkerWrite(())),
    )
    .await;
}
