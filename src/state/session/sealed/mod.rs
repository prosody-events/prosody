use super::{
    AdmissionChecks, CellKey, CellStore, CollectionRef, Duration, EventMarker, Future, MarkerWrite,
    ProvisionalWrite, RepinProof, STATE_FANOUT_CONCURRENCY, StateAccessError, StepOutcome, Uuid,
    resolve_collections, retry_step,
};
use futures::{StreamExt, stream};
use tokio::task::coop::cooperative;
use tracing::{error, warn};

mod gate;

pub use gate::{MutatePermit, OpPermit, SessionGate};

/// The message dedup id that settlement records and the duplicate filter
/// reads. Message sessions use their event id. Deferred reloads supply
/// an override through [`MarkerIdentity::set_reload_marker`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageMarker(Uuid);

impl MessageMarker {
    /// Wraps a message's dedup id.
    #[must_use]
    pub(crate) fn new(dedup_id: Uuid) -> Self {
        Self(dedup_id)
    }

    /// The raw id for dedup writes and lookups.
    #[must_use]
    pub(crate) fn into_uuid(self) -> Uuid {
        self.0
    }
}

/// One collection's staged cells and frozen marker, retained for promote.
/// Only `ReadCommitted` collections enter this receipt. A clear-only
/// collection has no cell writes. The durable marker stores
/// coordinates. This receipt retains values so promote needs no cell
/// reload.
// `Vec`, not `SmallVec`, deliberately: the receipt is held across the
// settle boundary's awaits, so inline entries bloat every such future past
// clippy's `large_futures` bound. The `with_capacity` folds at the build
// sites already bound the allocation.
pub struct StagedCollection {
    pub(super) collection: CollectionRef,
    pub(super) writes: Vec<(CellKey, ProvisionalWrite)>,
    pub(super) marker: EventMarker,
}

/// The provisional work that a successful stage produced.
#[must_use]
pub enum Finalized<S: CellStore, K: AdmissionChecks> {
    /// No provisional work remains.
    Clean,
    /// Promote the staged collections before the dedup row.
    Staged(Staged<S, K>),
}

/// One event's staged collections. Only finalize constructs this value.
#[must_use]
pub struct Staged<S: CellStore, K: AdmissionChecks> {
    pub(super) store: S,
    pub(super) collections: Vec<StagedCollection>,
    pub(super) checks: K,
    pub(super) key: crate::Key,
}

/// The promote result retains rejected collections for rollback.
pub(crate) enum Promoted<S: CellStore, K: AdmissionChecks> {
    Complete,
    Torn(Staged<S, K>),
    Rejected(Staged<S, K>),
    Abandoned,
}

impl<S: CellStore, K: AdmissionChecks> Staged<S, K> {
    /// Returns rejected collections for rollback.
    /// Shutdown removes the admission proof.
    pub(crate) async fn promote(mut self, shutdown: impl Fn() -> bool + Sync) -> Promoted<S, K> {
        let total = self.collections.len();
        self.collections = resolve_collections(&self.store, self.collections, &shutdown).await;
        if shutdown() {
            if let Err(error) = self.checks.unmark(&self.key).await {
                warn!(%error, key = %self.key, "cannot remove admission proof");
            }
            Promoted::Abandoned
        } else if self.collections.is_empty() {
            Promoted::Complete
        } else if self.collections.len() == total {
            Promoted::Rejected(self)
        } else {
            Promoted::Torn(self)
        }
    }

    /// Restores the rejected collections before the boundary commits the
    /// source.
    pub(crate) async fn abort(self, shutdown: impl Fn() -> bool + Sync) -> bool {
        let names: smallvec::SmallVec<[&str; 8]> = self
            .collections
            .iter()
            .map(|staged| staged.collection.id().name().as_str())
            .collect();
        error!(key = %self.key, collections = ?names,
            "promote rejected collections; restore committed state");
        let complete = stream::iter(0..self.collections.len())
            .map(|index| {
                let staged = &self.collections[index];
                cooperative(async {
                    !matches!(
                        retry_step(&shutdown, "keyed-state rollback", || {
                            self.store
                                .abort_provisional(&staged.collection, &staged.writes)
                        })
                        .await,
                        StepOutcome::Abandon
                    )
                })
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .fold(true, |all, done| async move { all && done })
            .await;
        if let Err(error) = self.checks.unmark(&self.key).await {
            warn!(%error, key = %self.key, "cannot remove admission proof");
        }
        complete
    }
}

/// Framework-only lifecycle over a per-event session: the settle boundary's
/// stage/record/promote moves plus the attempt and teardown verbs.
pub trait StateLifecycle: Clone + Send + Sync + 'static {
    /// The uniform durable cell store the session settles against —
    /// [`KeyedStateSession`](super::KeyedStateSession) projects its
    /// backend's store (`B::Cell`).
    type Cell: CellStore;

    /// The admission proof store.
    type Checks: AdmissionChecks;

    /// The session's operation gate (KV4) — the engine acquires each
    /// operation's permit through this accessor. On the sealed lifecycle
    /// trait, not on [`EventSession`](super::EventSession): the gate is
    /// framework plumbing, never a handler surface.
    fn gate(&self) -> &SessionGate;

    /// Closes the session's gate for settlement — one acquire, phase
    /// `Closed`, permit returned — tagging the wait warnings with this
    /// session's event and key. See [`SessionGate::close`].
    fn close_gate(&self) -> impl Future<Output = OpPermit<'_>> + Send + use<'_, Self>;

    /// Resolves every touched collection by its commit mode:
    /// `ReadCommitted` collections stage a provisional cell,
    /// `ReadUncommitted` collections write a resolved value — returning
    /// the staged work as the linear [`Finalized`] receipt the boundary
    /// consumes. Stages all collections before returning, so a stage
    /// error returns before the textually-later marker record; a staging
    /// failure is a type-erased store error with no receipt minted.
    ///
    /// On success the event's dirty range is drained — the stage consumes
    /// the buffered ops (including a [`Finalized::Clean`] return whose
    /// only work was `ReadUncommitted` resolved writes), so a second
    /// `finalize` finds an empty buffer and returns `Clean`: one stage
    /// mints at most one receipt. Failure paths leave the buffer whole so
    /// a retried `finalize` re-stages idempotently.
    fn finalize(
        &self,
    ) -> impl Future<Output = Result<Finalized<Self::Cell, Self::Checks>, StateAccessError>>
    + Send
    + use<'_, Self>;

    /// Records the message dedup id with an idempotent upsert.
    /// The boundary retries failures before it commits the source.
    fn record_marker(
        &self,
        marker: MessageMarker,
        proof: MarkerWrite,
    ) -> impl Future<Output = Result<(), StateAccessError>> + Send + use<'_, Self>;

    /// Discards just this event's buffered dirty cells — the isolation step
    /// of the attempt-boundary [`Self::reset`] transition (which then bumps
    /// the epoch under the same gate hold), and the failure-path backstop
    /// that [`EventStateScope`](crate::state::manager::EventStateScope)'s
    /// `Drop` runs on every exit path (error, abandon, panic unwind).
    ///
    /// The dirty workspace is partition-lifetime (manager-owned, shared by
    /// every session clone), so it must be cleared explicitly per event.
    /// The reload identity override is deliberately **not** cleared here
    /// (never cleared at all — see [`MarkerIdentity::set_reload_marker`]).
    /// On the success path [`Self::finalize`] has already drained
    /// the buffer (the stage consumes it), making the scope-drop
    /// clear a no-op there; the receipt's promote/rollback read
    /// only receipt-owned data, never dirty.
    fn discard_dirty(&self);

    /// Flips this session terminated, synchronously and idempotently — the
    /// teardown half of the session's `is_terminated`.
    /// The [`EventStateScope`](crate::state::manager::EventStateScope)'s
    /// `Drop` calls it on every dispatch exit (including a future dropped
    /// mid-flight, where no gated teardown runs), and the panic-unwind
    /// catch calls it under the held closed gate. After it, every op on any
    /// clone of this session errors — reads/inits `Terminated`, and a
    /// current-pin mutation past the closed gate `SessionClosed` (the gate
    /// closes first in the catch). Writes no epoch: a genuinely-leaked
    /// stale clone stays fenced by its old pin regardless.
    fn terminate(&self);

    /// The attempt-boundary transition: acquire the gate, discard this
    /// attempt's dirty overlay, and bump the epoch — all under **one** gate
    /// hold, so a stale queued write cannot land between the clear and the
    /// bump and survive into the next attempt. This is the epoch's ONLY
    /// bump site. Gated by [`RepinProof`] so a lone bump (a partial reset
    /// with no matching re-pin) is unwritable outside the two mint sites.
    fn reset(&self, proof: RepinProof) -> impl Future<Output = ()> + Send + use<'_, Self>;

    /// A session clone re-pinned to the CURRENT epoch — the crate-internal
    /// re-pin constructor. [`RepinProof`]-gated so only the two mint sites
    /// (the `next_attempt` verb and the settle final-hook stamp) can
    /// produce a live attempt-N+1 (or stamped-final) view.
    fn repin(&self, proof: RepinProof) -> Self;
}

/// The message commit-marker identity surface — the sole home of
/// `set_reload_marker`/`message_marker` (they are deliberately **not** on
/// [`StateLifecycle`], so the settlement surface carries no marker
/// vocabulary). Exactly three audiences reach it, each through the narrow
/// [`MarkerHandle`](super::MarkerHandle) rather than a raw session: the
/// message-defer reload path *sets* the override, and the dedup filter and
/// the settle boundary *read* it.
pub trait MarkerIdentity {
    /// Sets the deferred-reload identity override: the dedup id of the
    /// message the current dispatch loaded from the defer queue, set at
    /// exactly one site — the message-defer reload path, immediately
    /// after the load succeeds and before the inner dispatch.
    ///
    /// **Last-wins and never cleared.** Last-wins is load-bearing: a
    /// retry re-dispatch of the same defer timer after an ambiguous
    /// durable queue advance loads the *next* queued message — a
    /// different id — and the override must re-point at it (a set-once
    /// cell would dispatch message B under message A's identity). Never
    /// cleared is safe because every settle arm that consults
    /// [`Self::message_marker`] implies the final attempt's inner ran,
    /// which implies that attempt's reload performed the set — a stale
    /// read is unreachable, not merely forbidden.
    fn set_reload_marker(&self, marker: MessageMarker);

    /// The message commit-marker identity for this event: a message
    /// session's [`EventRef::Message`](crate::state::EventRef) dedup id;
    /// on a timer session, the reload override; else `None` (a pure
    /// timer, whose trigger commit is its dedup). A message session
    /// never reads the override — the match arm structurally ignores it,
    /// so a divergent override is unreadable rather than forbidden.
    fn message_marker(&self) -> Option<MessageMarker>;
}
