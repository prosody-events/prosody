//! The settlement lifecycle and message-marker identity of a session.

use super::KeyedStateSession;
use super::sealed::{
    Finalized, MarkerIdentity, MessageMarker, OpPermit, SessionGate, Staged, StateLifecycle,
};
use super::stage::{abort_stages, stage_collection};
use crate::consumer::middleware::deduplication::DeduplicationStore;
use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::access::StateAccessError;
use crate::state::backend::AdmissionChecks;
use crate::state::identity::CollectionId;
use crate::state::marker::{AttemptId, EventEvidence};
use crate::state::{CommitMode, EventRef, STATE_FANOUT_CONCURRENCY, StateBackend};
use futures::stream::{self, StreamExt};
use std::sync::atomic::Ordering;
use tokio::task::coop::cooperative;
use tracing::warn;

impl<B, L> StateLifecycle for KeyedStateSession<B, L>
where
    B: StateBackend,
    L: Clone + Send + Sync + 'static,
{
    type Cell = B::Cell;
    type Checks = B::Checks;

    fn gate(&self) -> &SessionGate {
        &self.inner.gate
    }

    async fn close_gate(&self) -> OpPermit<'_> {
        let event = self.inner.event;
        let key = &self.inner.state_key.key;
        self.inner
            .gate
            .close(|waited_s| {
                warn!(
                    event = ?event,
                    key = %key,
                    waited_s,
                    "settle waiting on the session operation gate; a session op future may be \
                     held un-polled"
                );
            })
            .await
    }

    async fn finalize(&self) -> Result<Finalized<B::Cell, B::Checks>, StateAccessError> {
        let touched = self
            .inner
            .overlay
            .dirty()
            .touched(&self.inner.state_key.key);
        let event = self.inner.event;
        let registry = &self.inner.registry;
        let lower = self.inner.overlay.lower();
        let state_key = &self.inner.state_key;
        let mut marker_touched = Vec::with_capacity(touched.len());
        for ((state_type, name), ..) in &touched {
            if registry.commit_mode_for(*state_type, name) == CommitMode::ReadCommitted {
                marker_touched.push((*state_type, name.clone()));
            }
        }
        marker_touched.sort_unstable();
        marker_touched.dedup();
        // Sized once to the touched-collection cardinality — the fold in
        // place of an unconstrained `try_collect` keeps the receipt's vector
        // from re-growing on the per-event hot path (bounded-allocation rule).
        let evidence = EventEvidence {
            attempt: *self.inner.stage_attempt.get_or_init(AttemptId::new),
            touched: marker_touched.into(),
            evidence_ttl: self.inner.dedup_ttl,
            dedup: self.message_marker().map(MessageMarker::into_uuid),
        };
        let capacity = touched.len();
        let collections = stream::iter(touched)
            .map(|((state_type, name), cleared, cells)| {
                let id = CollectionId::new(state_key.clone(), state_type, name);
                cooperative(stage_collection(
                    lower, registry, event, id, cleared, cells, &evidence,
                ))
            })
            .buffer_unordered(STATE_FANOUT_CONCURRENCY)
            .fold(Ok(Vec::with_capacity(capacity)), |acc, staged| async move {
                match (acc, staged) {
                    (Ok(mut acc), Ok(staged)) => {
                        acc.extend(staged);
                        Ok(acc)
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            })
            .await;
        let collections = match collections {
            Ok(collections) => collections,
            Err(error) => {
                if error.classify_error() == ErrorCategory::Permanent {
                    abort_stages(lower, registry, state_key, &evidence.touched, || {
                        self.is_terminated()
                    })
                    .await;
                }
                if let Err(unmark_error) = self.inner.checks.unmark(&self.inner.state_key.key).await
                {
                    warn!(error = %unmark_error, key = %self.inner.state_key.key,
                        "cannot remove admission proof after stage failure");
                }
                return Err(error);
            }
        };
        self.discard_dirty();
        if collections.is_empty() {
            return Ok(Finalized::Clean);
        }
        Ok(Finalized::Staged(Staged {
            store: lower.clone(),
            collections,
            checks: self.inner.checks.clone(),
            key: self.inner.state_key.key.clone(),
        }))
    }

    async fn record_marker(
        &self,
        marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> Result<(), StateAccessError> {
        self.inner
            .dedup
            .insert(marker.into_uuid())
            .await
            .map_err(|e| StateAccessError::store(&e))
    }

    fn discard_dirty(&self) {
        // Sync and ungated (Drop paths cannot await). Every caller either holds
        // the gate — settle/unwind under the closed-gate permit, or `reset`
        // under its read-permit, which waits out any in-flight session op
        // before clearing — or is the ungated `EventStateScope::Drop` teardown,
        // whose already-admitted-op residual is documented on that type. A
        // clone detached past its attempt does not survive into the next
        // attempt: `reset` bumps the epoch under the same hold (see
        // `AttemptEpoch`), so the stale write errors `Terminated`. Keep session
        // ops inside the owning handler future all the same.
        self.inner
            .overlay
            .dirty()
            .clear_event(&self.inner.state_key.key);
    }

    fn terminate(&self) {
        self.inner.terminated.store(true, Ordering::Relaxed);
    }

    async fn reset(&self, _proof: RepinProof) {
        // ONE gate hold spanning discard-then-bump. Separate steps would let a
        // stale queued write acquire the gate after the clear, buffer under the
        // old epoch, and survive into attempt N+1. Holding the gate here also
        // waits out any in-flight session op (the no-un-polled-op contract
        // still applies), so the discard sees a quiescent dirty range.
        let _permit = self.inner.gate.read().await;
        self.discard_dirty();
        self.bump_epoch();
    }

    fn repin(&self, _proof: RepinProof) -> Self {
        Self {
            inner: self.inner.clone(),
            pinned: self.current_epoch(),
        }
    }
}

impl<B, L> MarkerIdentity for KeyedStateSession<B, L>
where
    B: StateBackend,
{
    fn set_reload_marker(&self, marker: MessageMarker) {
        // Override implies timer session: only the deferred-message reload
        // sets it, and that reload always dispatches under a timer EventRef.
        debug_assert!(
            matches!(self.inner.event, EventRef::Timer(_)),
            "the reload override is set only on timer sessions"
        );
        *self.inner.reload_marker.lock() = Some(marker);
    }

    fn message_marker(&self) -> Option<MessageMarker> {
        match self.inner.event {
            // The message's own id — the override is never read here, so a
            // divergent override on a message session is unreadable.
            EventRef::Message { dedup_id } => Some(MessageMarker::new(dedup_id)),
            EventRef::Timer(_) => *self.inner.reload_marker.lock(),
        }
    }
}
