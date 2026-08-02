//! Publication-source acquisition: identity admission, the cached refresh, and
//! retry pacing.
//!
//! [`SnapshotState`] is what one reader remembers between operations, and
//! [`StateReader::snapshot`] is the refresh-if-stale entry point every read
//! goes through.

use super::admission::{Admission, Diagnostics};
use crate::state::descriptor::StateDescriptor;
use crate::state::publication::PublicationStore;
use crate::state_reader::ReaderBackend;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::source::{MAX_PUBLICATION_SOURCES, NoSnapshot, ValidatedPublications};
use crate::{Codec, state_reader::reader::StateReader};
use arc_swap::{ArcSwap, Guard};
use quanta::Instant;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// The default snapshot-refresh cadence: a source list changes rarely, so the
/// reader re-reads the routing table at most once per minute per collection.
pub(in crate::state_reader) const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// How long a failed refresh paces the next attempt. Unpaced, every read during
/// a routing-table outage pays its own store round trip before falling back to
/// the held snapshot, so a burst of reads waits one timeout each. Well under
/// [`DEFAULT_REFRESH_INTERVAL`], so a recovered store is picked up promptly.
pub(in crate::state_reader) const REFRESH_BACKOFF: Duration = Duration::from_secs(5);

/// How long a refresh that found nothing paces the next attempt.
///
/// A reader deployed ahead of its publisher reads a collection that has no
/// routing row yet, and that is a normal startup state rather than a fault.
/// Unpaced, every such read re-reads the routing table, so a read-heavy caller
/// turns a missing publisher into unbounded store load for as long as it is
/// missing. Much shorter than [`REFRESH_BACKOFF`], since nothing is broken: a
/// publisher that appears is admitted within a second.
pub(in crate::state_reader) const ABSENT_BACKOFF: Duration = Duration::from_secs(1);

/// What the last refresh acquired.
///
/// Exactly one of the three holds at a time, so the precedence a read needs is
/// structural. A Permanent fault can never be masked by the admitted subset it
/// was found alongside, because there is nowhere to hold both.
#[derive(Clone)]
enum Acquired {
    /// The validated snapshot reads resolve against.
    Sources(Arc<ValidatedPublications>),
    /// A Permanent misconfiguration (see [`Fault`]).
    Fault(Fault),
    /// Nothing to read yet (see [`Absence`]).
    Absent(Absence),
}

/// A Permanent misconfiguration a refresh found. It is **sticky**: it surfaces
/// on every read until a refresh re-validates it away. Only an operator
/// changing the deployment or the routing table can clear one, so re-reading
/// the routing table per read buys nothing.
#[derive(Clone)]
enum Fault {
    /// A source's frozen identity disagrees with the reader's descriptor,
    /// carrying the publishing group.
    IdentityMismatch(Arc<str>),
    /// The collection advertises more sources than the reader admits, carrying
    /// the number advertised.
    TooManySources(usize),
}

impl Fault {
    /// The error every read surfaces while this fault stands.
    fn error(&self) -> StateReaderError {
        match self {
            Self::IdentityMismatch(group) => StateReaderError::IdentityMismatch {
                group: group.clone(),
            },
            Self::TooManySources(found) => StateReaderError::TooManySources {
                found: *found,
                max: MAX_PUBLICATION_SOURCES,
            },
        }
    }
}

/// Why a refresh that reached the store acquired no sources. Both arms are
/// Transient: a later refresh admits a publisher that has only just appeared.
#[derive(Clone, Copy)]
enum Absence {
    /// The routing table holds no row for this collection.
    NoPublication,
    /// Rows exist, but no advertised group has a frozen identity yet.
    NoIdentity,
}

/// The reader's cached view of a collection's publication sources: what the
/// last refresh acquired, when it completed, and when the next attempt is due.
///
/// The default is the never-refreshed state, which is why a fresh reader
/// refreshes on its first read.
#[derive(Default)]
struct SnapshotState {
    /// What the last refresh acquired; `None` until one completes.
    acquired: Option<Acquired>,
    /// When the last refresh completed.
    refreshed_at: Option<Instant>,
    /// When the next attempt is permitted. A failed refresh sets it to
    /// [`REFRESH_BACKOFF`] out and one that found nothing to
    /// [`ABSENT_BACKOFF`]; a refresh that acquires sources or a fault clears
    /// it.
    retry_after: Option<Instant>,
}

/// The collection's published acquisition state.
///
/// One immutable [`SnapshotState`] is published at a time. A read loads it
/// without a lock. A stale read builds a replacement from the exact state it
/// observed and publishes that replacement with a pointer compare-and-swap, so
/// no read waits for another read's refresh.
#[derive(Default)]
pub(crate) struct PublicationSnapshot(ArcSwap<SnapshotState>);

/// The state a read resolves against after one publication attempt.
enum Published {
    /// This caller published its candidate.
    Won(Arc<SnapshotState>),
    /// Another caller published first. Its state is what every read now sees.
    Lost(Arc<SnapshotState>),
}

impl SnapshotState {
    /// The sources the last refresh acquired, if it acquired any.
    fn sources(&self) -> Option<Arc<ValidatedPublications>> {
        match &self.acquired {
            Some(Acquired::Sources(sources)) => Some(sources.clone()),
            _ => None,
        }
    }

    /// Whether the last refresh is recent enough to serve again.
    ///
    /// An absence is deliberately excluded. It must fall through to a re-read
    /// once its own shorter window lapses, so a publisher that appears is
    /// admitted within [`ABSENT_BACKOFF`] instead of a full refresh interval.
    fn is_fresh(&self, now: Instant, interval: Duration) -> bool {
        !matches!(self.acquired, Some(Acquired::Absent(_)))
            && self
                .refreshed_at
                .is_some_and(|at| now.duration_since(at) < interval)
    }

    /// Whether a paced retry is not yet due.
    fn within_pacing(&self, now: Instant) -> bool {
        self.retry_after.is_some_and(|deadline| now < deadline)
    }
}

impl<D, C, B> StateReader<D, C, B>
where
    D: StateDescriptor,
    C: Codec,
    B: ReaderBackend<C>,
{
    /// The refresh-if-stale acquisition, returning the validated snapshot every
    /// read resolves against.
    ///
    /// Refresh follows a three-outcome rule:
    ///
    /// * a **failed** read keeps the previous acquisition and paces the next
    ///   attempt (see [`Self::failed`]). Whatever is held is served, so a
    ///   sticky fault still outranks a snapshot. With nothing held, the read
    ///   that attempted gets the store error, and reads inside the pacing
    ///   window get [`StateReaderError::RefreshUnavailable`].
    /// * a **successful** read applies withdrawals unconditionally: a source no
    ///   longer advertised is dropped without consulting its identity. It
    ///   validates identity only for newly admitted groups, so an
    ///   already-admitted source is never re-validated.
    /// * an **emptied** routing table records the absence and paces on
    ///   [`ABSENT_BACKOFF`], so a publisher that appears is admitted promptly
    ///   without letting a read-heavy caller re-read the routing table on every
    ///   call. A table whose every source lacks a frozen identity is treated
    ///   the same.
    ///
    /// An identity that is present but does not match fails the whole
    /// acquisition with [`StateReaderError::IdentityMismatch`] (Permanent),
    /// held sticky until a refresh re-validates it away (see [`Fault`]). A
    /// missing identity skips that source with a `warn!`.
    ///
    /// A read loads the published state without a lock. A stale read refreshes
    /// and publishes the result with a compare-and-swap. A caller that loses
    /// that race adopts the winner's state, so no caller waits for another
    /// caller's refresh. Every stale caller that starts before a winner
    /// publishes may issue one routing read.
    pub(super) async fn snapshot(&self) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        // The owned handle is held across the refresh, so the observed
        // allocation stays alive and its address cannot be recycled under the
        // pointer compare-and-swap that publishes the replacement.
        let observed = self.publication.0.load_full();
        let now = self.clock.now();
        if observed.within_pacing(now) || observed.is_fresh(now, self.refresh_interval) {
            return self.serve_state(&observed);
        }
        self.refresh(&observed, now).await
    }

    /// The outcome a read resolves to for `acquired`.
    fn serve(&self, acquired: &Acquired) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        match acquired {
            Acquired::Sources(sources) => Ok(sources.clone()),
            Acquired::Fault(fault) => Err(fault.error()),
            Acquired::Absent(absence) => Err(self.absent(*absence)),
        }
    }

    /// The outcome a read resolves to for `state`: what its last refresh
    /// acquired, or [`StateReaderError::RefreshUnavailable`] when no refresh
    /// has completed.
    fn serve_state(
        &self,
        state: &SnapshotState,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        match &state.acquired {
            Some(acquired) => self.serve(acquired),
            None => Err(self.refresh_unavailable()),
        }
    }

    /// Re-reads the routing table from the state this caller observed and
    /// applies the three-outcome rule (see [`Self::snapshot`]).
    async fn refresh(
        &self,
        observed: &Arc<SnapshotState>,
        now: Instant,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let prior = observed.sources();
        let rows = match self
            .context
            .backend
            .publications()
            .read_publications(&self.subsystem, self.context.state_type, &self.context.name)
            .await
            .map_err(|error| StateReaderError::store(&error))
        {
            Ok(rows) => rows,
            // A failed read keeps the previous acquisition untouched, not
            // merely its admitted subset. A transient outage is no evidence a
            // known mismatch was repaired, so a sticky fault survives it.
            Err(error) => return self.failed(observed, "routing", error),
        };

        // The cap bounds the identity fan-out below, so it is checked against
        // the advertised rows here rather than against the admitted output
        // afterwards. Checked late, an oversized routing table would first pay
        // one identity read per advertised group.
        if rows.len() > MAX_PUBLICATION_SOURCES {
            return self.acquire(
                observed,
                Acquired::Fault(Fault::TooManySources(rows.len())),
                &Diagnostics::default(),
                now,
            );
        }

        let Admission {
            admitted,
            diagnostics,
        } = match self.admit(&rows, prior.as_deref()).await {
            Ok(admission) => admission,
            // An identity-read failure mid-admit is a transient outage, on the
            // same footing as a failed routing read.
            Err(error) => return self.failed(observed, "identity", error),
        };

        // Withdrawals took effect on `admitted` regardless of outcome, so the
        // acquisition is published even when it surfaces a fault: a later
        // refresh must see the withdrawal.
        let acquired = match diagnostics.mismatch() {
            Some(group) => Acquired::Fault(Fault::IdentityMismatch(group)),
            None => match ValidatedPublications::new(admitted) {
                Ok(sources) => Acquired::Sources(Arc::new(sources)),
                Err(NoSnapshot::NoSource) if diagnostics.any_missing() => {
                    Acquired::Absent(Absence::NoIdentity)
                }
                Err(NoSnapshot::NoSource) => Acquired::Absent(Absence::NoPublication),
                Err(NoSnapshot::TooManySources { found }) => {
                    Acquired::Fault(Fault::TooManySources(found))
                }
            },
        };
        self.acquire(observed, acquired, &diagnostics, now)
    }

    /// Publishes what a completed refresh acquired and returns the outcome
    /// every read now resolves to.
    ///
    /// An acquisition that found nothing paces the next attempt (see
    /// [`ABSENT_BACKOFF`]); one that found sources or a fault clears the
    /// pacing an earlier failure left behind. The pacing deadline is sampled
    /// here, after the refresh's reads returned, so a slow store cannot consume
    /// the window. `at` stays the pre-refresh observation instant, so freshness
    /// counts from when the caller decided to refresh.
    ///
    /// The winner of the publication emits the refresh's `diagnostics`, so a
    /// burst of speculative refreshers reports each observation once.
    fn acquire(
        &self,
        observed: &Arc<SnapshotState>,
        acquired: Acquired,
        diagnostics: &Diagnostics,
        at: Instant,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let retry_after = matches!(acquired, Acquired::Absent(_))
            .then(|| self.clock.now().checked_add(ABSENT_BACKOFF))
            .flatten();
        let candidate = Arc::new(SnapshotState {
            acquired: Some(acquired),
            refreshed_at: Some(at),
            retry_after,
        });
        match self.publish(observed, candidate) {
            Published::Won(state) => {
                diagnostics.emit(self.context.name.as_str());
                self.serve_state(&state)
            }
            Published::Lost(state) => self.serve_state(&state),
        }
    }

    /// Applies a failed refresh: pace the next attempt, then serve whatever the
    /// held state can still stand behind. A failure with nothing left to serve
    /// propagates `error`.
    ///
    /// Sources and a Permanent fault survive the failure. Reads keep working
    /// off the last snapshot, and an outage is no evidence a fault only an
    /// operator can clear was repaired. An absence does **not** survive: "no
    /// publisher yet" is exactly the claim this read failed to confirm, and
    /// reporting it during an outage sends an operator chasing a missing
    /// publisher instead of a broken store. Dropping it reverts the reader to
    /// never-refreshed, so the pacing window alone governs the next attempt.
    ///
    /// The pacing deadline is sampled here, after the failed read returned, so
    /// a store timeout cannot consume the window. `phase` names which of the
    /// refresh's two reads failed, the routing table or the identity admission.
    /// Reads inside the window get no cause of their own, so this is where an
    /// outage is diagnosed.
    ///
    /// A lost publication means another caller published while this read
    /// failed. That newer outcome is served, and the failure installs no
    /// pacing.
    fn failed(
        &self,
        observed: &Arc<SnapshotState>,
        phase: &'static str,
        error: StateReaderError,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let (acquired, refreshed_at) = match &observed.acquired {
            Some(kept @ (Acquired::Sources(_) | Acquired::Fault(_))) => {
                (Some(kept.clone()), observed.refreshed_at)
            }
            Some(Acquired::Absent(_)) | None => (None, None),
        };
        let candidate = Arc::new(SnapshotState {
            acquired,
            refreshed_at,
            retry_after: self.clock.now().checked_add(REFRESH_BACKOFF),
        });
        match self.publish(observed, candidate) {
            Published::Won(state) => {
                warn!(
                    collection = %self.context.name.as_str(),
                    phase,
                    error = %error,
                    "publication refresh failed; pacing the retry"
                );
                match &state.acquired {
                    Some(acquired) => self.serve(acquired),
                    None => Err(error),
                }
            }
            Published::Lost(state) => self.serve_state(&state),
        }
    }

    /// Publishes `candidate` if `observed` is still the published state.
    ///
    /// Publication is a pointer compare-and-swap from the exact state this
    /// caller read. A caller that loses adopts the winner's state and never
    /// republishes: its refresh observed a generation the winner superseded,
    /// so a retry would roll a newer outcome back. A rule that lets a
    /// successful loser replace a failure published from the same generation
    /// was examined and rejected: it needs a generation marker and a second
    /// compare-and-swap to shorten a window the failure backoff already bounds.
    fn publish(&self, observed: &Arc<SnapshotState>, candidate: Arc<SnapshotState>) -> Published {
        let previous = Guard::into_inner(
            self.publication
                .0
                .compare_and_swap(observed, Arc::clone(&candidate)),
        );
        if Arc::ptr_eq(&previous, observed) {
            Published::Won(candidate)
        } else {
            Published::Lost(previous)
        }
    }

    /// The Transient error a read gets while this collection has nothing to
    /// read.
    fn absent(&self, absence: Absence) -> StateReaderError {
        let name = Arc::from(&self.context.name);
        match absence {
            Absence::NoPublication => StateReaderError::UnknownPublication {
                subsystem: Arc::from(&self.subsystem),
                name,
            },
            Absence::NoIdentity => StateReaderError::IdentityUnavailable { name },
        }
    }

    /// The `RefreshUnavailable` error for this reader's collection.
    fn refresh_unavailable(&self) -> StateReaderError {
        StateReaderError::RefreshUnavailable {
            name: Arc::from(&self.context.name),
        }
    }
}
