//! The public standalone reader.
//!
//! [`StateReader`] is the cross-group read entry point. Given a published
//! collection's descriptor and the subsystem it routes under, it discovers the
//! collection's publication sources and validates each source's frozen identity
//! against the descriptor. Reads observe the **committed** state of at most one
//! source per operation (probe-and-pin; see
//! [`ReadSession`](super::session::ReadSession)).
//!
//! The read methods carry **zero per-descriptor logic**. Each builds a
//! [`ReadSession`], binds the descriptor to it, and delegates to the resulting
//! collection handle. That handle is the same one the owning consumer's
//! handlers use, so owner and reader share one read implementation. A
//! descriptor backed by a Kafka message reference takes the same path because
//! the session's loader is a [`ReaderLoader`].

use crate::Key;
use crate::codec::Codec;
use crate::state::StateName;
use crate::state::access::StateAccessError;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    CellType, ContextOf, DequeDescriptor, DequeHandle, FromSession, MapDescriptor, MapHandle,
    ResolvedOf, StateDescriptor, ValueDescriptor,
};
use crate::state::descriptor_identity::{self, DurableDescriptorIdentity};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state::publication::StatePublication;
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::{ReadSession, ReaderCollectionDef, ReaderContext};
use crate::state_reader::source::{
    MAX_PUBLICATION_SOURCES, NoSnapshot, Source, SourceId, ValidatedPublications,
};
use crate::subsystem::SubsystemName;
use futures::stream::{self, Stream, StreamExt};
use quanta::{Clock, Instant};
use smallvec::SmallVec;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::coop::cooperative;
use tracing::warn;

/// The default snapshot-refresh cadence: a source list changes rarely, so the
/// reader re-reads the routing table at most once per minute per collection.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_mins(1);

/// How long a failed refresh paces the next attempt. Unpaced, every read during
/// a routing-table outage pays its own store round trip before falling back to
/// the held snapshot, so a burst of reads waits one timeout each. Well under
/// [`DEFAULT_REFRESH_INTERVAL`], so a recovered store is picked up promptly.
pub(super) const REFRESH_BACKOFF: Duration = Duration::from_secs(5);

/// How long a refresh that found nothing paces the next attempt.
///
/// A reader deployed ahead of its publisher reads a collection that has no
/// routing row yet, and that is a normal startup state rather than a fault.
/// Unpaced, every such read re-reads the routing table, so a read-heavy caller
/// turns a missing publisher into unbounded store load for as long as it is
/// missing. Much shorter than [`REFRESH_BACKOFF`], since nothing is broken: a
/// publisher that appears is admitted within a second.
pub(super) const ABSENT_BACKOFF: Duration = Duration::from_secs(1);

/// What the last refresh acquired.
///
/// Exactly one of the three holds at a time, so the precedence a read needs is
/// structural. A Permanent fault can never be masked by the admitted subset it
/// was found alongside, because there is nowhere to hold both.
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

/// Publishing group ids gathered during one admission, bounded by the source
/// cap so the common case stays on the stack.
type GroupIds = SmallVec<[Arc<str>; MAX_PUBLICATION_SOURCES]>;

/// The running result of a refresh's identity validation: the admitted
/// sources, the first present-but-unequal group (a hard mismatch), and whether
/// any advertised source lacked a frozen identity.
#[derive(Default)]
struct Admission {
    admitted: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
    mismatch: Option<Arc<str>>,
    any_missing: bool,
}

/// A cross-group, read-only view over a published keyed-state collection.
///
/// Built from a [`SharedDeps`] bundle with [`StateReader::new`]. Reads observe
/// only [`Cell::project_committed`](crate::state::cell::Cell::project_committed)
/// of one source per operation, with honest bounded staleness. Two independent
/// sources bound that staleness. The descriptor's read-cache TTL bounds a
/// cached value's age. The owner's commit-to-apply window bounds the second: a
/// value can be committed before the owner applies it, so a read may return
/// that once-committed value early (see `project_committed` above). The second
/// source converges via the owner's recovery sweep or its next commit, not via
/// the read cache.
///
/// The reader is generic over the collection descriptor `D` and the message
/// codec `C`. The read methods live in descriptor-specialized impl blocks for
/// Value, Map, and Deque. Each is a thin bind-and-delegate over the shared read
/// machinery.
pub struct StateReader<D, C: Codec> {
    descriptor: D,
    subsystem: SubsystemName,
    /// The collection addressed and the handles every session clones.
    context: ReaderContext<C>,
    clock: Clock,
    refresh_interval: Duration,
    snapshot: Mutex<SnapshotState>,
    /// The source bundle's construction id, copied verbatim so a test can prove
    /// two readers descend from the same [`SharedDeps`] construction.
    #[cfg(test)]
    deps_instance_id: u64,
}

impl<D, C> StateReader<D, C>
where
    D: StateDescriptor,
    C: Codec,
{
    /// Builds a reader over the shared `deps` bundle for `descriptor`, routed
    /// under `subsystem`.
    ///
    /// The heavy handles (backend stores, message loader, byte-budgeted cache)
    /// are cloned from `deps`, so composing one bundle and building several
    /// readers shares one session and cache. The effective read-cache TTL and
    /// collection name are validated here.
    ///
    /// # Errors
    ///
    /// Returns [`StateReaderError::InvalidReadCache`] when the effective
    /// read-cache TTL is zero, or [`StateReaderError::Unsupported`] when the
    /// collection name is empty.
    pub fn new(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, DEFAULT_REFRESH_INTERVAL)
    }

    /// [`Self::new`] with an explicit refresh cadence — the tests drive it to
    /// [`Duration::ZERO`] so every operation refreshes the snapshot.
    fn with_refresh_interval(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
        refresh_interval: Duration,
    ) -> Result<Self, StateReaderError> {
        let collection = descriptor.collection_def();
        let read_cache_ttl = collection.read_cache.resolve(deps.default_read_cache_ttl());
        validate_read_cache(read_cache_ttl)?;
        let def = ReaderCollectionDef::new(collection, read_cache_ttl);
        let name =
            StateName::try_new(descriptor.name()).map_err(|_| StateReaderError::Unsupported {
                reason: "collection name is empty",
            })?;
        Ok(Self {
            context: ReaderContext::new(
                deps.stores().clone(),
                deps.loader().clone(),
                deps.cache().clone(),
                def,
                descriptor.state_type(),
                name,
            ),
            descriptor,
            subsystem,
            clock: deps.cache().clock(),
            refresh_interval,
            snapshot: Mutex::new(SnapshotState::default()),
            #[cfg(test)]
            deps_instance_id: deps.instance_id(),
        })
    }

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
    /// One mutex owns both the cached read and the refresh transition, so a
    /// refresh is one `&mut` read-modify-write over the whole state. The guard
    /// is held across [`Self::refresh`]'s network I/O on purpose: it
    /// single-flights the refresh, so one read re-reads the routing table while
    /// the rest wait and wake to the fresh state. Two changes to resist: do not
    /// drop the guard across the await, and do not publish the state through a
    /// lock-free swap so fresh reads skip the guard. A stale read must still
    /// wait, because a withdrawal or a newly present mismatch takes effect at
    /// the refresh that finds it.
    async fn snapshot(&self) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let mut state = self.snapshot.lock().await;
        let now = self.clock.now();
        if state.within_pacing(now) || state.is_fresh(now, self.refresh_interval) {
            return match &state.acquired {
                Some(acquired) => self.serve(acquired),
                // Nothing was ever acquired and a failed refresh is pacing the
                // retry. Re-attempting inside the window is what the pacing
                // exists to prevent.
                None => Err(self.refresh_unavailable()),
            };
        }
        self.refresh(&mut state, now).await
    }

    /// The outcome a read resolves to for `acquired`.
    fn serve(&self, acquired: &Acquired) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        match acquired {
            Acquired::Sources(sources) => Ok(sources.clone()),
            Acquired::Fault(fault) => Err(fault.error()),
            Acquired::Absent(absence) => Err(self.absent(*absence)),
        }
    }

    /// Re-reads the routing table and applies the three-outcome rule, mutating
    /// `state` in place (see [`Self::snapshot`]).
    async fn refresh(
        &self,
        state: &mut SnapshotState,
        now: Instant,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let prior = state.sources();
        let rows = match self
            .context
            .stores
            .read_publications(&self.subsystem, self.context.state_type, &self.context.name)
            .await
        {
            Ok(rows) => rows,
            // A failed read keeps the previous acquisition untouched, not
            // merely its admitted subset. A transient outage is no evidence a
            // known mismatch was repaired, so a sticky fault survives it.
            Err(error) => return self.failed(state, "routing", error),
        };

        // The cap bounds the identity fan-out below, so it is checked against
        // the advertised rows here rather than against the admitted output
        // afterwards. Checked late, an oversized routing table would first pay
        // one identity read per advertised group.
        if rows.len() > MAX_PUBLICATION_SOURCES {
            return self.acquire(
                state,
                Acquired::Fault(Fault::TooManySources(rows.len())),
                now,
            );
        }

        let admission = match self.admit(&rows, prior.as_deref()).await {
            Ok(admission) => admission,
            // An identity-read failure mid-admit is a transient outage, on the
            // same footing as a failed routing read.
            Err(error) => return self.failed(state, "identity", error),
        };

        // Withdrawals took effect on `admitted` regardless of outcome, so the
        // acquisition is published even when it surfaces a fault: a later
        // refresh must see the withdrawal.
        let acquired = match admission.mismatch {
            Some(group) => Acquired::Fault(Fault::IdentityMismatch(group)),
            None => match ValidatedPublications::new(admission.admitted) {
                Ok(sources) => Acquired::Sources(Arc::new(sources)),
                Err(NoSnapshot::NoSource) if admission.any_missing => {
                    Acquired::Absent(Absence::NoIdentity)
                }
                Err(NoSnapshot::NoSource) => Acquired::Absent(Absence::NoPublication),
                Err(NoSnapshot::TooManySources { found }) => {
                    Acquired::Fault(Fault::TooManySources(found))
                }
            },
        };
        self.acquire(state, acquired, now)
    }

    /// Publishes what a completed refresh acquired and returns the outcome
    /// every read now resolves to.
    ///
    /// An acquisition that found nothing paces the next attempt (see
    /// [`ABSENT_BACKOFF`]); one that found sources or a fault clears the
    /// pacing an earlier failure left behind. The pacing deadline is sampled
    /// here, after the refresh's reads returned, so a slow store cannot consume
    /// the window.
    fn acquire(
        &self,
        state: &mut SnapshotState,
        acquired: Acquired,
        at: Instant,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let outcome = self.serve(&acquired);
        let retry_after = matches!(acquired, Acquired::Absent(_))
            .then(|| self.clock.now().checked_add(ABSENT_BACKOFF))
            .flatten();
        *state = SnapshotState {
            acquired: Some(acquired),
            refreshed_at: Some(at),
            retry_after,
        };
        outcome
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
    fn failed(
        &self,
        state: &mut SnapshotState,
        phase: &'static str,
        error: StateReaderError,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        warn!(
            collection = %self.context.name.as_str(),
            phase,
            error = %error,
            "publication refresh failed; pacing the retry"
        );
        if matches!(state.acquired, Some(Acquired::Absent(_))) {
            *state = SnapshotState::default();
        }
        state.retry_after = self.clock.now().checked_add(REFRESH_BACKOFF);
        match &state.acquired {
            Some(acquired) => self.serve(acquired),
            None => Err(error),
        }
    }

    /// Validates each advertised source's frozen identity, admitting the ones
    /// whose group is already in `prior` without a fresh read.
    ///
    /// `rows` is capped at [`MAX_PUBLICATION_SOURCES`] by the caller, so both
    /// group lists and the fan-out below are bounded by that ceiling and stay
    /// on the stack.
    async fn admit(
        &self,
        rows: &[StatePublication],
        prior: Option<&ValidatedPublications>,
    ) -> Result<Admission, StateReaderError> {
        let prior_groups: GroupIds = prior
            .map(|snapshot| {
                snapshot
                    .sources()
                    .iter()
                    .map(|source| source.id.group_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let asserted = DurableDescriptorIdentity::from_identity(
            self.context.state_type,
            self.context.name.as_str(),
            &self.descriptor.structural_identity(),
        );

        // Distinct group ids in advertisement order (few sources; a linear
        // scan is cheaper than a hasher).
        let mut groups = GroupIds::new();
        for row in rows {
            if !groups.iter().any(|g| **g == *row.group_id) {
                groups.push(row.group_id.clone());
            }
        }

        // Fan the identity reads for the newly-advertised groups (those absent
        // from `prior`) out concurrently: a cold snapshot after a rebalance must
        // not pay one serial round trip per source. `buffered` is
        // order-preserving, so the read results stay aligned to the
        // newly-advertised groups in advertisement order and the fold below
        // stays deterministic.
        let mut reads = stream::iter(
            groups
                .iter()
                .filter(|group| !prior_groups.iter().any(|prior| prior == *group))
                .cloned(),
        )
        .map(|group| {
            cooperative(async move {
                self.context
                    .stores
                    .read_identity(&group, self.context.state_type, self.context.name.as_str())
                    .await
            })
        })
        .buffered(MAX_PUBLICATION_SOURCES)
        .collect::<SmallVec<[_; MAX_PUBLICATION_SOURCES]>>()
        .await
        .into_iter();

        // Fold sequentially in advertisement order: a prior group is admitted
        // with no read; a new group consumes its order-aligned identity read.
        let mut admission = Admission::default();
        for group in &groups {
            let admitted = if prior_groups.iter().any(|prior| prior == group) {
                true
            } else {
                match reads.next() {
                    Some(stored) => {
                        self.classify_identity(stored?, group, &asserted, &mut admission)
                    }
                    // Unreachable: every new group produced exactly one read.
                    None => false,
                }
            };
            if admitted {
                for row in rows.iter().filter(|row| *row.group_id == **group) {
                    admission.admitted.push(Source {
                        id: SourceId {
                            group_id: row.group_id.clone(),
                            topic: row.topic,
                        },
                        partition_count: row.partition_count,
                    });
                }
            }
        }
        Ok(admission)
    }

    /// Classifies a new group's already-read frozen identity, recording a
    /// mismatch or missing identity in `admission`; returns whether the group
    /// is admitted. This function does no I/O. The identity read runs
    /// concurrently in [`Self::admit`].
    fn classify_identity(
        &self,
        stored: Option<DurableDescriptorIdentity>,
        group: &Arc<str>,
        asserted: &DurableDescriptorIdentity,
        admission: &mut Admission,
    ) -> bool {
        let Some(stored) = stored else {
            warn!(group = %group, name = %self.context.name.as_str(), "publication source has no frozen identity yet");
            admission.any_missing = true;
            return false;
        };
        if descriptor_identity::validate::<StateAccessError>(stored, asserted).is_ok() {
            return true;
        }
        if admission.mismatch.is_none() {
            admission.mismatch = Some(Arc::clone(group));
        }
        false
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

    /// Builds a per-operation [`ReadSession`] over the current snapshot, with a
    /// fresh source pin. Rejects an empty key first: an empty or NULL key has
    /// no deterministic partition to route to.
    async fn session(&self, key: Key) -> Result<ReadSession<C>, StateReaderError> {
        if key.is_empty() {
            return Err(StateReaderError::EmptyKey);
        }
        let snapshot = self.snapshot().await?;
        Ok(ReadSession::new(self.context.clone(), snapshot, key))
    }
}

/// Rejects a degenerate read-cache TTL. A zero TTL would make every entry born
/// stale, so it fails `Permanent` at construction. Sub-millisecond TTLs are
/// supported: age is measured against a nanosecond-resolution monotonic clock.
fn validate_read_cache(ttl: Option<Duration>) -> Result<(), StateReaderError> {
    if let Some(ttl) = ttl
        && ttl.is_zero()
    {
        return Err(StateReaderError::InvalidReadCache {
            reason: "cache ttl is zero",
        });
    }
    Ok(())
}

// --- Value (and Kafka-message-ref) reads -----------------------------------

impl<T, C> StateReader<ValueDescriptor<T>, C>
where
    C: Codec,
    C::Payload: Clone,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, ReadSession<C>>,
{
    /// Reads and resolves the committed value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]: acquisition/identity failures, an empty key,
    /// or a store/decode failure from the bound handle.
    pub async fn get<K: Into<Key>>(
        &self,
        key: K,
    ) -> Result<Option<ResolvedOf<T>>, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle = self.descriptor.bind(&session)?;
        handle.get().await.map_err(|e| StateReaderError::store(&e))
    }
}

// --- Map reads --------------------------------------------------------------

impl<KC, V, C> StateReader<MapDescriptor<KC, V>, C>
where
    C: Codec,
    C::Payload: Clone,
    KC: OrderedKeyCodec + 'static,
    KC::Key: Display,
    V: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, V>: FromSession<'s, ReadSession<C>>,
{
    /// Reads and resolves the committed value for map entry `map_key` under
    /// partition `key`.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn get<K: Into<Key>>(
        &self,
        key: K,
        map_key: &KC::Key,
    ) -> Result<Option<ResolvedOf<V>>, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: MapHandle<_, KC, V> = self.descriptor.bind(&session)?;
        handle
            .get(map_key)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reads the committed values for `map_keys` as one isolated batch,
    /// index-aligned to the input.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn get_many<K: Into<Key>>(
        &self,
        key: K,
        map_keys: &[KC::Key],
    ) -> Result<Vec<Option<ResolvedOf<V>>>, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: MapHandle<_, KC, V> = self.descriptor.bind(&session)?;
        handle
            .get_many(map_keys)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Streams the committed live entries of the map under partition `key` in
    /// key order (ascending for [`Direction::Forward`]).
    ///
    /// The session is acquired up front and moved into the stream, so the
    /// returned stream is self-contained (owns its handles) and a binding can
    /// hold it beyond the reader's borrow.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`] from acquiring the session (empty key,
    /// acquisition/identity failures); per-source read failures surface as
    /// stream items.
    pub async fn stream<K: Into<Key>>(
        &self,
        key: K,
        dir: Direction,
    ) -> Result<
        impl Stream<Item = Result<(KC::Key, ResolvedOf<V>), StateReaderError>> + 'static,
        StateReaderError,
    >
    where
        V: 'static,
        ResolvedOf<V>: 'static,
    {
        let session = self.session(key.into()).await?;
        let handle: MapHandle<_, KC, V> = self.descriptor.bind(&session)?;
        Ok(async_stream::try_stream! {
            let inner = handle.stream(dir);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateReaderError::store(&e))?;
            }
        })
    }
}

// --- Deque reads ------------------------------------------------------------

impl<T, C> StateReader<DequeDescriptor<T>, C>
where
    C: Codec,
    C::Payload: Clone,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, ReadSession<C>>,
{
    /// Reads and resolves the committed element at front-relative `index`
    /// (`None` when `index >= len`).
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn get<K: Into<Key>>(
        &self,
        key: K,
        index: usize,
    ) -> Result<Option<ResolvedOf<T>>, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: DequeHandle<_, T> = self.descriptor.bind(&session)?;
        handle
            .get(index)
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// The number of committed live elements under partition `key`.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn len<K: Into<Key>>(&self, key: K) -> Result<usize, StateReaderError> {
        let session = self.session(key.into()).await?;
        let handle: DequeHandle<_, T> = self.descriptor.bind(&session)?;
        handle.len().await.map_err(|e| StateReaderError::store(&e))
    }

    /// Streams the committed live elements under partition `key` in index order
    /// (front to back for [`Direction::Forward`]).
    ///
    /// The session is acquired up front and moved into the stream, so the
    /// returned stream is self-contained (owns its handles) and a binding can
    /// hold it beyond the reader's borrow.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`] from acquiring the session (empty key,
    /// acquisition/identity failures); per-source read failures surface as
    /// stream items.
    pub async fn stream<K: Into<Key>>(
        &self,
        key: K,
        dir: Direction,
    ) -> Result<
        impl Stream<Item = Result<ResolvedOf<T>, StateReaderError>> + 'static,
        StateReaderError,
    >
    where
        T: 'static,
        ResolvedOf<T>: 'static,
    {
        let session = self.session(key.into()).await?;
        let handle: DequeHandle<_, T> = self.descriptor.bind(&session)?;
        Ok(async_stream::try_stream! {
            let inner = handle.stream(dir);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item.map_err(|e| StateReaderError::store(&e))?;
            }
        })
    }
}

#[cfg(test)]
impl<D, C> StateReader<D, C>
where
    D: StateDescriptor,
    C: Codec,
{
    /// A reader that refreshes its snapshot on every operation — the
    /// deterministic driver for the acquisition/refresh property tests.
    pub(crate) fn new_eager(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, Duration::ZERO)
    }

    /// A reader with an explicit non-zero refresh interval — the driver for the
    /// sticky-mismatch test, which needs the cached-snapshot fast path (the age
    /// since the last refresh still inside the interval) to fire on the second
    /// operation.
    pub(crate) fn new_with_interval(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
        refresh_interval: Duration,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, refresh_interval)
    }

    /// The source bundle's construction id (see [`SharedDeps::instance_id`]).
    pub(crate) fn deps_instance_id(&self) -> u64 {
        self.deps_instance_id
    }
}
