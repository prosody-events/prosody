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
use crate::state_reader::cache::ReaderCache;
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::session::{ReadSession, ReaderCollectionDef};
use crate::state_reader::source::{
    MAX_PUBLICATION_SOURCES, NoSnapshot, Source, SourceId, ValidatedPublications,
};
use crate::state_reader::stores::ReaderStores;
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

/// The reader's cached view of a collection's publication sources: what the
/// last refresh found, when it completed, and the retry pacing a failed one
/// left behind.
///
/// The snapshot is an `Option`. An **absent** snapshot is stored as `None`,
/// never as an empty [`ValidatedPublications`]. Absence means no admitted
/// source, or an emptied routing table. This keeps the non-empty invariant on
/// the validated snapshot structural.
///
/// The default is the never-refreshed state, which is why a fresh reader
/// refreshes on its first read.
#[derive(Default)]
struct SnapshotState {
    snapshot: Option<Arc<ValidatedPublications>>,
    /// The Permanent misconfiguration the last refresh found, if any (see
    /// [`Fault`]).
    fault: Option<Fault>,
    /// When the last refresh completed.
    refreshed_at: Option<Instant>,
    /// When a failed refresh permits the next attempt (see
    /// [`REFRESH_BACKOFF`]). `None` whenever the last refresh completed.
    retry_after: Option<Instant>,
}

impl SnapshotState {
    /// The state a completed refresh publishes. Publishing the whole state is
    /// what lifts the pacing an earlier failure left behind.
    fn refreshed(
        snapshot: Option<Arc<ValidatedPublications>>,
        fault: Option<Fault>,
        at: Instant,
    ) -> Self {
        Self {
            snapshot,
            fault,
            refreshed_at: Some(at),
            retry_after: None,
        }
    }

    /// The outcome a read resolves without touching the store, or `None` when
    /// this state holds nothing to serve.
    ///
    /// A fault outranks a held snapshot: a Permanent misconfiguration must
    /// never be masked by the admitted subset it was found alongside. This
    /// is the only place that precedence lives.
    fn cached_outcome(&self) -> Option<Result<Arc<ValidatedPublications>, StateReaderError>> {
        if let Some(fault) = &self.fault {
            return Some(Err(fault.error()));
        }
        self.snapshot.clone().map(Ok)
    }
}

/// A Permanent misconfiguration a refresh found. It is **sticky**: it surfaces
/// on every read until a refresh re-validates, never masked by an admitted
/// subset found alongside it. Only an operator changing the deployment or the
/// routing table can clear one, so re-reading the routing table per read buys
/// nothing.
///
/// A Transient absence is not a fault. An emptied or identity-less routing
/// table is stored as no snapshot and no fault, which re-reads eagerly so a
/// later read can re-admit.
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
    name: StateName,
    def: ReaderCollectionDef,
    stores: ReaderStores,
    loader: Arc<ReaderLoader<C>>,
    cache: ReaderCache,
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
            descriptor,
            subsystem,
            name,
            def,
            stores: deps.stores().clone(),
            loader: deps.loader().clone(),
            cache: deps.cache().clone(),
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
    /// * a **failed** read keeps the previous acquisition outcome and paces the
    ///   next attempt (see [`Self::failed`]). A held snapshot is returned, and
    ///   a known sticky mismatch outranks it. With nothing held, the read that
    ///   attempted gets the store error, and reads inside the pacing window get
    ///   [`StateReaderError::RefreshUnavailable`].
    /// * a **successful** read applies withdrawals unconditionally: a source no
    ///   longer advertised is dropped without consulting its identity. It
    ///   validates identity only for newly admitted groups, so an
    ///   already-admitted source is never re-validated.
    /// * an **emptied** routing table stores the absence and fails, so a later
    ///   read can re-admit. A table whose every source lacks a frozen identity
    ///   is treated the same.
    ///
    /// An identity that is present but does not match fails the whole
    /// acquisition with [`StateReaderError::IdentityMismatch`] (Permanent). It
    /// is held sticky so it surfaces on every read until a successful refresh
    /// clears it (see [`SnapshotState::mismatch`]). A missing identity skips
    /// that source with a `warn!`.
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
        let fresh = state
            .refreshed_at
            .is_some_and(|at| now.duration_since(at) < self.refresh_interval);
        let paced = state.retry_after.is_some_and(|deadline| now < deadline);
        if fresh || paced {
            if let Some(outcome) = state.cached_outcome() {
                return outcome;
            }
            // Nothing to serve. A paced read must not re-attempt; a merely fresh
            // one falls through, so an emptied routing table re-admits eagerly.
            if paced {
                return Err(self.refresh_unavailable());
            }
        }
        self.refresh(&mut state, now).await
    }

    /// Re-reads the routing table and applies the three-outcome rule, mutating
    /// `state` in place (see [`Self::snapshot`]).
    async fn refresh(
        &self,
        state: &mut SnapshotState,
        now: Instant,
    ) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let prior = state.snapshot.clone();
        let rows = match self
            .stores
            .read_publications(&self.subsystem, self.descriptor.state_type(), &self.name)
            .await
        {
            Ok(rows) => rows,
            // A failed read keeps the previous acquisition outcome untouched,
            // not merely its admitted subset. A known Permanent mismatch
            // outranks that subset. A transient outage is no evidence the
            // mismatch was repaired, so the mismatch stays sticky through the
            // outage.
            Err(error) => return self.failed(state, "routing", error),
        };

        if rows.is_empty() {
            *state = SnapshotState::refreshed(None, None, now);
            return Err(self.unknown_publication());
        }

        let admission = match self.admit(&rows, prior.as_deref()).await {
            Ok(admission) => admission,
            // An identity-read failure mid-admit is a transient outage, on the
            // same footing as a failed routing read.
            Err(error) => return self.failed(state, "identity", error),
        };

        // Withdrawals took effect on `admitted` regardless of outcome. Publish
        // it (or its absence) before surfacing a fault, so a later refresh sees
        // the withdrawal.
        let mismatch = admission.mismatch.map(Fault::IdentityMismatch);
        let (snapshot, fault) = match ValidatedPublications::new(admission.admitted) {
            Ok(sources) => (Some(Arc::new(sources)), mismatch),
            // No admitted source: a Transient absence, so no fault is recorded
            // and a later read re-admits eagerly.
            Err(NoSnapshot::NoSource) => (None, mismatch),
            // An oversized routing table is Permanent, on the same footing as a
            // mismatched identity, so it is recorded sticky (see [`Fault`]). A
            // mismatch found alongside it outranks it; both are Permanent, so
            // either is a correct answer.
            Err(NoSnapshot::TooManySources { found }) => {
                (None, mismatch.or(Some(Fault::TooManySources(found))))
            }
        };
        *state = SnapshotState::refreshed(snapshot, fault, now);

        if let Some(outcome) = state.cached_outcome() {
            return outcome;
        }
        if admission.any_missing {
            return Err(StateReaderError::IdentityUnavailable {
                name: self.name.as_str().to_owned(),
            });
        }
        Err(self.unknown_publication())
    }

    /// Applies a failed refresh: pace the next attempt, then serve whatever the
    /// held state can (see [`SnapshotState::cached_outcome`]). Only a failure
    /// with nothing held propagates `error`.
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
            collection = %self.name.as_str(),
            phase,
            error = %error,
            "publication refresh failed; pacing the retry"
        );
        state.retry_after = self.clock.now().checked_add(REFRESH_BACKOFF);
        match state.cached_outcome() {
            Some(outcome) => outcome,
            None => Err(error),
        }
    }

    /// Validates each advertised source's frozen identity, admitting the ones
    /// whose group is already in `prior` without a fresh read.
    async fn admit(
        &self,
        rows: &[StatePublication],
        prior: Option<&ValidatedPublications>,
    ) -> Result<Admission, StateReaderError> {
        let prior_groups: Vec<Arc<str>> = prior
            .map(|snapshot| {
                snapshot
                    .sources()
                    .iter()
                    .map(|source| source.id.group_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let asserted = DurableDescriptorIdentity::from_identity(
            self.descriptor.state_type(),
            self.name.as_str(),
            &self.descriptor.structural_identity(),
        );

        // Distinct group ids in advertisement order (few sources; a linear
        // scan is cheaper than a hasher).
        let mut groups: Vec<Arc<str>> = Vec::new();
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
                self.stores
                    .read_identity(&group, self.descriptor.state_type(), self.name.as_str())
                    .await
            })
        })
        .buffered(MAX_PUBLICATION_SOURCES)
        .collect::<Vec<_>>()
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
            warn!(group = %group, name = %self.name.as_str(), "publication source has no frozen identity yet");
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

    /// The `UnknownPublication` error for this reader's collection.
    fn unknown_publication(&self) -> StateReaderError {
        StateReaderError::UnknownPublication {
            subsystem: self.subsystem.as_str().to_owned(),
            name: self.name.as_str().to_owned(),
        }
    }

    /// The `RefreshUnavailable` error for this reader's collection.
    fn refresh_unavailable(&self) -> StateReaderError {
        StateReaderError::RefreshUnavailable {
            name: Arc::from(&self.name),
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
        Ok(ReadSession::new(
            snapshot,
            self.stores.clone(),
            self.loader.clone(),
            self.cache.clone(),
            key,
            self.def,
            self.descriptor.state_type(),
            self.name.clone(),
        ))
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
