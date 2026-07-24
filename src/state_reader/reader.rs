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
use crate::state_reader::cache::{ReaderCache, ReaderClock};
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::loader::ReaderLoader;
use crate::state_reader::session::{ReadSession, ReaderCollectionDef};
use crate::state_reader::source::{
    MAX_PUBLICATION_SOURCES, Source, SourceId, ValidatedPublications,
};
use crate::state_reader::stores::ReaderStores;
use crate::subsystem::SubsystemName;
use futures::stream::{self, Stream, StreamExt};
use smallvec::SmallVec;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::coop::cooperative;
use tracing::warn;

/// The default snapshot-refresh cadence: a source list changes rarely, so the
/// reader re-reads the routing table at most once per minute per collection.
const DEFAULT_REFRESH_INTERVAL_MS: u64 = 60_000;

/// The reader's cached view of a collection's publication sources plus the
/// wall-clock instant the view was last refreshed.
///
/// The snapshot is an `Option`. An **absent** snapshot is stored as `None`,
/// never as an empty [`ValidatedPublications`]. Absence means no admitted
/// source, or an emptied routing table. This keeps the non-empty invariant on
/// the validated snapshot structural.
struct SnapshotState {
    snapshot: Option<Arc<ValidatedPublications>>,
    /// An identity that was present but did not match, observed at the last
    /// refresh and held until the next refresh re-reads. `IdentityMismatch` is
    /// a Permanent misconfiguration. It must surface on **every** read within
    /// the refresh interval, never masked by the valid subset served from the
    /// cached snapshot. A refresh that re-reads and re-validates clears it
    /// automatically.
    mismatch: Option<String>,
    refreshed_at_ms: u64,
}

/// The running result of a refresh's identity validation: the admitted
/// sources, the first present-but-unequal group (a hard mismatch), and whether
/// any advertised source lacked a frozen identity.
#[derive(Default)]
struct Admission {
    admitted: SmallVec<[Source; MAX_PUBLICATION_SOURCES]>,
    mismatch: Option<String>,
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
    clock: ReaderClock,
    refresh_interval_ms: u64,
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
    /// read-cache TTL is degenerate (zero or sub-millisecond), or
    /// [`StateReaderError::Unsupported`] when the collection name is empty.
    pub fn new(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, DEFAULT_REFRESH_INTERVAL_MS)
    }

    /// [`Self::new`] with an explicit refresh cadence — the tests drive it to
    /// `0` so every operation refreshes the snapshot.
    fn with_refresh_interval(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
        refresh_interval_ms: u64,
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
            refresh_interval_ms,
            snapshot: Mutex::new(SnapshotState {
                snapshot: None,
                mismatch: None,
                refreshed_at_ms: 0,
            }),
            #[cfg(test)]
            deps_instance_id: deps.instance_id(),
        })
    }

    /// The refresh-if-stale acquisition, returning the validated snapshot every
    /// read resolves against.
    ///
    /// Refresh follows a three-outcome rule:
    ///
    /// * a **failed** routing-table read keeps the previous acquisition
    ///   outcome. A held snapshot is returned. A known sticky mismatch still
    ///   outranks that snapshot (see below); otherwise the read error
    ///   propagates.
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
    /// The `snapshot` mutex is held across [`Self::refresh`]'s network I/O on
    /// purpose: it single-flights the refresh so only one read re-reads the
    /// routing table while the rest wait and wake to the fresh snapshot. Do not
    /// "fix" this by dropping the guard across the await — that would let a
    /// thundering herd of reads all refresh at once.
    async fn snapshot(&self) -> Result<Arc<ValidatedPublications>, StateReaderError> {
        let mut state = self.snapshot.lock().await;
        let now = self.clock.now_ms();
        if now.saturating_sub(state.refreshed_at_ms) < self.refresh_interval_ms {
            // A sticky mismatch outranks the cached snapshot: a Permanent
            // misconfiguration surfaces on every read, never masked by the
            // admitted subset. An absent snapshot with no mismatch falls
            // through to re-read (a withdrawn table re-admits eagerly).
            if let Some(group) = &state.mismatch {
                return Err(StateReaderError::IdentityMismatch {
                    group: group.clone(),
                });
            }
            if let Some(snapshot) = &state.snapshot {
                return Ok(snapshot.clone());
            }
        }
        self.refresh(&mut state, now).await
    }

    /// Re-reads the routing table and applies the three-outcome rule, mutating
    /// `state` in place (see [`Self::snapshot`]).
    async fn refresh(
        &self,
        state: &mut SnapshotState,
        now: u64,
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
            Err(error) => return held_or_error(state.mismatch.as_deref(), prior, error),
        };

        if rows.is_empty() {
            state.snapshot = None;
            state.mismatch = None;
            state.refreshed_at_ms = now;
            return Err(self.unknown_publication());
        }

        let admission = match self.admit(&rows, prior.as_deref()).await {
            Ok(admission) => admission,
            // An identity-read failure mid-admit is a transient outage, on the
            // same footing as a failed routing read. It is no evidence a known
            // sticky mismatch was repaired, so it must not mask that mismatch,
            // nor demote a held snapshot to a bare error.
            Err(error) => return held_or_error(state.mismatch.as_deref(), prior, error),
        };

        // Withdrawals took effect on `admitted` regardless of outcome. Store it
        // (or its absence) before surfacing a mismatch, so a later refresh sees
        // the withdrawal. Record the mismatch as sticky (see
        // [`SnapshotState::mismatch`]).
        state.snapshot = if admission.admitted.is_empty() {
            None
        } else {
            Some(Arc::new(ValidatedPublications::new(
                admission.admitted,
                self.subsystem.as_str(),
                self.name.as_str(),
            )?))
        };
        state.mismatch.clone_from(&admission.mismatch);
        state.refreshed_at_ms = now;

        if let Some(group) = admission.mismatch {
            return Err(StateReaderError::IdentityMismatch { group });
        }
        match &state.snapshot {
            Some(snapshot) => Ok(snapshot.clone()),
            None if admission.any_missing => Err(StateReaderError::IdentityUnavailable {
                name: self.name.as_str().to_owned(),
            }),
            None => Err(self.unknown_publication()),
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
            admission.mismatch = Some(group.as_ref().to_owned());
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

/// Rejects a degenerate read-cache TTL. A cache policy whose TTL truncates
/// to zero milliseconds would make every entry born stale, so it fails
/// `Permanent` at construction.
fn validate_read_cache(ttl: Option<Duration>) -> Result<(), StateReaderError> {
    if let Some(ttl) = ttl
        && ttl.as_millis() == 0
    {
        return Err(StateReaderError::InvalidReadCache {
            reason: "cache ttl truncates to zero milliseconds",
        });
    }
    Ok(())
}

/// The held-snapshot fallback shared by the two refresh failure paths: a
/// failed routing-table read and a failed identity admission. A known sticky
/// mismatch outranks everything, since a transient outage is no evidence it was
/// repaired. Otherwise a held snapshot beats a bare error. Only a first-ever
/// failure with nothing held propagates the error.
fn held_or_error(
    mismatch: Option<&str>,
    prior: Option<Arc<ValidatedPublications>>,
    error: StateReaderError,
) -> Result<Arc<ValidatedPublications>, StateReaderError> {
    match (mismatch, prior) {
        (Some(group), _) => Err(StateReaderError::IdentityMismatch {
            group: group.to_owned(),
        }),
        (None, Some(snapshot)) => Ok(snapshot),
        (None, None) => Err(error),
    }
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
        Self::with_refresh_interval(deps, subsystem, descriptor, 0)
    }

    /// A reader with an explicit non-zero refresh interval — the driver for the
    /// sticky-mismatch test, which needs the cached-snapshot fast path
    /// (`now - refreshed_at_ms < interval`) to fire on the second operation.
    pub(crate) fn new_with_interval(
        deps: &SharedDeps<C>,
        subsystem: SubsystemName,
        descriptor: D,
        refresh_interval_ms: u64,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, refresh_interval_ms)
    }

    /// The source bundle's construction id (see [`SharedDeps::instance_id`]).
    pub(crate) fn deps_instance_id(&self) -> u64 {
        self.deps_instance_id
    }
}
