//! The public standalone reader.
//!
//! [`StateReader`] is the cross-group read entry point. Given a published
//! collection's descriptor and the subsystem it routes under, it discovers the
//! collection's publication sources and validates each source's frozen identity
//! against the descriptor. Reads observe the **committed** state of at most one
//! source per operation (probe-and-pin; see
//! [`ReadSession`](super::session::ReadSession)).
//!
//! Point reads acquire a session and bind a collection handle.
//! Queries bind the collection directly and use the shared query executor.
//! Owner and reader sessions use the same collection methods.
//! Message reference cells use the loader from the session's backend.
//!
//! [`acquisition`] owns source discovery, snapshot refresh, and retries.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "the acquisition state machine is its own impl beside the state it owns"
)]

pub(crate) mod acquisition;
mod admission;
mod deque;
mod map;
mod query;
mod set;
pub use deque::DequeReaderQuery;

pub use query::{MapReaderQuery, SetReaderQuery};

use crate::Key;
use crate::codec::Codec;
use crate::state::StateName;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    CellType, ContextOf, DequeDescriptor, FromSession, ResolvedOf, StateDescriptor, ValueDescriptor,
};
use crate::state::order_codec::UnitKey;
use crate::state_reader::deps::StateReaderDependencies;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::{ReadSession, ReaderCollectionDef, ReaderContext};
use crate::state_reader::{MemoryReaderBackend, ReaderBackend};
use crate::subsystem::SubsystemName;
use acquisition::{DEFAULT_REFRESH_INTERVAL, PublicationSnapshot};
use futures::stream::Stream;
use quanta::Clock;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

/// A cross-group, read-only view over a published keyed-state collection.
///
/// Built from a [`StateReaderDependencies`] bundle with [`StateReader::new`].
/// Each operation reads one source. Positive collection evidence makes a
/// committed provisional value visible before the owner applies it.
/// Committed clears restrict scans to their frozen survivors.
/// The read-cache TTL bounds cached value age. Store reads use evidence without
/// an owner admission.
///
/// The reader is generic over the collection descriptor `D` and the message
/// codec `C`. The read methods live in descriptor-specialized impl blocks for
/// Value, Map, Set, and Deque. Each is a thin bind-and-delegate over the shared
/// read machinery.
pub struct StateReader<D, C: Codec, B = MemoryReaderBackend<C>> {
    descriptor: D,
    subsystem: SubsystemName,
    /// The collection addressed and the handles every session clones.
    context: ReaderContext<C, B>,
    clock: Clock,
    refresh_interval: Duration,
    publication: Arc<PublicationSnapshot>,
    /// The source bundle's construction id, copied verbatim so a test can prove
    /// two readers descend from the same [`StateReaderDependencies`]
    /// construction.
    #[cfg(test)]
    deps_instance_id: u64,
}

impl<D, C, B> StateReader<D, C, B>
where
    D: StateDescriptor,
    C: Codec,
    B: ReaderBackend<C>,
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
        deps: &StateReaderDependencies<C, B>,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, DEFAULT_REFRESH_INTERVAL)
    }

    /// [`Self::new`] with an explicit refresh cadence — the tests drive it to
    /// [`Duration::ZERO`] so every operation refreshes the snapshot.
    pub(super) fn with_refresh_interval(
        deps: &StateReaderDependencies<C, B>,
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
        let publication = deps.publications().snapshot(
            &subsystem,
            descriptor.state_type(),
            &name,
            &descriptor.structural_identity(),
            refresh_interval,
        );
        Ok(Self {
            context: ReaderContext::new(
                deps.backend().clone(),
                deps.cache().clone(),
                def,
                descriptor.state_type(),
                name,
            ),
            descriptor,
            subsystem,
            clock: deps.cache().clock(),
            refresh_interval,
            publication,
            #[cfg(test)]
            deps_instance_id: deps.instance_id(),
        })
    }

    async fn bound(&self, key: Key) -> Result<D::Handle<ReadSession<C, B>>, StateReaderError> {
        let session = self.session(key).await?;
        Ok(self.descriptor.bind(&session)?)
    }

    /// Builds a per-operation [`ReadSession`] over the current snapshot, with a
    /// fresh source pin. Rejects an empty key first: an empty or NULL key has
    /// no deterministic partition to route to.
    pub(crate) async fn session(&self, key: Key) -> Result<ReadSession<C, B>, StateReaderError> {
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

impl<T, C, B> StateReader<ValueDescriptor<T>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, ReadSession<C, B>>,
{
    /// Reads and resolves the committed value for `key` (`None` when absent).
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]: an acquisition or identity failure, an empty
    /// key, or a store or decode failure from the bound handle.
    pub async fn get<K: Into<Key>>(
        &self,
        key: K,
    ) -> Result<Option<ResolvedOf<T>>, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle.get().await.map_err(|e| StateReaderError::store(&e))
    }
}

impl<T, C, B> StateReader<DequeDescriptor<T>, C, B>
where
    C: Codec,
    B: ReaderBackend<C>,
    C::Payload: Clone,
    T: CellType<Key = UnitKey>,
    for<'s> ContextOf<'s, T>: FromSession<'s, ReadSession<C, B>>,
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
        let handle = self.bound(key.into()).await?;
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
        let handle = self.bound(key.into()).await?;
        handle.len().await.map_err(|e| StateReaderError::store(&e))
    }

    /// Reports whether the committed deque is empty.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn is_empty<K: Into<Key>>(&self, key: K) -> Result<bool, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .is_empty()
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reads the committed front endpoint without a length read.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn peek_front<K: Into<Key>>(
        &self,
        key: K,
    ) -> Result<Option<ResolvedOf<T>>, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .peek_front()
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Reads the committed back endpoint without a length read.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`]; see [`StateReader::get`](StateReader::get).
    pub async fn peek_back<K: Into<Key>>(
        &self,
        key: K,
    ) -> Result<Option<ResolvedOf<T>>, StateReaderError> {
        let handle = self.bound(key.into()).await?;
        handle
            .peek_back()
            .await
            .map_err(|e| StateReaderError::store(&e))
    }

    /// Streams the committed live elements under partition `key` in index order
    /// (front to back for [`Direction::Forward`]).
    ///
    /// The stream owns its session and can outlive the reader's borrow.
    ///
    /// # Errors
    ///
    /// Any [`StateReaderError`] from acquiring the session: an empty key, or
    /// an acquisition or identity failure. Per-source read failures surface
    /// as stream items.
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
        self.query(key, dir).values().await
    }

    /// Builds a directional deque query for the partition key.
    pub fn query<K: Into<Key>>(&self, key: K, dir: Direction) -> DequeReaderQuery<'_, T, C, B> {
        DequeReaderQuery {
            reader: self,
            key: key.into(),
            dir,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            limit: None,
        }
    }
}

#[cfg(test)]
impl<D, C, B> StateReader<D, C, B>
where
    D: StateDescriptor,
    C: Codec,
    B: ReaderBackend<C>,
{
    /// A reader that refreshes its snapshot on every operation — the
    /// deterministic driver for the acquisition/refresh property tests.
    pub(crate) fn new_eager(
        deps: &StateReaderDependencies<C, B>,
        subsystem: SubsystemName,
        descriptor: D,
    ) -> Result<Self, StateReaderError> {
        Self::with_refresh_interval(deps, subsystem, descriptor, Duration::ZERO)
    }

    /// The source bundle's construction id (see
    /// [`StateReaderDependencies::instance_id`]).
    pub(crate) fn deps_instance_id(&self) -> u64 {
        self.deps_instance_id
    }
}
