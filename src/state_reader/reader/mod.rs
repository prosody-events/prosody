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
//!
//! Source discovery itself — the cached snapshot, its refresh, and retry
//! pacing — lives in [`acquisition`]. `clippy::multiple_inherent_impl` fires on
//! inherent impls sharing a self type across files, so one module-level
//! expectation covers the whole subtree.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "the acquisition state machine is its own impl beside the state it owns"
)]

pub(super) mod acquisition;

use crate::Key;
use crate::codec::Codec;
use crate::state::StateName;
use crate::state::cell_key::Direction;
use crate::state::descriptor::{
    CellType, ContextOf, DequeDescriptor, DequeHandle, FromSession, MapDescriptor, MapHandle,
    ResolvedOf, StateDescriptor, ValueDescriptor,
};
use crate::state::order_codec::{OrderedKeyCodec, UnitKey};
use crate::state_reader::deps::SharedDeps;
use crate::state_reader::error::StateReaderError;
use crate::state_reader::session::{ReadSession, ReaderCollectionDef, ReaderContext};
use crate::subsystem::SubsystemName;
use acquisition::{DEFAULT_REFRESH_INTERVAL, SnapshotState};
use futures::stream::{Stream, StreamExt};
use quanta::Clock;
use std::fmt::Display;
use std::time::Duration;
use tokio::sync::Mutex;

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
    pub(super) fn with_refresh_interval(
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

    /// The source bundle's construction id (see [`SharedDeps::instance_id`]).
    pub(crate) fn deps_instance_id(&self) -> u64 {
        self.deps_instance_id
    }
}
