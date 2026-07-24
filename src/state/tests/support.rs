//! Shared test doubles and fixture constructors for the keyed-state suites
//! and the consumer tests that mount them. The backend-generic suite
//! runners and their trace types stay in `cell_suite`/`collection_suite`/
//! `identity_suite`; this module holds the standalone doubles they don't own.

use crate::Topic;
use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Scan, Section};
use crate::state::descriptor::{CellResolver, StructuralIdentity};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::MemoryPublicationStore;
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::oracle::CommitOracle;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::registry::DEFAULT_KEYSET_LIMIT;
use crate::state::session::sealed::{MarkerIdentity, ReadAdmission, StateLifecycle};
use crate::state::session::{
    CellRead, CellWrite, Finalized, MessageMarker, MutatePermit, OpPermit, SessionGate,
};
use crate::state::store::{
    CacheBatch, CellBuffer, CellStore, CommittedBatch, CoordinateBatch, provisional_point_loop,
};
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, EventRef, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::subsystem::SubsystemName;
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::stream::{self, Stream};
use parking_lot::Mutex;
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::convert::Infallible;
use std::fmt;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use thiserror::Error;
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

/// Get-out-of-the-way commit oracle: `record_message` is a no-op and every
/// event resolves to the one fixed decision. Use it where the test is not
/// about commit resolution; the commit-tracking double is
/// [`ScriptedOracle`](super::cell_suite::ScriptedOracle).
#[derive(Clone)]
pub struct FixedOracle(CommitDecision);

impl FixedOracle {
    pub(crate) fn committed() -> Self {
        Self(CommitDecision::Committed)
    }

    pub(crate) fn not_committed() -> Self {
        Self(CommitDecision::NotCommitted)
    }
}

impl CommitOracle for FixedOracle {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(self.0)
    }
}

/// A commit oracle counting every `resolve` consult — the no-oracle pins'
/// probe: a verb that must never resolve leaves the counter at zero.
/// `record_message` is a no-op; `resolve` bumps and returns a fixed
/// `NotCommitted`.
#[derive(Clone, Default)]
pub(crate) struct CountingOracle(Arc<AtomicUsize>);

impl CountingOracle {
    /// Resolutions counted so far.
    pub(crate) fn resolves(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl CommitOracle for CountingOracle {
    type Error = Infallible;

    async fn record_message(&self, _dedup_id: Uuid) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(CommitDecision::NotCommitted)
    }
}

/// Stateless session stub: every state op reports
/// [`StateAccessError::Unavailable`] and the lifecycle is inert. Mounted by
/// contexts that carry no keyed state, so a bind against them fails Permanent.
#[derive(Clone)]
pub struct UnavailableState<P> {
    loader: MemoryLoader<P>,
    /// Inert but present: the sealed lifecycle requires a gate accessor.
    gate: Arc<SessionGate>,
}

impl<P> UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    /// Creates the stateless stub.
    #[must_use]
    pub fn new() -> Self {
        Self {
            loader: MemoryLoader::new(),
            gate: Arc::new(SessionGate::new()),
        }
    }
}

impl<P> Default for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<P> fmt::Debug for UnavailableState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UnavailableState")
    }
}

impl<P> ReadAdmission for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Permit<'s>
        = OpPermit<'s>
    where
        Self: 's;

    async fn permit(&self) -> OpPermit<'_> {
        self.gate.read().await
    }

    fn attempt_current(&self) -> bool {
        // Inert: every op errors `Unavailable` before the pin is consulted.
        true
    }
}

impl<P> CellRead for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Loader = MemoryLoader<P>;

    fn loader(&self) -> &Self::Loader {
        &self.loader
    }

    fn is_terminated(&self) -> bool {
        true
    }

    fn collection_has_ttl(&self, _state_type: StateType, _name: &StateName) -> bool {
        false
    }

    fn collection_keyset_limit(&self, _state_type: StateType, _name: &StateName) -> usize {
        // Unreachable in practice: every op on this stub errors `Unavailable`
        // first. The default keeps the trait total.
        DEFAULT_KEYSET_LIMIT
    }

    fn collection_capacity(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Option<NonZeroUsize> {
        // Unreachable in practice (see `collection_keyset_limit`); unbounded
        // keeps the trait total.
        None
    }

    fn verify_state_registration(
        &self,
        _name: &'static str,
        _state_type: StateType,
        _identity: &StructuralIdentity,
    ) -> Result<StateName, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn get(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<Option<Bytes>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn get_many(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
        _batch: &CoordinateBatch,
    ) -> Result<CellBuffer<Option<Bytes>>, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
    }
}

impl<P> CellWrite for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type MutatePermit<'s>
        = MutatePermit<'s>
    where
        Self: 's;

    async fn mutate_permit(&self) -> Result<MutatePermit<'_>, StateAccessError> {
        // Inert stub: every op fails with `Unavailable` first, so this never
        // returns a witness.
        Err(StateAccessError::Unavailable)
    }

    async fn set(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
        _value: &[u8],
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _cell: &CellKey,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn clear_section(
        &self,
        _state_type: StateType,
        _name: &StateName,
        _section: Section,
    ) -> Result<(), StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn commit(
        &self,
        _state_type: StateType,
        _name: &StateName,
    ) -> Result<StoreOutcome, StateAccessError> {
        Err(StateAccessError::Unavailable)
    }

    async fn rollback(&self, _state_type: StateType, _name: &StateName) -> StoreOutcome {
        // Stateless: nothing is ever buffered, so the discard is a NoOp.
        StoreOutcome::NoOp
    }
}

impl<P> StateLifecycle for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Cell = MemoryCellStore<FixedOracle>;

    fn gate(&self) -> &SessionGate {
        &self.gate
    }

    async fn close_gate(&self) -> OpPermit<'_> {
        // Inert session: nothing contends, so no wait tags are needed.
        self.gate.close(|_waited_s| {}).await
    }

    async fn finalize(&self) -> Result<Finalized<Self::Cell>, StateAccessError> {
        Ok(Finalized::Clean)
    }

    async fn record_marker(
        &self,
        _marker: MessageMarker,
        _proof: MarkerWrite,
    ) -> Result<(), StateAccessError> {
        Ok(())
    }

    fn discard_dirty(&self) {}

    fn terminate(&self) {}

    async fn reset(&self, _proof: RepinProof) {}

    fn repin(&self, _proof: RepinProof) -> Self {
        self.clone()
    }

    fn recovery_floor(&self) -> CompactDuration {
        CompactDuration::MIN
    }

    async fn backstop_armed(&self) -> Option<CompactDateTime> {
        None
    }

    async fn mark_backstop_armed(&self, _fire: CompactDateTime) {}

    async fn publish_first_writes(&self) -> Result<(), StateAccessError> {
        // Inert session: nothing is published.
        Ok(())
    }
}

impl<P> MarkerIdentity for UnavailableState<P>
where
    P: Clone + Send + Sync + 'static,
{
    fn set_reload_marker(&self, _marker: MessageMarker) {}

    fn message_marker(&self) -> Option<MessageMarker> {
        // Stateless: no event identity, so nothing filters or records.
        None
    }
}

/// A [`CellStore`] decorator counting every durable mutation, shared by the
/// sweep-idempotence and op-budget pins. Delegates to `inner`; the counters
/// ride an `Arc` so `Clone` shares them.
#[derive(Clone)]
pub(crate) struct CountingCellStore<S> {
    inner: S,
    counts: Arc<OpCounts>,
}

#[derive(Default)]
struct OpCounts {
    write_provisional: AtomicUsize,
    write_resolved: AtomicUsize,
    mark_resolved: AtomicUsize,
    commit_provisional: AtomicUsize,
    abort_provisional: AtomicUsize,
    standing_marker: AtomicUsize,
    get: AtomicUsize,
    get_many: AtomicUsize,
    get_many_for_cache: AtomicUsize,
    scan_cells: AtomicUsize,
    provisional_cells: AtomicUsize,
    provisional_cell_at: AtomicUsize,
    provisional_many: AtomicUsize,
}

impl<S> CountingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(OpCounts::default()),
        }
    }

    /// Total durable mutations (excludes reads). Folds the two settle verbs in:
    /// a forwarded `commit_provisional` / `abort_provisional` routes through
    /// the *inner* store's own primitives, invisible to this wrapper's
    /// per-primitive counters, so the settle itself is counted here. The same
    /// blind spot covers the write-help boundary: a `write_resolved` tick can
    /// fold in a whole standing-marker resolution performed inside the store,
    /// so a `write_resolved` count is not a lower bound on the mutations that
    /// tick issued.
    pub(crate) fn durable_writes(&self) -> usize {
        self.counts.write_provisional.load(Ordering::Relaxed)
            + self.counts.write_resolved.load(Ordering::Relaxed)
            + self.counts.mark_resolved.load(Ordering::Relaxed)
            + self.counts.commit_provisional.load(Ordering::Relaxed)
            + self.counts.abort_provisional.load(Ordering::Relaxed)
    }

    /// Standing-marker point reads — the quiescence counter: one per sweep
    /// marker leg. A read, so excluded from [`Self::durable_writes`].
    pub(crate) fn marker_reads(&self) -> usize {
        self.counts.standing_marker.load(Ordering::Relaxed)
    }

    /// Point reads issued to the lower store — zero on a warm `Cached::get`
    /// hit (present or negative-cached).
    pub(crate) fn lower_reads(&self) -> usize {
        self.counts.get.load(Ordering::Relaxed)
    }

    /// Visible point reads — the query-count-vocabulary view of the `get`
    /// counter, the read [`CellStore::get_many`] batches away. Same atomic as
    /// [`Self::lower_reads`], which frames it as cache fall-through; this name
    /// is the one the read-committed staging query-count pins assert against
    /// (one visible point read per untouched dirty cell).
    pub(crate) fn visible_point_reads(&self) -> usize {
        self.counts.get.load(Ordering::Relaxed)
    }

    /// `get_many` batch reads issued to the lower store — zero on an all-hit
    /// `Cached::get_many` chunk.
    pub(crate) fn batch_reads(&self) -> usize {
        self.counts.get_many.load(Ordering::Relaxed)
    }

    /// `get_many_for_cache` (cache-fill) batch reads issued to the lower store
    /// — exactly one per `Cached::get_many` miss arm.
    pub(crate) fn batch_cache_reads(&self) -> usize {
        self.counts.get_many_for_cache.load(Ordering::Relaxed)
    }

    /// Range scans issued to the lower store — exactly one per `Cached`
    /// scan (KV3: scans bypass the cache), so budget pins can assert a path
    /// issued no scans at all.
    pub(crate) fn lower_scans(&self) -> usize {
        self.counts.scan_cells.load(Ordering::Relaxed)
    }

    /// Recovery-sweep entries — one per `provisional_cells` (cold seed) call,
    /// so a test can pin how many times the sweep hit the durable cold
    /// source.
    pub(crate) fn recovery_sweeps(&self) -> usize {
        self.counts.provisional_cells.load(Ordering::Relaxed)
    }

    /// Warm-sweep point reads — one per `provisional_cell_at`, the reads a warm
    /// (seeded) sweep issues (bounded by #provisional).
    pub(crate) fn warm_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    /// Raw (un-oracle-resolved) provisional point reads — the query-count view
    /// of the `provisional_cell_at` counter. Same atomic as
    /// [`Self::warm_point_reads`], which frames it as the warm recovery sweep's
    /// per-coordinate read; this name is the one the marker-reconstruction
    /// query-count pins assert against (one raw point read per listed
    /// coordinate).
    pub(crate) fn raw_point_reads(&self) -> usize {
        self.counts.provisional_cell_at.load(Ordering::Relaxed)
    }

    /// `provisional_many` raw batch reads issued to the lower store — the
    /// query-count view of the batch verb (one per call, never inflating
    /// `raw_point_reads`).
    pub(crate) fn raw_batch_reads(&self) -> usize {
        self.counts.provisional_many.load(Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) {
        self.counts.write_provisional.store(0, Ordering::Relaxed);
        self.counts.write_resolved.store(0, Ordering::Relaxed);
        self.counts.mark_resolved.store(0, Ordering::Relaxed);
        self.counts.commit_provisional.store(0, Ordering::Relaxed);
        self.counts.abort_provisional.store(0, Ordering::Relaxed);
        self.counts.standing_marker.store(0, Ordering::Relaxed);
        self.counts.get.store(0, Ordering::Relaxed);
        self.counts.get_many.store(0, Ordering::Relaxed);
        self.counts.get_many_for_cache.store(0, Ordering::Relaxed);
        self.counts.scan_cells.store(0, Ordering::Relaxed);
        self.counts.provisional_cells.store(0, Ordering::Relaxed);
        self.counts.provisional_cell_at.store(0, Ordering::Relaxed);
        self.counts.provisional_many.store(0, Ordering::Relaxed);
    }
}

impl<S> CellStore for CountingCellStore<S>
where
    S: CellStore,
{
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.counts.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(collection, cell, own)
    }

    fn get_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CommittedBatch, Self::Error>> + Send + 'a {
        // One increment of the `batch_reads()` counter per batch read
        // *request*; delegating to the inner store keeps its own per-coordinate
        // `get` reads off this wrapper's `get` counter, so they never inflate
        // `lower_reads()`.
        self.counts.get_many.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(collection, section, batch, own)
    }

    fn get_many_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
        own: EventRef,
    ) -> impl Future<Output = Result<CacheBatch, Self::Error>> + Send + 'a {
        self.counts
            .get_many_for_cache
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .get_many_for_cache(collection, section, batch, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        // One increment per scan *request* (a gap query), not per yielded cell.
        self.counts.scan_cells.fetch_add(1, Ordering::Relaxed);
        self.inner.scan_cells(collection, scan, own)
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        // One increment per sweep entry, not per yielded provisional cell.
        self.counts
            .provisional_cells
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cells(collection)
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        // One increment per warm-sweep point read.
        self.counts
            .provisional_cell_at
            .fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_cell_at(collection, cell).await
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        // One increment per batch *request*; the inner store's own reads stay
        // off this wrapper's point-read counter.
        self.counts.provisional_many.fetch_add(1, Ordering::Relaxed);
        self.inner.provisional_many(collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        // One increment per collection-grain batch call (not one per cell).
        self.counts
            .write_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_provisional(collection, writes, marker)
            .await
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.counts.write_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.write_resolved(collection, cells, clears).await
    }

    async fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        self.counts.mark_resolved.fetch_add(1, Ordering::Relaxed);
        self.inner.mark_resolved(collection, cells).await
    }

    async fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        self.counts.standing_marker.fetch_add(1, Ordering::Relaxed);
        self.inner.standing_marker(collection).await
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        // Count the settle itself: the inner store routes to `mark_resolved` /
        // `write_resolved` on *itself*, so those never reach this wrapper's
        // per-primitive counters.
        self.counts
            .commit_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .commit_provisional(collection, writes, clears)
            .await
    }

    async fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        self.counts
            .abort_provisional
            .fetch_add(1, Ordering::Relaxed);
        self.inner.abort_provisional(collection, writes).await
    }
}

/// A session loader carrying a resolve counter, so a stream-laziness test can
/// bound how many values a stream materializes independently of the
/// [`CountingCellStore`] fetch counters. Mirrors that store's owned-counter,
/// read-by-reference shape — no process-global static, so parallel tests stay
/// isolated.
#[derive(Clone, Default)]
pub(crate) struct ResolveCounter(Arc<AtomicUsize>);

impl ResolveCounter {
    /// Resolutions counted so far.
    pub(crate) fn resolves(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn bump(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A passthrough JSON [`CellResolver`] that bumps the session loader's resolve
/// counter on every [`resolve`](CellResolver::resolve). Pair it as
/// `WithResolver<JsonCodec, CountingResolver>` over a [`ResolveCounter`]
/// session loader; a stream-laziness test then asserts a `take(k)` stream
/// resolves only its consumed prefix (± one chunk), never the whole collection.
pub(crate) struct CountingResolver;

impl CellResolver for CountingResolver {
    type Context<'s> = &'s ResolveCounter;
    type Resolved = Value;
    type Stored = Value;
    type Write<'a> = Value;

    const RESOLVER_ID: Option<&'static str> = Some("test-counting-resolver.v1");

    fn resolve(
        ctx: Self::Context<'_>,
        stored: Value,
    ) -> impl Future<Output = Result<Value, StateAccessError>> + Send {
        ctx.bump();
        ready(Ok(stored))
    }

    fn stored_from(write: Value) -> Value {
        write
    }
}

/// A [`CellStore`] decorator whose per-verb responses can be **withheld** —
/// the deterministic-schedule seam behind the KV4 gate pins and the D5
/// drop-arm pin. Each armed hold lets the inner call **complete**, then parks
/// the response on a release [`Notify`] (so the caller's future is suspended
/// at a point where the durable effect has or has not landed by the test's
/// choice of verb). Cancel-safe: dropping a parked future abandons the wait
/// harmlessly.
#[derive(Clone)]
pub(crate) struct HoldingCellStore<S> {
    inner: S,
    holds: Arc<Holds>,
}

/// One hold seam per withholdable verb.
#[derive(Default)]
pub(crate) struct Holds {
    get_for_cache: Hold,
    write_resolved: Hold,
    commit_provisional: Hold,
}

/// One verb's hold: `armed` calls park after their inner call completes;
/// `entered` signals the test a call is parked; `release` resumes it;
/// `landed` counts inner completions (the D5 pin's "the lower batch landed"
/// probe).
#[derive(Default)]
pub(crate) struct Hold {
    armed: AtomicUsize,
    entered: Notify,
    release: Notify,
    landed: AtomicUsize,
}

impl Hold {
    /// Arms the next `n` calls to park after their inner call. Only one parked
    /// call at a time is supported (single `Notify` pair) — arm the next charge
    /// after releasing the previous one.
    pub(crate) fn arm(&self, n: usize) {
        self.armed.store(n, Ordering::Relaxed);
    }

    /// Waits until a parked call signals it entered the hold.
    pub(crate) async fn entered(&self) {
        self.entered.notified().await;
    }

    /// Releases one parked call.
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }

    /// Inner-call completions so far — the "durable effect landed" probe.
    pub(crate) fn landed(&self) -> usize {
        self.landed.load(Ordering::Relaxed)
    }

    /// Runs `inner`, then parks the response while a hold charge is armed.
    async fn pass<T>(&self, inner: impl Future<Output = T>) -> T {
        let out = inner.await;
        self.landed.fetch_add(1, Ordering::Relaxed);
        let parked = self
            .armed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok();
        if parked {
            self.entered.notify_one();
            self.release.notified().await;
        }
        out
    }
}

impl<S> HoldingCellStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            holds: Arc::new(Holds::default()),
        }
    }

    /// The shared per-verb hold seams, for the test to arm and release.
    pub(crate) fn holds(&self) -> Arc<Holds> {
        self.holds.clone()
    }
}

impl Holds {
    pub(crate) fn get_for_cache(&self) -> &Hold {
        &self.get_for_cache
    }

    pub(crate) fn write_resolved(&self) -> &Hold {
        &self.write_resolved
    }

    pub(crate) fn commit_provisional(&self) -> &Hold {
        &self.commit_provisional
    }
}

impl<S> CellStore for HoldingCellStore<S>
where
    S: CellStore,
{
    type Error = S::Error;

    fn get<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> impl Future<Output = Result<Committed, Self::Error>> + Send + 'a {
        self.inner.get(collection, cell, own)
    }

    fn scan_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
        scan: Scan<'a>,
        own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        self.inner.scan_cells(collection, scan, own)
    }

    async fn get_for_cache<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
        own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        self.holds
            .get_for_cache
            .pass(self.inner.get_for_cache(collection, cell, own))
            .await
    }

    fn provisional_cells<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        self.inner.provisional_cells(collection)
    }

    fn provisional_cell_at<'a>(
        &'a self,
        collection: &'a CollectionId,
        cell: &'a CellKey,
    ) -> impl Future<Output = Result<Option<ProvisionalCell>, Self::Error>> + Send + 'a {
        self.inner.provisional_cell_at(collection, cell)
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        provisional_point_loop(self, collection, section, batch)
    }

    fn write_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        marker: Option<&'a EventMarker>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.write_provisional(collection, writes, marker)
    }

    async fn write_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [(CellKey, Option<Bytes>)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.holds
            .write_resolved
            .pass(self.inner.write_resolved(collection, cells, clears))
            .await
    }

    fn mark_resolved<'a>(
        &'a self,
        collection: &'a CollectionRef,
        cells: &'a [CellKey],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.mark_resolved(collection, cells)
    }

    fn standing_marker<'a>(
        &'a self,
        collection: &'a CollectionId,
    ) -> impl Future<Output = Result<Option<EventMarker>, Self::Error>> + Send + 'a {
        self.inner.standing_marker(collection)
    }

    async fn commit_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
        clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        self.holds
            .commit_provisional
            .pass(self.inner.commit_provisional(collection, writes, clears))
            .await
    }

    fn abort_provisional<'a>(
        &'a self,
        collection: &'a CollectionRef,
        writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.inner.abort_provisional(collection, writes)
    }
}

/// A leaf [`CellStore`] whose `get_for_cache` returns a fixed value and TTL,
/// used to prove the default `get_many_for_cache` carries backend TTL metadata
/// through every position (it must not default to `get_many` + `None` TTLs).
/// Every other verb is an inert no-op; only the read pair is exercised.
#[derive(Clone)]
pub(crate) struct TtlStub {
    value: Bytes,
    ttl: Option<CompactDuration>,
}

impl TtlStub {
    pub(crate) fn new(value: Bytes, ttl: Option<CompactDuration>) -> Self {
        Self { value, ttl }
    }
}

impl CellStore for TtlStub {
    type Error = Infallible;

    async fn get<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> Result<Committed, Self::Error> {
        Ok(Committed::new(Some(self.value.clone())))
    }

    async fn get_for_cache<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
        _own: EventRef,
    ) -> Result<(Committed, Option<CompactDuration>), Self::Error> {
        Ok((Committed::new(Some(self.value.clone())), self.ttl))
    }

    fn scan_cells<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _scan: Scan<'a>,
        _own: EventRef,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), Self::Error>> + Send + 'a {
        stream::empty()
    }

    fn provisional_cells<'a>(
        &'a self,
        _collection: &'a CollectionId,
    ) -> impl Stream<Item = Result<(CellKey, ProvisionalCell), Self::Error>> + Send + 'a {
        stream::empty()
    }

    async fn provisional_cell_at<'a>(
        &'a self,
        _collection: &'a CollectionId,
        _cell: &'a CellKey,
    ) -> Result<Option<ProvisionalCell>, Self::Error> {
        Ok(None)
    }

    fn provisional_many<'a>(
        &'a self,
        collection: &'a CollectionId,
        section: Section,
        batch: &'a CoordinateBatch,
    ) -> impl Future<Output = Result<CellBuffer<(Coordinate, ProvisionalCell)>, Self::Error>> + Send + 'a
    {
        // `provisional_cell_at` returns `None`, so the loop yields an empty
        // batch — correct for this read-only stub.
        provisional_point_loop(self, collection, section, batch)
    }

    async fn write_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _marker: Option<&'a EventMarker>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn write_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [(CellKey, Option<Bytes>)],
        _clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn mark_resolved<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _cells: &'a [CellKey],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn standing_marker<'a>(
        &'a self,
        _collection: &'a CollectionId,
    ) -> Result<Option<EventMarker>, Self::Error> {
        Ok(None)
    }

    async fn commit_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
        _clears: &'a [SectionClear],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn abort_provisional<'a>(
        &'a self,
        _collection: &'a CollectionRef,
        _writes: &'a [(CellKey, ProvisionalWrite)],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A collection identity over a fresh v4 segment per call.
///
/// This is the row-isolation mechanism for suites sharing a process-wide
/// store (the shared fjall test database, the shared `prosody_test`
/// keyspace): distinct segments keep rows disjoint even under parallel test
/// threads. Never substitute a fixed id here — use [`fixed_collection`] when
/// determinism, not isolation, is the point.
pub(crate) fn fresh_collection(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// A collection identity with a fixed, deterministic `StateKey`
/// (`Uuid::from_u128(0xA1B2_C3D4)`, key `"user-1"`) — for codec-pinning tests
/// whose value is reproducible identity bytes. Never use it where tests share
/// a store; that is [`fresh_collection`]'s contract.
pub(crate) fn fixed_collection(name: &str) -> Result<CollectionId> {
    Ok(CollectionId::new(
        StateKey::new(Uuid::from_u128(0xA1B2_C3D4), Arc::from("user-1")),
        StateType::Application,
        StateName::try_new(name)?,
    ))
}

/// A short coordinate over a tiny null-prone byte alphabet, so a codec is
/// exercised at the empty coordinate and at coordinates containing the bytes a
/// length-delimited scheme might mishandle.
pub(crate) fn arb_coordinate(g: &mut Gen) -> Coordinate {
    const ALPHABET: [u8; 3] = [0x00, 0x01, 0xFF];
    let len = usize::arbitrary(g) % 4;
    let bytes: Vec<u8> = (0..len)
        .map(|_| g.choose(&ALPHABET).copied().unwrap_or(0))
        .collect();
    Coordinate::from_bytes(bytes)
}

/// The single [`CoordinateBatch`] over `coords`. Every batch test's read list
/// is `≤ CELL_BATCH`, so `chunks` yields exactly one batch; each coordinate is
/// one byte at [`Section`]-0 (matching `cell_at`/`batch_cell`).
pub(crate) fn batch_of(coords: impl IntoIterator<Item = u8>) -> Result<CoordinateBatch> {
    CoordinateBatch::chunks(coords.into_iter().map(|b| Coordinate::from_bytes(vec![b])))
        .next()
        .ok_or_else(|| eyre!("a non-empty coordinate list must yield one batch"))
}

/// A message event with the deterministic dedup id `Uuid::from_u128(n)`.
/// Callers that need the id back recompute it from `n`.
pub(crate) fn probe(n: u128) -> EventRef {
    EventRef::Message {
        dedup_id: Uuid::from_u128(n),
    }
}

/// Asserts an explicit settle (promote, rollback, or sweep) left nothing
/// behind for `id`: no provisional cell and no standing event marker, read
/// **raw** from the durable maps. A resolving read cannot make this check —
/// it heals a still-provisional cell to the same bytes a correct settle
/// writes, so a skipped settle reads back identically. The marker leg is
/// load-bearing for clears-only stages, which stage zero provisional cells:
/// there the stranded marker is the only raw evidence of a skipped settle.
///
/// Call only where the harness guarantees the collection is fully settled;
/// first-touch heals leave the marker standing by design, so an event that
/// deliberately abandons its stage (reset, final-error) leaves residue a
/// later resolving read absorbs — don't probe across such an event.
pub(crate) fn assert_no_settlement_residue(cells: &MemoryCells, id: &CollectionId) -> Result<()> {
    if !cells.provisional_coordinates(id).is_empty() {
        bail!("settlement left a provisional cell standing");
    }
    if cells.standing_marker_of(id).is_some() {
        bail!("settlement left an event marker standing");
    }
    Ok(())
}

/// One recorded call against a [`ScriptedPublicationStore`]. The first-write
/// publication tests check both order and content against this log: a live
/// collection's row appears with the correct partition count, a collection
/// that never writes gets no upsert, and a private write never upserts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublicationCall {
    /// An `upsert` reached the store (recorded only for a successful,
    /// non-failing call, after any release gate opened).
    Upsert {
        /// The collection name.
        name: String,
        /// The publishing group.
        group: String,
        /// The topic.
        topic: String,
        /// The recorded live partition count.
        partition_count: i32,
    },
    /// A `remove` reached the store.
    Remove {
        /// The collection name.
        name: String,
        /// The group whose row was removed.
        group: String,
        /// The topic.
        topic: String,
    },
    /// A `read_publications` reached the store.
    Read {
        /// The collection name.
        name: String,
    },
}

/// A single-use release gate: a successful upsert signals `entered`, then
/// blocks on `released` until the test opens it. Lets a test observe the
/// pre-stage window (row not yet written, cell not yet durable)
/// deterministically without wall-clock timing.
struct ReleaseGate {
    entered: Semaphore,
    released: Semaphore,
}

/// Scripted routing-only publication store for the first-write publication
/// tests. Records every call, can be flipped to fail every `upsert` with a
/// `Transient` error, and can gate a successful `upsert` on a test-controlled
/// release. Wraps a real [`MemoryPublicationStore`] for the actual rows so
/// reads reflect what upserts applied. Cloning shares all state (the `Arc`s).
#[derive(Clone)]
pub(crate) struct ScriptedPublicationStore {
    inner: MemoryPublicationStore,
    calls: Arc<Mutex<Vec<PublicationCall>>>,
    /// When set, every `upsert` returns a `Transient` error instead of
    /// applying, and adds a permit to `errored`. Models a store that keeps
    /// failing until healed.
    fail: Arc<AtomicBool>,
    /// When set, every `read_publications` call still records the
    /// [`PublicationCall::Read`], then returns an error of this category.
    /// Use `Permanent` to model rows that fail to decode, or `Transient`
    /// to model a temporary read fault.
    read_fail: Arc<Mutex<Option<ErrorCategory>>>,
    errored: Arc<Semaphore>,
    /// When present, a successful `upsert` blocks on the release gate.
    gate: Option<Arc<ReleaseGate>>,
}

impl ScriptedPublicationStore {
    /// A store that applies every upsert immediately.
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryPublicationStore::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            fail: Arc::new(AtomicBool::new(false)),
            read_fail: Arc::new(Mutex::new(None)),
            errored: Arc::new(Semaphore::new(0)),
            gate: None,
        }
    }

    /// A store that fails every upsert with a `Transient` error until
    /// [`heal`](Self::heal) is called.
    pub(crate) fn failing() -> Self {
        let store = Self::new();
        store.fail.store(true, Ordering::Release);
        store
    }

    /// A store whose successful upserts block on a release gate.
    pub(crate) fn gated() -> Self {
        Self {
            gate: Some(Arc::new(ReleaseGate {
                entered: Semaphore::new(0),
                released: Semaphore::new(0),
            })),
            ..Self::new()
        }
    }

    /// Stops failing upserts (pairs with [`failing`](Self::failing)).
    pub(crate) fn heal(&self) {
        self.fail.store(false, Ordering::Release);
    }

    /// Makes every subsequent `read_publications` call still record the
    /// [`PublicationCall::Read`], then fail with `category`. Drives the
    /// reconciliation tests: skip the row on `Permanent`, propagate the
    /// error on `Transient`.
    pub(crate) fn fail_reads_with(&self, category: ErrorCategory) {
        *self.read_fail.lock() = Some(category);
    }

    /// Clears a prior [`fail_reads_with`](Self::fail_reads_with) call, so
    /// subsequent `read_publications` calls succeed again. Lets a refresh
    /// property toggle read failures on and off across rounds.
    pub(crate) fn heal_reads(&self) {
        *self.read_fail.lock() = None;
    }

    /// Waits until at least one upsert has failed. This is the deterministic
    /// signal that the settle loop attempted publication and was blocked by
    /// it.
    pub(crate) async fn wait_errored(&self) {
        if let Ok(permit) = self.errored.acquire().await {
            permit.forget();
        }
    }

    /// Waits until a gated upsert has entered and is blocked.
    pub(crate) async fn wait_entered(&self) {
        if let Some(gate) = &self.gate
            && let Ok(permit) = gate.entered.acquire().await
        {
            permit.forget();
        }
    }

    /// Opens the release gate for one blocked upsert.
    pub(crate) fn release(&self) {
        if let Some(gate) = &self.gate {
            gate.released.add_permits(1);
        }
    }

    /// Seeds a row directly, bypassing the call log, failure flag, and gate —
    /// the "pre-existing row" setup for the wrong-count and reconciliation
    /// tests.
    pub(crate) async fn seed(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) {
        let _ = self.inner.upsert(subsystem, state_type, name, row).await;
    }

    /// A snapshot of the recorded calls, in order.
    pub(crate) fn calls(&self) -> Vec<PublicationCall> {
        self.calls.lock().clone()
    }

    /// How many `upsert`s targeted `(name, topic)`.
    pub(crate) fn upserts_for(&self, name: &str, topic: &str) -> usize {
        self.calls
            .lock()
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    PublicationCall::Upsert { name: n, topic: t, .. } if n == name && t == topic
                )
            })
            .count()
    }

    /// The rows currently stored for `(subsystem, name)`.
    pub(crate) async fn rows(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Vec<StatePublication> {
        self.inner
            .read_publications(subsystem, state_type, name)
            .await
            .unwrap_or_default()
    }
}

impl PublicationStore for ScriptedPublicationStore {
    type Error = ScriptedPublicationError;

    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), Self::Error> {
        if self.fail.load(Ordering::Acquire) {
            self.errored.add_permits(1);
            return Err(ScriptedPublicationError(ErrorCategory::Transient));
        }
        if let Some(gate) = &self.gate {
            gate.entered.add_permits(1);
            if let Ok(permit) = gate.released.acquire().await {
                permit.forget();
            }
        }
        self.calls.lock().push(PublicationCall::Upsert {
            name: name.as_str().to_owned(),
            group: row.group_id.to_string(),
            topic: row.topic.to_string(),
            partition_count: i32::from(row.partition_count),
        });
        // Inner store is `Infallible`; the empty match discharges it.
        self.inner
            .upsert(subsystem, state_type, name, row)
            .await
            .map_err(|e| match e {})
    }

    async fn remove(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
        topic: Topic,
    ) -> Result<(), Self::Error> {
        self.calls.lock().push(PublicationCall::Remove {
            name: name.as_str().to_owned(),
            group: group_id.to_owned(),
            topic: topic.to_string(),
        });
        self.inner
            .remove(subsystem, state_type, name, group_id, topic)
            .await
            .map_err(|e| match e {})
    }

    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<Vec<StatePublication>, Self::Error> {
        self.calls.lock().push(PublicationCall::Read {
            name: name.as_str().to_owned(),
        });
        if let Some(category) = *self.read_fail.lock() {
            return Err(ScriptedPublicationError(category));
        }
        self.inner
            .read_publications(subsystem, state_type, name)
            .await
            .map_err(|e| match e {})
    }
}

/// Error from a [`ScriptedPublicationStore`], carrying the classification the
/// settle-path retry posture reads. Never `Terminal`.
#[derive(Clone, Copy, Debug, Error)]
#[error("scripted publication error ({0:?})")]
pub(crate) struct ScriptedPublicationError(ErrorCategory);

impl ClassifyError for ScriptedPublicationError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::registry::CollectionDefRegistry;

    /// Each per-query-kind read accessor counts exactly its own store call:
    /// a `get` bumps only `visible_point_reads`, a `get_many` bumps only
    /// `batch_reads` (its inner per-coordinate gets stay off the wrapper's
    /// `get` counter), and a `provisional_cell_at` bumps only
    /// `raw_point_reads`.
    #[tokio::test]
    async fn counters_increment_once_per_store_call() -> Result<()> {
        let store = CountingCellStore::new(MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            Arc::new(CollectionDefRegistry::default()),
        ));
        let id = fresh_collection("counter-probe")?;
        let cell = CellKey {
            section: Section::new(0),
            coordinate: Coordinate::from_bytes(vec![0]),
        };
        let own = probe(1);

        // Visible point read.
        store.reset();
        store.get(&id, &cell, own).await?;
        assert_eq!(store.visible_point_reads(), 1);
        assert_eq!(store.batch_reads(), 0);
        assert_eq!(store.raw_point_reads(), 0);

        // Visible batch read — must NOT inflate visible_point_reads (the inner
        // store's per-coordinate gets never reach this wrapper's get counter).
        store.reset();
        let batch = batch_of([0])?;
        store.get_many(&id, Section::new(0), &batch, own).await?;
        assert_eq!(store.batch_reads(), 1);
        assert_eq!(store.visible_point_reads(), 0);
        assert_eq!(store.raw_point_reads(), 0);

        // Raw provisional point read.
        store.reset();
        store.provisional_cell_at(&id, &cell).await?;
        assert_eq!(store.raw_point_reads(), 1);
        assert_eq!(store.visible_point_reads(), 0);
        assert_eq!(store.batch_reads(), 0);

        // Raw provisional batch read — bumps only `raw_batch_reads`; the inner
        // store's point reads bypass this wrapper, so `raw_point_reads` stays 0.
        store.reset();
        let batch = batch_of([0])?;
        store.provisional_many(&id, Section::new(0), &batch).await?;
        assert_eq!(store.raw_batch_reads(), 1);
        assert_eq!(store.raw_point_reads(), 0);
        assert_eq!(store.batch_reads(), 0);

        Ok(())
    }
}
