//! Shared test doubles and fixture constructors for the keyed-state suites
//! and the consumer tests that mount them. The backend-generic suite
//! runners and their trace types stay in `cell_suite`/`collection_suite`/
//! `identity_suite`; this module holds the standalone doubles they don't own.

use crate::consumer::middleware::{MarkerWrite, RepinProof};
use crate::loader::MemoryLoader;
use crate::state::access::StateAccessError;
use crate::state::cell::{Committed, ProvisionalCell, ProvisionalWrite};
use crate::state::cell_key::{CellKey, Coordinate, Scan, Section};
use crate::state::descriptor::{CellResolver, StructuralIdentity};
use crate::state::marker::{EventMarker, SectionClear};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::oracle::CommitOracle;
use crate::state::registry::DEFAULT_KEYSET_LIMIT;
use crate::state::session::sealed::{MarkerIdentity, StateLifecycle};
use crate::state::session::{CellSession, Finalized, MessageMarker, OpPermit, SessionGate};
use crate::state::store::{CacheBatch, CellStore, CommittedBatch, CoordinateBatch};
use crate::state::{
    CollectionId, CollectionRef, CommitDecision, EventRef, StateKey, StateName, StateType,
    StoreOutcome,
};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use bytes::Bytes;
use color_eyre::eyre::{Result, bail, eyre};
use futures::stream::{self, Stream};
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::convert::Infallible;
use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
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

impl<P> CellSession for UnavailableState<P>
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

    fn scan<'a>(
        &'a self,
        _state_type: StateType,
        _name: &'a StateName,
        _scan: Scan<'a>,
    ) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + 'a {
        stream::once(async { Err(StateAccessError::Unavailable) })
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

    fn attempt_current(&self) -> bool {
        // Inert: every op errors `Unavailable` before the pin is consulted.
        true
    }

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
    /// the *inner* store's `mark_resolved` / `write_resolved`, invisible to
    /// this wrapper's per-primitive counters, so the settle itself is
    /// counted here.
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
        // One increment per batch read *request*; delegating to the inner store
        // keeps its own per-coordinate `get` reads off this wrapper's `get`
        // counter, so a miss arm shows `batch_cache_reads()==1, lower_reads()==0`.
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
