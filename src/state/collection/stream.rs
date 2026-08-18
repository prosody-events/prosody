//! Managed stream plans: the owned state a collection's stream method carries
//! out of its planning invocation, and the four drivers that run it.
//!
//! A stream method is not a scoped operation — it outlives one. It runs a short
//! `#[read(op)]` planning method (a Map reads its keyset, a Deque its bounds),
//! and that invocation hands back a plan: the binding, the addressed section,
//! and the engine state frozen by [`ReadEngine::capture`](sealed::ReadEngine).
//! Driving the plan then re-enters the engine per chunk, so owner and published
//! reader share one stream implementation with no runtime branch.
//!
//! A plan carries no admission and no operation. The owner reacquires the gate
//! per coordinate chunk and pages a range gate-free; the reader carries the
//! source its planning command selected, so a chunk can never re-probe or
//! change source mid-stream. A range plan also carries the span that its
//! planning command chose. That span is the whole section or one bounded
//! window.

use super::operation::read_keys_bytes;
use super::{StateSession, resolve_batch, resolve_cell, sealed};
use crate::state::cell_key::{CellKey, Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, ContextOf, FromSession, KeyOf, ResolvedOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::store::{CELL_BATCH, CellBuffer, CoordinateBatch};
use crate::state::{SHARD_FANOUT_CONCURRENCY, StateAccessError, StateName, StateType};
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::Either;
use futures::stream::{self, Stream, StreamExt};
use std::marker::PhantomData;
use tokio::task::coop::cooperative;

/// One item a resolving managed stream yields: a decoded key paired with its
/// resolved value, or the error that ended the stream.
pub(crate) type ScanItem<T> = Result<(KeyOf<T>, ResolvedOf<T>), CellStateError<CellCodecError<T>>>;

/// One item a presence-only managed stream yields: a decoded key, or the error
/// that ended the stream. The value-free twin of [`ScanItem`].
pub(crate) type KeyItem<T> = Result<KeyOf<T>, CellStateError<CellCodecError<T>>>;

/// The engine state one invocation froze for its plan.
type PlanOf<S> = <<S as sealed::Session>::Engine as sealed::ReadEngine<S>>::Plan;

/// What every managed plan carries: the collection binding, the section its
/// cells live in, and the captured engine state each continuation resumes
/// under.
pub(crate) struct PlanBase<S: StateSession> {
    session: S,
    state_type: StateType,
    name: StateName,
    section: Section,
    plan: PlanOf<S>,
}

impl<S: StateSession> PlanBase<S> {
    /// Builds the shared half of a plan. Called only from the operation's plan
    /// constructors, which are the sole holders of the engine state.
    pub(super) fn new(
        session: S,
        state_type: StateType,
        name: StateName,
        section: Section,
        plan: PlanOf<S>,
    ) -> Self {
        Self {
            session,
            state_type,
            name,
            section,
            plan,
        }
    }
}

/// A managed point-get plan: the keys a metadata command enumerated, read back
/// in gate-scoped chunks of [`CELL_BATCH`], skipping the ones that read absent.
/// This driver owns the point-get chunk width, and every collection's point-get
/// stream arm runs on it.
///
/// The chunk width is the granularity of both the per-chunk admission and the
/// batch read. ONE aligned batch read fetches a chunk's cells (one Cassandra
/// query, or one fjall hop), and the typed resolves then fan out under
/// `RESOLVE_FANOUT`. Admission covers that raw batch read only: the owner takes
/// one per chunk and releases it before the chunk's resolves, and a published
/// reader holds no gate at all.
///
/// Membership is snapshotted at planning; values are read live, chunk by chunk.
/// A key that disappears between planning and its chunk reads absent and is
/// skipped — the uniform skip every coordinate source applies to TTL holes,
/// popped positions, and membership races alike.
pub(crate) struct CoordinatePlan<S: StateSession, T: CellType> {
    base: PlanBase<S>,
    keys: Vec<KeyOf<T>>,
    limit: Option<usize>,
}

/// A managed durable-range plan: one contiguous span of one section, walked in
/// `dir` order. It pages gate-free and cannot repair. A collection takes this
/// plan when it has no coordinate enumeration to point-get.
///
/// The planning command chooses the span. `Unbounded` edges with no limit walk
/// the whole section. That is the fallback for a collection that cannot say
/// where its cells are.
///
/// A collection with a contiguous coordinate window plans a narrower span
/// instead. A Deque converts its half-open window `[head, tail)` to the
/// inclusive span `[head, tail − 1]` and adds the window's own limit. The walk
/// then reads no row outside the window.
pub(crate) struct RangePlan<S: StateSession, T> {
    base: PlanBase<S>,
    start: ScanEdge<Coordinate>,
    dir: Direction,
    end: ScanEdge<Coordinate>,
    limit: Option<usize>,
    _cell: PhantomData<fn() -> T>,
}

/// The arm a collection's stream method takes, as the owned plan its planning
/// invocation captured. The two members carry the per-kind semantics; a
/// collection chooses between them in its planning method and drives the choice
/// through here.
///
/// A collection that enumerated no live coordinate plans an empty
/// [`Points`](Self::Points) arm: zero point gets and no scan. Its exhaustion
/// still passes the stream fence.
pub(crate) enum Plan<S: StateSession, T: CellType> {
    /// Point-get each planned coordinate, in plan order. A backward stream
    /// reverses the coordinate list at plan time.
    Points(CoordinatePlan<S, T>),

    /// Walk one durable range.
    Scan(RangePlan<S, T>),
}

impl<S: StateSession, T: CellType> Plan<S, T> {
    /// Sets the maximum number of present items that the plan can yield.
    pub(crate) fn with_limit(mut self, limit: usize) -> Self {
        match &mut self {
            Self::Points(plan) => plan.limit = Some(limit),
            Self::Scan(plan) => plan.limit = Some(limit),
        }
        self
    }

    /// Drives the planned arm and resolves each live entry.
    pub(crate) fn entries(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        match self {
            Self::Points(plan) => Either::Left(plan.entries()),
            Self::Scan(plan) => Either::Right(plan.entries()),
        }
    }

    /// Drives the planned arm presence-only. It yields keys and never touches a
    /// value.
    pub(crate) fn keys(self) -> impl Stream<Item = KeyItem<T>> + Send {
        match self {
            Self::Points(plan) => Either::Left(plan.keys()),
            Self::Scan(plan) => Either::Right(plan.keys()),
        }
    }
}

impl<S: StateSession, T: CellType> CoordinatePlan<S, T> {
    /// Builds the plan over the planning invocation's captured state.
    pub(super) fn new(base: PlanBase<S>, keys: Vec<KeyOf<T>>) -> Self {
        Self {
            base,
            keys,
            limit: None,
        }
    }

    /// Streams each planned key's live entry, resolved, in plan order.
    pub(crate) fn entries(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let session = self.base.session.clone();
        let limit = self.limit.unwrap_or(usize::MAX);
        fenced::<S, _, T>(session, self.entry_source().take(limit))
    }

    /// Streams the planned keys whose cell is present, **without decoding or
    /// resolving any value** — so a message-backed collection enumerates keys
    /// with zero loader fetches.
    pub(crate) fn keys(self) -> impl Stream<Item = KeyItem<T>> + Send {
        let session = self.base.session.clone();
        let limit = self.limit.unwrap_or(usize::MAX);
        fenced::<S, _, T>(session, self.key_source().take(limit))
    }

    /// The unfenced resolving body: one admission-scoped batch read, then one
    /// bounded resolve fan-out, per chunk.
    fn entry_source(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        try_stream! {
            let Self { base, keys, limit } = self;
            let base = &base;
            let chunks = stream::unfold((keys.into_iter().peekable(), true), |(mut keys, first)| async move {
                keys.peek()?; // exhausted ⇒ unfold ends
                // No production caller limits entries yet; owner ruling keeps
                // this path symmetric with the key scan.
                let chunk: CellBuffer<KeyOf<T>> =
                    keys.by_ref().take(chunk_width(limit, first)).collect();
                // Admission spans the chunk's raw batch read ONLY: it is
                // released before the chunk's bounded resolve fan-out — which
                // touches no collection state and may reach a loader — and so
                // long before any of the chunk's items reach the caller. An
                // attempt boundary therefore serializes after a chunk's read
                // and never tears one; a boundary during resolution is caught
                // by the per-emission fence before any item escapes.
                let entries = async {
                    let bytes = {
                        let mut inner = <S::Engine as sealed::ReadEngine<S>>::resume(
                            &base.session,
                            &base.plan,
                        )
                        .await;
                        read_keys_bytes::<S, T>(
                            &base.session,
                            &mut inner,
                            base.state_type,
                            &base.name,
                            base.section,
                            &chunk,
                        )
                        .await
                        .map_err(CellStateError::Access)?
                    };
                    let values = resolve_batch::<S, T>(&base.session, bytes).await?;
                    // A `None` is an absent cell: skipped, never an error.
                    Ok::<_, CellStateError<CellCodecError<T>>>(
                        chunk
                            .into_iter()
                            .zip(values)
                            .filter_map(|(key, value)| value.map(|v| (key, v)))
                            .collect::<CellBuffer<_>>(),
                    )
                }
                .await;
                Some((entries, (keys, false)))
            });
            futures::pin_mut!(chunks);
            while let Some(chunk) = chunks.next().await {
                for entry in chunk? {
                    yield entry;
                }
            }
        }
    }

    /// The unfenced presence-only body uses the same chunking. It reads one
    /// presence bit for each key.
    fn key_source(self) -> impl Stream<Item = KeyItem<T>> + Send {
        try_stream! {
            let Self { base, keys, limit } = self;
            let base = &base;
            let chunks = stream::unfold((keys.into_iter().peekable(), true), |(mut keys, first)| async move {
                keys.peek()?;
                let mut inner =
                    <S::Engine as sealed::ReadEngine<S>>::resume(&base.session, &base.plan).await;
                let chunk: CellBuffer<KeyOf<T>> =
                    keys.by_ref().take(chunk_width(limit, first)).collect();
                // Pair each key with its slot so the emission stage can drop
                // absent keys AND checkpoint per key.
                let paired = read_keys_presence::<S, T>(
                    &base.session,
                    &mut inner,
                    base.state_type,
                    &base.name,
                    base.section,
                    &chunk,
                )
                .await
                .map(|slots| {
                    chunk
                        .into_iter()
                        .zip(slots)
                        .collect::<CellBuffer<(KeyOf<T>, bool)>>()
                });
                Some((paired, (keys, false)))
            });
            futures::pin_mut!(chunks);
            while let Some(chunk) = chunks.next().await {
                // Per-key coop checkpoint under an ordered window: the presence
                // filter is synchronous, so a warm chunk of ready keys would
                // otherwise drain the coop budget in one poll (the resolving
                // twin spends the budget per item inside its resolve fan-out).
                // `buffered` keeps key order; absent keys are dropped here. The
                // fan-out is a no-op wrapper on purpose: `cooperative` is the
                // only per-item budget checkpoint reachable here, since tokio's
                // `rt` feature is off and `consume_budget` is therefore
                // uncallable. Do not re-litigate the empty `buffered` window.
                let emit = stream::iter(chunk?)
                    .map(|(key, slot)| {
                        cooperative(async move {
                            Ok::<Option<KeyOf<T>>, CellStateError<CellCodecError<T>>>(
                                slot.then_some(key),
                            )
                        })
                    })
                    .buffered(SHARD_FANOUT_CONCURRENCY);
                futures::pin_mut!(emit);
                while let Some(item) = emit.next().await {
                    if let Some(key) = item? {
                        yield key;
                    }
                }
            }
        }
    }
}

/// Narrows the first tracked chunk to the limit.
/// Dead tracked keys make later chunks return to full width.
fn chunk_width(limit: Option<usize>, first: bool) -> usize {
    if first {
        limit.map_or(CELL_BATCH, |limit| limit.min(CELL_BATCH))
    } else {
        CELL_BATCH
    }
}

impl<S: StateSession, T: CellType> RangePlan<S, T> {
    /// Builds the plan over the planning invocation's captured state. The plan
    /// walks `[start, end]` and yields at most `limit` cells. The edges are
    /// direction-relative, exactly as [`Scan`] defines them.
    pub(super) fn new(
        base: PlanBase<S>,
        start: ScanEdge<Coordinate>,
        dir: Direction,
        end: ScanEdge<Coordinate>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            base,
            start,
            dir,
            end,
            limit,
            _cell: PhantomData,
        }
    }

    /// Opens the plan's durable page over its planned span. It borrows the
    /// whole plan once, so the [`Scan`]'s edges name the plan's own owned
    /// coordinates.
    fn page(&self) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + '_ {
        let scan = Scan {
            section: self.base.section,
            start: self.start.as_ref(),
            dir: self.dir,
            end: self.end.as_ref(),
            limit: self.limit,
        };
        <S::Engine as sealed::ReadEngine<S>>::page(
            &self.base.session,
            &self.base.plan,
            self.base.state_type,
            &self.base.name,
            scan,
        )
    }

    /// Opens the plan's payload-free key page.
    fn page_keys(&self) -> impl Stream<Item = Result<CellKey, StateAccessError>> + Send + '_ {
        let scan = Scan {
            section: self.base.section,
            start: self.start.as_ref(),
            dir: self.dir,
            end: self.end.as_ref(),
            limit: self.limit,
        };
        <S::Engine as sealed::ReadEngine<S>>::page_keys(
            &self.base.session,
            &self.base.plan,
            self.base.state_type,
            &self.base.name,
            scan,
        )
    }

    /// Streams the section's live entries, resolved, in `dir` order.
    pub(crate) fn entries(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let session = self.base.session.clone();
        fenced::<S, _, T>(session, self.entry_source())
    }

    /// Streams the section's live keys in `dir` order through presence-only
    /// pages. It does not transfer, decode, or resolve a value.
    pub(crate) fn keys(self) -> impl Stream<Item = KeyItem<T>> + Send {
        let session = self.base.session.clone();
        fenced::<S, _, T>(session, self.key_source())
    }

    /// The unfenced resolving body: gate-free paging through an ordered
    /// resolution window.
    fn entry_source(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        try_stream! {
            let plan = self;
            <S::Engine as sealed::ReadEngine<S>>::fence(&plan.base.session)?;
            let session = &plan.base.session;
            // `cooperative` inline in the producing closure (a
            // `.map(cooperative)` stage trips a higher-ranked-lifetime error on
            // the non-`'static` per-item futures); `buffered` keeps key order.
            let inner = plan.page()
                .map(|item| {
                    cooperative(async move {
                        let (cell, bytes) = item?;
                        let key = <T::Key as OrderedKeyCodec>::decode(cell.coordinate.as_bytes())
                            .map_err(CellStateError::Key)?;
                        let resolved = resolve_cell::<S, T>(session, bytes).await?;
                        Ok::<_, CellStateError<CellCodecError<T>>>((key, resolved))
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }

    /// The unfenced presence-only body: the same paging, decoding only the
    /// coordinate.
    fn key_source(self) -> impl Stream<Item = KeyItem<T>> + Send {
        try_stream! {
            let plan = self;
            <S::Engine as sealed::ReadEngine<S>>::fence(&plan.base.session)?;
            let inner = plan.page_keys()
                .map(|item| {
                    cooperative(async move {
                        let cell = item?;
                        let key = <T::Key as OrderedKeyCodec>::decode(cell.coordinate.as_bytes())
                            .map_err(CellStateError::Key)?;
                        Ok::<KeyOf<T>, CellStateError<CellCodecError<T>>>(key)
                    })
                })
                .buffered(SHARD_FANOUT_CONCURRENCY);
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    }
}

/// Reads key presence in aligned batches without value payloads.
pub(super) async fn read_keys_presence<S, T>(
    session: &S,
    inner: &mut <S::Engine as sealed::ReadEngine<S>>::ReadInner<'_>,
    state_type: StateType,
    name: &StateName,
    section: Section,
    keys: &[KeyOf<T>],
) -> Result<CellBuffer<bool>, StateAccessError>
where
    S: StateSession,
    T: CellType,
{
    let coordinates = keys.iter().map(<T::Key as OrderedKeyCodec>::encode);
    let mut presence = CellBuffer::with_capacity(keys.len());
    for batch in CoordinateBatch::chunks(coordinates) {
        presence.extend(
            <S::Engine as sealed::ReadEngine<S>>::read_presence_batch(
                session, inner, state_type, name, section, &batch,
            )
            .await?,
        );
    }
    debug_assert_eq!(
        presence.len(),
        keys.len(),
        "batch read answers every input position"
    );
    Ok(presence)
}

/// The managed stream fence adapter — the SOLE home of a managed stream's
/// per-emission attempt fence.
///
/// It wraps a driver's source. It then runs
/// [`ReadEngine::fence`](sealed::ReadEngine::fence) after EVERY `inner.next()`
/// completion — `Some`, `Err`, and the exhaustion `None` alike — and BEFORE it
/// matches that completion. A stream leaked past its handler attempt therefore
/// errors [`Terminated`](crate::state::StateAccessError::Terminated) at its
/// next emission. A spawned task, an un-awaited future, and a foreign promise
/// all leak this way. An empty source still passes the fence on exhaustion, so
/// a leaked empty-plan stream errors instead of reporting a clean end.
///
/// # Invariant — no await, no buffering between the fence and the caller
///
/// Every source buffer sits BELOW this adapter: a coordinate chunk's buffer,
/// and a range source's `buffered` resolution window. A collection adds only
/// synchronous per-item transforms above it.
///
/// The check is a LINEARIZATION point, not a wall-clock wall. A completion
/// whose synchronous fence passed linearized before any concurrent attempt
/// boundary. The check holds no admission, because it is synchronous. A
/// concurrent reset needs the gate exclusively, and for a coordinate source it
/// queues behind the chunk's batch-read admission. Whether its bump landed
/// before a completion's check is what orders the two.
fn fenced<S, X, T>(
    session: S,
    inner: impl Stream<Item = Result<X, CellStateError<CellCodecError<T>>>> + Send,
) -> impl Stream<Item = Result<X, CellStateError<CellCodecError<T>>>> + Send
where
    S: StateSession,
    X: Send,
    T: CellType,
{
    // Heap-hold the source's state machine (the chunk unfold or the `buffered`
    // resolution window): it is the large part, so boxing it keeps the fence
    // adapter — and every collection stream that embeds it — a small future
    // (large-future stack bloat, not a per-item cost). One bounded allocation
    // per stream construction, never the steady-state per-item path;
    // `Pin<Box<_>>` is `Unpin`, so no `pin_mut!`. The boxed type is the
    // concrete source, not a `dyn Stream`.
    let mut inner = Box::pin(inner);
    try_stream! {
        loop {
            let item = inner.next().await;
            // Fence BEFORE matching the completion — `Some`, `Err`, and the
            // exhaustion `None` alike.
            <S::Engine as sealed::ReadEngine<S>>::fence(&session)?;
            match item {
                Some(item) => yield item?,
                None => break,
            }
        }
    }
}
