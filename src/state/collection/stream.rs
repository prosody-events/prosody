//! Managed stream plans: the owned state a collection's stream method carries
//! out of its planning invocation, and the two drivers that run it.
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
//! change source mid-stream. A range source carries the whole section
//! or the bounded window that its planning command chose.

use super::operation::read_keys;
use super::{StateSession, resolve_cell, sealed};
use crate::state::cell::{Presence, Projection, Values};
use crate::state::cell_key::{Coordinate, Direction, Scan, ScanEdge, Section};
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, ContextOf, FromSession, KeyOf, ResolvedOf,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::store::{CELL_BATCH, CellBuffer, FetchSchedule};
use crate::state::{RESOLVE_FANOUT, StateName, StateType};
use async_stream::try_stream;
use futures::future::Either;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use tokio::task::coop::cooperative;

/// The typed output of a projected collection stream.
/// Presence does not decode values or require a resolver context.
pub(crate) trait StreamProjection<S: StateSession, T: CellType>: Projection {
    type Item: Send;

    fn finish(
        session: &S,
        key: KeyOf<T>,
        payload: Self::Payload,
    ) -> impl Future<Output = Result<Self::Item, CellStateError<CellCodecError<T>>>> + Send;
}

impl<S, T> StreamProjection<S, T> for Values
where
    S: StateSession,
    T: CellType,
    for<'s> ContextOf<'s, T>: FromSession<'s, S>,
{
    type Item = (KeyOf<T>, ResolvedOf<T>);

    async fn finish(
        session: &S,
        key: KeyOf<T>,
        payload: Self::Payload,
    ) -> Result<Self::Item, CellStateError<CellCodecError<T>>> {
        Ok((key, resolve_cell::<S, T>(session, payload).await?))
    }
}

impl<S: StateSession, T: CellType> StreamProjection<S, T> for Presence {
    type Item = KeyOf<T>;

    fn finish(
        _session: &S,
        key: KeyOf<T>,
        (): Self::Payload,
    ) -> impl Future<Output = Result<Self::Item, CellStateError<CellCodecError<T>>>> + Send {
        ready(Ok(key))
    }
}

/// One projected item or the error that ends its stream.
pub(crate) type ProjectedItem<S, T, P> =
    Result<<P as StreamProjection<S, T>>::Item, CellStateError<CellCodecError<T>>>;

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

/// A stream source contains either ordered keys or direction-relative range
/// bounds. The collection selects it from stored metadata before execution
/// starts.
enum Source<K> {
    Points(Vec<K>),
    Range {
        start: ScanEdge<Coordinate>,
        dir: Direction,
        end: ScanEdge<Coordinate>,
    },
}

/// A captured collection read with a source and an optional result limit.
///
/// Each terminal applies the limit after absent cells have been removed.
/// The final attempt fence covers every completion, including exhaustion.
/// Source drivers cannot emit directly to the caller.
pub(crate) struct Plan<S: StateSession, T: CellType> {
    base: PlanBase<S>,
    source: Source<KeyOf<T>>,
    limit: Option<NonZeroUsize>,
}

impl<S: StateSession, T: CellType> Plan<S, T> {
    /// Captures keys in their required output order. An empty list performs no
    /// read.
    pub(super) fn coordinates(base: PlanBase<S>, keys: Vec<KeyOf<T>>) -> Self {
        Self {
            base,
            source: Source::Points(keys),
            limit: None,
        }
    }

    /// Captures one range within the collection section.
    pub(super) fn range(
        base: PlanBase<S>,
        start: ScanEdge<Coordinate>,
        dir: Direction,
        end: ScanEdge<Coordinate>,
    ) -> Self {
        Self {
            base,
            source: Source::Range { start, dir, end },
            limit: None,
        }
    }

    /// Bounds the present items the plan yields. A tighter limit wins, so a
    /// caller limit can never widen a source window.
    pub(crate) fn with_limit(mut self, limit: Option<NonZeroUsize>) -> Self {
        self.limit = self.limit.into_iter().chain(limit).min();
        self
    }

    /// Selects one concrete driver, then applies the shared limit and attempt
    /// fence.
    pub(crate) fn projected<P>(self) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send
    where
        P: StreamProjection<S, T>,
        S::Engine: sealed::Reads<S, P>,
    {
        let Self {
            base,
            source,
            limit,
        } = self;
        let session = base.session.clone();
        let inner = match source {
            Source::Points(keys) => Either::Left(coordinate_source::<S, T, P>(base, keys, limit)),
            Source::Range { start, dir, end } => {
                Either::Right(range_source::<S, T, P>(base, start, dir, end, limit))
            }
        };
        fenced::<S, _, T>(
            session,
            inner.take(limit.map_or(usize::MAX, NonZeroUsize::get)),
        )
    }
}

/// Reads aligned chunks under admission. The first chunk is sized to the
/// limit, and each later chunk doubles up to `CELL_BATCH`, so a hole in the
/// keyset costs at most one extra round trip per doubling. A whole chunk must
/// project successfully before it emits any item.
fn coordinate_source<S, T, P>(
    base: PlanBase<S>,
    keys: Vec<KeyOf<T>>,
    limit: Option<NonZeroUsize>,
) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send
where
    S: StateSession,
    T: CellType,
    P: StreamProjection<S, T>,
    S::Engine: sealed::Reads<S, P>,
{
    try_stream! {
        let mut keys = keys.into_iter().peekable();
        let mut fetch = FetchSchedule::new(limit, CELL_BATCH);
        while keys.peek().is_some() {
            let chunk: CellBuffer<_> = keys.by_ref().take(fetch.next().get()).collect();
            let slots = {
                let mut inner = <S::Engine as sealed::ReadEngine<S>>::resume(
                    &base.session,
                    &base.plan,
                ).await;
                read_keys::<S, T, P>(
                    &base.session,
                    &mut inner,
                    base.state_type,
                    &base.name,
                    base.section,
                    &chunk,
                ).await.map_err(CellStateError::Access)?
            };

            let session = &base.session;
            let buffer = CellBuffer::with_capacity(chunk.len());
            // `cooperative` is the only per-item budget checkpoint here. Tokio's `rt`
            // feature is off, so `consume_budget` is uncallable. For `Presence` the
            // wrapped future is a no-op; keep the wrapper. Do not re-litigate the window.
            let items = stream::iter(chunk.into_iter().zip(slots))
                .map(|(key, slot)| cooperative(async move {
                    match slot {
                        Some(payload) => P::finish(session, key, payload).await.map(Some),
                        None => Ok(None),
                    }
                }))
                .buffered(RESOLVE_FANOUT)
                .try_fold(buffer, |mut items, item| {
                    if let Some(item) = item {
                        items.push(item);
                    }
                    ready(Ok(items))
                }).await?;
            for item in items {
                yield item;
            }
        }
    }
}

/// Scans without admission and projects cells through an ordered window.
fn range_source<S, T, P>(
    base: PlanBase<S>,
    start: ScanEdge<Coordinate>,
    dir: Direction,
    end: ScanEdge<Coordinate>,
    limit: Option<NonZeroUsize>,
) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send
where
    S: StateSession,
    T: CellType,
    P: StreamProjection<S, T>,
    S::Engine: sealed::Reads<S, P>,
{
    try_stream! {
        <S::Engine as sealed::ReadEngine<S>>::fence(&base.session)?;
        let window = limit.map_or(RESOLVE_FANOUT, |n| n.get().min(RESOLVE_FANOUT));
        let scan = Scan {
            section: base.section,
            start: start.as_ref(),
            dir,
            end: end.as_ref(),
            fetch_hint: limit.map(|n| n.saturating_add(window)),
        };
        let page = <S::Engine as sealed::Reads<S, P>>::page(
            &base.session, &base.plan, base.state_type, &base.name, scan,
        );
        let session = &base.session;
        let inner = page
            .map(|item| cooperative(async move {
                let (cell, payload) = item?;
                let key = <T::Key as OrderedKeyCodec>::decode(cell.coordinate.as_bytes())
                    .map_err(CellStateError::Key)?;
                P::finish(session, key, payload).await
            }))
            // Both drivers resolve under `RESOLVE_FANOUT`; the limit bounds the window
            // so a small query does not over-pull the pager. `buffered` pulls up to
            // `window` rows past the last yielded item, so the first page includes that
            // headroom and an exact-limit query never fetches a second page.
            .buffered(window);
        futures::pin_mut!(inner);
        while let Some(item) = inner.next().await {
            yield item?;
        }
    }
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
    // Heap-hold the source's state machine (the chunk loop or the `buffered`
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
