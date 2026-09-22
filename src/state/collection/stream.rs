//! Managed collection reads share one plan and two source drivers.
//! Planning captures engine state without retaining admission.
//! Owners reacquire admission per point chunk; range scans run without it.
//! Readers retain their chosen source for the whole plan.
//! Session and projection types select the read behavior at compile time.

use super::operation::read_coordinates;
use super::{StateSession, resolve_cell, sealed};
use crate::state::StateAccessError;
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
use pin_project::pin_project;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::coop::cooperative;

/// The typed output of a projected collection stream.
/// Presence does not decode values or require a resolver context.
pub(crate) trait StreamProjection<S: StateSession, T: CellType>: Projection {
    type Item: Send;

    fn finish(
        session: &S,
        key: KeyOf<T>,
        payload: Self::Payload,
    ) -> impl Future<Output = Result<Self::Item, CellStateError<CellCodecError<T>>>>
    + Send
    + use<'_, Self, S, T>;
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
    ) -> impl Future<Output = Result<Self::Item, CellStateError<CellCodecError<T>>>> + Send + use<'_, S, T>
    {
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

/// A stream source contains either ordered coordinates or direction-relative
/// range bounds. The collection selects it from stored metadata before
/// execution starts.
enum Source<B> {
    Points(Vec<Coordinate>),
    Range {
        start: ScanEdge<B>,
        dir: Direction,
        end: ScanEdge<B>,
    },
}

/// A captured collection read with a source and an optional result limit.
///
/// Each terminal applies the limit after absent cells have been removed.
/// The final attempt fence covers every completion, including exhaustion.
/// Source drivers cannot emit directly to the caller.
pub(crate) struct Plan<S: StateSession, T: CellType, B> {
    base: PlanBase<S>,
    source: Source<B>,
    limit: Option<NonZeroUsize>,
    /// The cell type the plan projects. The source holds only coordinates.
    cell: PhantomData<fn() -> T>,
}

/// Checks the attempt fence before each result, including errors and
/// exhaustion. A failure or exhaustion ends the stream. No work follows the
/// fence before emission.
#[pin_project]
struct Fenced<S, I> {
    session: S,
    #[pin]
    inner: I,
    ended: bool,
}

impl<S: StateSession, T: CellType, B: AsRef<[u8]> + Send> Plan<S, T, B> {
    /// Captures coordinates in their required output order. An empty list
    /// performs no read.
    pub(super) fn coordinates(base: PlanBase<S>, coordinates: Vec<Coordinate>) -> Self {
        Self {
            base,
            source: Source::Points(coordinates),
            limit: None,
            cell: PhantomData,
        }
    }

    /// Captures one range within the collection section.
    pub(super) fn range(
        base: PlanBase<S>,
        start: ScanEdge<B>,
        dir: Direction,
        end: ScanEdge<B>,
    ) -> Self {
        Self {
            base,
            source: Source::Range { start, dir, end },
            limit: None,
            cell: PhantomData,
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
    pub(crate) fn projected<P>(
        self,
    ) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send + use<S, T, B, P>
    where
        P: StreamProjection<S, T>,
        S::Engine: sealed::Reads<S, P>,
    {
        let Self {
            base,
            source,
            limit,
            ..
        } = self;
        let session = base.session.clone();
        let inner = match source {
            Source::Points(coordinates) => {
                Either::Left(coordinate_source::<S, T, P>(base, coordinates, limit))
            }
            Source::Range { start, dir, end } => {
                Either::Right(range_source::<S, T, P, B>(base, start, dir, end, limit))
            }
        };
        Fenced {
            session,
            inner: inner.take(limit.map_or(usize::MAX, NonZeroUsize::get)),
            ended: false,
        }
    }
}

/// Reads aligned chunks under admission. [`Projection::demand`] sizes the
/// first chunk from the limit.
/// Each later chunk doubles up to `CELL_BATCH`, so a hole in the keyset costs
/// at most one extra round trip per doubling. A whole chunk must project
/// successfully before it emits any item.
fn coordinate_source<S, T, P>(
    base: PlanBase<S>,
    coordinates: Vec<Coordinate>,
    limit: Option<NonZeroUsize>,
) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send + use<S, T, P>
where
    S: StateSession,
    T: CellType,
    P: StreamProjection<S, T>,
    S::Engine: sealed::Reads<S, P>,
{
    try_stream! {
        let mut coordinates = coordinates.into_iter().peekable();
        let mut fetch = FetchSchedule::new(P::demand(limit), CELL_BATCH);
        while coordinates.peek().is_some() {
            let chunk: CellBuffer<_> = coordinates.by_ref().take(fetch.next().get()).collect();
            let slots = {
                let mut inner = <S::Engine as sealed::ReadEngine<S>>::resume(
                    &base.session,
                    &base.plan,
                ).await;
                read_coordinates::<S, P>(
                    &base.session,
                    &mut inner,
                    base.state_type,
                    &base.name,
                    base.section,
                    chunk.iter().cloned(),
                ).await.map_err(CellStateError::Access)?
            };

            let session = &base.session;
            let buffer = CellBuffer::with_capacity(chunk.len());
            // `cooperative` is the only per-item budget checkpoint here. Tokio's `rt`
            // feature is off, so `consume_budget` is uncallable. For `Presence` the
            // wrapped future is a no-op; keep the wrapper. Do not re-litigate the window.
            let items = stream::iter(chunk.into_iter().zip(slots))
                .map(|(coordinate, slot)| cooperative(async move {
                    match slot {
                        Some(payload) => project::<S, T, P>(session, &coordinate, payload)
                            .await
                            .map(Some),
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
fn range_source<S, T, P, B>(
    base: PlanBase<S>,
    start: ScanEdge<B>,
    dir: Direction,
    end: ScanEdge<B>,
    limit: Option<NonZeroUsize>,
) -> impl Stream<Item = ProjectedItem<S, T, P>> + Send + use<S, T, P, B>
where
    S: StateSession,
    B: AsRef<[u8]> + Send,
    T: CellType,
    P: StreamProjection<S, T>,
    S::Engine: sealed::Reads<S, P>,
{
    try_stream! {
        <S::Engine as sealed::ReadEngine<S>>::fence(&base.session)?;
        let window = limit.map_or(RESOLVE_FANOUT, |n| n.get().min(RESOLVE_FANOUT));
        let scan = Scan {
            section: base.section,
            start: start.as_ref().map(AsRef::as_ref),
            dir,
            end: end.as_ref().map(AsRef::as_ref),
            fetch_hint: P::demand(limit),
        };
        // Every backend page yields present cells only, so the limit ends paging
        // here, before resolution. The plan's own `take` stays the result bound.
        let page = <S::Engine as sealed::Reads<S, P>>::page(
            &base.session, &base.plan, base.state_type, &base.name, scan,
        )
        .take(limit.map_or(usize::MAX, NonZeroUsize::get));
        let session = &base.session;
        let inner = page
            .map(|item| cooperative(async move {
                let (cell, payload) = item?;
                project::<S, T, P>(session, &cell.coordinate, payload).await
            }))
            // Resolvers read the loader, so use RESOLVE_FANOUT, not shard fanout.
            // The limit bounds concurrent resolutions. Without a limit, early
            // cancellation can leave one window of resolutions already started.
            .buffered(window);
        futures::pin_mut!(inner);
        while let Some(item) = inner.next().await {
            yield item?;
        }
    }
}

/// Decodes one present cell's key and finishes its projection.
async fn project<S, T, P>(
    session: &S,
    coordinate: &Coordinate,
    payload: P::Payload,
) -> Result<P::Item, CellStateError<CellCodecError<T>>>
where
    S: StateSession,
    T: CellType,
    P: StreamProjection<S, T>,
{
    let key =
        <T::Key as OrderedKeyCodec>::decode(coordinate.as_bytes()).map_err(CellStateError::Key)?;
    P::finish(session, key, payload).await
}

impl<S, I, X, E> Stream for Fenced<S, I>
where
    S: StateSession,
    I: Stream<Item = Result<X, E>>,
    E: From<StateAccessError>,
{
    type Item = Result<X, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.ended {
            return Poll::Ready(None);
        }
        let Poll::Ready(item) = this.inner.poll_next(cx) else {
            return Poll::Pending;
        };
        if let Err(error) = <S::Engine as sealed::ReadEngine<S>>::fence(this.session) {
            *this.ended = true;
            return Poll::Ready(Some(Err(error.into())));
        }
        *this.ended = !matches!(item, Some(Ok(_)));
        Poll::Ready(item)
    }
}
