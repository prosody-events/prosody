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
//! change source mid-stream.

use super::operation::{read_keys_bytes, read_keys_resolved};
use super::{StateSession, resolve_cell, sealed};
use crate::state::cell_key::{CellKey, Direction, Scan, ScanEdge, Section};
use crate::state::descriptor::{
    CellCodecError, CellStateError, CellType, ContextOf, FromSession, KeyOf, ResolvedOf,
    STREAM_CHUNK,
};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::store::CellBuffer;
use crate::state::{SHARD_FANOUT_CONCURRENCY, StateAccessError, StateName, StateType};
use async_stream::try_stream;
use bytes::Bytes;
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
/// in gate-scoped chunks of [`STREAM_CHUNK`], skipping the ones that read
/// absent.
///
/// Membership is snapshotted at planning; values are read live, chunk by chunk.
/// A key that disappears between planning and its chunk reads absent and is
/// skipped — the uniform skip every coordinate source applies to TTL holes,
/// popped positions, and membership races alike.
pub(crate) struct CoordinatePlan<S: StateSession, T: CellType> {
    base: PlanBase<S>,
    keys: Vec<KeyOf<T>>,
}

/// A managed durable-range plan: the whole of one section, walked in `dir`
/// order. Pages gate-free and cannot repair, so it is the fallback for a
/// collection with no coordinate enumeration to point-get.
pub(crate) struct RangePlan<S: StateSession, T> {
    base: PlanBase<S>,
    dir: Direction,
    _cell: PhantomData<fn() -> T>,
}

impl<S: StateSession, T: CellType> CoordinatePlan<S, T> {
    /// Builds the plan over the planning invocation's captured state.
    pub(super) fn new(base: PlanBase<S>, keys: Vec<KeyOf<T>>) -> Self {
        Self { base, keys }
    }

    /// Streams each planned key's live entry, resolved, in plan order.
    pub(crate) fn entries(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let session = self.base.session.clone();
        fenced::<S, _, T>(session, self.entry_source())
    }

    /// Streams the planned keys whose cell is present, **without decoding or
    /// resolving any value** — so a message-backed collection enumerates keys
    /// with zero loader fetches.
    pub(crate) fn keys(self) -> impl Stream<Item = KeyItem<T>> + Send {
        let session = self.base.session.clone();
        fenced::<S, _, T>(session, self.key_source())
    }

    /// The unfenced resolving body: one admission, one aligned batch read, and
    /// one bounded resolve fan-out per chunk.
    fn entry_source(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        try_stream! {
            let Self { base, keys } = self;
            let base = &base;
            let chunks = stream::unfold(keys.into_iter().peekable(), |mut keys| async move {
                keys.peek()?; // exhausted ⇒ unfold ends
                // Admission per chunk, dropped with this future's scope before
                // any of the chunk's items reach the caller. It spans the whole
                // fetch and resolve, so an attempt boundary serializes after a
                // chunk and never tears one.
                let mut inner =
                    <S::Engine as sealed::ReadEngine<S>>::resume(&base.session, &base.plan).await;
                let chunk: CellBuffer<KeyOf<T>> = keys.by_ref().take(STREAM_CHUNK).collect();
                let entries = read_keys_resolved::<S, T>(
                    &base.session,
                    &mut inner,
                    base.state_type,
                    &base.name,
                    base.section,
                    &chunk,
                )
                .await
                .map(|values| {
                    // A `None` is an absent cell: skipped, never an error.
                    chunk
                        .into_iter()
                        .zip(values)
                        .filter_map(|(key, value)| value.map(|v| (key, v)))
                        .collect::<CellBuffer<_>>()
                });
                Some((entries, keys))
            });
            futures::pin_mut!(chunks);
            while let Some(chunk) = chunks.next().await {
                for entry in chunk? {
                    yield entry;
                }
            }
        }
    }

    /// The unfenced presence-only body: the same chunking, with the value bytes
    /// used as a presence bit and discarded.
    fn key_source(self) -> impl Stream<Item = KeyItem<T>> + Send {
        try_stream! {
            let Self { base, keys } = self;
            let base = &base;
            let chunks = stream::unfold(keys.into_iter().peekable(), |mut keys| async move {
                keys.peek()?;
                let mut inner =
                    <S::Engine as sealed::ReadEngine<S>>::resume(&base.session, &base.plan).await;
                let chunk: CellBuffer<KeyOf<T>> = keys.by_ref().take(STREAM_CHUNK).collect();
                // Pair each key with its slot so the emission stage can drop
                // absent keys AND checkpoint per key.
                let paired = read_keys_bytes::<S, T>(
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
                        .collect::<CellBuffer<(KeyOf<T>, Option<Bytes>)>>()
                });
                Some((paired, keys))
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
                                slot.map(|_| key),
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

impl<S: StateSession, T: CellType> RangePlan<S, T> {
    /// Builds the plan over the planning invocation's captured state.
    pub(super) fn new(base: PlanBase<S>, dir: Direction) -> Self {
        Self {
            base,
            dir,
            _cell: PhantomData,
        }
    }

    /// Streams the section's live entries, resolved, in `dir` order.
    pub(crate) fn entries(self) -> impl Stream<Item = ScanItem<T>> + Send
    where
        for<'s> ContextOf<'s, T>: FromSession<'s, S>,
    {
        let session = self.base.session.clone();
        fenced::<S, _, T>(session, self.entry_source())
    }

    /// Streams the section's live keys in `dir` order, **without decoding or
    /// resolving any value** — the paged envelope's value bytes are discarded.
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
            let Self { base, dir, .. } = self;
            <S::Engine as sealed::ReadEngine<S>>::fence(&base.session)?;
            let session = &base.session;
            // `cooperative` inline in the producing closure (a
            // `.map(cooperative)` stage trips a higher-ranked-lifetime error on
            // the non-`'static` per-item futures); `buffered` keeps key order.
            let inner = page(&base, dir)
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
            let Self { base, dir, .. } = self;
            <S::Engine as sealed::ReadEngine<S>>::fence(&base.session)?;
            let inner = page(&base, dir)
                .map(|item| {
                    cooperative(async move {
                        let (cell, _bytes) = item?; // value bytes discarded — never decoded
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

/// Opens the plan's durable page over the whole section. Both edges are
/// `Unbounded`: a range plan exists precisely because no coordinate enumeration
/// remains to fence the walk. `dir` still orders it.
fn page<S: StateSession>(
    base: &PlanBase<S>,
    dir: Direction,
) -> impl Stream<Item = Result<(CellKey, Bytes), StateAccessError>> + Send + '_ {
    let scan = Scan {
        section: base.section,
        start: ScanEdge::Unbounded,
        dir,
        end: ScanEdge::Unbounded,
        limit: None,
    };
    <S::Engine as sealed::ReadEngine<S>>::page(
        &base.session,
        &base.plan,
        base.state_type,
        &base.name,
        scan,
    )
}

/// The managed stream fence adapter — the SOLE home of a stream's per-emission
/// attempt fence. Wraps a driver's source and runs
/// [`ReadEngine::fence`](sealed::ReadEngine::fence) after EVERY
/// `inner.next()` completion — `Some`, `Err`, and the exhaustion `None`
/// alike — BEFORE matching it, so a stream leaked past its handler attempt (a
/// spawned task, an un-awaited future, a foreign promise) errors
/// [`Terminated`](crate::state::StateAccessError::Terminated) at its next
/// emission. Empty sources still pass the fence on exhaustion, so a leaked
/// empty-plan stream errors rather than reporting a clean end.
///
/// # Invariant — no await, no buffering between the fence and the caller
///
/// Every source buffer (a coordinate chunk's buffer, a range source's
/// `buffered` resolution window) sits BELOW this adapter; a collection adds
/// only synchronous per-item transforms above it. The check is a LINEARIZATION
/// point, not a wall-clock wall: a completion whose synchronous fence passed
/// linearized before any concurrent attempt boundary. It holds no admission
/// (the check is sync); a concurrent reset — which needs the gate exclusively
/// and, for a coordinate source, queues behind the whole chunk's admission — is
/// ordered relative to a completion by whether its bump landed before that
/// completion's check.
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
