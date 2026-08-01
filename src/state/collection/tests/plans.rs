//! Contract pins for the managed stream plans a `#[read(op)]` planning
//! invocation hands back.
//!
//! The subject is the driver, not any one collection: two one-family probe
//! layouts stand in for every collection that embeds them. [`GatedLayout`]'s
//! cells resolve through a gate ladder, so a test controls the completion order
//! of a plan's resolutions and can pin ordering and fan-out; [`PlainLayout`]'s
//! resolve trivially, which is what the decode-failure and fence pins need.

use crate::codec::{I64Codec, I64CodecError};
use crate::consumer::middleware::RepinProof;
use crate::loader::MemoryLoader;
use crate::state::cell_key::{CellKey, Direction};
use crate::state::collection::{
    Collection, CollectionRead, CollectionWrite, StateSession, collection_layout,
};
use crate::state::descriptor::tests::{TestBackend, session_parts, test_session, value_registry};
use crate::state::descriptor::{
    CellResolver, CellStateError, FromSession, Keyed, StructuralIdentity, ValueDescriptor,
    WithResolver, value_state,
};
use crate::state::order_codec::{I64KeyCodec, OrderedKeyCodec};
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{CellWrite, KeyedStateSession};
use crate::state::{
    CollectionKindId, RESOLVE_FANOUT, SHARD_FANOUT_CONCURRENCY, StateAccessError, StateKey,
    StateName, StateType,
};
use crate::test_util::TEST_RUNTIME;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::time::timeout;
use uuid::Uuid;

/// The gated probe collection's registered name.
const GATE_PROBE: &str = "gate-probe";

/// The plain probe collection's registered name.
const PLAIN_PROBE: &str = "plain-probe";

/// The gated cell type: an `i64`-addressed cell whose payload is the same
/// `i64`, resolved through the gate ladder.
type GatedCell = Keyed<I64KeyCodec, WithResolver<I64Codec, GateResolver>>;

collection_layout! {
    /// A one-family probe layout whose cells resolve through a gate ladder, so
    /// a test controls the completion order of a plan's resolutions.
    struct GatedLayout {
        /// The gated cells. A staged write lowers through the resolver's
        /// `stored_from` and never runs the gate, so this family seeds itself.
        #[id(0)]
        CELLS: GatedCell,
    }
}

collection_layout! {
    /// A one-family probe layout over plain, resolver-free `i64` cells — what
    /// the ordering-independent pins (decode failure, the per-emission fence)
    /// run on.
    struct PlainLayout {
        /// The probe cells.
        #[id(0)]
        CELLS: Keyed<I64KeyCodec, I64Codec>,
    }
}

/// The session type the gated fixture binds over: the standard memory backend
/// with the [`GateLoader`] capability slot.
type GateSession = KeyedStateSession<TestBackend, GateLoader>;

/// The session type the plain probes bind over.
type PlainSession = KeyedStateSession<TestBackend, MemoryLoader<Value>>;

/// A per-index resolution ladder: `wait(i)` parks until `release(i)` fires gate
/// `i`. Rides the session's loader slot so a resolver reads it as its
/// [`CellResolver::Context`], exercising the custom-context [`FromSession`]
/// extension point (a local context struct, not the built-in loader borrow).
struct GateLadder {
    gates: Vec<Notify>,
    parked: AtomicUsize,
}

impl GateLadder {
    fn new(n: usize) -> Self {
        Self {
            gates: (0..n).map(|_| Notify::new()).collect(),
            parked: AtomicUsize::new(0),
        }
    }

    async fn wait(&self, idx: usize) {
        if let Some(gate) = self.gates.get(idx) {
            // On the current-thread runtime the count-then-park pair runs
            // uninterrupted, so `parked` equals the number of resolutions
            // waiting on their gates whenever another task observes it.
            let notified = gate.notified();
            self.parked.fetch_add(1, Ordering::SeqCst);
            notified.await;
        }
    }

    /// How many resolutions are currently parked on their gates.
    fn parked(&self) -> usize {
        self.parked.load(Ordering::SeqCst)
    }

    fn release(&self, idx: usize) {
        if let Some(gate) = self.gates.get(idx) {
            gate.notify_one();
        }
    }
}

/// Session capability slot carrying the [`GateLadder`] the [`GateResolver`]
/// awaits through its context.
#[derive(Clone)]
struct GateLoader(Arc<GateLadder>);

impl GateLoader {
    fn ladder(&self) -> &GateLadder {
        &self.0
    }
}

/// Custom resolver context borrowing the gate ladder from the session — the
/// [`FromSession`] extension a resolver author writes for their own capability.
/// Coherence-disjoint from the built-in `()` and `&S::Loader` impls by being a
/// distinct local struct.
struct GateContext<'s>(&'s GateLadder);

impl<'s> FromSession<'s, GateSession> for GateContext<'s> {
    fn from_session(session: &'s GateSession) -> Self {
        GateContext(session.loader().ladder())
    }
}

/// A resolver whose `resolve` blocks on the stored index's gate, so a test
/// controls the completion order of a plan's in-flight resolutions.
struct GateResolver;

impl CellResolver for GateResolver {
    type Context<'s> = GateContext<'s>;
    type Resolved = i64;
    type Stored = i64;
    type Write<'a> = i64;

    const RESOLVER_ID: Option<&'static str> = Some("gate");

    // Desugared `-> impl Future + Send` (the house style guarding rustc
    // #100013): the future holds the borrowed context across the await.
    fn resolve(
        ctx: Self::Context<'_>,
        stored: i64,
    ) -> impl Future<Output = Result<i64, StateAccessError>> + Send {
        let GateContext(ladder) = ctx;
        let gate = usize::try_from(stored).unwrap_or(usize::MAX);
        async move {
            ladder.wait(gate).await;
            Ok(stored)
        }
    }

    fn stored_from(write: i64) -> i64 {
        write
    }
}

/// A release order over `0..n`, `n` bounded by the range driver's `buffered`
/// window so every seeded resolution is in flight at once.
#[derive(Clone, Debug)]
struct ReleaseOrder(Vec<usize>);

impl Arbitrary for ReleaseOrder {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = usize::arbitrary(g) % (SHARD_FANOUT_CONCURRENCY + 1);
        let mut order: Vec<usize> = (0..n).collect();
        // Fisher–Yates over the generator: a uniform permutation of `0..n`,
        // which includes the fully-reversed worst case.
        for i in (1..n).rev() {
            let j = usize::arbitrary(g) % (i + 1);
            order.swap(i, j);
        }
        Self(order)
    }
}

/// Ordered-window invariant: however the concurrent resolutions of a range plan
/// complete, its driver (a `buffered` window, never `buffer_unordered`) yields
/// the cells in key order. The seed count is held at or below
/// [`SHARD_FANOUT_CONCURRENCY`] so every resolution is in flight at once and
/// the release order is the completion order.
#[test]
fn prop_range_plan_yields_key_order_under_any_release_order() {
    fn prop(order: ReleaseOrder) -> TestResult {
        let ReleaseOrder(order) = order;
        let expected: Vec<i64> = (0..order.len() as i64).collect();
        let debug = format!("release={order:?}");
        // A fresh current-thread runtime per iteration: the fixture's parked
        // count is only meaningful when the collector and the releaser share
        // one thread.
        let runtime = match Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(e) => return TestResult::error(format!("runtime: {e}")),
        };
        match runtime.block_on(ranged_keys(&order)) {
            Ok(keys) if keys == expected => TestResult::passed(),
            Ok(keys) => TestResult::error(format!("out of order: {keys:?} for {debug}")),
            Err(e) => TestResult::error(format!("{debug}: {e:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ReleaseOrder) -> TestResult);
}

/// The strongest ordered-window case, pinned directly: releasing every
/// resolution in fully reversed order still yields ascending keys. This is the
/// case a `buffer_unordered` regression fails hardest.
#[tokio::test(flavor = "current_thread")]
async fn range_plan_yields_key_order_under_full_reverse_release() -> Result<()> {
    let n = SHARD_FANOUT_CONCURRENCY;
    let reverse: Vec<usize> = (0..n).rev().collect();
    let keys = ranged_keys(&reverse).await?;
    let expected: Vec<i64> = (0..n as i64).collect();
    if keys != expected {
        return Err(eyre!(
            "a buffered range plan must yield ascending keys under reversed release; got {keys:?}"
        ));
    }
    Ok(())
}

/// Terminate-at-first-error: a range plan over three ascending cells whose
/// middle payload does not decode yields the low cell, then the decode error,
/// then ends — the high cell beyond the error is never produced.
#[test]
fn range_plan_terminates_at_first_error() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let session = plain_session()?;
        let name = StateName::try_new(PLAIN_PROBE)?;
        // Ascending keys; the middle cell's bytes are not a valid `i64` frame.
        for (key, bytes) in [
            (-1_i64, [0_u8; 8].as_slice()),
            (0, b"not an i64".as_slice()),
            (5, [0_u8; 8].as_slice()),
        ] {
            let cell = CellKey {
                section: PlainLayout::CELLS.section(),
                coordinate: I64KeyCodec::encode(&key),
            };
            session
                .set(StateType::Application, &name, &cell, bytes)
                .await
                .map_err(|e| eyre!("seed {key}: {e}"))?;
        }

        let cells = bind_plain(&session)?;
        let plan = cells
            .read(async |op| op.range(PlainLayout::CELLS, Direction::Forward))
            .await;
        let stream = plan.entries();
        futures::pin_mut!(stream);
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }
        match items.as_slice() {
            [Ok((key, value)), Err(CellStateError::Codec(_))] => {
                if *key != -1 || *value != 0 {
                    return Err(eyre!("unexpected first item: ({key}, {value})"));
                }
                Ok(())
            }
            _ => Err(eyre!(
                "expected the low cell then a codec error and nothing more; got {items:?}"
            )),
        }
    })
}

/// The coordinate driver's fence runs on the exhaustion `None`, not only on
/// items: a plan whose attempt epoch is bumped after its last item then yields
/// `Terminated`, never a clean end. The plan is captured before the bump, so
/// the error can only come from the per-emission fence.
///
/// Its empty-plan twin is `empty_coordinate_plan_fences_on_exhaustion` in the
/// parent module.
#[tokio::test]
async fn coordinate_plan_fences_after_its_last_item() -> Result<()> {
    let session = plain_session()?;
    let cells = bind_plain(&session)?;
    cells
        .write(async |op| op.set(PlainLayout::CELLS, &7, 7))
        .await
        .map_err(|e| eyre!("seed: {e}"))?;

    let plan = cells
        .read(async |op| op.coordinates(PlainLayout::CELLS, vec![7_i64]))
        .await;
    let stream = plan.entries();
    futures::pin_mut!(stream);
    match stream.next().await {
        Some(Ok((7, 7))) => {}
        other => return Err(eyre!("first pull must be the seeded item, got {other:?}")),
    }
    // Bump the attempt epoch; the buffered chunk is already drained, so the
    // next pull drives the source to exhaustion and the fence catches it.
    session.reset(RepinProof::for_test()).await;
    match stream.next().await {
        Some(Err(CellStateError::Access(StateAccessError::Terminated))) => Ok(()),
        other => Err(eyre!(
            "the post-bump exhaustion pull must be Terminated, got {other:?}"
        )),
    }
}

/// One full resolve window runs concurrently: every gated resolver of a
/// `get_many` parks before any is released.
#[tokio::test(flavor = "current_thread")]
async fn get_many_resolves_full_window_concurrently() -> Result<()> {
    let n = RESOLVE_FANOUT;
    let ladder = Arc::new(GateLadder::new(n));
    let session = gate_session(ladder.clone())?;
    let cells = bind_gated(&session)?;
    // Seed all n as staged writes in the same session (payload == key), so the
    // batch read answers from the overlay — the pin isolates resolve
    // scheduling.
    seed_gated(&cells, n).await?;

    let keys: Vec<i64> = (0..n as i64).collect();
    let collector = async {
        Box::pin(cells.read(async |op| op.get_many(GatedLayout::CELLS, &keys).await))
            .await
            .map_err(|e| eyre!("get_many: {e}"))
    };
    // With the collector polled first, the full window parks under the one
    // buffered(RESOLVE_FANOUT) window before any release.
    let releaser = async {
        if ladder.parked() != n {
            bail!(
                "all {n} resolves must be in flight before release; parked = {}",
                ladder.parked()
            );
        }
        for idx in 0..n {
            ladder.release(idx);
        }
        Ok(())
    };
    // The deadline is a hang-guard, never the assertion.
    let (collected, outcome) = timeout(
        Duration::from_secs(30),
        Box::pin(async { tokio::join!(collector, releaser) }),
    )
    .await
    .map_err(|_| eyre!("resolve fan-out hung"))?;
    outcome?;
    let out = collected?;
    assert_eq!(out.len(), n, "aligned output");
    for (index, value) in out.iter().enumerate() {
        assert_eq!(
            *value,
            Some(index as i64),
            "position {index} resolved in order"
        );
    }
    Ok(())
}

/// Compile-time regression pin for the `-> impl Future + Send` desugar (rustc
/// #100013): both managed plan drivers must stay `Send`, since a collection's
/// stream method returns one out of a `Send` future. A regression to a source
/// whose `Send` cannot be proven would fail to compile here.
#[test]
fn plan_streams_are_send() -> Result<()> {
    fn assert_send<T: Send>(_value: T) {}

    TEST_RUNTIME.block_on(async {
        let session = gate_session(Arc::new(GateLadder::new(0)))?;
        let cells = bind_gated(&session)?;
        let range = cells
            .read(async |op| op.range(GatedLayout::CELLS, Direction::Forward))
            .await;
        assert_send(range.entries());
        let points = cells
            .read(async |op| op.coordinates(GatedLayout::CELLS, Vec::new()))
            .await;
        assert_send(points.entries());
        Ok(())
    })
}

/// Builds a [`GateSession`] over a fresh memory store carrying `ladder`, with
/// the gated probe registered.
fn gate_session(ladder: Arc<GateLadder>) -> Result<GateSession> {
    let descriptor: ValueDescriptor<WithResolver<I64Codec, GateResolver>> = value_state(GATE_PROBE);
    let (parts, _) = session_parts(
        GateLoader(ladder),
        value_registry(&descriptor)?,
        StateKey::new(Uuid::new_v4(), Arc::from("gate")),
        Arc::default(),
        false,
    );
    Ok(KeyedStateSession::new(parts))
}

/// Builds a session with the plain probe registered.
fn plain_session() -> Result<PlainSession> {
    let descriptor: ValueDescriptor<I64Codec> = value_state(PLAIN_PROBE);
    Ok(test_session(
        MemoryLoader::new(),
        value_registry(&descriptor)?,
    ))
}

/// Binds the gated probe collection over `session`. A layout brand is
/// independent of the durable identity, so a Value-kind registration admits the
/// one-family probe layout.
fn bind_gated(session: &GateSession) -> Result<Collection<GateSession, GatedLayout>> {
    Collection::bind(
        session,
        GATE_PROBE,
        StateType::Application,
        &StructuralIdentity::of::<WithResolver<I64Codec, GateResolver>>(CollectionKindId::Value),
    )
    .map_err(|e| eyre!("gated bind failed: {e}"))
}

/// Binds the plain probe collection over `session`.
fn bind_plain(session: &PlainSession) -> Result<Collection<PlainSession, PlainLayout>> {
    Collection::bind(
        session,
        PLAIN_PROBE,
        StateType::Application,
        &StructuralIdentity::of::<I64Codec>(CollectionKindId::Value),
    )
    .map_err(|e| eyre!("plain bind failed: {e}"))
}

/// Seeds cells `0..n` (payload == key) through the collection's own write
/// scope.
async fn seed_gated(cells: &Collection<GateSession, GatedLayout>, n: usize) -> Result<()> {
    cells
        .write(async |op| {
            for key in 0..n as i64 {
                op.set(GatedLayout::CELLS, &key, key)?;
            }
            Ok::<(), CellStateError<I64CodecError>>(())
        })
        .await
        .map_err(|e| eyre!("seeding the gated cells failed: {e}"))
}

/// Seeds `0..n`, plans a whole-section range, and drains it while releasing the
/// resolutions in `release` order. The returned keys are what the ordered
/// `buffered` window yielded.
async fn ranged_keys(release: &[usize]) -> Result<Vec<i64>> {
    let n = release.len();
    let ladder = Arc::new(GateLadder::new(n));
    let session = gate_session(ladder.clone())?;
    let cells = bind_gated(&session)?;
    seed_gated(&cells, n).await?;

    let plan = cells
        .read(async |op| op.range(GatedLayout::CELLS, Direction::Forward))
        .await;
    let collector = async {
        let stream = plan.entries();
        futures::pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            let (key, value) = item.map_err(|e| eyre!("range plan: {e}"))?;
            // The resolver returns the stored payload, which equals the key;
            // tie them so the decode-and-resolve path is actually exercised.
            if key != value {
                return Err(eyre!("resolver desync: key {key} != resolved {value}"));
            }
            keys.push(key);
        }
        Ok::<Vec<i64>, color_eyre::Report>(keys)
    };
    // `join!` polls the collector first, so all `n <= SHARD_FANOUT_CONCURRENCY`
    // resolutions register on their gates before the releaser fires them; the
    // wakes queue in `release` order, which `buffer_unordered` would surface but
    // `buffered` must not. The releaser PINS that assumption: were any
    // resolution not yet parked, its release would become a stored permit and
    // the release order would stop being the completion order — silently
    // degrading this fixture to a detector that cannot detect.
    let releaser = async {
        if ladder.parked() != n {
            bail!(
                "only {} of {n} resolutions parked before release",
                ladder.parked()
            );
        }
        for &idx in release {
            ladder.release(idx);
        }
        Ok(())
    };
    // The deadline is a hang-guard, never the assertion; boxed to keep the
    // joined future off the caller's stack (clippy::large_futures).
    let (collected, outcome) = timeout(
        Duration::from_secs(30),
        Box::pin(async { tokio::join!(collector, releaser) }),
    )
    .await
    .map_err(|_| eyre!("gated range plan hung"))?;
    outcome?;
    collected
}
