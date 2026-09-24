//! Contract pins for the managed stream plans that a `#[read(op)]` planning
//! invocation returns.
//!
//! The subject is the driver, not any one collection. Two one-family probe
//! layouts stand in for every collection that embeds them.
//!
//! [`GatedLayout`]'s cells resolve through a gate ladder. A test therefore
//! controls the completion order of a plan's resolutions, and can pin order and
//! fan-out. [`PlainLayout`]'s cells resolve trivially, which is what the
//! decode-failure and fence pins need.

use crate::codec::{I64Codec, I64CodecError};
use crate::consumer::middleware::RepinProof;
use crate::loader::MemoryLoader;
use crate::state::cell::Values;
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
use crate::state::session::KeyedStateSession;
use crate::state::session::sealed::StateLifecycle;
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
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::time::timeout;
use uuid::Uuid;

mod streams;

/// The gated probe collection's registered name.
const GATE_PROBE: &str = "gate-probe";

/// The plain probe collection's registered name.
const PLAIN_PROBE: &str = "plain-probe";

/// The gated cell type: an `i64`-addressed cell whose payload is the same
/// `i64`. The gate ladder resolves it.
type GatedCell = Keyed<I64KeyCodec, WithResolver<I64Codec, GateResolver>>;

collection_layout! {
    /// A one-family probe layout whose cells resolve through a gate ladder. A
    /// test therefore controls the completion order of a plan's resolutions.
    struct GatedLayout {
        /// The gated cells. A staged write lowers through the resolver's
        /// `stored_from` and never runs the gate, so this family seeds itself.
        #[id(0)]
        CELLS: GatedCell,
    }
}

collection_layout! {
    /// A one-family probe layout over plain, resolver-free `i64` cells. The
    /// order-independent pins run on it: decode failure and the per-emission
    /// fence.
    struct PlainLayout {
        /// The probe cells.
        #[id(0)]
        CELLS: Keyed<I64KeyCodec, I64Codec>,
    }
}

/// The session type the gated fixture binds over. It is the standard memory
/// backend with the [`GateLoader`] capability slot.
type GateSession = KeyedStateSession<TestBackend, GateLoader>;

/// The session type the plain probes bind over.
type PlainSession = KeyedStateSession<TestBackend, MemoryLoader<Value>>;

/// A per-index resolution ladder. `wait(i)` parks until `release(i)` fires gate
/// `i`.
///
/// The ladder rides the session's loader slot, so a resolver reads it as its
/// [`CellResolver::Context`]. This exercises the custom-context
/// [`FromSession`] extension point: a local context struct, not the built-in
/// loader borrow.
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
            // without interruption. When another task observes `parked`, it
            // equals the number of resolutions that wait on their gates.
            let notified = gate.notified();
            self.parked.fetch_add(1, Ordering::SeqCst);
            notified.await;
        }
    }

    /// The number of resolutions that park on their gates now.
    fn parked(&self) -> usize {
        self.parked.load(Ordering::SeqCst)
    }

    fn release(&self, idx: usize) {
        if let Some(gate) = self.gates.get(idx) {
            gate.notify_one();
        }
    }
}

/// The session capability slot that carries the [`GateLadder`]. The
/// [`GateResolver`] awaits that ladder through its context.
#[derive(Clone)]
struct GateLoader(Arc<GateLadder>);

impl GateLoader {
    fn ladder(&self) -> &GateLadder {
        &self.0
    }
}

/// A custom resolver context that borrows the gate ladder from the session.
/// This is the [`FromSession`] extension a resolver author writes for their own
/// capability. It is a distinct local struct, so it stays coherence-disjoint
/// from the built-in `()` and `&S::Loader` impls.
struct GateContext<'s>(&'s GateLadder);

impl<'s> FromSession<'s, GateSession> for GateContext<'s> {
    fn from_session(session: &'s GateSession) -> Self {
        GateContext(session.loader().ladder())
    }
}

/// A resolver whose `resolve` blocks on the stored index's gate. A test
/// therefore controls the completion order of a plan's live resolutions.
struct GateResolver;

impl CellResolver for GateResolver {
    type Context<'s> = GateContext<'s>;
    type Resolved = i64;
    type Stored = i64;
    type Write<'a> = i64;

    const RESOLVER_ID: Option<&'static str> = Some("gate");

    // Desugared `-> impl Future + Send`, the house style that guards against
    // rustc #100013. The future holds the borrowed context across the await.
    fn resolve(
        ctx: Self::Context<'_>,
        stored: i64,
    ) -> impl Future<Output = Result<i64, StateAccessError>> + Send + use<'_> {
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

/// A release order over `0..n`. The range driver's `buffered` window bounds
/// `n`, so every seeded resolution runs at the same time.
#[derive(Clone, Debug)]
struct ReleaseOrder(Vec<usize>);

impl Arbitrary for ReleaseOrder {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = usize::arbitrary(g) % (SHARD_FANOUT_CONCURRENCY + 1);
        let mut order: Vec<usize> = (0..n).collect();
        // Fisher–Yates over the generator gives a uniform permutation of
        // `0..n`. That includes the fully reversed worst case.
        for i in (1..n).rev() {
            let j = usize::arbitrary(g) % (i + 1);
            order.swap(i, j);
        }
        Self(order)
    }
}

/// Builds a [`GateSession`] over a fresh memory store that carries `ladder`,
/// and registers the gated probe.
fn gate_session(ladder: Arc<GateLadder>) -> Result<GateSession> {
    let descriptor: ValueDescriptor<WithResolver<I64Codec, GateResolver>> = value_state(GATE_PROBE);
    let (parts, _) = session_parts(
        GateLoader(ladder),
        value_registry(&descriptor)?,
        StateKey::new(Uuid::new_v4(), Arc::from("gate")),
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
/// independent of the durable identity, so a Value-kind registration accepts
/// the one-family probe layout.
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

/// Seeds cells `0..n` through the collection's own write scope, with
/// payload == key.
async fn seed_gated(cells: &Collection<GateSession, GatedLayout>, n: usize) -> Result<()> {
    cells
        .write(async |op| {
            for key in 0..n as i64 {
                op.set(GatedLayout::CELLS.at(&key), key)?;
            }
            Ok::<(), CellStateError<I64CodecError>>(())
        })
        .await
        .map_err(|e| eyre!("seeding the gated cells failed: {e}"))
}

/// Seeds `0..n`, plans a whole-section range, and drains it. The releaser fires
/// the resolutions in `release` order. The returned keys are what the ordered
/// `buffered` window yielded.
async fn ranged_keys(release: &[usize]) -> Result<Vec<i64>> {
    let n = release.len();
    let ladder = Arc::new(GateLadder::new(n));
    let session = gate_session(ladder.clone())?;
    let cells = bind_gated(&session)?;
    seed_gated(&cells, n).await?;

    let plan = cells
        .read(async |op| {
            op.range::<_, &[u8]>(
                GatedLayout::CELLS,
                Bound::Unbounded,
                Direction::Forward,
                Bound::Unbounded,
            )
        })
        .await;
    let collector = async {
        let stream = plan.projected::<Values>();
        futures::pin_mut!(stream);
        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            let (key, value) = item.map_err(|e| eyre!("range plan: {e}"))?;
            // The resolver returns the stored payload, which equals the key.
            // Tie them, so the run exercises the decode-and-resolve path.
            if key != value {
                return Err(eyre!("resolver desync: key {key} != resolved {value}"));
            }
            keys.push(key);
        }
        Ok::<Vec<i64>, color_eyre::Report>(keys)
    };
    // `join!` polls the collector first. All `n <= SHARD_FANOUT_CONCURRENCY`
    // resolutions therefore register on their gates before the releaser fires
    // them. The wakes queue in `release` order, which `buffer_unordered` would
    // show but `buffered` must not.
    //
    // The releaser PINS that assumption. If one resolution were not yet parked,
    // its release would become a stored permit. The release order would stop
    // being the completion order, and this fixture would silently become a
    // detector that cannot detect.
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
    // The deadline is a hang-guard, never the assertion. The box keeps the
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
