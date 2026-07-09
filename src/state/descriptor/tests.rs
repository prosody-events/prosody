//! Descriptor binding, registration, and typed round-trip tests.
//!
//! The typed `set(T) → cell bytes → store → get() → T` round-trip over the
//! real per-event session machinery; the one-binding-path proof that the
//! JSON and Kafka descriptors bind through the *same* session machinery
//! ([`bind_registered`]); and the registration and bind error surfaces,
//! including the state-unavailable stub on contexts without keyed state.

use super::*;
use crate::codec::{I64Codec, JsonCodecError};
use crate::consumer::event_context::EventContext;
use crate::consumer::kafka_state::{MessageCell, message_state};
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::dirty::DirtyStore;
use crate::state::manager::ArmedKeys;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::{I64KeyCodec, Utf8KeyCodec};
use crate::state::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::{CommitMode, EventRef, PartitionBackend, StateKey, StateName, StateType};
use crate::test_util::ArbJson;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

// Re-exported so contexts that mount a get-out-of-the-way oracle (here and the
// middleware tests) name one canonical type.
pub(crate) use crate::state::tests::support::FixedOracle;

/// Converts a property body's `Result<bool>` into a `TestResult`, surfacing
/// the offending input on failure.
fn finish_trace(result: Result<bool>, message: &str, input: &str) -> TestResult {
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("{message}: {input}")),
        Err(error) => TestResult::error(format!("{message}: {input}: {error:#}")),
    }
}

pub(crate) type TestSession = KeyedStateSession<
    PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, MemoryCellStore<FixedOracle>>,
    MemoryLoader<Value>,
>;

/// Builds a session with `descriptor` registered and binds it via
/// `StateDescriptor::bind` — the single shared machinery every descriptor
/// kind runs through (the one-binding-path proof is that both the JSON
/// tests here and the Kafka-message tests in
/// [`crate::consumer::kafka_state::tests`] call exactly this).
pub(crate) fn bind_registered<DESC>(
    descriptor: DESC,
    loader: MemoryLoader<Value>,
) -> Result<DESC::Handle<TestSession>>
where
    DESC: StateDescriptor,
{
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&descriptor, CollectionDef::new(None))?;
    let session = test_session(loader, registry);
    descriptor
        .bind(&session)
        .map_err(|e| eyre!("bind failed: {e}"))
}

pub(crate) fn test_session(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
) -> TestSession {
    test_session_parts(
        loader,
        registry,
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
    )
    .0
}

/// Like [`test_session`] but pins the session's [`StateKey`] and also returns
/// the underlying [`MemoryCellStore`] (a clone sharing the durable `Arc`), so a
/// caller can inspect the durable cell directly after driving the session
/// through its lifecycle.
pub(crate) fn test_session_parts(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
    state_key: StateKey,
) -> (TestSession, MemoryCellStore<FixedOracle>) {
    test_session_with_armed(loader, registry, state_key, Arc::default())
}

/// Like [`test_session_parts`] but shares an explicit `armed` set across
/// sessions, so a test can drive several events on the same key through one
/// per-partition backstop-amortization state.
pub(crate) fn test_session_with_armed(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
    state_key: StateKey,
    armed: ArmedKeys,
) -> (TestSession, MemoryCellStore<FixedOracle>) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let registry = Arc::new(registry);
    let cell_store = MemoryCellStore::new(
        MemoryCells::new(),
        FixedOracle::committed(),
        registry.clone(),
    );
    let session = KeyedStateSession::new(SessionParts {
        cell: cell_store.clone(),
        dirty: Arc::new(DirtyStore::new()),
        oracle: FixedOracle::committed(),
        loader,
        registry,
        state_key,
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        recovery_delay: CompactDuration::new(30),
        armed,
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    });
    (session, cell_store)
}

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Round-trip invariant: for every JSON-representable value, `set(v)` then
/// `get()` returns `Some(v)` — the value survives the full
/// `T → codec → cell bytes → store → cell bytes → codec → T` path through
/// the real session substrate.
async fn roundtrip(value: Value) -> Result<bool> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    handle.set(value.clone()).await?;
    Ok(handle.get().await? == Some(value))
}

#[test]
fn prop_descriptor_set_get_roundtrip() {
    fn prop(value: ArbJson) -> TestResult {
        let input_dbg = format!("{value:#?}");
        let result = executor::block_on(roundtrip(value.0));
        finish_trace(result, "typed roundtrip lost", &input_dbg)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

/// A never-written collection reads as `None`.
#[tokio::test]
async fn descriptor_get_absent_returns_none() -> Result<()> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// `set` then `clear` reads as `None`.
#[tokio::test]
async fn descriptor_clear_then_get_none() -> Result<()> {
    let handle = bind_registered(cart(), MemoryLoader::new())?;
    handle.set(json!({"items": [1_i32, 2_i32]})).await?;
    handle.clear().await?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// A user-written typed cell: the codec **is** the typing, so a `Cart`
/// cell is one `Codec` impl away — no second encoding layer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Cart {
    items: Vec<String>,
}

#[derive(Default)]
struct CartCodec;

impl Codec for CartCodec {
    type Error = JsonCodecError;
    type Payload = Cart;

    const CODEC_ID: &'static str = "test-cart";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Cart, JsonCodecError> {
        serde_json::from_slice(buf).map_err(JsonCodecError::Serde)
    }

    fn serialize(&mut self, payload: Cart, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
        serde_json::to_writer(buf, &payload).map_err(JsonCodecError::Serde)
    }

    fn with_cached_local<R>(f: impl FnOnce(&mut Self) -> R) -> R {
        thread_local! {
            static CACHE: RefCell<CartCodec> = const { RefCell::new(CartCodec) };
        }
        CACHE.with_borrow_mut(f)
    }
}

/// Typed round-trip: a cell declared with a user codec round-trips its payload
/// type and records the codec's token in the structural identity.
#[tokio::test]
async fn custom_codec_cell_roundtrips_typed_payload() -> Result<()> {
    let typed_cart: ValueDescriptor<CartCodec> = value_state("typed_cart");
    assert_eq!(typed_cart.structural_identity().codec_id, "test-cart");

    let handle = bind_registered(typed_cart, MemoryLoader::new())?;
    let cart = Cart {
        items: vec!["a".into(), "b".into()],
    };
    handle.set(cart.clone()).await?;
    assert_eq!(handle.get().await?, Some(cart));
    Ok(())
}

/// Wire-format freeze for the key-codec tokens, pinned end to end through
/// `structural_identity()`: the token is a durable identity column compared
/// on every acquisition, so changing a key codec's `CODEC_ID` literal — or
/// the map plumbing that lifts it into the identity — silently bricks existing
/// collections. Deque carries `None`: its ordering is fixed by the kind, and
/// that must stay frozen (like the Value `None` pinned in the identity
/// wire-contract test) so a future `Some` cannot brick existing deques.
#[test]
fn key_codec_wire_contract_is_frozen() {
    use crate::state::order_codec::{I64KeyCodec, U64KeyCodec};

    let utf8: MapDescriptor<Utf8KeyCodec> = map_state("m");
    assert_eq!(utf8.structural_identity().key_codec_id, Some("utf8.v1"));
    let i64_keyed: MapDescriptor<I64KeyCodec> = map_state("m");
    assert_eq!(i64_keyed.structural_identity().key_codec_id, Some("i64.v1"));
    let u64_keyed: MapDescriptor<U64KeyCodec> = map_state("m");
    assert_eq!(u64_keyed.structural_identity().key_codec_id, Some("u64.v1"));

    let deque: DequeDescriptor = deque_state("d");
    assert_eq!(deque.structural_identity().key_codec_id, None);
}

/// Binding against a context without keyed state (any context whose
/// session is the
/// [`UnavailableState`](crate::state::tests::support::UnavailableState)
/// stub — here the bare mock) fails with the Permanent
/// [`StateAccessError::Unavailable`].
#[test]
fn state_unavailable_without_keyed_state() -> Result<()> {
    let ctx: MockEventContext = MockEventContext::new();
    let Err(error) = ctx.state(Registered::new(cart())) else {
        return Err(eyre!("bind on a state-less context must fail"));
    };
    assert!(matches!(error, StateAccessError::Unavailable));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Binding an unregistered descriptor fails with a Permanent
/// [`StateAccessError::Unregistered`] — access requires prior
/// registration.
#[tokio::test]
async fn state_with_unregistered_descriptor_errors() -> Result<()> {
    let session = test_session(MemoryLoader::new(), CollectionDefRegistry::new(None));
    let Err(error) = cart().bind(&session) else {
        return Err(eyre!("unregistered bind must fail"));
    };
    assert!(matches!(
        error,
        StateAccessError::Unregistered { name: "cart" }
    ));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Binding a descriptor whose identity differs from the registered one
/// fails with [`StateAccessError::IdentityMismatch`].
#[tokio::test]
async fn bind_with_mismatched_identity_errors() -> Result<()> {
    let recoded: ValueDescriptor<CartCodec> = value_state("cart");
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&cart(), CollectionDef::new(None))?;
    let session = test_session(MemoryLoader::new(), registry);

    let Err(error) = recoded.bind(&session) else {
        return Err(eyre!("mismatched bind must fail"));
    };
    assert!(matches!(error, StateAccessError::IdentityMismatch { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Re-registering the same name with a *different* structural identity
/// is rejected — both for a differing `codec_id` (a Kafka descriptor over a
/// name registered as a JSON value) and for a differing collection `kind` (a
/// Map, then a Deque, over a name registered as a Value).
#[test]
fn conflicting_registration_is_rejected() -> Result<()> {
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&cart(), CollectionDef::new(None))?;

    // Same kind, different codec id.
    assert!(matches!(
        registry.register(
            &message_state::<MemoryLoader<Value>>("cart"),
            CollectionDef::new(None)
        ),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    // Different kind (Map / Deque) under the Value's name.
    let map: MapDescriptor<Utf8KeyCodec> = map_state("cart");
    assert!(matches!(
        registry.register(&map, CollectionDef::new(None)),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    let deque: DequeDescriptor = deque_state("cart");
    assert!(matches!(
        registry.register(&deque, CollectionDef::new(None)),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    Ok(())
}

/// Binding a descriptor of one kind where a different kind was registered
/// under the same name fails with a Permanent
/// [`StateAccessError::IdentityMismatch`] — the kind is part of the frozen
/// structural identity.
#[tokio::test]
async fn bind_with_mismatched_kind_errors() -> Result<()> {
    let map: MapDescriptor<Utf8KeyCodec> = map_state("cart");
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&map, CollectionDef::new(None))?;
    let session = test_session(MemoryLoader::new(), registry);

    // A Deque descriptor asserting a different `kind` for the same name.
    let deque: DequeDescriptor = deque_state("cart");
    let Err(error) = deque.bind(&session) else {
        return Err(eyre!("mismatched-kind bind must fail"));
    };
    assert!(matches!(error, StateAccessError::IdentityMismatch { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Re-registering the same name with an *unchanged* identity is
/// accepted and updates the operational settings — the second `CollectionDef`
/// wins. Identity is frozen; TTL and commit mode are not.
#[test]
fn reregistration_updates_operational_settings() -> Result<()> {
    let name = StateName::try_new("cart")?;
    let initial_ttl = CompactDuration::new(60);
    let updated_ttl = CompactDuration::new(7_200);

    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&cart(), CollectionDef::new(Some(initial_ttl)))?;
    assert_eq!(
        registry.ttl_for(StateType::Application, &name),
        Some(initial_ttl)
    );
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &name),
        CommitMode::ReadCommitted
    );

    // Same name, same identity, different operational settings.
    registry.register(
        &cart(),
        CollectionDef {
            commit_mode: CommitMode::ReadUncommitted,
            ..CollectionDef::new(Some(updated_ttl))
        },
    )?;
    assert_eq!(
        registry.ttl_for(StateType::Application, &name),
        Some(updated_ttl),
        "the re-registration's TTL must win"
    );
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &name),
        CommitMode::ReadUncommitted,
        "the re-registration's commit mode must win"
    );
    Ok(())
}

/// An empty descriptor name fails loudly at registration — the
/// fallible boundary backing the infallible `value_state`.
#[test]
fn empty_name_rejected_at_registration() {
    let mut registry = CollectionDefRegistry::new(None);
    let empty: ValueDescriptor = value_state("");
    let result = registry.register(&empty, CollectionDef::new(None));
    assert!(matches!(result, Err(RegisterStateError::Name(_))));
}

/// Descriptors are plain values: for any runtime name string, two
/// descriptors built independently from equal strings are interchangeable —
/// same (interned) name, same frozen identity — so registering the second
/// is the idempotent re-registration path, never an `IdentityConflict`.
/// This is the invariant that lets call sites build descriptors wherever
/// they need them instead of sharing one declaration.
#[test]
fn prop_descriptors_from_equal_strings_are_interchangeable() {
    fn prop(name: String) -> TestResult {
        if name.trim().is_empty() {
            return TestResult::discard();
        }
        let input_dbg = format!("name={name:?}");
        let result = (move || -> Result<bool> {
            let a: ValueDescriptor = value_state(&name);
            let b: ValueDescriptor = value_state(&name);
            let mut registry = CollectionDefRegistry::new(None);
            registry.register(&a, CollectionDef::new(None))?;
            registry.register(&b, CollectionDef::new(None))?;
            Ok(a.name() == b.name() && a.structural_identity() == b.structural_identity())
        })();
        finish_trace(
            result,
            "equal strings must build interchangeable descriptors",
            &input_dbg,
        )
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}

/// Behavioral arm of the `CollectionScopeContainment` invariant. The
/// *discriminating* proof is the trybuild compile-fail golden
/// (`tests/compile_fail/cellview_scope_is_pinned.rs`); this pins the runtime
/// behavior the type-level proof pairs with.
mod scope_containment {
    use super::*;
    use crate::state::order_codec::Utf8KeyCodec;

    fn wishlist() -> ValueDescriptor {
        value_state("wishlist")
    }

    fn counts() -> MapDescriptor<Utf8KeyCodec> {
        map_state("counts")
    }

    fn log() -> DequeDescriptor {
        deque_state("log")
    }

    /// A registry with sibling collections of every kind registered.
    fn registry_with_siblings() -> Result<CollectionDefRegistry> {
        let mut registry = CollectionDefRegistry::new(None);
        registry.register(&cart(), CollectionDef::new(None))?;
        registry.register(&wishlist(), CollectionDef::new(None))?;
        registry.register(&counts(), CollectionDef::new(None))?;
        registry.register(&log(), CollectionDef::new(None))?;
        Ok(registry)
    }

    /// Sibling descriptors of every kind bound against one session address
    /// disjoint cells — a write to one never leaks into another's read, even
    /// though Value/Map/Deque reuse the same section discriminants (`0`/`1`)
    /// and coordinate spaces. `CellView` pins `(state_type, name)`, so the
    /// handles cannot collide sharing a session and a key.
    #[test]
    fn prop_sibling_descriptors_do_not_leak() {
        async fn check(a: Value, b: Value) -> Result<bool> {
            let session = test_session(MemoryLoader::new(), registry_with_siblings()?);
            let cart = cart().bind(&session).map_err(|e| eyre!("bind cart: {e}"))?;
            let wishlist = wishlist()
                .bind(&session)
                .map_err(|e| eyre!("bind wishlist: {e}"))?;
            let counts = counts()
                .bind(&session)
                .map_err(|e| eyre!("bind counts: {e}"))?;
            let log = log().bind(&session).map_err(|e| eyre!("bind log: {e}"))?;

            // Distinct writes to each sibling, interleaved.
            cart.set(a.clone()).await?;
            wishlist.set(b.clone()).await?;
            counts.set("qty".to_owned(), b.clone()).await?;
            log.push_back(a.clone()).await?;

            // Each handle reads back exactly its own collection's data — no
            // cross-collection or cross-section bleed.
            Ok(cart.get().await? == Some(a.clone())
                && wishlist.get().await? == Some(b.clone())
                && counts.get(&"qty".to_owned()).await? == Some(b)
                && counts.get(&"missing".to_owned()).await?.is_none()
                && log.get(0).await? == Some(a)
                && log.len().await? == 1)
        }
        fn prop(a: ArbJson, b: ArbJson) -> TestResult {
            let input = format!("a={:#?} b={:#?}", a.0, b.0);
            finish_trace(
                executor::block_on(check(a.0, b.0)),
                "sibling leakage",
                &input,
            )
        }
        QuickCheck::new().quickcheck(prop as fn(ArbJson, ArbJson) -> TestResult);
    }
}

/// Typed-cell-view invariants that live on `CellView` itself rather than any
/// one kind: the ordered scan window resolves concurrently but yields in key
/// order, the scan terminates at the first error, the termination guard is
/// enforced by every kind's typed op, and the ops/scan stay `Send` (the
/// desugared `-> impl Future + Send` form guarding rustc issue #100013).
mod typed_cell_view {
    use super::*;
    use tokio::runtime::Builder;

    /// The single section every gated-scan / terminate-at-error fixture seeds
    /// its cells into.
    const CELL_SECTION: Section = Section::new(0);

    /// A per-index resolution ladder: `wait(i)` parks until `release(i)` fires
    /// gate `i`. Rides the session's loader slot so a resolver reads it as its
    /// [`CellResolver::Context`], exercising the custom-context [`FromSession`]
    /// extension point (a local context struct, not the built-in loader
    /// borrow).
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

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

    /// The session type the gated-scan fixture binds over: the standard memory
    /// backend with the [`GateLoader`] capability slot.
    type GateSession = KeyedStateSession<
        PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, MemoryCellStore<FixedOracle>>,
        GateLoader,
    >;

    /// Custom resolver context borrowing the gate ladder from the session — the
    /// [`FromSession`] extension a resolver author writes for their own
    /// capability. Coherence-disjoint from the built-in `()` and `&S::Loader`
    /// impls by being a distinct local struct.
    struct GateContext<'s>(&'s GateLadder);

    impl<'s> FromSession<'s, GateSession> for GateContext<'s> {
        fn from_session(session: &'s GateSession) -> Self {
            GateContext(session.loader().ladder())
        }
    }

    /// A resolver whose `resolve` blocks on the stored index's gate, so a test
    /// controls the completion order of a scan's in-flight resolutions.
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

    /// The gated cell type: an `i64`-addressed cell whose payload is the same
    /// `i64`, resolved through the gate ladder.
    type GatedCell = Keyed<I64KeyCodec, WithResolver<I64Codec, GateResolver>>;

    /// The seeding cell type over the same address + payload as [`GatedCell`],
    /// but a plain passthrough so a test can write the cells without a gate.
    type SeedCell = Keyed<I64KeyCodec, I64Codec>;

    /// Builds a [`GateSession`] over a fresh memory store carrying `ladder`.
    fn gate_session(ladder: Arc<GateLadder>) -> GateSession {
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let registry = Arc::new(CollectionDefRegistry::new(None));
        let cell = MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            registry.clone(),
        );
        KeyedStateSession::new(SessionParts {
            cell,
            dirty: Arc::new(DirtyStore::new()),
            oracle: FixedOracle::committed(),
            loader: GateLoader(ladder),
            registry,
            state_key: StateKey::new(Uuid::new_v4(), Arc::from("gate")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        })
    }

    /// Seeds cells `0..n` (payload == key) into a fresh gated session, then
    /// scans the whole section forward while releasing the resolutions in
    /// `release` order. The returned keys are what the ordered `buffered`
    /// window yielded.
    async fn gated_scan_keys(release: &[usize]) -> Result<Vec<i64>> {
        let n = release.len();
        let ladder = Arc::new(GateLadder::new(n));
        let scope = CellScope::new(
            gate_session(ladder.clone()),
            StateType::Application,
            StateName::try_new("gated")?,
        );

        let seed = scope.typed::<SeedCell>(CELL_SECTION);
        for key in 0..n as i64 {
            seed.set(&key, key)
                .await
                .map_err(|e| eyre!("seed {key}: {e}"))?;
        }

        let view = scope.typed::<GatedCell>(CELL_SECTION);
        let stream = view.scan(Bound::Unbounded, Direction::Forward, Bound::Unbounded, None);
        let collector = async {
            futures::pin_mut!(stream);
            let mut keys = Vec::new();
            while let Some(item) = stream.next().await {
                let (key, value) = item.map_err(|e| eyre!("scan: {e}"))?;
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
        // wakes queue in `release` order, which `buffer_unordered` would surface
        // but `buffered` must not. The releaser PINS that assumption: were any
        // resolution not yet parked, its release would become a stored permit
        // and the release order would stop being the completion order —
        // silently degrading this fixture to a detector that cannot detect.
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
        // The deadline is a hang-guard, never the assertion; boxed to keep
        // the joined future off the caller's stack (clippy::large_futures).
        let (collected, outcome) = timeout(
            Duration::from_secs(30),
            Box::pin(async { tokio::join!(collector, releaser) }),
        )
        .await
        .map_err(|_| eyre!("gated scan hung"))?;
        outcome?;
        collected
    }

    /// Ordered-window invariant: however the concurrent resolutions of a typed
    /// scan complete, `CellView::scan` (a `buffered` window, never
    /// `buffer_unordered`) yields the cells in key order. The seed count is
    /// held at or below [`SHARD_FANOUT_CONCURRENCY`] so every resolution is
    /// in flight at once and the release order is the completion order.
    ///
    /// Falsifiable: switching `scan`'s `buffered` to `buffer_unordered` makes a
    /// non-ascending release order surface out of order (see the full-reverse
    /// directed test).
    #[test]
    fn prop_scan_yields_key_order_under_any_release_order() {
        fn prop(order: ReleaseOrder) -> TestResult {
            let ReleaseOrder(order) = order;
            let expected: Vec<i64> = (0..order.len() as i64).collect();
            let debug = format!("release={order:?}");
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(e) => return TestResult::error(format!("runtime: {e}")),
            };
            match runtime.block_on(gated_scan_keys(&order)) {
                Ok(keys) if keys == expected => TestResult::passed(),
                Ok(keys) => TestResult::error(format!("out of order: {keys:?} for {debug}")),
                Err(e) => TestResult::error(format!("{debug}: {e:#}")),
            }
        }
        QuickCheck::new().quickcheck(prop as fn(ReleaseOrder) -> TestResult);
    }

    /// The strongest ordered-window case, pinned directly: releasing every
    /// resolution in fully reversed order still yields ascending keys. This is
    /// the case a `buffer_unordered` regression fails hardest.
    #[tokio::test]
    async fn scan_yields_key_order_under_full_reverse_release() -> Result<()> {
        let n = SHARD_FANOUT_CONCURRENCY;
        let reverse: Vec<usize> = (0..n).rev().collect();
        let keys = gated_scan_keys(&reverse).await?;
        let expected: Vec<i64> = (0..n as i64).collect();
        if keys != expected {
            return Err(eyre!(
                "buffered scan must yield ascending keys under reversed release; got {keys:?}"
            ));
        }
        Ok(())
    }

    /// Terminate-at-first-error: a typed scan over three ascending cells whose
    /// middle payload does not decode yields the low cell, then the decode
    /// error, then ends — the high cell beyond the error is never produced.
    ///
    /// Falsifiable: a scan that skipped rather than propagated the error would
    /// yield the low and high cells and no error.
    #[tokio::test]
    async fn typed_scan_terminates_at_first_error() -> Result<()> {
        let session = test_session(MemoryLoader::new(), CollectionDefRegistry::new(None));
        let name = StateName::try_new("scan_err")?;
        // Ascending keys; the middle cell's bytes are not valid JSON.
        for (key, bytes) in [
            (-1_i64, b"1".as_slice()),
            (0, b"not json".as_slice()),
            (5, b"2".as_slice()),
        ] {
            let cell = CellKey {
                section: CELL_SECTION,
                coordinate: I64KeyCodec::encode(&key),
            };
            session
                .set(StateType::Application, &name, &cell, bytes)
                .await
                .map_err(|e| eyre!("seed {key}: {e}"))?;
        }

        let scope = CellScope::new(session, StateType::Application, name);
        let view = scope.typed::<Keyed<I64KeyCodec, JsonCodec>>(CELL_SECTION);
        let stream = view.scan(Bound::Unbounded, Direction::Forward, Bound::Unbounded, None);
        futures::pin_mut!(stream);
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item);
        }

        match items.as_slice() {
            [Ok((key, value)), Err(CellStateError::Codec(_))] => {
                if *key != -1 || *value != json!(1_i32) {
                    return Err(eyre!("unexpected first item: ({key}, {value})"));
                }
                Ok(())
            }
            _ => Err(eyre!(
                "expected the low cell then a codec error and nothing more; got {items:?}"
            )),
        }
    }

    /// The termination guard, relocated onto every typed op, holds in each
    /// kind: a bound handle over a terminated session refuses the op with
    /// the (Transient) [`StateAccessError::Terminated`], surfaced through
    /// each kind's error type.
    #[tokio::test]
    async fn terminated_session_refuses_typed_ops_in_every_kind() -> Result<()> {
        let value = value_state::<JsonCodec>("term_value");
        let map = map_state::<Utf8KeyCodec, JsonCodec>("term_map");
        let deque = deque_state::<JsonCodec>("term_deque");
        let mut registry = CollectionDefRegistry::new(None);
        registry.register(&value, CollectionDef::new(None))?;
        registry.register(&map, CollectionDef::new(None))?;
        registry.register(&deque, CollectionDef::new(None))?;
        let session = terminated_session(MemoryLoader::new(), registry);

        let value_handle = value.bind(&session).map_err(|e| eyre!("bind value: {e}"))?;
        let value_result = value_handle.get().await;
        assert!(matches!(
            value_result,
            Err(CellStateError::Access(StateAccessError::Terminated))
        ));
        assert_eq!(
            value_result.err().map(|e| e.classify_error()),
            Some(ErrorCategory::Transient)
        );

        let map_handle = map.bind(&session).map_err(|e| eyre!("bind map: {e}"))?;
        assert!(matches!(
            map_handle.get(&"k".to_owned()).await,
            Err(MapStateError::Cell(CellStateError::Access(
                StateAccessError::Terminated
            )))
        ));

        let deque_handle = deque.bind(&session).map_err(|e| eyre!("bind deque: {e}"))?;
        assert!(matches!(
            deque_handle.len().await,
            Err(DequeStateError::Cell(CellStateError::Access(
                StateAccessError::Terminated
            )))
        ));
        Ok(())
    }

    /// Compile-time regression pin for the `-> impl Future + Send` desugar
    /// (rustc #100013): both a typed op future that holds a resolver's borrowed
    /// context across its await and the typed scan stream must stay `Send`. A
    /// regression to a plain `async fn` would drop `Send` and fail to compile
    /// here.
    #[test]
    fn typed_op_future_and_scan_stream_are_send() -> Result<()> {
        fn assert_send<T: Send>(_value: T) {}

        let loader = MemoryLoader::<Value>::new();
        let handle = bind_registered(
            message_state::<MemoryLoader<Value>>("send_value"),
            loader.clone(),
        )?;
        assert_send(handle.get());

        let scope = CellScope::new(
            test_session(loader, CollectionDefRegistry::new(None)),
            StateType::Application,
            StateName::try_new("send_scan")?,
        );
        let view =
            scope.typed::<Keyed<Utf8KeyCodec, MessageCell<MemoryLoader<Value>>>>(CELL_SECTION);
        assert_send(view.scan(Bound::Unbounded, Direction::Forward, Bound::Unbounded, None));
        Ok(())
    }

    /// A release order over `0..n`, `n` bounded by the scan's `buffered` window
    /// so every seeded resolution is in flight at once.
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

    /// Builds a session whose per-event cancellation is already tripped, so
    /// every typed op guards to [`StateAccessError::Terminated`]. Binding still
    /// succeeds — bind validates registration, not liveness.
    fn terminated_session(
        loader: MemoryLoader<Value>,
        registry: CollectionDefRegistry,
    ) -> TestSession {
        let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
        let (_cancel_tx, cancel_rx) = watch::channel(true);
        let registry = Arc::new(registry);
        let cell = MemoryCellStore::new(
            MemoryCells::new(),
            FixedOracle::committed(),
            registry.clone(),
        );
        KeyedStateSession::new(SessionParts {
            cell,
            dirty: Arc::new(DirtyStore::new()),
            oracle: FixedOracle::committed(),
            loader,
            registry,
            state_key: StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
            event: EventRef::Message {
                dedup_id: Uuid::new_v4(),
            },
            recovery_delay: CompactDuration::new(30),
            armed: Arc::default(),
            termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        })
    }
}
