//! Descriptor binding, registration, and typed round-trip tests.
//!
//! The typed `set(T) → cell bytes → store → get() → T` round-trip over the
//! real per-event session machinery; the one-binding-path proof that the
//! JSON and Kafka descriptors bind through the *same* session machinery
//! ([`bind_registered`]); and the registration and bind error surfaces,
//! including the state-unavailable stub on contexts without keyed state.

use super::*;
use crate::codec::{JsonCodec, JsonCodecError};
use crate::consumer::event_context::EventContext;
use crate::consumer::kafka_state::message_state;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::observer::KafkaObserver;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::cell_key::Direction;
use crate::state::dirty::DirtyStore;
use crate::state::first_write::FirstWritePublisher;
use crate::state::manager::ArmedKeys;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::{I64KeyCodec, Utf8KeyCodec};
use crate::state::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state::{CommitMode, EventRef, PartitionBackend, StateKey, StateName, StateType};
use crate::test_util::{ArbJson, TEST_RUNTIME, captured_spans};
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, eyre};
use futures::TryStreamExt;
use futures::executor;
use opentelemetry_sdk::trace::SpanData;
use quickcheck::{QuickCheck, TestResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::watch;
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

pub(crate) type TestSession = KeyedStateSession<TestBackend, MemoryLoader<Value>>;

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
    let session = test_session(loader, value_registry(&descriptor)?);
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
    let (parts, cell_store) = session_parts(loader, registry, state_key, armed, false);
    (KeyedStateSession::new(parts), cell_store)
}

/// Like [`test_session_parts`] but wires a [`FirstWritePublisher`] into the
/// session, so a test can drive the first-write publication barrier that
/// `Published` collections write through.
pub(crate) fn test_session_with_publisher(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
    state_key: StateKey,
    publisher: FirstWritePublisher<ScriptedPublicationStore, KafkaObserver>,
) -> (TestSession, MemoryCellStore<FixedOracle>) {
    let (mut parts, cell_store) = session_parts(loader, registry, state_key, Arc::default(), false);
    parts.publisher = Some(publisher);
    (KeyedStateSession::new(parts), cell_store)
}

/// The partition backend every test-session fixture in this module shares: the
/// memory cell store resolving through a get-out-of-the-way [`FixedOracle`].
pub(crate) type TestBackend = PartitionBackend<
    FixedOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<FixedOracle>,
    FirstWritePublisher<ScriptedPublicationStore, KafkaObserver>,
>;

/// Builds a test session over an arbitrary loader payload — the generic twin of
/// [`test_session`] (which pins the loader to `MemoryLoader<Value>`). The
/// erased FFI-seam parity suites drive this for both `serde_json::Value` and
/// `BinaryPayload` payloads.
pub(crate) fn test_session_for<L>(
    loader: L,
    registry: CollectionDefRegistry,
) -> KeyedStateSession<TestBackend, L> {
    let (parts, _cell_store) = session_parts(
        loader,
        registry,
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        Arc::default(),
        false,
    );
    KeyedStateSession::new(parts)
}

/// Assembles the [`SessionParts`] shared by every test-session fixture — a
/// fresh memory cell store over `registry`, the committed oracle, and the given
/// `loader`/`state_key`/`armed`. When `cancelled`, the per-event cancellation
/// watch starts tripped (binding still succeeds — bind validates registration,
/// not liveness — but every typed op then guards to
/// [`StateAccessError::Terminated`]). Returns the parts plus a store clone
/// sharing the durable `Arc`, so a caller can inspect the cell after driving
/// the lifecycle.
///
/// [`StateAccessError::Terminated`]: crate::state::access::StateAccessError::Terminated
pub(crate) fn session_parts<L>(
    loader: L,
    registry: CollectionDefRegistry,
    state_key: StateKey,
    armed: ArmedKeys,
    cancelled: bool,
) -> (SessionParts<TestBackend, L>, MemoryCellStore<FixedOracle>) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(cancelled);
    let registry = Arc::new(registry);
    let cell_store = MemoryCellStore::new(
        MemoryCells::new(),
        FixedOracle::committed(),
        registry.clone(),
    );
    let parts = SessionParts {
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
        publisher: None,
    };
    (parts, cell_store)
}

/// A registry holding exactly `descriptor`, for a fixture that binds one
/// collection.
pub(crate) fn value_registry<D: StateDescriptor>(descriptor: &D) -> Result<CollectionDefRegistry> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(descriptor, CollectionDef::new(None))?;
    Ok(registry)
}

/// Like [`test_session_parts`] but hands back the shared dirty overlay, so a
/// test can read the raw cells an invocation staged — or did not.
pub(crate) fn session_with_dirty(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
    state_key: StateKey,
) -> (TestSession, Arc<DirtyStore>) {
    let (parts, _cells) = session_parts(loader, registry, state_key, Arc::default(), false);
    let dirty = parts.dirty.clone();
    (KeyedStateSession::new(parts), dirty)
}

/// A test session over an arbitrary cell store `C` — the twin of
/// [`TestSession`], whose store is pinned to the plain memory one.
pub(crate) type SessionOver<C> = KeyedStateSession<
    PartitionBackend<
        FixedOracle,
        MemoryDescriptorIdentityStore,
        C,
        FirstWritePublisher<ScriptedPublicationStore, KafkaObserver>,
    >,
    MemoryLoader<Value>,
>;

/// A session over an arbitrary cell store — the fixture for I/O-budget tests,
/// which put a counting store under the committed cache.
pub(crate) fn session_over<C: CellStore>(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
    state_key: StateKey,
    cell: C,
) -> SessionOver<C> {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    KeyedStateSession::new(SessionParts {
        cell,
        dirty: Arc::new(DirtyStore::new()),
        oracle: FixedOracle::committed(),
        loader,
        registry: Arc::new(registry),
        state_key,
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    })
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

    const FORMAT_ID: &'static str = "test-cart";

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<Cart, JsonCodecError> {
        serde_json::from_slice(buf).map_err(JsonCodecError::Serde)
    }

    fn deserialize_owned(&mut self, buf: bytes::BytesMut) -> Result<Cart, JsonCodecError> {
        serde_json::from_slice(&buf).map_err(JsonCodecError::Serde)
    }

    fn serialize(&mut self, payload: Cart, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
        serde_json::to_writer(buf, &payload).map_err(JsonCodecError::Serde)
    }

    fn serialize_ref(&mut self, payload: &Cart, buf: &mut Vec<u8>) -> Result<(), JsonCodecError> {
        serde_json::to_writer(buf, payload).map_err(JsonCodecError::Serde)
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
    assert_eq!(typed_cart.structural_identity().format_id, "test-cart");

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
/// on every acquisition, so changing a key codec's `FORMAT_ID` literal — or
/// the derivation that lifts it off the cell type — silently bricks existing
/// collections. Deque and Value carry kind-pinned key axes (`I64KeyCodec`,
/// [`UnitKey`](crate::state::order_codec::UnitKey)); their tokens must stay
/// frozen just like the user-chosen ones.
#[test]
fn key_codec_wire_contract_is_frozen() {
    use crate::state::order_codec::{I64KeyCodec, U64KeyCodec};

    let utf8: MapDescriptor<Utf8KeyCodec> = map_state("m");
    assert_eq!(utf8.structural_identity().key_format_id, "utf8.v1");
    let i64_keyed: MapDescriptor<I64KeyCodec> = map_state("m");
    assert_eq!(i64_keyed.structural_identity().key_format_id, "i64.v1");
    let u64_keyed: MapDescriptor<U64KeyCodec> = map_state("m");
    assert_eq!(u64_keyed.structural_identity().key_format_id, "u64.v1");

    let deque: DequeDescriptor = deque_state("d");
    assert_eq!(deque.structural_identity().key_format_id, "i64.v1");
    let value: ValueDescriptor = value_state("v");
    assert_eq!(value.structural_identity().key_format_id, "unit.v1");
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
    let session = test_session(MemoryLoader::new(), CollectionDefRegistry::default());
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
    let mut registry = CollectionDefRegistry::default();
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
/// is rejected — both for a differing `format_id` (a Kafka descriptor over a
/// name registered as a JSON value) and for a differing collection `kind` (a
/// Map, then a Deque, over a name registered as a Value).
#[test]
fn conflicting_registration_is_rejected() -> Result<()> {
    let mut registry = CollectionDefRegistry::default();
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
    let mut registry = CollectionDefRegistry::default();
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

/// Re-registering the same name with an *unchanged* identity is rejected as a
/// duplicate declaration — one declaration per name per registry, never
/// last-wins. The first registration's operational settings stand; the second
/// errors [`RegisterStateError::Duplicate`] (`Permanent`) rather than silently
/// overwriting them.
#[test]
fn reregistration_is_rejected_as_duplicate() -> Result<()> {
    let name = StateName::try_new("cart")?;
    let initial_ttl = CompactDuration::new(60);
    let updated_ttl = CompactDuration::new(7_200);

    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(Some(initial_ttl)))?;

    // Same name, same identity, different operational settings — rejected.
    let duplicate = registry.register(
        &cart(),
        CollectionDef {
            commit_mode: CommitMode::ReadUncommitted,
            ..CollectionDef::new(Some(updated_ttl))
        },
    );
    let Err(error) = duplicate else {
        return Err(eyre!("a duplicate registration must be rejected"));
    };
    assert!(matches!(error, RegisterStateError::Duplicate { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);

    // The first registration's settings are untouched — no last-wins overwrite.
    assert_eq!(
        registry.ttl_for(StateType::Application, &name),
        Some(initial_ttl),
        "the rejected re-registration must not have changed the TTL"
    );
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &name),
        CommitMode::ReadCommitted,
        "the rejected re-registration must not have changed the commit mode"
    );
    Ok(())
}

/// The Map-only `keyset_limit` fluent method threads into the collection def
/// (uncompilable on a Value or Deque, which is a type-level property, not a
/// runtime one).
#[test]
fn keyset_limit_threads_into_the_collection_def() {
    let descriptor: MapDescriptor<I64KeyCodec> = map_state("m");
    assert_eq!(descriptor.keyset_limit(7).collection_def().keyset_limit, 7);
}

/// `.published(bool)` and every read-cache policy thread into the collection
/// def. `.published` is also reversible.
#[test]
fn visibility_and_read_cache_thread_into_the_collection_def() {
    use crate::state::ReadCachePolicy;
    use std::time::Duration;

    let ttl = Duration::from_secs(30);
    let def = cart().published(true).read_cache(ttl).collection_def();
    assert_eq!(def.visibility, StateVisibility::Published);
    assert_eq!(def.read_cache, ReadCachePolicy::Ttl(ttl));
    assert_eq!(
        cart()
            .read_cache(ReadCachePolicy::Disabled)
            .collection_def()
            .read_cache,
        ReadCachePolicy::Disabled,
    );
    assert_eq!(
        cart()
            .published(true)
            .published(false)
            .collection_def()
            .visibility,
        StateVisibility::Private,
        "published(true).published(false) reverts to Private",
    );
}

/// An empty descriptor name fails loudly at registration — the
/// fallible boundary backing the infallible `value_state`.
#[test]
fn empty_name_rejected_at_registration() {
    let mut registry = CollectionDefRegistry::default();
    let empty: ValueDescriptor = value_state("");
    let result = registry.register(&empty, CollectionDef::new(None));
    assert!(matches!(result, Err(RegisterStateError::Name(_))));
}

/// Descriptors are plain values: for any runtime name string, two
/// descriptors built independently from equal strings are interchangeable —
/// same (interned) name, same frozen identity — so a call site can build a
/// descriptor wherever it needs one instead of sharing one declaration. The
/// registry holds one declaration per name, so registering the second is
/// rejected loudly as a [`RegisterStateError::Duplicate`] (never last-wins);
/// interchangeability is the identity/name equality, provable without a
/// successful second register.
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
            let mut registry = CollectionDefRegistry::default();
            registry.register(&a, CollectionDef::new(None))?;
            // A same-identity re-registration of the equal descriptor is a
            // duplicate declaration — rejected, not silently overwritten.
            let duplicate = registry.register(&b, CollectionDef::new(None));
            if !matches!(duplicate, Err(RegisterStateError::Duplicate { .. })) {
                return Ok(false);
            }
            Ok(a.name() == b.name() && a.structural_identity() == b.structural_identity())
        })();
        finish_trace(
            result,
            "equal strings must build interchangeable descriptors and reject a duplicate register",
            &input_dbg,
        )
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}

/// The named attribute's exported value, stringified.
fn span_attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

/// A representative operation of every kind and shape (value read/write,
/// keyed map ops, deque mutators, both stream twins) exports a span named
/// for the operation, carrying the `collection` attribute (plus `map.key` /
/// `direction` where applicable), and parented on the ambient span — so a
/// handler's state access is visible and self-describing under its event
/// span without any explicit parenting.
#[test]
fn collection_ops_export_operation_spans() -> Result<()> {
    let outcome: RefCell<Result<()>> = RefCell::new(Ok(()));
    let spans = captured_spans(|| {
        let handler = tracing::info_span!("handler");
        let _guard = handler.enter();
        // `TEST_RUNTIME`, not `futures::executor`: the simple span processor
        // block_ons its export on span end, which may not nest inside a
        // `LocalPool`. The root future runs on this thread, so the entered
        // `handler` span stays ambient.
        *outcome.borrow_mut() = TEST_RUNTIME.block_on(async {
            let value = bind_registered(cart(), MemoryLoader::new())?;
            value.set(json!({"qty": 1_i32})).await?;
            value.get().await?;

            let map = bind_registered(
                map_state::<Utf8KeyCodec, JsonCodec>("counts"),
                MemoryLoader::new(),
            )?;
            map.set("k1".to_owned(), json!(1_i32)).await?;
            map.get(&"k1".to_owned()).await?;
            let _entries: Vec<_> = map.stream(Direction::Forward).try_collect().await?;
            map.remove(&"k1".to_owned()).await?;

            let deque = bind_registered(deque_state::<JsonCodec>("dq"), MemoryLoader::new())?;
            deque.push_back(json!(7_i32)).await?;
            let _elements: Vec<_> = deque.stream(Direction::Forward).try_collect().await?;
            deque.pop_front().await?;
            Ok(())
        });
    });
    outcome.into_inner()?;

    let handler_id = spans
        .iter()
        .find(|s| s.name == "handler")
        .ok_or_else(|| eyre!("handler span not exported"))?
        .span_context
        .span_id();

    for (name, collection) in [
        ("value.set", "cart"),
        ("value.get", "cart"),
        ("map.set", "counts"),
        ("map.get", "counts"),
        ("map.stream", "counts"),
        ("map.remove", "counts"),
        ("deque.push_back", "dq"),
        ("deque.stream", "dq"),
        ("deque.pop_front", "dq"),
    ] {
        let span = spans
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| eyre!("missing span {name}"))?;
        assert_eq!(
            span_attr(span, "collection").as_deref(),
            Some(collection),
            "{name} must carry its collection name"
        );
        assert_eq!(
            span.parent_span_id, handler_id,
            "{name} must nest under the ambient span"
        );
        if name.starts_with("map.") && name != "map.stream" {
            assert_eq!(
                span_attr(span, "map.key").as_deref(),
                Some("k1"),
                "{name} must carry the map key"
            );
        }
    }
    for name in ["map.stream", "deque.stream"] {
        let stream = spans
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| eyre!("missing span {name}"))?;
        assert_eq!(
            span_attr(stream, "direction").as_deref(),
            Some("Forward"),
            "{name} must carry the scan direction"
        );
    }
    Ok(())
}

/// Behavioral arm of the collection-containment invariant: a handle bound to
/// one collection cannot address another. Every kind carries `(state_type,
/// name)` on its [`Collection`] binding, and the type system enforces that.
/// This test pins the matching runtime behavior.
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
        let mut registry = CollectionDefRegistry::default();
        registry.register(&cart(), CollectionDef::new(None))?;
        registry.register(&wishlist(), CollectionDef::new(None))?;
        registry.register(&counts(), CollectionDef::new(None))?;
        registry.register(&log(), CollectionDef::new(None))?;
        Ok(registry)
    }

    /// Sibling descriptors of every kind bound against one session address
    /// disjoint cells — a write to one never leaks into another's read, even
    /// though Value/Map/Deque reuse the same section discriminants (`0`/`1`)
    /// and coordinate spaces. The binding pins `(state_type, name)`, so the
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

/// Every typed op enforces the termination guard, and the guard holds in each
/// kind. A bound handle over a terminated session refuses the op with the
/// Transient [`StateAccessError::Terminated`]. Each kind's error type carries
/// that refusal.
#[tokio::test]
async fn terminated_session_refuses_typed_ops_in_every_kind() -> Result<()> {
    let value = value_state::<JsonCodec>("term_value");
    let map = map_state::<Utf8KeyCodec, JsonCodec>("term_map");
    let deque = deque_state::<JsonCodec>("term_deque");
    let mut registry = CollectionDefRegistry::default();
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

/// Builds a session whose per-event cancellation is already tripped. Every
/// typed op then guards to [`StateAccessError::Terminated`]. Binding still
/// succeeds, because bind validates registration, not liveness.
fn terminated_session(loader: MemoryLoader<Value>, registry: CollectionDefRegistry) -> TestSession {
    let (parts, _) = session_parts(
        loader,
        registry,
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        Arc::default(),
        true,
    );
    KeyedStateSession::new(parts)
}

/// Compile-time regression pin for the `-> impl Future + Send` desugar, which
/// guards against rustc #100013. A handle's typed op future holds the
/// resolver's borrowed context across its await, so it must stay `Send`. A
/// plain `async fn` would drop `Send` and fail to compile here. The
/// plan-driver twin is `plan_streams_are_send` in
/// [`crate::state::collection::tests`].
#[test]
fn typed_op_future_is_send() -> Result<()> {
    fn assert_send<T: Send>(_value: T) {}

    let handle = bind_registered(
        message_state::<MemoryLoader<Value>>("send_value"),
        MemoryLoader::<Value>::new(),
    )?;
    assert_send(handle.get());
    Ok(())
}
