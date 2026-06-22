//! Descriptor binding, registration, and typed round-trip tests.
//!
//! N2 — the typed `set(T) → cell bytes → store → get() → T` round-trip over
//! the real per-event session machinery. N4 — the two-instance interface
//! proof: the JSON and Kafka descriptors bind through the *same* session
//! machinery ([`bind_registered`]). N5 — registration and bind error surfaces,
//! including the state-unavailable stub on contexts without keyed state.

use super::*;
use crate::codec::JsonCodecError;
use crate::consumer::event_context::EventContext;
use crate::consumer::kafka_state::message_state;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::partition::ShutdownPhase;
use crate::loader::MemoryLoader;
use crate::state::memory::{MemoryCellStore, MemoryCommittedCache, MemoryDescriptorIdentityStore};
use crate::state::oracle::CommitOracle;
use crate::state::partition_store::PartitionStateStore;
use crate::state::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::session::{ArmedKeys, KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::{
    CommitDecision, CommitMode, EventRef, SharedStateBackend, StateBackendFactory, StateKey,
    StateName, StateType,
};
use crate::test_util::ArbJson;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{QuickCheck, TestResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

/// Oracle returning a fixed decision; the session's marker flush and the
/// store's resolution both route through it.
#[derive(Clone)]
pub(crate) struct FixedOracle(CommitDecision);

impl FixedOracle {
    fn committed() -> Self {
        Self(CommitDecision::Committed)
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
    <SharedStateBackend<
        MemoryCellStore,
        MemoryDescriptorIdentityStore,
        FixedOracle,
        MemoryCommittedCache,
    > as StateBackendFactory>::Backend,
    MemoryLoader<Value>,
>;

/// Builds a session with `descriptor` registered and binds it via
/// `StateDescriptor::bind` — the single shared machinery every descriptor
/// kind runs through (the N4 proof is that both the JSON tests here and
/// the Kafka-message tests in
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
) -> (TestSession, MemoryCellStore) {
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
) -> (TestSession, MemoryCellStore) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let registry = Arc::new(registry);
    let cell_store = MemoryCellStore::new();
    let store = PartitionStateStore::new(
        cell_store.clone(),
        FixedOracle::committed(),
        MemoryCommittedCache::new(),
        registry.clone(),
    );
    let session = KeyedStateSession::new(SessionParts {
        store,
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

/// N2 invariant: for every JSON-representable value, `set(v)` then `get()`
/// returns `Some(v)` — the value survives the full
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

/// N2 (typed): a cell declared with a user codec round-trips its payload
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

/// N5: binding against a context without keyed state (any context whose
/// session is the [`UnavailableState`](crate::state::session::UnavailableState)
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

/// N5: binding an unregistered descriptor fails with a Permanent
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

/// N5: binding a descriptor whose identity differs from the registered one
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

/// N5: re-registering the same name with a *different* structural identity
/// is rejected.
///
/// Kept as a directed example rather than a generated property: today only
/// `codec_id` and `resolver_id` can differ (`kind` is always `Value`), so a
/// property over identity mismatches would add generation machinery without
/// covering a case this example misses. Revisit when a second `kind` exists.
#[test]
fn conflicting_registration_is_rejected() -> Result<()> {
    // The two descriptors share the cell kind (there is only one) but carry
    // different codec ids: a Kafka descriptor under a name already registered
    // as a JSON value.
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&cart(), CollectionDef::new(None))?;
    let conflict = registry.register(&message_state("cart"), CollectionDef::new(None));
    assert!(matches!(
        conflict,
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    Ok(())
}

/// N5: re-registering the same name with an *unchanged* identity is
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
        CollectionDef::new(Some(updated_ttl)).with_commit_mode(CommitMode::ReadUncommitted),
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

/// N5: an empty descriptor name fails loudly at registration — the
/// fallible boundary backing the infallible `const fn value_state`.
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
