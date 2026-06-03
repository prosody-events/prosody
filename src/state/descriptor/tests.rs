//! Descriptor binding, registration, and typed round-trip tests.
//!
//! N2 — the typed `set(T) → cell bytes → store → get() → T` round-trip over
//! the real [`TransactionValueStore`] machinery, run against both the memory
//! and fjall dirty backends. N4 — the two-instance interface proof: the JSON
//! and Kafka descriptors bind through the *same* `ctx.state(DESC)` machinery
//! ([`bind_registered`]). N5 — registration and bind error surfaces.

use super::*;
use crate::consumer::middleware::test_support::MockEventContext;
use crate::state::fjall::FjallDirtyValueStore;
use crate::state::memory::{MemoryDirtyValueStore, MemoryDurableValueStore};
use crate::state::middleware::{
    CollectionDef, CollectionDefRegistry, ContextParts, DirtyValueBundle, KeyedStateContext,
    RegisterStateError, TimerScope,
};
use crate::state::value_test_suite::finish_trace;
use crate::state::{EventRef, EventScopeId, StateKey};
use crate::test_util::{ArbJson, TEST_RUNTIME};
use color_eyre::eyre::{Result, eyre};
use fjall::{Config, PartitionCreateOptions};
use futures::executor;
use quickcheck::{QuickCheck, TestResult};
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

pub(crate) type TestCtx<S, L> =
    KeyedStateContext<MockEventContext, MemoryDurableValueStore, S, L, TimerScope>;

/// Builds a context with `descriptor` registered and binds it — the single
/// shared machinery every descriptor kind runs through (the N4 proof is
/// that both the JSON tests here and the Kafka tests in
/// [`super::kafka::tests`] call exactly this).
pub(crate) fn bind_registered<DESC, S, L>(
    descriptor: DESC,
    dirty: S,
    loader: L,
) -> Result<DESC::Handle>
where
    DESC: StateDescriptor<TestCtx<S, L>> + DescriptorIdentity + Copy,
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
    L: Clone,
{
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&descriptor, CollectionDef::new(None))?;
    let ctx = test_context(dirty, loader, registry);
    ctx.state(descriptor).map_err(|e| eyre!("bind failed: {e}"))
}

fn test_context<S, L>(dirty: S, loader: L, registry: CollectionDefRegistry) -> TestCtx<S, L>
where
    S: Clone,
{
    KeyedStateContext::new(ContextParts {
        inner: MockEventContext::new(),
        durable: MemoryDurableValueStore::for_tests(),
        dirty,
        loader,
        scope: TimerScope,
        registry: Arc::new(registry),
        state_key: StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
    })
}

fn fjall_dirty() -> Result<(TempDir, FjallDirtyValueStore)> {
    let dir = tempfile::tempdir()?;
    let keyspace = Config::new(dir.path()).open()?;
    let overlay =
        keyspace.open_partition("value_dirty_overlay", PartitionCreateOptions::default())?;
    Ok((
        dir,
        FjallDirtyValueStore::new(overlay, EventScopeId::fresh()),
    ))
}

const CART: ValueDescriptor<Value> = value_state("cart");

/// N2 invariant: for every JSON-representable `T`, `set(v)` then `get()`
/// returns `Some(v)` — the value survives the full
/// `T → codec → cell bytes → store → cell bytes → codec → T` path through
/// the real transaction substrate.
async fn roundtrip<S>(dirty: S, value: Value) -> Result<bool>
where
    S: DirtyValueBundle + fmt::Debug + Send + Sync + 'static,
{
    let handle = bind_registered(CART, dirty, NoLoader)?;
    handle.set(value.clone()).await?;
    Ok(handle.get().await? == Some(value))
}

#[test]
fn prop_descriptor_set_get_roundtrip_memory() {
    fn prop(value: ArbJson) -> TestResult {
        let input_dbg = format!("{value:#?}");
        let result = executor::block_on(roundtrip(MemoryDirtyValueStore::new(), value.0));
        finish_trace(result, "typed roundtrip lost", &input_dbg)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

#[test]
fn prop_descriptor_set_get_roundtrip_fjall() {
    fn prop(value: ArbJson) -> TestResult {
        let input_dbg = format!("{value:#?}");
        let result = fjall_dirty().and_then(|(_dir, dirty)| {
            // `_dir` lives until the round-trip completes.
            TEST_RUNTIME.block_on(roundtrip(dirty, value.0))
        });
        finish_trace(result, "typed roundtrip lost", &input_dbg)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbJson) -> TestResult);
}

/// A never-written collection reads as `None`.
#[tokio::test]
async fn descriptor_get_absent_returns_none() -> Result<()> {
    let handle = bind_registered(CART, MemoryDirtyValueStore::new(), NoLoader)?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// `set` then `clear` reads as `None`.
#[tokio::test]
async fn descriptor_clear_then_get_none() -> Result<()> {
    let handle = bind_registered(CART, MemoryDirtyValueStore::new(), NoLoader)?;
    handle.set(json!({"items": [1_i32, 2_i32]})).await?;
    handle.clear().await?;
    assert_eq!(handle.get().await?, None);
    Ok(())
}

/// N5: binding an unregistered descriptor fails with a Permanent
/// [`BindError::Unregistered`] — access requires prior registration.
#[tokio::test]
async fn state_with_unregistered_descriptor_errors() -> Result<()> {
    let ctx = test_context(
        MemoryDirtyValueStore::new(),
        NoLoader,
        CollectionDefRegistry::new(None),
    );
    let Err(error) = ctx.state(CART) else {
        return Err(eyre!("unregistered bind must fail"));
    };
    assert!(matches!(error, BindError::Unregistered { name: "cart" }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// N5: binding a descriptor whose identity differs from the registered one
/// fails with [`BindError::IdentityMismatch`].
#[tokio::test]
async fn bind_with_mismatched_identity_errors() -> Result<()> {
    const RELABELED: ValueDescriptor<Value> = value_state("cart").with_schema_label("v2");
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&CART, CollectionDef::new(None))?;
    let ctx = test_context(MemoryDirtyValueStore::new(), NoLoader, registry);

    let Err(error) = ctx.state(RELABELED) else {
        return Err(eyre!("mismatched bind must fail"));
    };
    assert!(matches!(error, BindError::IdentityMismatch { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// N5: re-registering the same name with a *different* structural identity
/// is rejected, whichever field differs (cell kind, codec id, schema label).
#[test]
fn conflicting_registration_is_rejected() -> Result<()> {
    // Different cell kind + codec id: a Kafka descriptor under a name
    // registered as a JSON value.
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&CART, CollectionDef::new(None))?;
    let conflict = registry.register(&kafka_message_state("cart"), CollectionDef::new(None));
    assert!(matches!(
        conflict,
        Err(RegisterStateError::IdentityConflict { .. })
    ));

    // Different schema label only.
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&CART, CollectionDef::new(None))?;
    let relabeled: ValueDescriptor<Value> = value_state("cart").with_schema_label("v2");
    let conflict = registry.register(&relabeled, CollectionDef::new(None));
    assert!(matches!(
        conflict,
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    Ok(())
}

/// N5: identical re-registration is idempotent (operational settings may
/// be updated; the identity is unchanged).
#[test]
fn identical_reregistration_ok() -> Result<()> {
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&CART, CollectionDef::new(None))?;
    registry.register(&CART, CollectionDef::new(None))?;
    Ok(())
}

/// N5: an empty descriptor name fails loudly at registration — the
/// fallible boundary backing the infallible `const fn value_state`.
#[test]
fn empty_name_rejected_at_registration() {
    let mut registry = CollectionDefRegistry::new(None);
    let result = registry.register(&value_state::<Value>(""), CollectionDef::new(None));
    assert!(matches!(result, Err(RegisterStateError::Name(_))));
}
