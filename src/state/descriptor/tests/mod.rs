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
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::cell_key::Direction;
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::{I64KeyCodec, Utf8KeyCodec};
use crate::state::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CellStore;
use crate::state::{CommitMode, EventRef, PartitionBackend, StateKey, StateName, StateType};
use crate::state::{DequeQuery, KeyQuery};
use crate::test_util::{ArbJson, TEST_RUNTIME, captured_spans, named};
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::{Result, eyre};
use futures::TryStreamExt;
use opentelemetry_sdk::trace::SpanData;
use quickcheck::{QuickCheck, TestResult};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

// Test contexts share one deduplication store type.
pub(crate) use crate::state::tests::support::MemoryDeduplicationStore;

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
) -> (TestSession, MemoryCellStore) {
    let (parts, cell_store) = session_parts(loader, registry, state_key, false);
    (KeyedStateSession::new(parts), cell_store)
}

/// The partition backend every test-session fixture in this module shares: the
/// memory cell store resolving through a get-out-of-the-way
/// [`MemoryDeduplicationStore`].
pub(crate) type TestBackend =
    PartitionBackend<MemoryDeduplicationStore, MemoryDescriptorIdentityStore, MemoryCellStore, ()>;

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
        false,
    );
    KeyedStateSession::new(parts)
}

/// Assembles the [`SessionParts`] shared by every test-session fixture — a
/// fresh memory cell store, the registry, loader, and state key.
/// When `cancelled`, the per-event cancellation
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
    cancelled: bool,
) -> (SessionParts<TestBackend, L>, MemoryCellStore) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(cancelled);
    let registry = Arc::new(registry);
    let cell_store = MemoryCellStore::new(MemoryCells::new());
    let parts = SessionParts {
        cell: cell_store.clone(),
        dirty: Arc::new(DirtyStore::new()),
        dedup: MemoryDeduplicationStore::new(),
        loader,
        registry,
        state_key,
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        dedup_ttl: CompactDuration::new(30),
        checks: (),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
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
    let (parts, _cells) = session_parts(loader, registry, state_key, false);
    let dirty = parts.dirty.clone();
    (KeyedStateSession::new(parts), dirty)
}

/// A test session over an arbitrary cell store `C` — the twin of
/// [`TestSession`], whose store is pinned to the plain memory one.
pub(crate) type SessionOver<C> = KeyedStateSession<
    PartitionBackend<MemoryDeduplicationStore, MemoryDescriptorIdentityStore, C, ()>,
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
        dedup: MemoryDeduplicationStore::new(),
        loader,
        registry: Arc::new(registry),
        state_key,
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        dedup_ttl: CompactDuration::new(30),
        checks: (),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    })
}

mod scope;

mod values;
use values::*;
mod lifecycle;
mod registration;
mod telemetry;
