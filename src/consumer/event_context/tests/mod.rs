//! Erased-vs-typed parity and pins for the [`DynEventContext`] keyed-state FFI
//! seam.
//!
//! The seven vend methods must use the typed `ctx.state(...)` path with
//! the codec recovered from the payload and the name resolved at runtime. The
//! flagship properties drive a random trace of by-name ops through the boxed
//! erased handles and assert agreement, after **every** op, against an
//! in-memory model (the strong oracle) — for both FFI payload erasures
//! (`serde_json::Value` and [`BinaryPayload`]) and all four collection kinds.
//! Because `<P as ErasedStateCodec>::Codec` *is* the codec the typed path uses,
//! `erased == typed` is structural; the model catches any encode/decode
//! corruption or name-resolution bug. Cursor laziness, the null-write
//! rejection, the never-`Terminal` fold, and the unregistered-name
//! classification are pinned alongside; duplicate-name is a registry-level pin
//! in `state/descriptor/tests.rs`.

use super::{
    BoxDequeState, BoxMapState, DequeScanConfig, DynEventContext, ErasedCategory, ErasedStateError,
    EventContext, KeyScanConfig, StateCursor,
};
use crate::codec::{BinaryPayload, ErasedStateCodec, JsonCodec};
use crate::consumer::kafka_state::{message_deque_state, message_map_state, message_state};
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::cell_key::Direction;
use crate::state::descriptor::tests::{TestBackend, test_session, test_session_for};
use crate::state::descriptor::{
    Registered, StateDescriptor, deque_state, map_state, set_state, value_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::store::CELL_BATCH;
use crate::state::tests::support::{CountingCellStore, FixedOracle};
use crate::state::{EventRef, PartitionBackend, StateKey};
use crate::test_util::ArbJson;
use crate::timers::duration::CompactDuration;
use crate::{Key, Topic};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult, empty_shrinker};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::iter::once;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

/// One collection name per kind — distinct, since a name is unique within a
/// state type and the three kinds assert different structural identities.
/// Name-resolution is pinned by the unregistered-name test.
const VALUE_NAME: &str = "v";
const MAP_NAME: &str = "m";
const SET_NAME: &str = "s";
const DEQUE_NAME: &str = "d";

/// A small key pool for map traces, so keys collide and re-use.
const KEYS: &[&str] = &["a", "b", "c"];

/// Cap on generated trace length, keeping property runs bounded.
const MAX_OPS: usize = 30;

/// The session type the parity suites drive, generic over the payload the
/// loader carries.
type ParitySession<P> = KeyedStateSession<TestBackend, MemoryLoader<P>>;

/// A payload the erased seam exposes, plus the test-only knobs the parity
/// suites need: a non-null sample generator and byte/structural equality.
trait ParityPayload: ErasedStateCodec + Clone {
    /// A storable sample — never the JSON-null absent sentinel (which the seam
    /// rejects).
    fn arbitrary_value(g: &mut Gen) -> Self;

    /// The JSON-null absent sentinel, which `set`/`push` must reject.
    fn null_value() -> Self;

    /// Observational equality (structural for `Value`, byte-wise for
    /// `BinaryPayload`).
    fn same(a: &Self, b: &Self) -> bool;
}

/// A non-null `serde_json::Value` sample.
fn arb_non_null(g: &mut Gen) -> Value {
    match ArbJson::arbitrary(g).0 {
        Value::Null => Value::Bool(true),
        other => other,
    }
}

impl ParityPayload for Value {
    fn arbitrary_value(g: &mut Gen) -> Self {
        arb_non_null(g)
    }

    fn null_value() -> Self {
        Value::Null
    }

    fn same(a: &Self, b: &Self) -> bool {
        a == b
    }
}

impl ParityPayload for BinaryPayload {
    fn arbitrary_value(g: &mut Gen) -> Self {
        // JSON-document bytes of a non-null value; `to_vec` cannot fail for the
        // float-free `ArbJson` domain.
        let bytes = serde_json::to_vec(&arb_non_null(g)).unwrap_or_default();
        BinaryPayload::new(bytes, None::<String>, None::<String>)
    }

    fn null_value() -> Self {
        BinaryPayload::new(b"null".to_vec(), None::<String>, None::<String>)
    }

    fn same(a: &Self, b: &Self) -> bool {
        a.bytes == b.bytes
    }
}

/// Builds a memory-backed context over the payload `P` with the pooled
/// value/map/deque collections registered under `P`'s recovered codec.
fn parity_context<P>() -> Result<MockEventContext<P, ParitySession<P>>>
where
    P: ParityPayload + Send + Sync + 'static,
{
    let mut registry = CollectionDefRegistry::default();
    registry.register(
        &value_state::<<P as ErasedStateCodec>::Codec>(VALUE_NAME),
        CollectionDef::new(None),
    )?;
    registry.register(
        &map_state::<Utf8KeyCodec, <P as ErasedStateCodec>::Codec>(MAP_NAME),
        CollectionDef::new(None),
    )?;
    registry.register(
        &deque_state::<<P as ErasedStateCodec>::Codec>(DEQUE_NAME),
        CollectionDef::new(None),
    )?;
    registry.register(
        &set_state::<Utf8KeyCodec>(SET_NAME),
        CollectionDef::new(None),
    )?;
    let session = test_session_for(MemoryLoader::<P>::new(), registry);
    Ok(MockEventContext::<P>::new().with_session(session))
}

/// Drains a scan cursor fully into a `Vec`, surfacing any scan error. Shared by
/// the map and deque parity runners, whose scan checks differ only in how they
/// compare the drained entries against the model.
async fn drain_cursor<T>(cursor: &StateCursor<T>) -> Result<Vec<T>> {
    let mut items = Vec::new();
    while let Some(item) = cursor.next().await.map_err(|e| eyre!("scan: {e}"))? {
        items.push(item);
    }
    Ok(items)
}

/// Compares two optional payloads observationally.
fn opt_same<P: ParityPayload>(a: Option<&P>, b: Option<&P>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => P::same(a, b),
        _ => false,
    }
}

/// Sweeps the pooled `KEYS` through the erased handle, asserting `get` and
/// `contains_key` both agree with `visible` for every key.
async fn assert_keys_visible<P: ParityPayload>(
    handle: &BoxMapState<P>,
    visible: &BTreeMap<String, P>,
) -> Result<bool> {
    for key in KEYS {
        let owned = (*key).to_owned();
        let erased = handle
            .get(owned.clone())
            .await
            .map_err(|e| eyre!("get: {e}"))?;
        let present = handle
            .contains_key(owned)
            .await
            .map_err(|e| eyre!("has: {e}"))?;
        if !opt_same::<P>(erased.as_ref(), visible.get(*key))
            || present != visible.contains_key(*key)
        {
            return Ok(false);
        }
    }
    Ok(handle
        .is_empty()
        .await
        .map_err(|e| eyre!("is_empty: {e}"))?
        == visible.is_empty())
}

// --- Value parity -----------------------------------------------------------

/// One value op against the erased seam.
#[derive(Clone, Debug)]
enum ValueOp {
    Get,
    Set,
    Clear,
    Commit,
    Rollback,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => ValueOp::Get,
            1 => ValueOp::Set,
            2 => ValueOp::Clear,
            3 => ValueOp::Commit,
            _ => ValueOp::Rollback,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            ValueOp::Set => Box::new(once(ValueOp::Clear)),
            _ => empty_shrinker(),
        }
    }
}

#[derive(Clone, Debug)]
struct ValueTrace(Vec<ValueOp>);

impl Arbitrary for ValueTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            Vec::<ValueOp>::arbitrary(g)
                .into_iter()
                .take(MAX_OPS)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(ValueTrace))
    }
}

/// Drives a value trace through the erased handle and a `(floor, visible)`
/// model, asserting `erased == typed == visible` after every op. `floor` is the
/// last durably committed value; `visible` is the read-your-writes value.
/// `set`/`clear` move only `visible`; `commit` promotes `visible` to `floor`;
/// `rollback` reverts `visible` to `floor`. Commit/rollback are issued through
/// the **erased** handle only — the typed handle shares the same overlay, so
/// calling its commit would mask a no-op erased commit. `set` values come from
/// a fresh generator per op so the property covers varied payloads.
fn run_value_parity<P>(ops: &[ValueOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx
            .value_state(VALUE_NAME)
            .map_err(|e| eyre!("vend value: {e}"))?;
        let mut floor: Option<P> = None;
        let mut visible: Option<P> = None;
        let mut sampler = Gen::new(8);
        for op in ops {
            match op {
                ValueOp::Get => {}
                ValueOp::Set => {
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .set(value.clone())
                        .await
                        .map_err(|e| eyre!("erased set: {e}"))?;
                    visible = Some(value);
                }
                ValueOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased clear: {e}"))?;
                    visible = None;
                }
                ValueOp::Commit => {
                    handle
                        .commit()
                        .await
                        .map_err(|e| eyre!("erased commit: {e}"))?;
                    floor = visible.clone();
                }
                ValueOp::Rollback => {
                    handle.rollback().await;
                    visible = floor.clone();
                }
            }
            let erased = handle.get().await.map_err(|e| eyre!("erased get: {e}"))?;
            let typed = ctx
                .state(Registered::new(
                    value_state::<<P as ErasedStateCodec>::Codec>(VALUE_NAME),
                ))
                .map_err(|e| eyre!("typed bind: {e}"))?
                .get()
                .await
                .map_err(|e| eyre!("typed get: {e}"))?;
            if !opt_same::<P>(erased.as_ref(), typed.as_ref())
                || !opt_same::<P>(erased.as_ref(), visible.as_ref())
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased value parity for the js/py/rb payload (`serde_json::Value`).
#[test]
fn prop_erased_value_parity_json() {
    fn prop(ValueTrace(ops): ValueTrace) -> TestResult {
        match run_value_parity::<Value>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("value parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("value trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ValueTrace) -> TestResult);
}

/// Erased value parity for the cs payload (`BinaryPayload`, verbatim JSON
/// bytes).
#[test]
fn prop_erased_value_parity_binary() {
    fn prop(ValueTrace(ops): ValueTrace) -> TestResult {
        match run_value_parity::<BinaryPayload>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("binary value parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("binary value trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(ValueTrace) -> TestResult);
}

mod deque_parity;
mod map_parity;
mod seams;
mod set_parity;
