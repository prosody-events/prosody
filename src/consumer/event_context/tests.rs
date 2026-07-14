//! Erased-vs-typed parity and pins for the [`DynEventContext`] keyed-state FFI
//! seam.
//!
//! The six vend methods must be *exactly* the typed `ctx.state(...)` path with
//! the codec recovered from the payload and the name resolved at runtime. The
//! flagship properties drive a random trace of by-name ops through the boxed
//! erased handles and assert agreement, after **every** op, against an
//! in-memory model (the strong oracle) — for both FFI payload erasures
//! (`serde_json::Value` and [`BinaryPayload`]) and all three collection kinds.
//! Because `<P as ErasedStateCodec>::Codec` *is* the codec the typed path uses,
//! `erased == typed` is structural; the model catches any encode/decode
//! corruption or name-resolution bug. Cursor laziness, the null-write
//! rejection, the never-`Terminal` fold, and the unregistered/duplicate-name
//! classifications are pinned alongside.

use super::{DynEventContext, ErasedCategory, ErasedStateError, EventContext};
use crate::codec::{BinaryPayload, ErasedStateCodec, JsonCodec};
use crate::consumer::kafka_state::message_state;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::cell_key::Direction;
use crate::state::descriptor::tests::{TestBackend, test_session, test_session_for};
use crate::state::descriptor::{
    Registered, STREAM_CHUNK, StateDescriptor, deque_state, map_state, value_state,
};
use crate::state::dirty::DirtyStore;
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::session::{KeyedStateSession, SessionParts, TerminationWatch};
use crate::state::tests::support::{CountingCellStore, FixedOracle};
use crate::state::{EventRef, PartitionBackend, StateKey};
use crate::test_util::ArbJson;
use crate::timers::duration::CompactDuration;
use crate::{Key, Topic};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult, empty_shrinker};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::iter::once;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

/// One collection name per kind — distinct, since a name is unique within a
/// state type and the three kinds assert different structural identities.
/// Name-resolution is pinned by the unregistered-name test.
const VALUE_NAME: &str = "v";
const MAP_NAME: &str = "m";
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
    let mut registry = CollectionDefRegistry::new(None);
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
    let session = test_session_for(MemoryLoader::<P>::new(), registry);
    Ok(MockEventContext::<P>::new().with_session(session))
}

/// Compares two optional payloads observationally.
fn opt_same<P: ParityPayload>(a: Option<&P>, b: Option<&P>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => P::same(a, b),
        _ => false,
    }
}

// --- Value parity -----------------------------------------------------------

/// One value op against the erased seam.
#[derive(Clone, Debug)]
enum ValueOp {
    Get,
    Set,
    Clear,
}

impl Arbitrary for ValueOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => ValueOp::Get,
            1 => ValueOp::Set,
            _ => ValueOp::Clear,
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

/// Drives a value trace through the erased handle and an `Option<P>` model,
/// asserting `erased == typed == model` after every op. `set` values come from
/// a fresh generator per op (seeded off the op index) so the property covers
/// varied payloads without threading them through the trace.
fn run_value_parity<P>(ops: &[ValueOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx
            .value_state(VALUE_NAME)
            .map_err(|e| eyre!("vend value: {e}"))?;
        let mut model: Option<P> = None;
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
                    model = Some(value);
                }
                ValueOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased clear: {e}"))?;
                    model = None;
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
                || !opt_same::<P>(erased.as_ref(), model.as_ref())
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

// --- Map parity -------------------------------------------------------------

/// One map op against the erased seam.
#[derive(Clone, Debug)]
enum MapOp {
    Get(usize),
    Set(usize),
    Remove(usize),
    Clear,
    Scan,
}

impl MapOp {
    fn key(idx: usize) -> String {
        KEYS[idx % KEYS.len()].to_owned()
    }
}

impl Arbitrary for MapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 5 {
            0 => MapOp::Get(usize::arbitrary(g)),
            1 => MapOp::Set(usize::arbitrary(g)),
            2 => MapOp::Remove(usize::arbitrary(g)),
            3 => MapOp::Clear,
            _ => MapOp::Scan,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            MapOp::Set(i) => Box::new(once(MapOp::Remove(*i))),
            _ => empty_shrinker(),
        }
    }
}

#[derive(Clone, Debug)]
struct MapTrace(Vec<MapOp>);

impl Arbitrary for MapTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            Vec::<MapOp>::arbitrary(g)
                .into_iter()
                .take(MAX_OPS)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(MapTrace))
    }
}

/// Drives a map trace through the erased handle and a `BTreeMap` model,
/// asserting after every op that each pooled key reads equal and a full
/// forward scan yields exactly the model's key-ordered entries.
fn run_map_parity<P>(ops: &[MapOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx.map_state(MAP_NAME).map_err(|e| eyre!("vend map: {e}"))?;
        let mut model: BTreeMap<String, P> = BTreeMap::new();
        let mut sampler = Gen::new(8);
        for op in ops {
            match op {
                MapOp::Scan => {}
                MapOp::Get(i) => {
                    // Exercise the specific-key read path; the after-op sweep
                    // below verifies its result against the model.
                    let key = MapOp::key(*i);
                    let erased = handle
                        .get(key.clone())
                        .await
                        .map_err(|e| eyre!("erased map get: {e}"))?;
                    if !opt_same::<P>(erased.as_ref(), model.get(&key)) {
                        return Ok(false);
                    }
                }
                MapOp::Set(i) => {
                    let key = MapOp::key(*i);
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .set(key.clone(), value.clone())
                        .await
                        .map_err(|e| eyre!("erased map set: {e}"))?;
                    model.insert(key, value);
                }
                MapOp::Remove(i) => {
                    let key = MapOp::key(*i);
                    handle
                        .remove(key.clone())
                        .await
                        .map_err(|e| eyre!("erased map remove: {e}"))?;
                    model.remove(&key);
                }
                MapOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased map clear: {e}"))?;
                    model.clear();
                }
            }
            for key in KEYS {
                let erased = handle
                    .get((*key).to_owned())
                    .await
                    .map_err(|e| eyre!("erased map get: {e}"))?;
                if !opt_same::<P>(erased.as_ref(), model.get(*key)) {
                    return Ok(false);
                }
            }
            // A forward scan must yield exactly the model's key-ordered entries.
            let cursor = handle.scan(Direction::Forward);
            let mut scanned: Vec<(String, P)> = Vec::new();
            while let Some(entry) = cursor.next().await.map_err(|e| eyre!("map scan: {e}"))? {
                scanned.push(entry);
            }
            let expected: Vec<(String, P)> =
                model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            if scanned.len() != expected.len()
                || scanned
                    .iter()
                    .zip(&expected)
                    .any(|((sk, sv), (ek, ev))| sk != ek || !P::same(sv, ev))
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased map parity for `serde_json::Value`.
#[test]
fn prop_erased_map_parity_json() {
    fn prop(MapTrace(ops): MapTrace) -> TestResult {
        match run_map_parity::<Value>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("map parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("map trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MapTrace) -> TestResult);
}

/// Erased map parity for `BinaryPayload`.
#[test]
fn prop_erased_map_parity_binary() {
    fn prop(MapTrace(ops): MapTrace) -> TestResult {
        match run_map_parity::<BinaryPayload>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("binary map parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("binary map trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(MapTrace) -> TestResult);
}

// --- Deque parity -----------------------------------------------------------

/// One deque op against the erased seam.
#[derive(Clone, Debug)]
enum DequeOp {
    PushBack,
    PushFront,
    PopFront,
    PopBack,
    Clear,
    Scan,
}

impl Arbitrary for DequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 6 {
            0 => DequeOp::PushBack,
            1 => DequeOp::PushFront,
            2 => DequeOp::PopFront,
            3 => DequeOp::PopBack,
            4 => DequeOp::Clear,
            _ => DequeOp::Scan,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            DequeOp::PushBack | DequeOp::PushFront => Box::new(once(DequeOp::PopFront)),
            _ => empty_shrinker(),
        }
    }
}

#[derive(Clone, Debug)]
struct DequeTrace(Vec<DequeOp>);

impl Arbitrary for DequeTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(
            Vec::<DequeOp>::arbitrary(g)
                .into_iter()
                .take(MAX_OPS)
                .collect(),
        )
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(DequeTrace))
    }
}

/// Drives a deque trace through the erased handle and a `VecDeque` model,
/// asserting `len`, every positional `get`, and a full forward scan agree
/// after every op.
fn run_deque_parity<P>(ops: &[DequeOp]) -> Result<bool>
where
    P: ParityPayload + Send + Sync + 'static,
{
    executor::block_on(async {
        let ctx = parity_context::<P>()?;
        let handle = ctx
            .deque_state(DEQUE_NAME)
            .map_err(|e| eyre!("vend deque: {e}"))?;
        let mut model: VecDeque<P> = VecDeque::new();
        let mut sampler = Gen::new(8);
        for op in ops {
            match op {
                DequeOp::Scan => {}
                DequeOp::PushBack => {
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .push_back(value.clone())
                        .await
                        .map_err(|e| eyre!("erased push_back: {e}"))?;
                    model.push_back(value);
                }
                DequeOp::PushFront => {
                    let value = P::arbitrary_value(&mut sampler);
                    handle
                        .push_front(value.clone())
                        .await
                        .map_err(|e| eyre!("erased push_front: {e}"))?;
                    model.push_front(value);
                }
                DequeOp::PopFront => {
                    let erased = handle
                        .pop_front()
                        .await
                        .map_err(|e| eyre!("pop_front: {e}"))?;
                    let expected = model.pop_front();
                    if !opt_same::<P>(erased.as_ref(), expected.as_ref()) {
                        return Ok(false);
                    }
                }
                DequeOp::PopBack => {
                    let erased = handle
                        .pop_back()
                        .await
                        .map_err(|e| eyre!("pop_back: {e}"))?;
                    let expected = model.pop_back();
                    if !opt_same::<P>(erased.as_ref(), expected.as_ref()) {
                        return Ok(false);
                    }
                }
                DequeOp::Clear => {
                    handle
                        .clear()
                        .await
                        .map_err(|e| eyre!("erased deque clear: {e}"))?;
                    model.clear();
                }
            }
            let len = handle.len().await.map_err(|e| eyre!("deque len: {e}"))?;
            if len != model.len()
                || handle
                    .is_empty()
                    .await
                    .map_err(|e| eyre!("is_empty: {e}"))?
                    != model.is_empty()
            {
                return Ok(false);
            }
            for index in 0..model.len() {
                let erased = handle
                    .get(index)
                    .await
                    .map_err(|e| eyre!("deque get: {e}"))?;
                if !opt_same::<P>(erased.as_ref(), model.get(index)) {
                    return Ok(false);
                }
            }
            let cursor = handle.scan(Direction::Forward);
            let mut scanned: Vec<P> = Vec::new();
            while let Some(item) = cursor.next().await.map_err(|e| eyre!("deque scan: {e}"))? {
                scanned.push(item);
            }
            if scanned.len() != model.len()
                || scanned
                    .iter()
                    .zip(model.iter())
                    .any(|(a, b)| !P::same(a, b))
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
}

/// Erased deque parity for `serde_json::Value`.
#[test]
fn prop_erased_deque_parity_json() {
    fn prop(DequeTrace(ops): DequeTrace) -> TestResult {
        match run_deque_parity::<Value>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("deque parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("deque trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(DequeTrace) -> TestResult);
}

/// Erased deque parity for `BinaryPayload`.
#[test]
fn prop_erased_deque_parity_binary() {
    fn prop(DequeTrace(ops): DequeTrace) -> TestResult {
        match run_deque_parity::<BinaryPayload>(&ops) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("binary deque parity diverged: {ops:?}")),
            Err(error) => TestResult::error(format!("binary deque trace errored: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(DequeTrace) -> TestResult);
}

// --- Kafka message seam -----------------------------------------------------

/// The erased Kafka-message value ops mirror the typed `MessageDescriptor`
/// path: `message_value_state(..).set(message)` records the message in hand and
/// `.get()` resolves it back to the full [`ConsumerMessage`] through the
/// loader.
#[tokio::test]
async fn erased_kafka_record_then_get_matches_typed() -> Result<()> {
    let topic = Topic::from("orders.v1");
    let (partition, offset) = (3_i32, 42_i64);
    let key: Key = Arc::from("user-1");
    let payload = json!({ "order": 7_i32 });

    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());

    let mut registry = CollectionDefRegistry::new(None);
    registry.register(
        &message_state::<MemoryLoader<Value>>("last_seen"),
        CollectionDef::new(None),
    )?;
    let session = test_session(loader, registry);
    let ctx = MockEventContext::<Value>::new().with_session(session);

    let message = ConsumerMessage::for_testing(topic, partition, offset, key, payload.clone())?;

    ctx.message_value_state("last_seen")
        .map_err(|e| eyre!("vend message value: {e}"))?
        .set(message)
        .await
        .map_err(|e| eyre!("erased record: {e}"))?;
    let erased = ctx
        .message_value_state("last_seen")
        .map_err(|e| eyre!("vend message value: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("erased get: {e}"))?
        .ok_or_else(|| eyre!("erased get resolved nothing"))?;
    assert_eq!(erased.topic(), topic);
    assert_eq!(erased.partition(), partition);
    assert_eq!(erased.offset(), offset);
    assert_eq!(*erased.payload(), payload);

    let typed = ctx
        .state(Registered::new(message_state("last_seen")))
        .map_err(|e| eyre!("typed kafka bind: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("typed kafka get: {e}"))?
        .ok_or_else(|| eyre!("typed get resolved nothing"))?;
    assert_eq!(typed.offset(), erased.offset());
    assert_eq!(*typed.payload(), *erased.payload());
    Ok(())
}

// --- Object safety / cloneability -------------------------------------------

/// The erased seam is the FFI deliverable, so `Box<dyn DynEventContext<Payload
/// = P>>` must construct (object safety), be callable, and clone into an alias
/// that shares the same per-event session.
#[tokio::test]
async fn dyn_event_context_state_is_object_safe_and_cloneable() -> Result<()> {
    let erased: Box<dyn DynEventContext<Payload = Value>> = Box::new(parity_context::<Value>()?);
    let alias = erased.clone();
    erased
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend through trait object: {e}"))?
        .set(json!({ "x": 1_i32 }))
        .await
        .map_err(|e| eyre!("set through trait object: {e}"))?;
    let observed = alias
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend through cloned trait object: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("get through cloned trait object: {e}"))?;
    assert_eq!(observed, Some(json!({ "x": 1_i32 })));
    Ok(())
}

// --- Registration classification --------------------------------------------

/// An unregistered name fails vending with a Permanent classification — a wrong
/// collection name is business logic, never retried. The compile-time
/// capability handle cannot express this (the erased seam mints its own token
/// by name), so the access-time check is the backstop.
#[tokio::test]
async fn erased_unregistered_name_is_permanent() -> Result<()> {
    let ctx = parity_context::<Value>()?;
    let Err(error) = ctx.value_state("never-registered") else {
        return Err(eyre!("an unregistered name must fail vending"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

// --- Null-write rejection ---------------------------------------------------

/// The JSON-null absent sentinel is rejected on every value-family `set`/`push`
/// with a Permanent error, and the store is left untouched — for both payload
/// erasures. `clear`/`remove` express deletion instead.
async fn assert_null_rejected<P>() -> Result<()>
where
    P: ParityPayload + Send + Sync + 'static,
{
    let ctx = parity_context::<P>()?;

    // Seed a prior value so "store untouched" is observable as the survivor.
    let seed = {
        let mut g = Gen::new(8);
        P::arbitrary_value(&mut g)
    };

    let value = ctx
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend value: {e}"))?;
    value
        .set(seed.clone())
        .await
        .map_err(|e| eyre!("seed set: {e}"))?;
    let Err(error) = value.set(P::null_value()).await else {
        return Err(eyre!("null value set must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    let after = value.get().await.map_err(|e| eyre!("value get: {e}"))?;
    if !opt_same::<P>(after.as_ref(), Some(&seed)) {
        return Err(eyre!("a rejected null set must leave the cell untouched"));
    }

    let map = ctx.map_state(MAP_NAME).map_err(|e| eyre!("vend map: {e}"))?;
    let Err(error) = map.set("k".to_owned(), P::null_value()).await else {
        return Err(eyre!("null map set must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert!(
        map.get("k".to_owned())
            .await
            .map_err(|e| eyre!("map get: {e}"))?
            .is_none(),
        "a rejected null map set must not insert the key"
    );

    let deque = ctx
        .deque_state(DEQUE_NAME)
        .map_err(|e| eyre!("vend deque: {e}"))?;
    let Err(error) = deque.push_back(P::null_value()).await else {
        return Err(eyre!("null push_back must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    let Err(error) = deque.push_front(P::null_value()).await else {
        return Err(eyre!("null push_front must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    assert_eq!(
        deque.len().await.map_err(|e| eyre!("deque len: {e}"))?,
        0,
        "a rejected null push must not extend the deque"
    );
    Ok(())
}

/// Null-write rejection for `serde_json::Value` (`Value::Null`).
#[tokio::test]
async fn erased_null_write_rejected_json() -> Result<()> {
    assert_null_rejected::<Value>().await
}

/// Null-write rejection for `BinaryPayload` (the literal `null` document).
#[tokio::test]
async fn erased_null_write_rejected_binary() -> Result<()> {
    assert_null_rejected::<BinaryPayload>().await
}

/// The C# byte path also rejects a whitespace-padded `null` document.
#[tokio::test]
async fn erased_null_write_rejected_binary_padded() -> Result<()> {
    let ctx = parity_context::<BinaryPayload>()?;
    let padded = BinaryPayload::new(b"  null\n".to_vec(), None::<String>, None::<String>);
    let Err(error) = ctx
        .value_state(VALUE_NAME)
        .map_err(|e| eyre!("vend value: {e}"))?
        .set(padded)
        .await
    else {
        return Err(eyre!("a padded null document must be rejected"));
    };
    assert_eq!(error.category(), ErasedCategory::Permanent);
    Ok(())
}

// --- Never-Terminal fold ----------------------------------------------------

/// A synthetic error classifying `Terminal`, to pin the boundary fold.
#[derive(Debug, Error)]
#[error("synthetic terminal error")]
struct SyntheticTerminal;

impl ClassifyError for SyntheticTerminal {
    fn classify_error(&self) -> ErrorCategory {
        ErrorCategory::Terminal
    }
}

/// The single boundary fold maps a lower-layer `Terminal` to `Transient` and
/// preserves `Permanent`/`Transient` — the state layer never surfaces
/// `Terminal`. `ErasedCategory` has no `Terminal` variant, so the folded error
/// is structurally never `Terminal`. Falsify: map `Terminal => Permanent` in
/// [`ErasedStateError::from_classified`] and this observes Permanent.
#[test]
fn never_terminal_fold() {
    let folded = ErasedStateError::from_classified(&SyntheticTerminal);
    assert_eq!(folded.category(), ErasedCategory::Transient);
    assert_eq!(folded.classify_error(), ErrorCategory::Transient);
}

// --- Cursor laziness (against a counting store) -----------------------------

/// The read-counting cell store the cursor-laziness pin drives.
type CountingStore = CountingCellStore<MemoryCellStore<FixedOracle>>;

/// The backend the cursor-laziness pin drives: a memory cell store wrapped in a
/// read-counting decorator, so a single `next()`'s durable reads are bounded.
type CountingBackend = PartitionBackend<FixedOracle, MemoryDescriptorIdentityStore, CountingStore>;

/// The context the cursor-laziness pin drives.
type CountingContext =
    MockEventContext<Value, KeyedStateSession<CountingBackend, MemoryLoader<Value>>>;

/// Builds a counting-store-backed context (map `MAP_NAME` registered), returning
/// the context and a clone of the counting store to read its `get` counter.
fn counting_context(registry: CollectionDefRegistry) -> (CountingContext, CountingStore) {
    let registry = Arc::new(registry);
    let counting = CountingCellStore::new(MemoryCellStore::new(
        MemoryCells::new(),
        FixedOracle::committed(),
        registry.clone(),
    ));
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let parts = SessionParts::<CountingBackend, _> {
        cell: counting.clone(),
        dirty: Arc::new(DirtyStore::new()),
        oracle: FixedOracle::committed(),
        loader: MemoryLoader::<Value>::new(),
        registry,
        state_key: StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        event: EventRef::Message {
            dedup_id: Uuid::new_v4(),
        },
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
    };
    let ctx = MockEventContext::<Value>::new().with_session(KeyedStateSession::new(parts));
    (ctx, counting)
}

/// A map cursor is demand-driven: one `next()` issues at most one chunk's worth
/// of durable point reads (plus the single keyset read), never the whole
/// collection. Falsify: eagerly drain the whole typed stream inside the scan
/// generator before yielding — the first `next()`'s durable reads then equal
/// the full seeded size.
#[tokio::test]
async fn map_cursor_is_lazy() -> Result<()> {
    // Seed enough entries that a full drain far exceeds one chunk.
    let entries = STREAM_CHUNK * 3;
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(
        &map_state::<Utf8KeyCodec, JsonCodec>(MAP_NAME),
        CollectionDef::new(None),
    )?;
    let (ctx, counting) = counting_context(registry);

    let map = ctx.map_state(MAP_NAME).map_err(|e| eyre!("vend map: {e}"))?;
    for i in 0..entries {
        map.set(format!("k{i:04}"), json!(i))
            .await
            .map_err(|e| eyre!("seed set: {e}"))?;
    }
    // Commit so the entries are durable committed cells the scan re-reads.
    map.commit().await.map_err(|e| eyre!("commit: {e}"))?;

    counting.reset();
    let cursor = ctx
        .map_state(MAP_NAME)
        .map_err(|e| eyre!("vend map: {e}"))?
        .scan(Direction::Forward);
    let first = cursor.next().await.map_err(|e| eyre!("first next: {e}"))?;
    assert!(first.is_some(), "the seeded map must yield a first entry");

    let reads = counting.lower_reads();
    // One keyset read plus at most one chunk of point reads.
    assert!(
        reads <= STREAM_CHUNK + 1,
        "one next() read {reads} cells; expected <= one chunk ({}) plus the keyset",
        STREAM_CHUNK + 1
    );
    assert!(
        reads < entries,
        "one next() must not drain all {entries} entries ({reads} reads)"
    );
    Ok(())
}

// --- Registration fluent-option survival ------------------------------------

/// The fluent options a binding sets on a descriptor (`ttl`,
/// `read_uncommitted`, map `keyset_limit`) thread through registration
/// unchanged — the erased seam adds no new registration surface. Confirms the
/// operational def a client registers is the one the registry holds.
#[test]
fn erased_registration_options_thread_through() -> Result<()> {
    use crate::state::registry::CommitMode;
    use crate::state::{StateName, StateType};

    let ttl = CompactDuration::new(3_600);
    let mut registry = CollectionDefRegistry::new(None);
    registry.register(
        &value_state::<JsonCodec>("v").ttl(ttl).read_uncommitted(),
        // The config layer derives the def from the descriptor's fluent
        // settings; here the descriptor carries them directly.
        value_state::<JsonCodec>("v")
            .ttl(ttl)
            .read_uncommitted()
            .collection_def(),
    )?;
    registry.register(
        &map_state::<Utf8KeyCodec, JsonCodec>("m").keyset_limit(7),
        map_state::<Utf8KeyCodec, JsonCodec>("m")
            .keyset_limit(7)
            .collection_def(),
    )?;

    let v = StateName::try_new("v")?;
    let m = StateName::try_new("m")?;
    assert_eq!(registry.ttl_for(StateType::Application, &v), Some(ttl));
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &v),
        CommitMode::ReadUncommitted
    );
    assert_eq!(registry.keyset_limit_for(StateType::Application, &m), 7);
    Ok(())
}
