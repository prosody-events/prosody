//! Erased-vs-typed parity for the [`DynEventContext`] keyed-state FFI seam.
//!
//! The erased ops must be *exactly* the typed `ctx.state(...)` path with the
//! codec recovered from the payload and the name resolved at runtime. Because
//! `<Value as ErasedStateCodec>::Codec` *is* [`JsonCodec`], the erased and
//! typed reads reduce to the same expression — so the load-bearing assertion is
//! `erased == model`: a round-trip of the erased write/clear path against an
//! independent in-memory model, which catches any encode/decode corruption or
//! name-resolution bug on the erased side. (`erased == typed` is also checked,
//! but is near-tautological given the shared codec; a true two-backend
//! differential is unnecessary because the erased path structurally *is* the
//! typed path with the codec fixed.) The flagship property drives a random
//! trace of by-name ops and asserts agreement after **every** op, including
//! missing-key reads. The Kafka-message half, trait-object usage, and the
//! unregistered-name classification are pinned by example.

use super::{DynEventContext, EventContext};
use crate::consumer::kafka_state::kafka_message_state;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::tests::test_support::MockEventContext;
use crate::error::ErrorCategory;
use crate::loader::MemoryLoader;
use crate::state::descriptor::tests::{TestSession, test_session};
use crate::state::descriptor::{Registered, value_state};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::test_util::ArbJson;
use crate::{JsonCodec, Key, Topic};
use color_eyre::eyre::{Result, eyre};
use futures::executor;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult, empty_shrinker};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;

/// Small fixed name pool so the trace re-uses and collides on names.
const NAMES: &[&str] = &["cart", "wishlist", "tally"];

/// Cap on generated trace length, keeping property runs bounded.
const MAX_OPS: usize = 30;

/// One by-name value op against the erased seam (and its typed mirror).
#[derive(Clone, Debug)]
enum Op {
    /// Read a name (no state change) — exercises hits and missing-key misses.
    Get(usize),
    /// Write a value to a name.
    Set(usize, Value),
    /// Clear a name.
    Clear(usize),
}

impl Op {
    fn name(&self) -> &'static str {
        let idx = match self {
            Op::Get(i) | Op::Set(i, _) | Op::Clear(i) => *i,
        };
        NAMES[idx % NAMES.len()]
    }
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let idx = usize::arbitrary(g) % NAMES.len();
        match u8::arbitrary(g) % 3 {
            0 => Op::Get(idx),
            1 => Op::Set(idx, ArbJson::arbitrary(g).0),
            _ => Op::Clear(idx),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrinking a `Set` toward a `Clear` keeps the trace meaningful while
        // dropping payload complexity; the trace `Vec` shrink (element
        // removal) does the heavy lifting.
        match self {
            Op::Set(i, _) => {
                let i = *i;
                Box::new(once(Op::Clear(i)))
            }
            _ => empty_shrinker(),
        }
    }
}

/// A bounded trace of ops.
#[derive(Clone, Debug)]
struct Trace(Vec<Op>);

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Vec::<Op>::arbitrary(g).into_iter().take(MAX_OPS).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.0.shrink().map(Trace))
    }
}

/// Builds a registry with every pooled value collection registered.
fn value_registry() -> Result<CollectionDefRegistry> {
    let mut registry = CollectionDefRegistry::new(None);
    for name in NAMES {
        registry.register(&value_state::<JsonCodec>(name), CollectionDef::new(None))?;
    }
    Ok(registry)
}

/// Context over a real memory-backed session with the pooled value
/// collections registered.
fn value_context() -> Result<MockEventContext<Value, TestSession>> {
    let session = test_session(MemoryLoader::new(), value_registry()?);
    Ok(MockEventContext::<Value>::new().with_session(session))
}

/// Reads `name` through both the erased and typed paths and confirms they
/// equal each other and `expected`.
async fn assert_agreement(
    ctx: &MockEventContext<Value, TestSession>,
    name: &'static str,
    expected: Option<&Value>,
) -> Result<bool> {
    let erased = ctx
        .get_cell(name)
        .await
        .map_err(|e| eyre!("get_cell({name}): {e}"))?;
    let typed = ctx
        .state(Registered::new(value_state::<JsonCodec>(name)))
        .map_err(|e| eyre!("typed bind({name}): {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("typed get({name}): {e}"))?;
    Ok(erased == typed && erased.as_ref() == expected)
}

/// Flagship: the erased value seam tracks the typed path op-for-op, with an
/// in-memory model as the third witness, across a random trace.
#[test]
fn prop_erased_value_parity() {
    fn prop(Trace(ops): Trace) -> TestResult {
        let input = format!("{ops:?}");
        let result: Result<bool> = executor::block_on(async {
            let ctx = value_context()?;
            let mut model: HashMap<&'static str, Value> = HashMap::new();
            for op in &ops {
                let name = op.name();
                match op {
                    Op::Get(_) => {}
                    Op::Set(_, value) => {
                        ctx.set_cell(name, value.clone())
                            .await
                            .map_err(|e| eyre!("set_cell({name}): {e}"))?;
                        model.insert(name, value.clone());
                    }
                    Op::Clear(_) => {
                        ctx.clear_cell(name)
                            .await
                            .map_err(|e| eyre!("clear_cell({name}): {e}"))?;
                        model.remove(name);
                    }
                }
                // After every op, every name must agree across erased, typed,
                // and the model — including names never written (missing-key
                // reads return `None` on both paths).
                for name in NAMES {
                    if !assert_agreement(&ctx, name, model.get(name)).await? {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        });
        match result {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("erased/typed/model diverged: {input}")),
            Err(error) => TestResult::error(format!("trace errored: {input}: {error:#}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Trace) -> TestResult);
}

/// The erased Kafka-message ops mirror the typed `KafkaMessageDescriptor`
/// path: `record_message` stores the message in hand, and `get_message`
/// resolves it back to `(offset, payload)` through the loader — matching the
/// typed handle's `set`/`get`.
#[tokio::test]
async fn erased_kafka_record_then_get_matches_typed() -> Result<()> {
    let topic = Topic::from("orders.v1");
    let (partition, offset) = (3_i32, 42_i64);
    let key: Key = Arc::from("user-1");
    let payload = json!({ "order": 7_i32 });

    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key.clone(), payload.clone());

    let mut registry = CollectionDefRegistry::new(None);
    registry.register(&kafka_message_state("last_seen"), CollectionDef::new(None))?;
    let session = test_session(loader, registry);
    let ctx = MockEventContext::<Value>::new().with_session(session);

    let message = ConsumerMessage::for_testing(topic, partition, offset, key, payload.clone())?;

    // Erased record + erased resolve. `get_message` yields the full
    // `ConsumerMessage` — exactly what a binding wraps into its host `Message`
    // (topic, partition, offset, key, payload), not a lossy projection.
    ctx.record_message("last_seen", &message)
        .await
        .map_err(|e| eyre!("record_message: {e}"))?;
    let erased = ctx
        .get_message("last_seen")
        .await
        .map_err(|e| eyre!("get_message: {e}"))?
        .ok_or_else(|| eyre!("erased get_message resolved nothing"))?;
    assert_eq!(erased.topic(), topic);
    assert_eq!(erased.partition(), partition);
    assert_eq!(erased.offset(), offset);
    assert_eq!(*erased.payload(), payload);

    // Typed resolve over the same session must agree exactly.
    let typed = ctx
        .state(Registered::new(kafka_message_state("last_seen")))
        .map_err(|e| eyre!("typed kafka bind: {e}"))?
        .get()
        .await
        .map_err(|e| eyre!("typed kafka get: {e}"))?
        .ok_or_else(|| eyre!("typed get resolved nothing"))?;
    assert_eq!(typed.topic(), erased.topic());
    assert_eq!(typed.partition(), erased.partition());
    assert_eq!(typed.offset(), erased.offset());
    assert_eq!(*typed.payload(), *erased.payload());
    Ok(())
}

/// The erased seam is the FFI deliverable, so it must work as an actual trait
/// object: `Box<dyn DynEventContext<Payload = P>>` must construct (object
/// safety), be callable, and be clonable into an alias that shares the same
/// per-event session.
#[tokio::test]
async fn dyn_event_context_state_is_object_safe_and_cloneable() -> Result<()> {
    let erased: Box<dyn DynEventContext<Payload = Value>> = Box::new(value_context()?);
    // Cloning the boxed trait object yields an alias over the same Arc-backed
    // session, so a write through one is visible through the other.
    let alias = erased.clone();
    erased
        .set_cell("cart", json!({ "x": 1_i32 }))
        .await
        .map_err(|e| eyre!("set_cell through trait object: {e}"))?;
    let observed = alias
        .get_cell("cart")
        .await
        .map_err(|e| eyre!("get_cell through cloned trait object: {e}"))?;
    assert_eq!(observed, Some(json!({ "x": 1_i32 })));
    Ok(())
}

/// A name the consumer never registered fails the erased path with a
/// Permanent classification — a wrong collection name is business logic,
/// never retried. The compile-time capability handle cannot express this
/// (the erased seam mints its own token by name), so the access-time check
/// is the backstop, surfaced here through the boxed error.
#[tokio::test]
async fn erased_unregistered_name_is_permanent() -> Result<()> {
    let ctx = value_context()?;
    let Err(error) = ctx.get_cell("never-registered").await else {
        return Err(eyre!("an unregistered name must fail the erased read"));
    };
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}
