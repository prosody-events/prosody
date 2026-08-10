//! Kafka-message keyed-state tests: the [`MessageRef`] serde round-trip
//! property and the end-to-end resolve-through-loader path.
//!
//! These bind through the *same* [`bind_registered`] machinery the JSON
//! descriptor tests use — the one-binding-path proof that the Kafka-message
//! descriptor is just a `ValueDescriptor<MessageCell<L>>` over a composed
//! cell type, with no bespoke binding path.

use super::*;
use crate::Key;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::Direction;
use crate::state::descriptor::tests::{TestSession, bind_registered};
use crate::state::descriptor::{DequeHandle, DescriptorIdentity, MapHandle};
use crate::state::order_codec::Utf8KeyCodec;
use color_eyre::eyre::{Result, bail, eyre};
use futures::StreamExt;
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

const TOPIC_POOL: &[&str] = &[
    "orders.v1",
    "billing.events",
    "telemetry",
    "shipments.outbound",
];

fn last_seen() -> MessageDescriptor<MemoryLoader<Value>> {
    message_state("last_seen")
}

/// Wire-format freeze: the Kafka-message collection's codec and resolver tokens
/// are written into the durable `keyed_state_identity` row and compared on
/// read. Changing either orphans every existing Kafka-message collection
/// (mismatch ⇒ Permanent), so pin both literals — a rename fails loudly here.
#[test]
fn kafka_message_identity_tokens_are_frozen() {
    use crate::codec::Codec;
    use crate::state::descriptor::CellResolver;

    assert_eq!(MessageRefCodec::FORMAT_ID, "message-ref");
    assert_eq!(
        <MessageResolver<MemoryLoader<Value>> as CellResolver>::RESOLVER_ID,
        Some("message-ref")
    );
}

#[derive(Clone, Debug)]
struct ArbMessageRef(MessageRef);

impl Arbitrary for ArbMessageRef {
    fn arbitrary(g: &mut Gen) -> Self {
        let topic_name = g.choose(TOPIC_POOL).copied().unwrap_or(TOPIC_POOL[0]);
        Self(MessageRef {
            topic: Topic::from(topic_name),
            partition: i32::arbitrary(g),
            offset: i64::arbitrary(g),
        })
    }
}

fn coords() -> (Topic, i32, i64) {
    (Topic::from("orders.v1"), 3, 42)
}

fn message_for_testing(payload: Value) -> Result<ConsumerMessage<Value>> {
    let (topic, partition, offset) = coords();
    let key: Key = Arc::from("user-1");
    ConsumerMessage::for_testing(topic, partition, offset, key, payload)
}

/// The [`MessageRefCodec`] round-trips every [`MessageRef`] exactly, driven
/// through the same `serialize`/`deserialize` calls the cell store uses —
/// not a raw `rmp_serde` call standing in for the codec.
#[test]
fn prop_kafka_message_ref_msgpack_roundtrip() {
    fn prop(message_ref: ArbMessageRef) -> bool {
        let ArbMessageRef(message_ref) = message_ref;
        let mut codec = MessageRefCodec;
        let mut buf = Vec::new();
        let Ok(()) = codec.serialize(message_ref.clone(), &mut buf) else {
            return false;
        };
        codec
            .deserialize(&mut buf)
            .is_ok_and(|decoded| decoded == message_ref)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbMessageRef) -> bool);
}

/// The [`MessageRefCodec`] wire encoding of a fixed [`MessageRef`] is
/// frozen: a `MsgPack` named-map of `topic`/`partition`/`offset`. The
/// round-trip property above proves `decode(encode(x)) == x` inside one
/// binary, which survives an encoding change as long as encoder and decoder
/// move together; this test pins the literal bytes so a wire-incompatible
/// change is caught even though both sides of the round-trip still agree
/// with each other.
#[test]
fn frozen_message_ref_bytes() -> Result<()> {
    let value = MessageRef {
        topic: Topic::from("orders.v1"),
        partition: 3,
        offset: 42,
    };
    let mut codec = MessageRefCodec;
    let mut buf = Vec::new();
    codec
        .serialize(value.clone(), &mut buf)
        .map_err(|error| eyre!("encode failed: {error}"))?;

    #[rustfmt::skip]
    let expected: Vec<u8> = vec![
        0x83, // fixmap, 3 entries
        0xa5, b't', b'o', b'p', b'i', b'c',                         // "topic"
        0xa9, b'o', b'r', b'd', b'e', b'r', b's', b'.', b'v', b'1', // "orders.v1"
        0xa9, b'p', b'a', b'r', b't', b'i', b't', b'i', b'o', b'n', // "partition"
        0x03,                                                       // 3
        0xa6, b'o', b'f', b'f', b's', b'e', b't',                   // "offset"
        0x2a,                                                       // 42
    ];
    assert_eq!(buf, expected);

    let decoded = codec
        .deserialize(&mut buf.clone())
        .map_err(|error| eyre!("decode failed: {error}"))?;
    assert_eq!(decoded, value);
    Ok(())
}

/// The ref derived from a message carries the message's exact Kafka
/// coordinates — `set(&message)` persists precisely what `get()` resolves.
#[test]
fn ref_from_message_carries_coordinates() -> Result<()> {
    let (topic, partition, offset) = coords();
    let message = message_for_testing(json!(1_i32))?;
    let message_ref = MessageRef::from(&message);
    assert_eq!(message_ref.topic, topic);
    assert_eq!(message_ref.partition, partition);
    assert_eq!(message_ref.offset, offset);
    Ok(())
}

/// The descriptor's cell is the ref derived from the message in hand;
/// `get()` resolves it through the message loader to the full
/// `ConsumerMessage` with matching coordinates and payload.
#[tokio::test]
async fn kafka_descriptor_set_then_get_loads_full_message() -> Result<()> {
    let (topic, partition, offset) = coords();
    let key: Key = Arc::from("user-1");
    let payload = json!({"order": 7_i32});
    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, key, payload.clone());

    let handle = bind_registered(last_seen(), loader)?;
    handle.set(&message_for_testing(payload.clone())?).await?;

    let message = handle
        .get()
        .await?
        .ok_or_else(|| eyre!("expected a resolved message"))?;
    assert_eq!(message.topic(), topic);
    assert_eq!(message.partition(), partition);
    assert_eq!(message.offset(), offset);
    assert_eq!(message.record().message(), Some(&payload));
    Ok(())
}

/// A vanished Kafka body (deleted/compacted offset) surfaces as a Permanent
/// loader error from `get()` — never `None`, never Terminal — so the row is
/// skipped, not retried and not fatal.
#[tokio::test]
async fn kafka_descriptor_deleted_offset_is_permanent() -> Result<()> {
    let (topic, partition, offset) = coords();
    let loader = MemoryLoader::<Value>::new();
    loader.store_message(topic, partition, offset, Arc::from("user-1"), json!(1_i32));

    let handle = bind_registered(last_seen(), loader.clone())?;
    handle.set(&message_for_testing(json!(1_i32))?).await?;
    loader.remove_message(topic, partition, offset);

    let Err(error) = handle.get().await else {
        return Err(eyre!("deleted offset must error, not return Ok"));
    };
    assert!(matches!(
        error,
        MessageStateError::Access(StateAccessError::Load { .. })
    ));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// An absent cell reads as `Ok(None)` without consulting the loader.
#[tokio::test]
async fn kafka_descriptor_absent_cell_returns_none() -> Result<()> {
    let handle = bind_registered(last_seen(), MemoryLoader::<Value>::new())?;
    assert!(handle.get().await?.is_none());
    Ok(())
}

/// State resolution never waits for capacity held by an earlier resolved
/// value. Saturation is a transient error.
#[tokio::test]
async fn kafka_state_capacity_exhaustion_never_waits() -> Result<()> {
    let (topic, partition, offset) = coords();
    let loader = MemoryLoader::with_capacity(1);
    loader.store_message(topic, partition, offset, Arc::from("user-1"), json!(1_i32));

    let handle = bind_registered(last_seen(), loader)?;
    handle.set(&message_for_testing(json!(1_i32))?).await?;
    let _first = handle
        .get()
        .await?
        .ok_or_else(|| eyre!("expected the first resolved message"))?;

    let error = timeout(Duration::from_millis(100), handle.get())
        .await
        .map_err(|_| eyre!("state resolution waited for loader capacity"))?
        .err()
        .ok_or_else(|| eyre!("state resolution must fail while capacity is held"))?;
    assert_eq!(error.classify_error(), ErrorCategory::Transient);
    Ok(())
}

/// The Kafka-message cell is an ordinary [`CellType`], so it works in **every**
/// collection kind — the release-blocker guarantee. These properties drive a
/// [`MapHandle`]/[`DequeHandle`] over message cells against a `BTreeMap`/
/// `VecDeque` model of the underlying message ids, resolving each stored
/// [`MessageRef`] back through a shared loader; the identity test pins that the
/// durable identity is the same message identity regardless of kind.
mod message_cell_in_every_kind {
    use super::*;

    /// The topic every seeded message shares; the id doubles as the offset, so
    /// a message is fully identified by its id.
    const TOPIC: &str = "orders.v1";

    /// The bounded key pool the map trace ranges over.
    const MAP_KEYS: &[&str] = &["a", "b", "c", "d"];

    /// Seeds the loader with the message identified by `id` (offset `id`,
    /// payload `id`) and returns the [`ConsumerMessage`] a handle writes.
    fn seed_message(loader: &MemoryLoader<Value>, id: i64) -> Result<ConsumerMessage<Value>> {
        let topic = Topic::from(TOPIC);
        let key: Key = Arc::from("k");
        let payload = json!(id);
        loader.store_message(topic, 0, id, key.clone(), payload.clone());
        ConsumerMessage::for_testing(topic, 0, id, key, payload)
    }

    /// Whether a resolved cell matches the model's message id: a `Some` cell
    /// must resolve to the full message at that offset with the matching
    /// payload, and absence must agree.
    fn matches_model(resolved: Option<&ConsumerMessage<Value>>, expected: Option<i64>) -> bool {
        match (resolved, expected) {
            (None, None) => true,
            (Some(message), Some(id)) => {
                message.offset() == id && message.record().message() == Some(&json!(id))
            }
            _ => false,
        }
    }

    /// The map key for a generated index.
    fn map_key(index: u8) -> String {
        MAP_KEYS[index as usize % MAP_KEYS.len()].to_owned()
    }

    /// Collects a message-map handle's `stream(dir)` into `(key, offset)`
    /// pairs.
    async fn collect_map(
        handle: &MapHandle<TestSession, Utf8KeyCodec, MessageCell<MemoryLoader<Value>>>,
        dir: Direction,
    ) -> Result<Vec<(String, i64)>> {
        let mut out = Vec::new();
        let stream = handle.stream(dir);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            let (key, message) = item?;
            out.push((key, message.offset()));
        }
        Ok(out)
    }

    /// Collects a message-deque handle's `stream(dir)` into element offsets.
    async fn collect_deque(
        handle: &DequeHandle<TestSession, MessageCell<MemoryLoader<Value>>>,
        dir: Direction,
    ) -> Result<Vec<i64>> {
        let mut out = Vec::new();
        let stream = handle.stream(dir);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            out.push(item?.offset());
        }
        Ok(out)
    }

    /// One map mutation or mid-trace read over the message-map handle.
    #[derive(Clone, Copy, Debug)]
    enum MsgMapOp {
        Set(u8, u8),
        Remove(u8),
        Get(u8),
    }

    impl Arbitrary for MsgMapOp {
        fn arbitrary(g: &mut Gen) -> Self {
            match u8::arbitrary(g) % 4 {
                0 | 1 => Self::Set(u8::arbitrary(g), u8::arbitrary(g)),
                2 => Self::Remove(u8::arbitrary(g)),
                _ => Self::Get(u8::arbitrary(g)),
            }
        }
    }

    /// One deque mutation over the message-deque handle.
    #[derive(Clone, Copy, Debug)]
    enum MsgDequeOp {
        PushBack(u8),
        PushFront(u8),
        PopBack,
        PopFront,
    }

    impl Arbitrary for MsgDequeOp {
        fn arbitrary(g: &mut Gen) -> Self {
            match u8::arbitrary(g) % 4 {
                0 => Self::PushBack(u8::arbitrary(g)),
                1 => Self::PushFront(u8::arbitrary(g)),
                2 => Self::PopBack,
                _ => Self::PopFront,
            }
        }
    }

    /// Drives a message-map trace against a `BTreeMap<String, i64>` model of
    /// the stored ids, resolving each cell through the shared loader;
    /// asserts every mid-trace `get`, the final per-key read-back, and both
    /// stream directions.
    async fn run_msg_map(ops: Vec<MsgMapOp>) -> Result<bool> {
        let loader = MemoryLoader::<Value>::new();
        let handle = bind_registered(
            message_map_state::<Utf8KeyCodec, MemoryLoader<Value>>("msg_map"),
            loader.clone(),
        )?;
        let mut model: BTreeMap<String, i64> = BTreeMap::new();

        for op in ops {
            match op {
                MsgMapOp::Set(key_index, id) => {
                    let key = map_key(key_index);
                    let id = i64::from(id);
                    let message = seed_message(&loader, id)?;
                    handle.set(key.clone(), &message).await?;
                    model.insert(key, id);
                }
                MsgMapOp::Remove(key_index) => {
                    let key = map_key(key_index);
                    handle.remove(&key).await?;
                    model.remove(&key);
                }
                MsgMapOp::Get(key_index) => {
                    let key = map_key(key_index);
                    if !matches_model(handle.get(&key).await?.as_ref(), model.get(&key).copied()) {
                        return Ok(false);
                    }
                }
            }
        }

        for name in MAP_KEYS {
            let resolved = handle.get(&(*name).to_owned()).await?;
            if !matches_model(resolved.as_ref(), model.get(*name).copied()) {
                return Ok(false);
            }
        }

        let ascending: Vec<(String, i64)> = model.iter().map(|(k, v)| (k.clone(), *v)).collect();
        if collect_map(&handle, Direction::Forward).await? != ascending {
            return Ok(false);
        }
        let descending: Vec<(String, i64)> =
            model.iter().rev().map(|(k, v)| (k.clone(), *v)).collect();
        Ok(collect_map(&handle, Direction::Backward).await? == descending)
    }

    /// Drives a message-deque trace against a `VecDeque<i64>` model of the
    /// stored ids; asserts every `pop` return, `len`, positional `get`, and
    /// both stream directions.
    async fn run_msg_deque(ops: Vec<MsgDequeOp>) -> Result<bool> {
        let loader = MemoryLoader::<Value>::new();
        let handle = bind_registered(
            message_deque_state::<MemoryLoader<Value>>("msg_deque"),
            loader.clone(),
        )?;
        let mut model: VecDeque<i64> = VecDeque::new();

        for op in ops {
            match op {
                MsgDequeOp::PushBack(id) => {
                    let id = i64::from(id);
                    let message = seed_message(&loader, id)?;
                    handle.push_back(&message).await?;
                    model.push_back(id);
                }
                MsgDequeOp::PushFront(id) => {
                    let id = i64::from(id);
                    let message = seed_message(&loader, id)?;
                    handle.push_front(&message).await?;
                    model.push_front(id);
                }
                MsgDequeOp::PopBack => {
                    if !matches_model(handle.pop_back().await?.as_ref(), model.pop_back()) {
                        return Ok(false);
                    }
                }
                MsgDequeOp::PopFront => {
                    if !matches_model(handle.pop_front().await?.as_ref(), model.pop_front()) {
                        return Ok(false);
                    }
                }
            }
        }

        if handle.len().await? != model.len() {
            return Ok(false);
        }
        for index in 0..model.len() + 2 {
            if !matches_model(handle.get(index).await?.as_ref(), model.get(index).copied()) {
                return Ok(false);
            }
        }
        let forward: Vec<i64> = model.iter().copied().collect();
        if collect_deque(&handle, Direction::Forward).await? != forward {
            return Ok(false);
        }
        let backward: Vec<i64> = model.iter().rev().copied().collect();
        Ok(collect_deque(&handle, Direction::Backward).await? == backward)
    }

    /// Converts a property body's `Result<bool>` into a `TestResult`, surfacing
    /// the offending trace on failure.
    fn finish(result: Result<bool>, label: &str, input: &str) -> TestResult {
        match result {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error(format!("{label}: {input}")),
            Err(error) => TestResult::error(format!("{label}: {input}: {error:#}")),
        }
    }

    /// A message cell in a Map behaves exactly like a `BTreeMap` keyed by the
    /// user key, valued by the resolved Kafka message.
    #[test]
    fn prop_message_cell_in_map_tracks_btreemap() {
        fn prop(ops: Vec<MsgMapOp>) -> TestResult {
            let input = format!("{ops:?}");
            finish(block_on(run_msg_map(ops)), "message-map divergence", &input)
        }
        QuickCheck::new().quickcheck(prop as fn(Vec<MsgMapOp>) -> TestResult);
    }

    /// A message cell in a Deque behaves exactly like a `VecDeque` of resolved
    /// Kafka messages.
    #[test]
    fn prop_message_cell_in_deque_tracks_vecdeque() {
        fn prop(ops: Vec<MsgDequeOp>) -> TestResult {
            let input = format!("{ops:?}");
            finish(
                block_on(run_msg_deque(ops)),
                "message-deque divergence",
                &input,
            )
        }
        QuickCheck::new().quickcheck(prop as fn(Vec<MsgDequeOp>) -> TestResult);
    }

    /// A pop whose message resolution fails (deleted/compacted offset) errors
    /// *before* any mutation is buffered — the resolve-before-mutate guarantee
    /// documented on the pop ops. The failed element stays at its end at the
    /// same length, and pops normally once the message is restored.
    #[tokio::test]
    async fn deque_pop_resolve_failure_leaves_deque_unmutated() -> Result<()> {
        let loader = MemoryLoader::<Value>::new();
        let handle = bind_registered(
            message_deque_state::<MemoryLoader<Value>>("msg_deque"),
            loader.clone(),
        )?;

        let keep = seed_message(&loader, 1)?;
        let vanish = seed_message(&loader, 2)?;
        handle.push_back(&keep).await?;
        handle.push_back(&vanish).await?;
        loader.remove_message(Topic::from(TOPIC), 0, 2);

        let Err(error) = handle.pop_back().await else {
            bail!("popping a vanished message must error");
        };
        assert_eq!(error.classify_error(), ErrorCategory::Permanent);

        // Unmutated: same length, the vanished element still at the back.
        assert_eq!(handle.len().await?, 2);
        loader.store_message(Topic::from(TOPIC), 0, 2, Arc::from("k"), json!(2_i64));
        if !matches_model(handle.pop_back().await?.as_ref(), Some(2)) {
            bail!("restored element must pop with its original id");
        }
        assert_eq!(handle.len().await?, 1);
        Ok(())
    }

    /// The message cell carries the same durable identity — codec
    /// `"message-ref"`, resolver `Some("message-ref")` — in every kind;
    /// only the key axis varies with the kind. This is what lets
    /// cross-language consumers share a message collection regardless of
    /// the kind it is stored in.
    #[test]
    fn message_cell_carries_message_identity_in_every_kind() {
        let map: MapDescriptor<Utf8KeyCodec, MessageCell<MemoryLoader<Value>>> =
            message_map_state("m");
        let map_id = map.structural_identity();
        assert_eq!(map_id.format_id, "message-ref");
        assert_eq!(map_id.resolver_id, Some("message-ref"));
        assert_eq!(map_id.key_format_id, "utf8.v1");

        let deque: DequeDescriptor<MessageCell<MemoryLoader<Value>>> = message_deque_state("d");
        let deque_id = deque.structural_identity();
        assert_eq!(deque_id.format_id, "message-ref");
        assert_eq!(deque_id.resolver_id, Some("message-ref"));
        assert_eq!(deque_id.key_format_id, "i64.v1");

        let value: MessageDescriptor<MemoryLoader<Value>> = message_state("v");
        let value_id = value.structural_identity();
        assert_eq!(value_id.format_id, "message-ref");
        assert_eq!(value_id.resolver_id, Some("message-ref"));
        assert_eq!(value_id.key_format_id, "unit.v1");
    }
}
