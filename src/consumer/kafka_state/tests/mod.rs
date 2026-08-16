//! Kafka-message keyed-state tests: the [`MessageRef`] serde round-trip
//! property and the end-to-end resolve-through-loader path.
//!
//! These bind through the *same* [`bind_registered`] machinery the JSON
//! descriptor tests use — the one-binding-path proof that the Kafka-message
//! descriptor is just a `ValueDescriptor<MessageCell<L>>` over a composed
//! cell type, with no bespoke binding path.

use super::*;
use crate::Key;
use crate::codec::Codec;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::Direction;
use crate::state::descriptor::tests::{TestSession, bind_registered};
use crate::state::descriptor::{DequeHandle, DescriptorIdentity, MapHandle};
use crate::state::order_codec::Utf8KeyCodec;
use bytes::BytesMut;
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
        let mut borrowed = Vec::new();
        codec.serialize_ref(&message_ref, &mut borrowed).is_ok()
            && borrowed == buf
            && codec
                .deserialize(&mut buf.clone())
                .is_ok_and(|decoded| decoded == message_ref)
            && codec
                .deserialize_owned(BytesMut::from(buf.as_slice()))
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
mod message_cell_in_every_kind;
