//! Kafka-message keyed-state tests: the [`KafkaMessageRef`] serde round-trip
//! property and the end-to-end resolve-through-loader path.
//!
//! These bind through the *same* [`bind_registered`] machinery the JSON
//! descriptor tests use — the N4 proof that the Kafka-message descriptor is
//! just a `ValueDescriptor<C, R>` over a codec + resolver, with no bespoke
//! binding path.

use super::*;
use crate::Key;
use crate::consumer::event_context::StateAccessError;
use crate::consumer::message::ConsumerMessage;
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MemoryLoader;
use crate::state::descriptor::tests::bind_registered;
use crate::state::memory::MemoryDirtyValueStore;
use color_eyre::eyre::{Result, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::{Value, json};
use std::sync::Arc;

const TOPIC_POOL: &[&str] = &[
    "orders.v1",
    "billing.events",
    "telemetry",
    "shipments.outbound",
];

fn last_seen() -> KafkaMessageDescriptor {
    kafka_message_state("last_seen")
}

#[derive(Clone, Debug)]
struct ArbKafkaMessageRef(KafkaMessageRef);

impl Arbitrary for ArbKafkaMessageRef {
    fn arbitrary(g: &mut Gen) -> Self {
        let topic_name = g.choose(TOPIC_POOL).copied().unwrap_or(TOPIC_POOL[0]);
        Self(KafkaMessageRef {
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

/// The `MsgPack` serde of [`KafkaMessageRef`] round-trips exactly — this is
/// the [`KafkaRefCodec`] cell format.
#[test]
fn prop_kafka_message_ref_msgpack_roundtrip() {
    fn prop(message_ref: ArbKafkaMessageRef) -> bool {
        let ArbKafkaMessageRef(message_ref) = message_ref;
        let Ok(cell) = rmp_serde::to_vec_named(&message_ref) else {
            return false;
        };
        rmp_serde::from_slice::<KafkaMessageRef>(&cell).is_ok_and(|decoded| decoded == message_ref)
    }
    QuickCheck::new().quickcheck(prop as fn(ArbKafkaMessageRef) -> bool);
}

/// The ref derived from a message carries the message's exact Kafka
/// coordinates — `set(&message)` persists precisely what `get()` resolves.
#[test]
fn ref_from_message_carries_coordinates() -> Result<()> {
    let (topic, partition, offset) = coords();
    let message = message_for_testing(json!(1_i32))?;
    let message_ref = KafkaMessageRef::from(&message);
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

    let handle = bind_registered(last_seen(), MemoryDirtyValueStore::new(), loader)?;
    handle.set(&message_for_testing(payload.clone())?).await?;

    let message = handle
        .get()
        .await?
        .ok_or_else(|| eyre!("expected a resolved message"))?;
    assert_eq!(message.topic(), topic);
    assert_eq!(message.partition(), partition);
    assert_eq!(message.offset(), offset);
    assert_eq!(*message.payload(), payload);
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

    let handle = bind_registered(last_seen(), MemoryDirtyValueStore::new(), loader.clone())?;
    handle.set(&message_for_testing(json!(1_i32))?).await?;
    loader.remove_message(topic, partition, offset);

    let Err(error) = handle.get().await else {
        return Err(eyre!("deleted offset must error, not return Ok"));
    };
    assert!(matches!(
        error,
        KafkaStateError::Access(StateAccessError::Load { .. })
    ));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// An absent cell reads as `Ok(None)` without consulting the loader.
#[tokio::test]
async fn kafka_descriptor_absent_cell_returns_none() -> Result<()> {
    let handle = bind_registered(
        last_seen(),
        MemoryDirtyValueStore::new(),
        MemoryLoader::<Value>::new(),
    )?;
    assert!(handle.get().await?.is_none());
    Ok(())
}
