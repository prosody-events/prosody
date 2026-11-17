//! What publishing a collection makes discoverable: the routing row in
//! `keyed_state_publication`, and reading the committed value back the way a
//! different consumer group would — through a standalone [`StateReader`] over
//! the production read path.

use crate::cart::{LAST_SEEN, RECEIPT, cart};
use crate::common;
use color_eyre::eyre::{Result, ensure, eyre};
use prosody::cassandra::CassandraStore;
use prosody::consumer::{ConsumerConfiguration, KeyedStateConfiguration, message_state};
use prosody::loader::KafkaLoader;
use prosody::state::cassandra::{CassandraPublicationStore, PublicationQueries};
use prosody::state::publication::PublicationStore;
use prosody::state::{StateName, StateType};
use prosody::state_reader::{CassandraReaderBackend, StateReaderClient, StateReaderDependencies};
use prosody::subsystem::SubsystemName;
use prosody::{ByteSize, JsonCodec, Topic};
use serde_json::json;
use std::num::NonZeroU64;
use std::sync::Arc;

/// The reader cache budget. This test never exercises eviction; the reader just
/// requires a non-zero budget.
const READER_CACHE_BYTES: u64 = 1_048_576;

/// Reads the `keyed_state_publication` table directly. Asserts exactly one
/// routing row for `group_id` under each published collection the handler
/// writes, `cart` and `receipt`. Each row carries `topic` and the topic's
/// partition count. Then asserts the private `last_seen` collection has no row
/// at all. The check reads the real table.
pub(crate) async fn assert_routing_row(
    subsystem: &SubsystemName,
    group_id: &str,
    topic: Topic,
) -> Result<()> {
    let publication_store = publication_store().await?;

    for published in ["cart", RECEIPT] {
        let name = StateName::try_new(published)?;
        let own: Vec<_> = publication_store
            .read_publications(subsystem, StateType::Application, &name)
            .await?
            .into_iter()
            .filter(|r| r.group_id.as_ref() == group_id)
            .map(|r| (r.topic, i32::from(r.partition_count)))
            .collect();
        ensure!(
            own == [(topic, 1_i32)],
            "{published} must have one row for the writing topic with count 1, got {own:?}"
        );
    }

    let private = StateName::try_new(LAST_SEEN)?;
    ensure!(
        publication_store
            .read_publications(subsystem, StateType::Application, &private)
            .await?
            .is_empty(),
        "a private collection must never write a routing row"
    );
    Ok(())
}

/// Opens a store over the `keyed_state_publication` table in the shared test
/// keyspace, so a test can check routing rows directly.
pub(crate) async fn publication_store() -> Result<CassandraPublicationStore> {
    let store = CassandraStore::new(&common::test_cassandra_config()).await?;
    let queries = Arc::new(PublicationQueries::new(store.session(), common::TEST_KEYSPACE).await?);
    Ok(CassandraPublicationStore::new(store, queries))
}

/// Reads the published `cart` and `receipt` back through standalone
/// Cassandra-backed readers, exercising the full production read path:
/// `StateReaderDependencies::cassandra`, publication-source discovery,
/// frozen-identity validation against the reader's descriptor, probe-and-pin,
/// and the committed projection. Both reads must observe exactly what the
/// pipeline committed.
pub(crate) async fn read_cart_via_standalone_reader(
    subsystem: &SubsystemName,
    consumer_config: &ConsumerConfiguration,
    key: &str,
) -> Result<()> {
    // One `connect` opens the session, prepares the reader's queries, and builds
    // the Kafka loader. The plain `Value` read below never consults the loader;
    // the receipt read does.
    let keyed_state = KeyedStateConfiguration::builder()
        .read_cache_size(Some(ByteSize::new(
            NonZeroU64::new(READER_CACHE_BYTES)
                .ok_or_else(|| eyre!("reader cache budget must be positive"))?,
        )))
        .build()?;
    let deps = StateReaderDependencies::<JsonCodec, CassandraReaderBackend<JsonCodec>>::cassandra(
        consumer_config,
        &common::test_cassandra_config(),
        &keyed_state,
    )
    .await?;
    let reader = StateReaderClient::new(deps);

    let value = reader.state(subsystem.clone(), cart())?.get(key).await?;
    ensure!(
        value == Some(json!(["apple", "banana"])),
        "standalone reader must observe the committed cart, got {value:?}"
    );

    // The receipt is a Kafka-message cell, so this read goes through
    // `KafkaLoader`, which re-fetches the committed message ref's body
    // from Kafka. The reader binds the same message identity under its own
    // loader because the resolver id does not depend on the loader, so the
    // source discovered above serves the second message the consumer recorded.
    let receipt = reader
        .state(
            subsystem.clone(),
            message_state::<KafkaLoader<JsonCodec>>(RECEIPT),
        )?
        .get(key)
        .await?
        .ok_or_else(|| eyre!("standalone reader observed no published receipt"))?;
    ensure!(
        receipt.offset() == 1,
        "receipt must reference the second message's offset, got {}",
        receipt.offset()
    );
    ensure!(
        receipt.payload() == &json!({ "id": "evt-2", "item": "banana" }),
        "receipt must re-fetch the second message's body, got {:?}",
        receipt.payload()
    );
    Ok(())
}
