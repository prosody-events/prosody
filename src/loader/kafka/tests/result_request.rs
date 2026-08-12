//! A deferred message is re-read through this loader, so the destination a
//! record asked its response to go to survives a retry only when the loader
//! decodes under the same subsystem the poll loop was given. The message-defer
//! store persists offsets alone — never the record — so that reload is the
//! whole of "the result request round-trips message defer".

use super::{
    HeartbeatRegistry, JsonCodec, KafkaLoader, LoaderConfiguration, Offset, Topic, loader_config,
    producer, with_topic,
};
use crate::peer::response::RequestId;
use crate::peer::response::headers::{
    RESPONSE_AWAITED_HEADER, RESPONSE_DEADLINE_HEADER, RESPONSE_PEER_HEADER,
    RESPONSE_REQUEST_ID_HEADER, RESPONSE_VERSION_HEADER, RequestDeadline, ResultRequest,
};
use crate::peer::router::PeerId;
use crate::subsystem::SubsystemName;
use crate::tracing::init_test_logging;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::time::timeout;

/// The subsystem the loader under test answers for. It is deliberately the
/// **second** awaited name on the requested record, so a parse that stops at
/// the first awaited header loses the request.
const RESPONDER: &str = "billing";

#[tokio::test]
async fn a_reloaded_record_carries_its_result_request() -> color_eyre::Result<()> {
    let _ = color_eyre::install();
    init_test_logging();

    with_topic("result_request", async |topic_name| {
        let id = RequestId::new();
        let peer = PeerId::from_bytes([7; 16]);
        let (id_text, peer_text) = (id.to_string(), peer.to_string());
        let producer = producer()?;

        let requested_offset = produce(
            &producer,
            topic_name,
            "requested",
            OwnedHeaders::new()
                .insert(Header {
                    key: RESPONSE_VERSION_HEADER,
                    value: Some("2"),
                })
                .insert(Header {
                    key: RESPONSE_REQUEST_ID_HEADER,
                    value: Some(id_text.as_str()),
                })
                .insert(Header {
                    key: RESPONSE_PEER_HEADER,
                    value: Some(peer_text.as_str()),
                })
                .insert(Header {
                    key: RESPONSE_DEADLINE_HEADER,
                    value: Some("1700000000000000"),
                })
                .insert(Header {
                    key: RESPONSE_AWAITED_HEADER,
                    value: Some("ledger"),
                })
                .insert(Header {
                    key: RESPONSE_AWAITED_HEADER,
                    value: Some(RESPONDER),
                }),
        )
        .await?;
        let plain_offset = produce(&producer, topic_name, "plain", OwnedHeaders::new()).await?;
        let malformed_offset = produce(
            &producer,
            topic_name,
            "malformed",
            OwnedHeaders::new()
                .insert(Header {
                    key: RESPONSE_VERSION_HEADER,
                    value: Some("2"),
                })
                .insert(Header {
                    key: RESPONSE_VERSION_HEADER,
                    value: Some("2"),
                }),
        )
        .await?;

        let config = LoaderConfiguration {
            responder: Some(SubsystemName::try_new(RESPONDER)?),
            ..loader_config()
        };
        let loader = KafkaLoader::<JsonCodec>::new(config, &HeartbeatRegistry::test())?;
        let topic = Topic::from(topic_name);

        assert_eq!(
            load(&loader, topic, requested_offset).await?,
            Some(ResultRequest::new(
                id,
                peer,
                RequestDeadline::from_unix_micros(1_700_000_000_000_000),
            )),
            "the reloaded record lost the destination its headers named"
        );
        // The negative control: without it, a loader that reported the same request
        // for every record would satisfy the assertion above.
        assert_eq!(
            load(&loader, topic, plain_offset).await?,
            None,
            "a record that asked for no response must carry no destination"
        );
        // Asking for a response badly costs the request its destination, never
        // the record its reload: a decode that discarded it would fail this
        // load.
        assert_eq!(
            load(&loader, topic, malformed_offset).await?,
            None,
            "a record with unusable response headers must still reload"
        );
        Ok(())
    })
    .await
}

/// Produces one record and reports the offset it landed at.
async fn produce(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    headers: OwnedHeaders,
) -> color_eyre::Result<Offset> {
    let delivery = producer
        .send(
            FutureRecord::to(topic)
                .partition(0)
                .key(key)
                .payload(r#"{"test_id":1,"data":"result-request"}"#)
                .headers(headers),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(error, _)| error)?;
    Ok(delivery.offset)
}

/// Reads one offset back through the loader's own decode. The deadline is a
/// hang guard, never the assertion.
async fn load(
    loader: &KafkaLoader<JsonCodec>,
    topic: Topic,
    offset: Offset,
) -> color_eyre::Result<Option<ResultRequest>> {
    let decoded = timeout(
        Duration::from_mins(1),
        loader.load_from_kafka(topic, 0, offset),
    )
    .await??;
    Ok(decoded.value.request)
}
