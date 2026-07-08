use super::*;
use crate::telemetry::Telemetry;
use crate::telemetry::event::Data;
use chrono::Utc;
use color_eyre::eyre::{Result, bail, ensure};

/// Builds a fresh sender and its subscribed receiver for a test.
fn harness() -> (TelemetrySender, broadcast::Receiver<TelemetryEvent>) {
    let telemetry = Telemetry::new();
    let rx = telemetry.subscribe();
    (telemetry.sender(), rx)
}

#[test]
fn message_sent_emits_correct_variant() -> Result<()> {
    let (sender, mut rx) = harness();

    let topic: Topic = "test-topic".into();
    let partition: Partition = 3;
    let offset: i64 = 42;
    let key: Key = Arc::from("test-key");
    let source: Arc<str> = Arc::from("test-source");

    sender.message_sent(topic, partition, offset, key.clone(), source.clone());

    let event = rx.try_recv()?;
    assert_eq!(event.topic, topic);
    assert_eq!(event.partition, partition);

    let Data::MessageSent(msg) = &*event.data else {
        bail!("expected Data::MessageSent variant");
    };
    assert_eq!(msg.topic, topic);
    assert_eq!(msg.partition, partition);
    assert_eq!(msg.offset, offset);
    assert_eq!(msg.key, key);
    assert_eq!(&*msg.source, &*source);
    Ok(())
}

#[test]
fn message_sent_event_time_is_recent() -> Result<()> {
    let (sender, mut rx) = harness();

    let topic: Topic = "time-topic".into();
    let key: Key = Arc::from("time-key");
    let source: Arc<str> = Arc::from("time-source");

    let before = Utc::now();
    sender.message_sent(topic, 0, 0, key, source);
    let after = Utc::now();

    let event = rx.try_recv()?;
    let Data::MessageSent(msg) = &*event.data else {
        bail!("expected Data::MessageSent variant");
    };

    ensure!(
        msg.event_time >= before,
        "event_time predates call: {:?} < {:?}",
        msg.event_time,
        before
    );
    ensure!(
        msg.event_time <= after + chrono::Duration::seconds(5),
        "event_time too far in the future: {:?}",
        msg.event_time
    );
    Ok(())
}
