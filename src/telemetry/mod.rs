use crate::telemetry::event::TelemetryEvent;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::telemetry::sender::TelemetrySender;
use crate::{Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::mpsc;

mod event;
pub mod partition;
pub mod sender;

const TELEMETRY_CHANNEL_CAPACITY: usize = 8096;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Telemetry {
    #[educe(Debug(ignore))]
    tx: mpsc::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,
}

impl Telemetry {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(TELEMETRY_CHANNEL_CAPACITY);
        let clock = Clock::new();
        Self { tx, clock }
    }

    pub fn sender(&self) -> TelemetrySender {
        TelemetrySender::new(self.tx.clone(), self.clock.clone())
    }

    pub fn partition_sender(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(topic, partition, self.tx.clone(), self.clock.clone())
    }
}
