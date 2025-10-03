use crate::telemetry::event::TelemetryEvent;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::telemetry::sender::TelemetrySender;
use crate::{Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::broadcast;

pub mod event;
pub mod partition;
pub mod sender;

const TELEMETRY_CHANNEL_CAPACITY: usize = 8096;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Telemetry {
    #[educe(Debug(ignore))]
    tx: broadcast::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl Telemetry {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(TELEMETRY_CHANNEL_CAPACITY);
        let clock = Clock::new();

        Self { tx, clock }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.tx.subscribe()
    }

    pub fn sender(&self) -> TelemetrySender {
        TelemetrySender::new(self.tx.clone(), self.clock.clone())
    }

    pub fn partition_sender(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(topic, partition, self.tx.clone(), self.clock.clone())
    }
}
