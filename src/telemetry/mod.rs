//! Telemetry system for monitoring consumer lifecycle events.

use crate::telemetry::event::TelemetryEvent;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::telemetry::sender::TelemetrySender;
use crate::{Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::broadcast;

/// Background Kafka emitter for telemetry events.
pub mod emitter;
/// Telemetry event definitions.
pub mod event;
/// Trace context extractor for telemetry events.
pub(crate) mod injector;
/// Partition-scoped telemetry sender.
pub mod partition;
/// Global telemetry sender.
pub mod sender;

pub use emitter::{
    EmitterError, TelemetryEmitterConfiguration, TelemetryEmitterConfigurationBuilder,
    spawn_telemetry_emitter,
};

const TELEMETRY_CHANNEL_CAPACITY: usize = 8096;

/// Telemetry system for broadcasting consumer lifecycle events.
///
/// Provides a broadcast channel for telemetry events and monotonic clock
/// for timestamping.
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
    /// Creates a new telemetry system with broadcast channel and monotonic
    /// clock.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(TELEMETRY_CHANNEL_CAPACITY);
        let clock = Clock::new();

        Self { tx, clock }
    }

    /// Subscribes to telemetry events.
    ///
    /// Returns a receiver that will receive all telemetry events broadcast
    /// after subscription.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.tx.subscribe()
    }

    /// Creates a global telemetry sender.
    ///
    /// Returns a sender that can emit telemetry events for any partition.
    #[must_use]
    pub fn sender(&self) -> TelemetrySender {
        TelemetrySender::new(self.tx.clone(), self.clock.clone())
    }

    /// Creates a partition-scoped telemetry sender.
    ///
    /// Returns a sender pre-configured for a specific topic and partition.
    #[must_use]
    pub fn partition_sender(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(topic, partition, self.tx.clone(), self.clock.clone())
    }

    /// Emits a raw telemetry event with custom timestamp.
    ///
    /// This method is intended for testing and allows full control
    /// over event timing for testing purposes.
    #[doc(hidden)]
    pub fn test_emit(&self, event: event::TelemetryEvent) {
        let _ = self.tx.send(event);
    }
}
