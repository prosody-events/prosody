//! Partition-scoped telemetry sender for consumer lifecycle events.

use crate::consumer::DemandType;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, PartitionEvent, PartitionState, TelemetryEvent,
};
use crate::{Key, Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::broadcast;

/// Telemetry sender pre-configured for a specific partition.
///
/// Emits telemetry events for partition and key lifecycle events
/// without requiring topic/partition parameters.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct TelemetryPartitionSender {
    topic: Topic,
    partition: Partition,

    #[educe(Debug(ignore))]
    tx: broadcast::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,
}

impl TelemetryPartitionSender {
    pub(crate) fn new(
        topic: Topic,
        partition: Partition,
        tx: broadcast::Sender<TelemetryEvent>,
        clock: Clock,
    ) -> Self {
        Self {
            topic,
            partition,
            tx,
            clock,
        }
    }

    /// Emits a partition paused event.
    pub fn partition_paused(&self) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Paused,
            }),
        });
    }

    /// Emits a partition resumed event.
    pub fn partition_resumed(&self) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Resumed,
            }),
        });
    }

    /// Emits a partition assigned event.
    pub fn partition_assigned(&self) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Assigned,
            }),
        });
    }

    /// Emits a partition revoked event.
    pub fn partition_revoked(&self) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Revoked,
            }),
        });
    }

    /// Emits a middleware entered event for the given key.
    pub fn middleware_entered(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::MiddlewareEntered,
            }),
        });
    }

    /// Emits a handler invoked event for the given key.
    pub fn handler_invoked(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerInvoked,
            }),
        });
    }

    /// Emits a handler succeeded event for the given key.
    pub fn handler_succeeded(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerSucceeded,
            }),
        });
    }

    /// Emits a handler failed event for the given key.
    pub fn handler_failed(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerFailed,
            }),
        });
    }

    /// Emits a middleware exited event for the given key.
    pub fn middleware_exited(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::MiddlewareExited,
            }),
        });
    }
}
