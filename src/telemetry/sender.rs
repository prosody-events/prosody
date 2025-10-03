use crate::consumer::DemandType;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, PartitionEvent, PartitionState, TelemetryEvent,
};
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::{Key, Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::broadcast;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct TelemetrySender {
    #[educe(Debug(ignore))]
    tx: broadcast::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,
}

impl TelemetrySender {
    pub(crate) fn new(tx: broadcast::Sender<TelemetryEvent>, clock: Clock) -> Self {
        Self { tx, clock }
    }

    pub fn partition_paused(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Paused,
            }),
        });
    }

    pub fn partition_resumed(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Resumed,
            }),
        });
    }

    pub fn partition_assigned(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Assigned,
            }),
        });
    }

    pub fn partition_revoked(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Revoked,
            }),
        });
    }

    pub fn middleware_entered(
        &self,
        topic: Topic,
        partition: Partition,
        key: Key,
        demand_type: DemandType,
    ) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::MiddlewareEntered,
            }),
        });
    }

    pub fn handler_invoked(
        &self,
        topic: Topic,
        partition: Partition,
        key: Key,
        demand_type: DemandType,
    ) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerInvoked,
            }),
        });
    }

    pub fn handler_succeeded(
        &self,
        topic: Topic,
        partition: Partition,
        key: Key,
        demand_type: DemandType,
    ) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerSucceeded,
            }),
        });
    }

    pub fn handler_failed(
        &self,
        topic: Topic,
        partition: Partition,
        key: Key,
        demand_type: DemandType,
    ) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerFailed,
            }),
        });
    }

    pub fn middleware_exited(
        &self,
        topic: Topic,
        partition: Partition,
        key: Key,
        demand_type: DemandType,
    ) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::MiddlewareExited,
            }),
        });
    }

    pub fn for_partition(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(topic, partition, self.tx.clone(), self.clock.clone())
    }
}
