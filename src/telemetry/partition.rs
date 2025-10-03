use crate::consumer::DemandType;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, PartitionEvent, PartitionState, TelemetryEvent,
};
use crate::{Key, Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::broadcast;

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
