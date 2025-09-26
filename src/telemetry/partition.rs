use crate::telemetry::event::{
    Data, KeyEvent, KeyState, PartitionEvent, PartitionState, TelemetryEvent,
};
use crate::{Key, Partition, Topic};
use educe::Educe;
use quanta::Clock;
use tokio::sync::mpsc;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct TelemetryPartitionSender {
    topic: Topic,
    partition: Partition,

    #[educe(Debug(ignore))]
    tx: mpsc::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,
}

impl TelemetryPartitionSender {
    pub(crate) fn new(
        topic: Topic,
        partition: Partition,
        tx: mpsc::Sender<TelemetryEvent>,
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
        let _ = self.tx.try_send(TelemetryEvent {
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
        let _ = self.tx.try_send(TelemetryEvent {
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
        let _ = self.tx.try_send(TelemetryEvent {
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
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Partition(PartitionEvent {
                state: PartitionState::Revoked,
            }),
        });
    }

    pub fn middleware_entered(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::MiddlewareEntered,
            }),
        });
    }

    pub fn handler_invoked(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::HandlerInvoked,
            }),
        });
    }

    pub fn handler_succeeded(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::HandlerSucceeded,
            }),
        });
    }

    pub fn handler_failed(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::HandlerFailed,
            }),
        });
    }

    pub fn handler_returned(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::HandlerReturned,
            }),
        });
    }

    pub fn middleware_exited(&self, key: Key) {
        let timestamp = self.clock.now();
        let _ = self.tx.try_send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Key(KeyEvent {
                key,
                state: KeyState::MiddlewareExited,
            }),
        });
    }
}
