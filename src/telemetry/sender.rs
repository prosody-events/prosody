//! Global telemetry sender for consumer lifecycle events.

use crate::consumer::DemandType;
use crate::propagator::new_propagator;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, MessageSentEvent, PartitionEvent, PartitionState, TelemetryEvent,
};
use crate::telemetry::injector::TelemetryInjector;
use crate::telemetry::partition::TelemetryPartitionSender;
use crate::{Key, Partition, Topic};
use chrono::Utc;
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use quanta::Clock;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Global telemetry sender for emitting lifecycle events.
///
/// Emits telemetry events for partition and key lifecycle events
/// across any topic and partition.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct TelemetrySender {
    #[educe(Debug(ignore))]
    tx: broadcast::Sender<TelemetryEvent>,

    #[educe(Debug(ignore))]
    clock: Clock,

    #[educe(Debug(ignore))]
    propagator: Arc<TextMapCompositePropagator>,
}

impl TelemetrySender {
    pub(crate) fn new(tx: broadcast::Sender<TelemetryEvent>, clock: Clock) -> Self {
        Self {
            tx,
            clock,
            propagator: Arc::new(new_propagator()),
        }
    }

    /// Emits a partition paused event.
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

    /// Emits a partition resumed event.
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

    /// Emits a partition assigned event.
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

    /// Emits a partition revoked event.
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

    /// Emits a middleware entered event for the given key.
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

    /// Emits a handler invoked event for the given key.
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

    /// Emits a handler succeeded event for the given key.
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

    /// Emits a handler failed event for the given key.
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

    /// Emits a middleware exited event for the given key.
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

    /// Emits a producer message sent event.
    pub fn message_sent(
        &self,
        topic: Topic,
        partition: Partition,
        offset: i64,
        key: Key,
        source: Arc<str>,
    ) {
        let injector = TelemetryInjector::extract(&self.propagator);
        let (trace_parent, trace_state) = injector.into_parts();
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Data::MessageSent(MessageSentEvent {
                event_time: Utc::now(),
                topic,
                partition,
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            }),
        });
    }

    /// Creates a partition-scoped telemetry sender.
    ///
    /// Returns a sender pre-configured for a specific topic and partition.
    #[must_use]
    pub fn for_partition(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(topic, partition, self.tx.clone(), self.clock.clone())
    }
}
