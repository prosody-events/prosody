//! Global telemetry sender for consumer lifecycle events.

use crate::telemetry::event::{
    Data, MessageSentEvent, PartitionEvent, PartitionState, TelemetryEvent,
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
/// Emits partition lifecycle and producer-send events across any topic
/// and partition.
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
    pub(crate) fn new(
        tx: broadcast::Sender<TelemetryEvent>,
        clock: Clock,
        propagator: Arc<TextMapCompositePropagator>,
    ) -> Self {
        Self {
            tx,
            clock,
            propagator,
        }
    }

    /// Emits a partition assigned event.
    pub fn partition_assigned(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Arc::new(Data::Partition(PartitionEvent {
                state: PartitionState::Assigned,
            })),
        });
    }

    /// Emits a partition revoked event.
    pub fn partition_revoked(&self, topic: Topic, partition: Partition) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic,
            partition,
            data: Arc::new(Data::Partition(PartitionEvent {
                state: PartitionState::Revoked,
            })),
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
            data: Arc::new(Data::MessageSent(MessageSentEvent {
                event_time: Utc::now(),
                topic,
                partition,
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            })),
        });
    }

    /// Creates a partition-scoped telemetry sender.
    ///
    /// Returns a sender pre-configured for a specific topic and partition.
    #[must_use]
    pub fn partition_sender(&self, topic: Topic, partition: Partition) -> TelemetryPartitionSender {
        TelemetryPartitionSender::new(
            topic,
            partition,
            self.tx.clone(),
            self.clock.clone(),
            self.propagator.clone(),
        )
    }
}

#[cfg(test)]
mod tests;
