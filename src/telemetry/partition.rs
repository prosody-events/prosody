//! Partition-scoped telemetry sender for consumer lifecycle events.

use crate::consumer::DemandType;
use crate::error::ErrorCategory;
use crate::propagator::new_propagator;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, MessageEventType, MessageTelemetryEvent, PartitionEvent,
    PartitionState, TelemetryEvent, TimerEventType, TimerTelemetryEvent,
};
use crate::telemetry::injector::TelemetryInjector;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::{Key, Offset, Partition, Topic};
use chrono::Utc;
use educe::Educe;
use opentelemetry::propagation::TextMapCompositePropagator;
use quanta::Clock;
use std::sync::Arc;
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

    #[educe(Debug(ignore))]
    propagator: Arc<TextMapCompositePropagator>,
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
            propagator: Arc::new(new_propagator()),
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

    /// Emits a message dispatched event.
    pub fn message_dispatched(
        &self,
        key: Key,
        offset: Offset,
        demand_type: DemandType,
        source: Arc<str>,
    ) {
        let injector = TelemetryInjector::extract(&self.propagator);
        let (trace_parent, trace_state) = injector.into_parts();
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Message(MessageTelemetryEvent {
                event_type: MessageEventType::Dispatched { demand_type },
                event_time: Utc::now(),
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            }),
        });
    }

    /// Emits a message succeeded event.
    pub fn message_succeeded(
        &self,
        key: Key,
        offset: Offset,
        demand_type: DemandType,
        source: Arc<str>,
    ) {
        let injector = TelemetryInjector::extract(&self.propagator);
        let (trace_parent, trace_state) = injector.into_parts();
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Message(MessageTelemetryEvent {
                event_type: MessageEventType::Succeeded { demand_type },
                event_time: Utc::now(),
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            }),
        });
    }

    /// Emits a message failed event.
    pub fn message_failed(
        &self,
        key: Key,
        offset: Offset,
        demand_type: DemandType,
        source: Arc<str>,
        error_category: ErrorCategory,
        exception: Box<str>,
    ) {
        let injector = TelemetryInjector::extract(&self.propagator);
        let (trace_parent, trace_state) = injector.into_parts();
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Message(MessageTelemetryEvent {
                event_type: MessageEventType::Failed {
                    demand_type,
                    error_category,
                    exception,
                },
                event_time: Utc::now(),
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            }),
        });
    }

    /// Emits a timer lifecycle event with an explicit event type.
    pub fn emit_timer(
        &self,
        event_type: TimerEventType,
        key: Key,
        scheduled_time: CompactDateTime,
        timer_type: TimerType,
        source: Arc<str>,
    ) {
        let injector = TelemetryInjector::extract(&self.propagator);
        let (trace_parent, trace_state) = injector.into_parts();
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Data::Timer(TimerTelemetryEvent {
                event_type,
                event_time: Utc::now(),
                scheduled_time,
                timer_type,
                key,
                source,
                trace_parent,
                trace_state,
            }),
        });
    }

    /// Emits a timer scheduled event.
    pub fn timer_scheduled(
        &self,
        key: Key,
        scheduled_time: CompactDateTime,
        timer_type: TimerType,
        source: Arc<str>,
    ) {
        self.emit_timer(
            TimerEventType::Scheduled,
            key,
            scheduled_time,
            timer_type,
            source,
        );
    }

    /// Emits a timer dispatched event.
    pub fn timer_dispatched(
        &self,
        key: Key,
        scheduled_time: CompactDateTime,
        timer_type: TimerType,
        demand_type: DemandType,
        source: Arc<str>,
    ) {
        self.emit_timer(
            TimerEventType::Dispatched { demand_type },
            key,
            scheduled_time,
            timer_type,
            source,
        );
    }

    /// Emits a timer succeeded event.
    pub fn timer_succeeded(
        &self,
        key: Key,
        scheduled_time: CompactDateTime,
        timer_type: TimerType,
        demand_type: DemandType,
        source: Arc<str>,
    ) {
        self.emit_timer(
            TimerEventType::Succeeded { demand_type },
            key,
            scheduled_time,
            timer_type,
            source,
        );
    }
}
