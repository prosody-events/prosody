//! Partition-scoped telemetry sender for consumer lifecycle events.

use crate::consumer::DemandType;
use crate::error::ErrorCategory;
use crate::telemetry::event::{
    Data, KeyEvent, KeyState, MessageEventType, MessageTelemetryEvent, TelemetryEvent,
    TimerEventType, TimerTelemetryEvent,
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
/// Emits key, message, and timer lifecycle events scoped to one
/// topic/partition, so callers never pass those parameters.
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
        propagator: Arc<TextMapCompositePropagator>,
    ) -> Self {
        Self {
            topic,
            partition,
            tx,
            clock,
            propagator,
        }
    }

    /// Emits a handler invoked event for the given key.
    pub fn handler_invoked(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Arc::new(Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerInvoked,
            })),
        });
    }

    /// Emits a handler succeeded event for the given key.
    pub fn handler_succeeded(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Arc::new(Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerSucceeded,
            })),
        });
    }

    /// Emits a handler failed event for the given key.
    pub fn handler_failed(&self, key: Key, demand_type: DemandType) {
        let timestamp = self.clock.now();
        let _ = self.tx.send(TelemetryEvent {
            timestamp,
            topic: self.topic,
            partition: self.partition,
            data: Arc::new(Data::Key(KeyEvent {
                key,
                demand_type,
                state: KeyState::HandlerFailed,
            })),
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
            data: Arc::new(Data::Message(MessageTelemetryEvent {
                event_type: MessageEventType::Dispatched { demand_type },
                event_time: Utc::now(),
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            })),
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
            data: Arc::new(Data::Message(MessageTelemetryEvent {
                event_type: MessageEventType::Succeeded { demand_type },
                event_time: Utc::now(),
                offset,
                key,
                source,
                trace_parent,
                trace_state,
            })),
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
            data: Arc::new(Data::Message(MessageTelemetryEvent {
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
            })),
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
            data: Arc::new(Data::Timer(TimerTelemetryEvent {
                event_type,
                event_time: Utc::now(),
                scheduled_time,
                timer_type,
                key,
                source,
                trace_parent,
                trace_state,
            })),
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

    /// Emits a timer cancelled event.
    pub fn timer_cancelled(
        &self,
        key: Key,
        scheduled_time: CompactDateTime,
        timer_type: TimerType,
        source: Arc<str>,
    ) {
        self.emit_timer(
            TimerEventType::Cancelled,
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
