//! Telemetry event types for consumer lifecycle monitoring.

use crate::consumer::DemandType;
use crate::error::ErrorCategory;
use crate::timers::TimerType;
use crate::timers::datetime::CompactDateTime;
use crate::{Key, Offset, Partition, Topic};
use chrono::{DateTime, Utc};
use quanta::Instant;
use std::sync::Arc;

/// A telemetry event capturing consumer lifecycle activity.
#[derive(Clone, Debug)]
pub struct TelemetryEvent {
    /// Monotonic timestamp when the event occurred.
    pub timestamp: Instant,
    /// Topic associated with the event.
    pub topic: Topic,
    /// Partition associated with the event.
    pub partition: Partition,
    /// Event-specific data.
    pub data: Data,
}

/// Event data payload.
#[derive(Clone, Debug)]
pub enum Data {
    /// Partition-level event.
    Partition(PartitionEvent),
    /// Key-level event.
    Key(KeyEvent),
    /// Timer lifecycle event (scheduled, cancelled, dispatched, succeeded,
    /// failed).
    Timer(TimerTelemetryEvent),
    /// Message lifecycle event (dispatched, succeeded, failed).
    Message(MessageTelemetryEvent),
    /// Producer message sent event.
    MessageSent(MessageSentEvent),
}

/// Partition lifecycle event.
#[derive(Clone, Debug)]
pub struct PartitionEvent {
    /// Partition state.
    pub state: PartitionState,
}

/// Key-level processing event.
#[derive(Clone, Debug)]
pub struct KeyEvent {
    /// Message key.
    pub key: Key,
    /// Demand type (normal or urgent).
    pub demand_type: DemandType,
    /// Key processing state.
    pub state: KeyState,
}

/// Partition lifecycle states.
#[derive(Clone, Debug)]
pub enum PartitionState {
    /// Partition was assigned to this consumer.
    Assigned,
    /// Partition consumption was paused due to backpressure.
    Paused,
    /// Partition consumption was resumed.
    Resumed,
    /// Partition was revoked from this consumer.
    Revoked,
}

/// Key processing lifecycle states.
#[derive(Clone, Debug)]
pub enum KeyState {
    /// Middleware layer begins processing.
    MiddlewareEntered,
    /// User handler function is called.
    HandlerInvoked,
    /// User handler completed successfully.
    HandlerSucceeded,
    /// User handler returned an error.
    HandlerFailed,
    /// Middleware layer completes processing.
    MiddlewareExited,
}

/// Timer lifecycle event for external telemetry emission.
#[derive(Clone, Debug)]
pub struct TimerTelemetryEvent {
    /// Lifecycle stage of the timer event.
    pub event_type: TimerEventType,
    /// Wall-clock time captured at the causal moment.
    pub event_time: DateTime<Utc>,
    /// When the timer is/was scheduled to fire.
    pub scheduled_time: CompactDateTime,
    /// Timer type classification.
    pub timer_type: TimerType,
    /// Timer entity key.
    pub key: Key,
    /// Consumer `group_id`.
    pub source: Arc<str>,
    /// W3C traceparent header.
    pub trace_parent: Option<Box<str>>,
    /// W3C tracestate header.
    pub trace_state: Option<Box<str>>,
}

/// Timer event lifecycle stage with stage-specific fields.
#[derive(Clone, Debug)]
pub enum TimerEventType {
    /// Timer was written to store.
    Scheduled,
    /// Timer was cancelled before firing.
    Cancelled,
    /// Timer fired, handler about to be called.
    Dispatched {
        /// Demand type (normal or failure).
        demand_type: DemandType,
    },
    /// Handler returned Ok.
    Succeeded {
        /// Demand type (normal or failure).
        demand_type: DemandType,
    },
    /// Handler returned Err.
    Failed {
        /// Demand type (normal or failure).
        demand_type: DemandType,
        /// Error classification.
        error_category: ErrorCategory,
        /// Error debug representation.
        exception: Box<str>,
    },
}

/// Message lifecycle event for external telemetry emission.
#[derive(Clone, Debug)]
pub struct MessageTelemetryEvent {
    /// Lifecycle stage of the message event.
    pub event_type: MessageEventType,
    /// Wall-clock time captured at the causal moment.
    pub event_time: DateTime<Utc>,
    /// Kafka offset of the consumed message.
    pub offset: Offset,
    /// Message entity key.
    pub key: Key,
    /// Consumer `group_id`.
    pub source: Arc<str>,
    /// W3C traceparent header.
    pub trace_parent: Option<Box<str>>,
    /// W3C tracestate header.
    pub trace_state: Option<Box<str>>,
}

/// Message event lifecycle stage with stage-specific fields.
#[derive(Clone, Debug)]
pub enum MessageEventType {
    /// Message dispatched to handler.
    Dispatched {
        /// Demand type (normal or failure).
        demand_type: DemandType,
    },
    /// Handler returned Ok.
    Succeeded {
        /// Demand type (normal or failure).
        demand_type: DemandType,
    },
    /// Handler returned Err.
    Failed {
        /// Demand type (normal or failure).
        demand_type: DemandType,
        /// Error classification.
        error_category: ErrorCategory,
        /// Error debug representation.
        exception: Box<str>,
    },
}

/// Producer message sent event for external telemetry emission.
#[derive(Clone, Debug)]
pub struct MessageSentEvent {
    /// Wall-clock time immediately after delivery ack.
    pub event_time: DateTime<Utc>,
    /// Destination topic.
    pub topic: Topic,
    /// Destination partition.
    pub partition: Partition,
    /// Destination offset.
    pub offset: i64,
    /// Message key.
    pub key: Key,
    /// Producer `source_system`.
    pub source: Arc<str>,
    /// W3C traceparent header.
    pub trace_parent: Option<Box<str>>,
    /// W3C tracestate header.
    pub trace_state: Option<Box<str>>,
}
