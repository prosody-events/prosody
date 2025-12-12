//! Telemetry event types for consumer lifecycle monitoring.

use crate::consumer::DemandType;
use crate::{Key, Partition, Topic};
use quanta::Instant;

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
