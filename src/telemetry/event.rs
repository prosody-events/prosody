use crate::{Key, Partition, Topic};
use quanta::Instant;

pub struct TelemetryEvent {
    pub timestamp: Instant,
    pub topic: Topic,
    pub partition: Partition,
    pub data: Data,
}

pub enum Data {
    Partition(PartitionEvent),
    Key(KeyEvent),
}

pub struct PartitionEvent {
    pub state: PartitionState,
}

pub struct KeyEvent {
    pub key: Key,
    pub state: KeyState,
}

pub enum PartitionState {
    Assigned,
    Paused,
    Resumed,
    Revoked,
}

pub enum KeyState {
    MiddlewareEntered, // Middleware layer begins processing
    HandlerInvoked,    // User handler function is called
    HandlerSucceeded,  // User handler completed successfully
    HandlerFailed,     // User handler returned an error
    HandlerReturned,   // User handler function has returned
    MiddlewareExited,  // Middleware layer completes processing
}
