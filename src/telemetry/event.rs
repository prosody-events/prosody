use crate::consumer::DemandType;
use crate::{Key, Partition, Topic};
use quanta::Instant;

#[derive(Clone, Debug)]
pub struct TelemetryEvent {
    pub timestamp: Instant,
    pub topic: Topic,
    pub partition: Partition,
    pub data: Data,
}

#[derive(Clone, Debug)]
pub enum Data {
    Partition(PartitionEvent),
    Key(KeyEvent),
}

#[derive(Clone, Debug)]
pub struct PartitionEvent {
    pub state: PartitionState,
}

#[derive(Clone, Debug)]
pub struct KeyEvent {
    pub key: Key,
    pub demand_type: DemandType,
    pub state: KeyState,
}

#[derive(Clone, Debug)]
pub enum PartitionState {
    Assigned,
    Paused,
    Resumed,
    Revoked,
}

#[derive(Clone, Debug)]
pub enum KeyState {
    MiddlewareEntered, // Middleware layer begins processing
    HandlerInvoked,    // User handler function is called
    HandlerSucceeded,  // User handler completed successfully
    HandlerFailed,     // User handler returned an error
    MiddlewareExited,  // Middleware layer completes processing
}
