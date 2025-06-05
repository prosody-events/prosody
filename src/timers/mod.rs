#![allow(dead_code)]

use crate::Key;
use crate::timers::datetime::CompactDateTime;
use educe::Educe;
use tracing::Span;

#[derive(Clone, Debug, Educe)]
#[educe(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub key: Key,
    pub time: CompactDateTime,

    #[educe(Hash(ignore), PartialEq(ignore), PartialOrd(ignore))]
    pub span: Span,
}

mod active;
pub mod datetime;
pub mod duration;
mod error;
mod loader;
mod manager;
mod queue;
mod scheduler;
mod slab;
mod slab_lock;
pub mod store;
mod uncommitted;

pub use manager::TimerManager;
pub use uncommitted::UncommittedTimer;
