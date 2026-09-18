//! The maintenance client: read the durable defer and timer rows, judge what
//! they say, and repair what is broken.
//!
//! A stranded deferred key has rows in its queue and no retry timer of the
//! matching type. Nothing reloads that queue. The client finds every stranded
//! key and arms one timer for it.
//!
//! The client keeps five concerns apart. Each concern is a value or a type. No
//! value carries another concern's state.
//!
//! Identity names one consumer group, one topic, and one partition. A
//! [`Segment`] derives both frozen on-disk segment ids from that triple.
//!
//! Facts are what the stores said at one moment. Each fact is a plain owned
//! record. It holds no generic, no borrow, and no connection.
//!
//! Judgment reads facts and reports findings. It does not read or write data.
//!
//! Action plans repairs and applies them.
//!
//! Posture says whether the group that owns a segment may run.
//!
//! The [`Catalog`] runs the scans production code never runs. The scans that
//! restrict a partial partition key live only in the Cassandra catalog. No
//! other module in the crate runs such a scan.

mod catalog;
mod facts;
mod identity;

pub use catalog::{CassandraCatalog, Catalog, MemoryCatalog};
pub use facts::{DeferredKey, DeferredQueue, RetryTimer, SegmentFacts};
pub use identity::{DeferSegmentId, GroupId, Segment, TimerSegmentId};

#[cfg(test)]
mod tests;
