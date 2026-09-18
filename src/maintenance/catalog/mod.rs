//! The catalog: the scans production code never runs.
//!
//! Production reads one partition at a time, because it always knows the key it
//! wants. Maintenance must first learn which segments and which keys exist. The
//! [`Catalog`] answers that question over either backend.

mod cassandra;
mod memory;

pub use cassandra::CassandraCatalog;
pub use memory::MemoryCatalog;

use super::identity::{DeferSegmentId, Segment, TimerSegmentId};
use crate::Key;
use crate::error::ClassifyError;
use crate::timers::store::Segment as TimerSegment;
use futures::Stream;
use std::error::Error;
use std::future::Future;

/// Enumerates the identities a maintenance run visits.
///
/// Every method streams or returns owned values and keeps no state between
/// calls. Dropping a stream stops the scan behind it.
///
/// The catalog reads identities and one small row. It never reads a queue head
/// or a retry timer: the production point reads answer those, one key at a
/// time, under a bound.
pub trait Catalog: Clone + Send + Sync + 'static {
    /// The backend's own error. It is classified, so an operator tool can
    /// decide to retry or to stop.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Every segment that ever deferred, from the registry of deferred
    /// segments.
    fn segments(&self) -> impl Stream<Item = Result<Segment, Self::Error>> + Send;

    /// The timer segment row, or `None` when the segment never ran a scheduler.
    ///
    /// This is a plain read. It never migrates the segment.
    fn timer_segment(
        &self,
        id: TimerSegmentId,
    ) -> impl Future<Output = Result<Option<TimerSegment>, Self::Error>> + Send;

    /// Keys with deferred message rows in one segment.
    ///
    /// A key can come back with an empty queue, because a partition can hold a
    /// retry count and no queue row. The production point read decides.
    fn message_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send;

    /// Keys with deferred timer rows in one segment. The twin of
    /// [`message_keys`](Self::message_keys).
    fn timer_keys(&self, id: DeferSegmentId)
    -> impl Stream<Item = Result<Key, Self::Error>> + Send;
}
