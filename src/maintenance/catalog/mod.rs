//! The catalog: the scans production code never runs.
//!
//! Production reads one partition at a time, because it always knows the key it
//! wants. Maintenance must first learn which segments and which keys exist. The
//! [`Catalog`] answers that question over either backend.

// `pub(super)` so the maintenance tests can name `CATALOG_PAGE_SIZE` and prove
// a key scan crosses a page boundary.
pub(super) mod cassandra;
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
/// Each scan reports an identity at most once. The order is unspecified.
pub trait Catalog: Clone + Send + Sync + 'static {
    /// The backend's own error. It is classified, so an operator tool can
    /// decide to retry or to stop.
    type Error: ClassifyError + Error + Send + Sync + 'static;

    /// Every segment that ever deferred, from the registry of deferred
    /// segments.
    ///
    /// The scan reports a segment only when the row names a group, a topic,
    /// and a partition. It skips any other row.
    fn segments(&self) -> impl Stream<Item = Result<Segment, Self::Error>> + Send + 'static;

    /// The timer segment row, or `None` when the segment never ran a scheduler.
    ///
    /// This is a plain read. It never migrates the segment.
    fn timer_segment(
        &self,
        id: TimerSegmentId,
    ) -> impl Future<Output = Result<Option<TimerSegment>, Self::Error>> + Send;

    /// Keys with deferred message rows in one segment.
    ///
    /// A key can come back with an empty queue. No production path writes a
    /// retry count with no queue row, so a caller reads the queue head and
    /// drops the key when the head is absent.
    fn message_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static;

    /// Keys with deferred timer rows in one segment. The twin of
    /// [`message_keys`](Self::message_keys).
    fn timer_keys(
        &self,
        id: DeferSegmentId,
    ) -> impl Stream<Item = Result<Key, Self::Error>> + Send + 'static;
}
