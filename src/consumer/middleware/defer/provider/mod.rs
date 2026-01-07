//! Unified provider trait and implementations for defer store backends.
//!
//! [`DeferStoreProvider`] combines segment, message, and timer store creation
//! into a single trait with associated types. This ensures all stores use the
//! same backend and share the same segment.

pub mod cassandra;
pub mod memory;

use super::message::store::MessageDeferStore;
use super::segment::{LazySegment, SegmentStore};
use super::timer::store::TimerDeferStore;

pub use cassandra::CassandraDeferStoreProvider;
pub use memory::MemoryDeferStoreProvider;

/// Unified provider for creating defer stores.
///
/// Represents a configured storage backend for the defer middleware. Provides
/// access to the segment store and creates message/timer stores that use the
/// same backend.
///
/// # Design
///
/// Using a single trait with associated types instead of separate provider
/// traits eliminates type parameter proliferation in the middleware. The
/// middleware only needs one type parameter for storage instead of three.
///
/// # Implementations
///
/// - [`MemoryDeferStoreProvider`]: In-memory stores for testing
/// - [`CassandraDeferStoreProvider`]: Persistent Cassandra stores for
///   production
pub trait DeferStoreProvider: Clone + Send + Sync + 'static {
    /// Segment store type for this backend.
    type Segment: SegmentStore;

    /// Message defer store type created by this provider.
    type MessageStore: MessageDeferStore;

    /// Timer defer store type created by this provider.
    type TimerStore: TimerDeferStore;

    /// Returns a reference to the segment store.
    fn segment(&self) -> &Self::Segment;

    /// Creates a message defer store for the given lazy segment.
    fn create_message_store(&self, segment: LazySegment<Self::Segment>) -> Self::MessageStore;

    /// Creates a timer defer store for the given lazy segment.
    fn create_timer_store(&self, segment: LazySegment<Self::Segment>) -> Self::TimerStore;
}
