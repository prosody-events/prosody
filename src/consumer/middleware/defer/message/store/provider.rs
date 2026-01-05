//! Provider trait for creating partition-specific defer stores.

use super::MessageDeferStore;
use crate::consumer::middleware::ClassifyError;
use crate::consumer::middleware::defer::segment::Segment;
use std::error::Error;
use std::future::Future;

/// Factory for creating partition-specific [`MessageDeferStore`] instances.
///
/// The provider holds shared resources (Cassandra session, prepared queries)
/// and creates store instances for a given [`Segment`].
///
/// # Usage
///
/// ```text
/// // Create provider once at application startup
/// let provider = CassandraDeferStoreProvider::new(session, keyspace).await?;
///
/// // Create store for a segment
/// let store = provider.create_store(&segment).await?;
/// ```
///
/// # Segment Lifecycle
///
/// The [`Segment`] must be created and persisted via [`SegmentStore`] before
/// calling `create_store`. This ensures:
/// 1. Segment metadata is in the database
/// 2. Both message and timer defer share the same segment
/// 3. No redundant segment ID computation
///
/// [`SegmentStore`]: crate::consumer::middleware::defer::segment::SegmentStore
///
/// # Implementations
///
/// - `CassandraDeferStoreProvider`: Creates `CassandraDeferStore` instances
/// - `MemoryDeferStoreProvider`: Creates `MemoryDeferStore` instances (for
///   testing)
pub trait MessageDeferStoreProvider: Clone + Send + Sync + 'static {
    /// The store type created by this provider.
    ///
    /// The store's error type must match this provider's error type to allow
    /// unified error handling in the lazy store wrapper.
    type Store: MessageDeferStore<Error = Self::Error>;

    /// Error type for store creation and store operations.
    ///
    /// This unified error type is used for both provider creation errors
    /// and errors from the created store instances.
    type Error: Error + ClassifyError + Send + Sync + 'static;

    /// Creates a store for the specified segment.
    ///
    /// # Arguments
    ///
    /// * `segment` - The segment (`topic/partition/consumer_group` context)
    ///
    /// # Preconditions
    ///
    /// The segment must already be persisted via
    /// [`SegmentStore::get_or_create_segment`]. This method does NOT insert
    /// segment metadata.
    ///
    /// # Errors
    ///
    /// Returns error if store initialization fails.
    ///
    /// [`SegmentStore::get_or_create_segment`]: crate::consumer::middleware::defer::segment::SegmentStore::get_or_create_segment
    fn create_store(
        &self,
        segment: &Segment,
    ) -> impl Future<Output = Result<Self::Store, Self::Error>> + Send;
}
