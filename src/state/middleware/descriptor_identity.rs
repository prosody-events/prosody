//! Lazy first-dispatch identity validation for the keyed-state middleware.
//!
//! [`LazyDescriptorIdentity`] mirrors the defer middleware's `LazySegment`
//! pattern: constructed synchronously in `handler_for_partition` (no I/O),
//! validated on first dispatch via `get_or_try_init`, errors **not**
//! cached so transient store failures retry on the next event. The
//! validation body itself lives in
//! [`crate::state::descriptor_identity::acquire_descriptor_identities`],
//! shared with the state manager's eager acquisition path.

use crate::state::descriptor_identity::{
    DescriptorIdentityError, DescriptorIdentityStore, acquire_descriptor_identities,
};
use crate::state::registry::CollectionDefRegistry;
use crate::timers::store::SegmentId;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Defers durable identity validation until the first dispatch.
///
/// Cheap to clone (`Arc` internally); clones share the validated flag.
pub(crate) struct LazyDescriptorIdentity<St> {
    inner: Arc<LazyIdentityInner<St>>,
}

impl<St> Clone for LazyDescriptorIdentity<St> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct LazyIdentityInner<St> {
    cell: OnceCell<()>,
    store: St,
    registry: Arc<CollectionDefRegistry>,
    segment_id: SegmentId,
}

impl<St> LazyDescriptorIdentity<St>
where
    St: DescriptorIdentityStore,
{
    /// Creates a lazy validator (no I/O until [`ensure`](Self::ensure)).
    pub(crate) fn new(
        store: St,
        registry: Arc<CollectionDefRegistry>,
        segment_id: SegmentId,
    ) -> Self {
        Self {
            inner: Arc::new(LazyIdentityInner {
                cell: OnceCell::new(),
                store,
                registry,
                segment_id,
            }),
        }
    }

    /// Validates the registered descriptors once per partition assignment;
    /// errors are not cached, so a transient store failure retries on the
    /// next dispatch.
    ///
    /// # Errors
    ///
    /// See [`acquire_descriptor_identities`].
    pub(crate) async fn ensure(&self) -> Result<(), DescriptorIdentityError<St::Error>> {
        self.inner
            .cell
            .get_or_try_init(|| {
                acquire_descriptor_identities(
                    &self.inner.store,
                    &self.inner.registry,
                    self.inner.segment_id,
                )
            })
            .await
            .copied()
    }
}
