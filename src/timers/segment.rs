//! Segment bootstrap helper used by the [`TimerManager`] constructor before
//! the scheduler actor spawns.
//!
//! [`TimerManager`]: crate::timers::manager::TimerManager

use crate::timers::error::TimerManagerError;
use crate::timers::store::{Segment, TriggerStore};

/// Retrieves or creates a [`Segment`] in the store.
///
/// If a segment already exists in the store, it is returned. Otherwise, a new
/// segment is inserted using the store's segment identity.
///
/// # Arguments
///
/// * `store` - The persistent trigger store.
/// * `_name` - Reserved for future use; the store's segment carries its own
///   name already.
///
/// # Errors
///
/// Returns [`TimerManagerError`] if any store operation fails.
///
/// # Returns
///
/// The existing or newly created [`Segment`] object.
pub async fn get_or_create_segment<T>(
    store: &T,
    _name: &str,
) -> Result<Segment, TimerManagerError<T::Error>>
where
    T: TriggerStore,
{
    if let Some(segment) = store
        .get_segment()
        .await
        .map_err(TimerManagerError::Store)?
    {
        return Ok(segment);
    }

    store
        .insert_segment()
        .await
        .map_err(TimerManagerError::Store)?;

    Ok(store.segment())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::duration::CompactDuration;
    use crate::timers::store::memory::memory_store;
    use crate::timers::store::{Segment, SegmentVersion};
    use color_eyre::eyre::{self as eyre, Result};
    use futures::future;
    use uuid::Uuid;

    fn create_test_segment() -> Segment {
        Segment {
            id: Uuid::new_v4(),
            name: "test-segment".to_owned(),
            slab_size: CompactDuration::new(60),
            version: SegmentVersion::V3,
        }
    }

    #[tokio::test]
    async fn test_get_or_create_segment_new() -> Result<()> {
        let test_segment = create_test_segment();
        let store = memory_store(test_segment.clone());

        let segment = get_or_create_segment(&store, "test-segment").await?;
        assert_eq!(segment.id, test_segment.id);
        assert_eq!(segment.name, test_segment.name);
        assert_eq!(segment.slab_size, test_segment.slab_size);

        let retrieved = store.get_segment().await?;
        let retrieved = retrieved.ok_or_else(|| eyre::eyre!("Expected segment to be stored"))?;
        assert_eq!(retrieved.id, test_segment.id);
        assert_eq!(retrieved.name, test_segment.name);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_segment_existing() -> Result<()> {
        let test_segment = create_test_segment();
        let store = memory_store(test_segment.clone());

        let segment1 = get_or_create_segment(&store, "first-segment").await?;
        let segment2 = get_or_create_segment(&store, "second-segment").await?;

        // Should return the first segment (existing one; name unchanged).
        assert_eq!(segment1.id, segment2.id);
        assert_eq!(segment1.name, segment2.name);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_segment_concurrent() -> Result<()> {
        let test_segment = create_test_segment();
        let store = memory_store(test_segment.clone());

        let futures: Vec<_> = (0_i32..10_i32)
            .map(|_| {
                let store_clone = store.clone();
                async move { get_or_create_segment(&store_clone, "concurrent-test").await }
            })
            .collect();

        let results: Vec<_> = future::join_all(futures).await;
        for result in results {
            let segment = result?;
            assert_eq!(segment.id, test_segment.id);
            assert_eq!(segment.name, test_segment.name);
            assert_eq!(segment.slab_size, test_segment.slab_size);
        }

        let stored_segment = store.get_segment().await?;
        let stored_segment =
            stored_segment.ok_or_else(|| eyre::eyre!("Expected segment to be stored"))?;
        assert_eq!(stored_segment.id, test_segment.id);
        Ok(())
    }
}
