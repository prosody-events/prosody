//! Segment bootstrap helper used by the [`TimerManager`] constructor before
//! the scheduler actor spawns.
//!
//! [`TimerManager`]: crate::timers::manager::TimerManager

use crate::timers::error::TimerManagerError;
use crate::timers::store::{StoredSegment, TriggerStore};

/// Retrieves or creates the store's segment.
///
/// If a segment already exists in the store, it is returned with its slab
/// watermark. Otherwise, a new segment is inserted using the store's segment
/// identity, and it has no watermark.
pub(super) async fn get_or_create_segment<T>(
    store: &T,
) -> Result<StoredSegment, TimerManagerError<T::Error>>
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

    Ok(store.segment().into())
}

#[cfg(test)]
mod tests;
