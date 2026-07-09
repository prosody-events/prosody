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
pub(super) async fn get_or_create_segment<T>(
    store: &T,
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
mod tests;
