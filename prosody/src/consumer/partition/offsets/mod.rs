//! Manages and synchronizes offset handling in Kafka, allowing
//! for concurrent processing while ensuring offsets are committed in order. The
//! module provides mechanisms to reserve and commit offsets, and
//! maintains a watermark to track the latest contiguous committed offset.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;
use educe::Educe;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::{channel, OwnedPermit, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{error, instrument, warn};

use crate::Offset;

#[cfg(test)]
mod test;

/// Manages uncommitted offsets and tracks the highest successfully committed
/// offset. Utilizes atomic operations and a mutex-guarded task handle for
/// managing background operations that update the watermark based on committed
/// offsets.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OffsetTracker {
    /// Channel to transmit actions related to offset management.
    #[educe(Debug(ignore))]
    action_tx: Sender<Action>,

    /// Stores the highest committed offset in an atomic variable.
    #[educe(Debug(ignore))]
    watermark: Arc<CachePadded<AtomicI64>>,

    /// Manages the background task for watermark updates.
    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// Holds an offset that has been reserved but not yet committed, with
/// mechanisms to commit the offset.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedOffset {
    /// The specific offset that has been taken and awaits commitment.
    offset: Offset,

    /// Permit to send the offset commit message.
    #[educe(Debug(ignore))]
    permit: Option<OwnedPermit<Action>>,
}

/// Describes an operation to be performed on an offset, such as reserving or
/// committing it.
#[derive(Clone, Debug)]
struct Action {
    offset: Offset,
    operation: Operation,
}

/// Defines possible operations on offsets
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Operation {
    Take,
    Commit,
}

impl OffsetTracker {
    /// Initializes a new `OffsetTracker` with the specified capacity for
    /// uncommitted offsets. Spawns a background task to monitor and commit
    /// offsets, updating the watermark accordingly.
    ///
    /// # Arguments
    /// * `max_uncommitted` - Maximum number of uncommitted offsets allowed.
    /// * `watermark_version` - Shared atomic variable to track changes in the
    ///   watermark.
    pub fn new(max_uncommitted: usize, watermark_version: Arc<CachePadded<AtomicUsize>>) -> Self {
        let (action_tx, action_rx) = channel(max_uncommitted + 1);
        let watermark = Arc::new(CachePadded::new(AtomicI64::new(-1)));

        let handle = Arc::new(Mutex::new(Some(spawn(track_watermark(
            action_rx,
            watermark.clone(),
            watermark_version,
        )))));

        Self {
            action_tx,
            watermark,
            handle,
        }
    }

    /// Reserves an offset for potential future commitment. Provides an
    /// UncommittedOffset that can be committed or rolled back.
    ///
    /// # Arguments
    /// * `offset` - The offset to reserve.
    ///
    /// # Returns
    /// A Result containing either an UncommittedOffset or an
    /// OffsetTrackerError.
    ///
    /// # Errors
    /// Returns `OffsetTrackerError::Shutdown` if the system is shutting down
    /// and cannot accept new reservations.
    #[instrument(level = "debug")]
    pub async fn take(&self, offset: Offset) -> Result<UncommittedOffset, OffsetTrackerError> {
        let permit = Some(
            self.action_tx
                .clone()
                .reserve_owned()
                .await
                .map_err(|_| OffsetTrackerError::Shutdown)?,
        );

        self.action_tx
            .send(Action::take(offset))
            .await
            .map_err(|_| OffsetTrackerError::Shutdown)?;

        Ok(UncommittedOffset { offset, permit })
    }

    /// Fetches the current watermark, representing the highest committed
    /// offset, if available.
    ///
    /// # Returns
    /// An Option containing the current watermark if it is valid.
    pub fn watermark(&self) -> Option<Offset> {
        fetch_watermark(&self.watermark)
    }

    /// Shuts down the OffsetTracker, closing the action channel and awaiting
    /// the completion of the background task to ensure all pending actions
    /// are resolved.
    ///
    /// # Returns
    /// An Option containing the last known valid watermark if available.
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        drop(self.action_tx);
        let Some(handle) = self.handle.lock().take() else {
            return fetch_watermark(&self.watermark);
        };

        let _ = handle.await;
        fetch_watermark(&self.watermark)
    }
}

/// Fetches the watermark from an atomic variable, ensuring it reflects
/// committed data.
///
/// # Returns
/// An Option containing the watermark if it represents a committed offset.
fn fetch_watermark(watermark: &CachePadded<AtomicI64>) -> Option<Offset> {
    let watermark = watermark.load(Ordering::Acquire);
    (watermark >= 0).then_some(watermark)
}

/// Enumerates potential errors that can occur during offset management.
#[derive(Debug, Error)]
pub enum OffsetTrackerError {
    /// Indicates a failure due to the system shutting down, preventing
    /// operation completion.
    #[error("operation failed: shutdown")]
    Shutdown,
}

impl UncommittedOffset {
    /// Commits the reserved offset, finalizing its state.
    pub fn commit(mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        permit.send(Action::commit(self.offset));
    }
}

impl Drop for UncommittedOffset {
    /// Logs a warning if an offset is dropped without being committed,
    /// indicating potential data loss.
    fn drop(&mut self) {
        let Some(_) = self.permit.take() else {
            return;
        };

        warn!(%self.offset, "offset was dropped without committing");
    }
}

impl Action {
    /// Creates a Take action for a specific offset.
    fn take(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Take,
        }
    }

    /// Creates a Commit action for a specific offset.
    fn commit(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Commit,
        }
    }
}

/// Processes actions from a receiver to update the watermark based on committed
/// offsets. This background task adjusts the watermark to reflect the highest
/// contiguous committed offset. It maintains a sorted map of offsets and
/// updates the watermark whenever it can extend the contiguous sequence of
/// committed offsets. The process continues until the action channel closes
/// during system shutdown.
///
/// # Arguments
/// * `action_rx` - Receiver for offset actions, each either reserving or
///   committing an offset.
/// * `watermark` - Shared atomic variable tracking the highest committed
///   offset.
/// * `watermark_version` - Atomic variable to track watermark updates.
async fn track_watermark(
    mut action_rx: Receiver<Action>,
    watermark: Arc<CachePadded<AtomicI64>>,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
) {
    // Map to keep track of offset actions.
    let mut watermarks = BTreeMap::new();

    // Process actions from the receiver until the channel is closed.
    while let Some(action) = action_rx.recv().await {
        // Insert action into the map.
        watermarks.insert(action.offset, action.operation);

        // Initialize a placeholder for potentially updating the watermark.
        let mut new_watermark = None;

        // Attempt to update the watermark based on the lowest available offsets.
        // This loop checks each entry, advancing the watermark when contiguous
        // committed offsets are found and removing them to keep the dataset
        // minimal.
        while let Some(entry) = watermarks.first_entry() {
            // If the lowest offset in the map is committed, update the watermark.
            if *entry.get() == Operation::Commit {
                let (offset, _) = entry.remove_entry();
                new_watermark = Some(offset);
            } else {
                break;
            }
        }

        // Update the shared watermark if a new one was established.
        if let Some(new_offset) = new_watermark {
            watermark.store(new_offset, Ordering::Release);

            // Increment the version to indicate a change.
            watermark_version.fetch_add(1, Ordering::AcqRel);
        }
    }
}
