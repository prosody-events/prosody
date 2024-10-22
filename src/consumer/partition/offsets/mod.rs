//! Manages and synchronizes offset handling in Kafka, enabling concurrent
//! processing while ensuring ordered offset commitment. This module offers
//! mechanisms to reserve and commit offsets, and maintains a watermark to track
//! the latest contiguous committed offset.

use crossbeam_utils::CachePadded;
use educe::Educe;
use humantime::format_duration;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc::{channel, OwnedPermit, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep_until;
use tokio::{select, spawn};
use tracing::{debug, error, info, instrument, warn};

use crate::{Offset, Partition, Topic};

#[cfg(test)]
mod test;

/// Manages uncommitted offsets and tracks the highest successfully committed
/// offset.
///
/// Uses atomic operations and a mutex-guarded task handle for managing
/// background operations that update the watermark based on committed offsets.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OffsetTracker {
    /// Channel to transmit actions related to offset management.
    #[educe(Debug(ignore))]
    action_tx: Sender<Action>,

    /// Stores the highest committed offset in an atomic variable.
    #[educe(Debug(ignore))]
    watermark: Arc<CachePadded<AtomicI64>>,

    #[educe(Debug(ignore))]
    is_stalled: Arc<AtomicBool>,

    /// Manages the background task for watermark updates.
    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

/// Holds an offset that has been reserved but not yet committed.
///
/// Provides mechanisms to commit or abort the offset.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedOffset {
    /// The specific offset that has been taken and awaits commitment.
    offset: Offset,

    /// Permit to send the offset commit message.
    #[educe(Debug(ignore))]
    permit: Option<OwnedPermit<Action>>,
}

/// Describes an operation to be performed on an offset.
#[derive(Clone, Debug)]
struct Action {
    offset: Offset,
    operation: Operation,
}

/// Defines possible operations on offsets.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Operation {
    Take(Instant),
    Commit,
}

impl OffsetTracker {
    /// Creates a new `OffsetTracker` with the specified capacity for
    /// uncommitted offsets.
    ///
    /// Spawns a background task to monitor and commit offsets, updating the
    /// watermark accordingly.
    ///
    /// # Arguments
    ///
    /// * `max_uncommitted` - Maximum number of uncommitted offsets allowed.
    /// * `watermark_version` - Shared atomic variable to track changes in the
    ///   watermark.
    pub fn new(
        topic: Topic,
        partition: Partition,
        max_uncommitted: usize,
        stall_threshold: Duration,
        watermark_version: Arc<CachePadded<AtomicUsize>>,
    ) -> Self {
        let (action_tx, action_rx) = channel(max_uncommitted + 1);
        let watermark = Arc::new(CachePadded::new(AtomicI64::new(-1)));
        let is_stalled = Arc::new(AtomicBool::new(false));

        let handle = Arc::new(Mutex::new(Some(spawn(track_watermark(
            topic,
            partition,
            action_rx,
            watermark.clone(),
            watermark_version,
            stall_threshold,
            is_stalled.clone(),
        )))));

        Self {
            action_tx,
            watermark,
            is_stalled,
            handle,
        }
    }

    /// Reserves an offset for potential future commitment.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to reserve.
    ///
    /// # Returns
    ///
    /// A `Result` containing either an `UncommittedOffset` or an
    /// `OffsetTrackerError`.
    ///
    /// # Errors
    ///
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
    /// offset.
    ///
    /// # Returns
    ///
    /// An `Option` containing the current watermark if it is valid.
    pub fn watermark(&self) -> Option<Offset> {
        fetch_watermark(&self.watermark)
    }

    pub fn is_stalled(&self) -> bool {
        self.is_stalled.load(Ordering::Acquire)
    }

    /// Shuts down the `OffsetTracker`, closing the action channel and awaiting
    /// the completion of the background task to ensure all pending actions are
    /// resolved.
    ///
    /// # Returns
    ///
    /// An `Option` containing the last known valid watermark if available.
    #[instrument(level = "debug")]
    pub async fn shutdown(self) -> Option<Offset> {
        drop(self.action_tx);
        let Some(handle) = self.handle.lock().take() else {
            warn!("offset tracker already shutdown");
            return fetch_watermark(&self.watermark);
        };

        let _ = handle.await;
        fetch_watermark(&self.watermark)
    }
}

/// Fetches the watermark from an atomic variable, ensuring it reflects
/// committed data.
///
/// # Arguments
///
/// * `watermark` - The atomic variable containing the watermark.
///
/// # Returns
///
/// An `Option` containing the watermark if it represents a committed offset.
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
    /// Commits the offset.
    pub fn commit(mut self) {
        let Some(permit) = self.permit.take() else {
            error!("offset {} already committed", self.offset);
            return;
        };
        permit.send(Action::commit(self.offset));
    }

    /// Aborts committing the offset.
    pub fn abort(mut self) {
        let Some(_) = self.permit.take() else {
            error!("offset {} already committed", self.offset);
            return;
        };

        warn!(self.offset, "commit aborted");
    }
}

impl Drop for UncommittedOffset {
    fn drop(&mut self) {
        let Some(_) = self.permit.take() else {
            return;
        };

        warn!(self.offset, "offset was dropped without committing");
    }
}

impl Action {
    /// Creates a Take action for a specific offset.
    fn take(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Take(Instant::now()),
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
/// offsets.
///
/// This background task adjusts the watermark to reflect the highest contiguous
/// committed offset. It maintains a sorted map of offsets and updates the
/// watermark whenever it can extend the contiguous sequence of committed
/// offsets. The process continues until the action channel closes during system
/// shutdown.
///
/// # Arguments
///
/// * `action_rx` - Receiver for offset actions, each either reserving or
///   committing an offset.
/// * `watermark` - Shared atomic variable tracking the highest committed
///   offset.
/// * `watermark_version` - Atomic variable to track watermark updates.
async fn track_watermark(
    topic: Topic,
    partition: Partition,
    mut action_rx: Receiver<Action>,
    watermark: Arc<CachePadded<AtomicI64>>,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
    stall_threshold: Duration,
    is_stalled: Arc<AtomicBool>,
) {
    let topic = topic.as_ref();

    // Map to keep track of offset actions.
    let mut watermarks = BTreeMap::new();

    loop {
        let stall_future = wait_for_stall(stall_threshold, &is_stalled, &watermarks);

        select! {
            Some((offset, take_time)) = stall_future => {
                is_stalled.store(true, Ordering::Release);
                warn!(
                    "{topic}:{partition} has stalled at offset {offset} \
                    for {}, which exceeded the stall threshold of {}",
                    format_duration(take_time.elapsed()),
                    format_duration(stall_threshold),
                );
            }

            maybe_action = action_rx.recv() => {
                let Some(action) = maybe_action else {
                    break;
                };

                // Insert action into the map.
                watermarks.insert(action.offset, action.operation);

                // Initialize a placeholder for potentially updating the watermark.
                let mut new_watermark = None;

                // Attempt to update the watermark based on the lowest available offsets.
                // This loop checks each entry, advancing the watermark when contiguous
                // committed offsets are found and removing them to keep the dataset minimal.
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

                    if is_stalled.load(Ordering::Acquire) {
                        info!("{topic}:{partition} is no longer stalled");
                    }

                    is_stalled.store(false, Ordering::Release);
                }
            }
        }
    }

    debug!("watermark tracking shutdown");
}

async fn wait_for_stall(
    stall_threshold: Duration,
    is_stalled: &AtomicBool,
    watermarks: &BTreeMap<Offset, Operation>,
) -> Option<(Offset, Instant)> {
    // Don't set stalled if it is already set
    if is_stalled.load(Ordering::Acquire) {
        return None;
    }

    let (offset, take_time) = watermarks.iter().find_map(|(offset, operation)| {
        let Operation::Take(take_time) = operation else {
            return None;
        };
        Some((*offset, *take_time))
    })?;

    let expiration = take_time + stall_threshold;
    sleep_until(expiration.into()).await;

    Some((offset, take_time))
}
