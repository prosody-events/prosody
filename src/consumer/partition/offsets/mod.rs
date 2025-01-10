//! Manages Kafka message offset tracking and commitment.
//!
//! This module provides concurrent offset management that:
//! - Tracks reserved and committed message offsets
//! - Maintains a watermark of the highest contiguous committed offset
//! - Detects and reports stalled message processing
//! - Ensures ordered offset commitment while enabling concurrent processing
//!
//! The core type is [`OffsetTracker`], which coordinates offset reservations,
//! commitments, and watermark updates through a background task.

use crossbeam_utils::CachePadded;
use educe::Educe;
use humantime::format_duration;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::{channel, OwnedPermit, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};
use tokio::{select, spawn};
use tracing::{debug, error, info, instrument, warn};

use crate::{Offset, Partition, Topic};

#[cfg(test)]
mod test;

/// Manages offset tracking and commitment for a Kafka partition.
///
/// Coordinates offset reservations, commitments, and watermark updates through
/// a background task. Detects stalled message processing and maintains the
/// highest contiguous committed offset.
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OffsetTracker {
    /// Channel for sending offset management actions
    #[educe(Debug(ignore))]
    action_tx: Sender<Action>,

    /// Highest contiguous committed offset
    #[educe(Debug(ignore))]
    watermark: Arc<CachePadded<AtomicI64>>,

    /// Indicates if message processing has stalled
    #[educe(Debug(ignore))]
    is_stalled: Arc<CachePadded<AtomicBool>>,

    /// Background task handle for watermark updates
    #[educe(Debug(ignore))]
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl OffsetTracker {
    /// Creates a new offset tracker.
    ///
    /// # Arguments
    ///
    /// * `topic` - The Kafka topic
    /// * `partition` - The topic partition
    /// * `max_uncommitted` - Maximum number of uncommitted offsets allowed
    /// * `stall_threshold` - Duration after which processing is considered
    ///   stalled
    /// * `watermark_version` - Shared counter tracking watermark changes
    pub fn new(
        topic: Topic,
        partition: Partition,
        max_uncommitted: usize,
        stall_threshold: Duration,
        watermark_version: Arc<CachePadded<AtomicUsize>>,
    ) -> Self {
        // Create channel with space for all uncommitted offsets plus one
        let (action_tx, action_rx) = channel(max_uncommitted + 1);
        let watermark = Arc::new(CachePadded::new(AtomicI64::new(-1)));
        let is_stalled = Arc::new(CachePadded::new(AtomicBool::new(false)));

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

    /// Reserves an offset for future commitment.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to reserve
    ///
    /// # Returns
    ///
    /// The reserved offset handle if successful
    ///
    /// # Errors
    ///
    /// Returns `OffsetTrackerError::Shutdown` if the tracker is shutting down
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

    /// Returns the current highest contiguous committed offset.
    ///
    /// # Returns
    ///
    /// The current watermark if any offsets have been committed
    pub fn watermark(&self) -> Option<Offset> {
        fetch_watermark(&self.watermark)
    }

    /// Checks if message processing has stalled.
    ///
    /// # Returns
    ///
    /// `true` if processing has stalled, `false` otherwise
    pub fn is_stalled(&self) -> bool {
        self.is_stalled.load(Ordering::Acquire)
    }

    /// Shuts down the offset tracker.
    ///
    /// Closes the action channel and waits for the background task to complete.
    ///
    /// # Returns
    ///
    /// The final watermark value if any offsets were committed
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

/// A reserved offset pending commitment.
///
/// Provides methods to commit or abort the offset reservation.
/// Automatically aborts the reservation if dropped without commitment.
#[derive(Educe)]
#[educe(Debug)]
pub struct UncommittedOffset {
    /// The reserved offset value
    offset: Offset,

    /// Channel permit for sending the commit action
    #[educe(Debug(ignore))]
    permit: Option<OwnedPermit<Action>>,
}

impl UncommittedOffset {
    /// Commits this offset.
    pub fn commit(mut self) {
        let Some(permit) = self.permit.take() else {
            error!("offset {} already committed", self.offset);
            return;
        };
        permit.send(Action::commit(self.offset));
    }

    /// Aborts committing this offset.
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

/// An offset management action.
#[derive(Clone, Debug)]
struct Action {
    offset: Offset,
    operation: Operation,
}

impl Action {
    /// Creates a new Take action.
    fn take(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Take(Instant::now()),
        }
    }

    /// Creates a new Commit action.
    fn commit(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Commit,
        }
    }
}

/// Available offset operations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Operation {
    /// Reserve an offset with timestamp
    Take(Instant),
    /// Commit a previously reserved offset
    Commit,
}

/// Errors that may occur during offset tracking.
#[derive(Debug, Error)]
pub enum OffsetTrackerError {
    /// The tracker is shutting down and cannot process the request
    #[error("operation failed: shutdown")]
    Shutdown,
}

/// Retrieves the current watermark value.
///
/// # Arguments
///
/// * `watermark` - The atomic watermark value
///
/// # Returns
///
/// The watermark if it contains a valid offset
fn fetch_watermark(watermark: &CachePadded<AtomicI64>) -> Option<Offset> {
    let watermark = watermark.load(Ordering::Acquire);
    (watermark >= 0).then_some(watermark)
}

/// Background task that processes offset actions and updates the watermark.
///
/// Maintains an ordered map of offset operations and advances the watermark
/// when contiguous ranges of offsets are committed. Also monitors for stalled
/// message processing.
///
/// # Arguments
///
/// * `topic` - The Kafka topic
/// * `partition` - The topic partition
/// * `action_rx` - Channel receiving offset actions
/// * `watermark` - The shared watermark value
/// * `watermark_version` - Counter tracking watermark changes
/// * `stall_threshold` - Duration after which processing is considered stalled
/// * `is_stalled` - Flag indicating stalled processing
async fn track_watermark(
    topic: Topic,
    partition: Partition,
    mut action_rx: Receiver<Action>,
    watermark: Arc<CachePadded<AtomicI64>>,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
    stall_threshold: Duration,
    is_stalled: Arc<CachePadded<AtomicBool>>,
) {
    let topic = topic.as_ref();

    // Track offset operations in order
    let mut watermarks = BTreeMap::new();

    loop {
        let stall_future = wait_for_stall(stall_threshold, &is_stalled, &watermarks);

        select! {
            // Handle stalled processing detection
            Some((offset, take_time)) = stall_future => {
                is_stalled.store(true, Ordering::Release);
                warn!(
                    "{topic}:{partition} has stalled at offset {offset} \
                    for {}, which exceeded the stall threshold of {}",
                    format_duration(take_time.elapsed()),
                    format_duration(stall_threshold),
                );
            }

            // Process incoming offset actions
            maybe_action = action_rx.recv() => {
                let Some(action) = maybe_action else {
                    break;
                };

                watermarks.insert(action.offset, action.operation);

                let mut new_watermark = None;

                // Update watermark by finding contiguous committed offsets
                while let Some(entry) = watermarks.first_entry() {
                    if *entry.get() == Operation::Commit {
                        let (offset, _) = entry.remove_entry();
                        new_watermark = Some(offset);
                    } else {
                        break;
                    }
                }

                // Apply watermark update if found
                if let Some(new_offset) = new_watermark {
                    watermark.store(new_offset, Ordering::Release);
                    watermark_version.fetch_add(1, Ordering::AcqRel);

                    // Clear stalled state if it was set
                    if is_stalled.fetch_and(false, Ordering::AcqRel) {
                        info!("{topic}:{partition} is no longer stalled");
                    }
                }
            }
        }
    }

    debug!("watermark tracking shutdown");
}

/// Monitors for stalled message processing.
///
/// # Arguments
///
/// * `stall_threshold` - Duration after which processing is considered stalled
/// * `is_stalled` - Flag indicating stalled processing
/// * `watermarks` - Current offset operations map
///
/// # Returns
///
/// The stalled offset and its take timestamp if stalled
async fn wait_for_stall(
    stall_threshold: Duration,
    is_stalled: &AtomicBool,
    watermarks: &BTreeMap<Offset, Operation>,
) -> Option<(Offset, Instant)> {
    // Skip if already stalled
    if is_stalled.load(Ordering::Acquire) {
        return None;
    }

    // Find oldest uncommitted offset
    let (offset, take_time) = watermarks.iter().find_map(|(offset, operation)| {
        let Operation::Take(take_time) = operation else {
            return None;
        };
        Some((*offset, *take_time))
    })?;

    // Wait until stall threshold is exceeded
    let expiration = take_time + stall_threshold;
    sleep_until(expiration).await;

    Some((offset, take_time))
}
