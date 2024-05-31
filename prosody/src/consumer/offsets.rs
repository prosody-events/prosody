use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use thiserror::Error;
use tokio::spawn;
use tokio::sync::mpsc::{channel, OwnedPermit, Receiver, Sender};
use tracing::warn;

use crate::Offset;

pub struct OffsetTracker {
    action_tx: Sender<Action>,
    watermark: Arc<CachePadded<AtomicI64>>,
}

pub struct OffsetPermit {
    offset: Offset,
    permit: Option<OwnedPermit<Action>>,
}

struct Action {
    offset: Offset,
    operation: Operation,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Operation {
    Take,
    Commit,
}

impl OffsetTracker {
    pub fn new(max_uncommitted: usize, watermark_version: Arc<CachePadded<AtomicUsize>>) -> Self {
        let (action_tx, action_rx) = channel(max_uncommitted);
        let watermark = Arc::new(CachePadded::new(AtomicI64::new(-1)));

        spawn(track_watermark(
            action_rx,
            watermark.clone(),
            watermark_version,
        ));

        Self {
            action_tx,
            watermark,
        }
    }

    pub async fn take(&self, offset: Offset) -> Result<OffsetPermit, OffsetTrackerError> {
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

        Ok(OffsetPermit { offset, permit })
    }

    pub fn watermark(&self) -> Option<Offset> {
        let watermark = self.watermark.load(Ordering::Acquire);
        (watermark >= 0).then_some(watermark)
    }
}

#[derive(Debug, Error)]
pub enum OffsetTrackerError {
    #[error("operation failed: shutdown")]
    Shutdown,
}

impl OffsetPermit {
    pub fn commit(mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        permit.send(Action::commit(self.offset));
    }
}

impl Drop for OffsetPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };

        warn!(%self.offset, "permit was dropped without committing; committing offset");
        permit.send(Action::commit(self.offset));
    }
}

impl Action {
    fn take(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Take,
        }
    }

    fn commit(offset: Offset) -> Self {
        Self {
            offset,
            operation: Operation::Commit,
        }
    }
}

async fn track_watermark(
    mut action_rx: Receiver<Action>,
    watermark: Arc<CachePadded<AtomicI64>>,
    watermark_version: Arc<CachePadded<AtomicUsize>>,
) {
    let mut watermarks = BTreeMap::new();
    while let Some(action) = action_rx.recv().await {
        watermarks.insert(action.offset, action.operation);

        let mut new_watermark = None;
        while let Some(entry) = watermarks.first_entry() {
            if *entry.get() == Operation::Commit {
                let (offset, _) = entry.remove_entry();
                new_watermark = Some(offset);
            } else {
                break;
            }
        }

        if let Some(offset) = new_watermark {
            watermark.store(offset, Ordering::Release);
            watermark_version.fetch_add(1, Ordering::AcqRel);
        }
    }
}
