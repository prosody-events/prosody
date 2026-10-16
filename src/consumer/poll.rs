//! The Kafka poll loop: serves the consumer's events and stores its offsets.
//!
//! Records do not pass through this loop. Each partition task reads its own
//! partition queue (see [`source`](crate::consumer::source)). The loop serves
//! rebalances, statistics, and errors. It also stores the committed watermark
//! of each partition so that librdkafka commits it, and it closes the client
//! when the consumer stops.
//!
//! The main entry point is the [`poll`] function.

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::runtime::Handle;
use tracing::{debug, error};

use crate::Topic;
use crate::consumer::sweep::drain_managers;
use crate::consumer::{Managers, WatermarkVersion};
use crate::heartbeat::Heartbeat;

/// Runs the Kafka poll loop until `shutdown` is set, then closes the client.
///
/// Each pass beats `heartbeat`, stores the watermarks that advanced, and
/// serves the events that arrive within `poll_interval`. The loop holds the
/// only reference to `consumer` that no partition queue holds.
pub(super) fn poll<Ctx: ConsumerContext>(
    poll_interval: Duration,
    consumer: Arc<BaseConsumer<Ctx>>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    heartbeat: &Heartbeat,
    shutdown: &AtomicBool,
) {
    let mut last_version = watermark_version.load(Ordering::Acquire);
    while !shutdown.load(Ordering::Relaxed) {
        heartbeat.beat();
        store_watermarks(&consumer, watermark_version, managers, &mut last_version);

        match consumer.poll(Timeout::After(poll_interval)) {
            None => {}
            // Every assigned partition reads its own queue. A record here
            // belongs to a partition that has no manager.
            Some(Ok(message)) => error!(
                topic = message.topic(),
                partition = message.partition(),
                offset = message.offset(),
                "record reached the consumer queue; discarding"
            ),
            Some(Err(error)) => error!("error polling for events: {error:#}"),
        }
    }

    debug!("polling stopped");
    close(consumer, managers);
}

/// Stops every partition, then drops the client.
///
/// Each partition task holds a strong reference to the client through its
/// queue. When the sweep ends, every task has stopped, so this loop holds the
/// last reference. The drop closes the client on this thread. It blocks until
/// the client leaves the group, and the final revoke finds no partition left.
fn close<Ctx: ConsumerContext>(consumer: Arc<BaseConsumer<Ctx>>, managers: &Managers) {
    Handle::current().block_on(drain_managers(managers));

    if let Some(consumer) = Arc::into_inner(consumer) {
        drop(consumer);
    } else {
        error!("a partition queue outlived its partition; the client closes elsewhere");
    }
}

/// Stores the watermark of every partition when a watermark advanced since the
/// last store. librdkafka commits the stored offsets on its commit interval.
fn store_watermarks<Ctx: ConsumerContext>(
    consumer: &BaseConsumer<Ctx>,
    watermark_version: &WatermarkVersion,
    managers: &Managers,
    last_version: &mut usize,
) {
    // Skip if no watermark updates have occurred
    let current_version = watermark_version.load(Ordering::Acquire);
    if current_version == *last_version {
        return;
    }

    // Try to acquire read lock without blocking
    let Some(managers) = managers.try_read() else {
        return;
    };

    let mut success = true;
    let mut list = TopicPartitionList::with_capacity(managers.len());

    // Build list of offsets to commit from each partition manager
    for ((topic, partition), manager) in managers.iter() {
        let Some(watermark) = manager.watermark() else {
            continue;
        };

        // Store next offset after the watermark (Kafka commits the next
        // expected offset)
        let next_offset = Offset::Offset(watermark + 1);
        if let Err(error) = list.add_partition_offset(topic, *partition, next_offset) {
            error!(
                topic = Topic::as_ref(*topic),
                partition, watermark, "failed to add offset to commit list: {error:#}"
            );

            success = false;
        }
    }

    // Skip if no offsets to commit
    if list.count() == 0 {
        debug!("nothing to commit");
        return;
    }

    // Store offsets in librdkafka for auto-commit
    debug!("storing watermarks for commit: {list:?}");
    if let Err(error) = consumer.store_offsets(&list) {
        error!("failed to store offsets: {error:#}");
        success = false;
    }

    // Update version only if all operations succeeded
    if success {
        *last_version = current_version;
        debug!("watermarks stored successfully");
    }
}
