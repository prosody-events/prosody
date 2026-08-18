use super::{
    ActiveRequests, BaseConsumer, Consumer, Duration, KafkaLoaderError, KafkaResult, Offset,
    Partition, Request, SmallVec, Timeout, Topic, TopicPartitionList, debug, error, warn,
};

pub(super) fn handle_request<P>(
    request: Request<P>,
    active: &mut ActiveRequests<P>,
    consumer: &BaseConsumer,
) {
    use std::collections::btree_map::Entry;

    let Request {
        topic,
        partition,
        offset,
        tx,
    } = request;

    debug!(
        topic = %topic,
        partition = partition,
        offset = offset,
        "Processing message load request"
    );

    if let Err(error) = assign_if_needed(active, consumer, topic, partition, offset) {
        error!(
            topic = %topic,
            partition = partition,
            offset = offset,
            error = %format_args!("{error:#}"),
            "Failed to assign partition for message load"
        );
        let _ = tx.send(Err(KafkaLoaderError::Kafka(error)));
        return;
    }

    let state = active.entry((topic, partition)).or_default();

    match state.offsets.entry(offset) {
        Entry::Vacant(entry) => {
            // First request for this offset
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                "First request for offset, will decode when polled"
            );
            let mut senders = SmallVec::new();
            senders.push(tx);
            entry.insert(senders);
        }
        Entry::Occupied(mut entry) => {
            // Subsequent request - coalesce with existing
            debug!(
                topic = %topic,
                partition = partition,
                offset = offset,
                coalesced_count = entry.get().len() + 1,
                "Coalescing with existing request for same offset"
            );
            entry.get_mut().push(tx);
        }
    }
}

/// Seeks partitions to their first active offset when beneficial.
///
/// Seeks are a performance optimization: reading 100 messages (~1-10ms) is
/// faster than seeking (~10-100ms) when within the discard threshold.
///
/// **Strategy:** Seeks to deleted offsets succeed and auto-position at LSO
/// (same as assign behavior). Lazy validation in [`process_poll_result`]
/// detects deletions when messages arrive. Seek failures (network errors,
/// Kafka down) trigger retry via the caller's continue loop.
///
/// Partitions whose `pending_seek` flag is already set are skipped: a seek is
/// only materialised (position advances out of Invalid) after the consumer
/// receives a message from the broker. Re-seeking before that point resets the
/// consumer back to the (possibly deleted) target offset on every iteration,
/// preventing the broker's auto-reset from delivering the LSO message.
/// `pending_seek` is set here for each partition whose seek succeeds and is
/// cleared in [`process_poll_result`] when a message is received.
///
/// On a seek failure, the caller must NOT poll — the consumer's position is
/// unknown and polling would risk misclassifying pending offsets as deleted.
/// The caller should retry the seek on the next iteration.
pub(super) fn seek_to_first_active_offset<P>(
    active: &mut ActiveRequests<P>,
    consumer: &BaseConsumer,
    discard_threshold: i64,
    seek_timeout: Duration,
) -> KafkaResult<()> {
    if active.is_empty() {
        return Ok(());
    }

    let mut seek_list = TopicPartitionList::new();

    // One call retrieves positions for all assigned partitions from local
    // librdkafka state — no network round-trip.
    let positions = consumer.position()?;

    for ((topic, partition), state) in active.iter() {
        let Some((&min_offset, _)) = state.offsets.first_key_value() else {
            continue;
        };

        // Skip if we already have a seek in flight that will land at or before
        // min_offset. If a new lower-offset request arrived after the seek was
        // dispatched, min_offset < pending_seek and we must re-seek.
        if state.pending_seek.is_some_and(|s| s <= min_offset) {
            continue;
        }

        let current_position = positions
            .find_partition(topic.as_ref(), *partition)
            .and_then(|elem| match elem.offset() {
                rdkafka::Offset::Offset(offset) => Some(offset),
                _ => None,
            });

        // Avoid expensive seeks when close enough to read sequentially.
        // Seek (~10-100ms) vs sequential read of N messages (~1-10ms):
        // - Don't seek: within threshold and before target (sequential read cheaper)
        // - Seek: past target (backward), too far behind
        // - Seek: unknown position (Invalid) — position() returns Invalid after
        //   incremental_assign() until the first message is consumed. assign_if_needed
        //   only assigns on the first request; concurrent lower-offset requests skip
        //   re-assignment, so the consumer may be anchored above min_offset. Always
        //   seek when Invalid to land at the correct starting point.
        let should_seek = match current_position {
            None => true,
            Some(position) => {
                let past_target = position > min_offset;
                let too_far_behind = position + discard_threshold < min_offset;
                past_target || too_far_behind
            }
        };

        debug!(
            topic = %AsRef::<str>::as_ref(topic),
            partition = partition,
            min_offset = min_offset,
            current_position = ?current_position,
            should_seek = should_seek,
            "Evaluating seek decision for partition"
        );

        if should_seek {
            debug!(
                topic = %AsRef::<str>::as_ref(topic),
                partition = partition,
                target_offset = min_offset,
                "Adding partition to seek list"
            );
            seek_list.add_partition_offset(
                topic.as_ref(),
                *partition,
                rdkafka::Offset::Offset(min_offset),
            )?;
        }
    }

    if seek_list.count() == 0 {
        return Ok(());
    }

    debug!(
        partition_count = seek_list.count(),
        "Executing seek operation"
    );
    let result = consumer.seek_partitions(seek_list, Timeout::After(seek_timeout))?;

    // Set pending_seek for each partition that succeeded before checking for
    // errors. This way, if partition A succeeds and partition B fails, A's flag
    // is correctly set before we return the error.
    for elem in result.elements() {
        if let Err(e) = elem.error() {
            warn!(
                topic = elem.topic(),
                partition = elem.partition(),
                offset = ?elem.offset(),
                "Seek failed for partition: {e:#}"
            );
            return Err(e);
        }
        debug!(
            topic = elem.topic(),
            partition = elem.partition(),
            offset = ?elem.offset(),
            "Seek succeeded for partition"
        );
        if let rdkafka::Offset::Offset(sought_offset) = elem.offset() {
            let key = (Topic::from(elem.topic()), elem.partition());
            if let Some(state) = active.get_mut(&key) {
                state.pending_seek = Some(sought_offset);
            }
        }
    }

    Ok(())
}

/// Assigns a partition at the requested offset if not already assigned.
///
/// Uses manual partition assignment with the exact offset. If the offset has
/// been deleted, rdkafka auto-positions at the Log Start Offset (LSO). Lazy
/// validation in [`process_poll_result`] detects this case by comparing
/// requested vs received offsets.
fn assign_if_needed<P>(
    active: &ActiveRequests<P>,
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
    offset: Offset,
) -> KafkaResult<()> {
    if active.contains_key(&(topic, partition)) {
        debug!(
            topic = %topic,
            partition = partition,
            offset = offset,
            "Partition already assigned, skipping assignment"
        );
        return Ok(());
    }

    // Incrementally assign partition at requested offset.
    // Must use incremental_assign() to ADD to existing assignments, not replace
    // them. This pairs with incremental_unassign() used when partitions are
    // fulfilled.
    //
    // Note: incremental_assign() with deleted offset auto-resets to LSO cleanly.
    // Lazy validation in process_poll_result detects when received != requested.
    let mut to_assign = TopicPartitionList::new();
    to_assign.add_partition_offset(topic.as_ref(), partition, rdkafka::Offset::Offset(offset))?;
    consumer.incremental_assign(&to_assign)?;

    debug!(
        topic = %topic,
        partition = partition,
        offset = offset,
        "Assigned partition for message loading"
    );

    Ok(())
}

/// Unassigns a partition from the consumer.
///
/// Removes the partition assignment since all requested offsets have been
/// fulfilled. This keeps resource usage minimal by only holding assignments
/// for partitions with active requests.
pub(super) fn unassign_partition(
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
) -> KafkaResult<()> {
    let mut to_unassign = TopicPartitionList::new();
    to_unassign.add_partition(topic.as_ref(), partition);
    consumer.incremental_unassign(&to_unassign)?;
    debug!(
        topic = %topic,
        partition = partition,
        "Unassigned partition after fulfilling all deferred load requests"
    );
    Ok(())
}
