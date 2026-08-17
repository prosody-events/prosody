use super::request::{handle_request, seek_to_first_active_offset, unassign_partition};
use super::{
    ActiveRequests, BTreeMap, BaseConsumer, BorrowedMessage, Codec, DecodedMessage, Handle,
    HashMap, Heartbeat, KafkaError, KafkaLoaderError, LoaderConfiguration, Message, Offset,
    Partition, Request, Responses, Span, SpanRelation, TextMapCompositePropagator, Timeout, Topic,
    TryRecvError, debug, decode_record, error, mpsc, new_propagator, related_span, select, warn,
};
use crate::subsystem::SubsystemName;

pub(super) fn poll_loop<C: Codec>(
    mut rx: mpsc::Receiver<Request<C::Payload>>,
    consumer: &BaseConsumer,
    config: &LoaderConfiguration,
    heartbeat: &Heartbeat,
) where
    C::Payload: Clone,
{
    let mut active: ActiveRequests<C::Payload> = HashMap::default();
    let propagator = new_propagator();
    let mut codec = C::default();

    debug!("Message loader poll loop started");

    loop {
        heartbeat.beat();

        // Drain all pending requests
        loop {
            match rx.try_recv() {
                Ok(request) => handle_request(request, &mut active, consumer),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    debug!("Message loader poll loop shutting down");
                    return;
                }
            }
        }

        // If idle, wait for a request (with heartbeat timeout)
        if active.is_empty() {
            debug!("Poll loop idle, waiting for requests");
            if let Some(request) = Handle::current().block_on(async {
                select! {
                    r = rx.recv() => r,
                    () = heartbeat.next() => None,
                }
            }) {
                handle_request(request, &mut active, consumer);
            }

            // Channel close (None from recv) detected on next drain iteration
            continue;
        }

        debug!(
            active_partitions = active.len(),
            total_pending_offsets = active.values().map(|s| s.offsets.len()).sum::<usize>(),
            "Poll loop iteration with active requests"
        );

        // Seek partitions that need it. Per-partition pending_seek flags prevent
        // re-seeking a partition before the broker's auto-reset delivers the LSO
        // message. Partitions with pending_seek are skipped inside the function.
        if let Err(error) = seek_to_first_active_offset(
            &mut active,
            consumer,
            config.discard_threshold,
            config.seek_timeout,
        ) {
            warn!("Seek failed, retrying next iteration: {error:#}");
            // Do NOT fall through to poll(). If the seek failed, the
            // consumer's position is unknown — polling and running
            // split_off with an untrustworthy position would
            // misclassify pending offsets as deleted, which is data
            // corruption. Skip this iteration and retry the seek next
            // time around. The seek_timeout provides implicit backoff
            // (~5s) so this does not spin.
            continue;
        }

        // Poll once per iteration
        let Some(result) = consumer.poll(Timeout::After(config.poll_interval)) else {
            debug!("Poll returned no message");
            continue;
        };

        process_poll_result::<C>(
            result,
            &propagator,
            &mut codec,
            &mut active,
            consumer,
            config.responder.as_ref(),
        );
    }
}

/// Processes a poll result and fulfills any matching active requests.
///
/// Performs lazy validation to detect deleted offsets by comparing requested
/// offsets against the received offset. Decodes the message using the first
/// response's permit and sends the result to all waiting channels. Unassigns
/// the partition if all requests are fulfilled.
fn process_poll_result<C: Codec>(
    result: Result<BorrowedMessage, KafkaError>,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
    active: &mut ActiveRequests<C::Payload>,
    consumer: &BaseConsumer,
    responder: Option<&SubsystemName>,
) where
    C::Payload: Clone,
{
    let mut message = match result {
        Ok(message) => message,
        Err(error) => {
            error!(error = %format_args!("{error:#}"), "Error polling for message");
            return;
        }
    };

    let msg_topic = Topic::from(message.topic());
    let msg_partition = message.partition();
    let msg_offset = message.offset();

    debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset, "Polled message");

    let Some(state) = active.get_mut(&(msg_topic, msg_partition)) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Received message for partition with no active requests");
        return;
    };

    // LAZY VALIDATION: Detect deleted offsets (LSO moved forward).
    //
    // `pending_seek` records the offset we sought to when the seek was
    // dispatched. Requests that arrived in the channel AFTER the seek was
    // issued have offsets below `pending_seek` and are NOT deleted — they
    // just arrived late and need a fresh seek. Only offsets in the range
    // `[pending_seek, msg_offset)` were genuinely skipped by the broker
    // (i.e., deleted via retention/compaction).
    //
    // When `pending_seek` is None (sequential read, no seek was issued), fall
    // back to the current minimum offset as the split boundary — the original
    // behaviour before this fix was introduced.
    let split_start = state
        .pending_seek
        .take()
        .unwrap_or_else(|| state.offsets.keys().next().copied().unwrap_or(msg_offset));

    // Split out entries that were present at seek time: [split_start..)
    // Entries in [0..split_start) are late arrivals; they stay in state.offsets.
    let working_set = state.offsets.split_off(&split_start);

    // Within the seek-time entries, partition into deleted and future:
    //   deleted  = [split_start..msg_offset)
    //   remaining = [msg_offset..)
    let mut deleted_offsets = working_set;
    let remaining = deleted_offsets.split_off(&msg_offset);

    // Merge future entries back so they aren't lost.
    state.offsets.extend(remaining);

    notify_deleted_offsets(deleted_offsets, msg_topic, msg_partition, msg_offset);

    let Some(senders) = state.offsets.remove(&msg_offset) else {
        debug!(topic = %msg_topic, partition = msg_partition, offset = msg_offset,
            "Discarding intermediate message (not requested)");
        cleanup_if_empty(active, consumer, msg_topic, msg_partition);
        return;
    };

    fulfill_requests::<C>(
        senders,
        &mut message,
        propagator,
        codec,
        msg_topic,
        responder,
    );

    cleanup_if_empty(active, consumer, msg_topic, msg_partition);
}

/// Notifies senders about deleted offsets and logs warnings.
fn notify_deleted_offsets<P>(
    deleted_offsets: BTreeMap<Offset, Responses<P>>,
    topic: Topic,
    partition: Partition,
    next_offset: Offset,
) {
    for (requested_offset, senders) in deleted_offsets {
        warn!(
            topic = %topic,
            partition = partition,
            requested_offset = requested_offset,
            next_offset = next_offset,
            affected_requests = senders.len(),
            "Message offset no longer exists (deleted by retention or compaction)"
        );
        let error = KafkaLoaderError::OffsetDeleted {
            topic,
            partition,
            requested_offset,
            next_offset,
        };
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

/// Decodes and fulfills requests for a specific offset.
fn fulfill_requests<C: Codec>(
    senders: Responses<C::Payload>,
    message: &mut BorrowedMessage<'_>,
    propagator: &TextMapCompositePropagator,
    codec: &mut C,
    topic: Topic,
    responder: Option<&SubsystemName>,
) where
    C::Payload: Clone,
{
    // Read before the `&mut` borrow the decode takes; the caller already
    // matched this message to these senders by both coordinates.
    let partition = message.partition();
    let offset = message.offset();

    let request_count = senders.len();
    debug!(topic = %topic, partition = partition, offset = offset, request_count = request_count,
        "Fulfilling active requests for message");

    let decoded_message = decode_record(message, propagator, codec, &responder);

    if let Some(decoded) = decoded_message {
        debug!(topic = %topic, partition = partition, offset = offset, request_count = request_count,
            "Message loaded successfully");
        for sender in senders {
            let _ = sender.send(Ok(decoded.clone()));
        }
    } else {
        error!(topic = %topic, partition = partition, offset = offset,
            "Failed to decode message");
        let error = KafkaLoaderError::DecodeError(topic, partition, offset);
        for sender in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

/// Cleans up partition entry and unassigns if no more requests remain.
fn cleanup_if_empty<P>(
    active: &mut ActiveRequests<P>,
    consumer: &BaseConsumer,
    topic: Topic,
    partition: Partition,
) {
    let should_cleanup = active
        .get(&(topic, partition))
        .is_some_and(|s| s.offsets.is_empty());

    if should_cleanup {
        active.remove(&(topic, partition));
        if let Err(error) = unassign_partition(consumer, topic, partition) {
            warn!(topic = %topic, partition = partition, error = %format_args!("{error:#}"),
                "Failed to unassign partition after fulfilling all requests");
        }
    }
}

/// Creates a load span with the decoded record's upstream context.
///
/// The span lifecycle does not depend on cache eviction.
pub(super) fn create_load_span<P>(
    decoded: &DecodedMessage<P>,
    cached: bool,
    relation: SpanRelation,
) -> Span {
    related_span!(
        relation,
        decoded.parent_context.clone(),
        "load",
        messaging.system = "kafka",
        partition = decoded.value.partition,
        offset = decoded.value.offset,
        topic = %decoded.value.topic,
        key = %decoded.value.key,
        cached = cached,
    )
}
