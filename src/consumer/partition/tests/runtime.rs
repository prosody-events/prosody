use super::*;

#[tokio::test]
async fn test_partition_manager_event_type_filtering() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    // Only allow events whose "type" field contains "allowed"
    config.allowed_events = Some(
        AhoCorasick::builder()
            .start_kind(StartKind::Anchored)
            .build(["allowed"])?,
    );
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    let test_semaphore = Arc::new(Semaphore::new(10));

    // 1) a disallowed event ("type": "disallowed")
    let disallowed = ConsumerMessage::new(
        ConsumerMessageValue {
            offset: Offset::from(0u8),
            key: "key".into(),
            payload: json!({ "type": "disallowed" }),
            ..Default::default()
        },
        Span::current(),
        test_semaphore.clone().try_acquire_owned()?,
    );
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Message(disallowed))
            .is_ok()
    );

    // 2) an allowed event ("type": "allowed")
    let allowed = ConsumerMessage::new(
        ConsumerMessageValue {
            offset: Offset::from(1u8),
            key: "key".into(),
            payload: json!({ "type": "allowed" }),
            ..Default::default()
        },
        Span::current(),
        test_semaphore.clone().try_acquire_owned()?,
    );
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Message(allowed))
            .is_ok()
    );

    let excise = ConsumerMessage::new(
        ConsumerMessageValue {
            offset: Offset::from(2_u8),
            key: "key".into(),
            payload: (),
            ..Default::default()
        },
        Span::current(),
        test_semaphore.try_acquire_owned()?,
    );
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Excise(excise))
            .is_ok()
    );

    wait_for_processed_offsets(&handler, 2, Duration::from_secs(1)).await?;

    let processed = handler.processed_offsets.lock().await;
    assert_eq!(
        processed.as_slice(),
        &[Offset::from(1_u8), Offset::from(2_u8)],
        "The filter must pass excise records"
    );

    partition_manager.shutdown().await;
    Ok(())
}

/// Waits for a specific number of messages to be processed or times out.
pub(super) async fn wait_for_processed_offsets<H>(
    handler: &H,
    expected_count: usize,
    timeout: Duration,
) -> color_eyre::Result<()>
where
    H: HasProcessedOffsets + ?Sized,
{
    let deadline = Instant::now() + timeout;
    loop {
        {
            let processed = handler.processed_offsets().lock().await;
            if processed.len() >= expected_count {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(eyre!("Timeout waiting for {expected_count} messages"));
        }
        let notified = handler.notify().notified();
        tokio::select! {
            () = notified => {},
            () = sleep_until(deadline) => {
                return Err(eyre!("Timeout waiting for {expected_count} messages"));
            }
        }
    }
}

/// Waits for partition stall state to match `expected` or times out.
///
/// Awaits the offset tracker's stall-transition signal
/// (`OffsetTracker::wait_for_stall_state`) rather than polling: the background
/// watermark task flips the offset stall flag at two explicit points (the
/// oldest uncommitted offset exceeding the threshold, and a watermark advance
/// clearing it). The composite `PartitionManager::is_stalled` also folds in
/// heartbeat staleness, but the keyed processing loop beats its heartbeat every
/// `stall_threshold / HEARTBEAT_MARGIN`, so under normal dispatch only the
/// offset half ever transitions — this asserts the composite once the edge
/// fires to confirm it matches.
pub(super) async fn wait_for_partition_stalled<P>(
    partition_manager: &PartitionManager<P>,
    expected: bool,
    timeout: Duration,
) -> color_eyre::Result<()>
where
    P: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    tokio::select! {
        result = partition_manager.offsets.wait_for_stall_state(expected) => result?,
        () = sleep_until(deadline) => {
            return Err(eyre!(
                "Timeout waiting for partition stalled state {expected}; last state was {}",
                partition_manager.is_stalled()
            ));
        }
    }
    let actual = partition_manager.is_stalled();
    if actual == expected {
        Ok(())
    } else {
        Err(eyre!(
            "partition stalled state {actual} did not match expected {expected} after the offset \
             stall signal fired"
        ))
    }
}

/// A test handler that records processed offsets and detects concurrent
/// processing.
#[derive(Clone)]
pub(super) struct TestHandler {
    pub(super) processed_offsets: Arc<Mutex<Vec<Offset>>>,
    pub(super) has_concurrent_processing: Arc<Mutex<bool>>,
    pub(super) timer_fires: Arc<AtomicUsize>,
    keys_in_processing: Arc<Mutex<Vec<Key>>>,
    notify: Arc<Notify>,
    delay: Duration,
}

impl TestHandler {
    pub(super) fn new() -> Self {
        Self::with_delay(Duration::ZERO)
    }

    /// A handler that holds each key "in processing" for `delay` (simulated
    /// processing time), widening the window in which a second same-key
    /// dispatch would overlap and trip the concurrency flag.
    pub(super) fn with_delay(delay: Duration) -> Self {
        Self {
            processed_offsets: Arc::new(Mutex::new(Vec::new())),
            has_concurrent_processing: Arc::new(Mutex::new(false)),
            timer_fires: Arc::new(AtomicUsize::new(0)),
            keys_in_processing: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            delay,
        }
    }

    async fn process<P: Send + Sync + 'static>(&self, message: UncommittedMessage<P>) {
        let key = message.key().clone();
        let offset = message.offset();
        let processed = self.processed_offsets.clone();
        let concurrent_flag = self.has_concurrent_processing.clone();
        let keys_proc = self.keys_in_processing.clone();
        let notify = self.notify.clone();
        let delay = self.delay;
        {
            let mut keys = keys_proc.lock().await;
            if keys.contains(&key) {
                let mut flag = concurrent_flag.lock().await;
                *flag = true;
            } else {
                keys.push(key.clone());
            }
        }
        if !delay.is_zero() {
            sleep(delay).await;
        }
        processed.lock().await.push(offset);
        keys_proc.lock().await.retain(|candidate| candidate != &key);
        notify.notify_waiters();
        message.commit().await;
    }
}

impl HasProcessedOffsets for TestHandler {
    fn processed_offsets(&self) -> &Arc<Mutex<Vec<Offset>>> {
        &self.processed_offsets
    }

    fn notify(&self) -> &Arc<Notify> {
        &self.notify
    }
}

impl EventHandler for TestHandler {
    type Payload = serde_json::Value;

    fn on_message<C>(
        &self,
        _context: C,
        message: UncommittedMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> impl Future<Output = ()> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.process(message)
    }

    async fn on_excise<C>(
        &self,
        _context: C,
        message: UncommittedMessage<()>,
        _demand_type: DemandType,
    ) where
        C: EventContext<Payload = Self::Payload>,
    {
        self.process(message).await;
    }

    async fn on_timer<C, U>(&self, _context: C, _timer: U, _demand_type: DemandType)
    where
        C: EventContext<Payload = Self::Payload>,
        U: UncommittedTimer,
    {
        self.timer_fires.fetch_add(1, Ordering::SeqCst);
    }

    async fn shutdown(self) {}
}

/// Helper functions to create test messages.
pub(super) fn create_test_message(
    offset: Offset,
    key: &str,
) -> color_eyre::Result<ConsumerMessage<serde_json::Value>> {
    let semaphore = Arc::new(Semaphore::new(10));
    Ok(ConsumerMessage::new(
        ConsumerMessageValue {
            offset,
            key: key.into(),
            ..Default::default()
        },
        Span::current(),
        semaphore.try_acquire_owned()?,
    ))
}

/// Timer and processing heartbeats stay integrated into partition stall
/// detection: a registered-but-never-beaten heartbeat trips stall detection
/// once its last beat is older than the threshold, so remaining un-stalled
/// across a window several thresholds wide proves every registered heartbeat
/// is actively beaten.
#[tokio::test]
async fn test_partition_manager_timer_heartbeat_integration() -> color_eyre::Result<()> {
    init_test_logging();

    let handler = TestHandler::new();
    let mut config = default_config();
    config.stall_threshold = Duration::from_millis(200);
    let partition_manager = PartitionManager::new(config, handler.clone(), "test-topic".into(), 0);

    // Initially, the partition should not be stalled
    assert!(
        !partition_manager.is_stalled(),
        "Partition should not be stalled initially"
    );

    // Send a message to spin up the keyed processing loop and timer manager,
    // registering their heartbeats
    let message = create_test_message(1, "test-key")?;
    assert!(
        partition_manager
            .try_send_record(ConsumerRecord::Message(message))
            .is_ok(),
        "Message send should succeed"
    );
    wait_for_processed_offsets(&handler, 1, Duration::from_secs(1)).await?;

    // Negative-invariant observation window (~3× the stall threshold): by its
    // end every registered heartbeat is past the threshold unless actively
    // beaten, so this fails if the heartbeats stop being beaten or integrated
    sleep(Duration::from_millis(600)).await;
    assert!(
        !partition_manager.is_stalled(),
        "Partition must not stall while its heartbeats are actively beaten"
    );

    // Shutdown drains the in-flight commit, so the watermark reflects it
    let watermark = partition_manager.shutdown().await;
    assert_eq!(watermark, Some(1), "Shutdown should drain the commit");
    Ok(())
}
