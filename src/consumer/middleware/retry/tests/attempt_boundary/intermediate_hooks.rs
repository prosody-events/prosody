use super::*;
use std::future::ready;

#[derive(Clone)]
struct IntermediateHookHandler {
    calls: Arc<AtomicUsize>,
    succeed_on: usize,
    reads: Arc<Mutex<Vec<ReadObs>>>,
    commits: Arc<Mutex<Vec<CommitObs>>>,
}

impl IntermediateHookHandler {
    async fn handle<C>(&self, context: C) -> Result<(), TestError>
    where
        C: EventContext<Payload = Value>,
    {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let handle = context
            .state(Registered::new(cart()))
            .map_err(|_| TestError(ErrorCategory::Terminal))?;
        handle
            .set(json!({ "attempt": n as i32 }))
            .await
            .map_err(|_| TestError(ErrorCategory::Terminal))?;
        if n >= self.succeed_on {
            Ok(())
        } else {
            Err(TestError(ErrorCategory::Transient))
        }
    }
}

impl FallibleHandler for IntermediateHookHandler {
    type Error = TestError;
    type Output = ();
    type Payload = Value;

    async fn on_excise<C>(
        &self,
        context: C,
        _message: ConsumerMessage<()>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context).await
    }

    async fn on_message<C>(
        &self,
        context: C,
        _message: ConsumerMessage<Self::Payload>,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.handle(context).await
    }

    fn on_timer<C>(
        &self,
        _context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        ready(Ok(()))
    }

    async fn after_abort<C>(&self, context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let read = match context.state(Registered::new(cart())) {
            Ok(handle) => classify_read(handle.get().await),
            Err(e) => ReadObs::Other(format!("bind: {e}")),
        };
        self.reads.lock().push(read);
        let commit = match context.state(Registered::new(cart())) {
            Ok(handle) => classify_commit(handle.commit().await),
            Err(e) => CommitObs::Other(format!("bind: {e}")),
        };
        self.commits.lock().push(commit);
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for IntermediateHookHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}

/// The intermediate hook is state-dead: the between-attempts `after_abort`
/// holds the EXPIRED pre-verb context (pinned at the failed attempt, the
/// epoch already bumped), so both its `get()` and its `commit()` error
/// `Terminated` and its `commit()` has zero durable effect — a mid-loop
/// commit of the failed attempt's overlay is impossible. With
/// `succeed_on = 3`, attempts 1 and 2 fail so two intermediate hooks fire,
/// exercising the repeated-hook shape. Passing the LIVE post-verb context
/// to the hook instead of the expired clone makes the reads succeed
/// (`Value`, not `Terminated`), failing this pin.
#[tokio::test]
async fn intermediate_hook_is_state_dead() -> Result<()> {
    let reads = Arc::new(Mutex::new(Vec::new()));
    let commits = Arc::new(Mutex::new(Vec::new()));
    let handler = IntermediateHookHandler {
        calls: Arc::new(AtomicUsize::new(0)),
        succeed_on: 3,
        reads: reads.clone(),
        commits: commits.clone(),
    };
    let retry_handler = create_retry_handler(handler, 10);
    let (context, cell_store, state_key) =
        hook_fixture(|r| Ok(r.register(&cart(), CollectionDef::new(None))?))?;

    let tracker = create_offset_tracker();
    let uncommitted = create_test_message()?.into_uncommitted(tracker.take(0).await?);
    EventHandler::on_message(&retry_handler, context, uncommitted, DemandType::Normal).await;

    assert_eq!(
        reads.lock().clone(),
        vec![ReadObs::Terminated, ReadObs::Terminated],
        "both intermediate after_abort reads are fenced (two hooks fire)",
    );
    assert_eq!(
        commits.lock().clone(),
        vec![CommitObs::Terminated, CommitObs::Terminated],
        "both intermediate after_abort commits are fenced",
    );
    assert_eq!(
        committed_json_value(&cell_store, state_key, "cart").await?,
        Some(json!({ "attempt": 3_i32 })),
        "only the successful attempt 3's cart commits; the fenced intermediate commits added \
         nothing durable",
    );
    let _ = tracker.shutdown().await;
    Ok(())
}
