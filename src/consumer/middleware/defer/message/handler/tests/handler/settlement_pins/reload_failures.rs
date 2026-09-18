use super::*;
use crate::consumer::middleware::providers::LeafHandler;
use crate::consumer::middleware::tests::test_support::settlement_name;
use std::future::ready;

#[tokio::test]
async fn reload_permanent_failure_records_the_reloaded_id() -> Result<()> {
    let fx = Fixture::new()?;
    let expected = fx.seed_message(0);
    fx.defer_store
        .defer_first_message(&Key::from(KEY), 0)
        .await?;

    let (session, cell_store, _dirty, recorded) = fx.session(timer_event())?;
    let scope = EventStateScope::new(session);
    let context = MockEventContext::new()
        .with_session(scope.handle())
        .with_timer_tracking();
    let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());

    fx.leaf.fail_next(ErrorCategory::Permanent);
    EventHandler::on_timer(&fx.handler, context, timer, DemandType::Normal).await;

    assert_eq!(
        recorded.lock().clone(),
        vec![expected],
        "a permanently-failed reload records the reloaded message's id",
    );
    assert_eq!(
        committed_json_value(&cell_store, fx.registry_key.clone(), "cart").await?,
        None,
        "permanent reload failure stages nothing",
    );
    assert_eq!(
        fx.defer_store.is_deferred(&Key::from(KEY)).await?,
        None,
        "the permanent failure advances (empties) the queue",
    );
    assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
    Ok(())
}

/// Last-wins override: a retry re-dispatch of the same defer timer after
/// a durable queue advance loads a DIFFERENT queue head and records
/// under the NEW head's id — a set-once override would record message
/// B's dispatch under message A's identity.
#[tokio::test(start_paused = true)]
async fn retry_redispatch_records_under_the_new_head_id() -> Result<()> {
    let fx = Fixture::new()?;
    let id_m1 = fx.seed_message(0);
    let id_m2 = fx.seed_message(1);
    fx.defer_store
        .defer_first_message(&Key::from(KEY), 0)
        .await?;
    fx.defer_store
        .defer_additional_message(&Key::from(KEY), 1)
        .await?;

    let (session, _cell_store, _dirty, recorded) = fx.session(timer_event())?;
    let scope = EventStateScope::new(session);
    // Poison the FIRST timer op: attempt 1 reloads M1, the leaf
    // succeeds, the queue durably advances to M2, then the
    // reschedule fails Transient — the outer retry re-dispatches and
    // attempt 2 reloads M2.
    let context = MockEventContext::new()
        .with_session(scope.handle())
        .with_timer_tracking()
        .with_timer_failures(1, ErrorCategory::Transient);
    let (timer, committed, _aborted) = RecordingTimer::new(defer_trigger());

    let retry_provider = RetryMiddleware::new(RetryConfiguration::builder().build()?)?
        .with_provider(FallibleCloneProvider::new(fx.handler.clone()));
    let retry_handler = FallibleHandlerProvider::handler_for_partition(
        &retry_provider,
        Topic::from(TOPIC),
        Partition::from(PARTITION),
    );

    EventHandler::on_timer(&retry_handler, context, timer, DemandType::Normal).await;

    assert_eq!(
        fx.leaf.processed(),
        vec![0, 1],
        "attempt 1 reloaded M1; attempt 2 reloaded the advanced head M2",
    );
    assert_eq!(
        recorded.lock().clone(),
        vec![id_m2],
        "the marker records under the NEW head's id — last-wins, never M1's",
    );
    assert_ne!(id_m1, id_m2, "distinct offsets hash to distinct ids");
    assert_eq!(committed.load(Ordering::SeqCst), 1, "the trigger commits");
    Ok(())
}

/// Store double for the classification table only: its `Error` is
/// constructible (unlike the memory store's `Infallible`), so the
/// `DeferError::Store` row can be exercised. `settlement()` is a pure
/// function of the result value, so no store method ever runs.
#[derive(Clone)]
struct TableStore;

impl MessageDeferStore for TableStore {
    type Error = StagingError;

    fn defer_first_message(
        &self,
        _key: &Key,
        _offset: Offset,
    ) -> impl Future<Output = Result<(), StagingError>> {
        ready(Ok(()))
    }

    fn get_next_deferred_message(
        &self,
        _key: &Key,
    ) -> impl Future<Output = Result<Option<(Offset, u32)>, StagingError>> {
        ready(Ok(None))
    }

    fn append_deferred_message(
        &self,
        _key: &Key,
        _offset: Offset,
    ) -> impl Future<Output = Result<(), StagingError>> {
        ready(Ok(()))
    }

    fn remove_deferred_message(
        &self,
        _key: &Key,
        _offset: Offset,
    ) -> impl Future<Output = Result<(), StagingError>> {
        ready(Ok(()))
    }

    fn set_retry_count(
        &self,
        _key: &Key,
        _retry_count: u32,
    ) -> impl Future<Output = Result<(), StagingError>> {
        ready(Ok(()))
    }

    fn delete_key(&self, _key: &Key) -> impl Future<Output = Result<(), StagingError>> {
        ready(Ok(()))
    }
}

/// The settlement classification table for the message-defer wrapper:
/// every Output and error variant. The delegating rows are proven to
/// delegate by routing through the dedup wrapper's own table (`Inner
/// None` reaches dedup's `Bypassed`, never a hardcoded `Final`).
#[test]
fn settlement_classification_table() {
    use crate::consumer::middleware::deduplication::DeduplicationError;
    use crate::timers::datetime::CompactDateTimeError;

    type Subject = MessageDeferHandler<
        DeduplicationHandler<LeafHandler<StagingLeaf>, MemoryDeduplicationStore>,
        TableStore,
        MemoryLoader<Value>,
        AlwaysDefer,
    >;
    type Out = DeferOutput<Option<()>, DeduplicationError<StagingError>>;
    type TableErr = DeferError<StagingError, DeduplicationError<StagingError>, MemoryLoaderError>;

    let rows: Vec<(&str, Result<Out, TableErr>, &str)> = vec![
        (
            "Inner(Some) delegates through dedup to the leaf's Final",
            Ok(DeferOutput::Inner(Some(()))),
            "Final",
        ),
        (
            "Inner(None) delegates to dedup's Bypassed (dedup hit)",
            Ok(DeferOutput::Inner(None)),
            "Bypassed",
        ),
        (
            "Deferred is Bypassed (parked for retry)",
            Ok(DeferOutput::Deferred(DeduplicationError::Inner(
                StagingError(ErrorCategory::Transient),
            ))),
            "Bypassed",
        ),
        (
            "NoInner is Bypassed (queued behind / load handled)",
            Ok(DeferOutput::NoInner),
            "Bypassed",
        ),
        (
            "Handler(Inner leaf error) delegates to Rejected",
            Err(DeferError::Handler(DeduplicationError::Inner(
                StagingError(ErrorCategory::Permanent),
            ))),
            "Rejected",
        ),
        (
            "Handler(dedup Store) delegates to dedup's Abandoned",
            Err(DeferError::Handler(DeduplicationError::Store(Box::new(
                StagingError(ErrorCategory::Transient),
            )))),
            "Abandoned",
        ),
        (
            "Store bookkeeping failure is Abandoned",
            Err(DeferError::Store(StagingError(ErrorCategory::Transient))),
            "Abandoned",
        ),
        (
            "Timer bookkeeping failure is Abandoned",
            Err(DeferError::Timer(Box::new(StagingError(
                ErrorCategory::Transient,
            )))),
            "Abandoned",
        ),
        (
            "Loader bookkeeping failure is Abandoned",
            Err(DeferError::Loader(MemoryLoaderError::LoaderShutdown)),
            "Abandoned",
        ),
        (
            "CompactTime (backoff computation, Permanent) is Abandoned",
            Err(DeferError::CompactTime(CompactDateTimeError::OutOfRange)),
            "Abandoned",
        ),
    ];
    for (label, result, expected) in rows {
        assert_eq!(
            settlement_name(Subject::settlement(result.as_ref())),
            expected,
            "{label}"
        );
    }
}
