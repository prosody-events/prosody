//! Deferred queue repair and incomplete settlement contracts.

use super::*;
use crate::consumer::middleware::tests::test_support::create_test_message_from;
use crate::test_util::TEST_RUNTIME;
use quickcheck_macros::quickcheck;
use std::iter::once;

/// Each append restores an absent timer and preserves an existing timer.
#[quickcheck]
fn queue_append_restores_timer(retry_count: u32, losses: Vec<bool>) -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let fx = Fixture::new()?;
        let key = Key::from(KEY);
        fx.defer_store.defer_first_message(&key, 0).await?;
        fx.defer_store.set_retry_count(&key, retry_count).await?;
        let context = MockEventContext::new().with_timer_tracking();

        for (index, lost) in once(true).chain(losses).enumerate() {
            if lost {
                context.clear_scheduled(TimerType::DeferredMessage).await?;
            }
            let before = context.scheduled(TimerType::DeferredMessage).await?;
            let offset = Offset::try_from(index)? + 1;
            let message = create_test_message_from(ConsumerMessageValue {
                key: key.clone(),
                offset,
                ..Default::default()
            })?;
            let output = FallibleHandler::on_message(
                &fx.handler,
                context.clone(),
                message,
                DemandType::Normal,
            )
            .await?;

            assert!(matches!(output, DeferOutput::NoInner));
            let after = context.scheduled(TimerType::DeferredMessage).await?;
            assert_eq!(after.len(), 1, "each append must leave a retry timer");
            if !before.is_empty() {
                assert_eq!(after, before, "an existing timer must stay unchanged");
            }
            assert_eq!(
                fx.defer_store.get_next_deferred_message(&key).await?,
                Some((0, retry_count)),
                "append must preserve the queue head and retry count",
            );
        }
        assert!(fx.leaf.processed().is_empty());
        Ok(())
    })
}

/// Failed defer bookkeeping abandons either source without state or a marker.
#[quickcheck]
fn failed_bookkeeping_abandons_source(timer: bool, permanent: bool) -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let fx = Fixture::new()?;
        let category = if permanent {
            ErrorCategory::Permanent
        } else {
            ErrorCategory::Transient
        };
        let event = if timer {
            timer_event()
        } else {
            EventRef::Message {
                dedup_id: message_id(1),
            }
        };
        let key = Key::from(KEY);
        fx.seed_message(0);
        fx.defer_store.defer_first_message(&key, 0).await?;
        let (session, cells, dirty, recorded) = fx.session(event)?;
        let scope = EventStateScope::new(session);
        let context = MockEventContext::new()
            .with_session(scope.handle())
            .with_timer_tracking()
            .with_timer_failures(1, category);

        if timer {
            fx.leaf.fail_next(ErrorCategory::Transient);
            let (trigger, committed, aborted) = RecordingTimer::new(defer_trigger());
            EventHandler::on_timer(&fx.handler, context, trigger, DemandType::Normal).await;
            assert_eq!(
                committed.load(Ordering::SeqCst),
                0,
                "incomplete timer must not commit"
            );
            assert_eq!(aborted.load(Ordering::SeqCst), 1);
        } else {
            let (message, tracker) = uncommitted_message(1).await?;
            EventHandler::on_message(&fx.handler, context, message, DemandType::Normal).await;
            assert_eq!(
                tracker.shutdown().await,
                None,
                "failed bookkeeping must not commit the message"
            );
        }

        assert!(recorded.lock().is_empty());
        assert!(dirty.touched(&key).is_empty());
        assert_eq!(
            committed_json_value(&cells, fx.registry_key, "cart").await?,
            None
        );
        assert_eq!(
            fx.defer_store.get_next_deferred_message(&key).await?,
            Some((0, u32::from(timer))),
        );
        Ok(())
    })
}
