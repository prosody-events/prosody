use tracing::debug;

use super::{MessageDeferHandler, MessageDeferOutput};
use crate::consumer::event_context::EventContext;
use crate::consumer::message::ConsumerMessage;
use crate::consumer::middleware::FallibleHandler;
use crate::consumer::middleware::deduplication::{DedupIdentity, dedup_uuid_for_message};
use crate::consumer::middleware::defer::decider::DeferralDecider;
use crate::consumer::middleware::defer::error::DeferError;
use crate::consumer::middleware::defer::message::store::MessageDeferStore;
use crate::consumer::{DemandType, Keyed};
use crate::error::{ClassifyError, ErrorCategory};
use crate::loader::MessageLoader;
use crate::state::session::{MarkerAccessExt, MessageMarker};
use crate::timers::{TimerType, Trigger};

impl<T, M, L, D> FallibleHandler for MessageDeferHandler<T, M, L, D>
where
    T: FallibleHandler<Payload = L::Payload>,
    M: MessageDeferStore,
    L: MessageLoader + 'static,
    D: DeferralDecider,
    L::Payload: crate::EventIdentity,
{
    type Error = DeferError<M::Error, T::Error, L::Error>;
    /// Encodes the inner's outcome; drives apply-hook routing. See
    /// [`MessageDeferOutput`] and the module-level apply-hooks section.
    type Output = MessageDeferOutput<T::Output, T::Error>;
    type Payload = T::Payload;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<T::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Already deferred: queue behind existing messages (ordering
        // invariant). Inner does not run -> NoInner.
        if self
            .store
            .is_deferred(message.key())
            .await
            .map_err(DeferError::Store)?
            .is_some()
        {
            let offset = message.offset();
            return self.append_to_deferred_queue(message.key(), offset).await;
        }

        // Not deferred: try handler, defer on transient failure if enabled.
        let message_key = message.key().clone();
        let offset = message.offset();

        let error = match self
            .handler
            .on_message(context.clone(), message, demand_type)
            .await
        {
            Ok(output) => return Ok(MessageDeferOutput::Inner(output)),
            Err(error) => error,
        };

        if !matches!(error.classify_error(), ErrorCategory::Transient) {
            return Err(DeferError::Handler(error));
        }

        // Only gate initial deferral; once deferred, always re-defer transient.
        if !self.config.enabled {
            debug!(
                key = ?message_key,
                offset = offset,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: middleware disabled"
            );
            return Err(DeferError::Handler(error));
        }

        if !self.decider.should_defer() {
            debug!(
                key = ?message_key,
                offset = offset,
                topic = %self.topic,
                partition = self.partition,
                "Deferral skipped: decider threshold not met"
            );
            return Err(DeferError::Handler(error));
        }

        self.defer_message(context, &message_key, offset, error)
            .await
    }

    /// Processes an excise record with the message defer policy.
    fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type)
    }

    /// Keeps each deferred-message trigger inside this middleware.
    ///
    /// An empty queue returns `NoInner`. It does not enter the application
    /// timer path. This rule preserves the reload identity across attempts.
    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        if trigger.timer_type != TimerType::DeferredMessage {
            return self
                .handler
                .on_timer(context, trigger, demand_type)
                .await
                .map(MessageDeferOutput::Inner)
                .map_err(DeferError::Handler);
        }

        let message_key = &trigger.key;

        debug!(
            key = ?message_key,
            scheduled_time = %trigger.time,
            topic = %self.topic,
            partition = self.partition,
            "Defer retry timer fired"
        );

        let Some((offset, retry_count)) = self
            .store
            .get_next_deferred_message(message_key)
            .await
            .map_err(DeferError::Store)?
        else {
            // Orphan timer for an empty queue — inner did not run.
            debug!(
                key = ?message_key,
                topic = %self.topic,
                partition = self.partition,
                "Clearing orphaned defer timer: queue empty"
            );
            self.store
                .delete_key(message_key)
                .await
                .map_err(DeferError::Store)?;
            return Ok(MessageDeferOutput::NoInner);
        };

        let Some(message) = self
            .load_deferred_message(&context, message_key, offset, retry_count)
            .await?
        else {
            // Loader handled the failure (retry rescheduled, queue advanced
            // past a permanent skip, or key-mismatch skip) — inner did not
            // run for this dispatch.
            return Ok(MessageDeferOutput::NoInner);
        };

        // The exactly-one set site of the reload identity override:
        // immediately after the load succeeds and before the inner dispatch,
        // so the dedup filter inside this dispatch and the settle boundary's
        // marker record both read the RELOADED message's id while the
        // session stages under the timer's EventRef. Last-wins by design: a
        // retry re-dispatch of this same timer after a durable queue advance
        // loads the next head and re-points the override at it.
        if let Ok(marker) = context.marker_identity() {
            marker.set_reload_marker(MessageMarker::new(dedup_uuid_for_message(
                DedupIdentity {
                    version: &self.dedup_version,
                    group_id: &self.source,
                    topic: self.topic.as_ref(),
                    partition: self.partition,
                },
                &message,
            )));
        }

        debug!(
            key = ?message_key,
            offset = offset,
            retry_count = retry_count,
            topic = %self.topic,
            partition = self.partition,
            "Loaded deferred message - attempting retry"
        );

        self.retry_deferred_message(context, &trigger, message_key, offset, retry_count, message)
            .await
    }

    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Apply-hook routing (see module docs):
        // - Inner(o):    inner ran and succeeded           -> after_commit(Ok)
        // - NoInner:     no inner dispatch happened        -> suppress
        // - Deferred(e): inner ran, transient err deferred -> after_abort(Err(e))
        //   (retry coming)
        // - Handler(e):  inner ran and surfaced an error   -> after_commit(Err)
        // - Store/Timer/Loader: defer-layer rescue failed. Suppress the inner hook
        //   regardless of whether the inner ran on this dispatch. These errors classify
        //   as Transient (see `DeferError::classify_error`) so the outer retry layer
        //   will redrive the whole stack; consistency lives in the failed Result, not
        //   in the hook. Apply hooks are best-effort — see
        //   `FallibleHandler::after_commit` docs.
        match result {
            Ok(MessageDeferOutput::Inner(output)) => {
                self.handler.after_commit(context, Ok(output)).await;
            }
            Ok(MessageDeferOutput::Deferred(error)) => {
                self.handler.after_abort(context, Err(error)).await;
            }
            Err(DeferError::Handler(error)) => {
                self.handler.after_commit(context, Err(error)).await;
            }
            Ok(MessageDeferOutput::NoInner) | Err(_) => {}
        }
    }

    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        // Symmetric to after_commit. Two notes:
        //   - Deferred(e) still routes to after_abort(Err(e)) regardless of the outer
        //     commit/abort decision (a retry is coming via the deferred timer).
        //   - Store/Timer/Loader rescue-failure paths suppress the inner hook on
        //     purpose; the outer retry redrives the stack. See after_commit.
        match result {
            Ok(MessageDeferOutput::Inner(output)) => {
                self.handler.after_abort(context, Ok(output)).await;
            }
            Ok(MessageDeferOutput::Deferred(error)) | Err(DeferError::Handler(error)) => {
                self.handler.after_abort(context, Err(error)).await;
            }
            Ok(MessageDeferOutput::NoInner) | Err(_) => {}
        }
    }

    async fn shutdown(self) {
        self.handler.shutdown().await;
    }
}
