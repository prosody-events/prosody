use super::{
    ClassifyError, Codec, ConsumerMessage, DemandType, EXCISE_SOURCE_KIND, ErrorCategory,
    EventContext, EventIdentity, FailureTopicError, FailureTopicHandler, FailureTopicOutput,
    FallibleHandler, Keyed, MESSAGE_SOURCE_KIND, SecondsFormat, Settlement, SettlementHandler,
    Trigger, debug, error, info,
};

impl<T, Enc> FallibleHandler for FailureTopicHandler<T, Enc>
where
    T: FallibleHandler,
    Enc: Codec<Payload = T::Payload>,
    T::Payload: Clone + EventIdentity,
{
    type Error = FailureTopicError<T::Error, Enc::Error>;
    /// Output for the DLQ middleware. The inner handler always ran when this
    /// type is produced (unlike middlewares that may short-circuit):
    /// [`FailureTopicOutput::Inner`] carries the inner's success,
    /// [`FailureTopicOutput::Routed`] preserves the rescued inner error so
    /// the apply hook can forward it — see `after_commit` / `after_abort`
    /// below. We must not collapse this to `()` — see the
    /// [`FallibleHandler`] trait-level docs.
    type Output = FailureTopicOutput<T::Output, T::Error>;
    type Payload = T::Payload;

    /// Handles a message, attempting to process it with the wrapped handler.
    /// If processing fails with a non-Terminal error, sends the message to
    /// the failure topic.
    ///
    /// Returns `Ok` wrapping a [`FailureTopicOutput`] (inner success, or a
    /// rescued non-Terminal error the DLQ accepted); fails with
    /// [`FailureTopicError::Handler`] for a Terminal inner error or
    /// [`FailureTopicError::DlqSendFailed`] when the failure-topic send
    /// fails — see the variant docs for what each preserves.
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        let topic = message.topic().as_ref();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();

        let timestamp = message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true);

        let source_kind = MESSAGE_SOURCE_KIND;

        // Attempt to process the record with the wrapped handler.
        let error = match self
            .handler
            .on_message(context, message.clone(), demand_type)
            .await
        {
            Ok(output) => return Ok(FailureTopicOutput::Inner(output)),
            Err(error) => error,
        };

        // Handle terminal errors by aborting
        if matches!(error.classify_error(), ErrorCategory::Terminal) {
            info!(
                topic,
                partition,
                key = key.as_ref(),
                offset,
                "terminal condition encountered while handling {source_kind}: {error:#}; aborting"
            );
            return Err(FailureTopicError::Handler(error));
        }

        // Log the error and prepare to send to failure topic
        error!(
            topic,
            partition,
            key = key.as_ref(),
            offset,
            "failed to process {source_kind}: {error:#}; sending to {}",
            self.topic
        );

        // Prepare headers for the failure message
        let headers = [
            ("source-kind", source_kind),
            ("source-topic", topic),
            ("source-partition", &partition.to_string()),
            ("source-offset", &offset.to_string()),
            ("source-timestamp", &timestamp),
            ("source-group-id", &self.group_id),
            ("source-error", &error.to_string()),
        ];

        // Send the failed message to the failure topic. On failure, surface
        // BOTH the inner handler error and the producer error so the inner's
        // apply hook can fire on outer-retry re-dispatch.
        let sent = self
            .producer
            .send(headers, self.topic, key, message.payload().clone())
            .await;
        match sent {
            // The inner attempt failed but the dispatch resolves `Ok`: the
            // `Routed` variant classifies `Bypassed` at the settle boundary,
            // so the failed attempt's dirty ops never stage and no marker
            // records — the swallow's safety is the result value itself.
            Ok(()) => Ok(FailureTopicOutput::Routed(error)),
            Err(producer) => Err(FailureTopicError::DlqSendFailed {
                inner: error,
                producer,
            }),
        }
    }

    /// Routes an excise failure with the same policy as a message failure.
    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<()>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let topic = message.topic().as_ref();
        let partition = message.partition();
        let key = message.key();
        let offset = message.offset();
        let timestamp = message
            .timestamp()
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        let error = match self
            .handler
            .on_excise(context, message.clone(), demand_type)
            .await
        {
            Ok(output) => return Ok(FailureTopicOutput::Inner(output)),
            Err(error) => error,
        };
        if matches!(error.classify_error(), ErrorCategory::Terminal) {
            info!(
                topic,
                partition,
                key = key.as_ref(),
                offset,
                "terminal condition encountered while handling {EXCISE_SOURCE_KIND}: {error:#}; \
                 aborting"
            );
            return Err(FailureTopicError::Handler(error));
        }
        error!(
            topic,
            partition,
            key = key.as_ref(),
            offset,
            "failed to process {EXCISE_SOURCE_KIND}: {error:#}; sending to {}",
            self.topic
        );
        let headers = [
            ("source-kind", EXCISE_SOURCE_KIND),
            ("source-topic", topic),
            ("source-partition", &partition.to_string()),
            ("source-offset", &offset.to_string()),
            ("source-timestamp", &timestamp),
            ("source-group-id", &self.group_id),
            ("source-error", &error.to_string()),
        ];
        match self.producer.excise(headers, self.topic, key).await {
            Ok(()) => Ok(FailureTopicOutput::Routed(error)),
            Err(producer) => Err(FailureTopicError::DlqSendFailed {
                inner: error,
                producer,
            }),
        }
    }

    /// Propagates timer failures without a failure-topic write.
    ///
    /// This method preserves the inner error category for outer middleware.
    async fn on_timer<C>(
        &self,
        context: C,
        timer: Trigger,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = T::Payload>,
    {
        match self.handler.on_timer(context, timer, demand_type).await {
            Ok(output) => Ok(FailureTopicOutput::Inner(output)),
            Err(error) => Err(FailureTopicError::Handler(error)),
        }
    }

    /// Resolves the inner's apply hook on a **committed** marker.
    ///
    /// Routing per the work-centric invariant:
    /// - `Ok(Inner(o))` → `inner.after_commit(Ok(o))`. Inner ran, succeeded;
    ///   dispatch is final.
    /// - `Ok(Routed(e))` → `inner.after_commit(Err(e))`. DLQ accepted, the
    ///   marker committed, the inner will not see this logical message/timer
    ///   again — fire its apply hook with its original error.
    /// - `Err(Handler(e))` → `inner.after_commit(Err(e))`. Terminal error that
    ///   the framework chose to commit (rather than abort); forward it to the
    ///   inner.
    /// - `Err(DlqSendFailed { inner, .. })` → `inner.after_commit(Err(inner))`.
    ///   This branch only fires if the outer treats the producer error as final
    ///   (no retry); the inner's typed error is still forwarded so 2PC handlers
    ///   further down can finalise correctly.
    async fn after_commit<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        match result {
            Ok(FailureTopicOutput::Inner(output)) => {
                self.handler.after_commit(context, Ok(output)).await;
            }
            Ok(FailureTopicOutput::Routed(inner))
            | Err(
                FailureTopicError::Handler(inner) | FailureTopicError::DlqSendFailed { inner, .. },
            ) => {
                self.handler.after_commit(context, Err(inner)).await;
            }
        }
    }

    /// Resolves the inner's apply hook on an **aborted** marker.
    ///
    /// Routing per the work-centric invariant:
    /// - `Ok(Inner(o))` → `inner.after_abort(Ok(o))`. Inner succeeded but the
    ///   outer aborted (e.g. shutdown intervened); forward Ok.
    /// - `Ok(Routed(e))` → `inner.after_abort(Err(e))`. Rare path: the outer
    ///   aborted despite the DLQ accepting the routed message; re-dispatch is
    ///   coming, so the inner sees abort with its original error.
    /// - `Err(Handler(e))` → `inner.after_abort(Err(e))`. Terminal error;
    ///   marker aborted.
    /// - `Err(DlqSendFailed { inner, .. })` → `inner.after_abort(Err(inner))`.
    ///   The outer retry layer will re-drive the whole stack including the
    ///   inner; the inner's apply hook fires as `after_abort` with its original
    ///   error.
    async fn after_abort<C>(&self, context: C, result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = T::Payload>,
    {
        match result {
            Ok(FailureTopicOutput::Inner(output)) => {
                self.handler.after_abort(context, Ok(output)).await;
            }
            Ok(FailureTopicOutput::Routed(inner))
            | Err(
                FailureTopicError::Handler(inner) | FailureTopicError::DlqSendFailed { inner, .. },
            ) => {
                self.handler.after_abort(context, Err(inner)).await;
            }
        }
    }

    async fn shutdown(self) {
        debug!("shutting down failure topic handler");

        // No failure topic-specific state to clean up (producer is shared)
        // Cascade shutdown to the inner handler
        self.handler.shutdown().await;
    }
}

impl<T, Enc> SettlementHandler for FailureTopicHandler<T, Enc>
where
    T: SettlementHandler,
    Enc: Codec<Payload = T::Payload>,
    T::Payload: Clone + EventIdentity,
{
    fn settlement(result: Result<&Self::Output, &Self::Error>) -> Settlement {
        match result {
            // Inner ran and succeeded: its result is the dispatch's outcome.
            Ok(FailureTopicOutput::Inner(output)) => T::settlement(Ok(output)),
            // Routed to the DLQ: the outcome lives there — nothing here may
            // stage or record.
            Ok(FailureTopicOutput::Routed(_)) => Settlement::Bypassed,
            // Inner ran and its error surfaced un-rescued.
            Err(FailureTopicError::Handler(error)) => T::settlement(Err(error)),
            // Marker eligibility follows the INNER error, guarded by its
            // category, even though the retry-facing classification is the
            // producer's:
            // - a Permanent inner would have certified on its own (it is final regardless of the
            //   DLQ), so delegate its settlement;
            // - a Transient inner never certifies — the message is neither handled nor in the DLQ,
            //   so a marker here would silently filter its redelivery under a Permanent producer
            //   error. An unconditional delegate would bottom out at the leaf's `Final` and do
            //   exactly that.
            Err(FailureTopicError::DlqSendFailed { inner, .. }) => match inner.classify_error() {
                ErrorCategory::Permanent => T::settlement(Err(inner)),
                _ => Settlement::Bypassed,
            },
        }
    }
}
