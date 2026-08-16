use super::*;

// =========================================================================
// Recording-session harness for the settlement-boundary marker tests
// =========================================================================
//
// The marker-hygiene triangle — retry between attempts, settle on the final
// outcome, and every defer/route Err→Ok swallow — shares one observable
// contract: a `Bypassed` or discarded attempt's buffered writes never commit
// and no marker records for it. These parts build a **real**
// `KeyedStateSession` whose marker record routes through a recording oracle
// so each seam's test can assert that contract directly.

/// Oracle that logs every marker `settle` records and always resolves
/// Committed, so a test can read back exactly which markers `settle` certified.
#[derive(Clone)]
pub struct RecordingOracle {
    recorded: Arc<Mutex<Vec<Uuid>>>,
}

impl RecordingOracle {
    /// A fresh oracle with an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            recorded: Arc::default(),
        }
    }

    /// The shared log this oracle pushes every recorded marker into.
    #[must_use]
    pub fn recorded(&self) -> Arc<Mutex<Vec<Uuid>>> {
        self.recorded.clone()
    }
}

impl Default for RecordingOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitOracle for RecordingOracle {
    type Error = Infallible;

    async fn record_message(&self, dedup_id: Uuid) -> Result<(), Self::Error> {
        self.recorded.lock().push(dedup_id);
        Ok(())
    }

    async fn resolve<'a>(
        &'a self,
        _state_key: &'a StateKey,
        _event: EventRef,
    ) -> Result<CommitDecision, Self::Error> {
        Ok(CommitDecision::Committed)
    }
}

/// Backend of a [`recording_session`].
pub type RecordingBackend = PartitionBackend<
    RecordingOracle,
    MemoryDescriptorIdentityStore,
    MemoryCellStore<RecordingOracle>,
>;

/// Session type built by [`recording_session`].
pub type RecordingSession = KeyedStateSession<RecordingBackend, MemoryLoader<Value>>;

/// What [`recording_session`] hands back: the session, its durable cell store
/// (a clone sharing the durable `Arc`), the session's dirty store, and the
/// shared log of every marker the oracle recorded — the surfaces the
/// settlement-boundary marker tests assert on.
pub type RecordingParts = (
    RecordingSession,
    MemoryCellStore<RecordingOracle>,
    Arc<DirtyStore>,
    Arc<Mutex<Vec<Uuid>>>,
);

/// A real session over `registry` for `state_key` and `event` (the identity
/// the settle boundary reads its marker from); see [`RecordingParts`] for
/// the returned surfaces.
#[must_use]
pub fn recording_session(
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
) -> RecordingParts {
    recording_session_with_loader(registry, state_key, event, MemoryLoader::new())
}

/// [`recording_session`] over a caller-supplied loader, for reload tests
/// that seed messages into it.
#[must_use]
pub fn recording_session_with_loader(
    registry: CollectionDefRegistry,
    state_key: StateKey,
    event: EventRef,
    loader: MemoryLoader<Value>,
) -> RecordingParts {
    let registry = Arc::new(registry);
    let oracle = RecordingOracle::new();
    let recorded = oracle.recorded();
    let cell_store = MemoryCellStore::new(MemoryCells::new(), oracle.clone(), registry.clone());
    let dirty = Arc::new(DirtyStore::new());
    let (_shutdown_tx, shutdown_rx) = watch::channel(ShutdownPhase::default());
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let session = KeyedStateSession::new(SessionParts {
        cell: cell_store.clone(),
        dirty: dirty.clone(),
        oracle,
        loader,
        registry,
        state_key,
        event,
        recovery_delay: CompactDuration::new(30),
        armed: Arc::default(),
        termination: TerminationWatch::new(shutdown_rx, cancel_rx),
        publisher: None,
    });
    (session, cell_store, dirty, recorded)
}

/// The committed value at the single Value cell of `name` under `state_key`,
/// failing on a store read error or undecodable bytes.
pub async fn committed_json_value(
    cell_store: &MemoryCellStore<RecordingOracle>,
    state_key: StateKey,
    name: &str,
) -> color_eyre::Result<Option<Value>> {
    let id = CollectionId::new(state_key, StateType::Application, StateName::try_new(name)?);
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(u128::MAX),
    };
    match Committed::into_inner(cell_store.get(&id, &value_cell(), probe).await?) {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Error of a [`StagingTransientHandler`] attempt, carrying its
/// classification.
#[derive(Debug, Error)]
#[error("staging attempt failed ({0:?})")]
pub struct StagingError(pub ErrorCategory);

impl ClassifyError for StagingError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}

/// Which apply hook a [`StagingTransientHandler`] observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StagingHook {
    /// `after_commit` fired — the dispatch was final.
    Commit,
    /// `after_abort` fired — the attempt was rolled back; a retry is coming.
    Abort,
}

/// Inner handler for the defer/route swallow tests: on every dispatch it
/// binds `cart`, buffers one write, then fails Transient — the exact attempt
/// whose swallow the settle boundary must classify `Bypassed` so nothing
/// stages and no marker records. Records its apply hooks so a test can prove
/// the swallow path (not a surfaced error) handled the dispatch.
#[derive(Clone, Default)]
pub struct StagingTransientHandler {
    hooks: Arc<Mutex<Vec<StagingHook>>>,
}

impl StagingTransientHandler {
    /// A handler staging one `cart` write per attempt.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The `cart` collection every attempt writes; register it in the
    /// session's registry.
    #[must_use]
    pub fn collection() -> ValueDescriptor {
        value_state("cart")
    }

    /// The apply hooks observed so far, in order.
    #[must_use]
    pub fn hooks(&self) -> Vec<StagingHook> {
        self.hooks.lock().clone()
    }

    /// One failed attempt: buffer a `cart` write, fail Transient.
    async fn stage<C>(&self, context: &C) -> Result<(), StagingError>
    where
        C: EventContext<Payload = Value>,
    {
        let handle = context
            .state(Registered::new(Self::collection()))
            .map_err(|_| StagingError(ErrorCategory::Terminal))?;
        handle
            .set(json!({ "attempt": 1_i32 }))
            .await
            .map_err(|_| StagingError(ErrorCategory::Terminal))?;
        Err(StagingError(ErrorCategory::Transient))
    }
}

impl FallibleHandler for StagingTransientHandler {
    type Error = StagingError;
    type Output = ();
    type Payload = Value;

    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        FallibleHandler::on_message(self, context, message, demand_type).await
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
        self.stage(&context).await
    }

    async fn on_timer<C>(
        &self,
        context: C,
        _trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.stage(&context).await
    }

    async fn after_commit<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks.lock().push(StagingHook::Commit);
    }

    async fn after_abort<C>(&self, _context: C, _result: Result<Self::Output, Self::Error>)
    where
        C: EventContext<Payload = Self::Payload>,
    {
        self.hooks.lock().push(StagingHook::Abort);
    }

    async fn shutdown(self) {}
}

impl SettlementHandler for StagingTransientHandler {
    fn settlement(_result: Result<&Self::Output, &Self::Error>) -> Settlement {
        Settlement::Final
    }
}
