//! Scripted publication storage used by routing and refresh tests.

use super::*;
use tokio::task::yield_now;

struct ReleaseGate {
    entered: Semaphore,
    released: Semaphore,
}

/// A read left parked at the read gate after gating stopped. A later read then
/// runs to completion while this one stays parked, which is how a schedule
/// proves an in-flight refresh blocks nobody.
pub(crate) struct ParkedRead(Arc<ReleaseGate>);

impl ParkedRead {
    /// Releases the parked read, consuming the handle: a park is released once.
    pub(crate) fn release(self) {
        self.0.released.add_permits(1);
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedPublicationStore {
    inner: MemoryPublicationStore,
    reads: Arc<AtomicUsize>,
    upserts: Arc<AtomicUsize>,
    removes: Arc<AtomicUsize>,
    read_fail: Arc<Mutex<Option<ErrorCategory>>>,
    write_fail: Arc<Mutex<Option<ErrorCategory>>>,
    read_gate: Arc<Mutex<Option<Arc<ReleaseGate>>>>,
}

impl ScriptedPublicationStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryPublicationStore::new(),
            reads: Arc::default(),
            upserts: Arc::default(),
            removes: Arc::default(),
            read_fail: Arc::new(Mutex::new(None)),
            write_fail: Arc::new(Mutex::new(None)),
            read_gate: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn fail_reads_with(&self, category: ErrorCategory) {
        *self.read_fail.lock() = Some(category);
    }

    /// Fails each upsert and removal with `category`. `None` heals the store.
    pub(crate) fn fail_writes_with(&self, category: Option<ErrorCategory>) {
        *self.write_fail.lock() = category;
    }

    pub(crate) fn heal_reads(&self) {
        *self.read_fail.lock() = None;
    }

    pub(crate) fn gate_reads(&self) {
        *self.read_gate.lock() = Some(Arc::new(ReleaseGate {
            entered: Semaphore::new(0),
            released: Semaphore::new(0),
        }));
    }

    pub(crate) async fn wait_read_entered(&self) {
        let gate = self.read_gate.lock().clone();
        if let Some(gate) = gate
            && let Ok(permit) = gate.entered.acquire().await
        {
            permit.forget();
        }
    }

    /// Stops gating new reads and returns a handle to the read already parked.
    pub(crate) fn stop_gating_reads(&self) -> Option<ParkedRead> {
        self.read_gate.lock().take().map(ParkedRead)
    }

    pub(crate) async fn seed(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) {
        let _ = self.inner.upsert(subsystem, state_type, name, row).await;
    }

    pub(crate) fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    pub(crate) fn upserts(&self) -> usize {
        self.upserts.load(Ordering::SeqCst)
    }

    pub(crate) fn removes(&self) -> usize {
        self.removes.load(Ordering::SeqCst)
    }

    pub(crate) async fn rows(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Vec<StatePublication> {
        self.inner
            .read_publications(subsystem, state_type, name)
            .await
            .unwrap_or_default()
            .into_vec()
    }
}

impl PublicationStore for ScriptedPublicationStore {
    type Error = ScriptedPublicationError;

    async fn upsert(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        row: &StatePublication,
    ) -> Result<(), Self::Error> {
        self.upserts.fetch_add(1, Ordering::SeqCst);
        // A real write suspends, so concurrent callers can interleave.
        yield_now().await;
        if let Some(category) = *self.write_fail.lock() {
            return Err(ScriptedPublicationError(category));
        }
        self.inner
            .upsert(subsystem, state_type, name, row)
            .await
            .map_err(|error| match error {})
    }

    async fn remove_group(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
        group_id: &str,
    ) -> Result<(), Self::Error> {
        self.removes.fetch_add(1, Ordering::SeqCst);
        yield_now().await;
        if let Some(category) = *self.write_fail.lock() {
            return Err(ScriptedPublicationError(category));
        }
        self.inner
            .remove_group(subsystem, state_type, name, group_id)
            .await
            .map_err(|error| match error {})
    }

    async fn read_publications(
        &self,
        subsystem: &SubsystemName,
        state_type: StateType,
        name: &StateName,
    ) -> Result<PublicationRows, Self::Error> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let gate = self.read_gate.lock().clone();
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            if let Ok(permit) = gate.released.acquire().await {
                permit.forget();
            }
        }
        if let Some(category) = *self.read_fail.lock() {
            return Err(ScriptedPublicationError(category));
        }
        self.inner
            .read_publications(subsystem, state_type, name)
            .await
            .map_err(|error| match error {})
    }
}

#[derive(Clone, Copy, Debug, Error)]
#[error("scripted publication error ({0:?})")]
pub(crate) struct ScriptedPublicationError(ErrorCategory);

impl ClassifyError for ScriptedPublicationError {
    fn classify_error(&self) -> ErrorCategory {
        self.0
    }
}
