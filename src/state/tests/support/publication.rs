//! Scripted publication storage used by routing and refresh tests.

use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublicationCall {
    Upsert {
        name: String,
        group: String,
        topic: String,
        partition_count: i32,
    },
    Remove {
        name: String,
        group: String,
    },
    Read {
        name: String,
    },
}

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
    calls: Arc<Mutex<Vec<PublicationCall>>>,
    read_fail: Arc<Mutex<Option<ErrorCategory>>>,
    remove_fail: Arc<Mutex<Option<ErrorCategory>>>,
    read_gate: Arc<Mutex<Option<Arc<ReleaseGate>>>>,
}

impl ScriptedPublicationStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryPublicationStore::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            read_fail: Arc::new(Mutex::new(None)),
            remove_fail: Arc::new(Mutex::new(None)),
            read_gate: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn fail_reads_with(&self, category: ErrorCategory) {
        *self.read_fail.lock() = Some(category);
    }

    pub(crate) fn fail_removes_with(&self, category: ErrorCategory) {
        *self.remove_fail.lock() = Some(category);
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

    pub(crate) fn calls(&self) -> Vec<PublicationCall> {
        self.calls.lock().clone()
    }

    pub(crate) fn reads(&self) -> usize {
        self.calls
            .lock()
            .iter()
            .filter(|call| matches!(call, PublicationCall::Read { .. }))
            .count()
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
        self.calls.lock().push(PublicationCall::Upsert {
            name: name.as_str().to_owned(),
            group: row.group_id.to_string(),
            topic: row.topic.to_string(),
            partition_count: i32::from(row.partition_count),
        });
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
        self.calls.lock().push(PublicationCall::Remove {
            name: name.as_str().to_owned(),
            group: group_id.to_owned(),
        });
        if let Some(category) = *self.remove_fail.lock() {
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
        self.calls.lock().push(PublicationCall::Read {
            name: name.as_str().to_owned(),
        });
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
