//! The durable stores, the processes, and the partition assignments of the
//! routing property.

use super::*;

/// One consumer process of the group.
type Process = StateManagerProvider<Factory, ()>;

/// The state manager of one partition assignment.
type Manager = <Process as PartitionStateProvider<()>>::Manager;

/// Mints the backends of one process over the shared durable stores.
#[derive(Clone)]
pub(super) struct Factory {
    cells: MemoryCells,
    identities: MemoryDescriptorIdentityStore,
    routes: Routes<ScriptedPublicationStore>,
}

impl StateBackendFactory<()> for Factory {
    type Backend = PartitionBackend<
        MemoryDeduplicationStore,
        MemoryDescriptorIdentityStore,
        MemoryCellStore,
        (),
        ScriptedPublicationStore,
    >;

    fn for_partition(&self, _topic: Topic, _partition: Partition, (): ()) -> Self::Backend {
        PartitionBackend::new(
            MemoryDeduplicationStore::default(),
            self.identities.clone(),
            MemoryCellStore::new(self.cells.clone()),
            (),
            self.routes.clone(),
        )
    }
}

/// The durable stores, which outlive each process, and the live assignments.
pub(super) struct Harness {
    pub(super) publications: ScriptedPublicationStore,
    cells: MemoryCells,
    identities: MemoryDescriptorIdentityStore,
    pub(super) subsystem: SubsystemName,
    registry: Arc<CollectionDefRegistry>,
    pub(super) group: ConsumerGroup,
    pub(super) processes: Vec<Process>,
    pub(super) assignments: HashMap<Slot, Manager>,
    // The senders keep the termination receivers of each session open.
    shutdown: watch::Sender<ShutdownPhase>,
    cancel: watch::Sender<bool>,
}

impl Harness {
    /// Seeds the stale rows when `stale` is set, and starts each process.
    pub(super) async fn start(stale: bool, model: &mut Model) -> Result<Self> {
        let mut registry = CollectionDefRegistry::default();
        for (collection, visibility, commit_mode) in COLLECTIONS {
            registry.register(
                &value_state::<JsonCodec>(collection),
                CollectionDef {
                    commit_mode,
                    visibility,
                    ..CollectionDef::new(None)
                },
            )?;
        }
        let subsystem = SubsystemName::try_new("orders")?;
        let publications = ScriptedPublicationStore::new();
        if stale {
            // An earlier deployment published every collection from a topic
            // that the group left. Another group publishes them too.
            for (collection, (label, ..)) in COLLECTIONS.into_iter().enumerate() {
                for owner in [GROUP, OTHER_GROUP] {
                    let row = StatePublication {
                        group_id: Arc::from(owner),
                        topic: Topic::from(RETIRED_TOPIC.0),
                        partition_count: PartitionCount::try_from(RETIRED_TOPIC.1)?,
                    };
                    publications
                        .seed(&subsystem, StateType::Application, &name(label)?, &row)
                        .await;
                    model
                        .rows
                        .insert((collection, row.group_id, row.topic, RETIRED_TOPIC.1));
                }
            }
        }

        let (shutdown, _) = watch::channel(ShutdownPhase::default());
        let (cancel, _) = watch::channel(false);
        let mut harness = Self {
            publications,
            cells: MemoryCells::new(),
            identities: MemoryDescriptorIdentityStore::new(),
            subsystem,
            registry: Arc::new(registry),
            group: Arc::from(GROUP),
            processes: Vec::with_capacity(PROCESSES),
            assignments: HashMap::new(),
            shutdown,
            cancel,
        };
        for _ in 0..PROCESSES {
            let process = harness.process()?;
            harness.processes.push(process);
        }
        Ok(harness)
    }

    /// A new process with new routes over the durable stores.
    pub(super) fn process(&self) -> Result<Process> {
        let counts = TOPICS
            .iter()
            .map(|&(topic, count)| Ok((Topic::from(topic), PartitionCount::try_from(count)?)))
            .collect::<Result<Vec<_>>>()?;
        let routes = Routes::new(
            self.publications.clone(),
            self.subsystem.clone(),
            self.group.clone(),
            counts,
        );
        let factory = Factory {
            cells: self.cells.clone(),
            identities: self.identities.clone(),
            routes,
        };
        Ok(StateManagerProvider::new(
            factory,
            (),
            self.registry.clone(),
            self.group.clone(),
            CompactDuration::new(30),
        ))
    }

    /// Acquires `slot` as a new assignment. The routing store fails each
    /// removal with `failure`. Returns `false` on a model mismatch.
    pub(super) async fn assign(
        &mut self,
        model: &mut Model,
        slot: Slot,
        failure: Option<ErrorCategory>,
    ) -> Result<bool> {
        let (process, topic, partition) = slot;
        self.publications.fail_writes_with(failure);
        let removes = self.publications.removes();
        let acquired = self.processes[process]
            .acquire(Topic::from(TOPICS[topic].0), partition, ())
            .await;
        self.publications.fail_writes_with(None);

        // A failed removal never fails the acquisition. The next acquisition
        // of the process tries the removal again.
        let Ok(manager) = acquired else {
            return Ok(false);
        };
        let private = (0..COLLECTIONS.len()).filter(|&c| !published(c)).count();
        let expected = if model.withdrawn[process] { 0 } else { private };
        if self.publications.removes() - removes != expected {
            return Ok(false);
        }
        if failure.is_none() {
            model.withdrawn[process] = true;
            model
                .rows
                .retain(|row| published(row.0) || row.1.as_ref() != GROUP);
        }
        model.latched.retain(|(latched, _)| *latched != slot);
        self.assignments.insert(slot, manager);
        Ok(true)
    }

    /// Runs write number `n` through a new session of its assignment. Returns
    /// whether the commit applied or the finalize staged, and whether a cell is
    /// durable afterwards.
    pub(super) async fn write(
        &self,
        write: Write,
        n: u128,
    ) -> Result<(Result<bool, StateAccessError>, bool)> {
        let (_, topic, partition) = write.slot;
        let topic = Topic::from(TOPICS[topic].0);
        let name = name(COLLECTIONS[write.collection].0)?;
        let key: Key = Arc::from(format!("key-{n}"));
        let manager = self
            .assignments
            .get(&write.slot)
            .ok_or_else(|| eyre!("write {n} has no assignment"))?;
        let termination = TerminationWatch::new(self.shutdown.subscribe(), self.cancel.subscribe());
        let scope = manager.session(key.clone(), probe(n), termination);
        let session = scope.handle();
        if write.seeded {
            session
                .seed(StateType::Application, &name, &value_cell(), Some(b"v"))
                .await;
        }

        let result = if write.commit {
            let permit = session.permit().await;
            session
                .commit(&permit, StateType::Application, &name)
                .await
                .map(|outcome| matches!(outcome, StoreOutcome::Applied))
        } else {
            match session.finalize().await {
                Ok(Finalized::Clean) => Ok(false),
                Ok(Finalized::Staged(staged)) => {
                    let promoted = staged.promote(&RetryAlert::default(), || false).await;
                    ensure!(
                        matches!(promoted, Promoted::Complete),
                        "the promote of write {n} did not complete"
                    );
                    Ok(true)
                }
                Err(error) => Err(error),
            }
        };

        let segment = partition_segment_id(topic, partition, &self.group);
        let id = CollectionId::new(StateKey::new(segment, key), StateType::Application, name);
        let store = MemoryCellStore::new(self.cells.clone());
        let committed = CellRead::<Values>::read(&store, &id, value_cell().as_ref())
            .await?
            .0
            .into_inner();
        let durable = committed.is_some() || !self.cells.provisional_coordinates(&id).is_empty();
        Ok((result, durable))
    }

    /// Every routing row of each collection.
    pub(super) async fn rows(&self) -> Result<BTreeSet<Row>> {
        let mut rows = BTreeSet::new();
        for (collection, (label, ..)) in COLLECTIONS.into_iter().enumerate() {
            let found = self
                .publications
                .rows(&self.subsystem, StateType::Application, &name(label)?)
                .await;
            rows.extend(found.into_iter().map(|row| {
                let count = i32::from(row.partition_count);
                (collection, row.group_id, row.topic, count)
            }));
        }
        Ok(rows)
    }
}
