//! The advertised routes must equal the topics that wrote each collection.
//!
//! Two processes of one group share the durable stores. Each process owns its
//! routes and acquires its partitions through a real `StateManagerProvider`.
//! Writes use both durable write paths: the settle-time finalize and the
//! mid-handler commit. A restart replaces one process while the other keeps
//! its latches. A plain model holds the expected rows.

use super::*;
use crate::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::{ClassifyError, ErrorCategory, RetryAlert};
use crate::segment::partition_segment_id;
use crate::state::cell::Values;
use crate::state::descriptor::value_state;
use crate::state::manager::{
    PartitionStateManager, PartitionStateProvider, StateAcquireError, StateManagerProvider,
};
use crate::state::memory::{MemoryCellStore, MemoryCells, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CommitMode, StateVisibility};
use crate::state::session::sealed::StateLifecycle;
use crate::state::session::{Finalized, Promoted, TerminationWatch};
use crate::state::store::CellRead;
use crate::state::tests::cell_suite::{MemoryDeduplicationStore, value_cell};
use crate::state::tests::support::{ScriptedPublicationStore, probe};
use crate::state::{CollectionId, PartitionBackend, StateBackendFactory, StateKey};
use crate::test_util::{ModelProperty, TEST_RUNTIME};
use crate::timers::duration::CompactDuration;
use crate::{Key, Partition};
use color_eyre::eyre::{Result, ensure, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::BTreeSet;
use tokio::sync::watch;

const GROUP: &str = "group-a";
const OTHER_GROUP: &str = "group-b";

/// A topic that an earlier deployment of the group subscribed to.
const RETIRED_TOPIC: (&str, i32) = ("orders-retired", 2);

/// The subscribed topics and their partition counts.
const TOPICS: [(&str, i32); 3] = [("orders-a", 3), ("orders-b", 7), ("orders-c", 1)];

/// The registered collections. `wishlist` is private, so it never has a route.
const COLLECTIONS: [(&str, StateVisibility, CommitMode); 3] = [
    (
        "cart",
        StateVisibility::Published,
        CommitMode::ReadCommitted,
    ),
    (
        "ledger",
        StateVisibility::Published,
        CommitMode::ReadUncommitted,
    ),
    (
        "wishlist",
        StateVisibility::Private,
        CommitMode::ReadCommitted,
    ),
];

/// The number of processes in the group.
const PROCESSES: usize = 2;

/// One consumer process of the group.
type Process = StateManagerProvider<Factory, ()>;

/// A `(collection, topic)` pair, as indices into the fixture tables.
type RouteIndex = (usize, usize);

/// Mints the backends of one process over the shared durable stores.
#[derive(Clone)]
struct Factory {
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

/// The durable write path that a handler uses.
#[derive(Clone, Copy, Debug)]
enum Funnel {
    Finalize,
    Commit,
}

/// One durable write of one collection from one partition of one topic.
#[derive(Clone, Copy, Debug)]
struct Write {
    process: usize,
    topic: usize,
    partition: Partition,
    collection: usize,
    funnel: Funnel,
}

impl Arbitrary for Write {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            process: usize::arbitrary(g) % PROCESSES,
            topic: usize::arbitrary(g) % TOPICS.len(),
            partition: Partition::from(u8::arbitrary(g) % 2),
            collection: usize::arbitrary(g) % COLLECTIONS.len(),
            funnel: if bool::arbitrary(g) {
                Funnel::Finalize
            } else {
                Funnel::Commit
            },
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// One write, or two concurrent writes. The routing store rejects each
    /// upsert when `upsert_fails` is set.
    Write {
        first: Write,
        racer: Option<Write>,
        upsert_fails: bool,
    },
    /// The process stops, and a new process acquires a partition. When
    /// `removal_fails` is set, the first acquisition fails and the next one
    /// succeeds.
    Restart { process: usize, removal_fails: bool },
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        if u8::arbitrary(g) % 6 == 0 {
            return Self::Restart {
                process: usize::arbitrary(g) % PROCESSES,
                removal_fails: bool::arbitrary(g),
            };
        }
        Self::Write {
            first: Write::arbitrary(g),
            racer: Option::arbitrary(g),
            upsert_fails: u8::arbitrary(g) % 4 == 0,
        }
    }
}

/// `stale` seeds rows that earlier deployments left in the routing table.
#[derive(Clone, Debug)]
struct Trace {
    stale: bool,
    ops: Vec<Op>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            stale: bool::arbitrary(g),
            ops: Vec::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let stale = self.stale;
        Box::new(self.ops.shrink().map(move |ops| Self { stale, ops }))
    }
}

/// The durable stores, which outlive each process, and the live processes.
struct Harness {
    publications: ScriptedPublicationStore,
    cells: MemoryCells,
    identities: MemoryDescriptorIdentityStore,
    subsystem: SubsystemName,
    registry: Arc<CollectionDefRegistry>,
    group: ConsumerGroup,
    processes: Vec<Process>,
    // The senders keep the termination receivers of each session open.
    shutdown: watch::Sender<ShutdownPhase>,
    cancel: watch::Sender<bool>,
}

impl Harness {
    /// Seeds the stale rows when `stale` is set, then starts each process.
    async fn start(stale: bool) -> Result<Self> {
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
            for (collection, ..) in COLLECTIONS {
                for owner in [GROUP, OTHER_GROUP] {
                    let row = StatePublication {
                        group_id: Arc::from(owner),
                        topic: Topic::from(RETIRED_TOPIC.0),
                        partition_count: PartitionCount::try_from(RETIRED_TOPIC.1)?,
                    };
                    publications
                        .seed(&subsystem, StateType::Application, &name(collection)?, &row)
                        .await;
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
            shutdown,
            cancel,
        };
        for process in 0..PROCESSES {
            let started = harness.process()?;
            harness.processes.push(started);
            harness.join(process, false).await?;
        }

        let unsubscribed = Topic::from("orders-unsubscribed");
        let acquired = harness.processes[0].acquire(unsubscribed, 0, ()).await;
        ensure!(
            matches!(acquired, Err(StateAcquireError::Unrouted(topic)) if topic == unsubscribed),
            "a publishing process acquired a topic that it does not subscribe to"
        );
        Ok(harness)
    }

    /// A new process with new routes over the durable stores.
    fn process(&self) -> Result<Process> {
        let counts = TOPICS
            .iter()
            .map(|&(topic, count)| Ok((Topic::from(topic), PartitionCount::try_from(count)?)))
            .collect::<Result<Vec<_>>>()?;
        let routes = Routes::new(
            self.publications.clone(),
            self.subsystem.clone(),
            &self.registry,
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

    /// Replaces `process` with a new process.
    async fn restart(&mut self, process: usize, removal_fails: bool) -> Result<()> {
        self.processes[process] = self.process()?;
        self.join(process, removal_fails).await
    }

    /// Acquires the first partition of `process`, as an assignment does.
    async fn join(&self, process: usize, removal_fails: bool) -> Result<()> {
        let topic = Topic::from(TOPICS[0].0);
        if removal_fails {
            self.publications
                .fail_writes_with(Some(ErrorCategory::Transient));
            let acquired = self.processes[process].acquire(topic, 0, ()).await;
            self.publications.fail_writes_with(None);
            ensure!(
                matches!(&acquired, Err(error @ StateAcquireError::Withdrawal(_))
                    if error.classify_error() == ErrorCategory::Transient),
                "a failed removal did not fail the acquisition"
            );
        }
        self.processes[process].acquire(topic, 0, ()).await?;
        Ok(())
    }

    /// Runs write number `n` through a new session of the write's partition.
    /// Returns the write result, and whether the cell is durable afterwards.
    async fn write(&self, write: Write, n: u128) -> Result<(Result<(), StateAccessError>, bool)> {
        let topic = Topic::from(TOPICS[write.topic].0);
        let name = name(COLLECTIONS[write.collection].0)?;
        let key: Key = Arc::from(format!("key-{n}"));
        let manager = self.processes[write.process]
            .acquire(topic, write.partition, ())
            .await?;
        let termination = TerminationWatch::new(self.shutdown.subscribe(), self.cancel.subscribe());
        let scope = manager.session(key.clone(), probe(n), termination);
        let session = scope.handle();
        session
            .seed(StateType::Application, &name, &value_cell(), Some(b"v"))
            .await;

        let result = match write.funnel {
            Funnel::Commit => {
                let permit = session.permit().await;
                session
                    .commit(&permit, StateType::Application, &name)
                    .await
                    .map(|_| ())
            }
            Funnel::Finalize => match session.finalize().await {
                Ok(Finalized::Clean) => Ok(()),
                Ok(Finalized::Staged(staged)) => {
                    let promoted = staged.promote(&RetryAlert::default(), || false).await;
                    ensure!(
                        matches!(promoted, Promoted::Complete),
                        "the promote of write {n} did not complete"
                    );
                    Ok(())
                }
                Err(error) => Err(error),
            },
        };

        let segment = partition_segment_id(topic, write.partition, &self.group);
        let id = CollectionId::new(StateKey::new(segment, key), StateType::Application, name);
        let store = MemoryCellStore::new(self.cells.clone());
        let committed = CellRead::<Values>::read(&store, &id, value_cell().as_ref())
            .await?
            .0
            .into_inner();
        let durable = committed.is_some() || !self.cells.provisional_coordinates(&id).is_empty();
        Ok((result, durable))
    }

    /// The `(topic, partition count)` rows of `group` for `collection`.
    async fn rows(&self, collection: usize, group: &str) -> Result<BTreeSet<(Topic, i32)>> {
        let name = name(COLLECTIONS[collection].0)?;
        Ok(self
            .publications
            .rows(&self.subsystem, StateType::Application, &name)
            .await
            .into_iter()
            .filter(|row| row.group_id.as_ref() == group)
            .map(|row| (row.topic, i32::from(row.partition_count)))
            .collect())
    }

    /// Compares the routing table with the model's `advertised` routes.
    async fn matches(&self, advertised: &BTreeSet<RouteIndex>, stale: bool) -> Result<bool> {
        let retired: BTreeSet<_> = stale
            .then_some((Topic::from(RETIRED_TOPIC.0), RETIRED_TOPIC.1))
            .into_iter()
            .collect();
        for collection in 0..COLLECTIONS.len() {
            let mut expected: BTreeSet<(Topic, i32)> = advertised
                .iter()
                .filter(|(written, _)| *written == collection)
                .map(|&(_, topic)| (Topic::from(TOPICS[topic].0), TOPICS[topic].1))
                .collect();
            // A process removes the stale row of a private collection only.
            if published(collection) {
                expected.extend(&retired);
            }
            if self.rows(collection, GROUP).await? != expected
                || self.rows(collection, OTHER_GROUP).await? != retired
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn name(value: &str) -> Result<StateName> {
    StateName::try_new(value).map_err(|error| eyre!("name: {error}"))
}

fn published(collection: usize) -> bool {
    COLLECTIONS[collection].1 == StateVisibility::Published
}

/// Runs `trace` against the model. Returns `Ok(false)` on a model mismatch.
async fn run_trace(trace: Trace) -> Result<bool> {
    let mut harness = Harness::start(trace.stale).await?;
    // The model: the routes that the group advertises, and the routes that
    // each live process made durable.
    let mut advertised: BTreeSet<RouteIndex> = BTreeSet::new();
    let mut latched: [BTreeSet<RouteIndex>; PROCESSES] = Default::default();
    let mut writes = 0_u128;

    for op in trace.ops {
        match op {
            Op::Restart {
                process,
                removal_fails,
            } => {
                harness.restart(process, removal_fails).await?;
                latched[process].clear();
            }
            Op::Write {
                first,
                racer,
                upsert_fails,
            } => {
                let failure = upsert_fails.then_some(ErrorCategory::Transient);
                harness.publications.fail_writes_with(failure);
                let before = harness.publications.upserts();
                let outcomes = match racer {
                    Some(racer) => {
                        let (one, two) = tokio::join!(
                            harness.write(first, writes),
                            harness.write(racer, writes + 1)
                        );
                        vec![(first, one?), (racer, two?)]
                    }
                    None => vec![(first, harness.write(first, writes).await?)],
                };
                writes += 2;
                harness.publications.fail_writes_with(None);

                // The routes that each process latched in this operation.
                let mut opened: BTreeSet<(usize, RouteIndex)> = BTreeSet::new();
                for (write, (result, durable)) in outcomes {
                    let route = (write.collection, write.topic);
                    let needs_upsert =
                        published(write.collection) && !latched[write.process].contains(&route);
                    if needs_upsert && upsert_fails {
                        // The route is not durable, so no cell can be durable.
                        let rejected = matches!(&result, Err(error)
                            if error.classify_error() == ErrorCategory::Transient);
                        if !rejected || durable {
                            return Ok(false);
                        }
                    } else {
                        if result.is_err() || !durable {
                            return Ok(false);
                        }
                        if needs_upsert {
                            opened.insert((write.process, route));
                        }
                    }
                }
                // Concurrent first writers in one process share one upsert.
                if !upsert_fails && harness.publications.upserts() - before != opened.len() {
                    return Ok(false);
                }
                for (process, route) in opened {
                    latched[process].insert(route);
                    advertised.insert(route);
                }
            }
        }
        if !harness.matches(&advertised, trace.stale).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// After each operation, a published collection advertises exactly the topics
/// that wrote it, with the partition count of each topic. Seeded stale rows of
/// published collections and of other groups stay. A private collection
/// advertises nothing for the group. A write whose upsert fails leaves no
/// durable cell. Concurrent first writers in one process share one upsert. A
/// restart of one process removes no row that the other process latched.
#[test]
fn advertised_routes_equal_the_topics_that_wrote() {
    fn property(trace: Trace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_trace(trace))
    }
    QuickCheck::new().quickcheck(ModelProperty(property));
}
