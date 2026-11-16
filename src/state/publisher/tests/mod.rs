//! The advertised routes must equal the routes that each assignment wrote.
//!
//! Two processes of one group share the durable stores. Each process owns its
//! routes and acquires its partitions through a real `StateManagerProvider`.
//! Each acquisition is one partition assignment with its own latches. Writes
//! use both durable write paths: the settle-time finalize and the mid-handler
//! commit. A plain model holds the expected rows.

use super::*;
use crate::JsonCodec;
use crate::consumer::partition::ShutdownPhase;
use crate::error::RetryAlert;
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
use crate::state::{PartitionBackend, StateBackendFactory, StateKey, StoreOutcome};
use crate::test_util::{ModelProperty, TEST_RUNTIME};
use crate::timers::duration::CompactDuration;
use crate::{Key, Partition};
use color_eyre::eyre::{Result, ensure, eyre};
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::BTreeSet;
use strum::VariantArray;
use tokio::sync::watch;

mod harness;

use harness::Harness;

const GROUP: &str = "group-a";
const OTHER_GROUP: &str = "group-b";

/// A topic that an earlier deployment of the group subscribed to.
const RETIRED_TOPIC: (&str, i32) = ("orders-retired", 2);

/// The subscribed topics and their partition counts.
const TOPICS: [(&str, i32); 3] = [("orders-a", 3), ("orders-b", 7), ("orders-c", 1)];

/// The registered collections. The first two are published. `wishlist` is
/// private, so it never has a route.
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

/// A partition assignment `(process, topic, partition)`, with indices into the
/// fixture tables.
type Slot = (usize, usize, Partition);

/// A routing row `(collection, group, topic, partition count)`.
type Row = (usize, ConsumerGroup, Topic, i32);

/// One durable write of one collection from one partition of one topic. An
/// unseeded write buffers nothing, so it has nothing to write.
#[derive(Clone, Copy, Debug)]
struct Write {
    slot: Slot,
    collection: usize,
    seeded: bool,
    commit: bool,
}

impl Arbitrary for Write {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            slot: slot(g),
            collection: usize::arbitrary(g) % COLLECTIONS.len(),
            seeded: u8::arbitrary(g) % 4 != 0,
            commit: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// One write, or two concurrent writes. The routing store fails each
    /// upsert with `failure`.
    Write {
        first: Write,
        racer: Option<Write>,
        failure: Option<ErrorCategory>,
    },
    /// An operator or an older version removes the rows of the group for a
    /// published collection.
    Remove { collection: usize },
    /// A rebalance moves a partition away from its process and back.
    Reassign { slot: Slot },
    /// The process stops, and a new process acquires a partition. The routing
    /// store fails the removals of that acquisition with `failure`.
    Restart {
        process: usize,
        failure: Option<ErrorCategory>,
    },
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 10 {
            0 => Self::Restart {
                process: usize::arbitrary(g) % PROCESSES,
                failure: failure(g),
            },
            1 => Self::Remove {
                collection: usize::arbitrary(g) % 2,
            },
            2 | 3 => Self::Reassign { slot: slot(g) },
            _ => Self::Write {
                first: Write::arbitrary(g),
                racer: Option::arbitrary(g),
                failure: failure(g),
            },
        }
    }
}

/// `stale` seeds rows that earlier deployments left in the routing table. The
/// routing store fails the removals of the first acquisitions with `failure`.
#[derive(Clone, Debug)]
struct Trace {
    stale: bool,
    failure: Option<ErrorCategory>,
    ops: Vec<Op>,
}

impl Arbitrary for Trace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            stale: bool::arbitrary(g),
            failure: failure(g),
            ops: Vec::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let (stale, failure) = (self.stale, self.failure);
        Box::new(self.ops.shrink().map(move |ops| Self {
            stale,
            failure,
            ops,
        }))
    }
}

/// The expected routing table, and the routes that each live assignment made
/// durable.
#[derive(Default)]
struct Model {
    rows: BTreeSet<Row>,
    latched: BTreeSet<(Slot, usize)>,
    /// Whether each live process removed the rows of the private collections.
    withdrawn: [bool; PROCESSES],
}

fn name(value: &str) -> Result<StateName> {
    StateName::try_new(value).map_err(|error| eyre!("name: {error}"))
}

fn published(collection: usize) -> bool {
    COLLECTIONS[collection].1 == StateVisibility::Published
}

fn slot(g: &mut Gen) -> Slot {
    (
        usize::arbitrary(g) % PROCESSES,
        usize::arbitrary(g) % TOPICS.len(),
        Partition::from(u8::arbitrary(g) % 2),
    )
}

/// The first assignment of a process: partition 0 of the first topic.
fn home(process: usize) -> Slot {
    (process, 0, Partition::from(0_u8))
}

/// A store failure in one third of the cases, of any category.
fn failure(g: &mut Gen) -> Option<ErrorCategory> {
    let category = ErrorCategory::VARIANTS[usize::arbitrary(g) % ErrorCategory::VARIANTS.len()];
    (u8::arbitrary(g) % 3 == 0).then_some(category)
}

/// Runs one write operation. Returns `Ok(false)` on a model mismatch.
async fn run_writes(
    harness: &mut Harness,
    model: &mut Model,
    writes: &[Write],
    failure: Option<ErrorCategory>,
    n: u128,
) -> Result<bool> {
    // A write runs in the live assignment of its partition. A partition that
    // has no assignment is acquired first.
    for write in writes {
        if !harness.assignments.contains_key(&write.slot)
            && !harness.assign(model, write.slot, None).await?
        {
            return Ok(false);
        }
    }
    harness.publications.fail_writes_with(failure);
    let upserts = harness.publications.upserts();
    let outcomes = match writes {
        [first, racer] => {
            let (one, two) = tokio::join!(harness.write(*first, n), harness.write(*racer, n + 1));
            vec![(*first, one?), (*racer, two?)]
        }
        _ => vec![(writes[0], harness.write(writes[0], n).await?)],
    };
    harness.publications.fail_writes_with(None);

    // The routes that each assignment latched in this operation.
    let mut opened: BTreeSet<(Slot, usize)> = BTreeSet::new();
    for (write, (result, durable)) in outcomes {
        let route = (write.slot, write.collection);
        let needs_upsert =
            write.seeded && published(write.collection) && !model.latched.contains(&route);
        if let (true, Some(category)) = (needs_upsert, failure) {
            // The route is not durable, so no cell can be durable. A routing
            // failure is never Permanent, so settle retries the event.
            let expected = match category {
                ErrorCategory::Terminal => ErrorCategory::Terminal,
                ErrorCategory::Transient | ErrorCategory::Permanent => ErrorCategory::Transient,
            };
            if !matches!(&result, Err(error) if error.classify_error() == expected) || durable {
                return Ok(false);
            }
        } else {
            // An unseeded write writes nothing, and it never fails. A
            // `ReadUncommitted` finalize writes its cells but stages nothing.
            let reported = write.seeded
                && (write.commit || COLLECTIONS[write.collection].2 == CommitMode::ReadCommitted);
            if !matches!(result, Ok(staged) if staged == reported) || durable != write.seeded {
                return Ok(false);
            }
            if needs_upsert {
                opened.insert(route);
            }
        }
    }
    // Concurrent first writers in one assignment share one upsert.
    if failure.is_none() && harness.publications.upserts() - upserts != opened.len() {
        return Ok(false);
    }
    for (slot, collection) in opened {
        let (_, topic, _) = slot;
        let (label, count) = TOPICS[topic];
        model.latched.insert((slot, collection));
        model
            .rows
            .insert((collection, harness.group.clone(), Topic::from(label), count));
    }
    Ok(true)
}

/// Runs `trace` against the model. Returns `Ok(false)` on a model mismatch.
async fn run_trace(trace: Trace) -> Result<bool> {
    let mut model = Model::default();
    let mut harness = Harness::start(trace.stale, &mut model).await?;
    for process in 0..PROCESSES {
        if !harness
            .assign(&mut model, home(process), trace.failure)
            .await?
        {
            return Ok(false);
        }
    }
    let mut writes = 0_u128;

    for op in trace.ops {
        let matched = match op {
            Op::Write {
                first,
                racer,
                failure,
            } => {
                let batch: Vec<Write> = [first].into_iter().chain(racer).collect();
                writes += 2;
                run_writes(&mut harness, &mut model, &batch, failure, writes).await?
            }
            Op::Remove { collection } => {
                let name = name(COLLECTIONS[collection].0)?;
                harness
                    .publications
                    .remove_group(&harness.subsystem, StateType::Application, &name, GROUP)
                    .await?;
                model
                    .rows
                    .retain(|row| row.0 != collection || row.1.as_ref() != GROUP);
                true
            }
            Op::Reassign { slot } => harness.assign(&mut model, slot, None).await?,
            Op::Restart { process, failure } => {
                harness.processes[process] = harness.process()?;
                harness.assignments.retain(|slot, _| slot.0 != process);
                model.latched.retain(|(slot, _)| slot.0 != process);
                model.withdrawn[process] = false;
                harness.assign(&mut model, home(process), failure).await?
            }
        };
        if !matched || harness.rows().await? != model.rows {
            return Ok(false);
        }
    }

    // A publishing process never acquires a topic that it does not subscribe
    // to.
    let unsubscribed = Topic::from("orders-unsubscribed");
    let acquired = harness.processes[0].acquire(unsubscribed, 0, ()).await;
    Ok(matches!(acquired, Err(StateAcquireError::Unrouted(topic)) if topic == unsubscribed))
}

/// After each operation, the routing table holds exactly the expected rows. A
/// published collection advertises each topic that wrote it in a live or past
/// assignment, unless a removal from outside deleted the row. Each new
/// assignment writes the row again before its first write. A write whose
/// upsert fails returns an error that is not Permanent and leaves no durable
/// cell. A write with nothing buffered writes no row and never fails.
/// Concurrent first writers in one assignment share one upsert. A failed
/// removal of private rows never fails an acquisition, and the next
/// acquisition of the process completes it. Stale rows of published
/// collections and of other groups stay.
#[test]
fn advertised_routes_equal_the_topics_that_wrote() {
    fn property(trace: Trace) -> Result<bool> {
        TEST_RUNTIME.block_on(run_trace(trace))
    }
    QuickCheck::new().quickcheck(ModelProperty(property));
}
