//! The advertised routes must equal the routes that each assignment wrote.
//!
//! Two processes of one group share the durable stores. Each partition has one
//! owner process. Each process owns its routes and acquires its partitions
//! through a real `StateManagerProvider`, all at the same time, as a rebalance
//! does. Each acquisition is one partition assignment with its own latches.
//! Writes use both durable write paths: the settle-time finalize and the
//! mid-handler commit. A plain model holds the expected rows.

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
use crate::state::{SharedStateBackend, StateKey, StoreOutcome};
use crate::test_util::ModelProperty;
use crate::timers::duration::CompactDuration;
use crate::{Key, Partition};
use color_eyre::eyre::{Result, ensure, eyre};
use futures::future::join_all;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use std::collections::{BTreeMap, BTreeSet};
use strum::VariantArray;
use tokio::runtime::Builder;
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

/// A partition `(topic, partition)` of a subscribed topic, with the topic as an
/// index into [`TOPICS`].
type Slot = (usize, Partition);

/// A routing row `(collection, group, topic, partition count)`.
type Row = (usize, ConsumerGroup, Topic, i32);

/// One durable write of one collection from one partition. It runs in the
/// assignment of the partition owner. An unseeded write buffers nothing, so it
/// has nothing to write.
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
    /// collection.
    Remove { collection: usize },
    /// A rebalance moves a partition to `process`, which can be its owner.
    Reassign { slot: Slot, process: usize },
    /// The owner of `slot` stops. A new process acquires all of its partitions.
    /// The routing store fails the removals of these acquisitions with
    /// `failure`.
    Restart {
        slot: Slot,
        failure: Option<ErrorCategory>,
    },
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 10 {
            0 => Self::Restart {
                slot: slot(g),
                failure: failure(g),
            },
            1 => Self::Remove {
                collection: usize::arbitrary(g) % COLLECTIONS.len(),
            },
            2 | 3 => Self::Reassign {
                slot: slot(g),
                process: usize::arbitrary(g) % PROCESSES,
            },
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

fn published(collection: usize) -> bool {
    COLLECTIONS[collection].1 == StateVisibility::Published
}

fn slot(g: &mut Gen) -> Slot {
    let topic = usize::arbitrary(g) % TOPICS.len();
    (topic, Partition::from(u8::arbitrary(g)) % TOPICS[topic].1)
}

/// Every partition of the subscribed topics.
fn slots() -> impl Iterator<Item = Slot> {
    (0..TOPICS.len())
        .flat_map(|topic| (0_i32..TOPICS[topic].1).map(move |partition| (topic, partition)))
}

/// A store failure in one third of the cases, of any category.
fn failure(g: &mut Gen) -> Option<ErrorCategory> {
    let category = ErrorCategory::VARIANTS[usize::arbitrary(g) % ErrorCategory::VARIANTS.len()];
    (u8::arbitrary(g) % 3 == 0).then_some(category)
}

/// Runs `first` and a concurrent `racer`. Returns `Ok(false)` on a model
/// mismatch.
async fn run_writes(
    harness: &Harness,
    model: &mut Model,
    first: Write,
    racer: Option<Write>,
    failure: Option<ErrorCategory>,
    n: u128,
) -> Result<bool> {
    harness.publications.fail_writes_with(failure);
    let upserts = harness.publications.upserts();
    let outcomes = match racer {
        Some(racer) => {
            let (one, two) = tokio::join!(harness.write(first, n), harness.write(racer, n + 1));
            vec![(first, one?), (racer, two?)]
        }
        None => vec![(first, harness.write(first, n).await?)],
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
        let (label, count) = TOPICS[slot.0];
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
        let owned: Vec<Slot> = slots().skip(process).step_by(PROCESSES).collect();
        if !harness
            .assign(&mut model, process, &owned, trace.failure)
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
                writes += 2;
                run_writes(&harness, &mut model, first, racer, failure, writes).await?
            }
            Op::Remove { collection } => {
                let name = StateName::try_new(COLLECTIONS[collection].0)?;
                harness
                    .publications
                    .remove_group(&harness.subsystem, StateType::Application, &name, GROUP)
                    .await?;
                model
                    .rows
                    .retain(|row| row.0 != collection || row.1.as_ref() != GROUP);
                true
            }
            Op::Reassign { slot, process } => {
                harness.assignments.remove(&slot);
                harness.assign(&mut model, process, &[slot], None).await?
            }
            Op::Restart { slot, failure } => {
                let process = harness.assignments[&slot].0;
                let owned: Vec<Slot> = harness
                    .assignments
                    .iter()
                    .filter(|(_, (owner, _))| *owner == process)
                    .map(|(&slot, _)| slot)
                    .collect();
                harness
                    .assignments
                    .retain(|_, (owner, _)| *owner != process);
                harness.processes[process] = harness.process()?;
                model.withdrawn[process] = false;
                harness.assign(&mut model, process, &owned, failure).await?
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

/// After each operation, the routing table holds exactly the expected rows.
/// [`Assignment`] owns the rule for which rows exist.
///
/// A write whose upsert fails returns an error that is not Permanent and
/// leaves no durable cell. A write with nothing buffered writes no row and
/// never fails. Concurrent acquisitions of a process make one removal attempt.
/// A failed removal never fails an acquisition, and the next acquisition of
/// the process tries again. Stale rows of published collections and of other
/// groups stay. A process never acquires a topic that it does not subscribe to
/// while a collection is published.
#[test]
fn advertised_routes_equal_the_topics_that_wrote() {
    // A current-thread runtime defers each yield to the end of the poll, so a
    // suspended store call lets the other acquisitions and writes run.
    fn property(trace: Trace) -> Result<bool> {
        Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run_trace(trace))
    }
    QuickCheck::new().quickcheck(ModelProperty(property));
}
