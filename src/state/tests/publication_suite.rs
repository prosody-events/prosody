//! Backend-generic routing-only publication-store suite.
//!
//! Random upsert/re-upsert/remove traces over a small pool vs a `BTreeMap`
//! oracle keyed by the full primary key `(subsystem, state_type, name, group,
//! topic)`; equivalence is asserted after EVERY op by re-reading every
//! `(subsystem, state_type, name)` triple in the pool. Instantiated by the
//! memory suite
//! (`QUICKCHECK_TESTS`) and the Cassandra suite (`INTEGRATION_TESTS`). Every
//! backend must satisfy the same invariants:
//!
//! * **Upsert idempotence** — re-`Upsert` of an existing `(group, topic)`
//!   overwrites the routing facts in place; a duplicate would surface as an
//!   extra row in the sorted set-equality.
//! * **Remove** — after a `Remove`, that source is gone from the read.
//! * **Subsystem/`state_type`/name isolation** — the pool holds the same name
//!   under two subsystems and two state types, and the same `(group, topic)`
//!   under different names, so any key that ignored one of the three would leak
//!   rows across partitions.
//!
//! The model is a plain `BTreeMap`, never a re-implementation of the store. No
//! identity is tested here — a publication row carries none (it is validated
//! against `keyed_state_identity` at acquisition).

use crate::Topic;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::tests::cell_suite::capped_vec;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, eyre};
use internment::Intern;
use quickcheck::{Arbitrary, Gen};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The distinct subsystems the pool spans, so the same name recurs across
/// subsystems (isolation).
const SUBSYSTEMS: usize = 2;
/// The state types the pool spans, so the same name recurs across namespaces
/// (isolation). `Framework` is the test-only second namespace.
const STATE_TYPES: [StateType; 2] = [StateType::Application, StateType::Framework];
/// The collection names the pool spans.
const NAMES: [&str; 3] = ["c0", "c1", "c2"];
/// The publishing groups the pool spans.
const GROUPS: [&str; 3] = ["g0", "g1", "g2"];
/// The topics the pool spans.
const TOPICS: [&str; 2] = ["t0", "t1"];

/// A full-PK oracle key: `(subsystem, state_type, name, group, topic)`.
type OracleKey = (String, i8, String, String, String);

/// The full-PK oracle key for one publication source.
fn oracle_key(
    subsystem: &SubsystemName,
    state_type: StateType,
    name: &StateName,
    group: &str,
    topic: Topic,
) -> OracleKey {
    (
        subsystem.as_str().to_owned(),
        i8::from(state_type),
        name.as_str().to_owned(),
        group.to_owned(),
        topic.as_ref().to_owned(),
    )
}

/// Resolves a subsystem seed to a name from the pool. `token` namespaces the
/// subsystem pool so concurrent/repeated runs against the shared keyspace never
/// collide, while the distinct subsystems within a run give isolation coverage.
fn subsystem_for(token: &str, seed: u8) -> Result<SubsystemName> {
    Ok(SubsystemName::try_new(format!(
        "{token}-s{}",
        usize::from(seed) % SUBSYSTEMS
    ))?)
}

/// Resolves a state-type seed to a namespace from the pool.
fn state_type_for(seed: u8) -> StateType {
    STATE_TYPES[usize::from(seed) % STATE_TYPES.len()]
}

/// Resolves a name seed to a collection name from the pool.
fn name_for(seed: u8) -> Result<StateName> {
    Ok(StateName::try_new(NAMES[usize::from(seed) % NAMES.len()])?)
}

/// Resolves a group seed to a publishing group from the pool.
fn group_for(seed: u8) -> &'static str {
    GROUPS[usize::from(seed) % GROUPS.len()]
}

/// Resolves a topic seed to a topic from the pool.
fn topic_for(seed: u8) -> Topic {
    Intern::<str>::from(TOPICS[usize::from(seed) % TOPICS.len()])
}

/// Resolves a count seed to a partition count in `[1, 16]`.
fn count_for(seed: u8) -> Result<PartitionCount> {
    Ok(PartitionCount::try_from(1 + i32::from(seed % 16))?)
}

/// One store operation in a [`PublicationTrace`].
#[derive(Clone, Debug)]
enum PublicationOp {
    /// Upsert `(group, topic, count)` under `(sub, st, name)`.
    Upsert {
        sub: u8,
        st: u8,
        name: u8,
        group: u8,
        topic: u8,
        count: u8,
    },

    /// Remove the `(group, topic)` source of `(sub, st, name)`.
    Remove {
        sub: u8,
        st: u8,
        name: u8,
        group: u8,
        topic: u8,
    },
}

impl Arbitrary for PublicationOp {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Upsert {
                sub: u8::arbitrary(g),
                st: u8::arbitrary(g),
                name: u8::arbitrary(g),
                group: u8::arbitrary(g),
                topic: u8::arbitrary(g),
                count: u8::arbitrary(g),
            }
        } else {
            Self::Remove {
                sub: u8::arbitrary(g),
                st: u8::arbitrary(g),
                name: u8::arbitrary(g),
                group: u8::arbitrary(g),
                topic: u8::arbitrary(g),
            }
        }
    }
}

/// A bounded random sequence of publication-store operations.
#[derive(Clone, Debug)]
pub(crate) struct PublicationTrace(Vec<PublicationOp>);

impl Arbitrary for PublicationTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(capped_vec(g, 24))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrinking the op vector (dropping ops) reduces a failing trace to a
        // minimal reproduction.
        Box::new(self.0.shrink().map(Self))
    }
}

/// Drives `store` and a plain `BTreeMap` model through `trace`, asserting
/// equivalence after every op by re-reading every pool
/// `(subsystem, state_type, name)`.
/// Returns `Ok(false)` on a model divergence (a real invariant break); store
/// errors propagate.
pub(crate) async fn run_publication_trace<S>(
    store: &S,
    token: &str,
    trace: PublicationTrace,
) -> Result<bool>
where
    S: PublicationStore,
{
    let mut oracle: BTreeMap<OracleKey, i32> = BTreeMap::new();
    for op in trace.0 {
        match op {
            PublicationOp::Upsert {
                sub,
                st,
                name,
                group,
                topic,
                count,
            } => {
                let subsystem = subsystem_for(token, sub)?;
                let state_type = state_type_for(st);
                let name = name_for(name)?;
                let group = group_for(group);
                let topic = topic_for(topic);
                let count = count_for(count)?;
                store
                    .upsert(
                        &subsystem,
                        state_type,
                        &name,
                        &StatePublication {
                            group_id: Arc::from(group),
                            topic,
                            partition_count: count,
                        },
                    )
                    .await
                    .map_err(|e| eyre!("upsert failed: {e}"))?;
                oracle.insert(
                    oracle_key(&subsystem, state_type, &name, group, topic),
                    i32::from(count),
                );
            }
            PublicationOp::Remove {
                sub,
                st,
                name,
                group,
                topic,
            } => {
                let subsystem = subsystem_for(token, sub)?;
                let state_type = state_type_for(st);
                let name = name_for(name)?;
                let group = group_for(group);
                let topic = topic_for(topic);
                store
                    .remove(&subsystem, state_type, &name, group, topic)
                    .await
                    .map_err(|e| eyre!("remove failed: {e}"))?;
                oracle.remove(&oracle_key(&subsystem, state_type, &name, group, topic));
            }
        }

        // Re-read every pool (subsystem, state_type, name) and compare sorted
        // set-equality against the oracle's matching prefix. Content, not
        // order — PublicationStore makes no clustering-order guarantee (both
        // backends happen to return group/topic-ascending anyway).
        for s in 0..u8::try_from(SUBSYSTEMS)? {
            for t in 0..u8::try_from(STATE_TYPES.len())? {
                for n in 0..u8::try_from(NAMES.len())? {
                    let subsystem = subsystem_for(token, s)?;
                    let state_type = state_type_for(t);
                    let name = name_for(n)?;
                    let mut got: Vec<(String, String, i32)> = store
                        .read_publications(&subsystem, state_type, &name)
                        .await
                        .map_err(|e| eyre!("read_publications failed: {e}"))?
                        .into_iter()
                        .map(|p| {
                            (
                                p.group_id.to_string(),
                                p.topic.as_ref().to_owned(),
                                i32::from(p.partition_count),
                            )
                        })
                        .collect();
                    got.sort();
                    let mut expected: Vec<(String, String, i32)> = oracle
                        .iter()
                        .filter(|((sub, st, nm, ..), _)| {
                            sub == subsystem.as_str()
                                && *st == i8::from(state_type)
                                && nm == name.as_str()
                        })
                        .map(|((.., group, topic), count)| (group.clone(), topic.clone(), *count))
                        .collect();
                    expected.sort();
                    if got != expected {
                        return Ok(false);
                    }
                }
            }
        }
    }
    Ok(true)
}
