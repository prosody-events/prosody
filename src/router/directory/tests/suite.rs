//! The shared node directory suite and its plain map oracle.
//!
//! Every directory statement runs at `LOCAL_ONE`. That level does not provide
//! read-after-write consistency on a multi-node cluster. The shared
//! `prosody_test` keyspace uses one local node. A `LOCAL_ONE` read therefore
//! follows a `LOCAL_ONE` write in this suite.

use super::support::{ArbRegistration, endpoint, label, node_id};
use crate::router::directory::{Endpoint, NetworkId, NodeDirectory, NodeRegistration};
use crate::router::{Host, MAX_LABEL_BYTES, NodeId};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use fixedstr::Flexstr;
use quickcheck::{Arbitrary, Gen};
use std::array::from_fn;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

/// Most nodes one trace names. The memory directory under test holds at least
/// this many, so no assertion here can be answered by an eviction.
const MAX_POOL: usize = 4;

/// What the suite's memory directory holds. Above [`MAX_POOL`], so the whole
/// pool stays resident and both backends answer from a full map.
///
/// `match`, not `NonZeroUsize::new(..).unwrap_or(..)`: `Option::unwrap_or` is
/// not const, and the tests forbid `unwrap`.
pub(crate) const SUITE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(capacity) => capacity,
    None => NonZeroUsize::MIN,
};

const _: () = assert!(SUITE_CAPACITY.get() > MAX_POOL);

/// Most operations one trace drives. Short enough that a failing trace is
/// readable.
const MAX_OPS: usize = 12;

/// How many registration shapes one node id has. Every optional field is drawn
/// per shape, so a re-registration can drop a field the entry before it
/// carried. That is the case a backend which left an absent column unset would
/// keep, and the case a port-only overwrite can never reach.
const SHAPES: usize = 2;

/// A lease longer than any suite run, so no entry ages out mid-trace and the
/// two backends' clocks cannot disagree.
pub(crate) const STABLE_LEASE: Duration = Duration::from_mins(10);

const LABELS: [Label; 2] = [Label::Network, Label::Hostname];

/// One operation against a generated registration pool. The first index names
/// a pooled node; `Register`'s second names which of that node's shapes it
/// publishes.
#[derive(Clone, Debug)]
pub(crate) enum DirectoryOp {
    Register(usize, usize),
    Read(usize),
    Deregister(usize),
}

/// A bounded operation trace over a bounded registration pool. Each pool entry
/// is one node id under [`SHAPES`] registration shapes.
#[derive(Clone, Debug)]
pub(crate) struct DirectoryTrace {
    pool: Vec<[NodeRegistration; SHAPES]>,
    ops: Vec<DirectoryOp>,
}

/// One label that the directory bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Label {
    Network,
    Hostname,
}

impl DirectoryTrace {
    /// The node id every shape at `index` shares.
    fn node(&self, index: usize) -> NodeId {
        self.pool[index][0].node
    }
}

impl Arbitrary for DirectoryTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let pool_len = 1 + usize::arbitrary(g) % MAX_POOL;
        let mut pool = Vec::with_capacity(pool_len);
        for _ in 0..pool_len {
            let node = node_id(g);
            pool.push(from_fn(|_| shape(g, node)));
        }

        let op_len = 1 + usize::arbitrary(g) % MAX_OPS;
        let mut ops = Vec::with_capacity(op_len);
        ops.push(DirectoryOp::Register(0, usize::arbitrary(g) % SHAPES));
        for _ in 1..op_len {
            let index = usize::arbitrary(g) % pool_len;
            let op = match u8::arbitrary(g) % 3 {
                0 => DirectoryOp::Register(index, usize::arbitrary(g) % SHAPES),
                1 => DirectoryOp::Read(index),
                _ => DirectoryOp::Deregister(index),
            };
            ops.push(op);
        }
        Self { pool, ops }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let pool = self.pool.clone();
        let ops = self.ops.clone();
        Box::new((1..ops.len()).rev().map(move |length| Self {
            pool: pool.clone(),
            ops: ops[..length].to_vec(),
        }))
    }
}

/// Drives `trace` and reports what the directory answered after every
/// operation, then after one final read of every pooled node.
///
/// Lockstep: each entry is the read of the node the operation touched, taken
/// immediately after it. A mutation that did nothing therefore cannot be
/// hidden by a later mutation of the same node.
pub(crate) async fn run_directory_trace<D: NodeDirectory>(
    directory: &D,
    trace: &DirectoryTrace,
) -> Result<Vec<Option<NodeRegistration>>> {
    // A shrunk trace names the nodes the run that failed already registered,
    // and a Cassandra entry outlives that run. Clearing first makes every
    // replay of a trace start where the oracle starts: empty.
    for shapes in &trace.pool {
        directory.deregister(&shapes[0]).await?;
    }
    let mut answers = Vec::with_capacity(trace.ops.len() + trace.pool.len());
    for op in &trace.ops {
        let index = match *op {
            DirectoryOp::Register(index, shape) => {
                directory.register(&trace.pool[index][shape]).await?;
                index
            }
            DirectoryOp::Read(index) => index,
            DirectoryOp::Deregister(index) => {
                directory.deregister(&trace.pool[index][0]).await?;
                index
            }
        };
        answers.push(directory.read(trace.node(index)).await?);
    }
    for shapes in &trace.pool {
        answers.push(directory.read(shapes[0].node).await?);
    }
    Ok(answers)
}

/// The same trace replayed against a plain `HashMap<NodeId, NodeRegistration>`.
/// Deliberately trivial: it is the oracle, never a second implementation.
pub(crate) fn expected_answers(trace: &DirectoryTrace) -> Vec<Option<NodeRegistration>> {
    let mut model = HashMap::with_capacity(trace.pool.len());
    let mut answers = Vec::with_capacity(trace.ops.len() + trace.pool.len());
    for op in &trace.ops {
        let index = match *op {
            DirectoryOp::Register(index, shape) => {
                let registration = trace.pool[index][shape].clone();
                model.insert(registration.node, registration);
                index
            }
            DirectoryOp::Read(index) => index,
            DirectoryOp::Deregister(index) => {
                model.remove(&trace.node(index));
                index
            }
        };
        answers.push(model.get(&trace.node(index)).cloned());
    }
    for shapes in &trace.pool {
        answers.push(model.get(&shapes[0].node).cloned());
    }
    answers
}

/// Reports the first index at which two answer vectors differ, naming the
/// operation that produced it.
pub(crate) fn first_divergence(
    trace: &DirectoryTrace,
    left: &[Option<NodeRegistration>],
    right: &[Option<NodeRegistration>],
) -> Option<String> {
    let length = left.len().max(right.len());
    for index in 0..length {
        if same_answer(left.get(index), right.get(index)) {
            continue;
        }
        let position = if index < trace.ops.len() {
            format!("operation {index} ({:?})", trace.ops[index])
        } else {
            format!("final pool read {}", index - trace.ops.len())
        };
        return Some(format!(
            "the answers differ after {position}: left={:?}, right={:?}",
            left.get(index),
            right.get(index)
        ));
    }
    None
}

/// A label is bounded at both ends: a registration whose labels are all exactly
/// `MAX_LABEL_BYTES` resolves and holds no label on the heap. One byte more on
/// any one label makes the whole entry unresolvable.
///
/// The heap assertion proves that accepted labels stay inline. The entry goes
/// rather than the label, because a shorter label would resolve a different
/// host.
pub(crate) async fn run_label_bound_case<D: NodeDirectory>(directory: &D) -> Result<()> {
    let bounded = labelled(NodeId::new(), None);
    directory.register(&bounded).await?;
    let read = directory
        .read(bounded.node)
        .await?
        .ok_or_else(|| eyre!("a registration at the bound must resolve"))?;
    ensure!(
        same_registration(&read, &bounded),
        "a registration at the bound did not survive the round trip"
    );
    let inline = read.hostname.is_fixed() && read.network.as_ref().is_some_and(Flexstr::is_fixed);
    ensure!(
        inline,
        "a resolved registration must hold no label on the heap"
    );

    for over in LABELS {
        let oversized = labelled(NodeId::new(), Some(over));
        directory.register(&oversized).await?;
        ensure!(
            directory.read(oversized.node).await?.is_none(),
            "a registration whose {over:?} is one byte over the bound must not resolve"
        );
    }
    Ok(())
}

/// A shutdown delete removes the entry, and repeating it changes nothing.
pub(crate) async fn run_idempotent_deregister_case<D: NodeDirectory>(directory: &D) -> Result<()> {
    let written = labelled(NodeId::new(), None);
    directory.register(&written).await?;
    ensure!(
        directory.read(written.node).await?.is_some(),
        "the registration must resolve before deletion"
    );
    for attempt in 1_u8..=2 {
        directory.deregister(&written).await?;
        ensure!(
            directory.read(written.node).await?.is_none(),
            "attempt {attempt}: the registration must stay absent"
        );
    }
    Ok(())
}

/// One generated registration for `node`. Every field but the id is drawn
/// afresh, so two shapes of one node can differ in their optional fields as
/// well as in their endpoint.
fn shape(g: &mut Gen, node: NodeId) -> NodeRegistration {
    let ArbRegistration(mut registration) = ArbRegistration::arbitrary(g);
    registration.node = node;
    registration.direct = endpoint(g);
    registration.hostname = Host::make(&label(g));
    registration
}

/// A registration with bounded labels, except for the selected oversized one.
fn labelled(node: NodeId, over: Option<Label>) -> NodeRegistration {
    let text = |label: Label| "n".repeat(MAX_LABEL_BYTES + usize::from(over == Some(label)));
    NodeRegistration {
        node,
        direct: Endpoint::from_static("http://direct.test"),
        advertised: Some(Endpoint::from_static("http://advertised.test")),
        network: Some(NetworkId::make(&text(Label::Network))),
        hostname: Host::make(&text(Label::Hostname)),
    }
}

fn same_answer(
    left: Option<&Option<NodeRegistration>>,
    right: Option<&Option<NodeRegistration>>,
) -> bool {
    match (left, right) {
        (Some(Some(left)), Some(Some(right))) => same_registration(left, right),
        (Some(None), Some(None)) | (None, None) => true,
        _ => false,
    }
}

pub(crate) fn same_registration(left: &NodeRegistration, right: &NodeRegistration) -> bool {
    left.node == right.node
        && left.direct.uri() == right.direct.uri()
        && left.advertised.as_ref().map(Endpoint::uri)
            == right.advertised.as_ref().map(Endpoint::uri)
        && left.network == right.network
        && left.hostname == right.hostname
}
