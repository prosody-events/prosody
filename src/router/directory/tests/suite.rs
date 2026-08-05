//! The shared node directory suite and its plain map oracle.
//!
//! Every directory statement runs at `LOCAL_ONE`. That level does not provide
//! read-after-write consistency on a multi-node cluster. The shared
//! `prosody_test` keyspace uses one local node. A `LOCAL_ONE` read therefore
//! follows a `LOCAL_ONE` write in this suite.

use super::support::{ArbRegistration, endpoint, label, node_id};
use crate::router::directory::{
    Endpoint, GroupMembership, NetworkId, NodeDirectory, NodeRegistration,
};
use crate::router::{Host, MAX_LABEL_BYTES, NodeId};
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use fixedstr::Flexstr;
use quickcheck::{Arbitrary, Gen};
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

/// A lease longer than any suite run, so no entry ages out mid-trace and the
/// two backends' clocks cannot disagree.
pub(crate) const STABLE_LEASE: Duration = Duration::from_mins(10);

const LABELS: [Label; 6] = [
    Label::DirectHost,
    Label::AdvertisedHost,
    Label::Network,
    Label::Cluster,
    Label::Group,
    Label::Hostname,
];

/// One operation against a generated registration pool.
#[derive(Clone, Debug)]
pub(crate) enum DirectoryOp {
    Register(usize, u16),
    Read(usize),
    Deregister(usize),
}

/// A bounded operation trace over a bounded registration pool.
#[derive(Clone, Debug)]
pub(crate) struct DirectoryTrace {
    pool: Vec<NodeRegistration>,
    ops: Vec<DirectoryOp>,
}

/// One label that the directory bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Label {
    DirectHost,
    AdvertisedHost,
    Network,
    Cluster,
    Group,
    Hostname,
}

impl Arbitrary for DirectoryTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let pool_len = 1 + usize::arbitrary(g) % MAX_POOL;
        let mut pool = Vec::with_capacity(pool_len);
        for _ in 0..pool_len {
            let ArbRegistration(mut registration) = ArbRegistration::arbitrary(g);
            registration.node = node_id(g);
            registration.direct = endpoint(g);
            registration.hostname = Host::make(&label(g));
            pool.push(registration);
        }

        let op_len = 1 + usize::arbitrary(g) % MAX_OPS;
        let mut ops = Vec::with_capacity(op_len);
        ops.push(DirectoryOp::Register(0, u16::arbitrary(g)));
        for _ in 1..op_len {
            let index = usize::arbitrary(g) % pool_len;
            let op = match u8::arbitrary(g) % 3 {
                0 => DirectoryOp::Register(index, u16::arbitrary(g)),
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
    let mut answers = Vec::with_capacity(trace.ops.len() + trace.pool.len());
    for op in &trace.ops {
        let index = match *op {
            DirectoryOp::Register(index, port) => {
                let mut registration = trace.pool[index].clone();
                registration.direct.port = port;
                directory.register(&registration).await?;
                index
            }
            DirectoryOp::Read(index) => index,
            DirectoryOp::Deregister(index) => {
                directory.deregister(&trace.pool[index]).await?;
                index
            }
        };
        answers.push(directory.read(trace.pool[index].node).await?);
    }
    for registration in &trace.pool {
        answers.push(directory.read(registration.node).await?);
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
            DirectoryOp::Register(index, port) => {
                let mut registration = trace.pool[index].clone();
                registration.direct.port = port;
                model.insert(registration.node, registration);
                index
            }
            DirectoryOp::Read(index) => index,
            DirectoryOp::Deregister(index) => {
                model.remove(&trace.pool[index].node);
                index
            }
        };
        answers.push(model.get(&trace.pool[index].node).cloned());
    }
    for registration in &trace.pool {
        answers.push(model.get(&registration.node).cloned());
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
        if left.get(index) == right.get(index) {
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
/// The heap assertion is what makes the address cache bounded in bytes as well
/// as in entries: the cache charges one unit per entry however many bytes that
/// entry holds. The entry goes rather than the label, because a shorter label
/// would resolve a different host.
pub(crate) async fn run_label_bound_case<D: NodeDirectory>(directory: &D) -> Result<()> {
    let bounded = labelled(NodeId::new(), None);
    directory.register(&bounded).await?;
    let read = directory
        .read(bounded.node)
        .await?
        .ok_or_else(|| eyre!("a registration at the bound must resolve"))?;
    ensure!(
        read == bounded,
        "a registration at the bound did not survive the round trip"
    );
    let inline = read.direct.host.is_fixed()
        && read.hostname.is_fixed()
        && read.network.as_ref().is_some_and(Flexstr::is_fixed)
        && read
            .advertised
            .as_ref()
            .is_some_and(|entry| entry.host.is_fixed())
        && read
            .group
            .as_ref()
            .is_some_and(|membership| membership.cluster.is_fixed() && membership.group.is_fixed());
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

/// A registration with bounded labels, except for the selected oversized one.
fn labelled(node: NodeId, over: Option<Label>) -> NodeRegistration {
    let text = |label: Label| "n".repeat(MAX_LABEL_BYTES + usize::from(over == Some(label)));
    NodeRegistration {
        node,
        direct: Endpoint {
            host: Host::make(&text(Label::DirectHost)),
            port: 7777,
        },
        advertised: Some(Endpoint {
            host: Host::make(&text(Label::AdvertisedHost)),
            port: 443,
        }),
        network: Some(NetworkId::make(&text(Label::Network))),
        group: Some(GroupMembership {
            cluster: Flexstr::make(&text(Label::Cluster)),
            group: Flexstr::make(&text(Label::Group)),
        }),
        hostname: Host::make(&text(Label::Hostname)),
    }
}
