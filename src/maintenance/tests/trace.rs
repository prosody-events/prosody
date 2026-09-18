//! The catalog trace and its model oracle.
//!
//! The trace generates the operations production issues against the defer
//! stores. The model records what those operations leave behind, so the
//! properties compare each catalog against an obviously correct value and not
//! only against each other.

use crate::maintenance::{GroupId, Segment};
use crate::timers::datetime::CompactDateTime;
use crate::timers::duration::CompactDuration;
use crate::timers::store::Segment as TimerSegment;
use crate::{Offset, Partition, Topic};
use quickcheck::{Arbitrary, Gen, empty_shrinker};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::once;
use std::ops::RangeInclusive;
use uuid::Uuid;

/// The one topic every trace defers on. A fresh topic per iteration would leak
/// one interned string per case; the fresh group id isolates the rows instead.
const TOPIC: &str = "maintenance-catalog";

/// The slab size every trace writes into its timer segment rows.
pub(super) const SLAB_SIZE: CompactDuration = CompactDuration::new(600);

/// Partitions a trace spreads its keys over. The last one deliberately gets no
/// timer segment row, so an absent row is covered on both backends.
const PARTITIONS: RangeInclusive<usize> = 2..=3;

/// Keys a trace reuses, so collisions and re-use actually happen.
const KEYS: RangeInclusive<usize> = 2..=5;

/// Operations in a trace.
const OPS: RangeInclusive<usize> = 10..=40;

/// The operations production issues against the defer stores.
///
/// A raw `set_retry_count` is absent on purpose. A blind static write leaves
/// Cassandra a partition with no clustering row, which the memory store never
/// creates, so the two catalogs would disagree for a reason no production path
/// can produce.
#[derive(Clone, Copy, Debug)]
pub(super) enum CatalogOp {
    DeferMessage {
        partition: usize,
        key: usize,
        offset: u32,
    },
    CompleteMessage {
        partition: usize,
        key: usize,
    },
    IncrementMessageRetry {
        partition: usize,
        key: usize,
    },
    DeferTimer {
        partition: usize,
        key: usize,
        time: u32,
    },
    CompleteTimer {
        partition: usize,
        key: usize,
    },
    IncrementTimerRetry {
        partition: usize,
        key: usize,
    },
}

/// A trace of production-shaped defer operations over a small pool of
/// partitions and keys.
#[derive(Clone, Debug)]
pub(super) struct CatalogTrace {
    pub(super) partitions: usize,
    pub(super) keys: usize,
    pub(super) ops: Vec<CatalogOp>,
}

/// The store call one operation resolves to once the model has checked its
/// precondition.
#[derive(Clone, Copy, Debug)]
pub(super) enum Effect {
    DeferFirstMessage {
        partition: usize,
        key: usize,
        offset: Offset,
    },
    DeferAdditionalMessage {
        partition: usize,
        key: usize,
        offset: Offset,
    },
    CompleteMessage {
        partition: usize,
        key: usize,
        offset: Offset,
    },
    IncrementMessageRetry {
        partition: usize,
        key: usize,
    },
    DeferFirstTimer {
        partition: usize,
        key: usize,
        time: CompactDateTime,
    },
    DeferAdditionalTimer {
        partition: usize,
        key: usize,
        time: CompactDateTime,
    },
    CompleteTimer {
        partition: usize,
        key: usize,
        time: CompactDateTime,
    },
    IncrementTimerRetry {
        partition: usize,
        key: usize,
    },
}

/// The plain model of what the defer stores hold.
///
/// It records the two queues and the partitions a store call ever addressed.
/// The defer stores register a segment on first access, so a partition with no
/// store call never enters the registry.
#[derive(Debug, Default)]
pub(super) struct Model {
    messages: BTreeMap<(usize, usize), BTreeSet<Offset>>,
    timers: BTreeMap<(usize, usize), BTreeSet<CompactDateTime>>,
    registered: BTreeSet<usize>,
}

/// The timer segment row reduced to the fields a snapshot compares. The
/// expected and the observed value share this one conversion, so neither can
/// drift on its own.
pub(super) type TimerSegmentRow = (String, u32, i8);

/// Reduces a timer segment row to [`TimerSegmentRow`].
pub(super) fn timer_segment_row(row: TimerSegment) -> TimerSegmentRow {
    (row.name, row.slab_size.seconds(), i8::from(row.version))
}

/// What one catalog said, keyed by the segment's rendering so the comparison
/// needs no ordering on a segment and reads in a failure message.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Snapshot {
    pub(super) segments: BTreeSet<String>,
    pub(super) message_keys: BTreeMap<String, BTreeSet<String>>,
    pub(super) timer_keys: BTreeMap<String, BTreeSet<String>>,
    pub(super) timer_segments: BTreeMap<String, Option<TimerSegmentRow>>,
}

impl Arbitrary for CatalogTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let partitions = pick(g, &PARTITIONS);
        let keys = pick(g, &KEYS);
        let count = pick(g, &OPS);

        // Open with one deferral of each twin. Every trace then registers at
        // least one segment, so an empty snapshot can never pass by accident.
        let mut ops = vec![
            CatalogOp::DeferMessage {
                partition: 0,
                key: 0,
                offset: 0,
            },
            CatalogOp::DeferTimer {
                partition: 0,
                key: 0,
                time: 0,
            },
        ];
        ops.extend((ops.len()..count).map(|_| CatalogOp::generate(g, partitions, keys)));

        Self {
            partitions,
            keys,
            ops,
        }
    }

    /// Drops trailing operations. Without it a failing forty-operation trace is
    /// nearly undebuggable.
    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Never shrink past the two opening deferrals.
        if self.ops.len() <= 2 {
            return empty_shrinker();
        }
        let mut shorter = self.clone();
        shorter.ops.truncate(self.ops.len() - 1);
        Box::new(once(shorter))
    }
}

impl CatalogOp {
    /// One operation over the trace's partition and key pools. Deferrals are
    /// generated twice as often as completions, so queues survive a long trace.
    fn generate(g: &mut Gen, partitions: usize, keys: usize) -> Self {
        let partition = usize::arbitrary(g) % partitions;
        let key = usize::arbitrary(g) % keys;
        let offset = u32::arbitrary(g) % 64;

        match u8::arbitrary(g) % 8 {
            0 | 1 => Self::DeferMessage {
                partition,
                key,
                offset,
            },
            2 => Self::CompleteMessage { partition, key },
            3 => Self::IncrementMessageRetry { partition, key },
            4 | 5 => Self::DeferTimer {
                partition,
                key,
                time: offset,
            },
            6 => Self::CompleteTimer { partition, key },
            _ => Self::IncrementTimerRetry { partition, key },
        }
    }
}

impl Model {
    /// Records `op` and returns the store call it resolves to.
    ///
    /// Returns `None` when the operation's precondition does not hold, because
    /// production never issues it then.
    pub(super) fn apply(&mut self, op: CatalogOp) -> Option<Effect> {
        let effect = match op {
            CatalogOp::DeferMessage {
                partition,
                key,
                offset,
            } => {
                let offset = Offset::from(offset);
                let queue = self.messages.entry((partition, key)).or_default();
                let first = queue.is_empty();
                queue.insert(offset);
                if first {
                    Effect::DeferFirstMessage {
                        partition,
                        key,
                        offset,
                    }
                } else {
                    Effect::DeferAdditionalMessage {
                        partition,
                        key,
                        offset,
                    }
                }
            }
            CatalogOp::CompleteMessage { partition, key } => {
                let queue = self.messages.get_mut(&(partition, key))?;
                let offset = *queue.first()?;
                queue.remove(&offset);
                Effect::CompleteMessage {
                    partition,
                    key,
                    offset,
                }
            }
            CatalogOp::IncrementMessageRetry { partition, key } => {
                let queue = self.messages.get(&(partition, key))?;
                if queue.is_empty() {
                    return None;
                }
                Effect::IncrementMessageRetry { partition, key }
            }
            CatalogOp::DeferTimer {
                partition,
                key,
                time,
            } => {
                let time = CompactDateTime::from(time);
                let queue = self.timers.entry((partition, key)).or_default();
                let first = queue.is_empty();
                queue.insert(time);
                if first {
                    Effect::DeferFirstTimer {
                        partition,
                        key,
                        time,
                    }
                } else {
                    Effect::DeferAdditionalTimer {
                        partition,
                        key,
                        time,
                    }
                }
            }
            CatalogOp::CompleteTimer { partition, key } => {
                let queue = self.timers.get_mut(&(partition, key))?;
                let time = *queue.first()?;
                queue.remove(&time);
                Effect::CompleteTimer {
                    partition,
                    key,
                    time,
                }
            }
            CatalogOp::IncrementTimerRetry { partition, key } => {
                let queue = self.timers.get(&(partition, key))?;
                if queue.is_empty() {
                    return None;
                }
                Effect::IncrementTimerRetry { partition, key }
            }
        };

        self.registered.insert(effect.partition());
        Some(effect)
    }

    /// The snapshot a correct catalog must report for `group`.
    fn snapshot(&self, trace: &CatalogTrace, group: &GroupId) -> Snapshot {
        let mut out = Snapshot::default();
        for &partition in &self.registered {
            let label = segment(group, partition).to_string();
            out.segments.insert(label.clone());
            out.message_keys
                .insert(label.clone(), live_keys(&self.messages, partition));
            out.timer_keys
                .insert(label.clone(), live_keys(&self.timers, partition));
            out.timer_segments
                .insert(label, expected_timer_segment(trace, group, partition));
        }
        out
    }
}

impl Effect {
    pub(super) fn partition(self) -> usize {
        match self {
            Self::DeferFirstMessage { partition, .. }
            | Self::DeferAdditionalMessage { partition, .. }
            | Self::CompleteMessage { partition, .. }
            | Self::IncrementMessageRetry { partition, .. }
            | Self::DeferFirstTimer { partition, .. }
            | Self::DeferAdditionalTimer { partition, .. }
            | Self::CompleteTimer { partition, .. }
            | Self::IncrementTimerRetry { partition, .. } => partition,
        }
    }
}

/// A group id no other iteration and no earlier run can collide with. The
/// shared keyspace keeps every row ever written, so identity is the isolation.
pub(super) fn fresh_group() -> GroupId {
    GroupId::new(&format!("maintenance-catalog-{}", Uuid::new_v4()))
}

/// The topic every trace defers on.
pub(super) fn topic() -> Topic {
    Topic::from(TOPIC)
}

/// The name of one key in the trace's pool. The runner defers on it and the
/// model predicts it, so the format lives here once.
pub(super) fn key_name(index: usize) -> String {
    format!("catalog-key-{index}")
}

/// The segment a trace's partition addresses.
pub(super) fn segment(group: &GroupId, partition: usize) -> Segment {
    Segment::new(group.clone(), topic(), partition as Partition)
}

/// The snapshot the model alone predicts for `trace`.
pub(super) fn expected_snapshot(trace: &CatalogTrace, group: &GroupId) -> Snapshot {
    let mut model = Model::default();
    for &op in &trace.ops {
        let _ = model.apply(op);
    }
    model.snapshot(trace, group)
}

/// The timer store's segment for one partition of the trace.
pub(super) fn timer_segment(group: &GroupId, partition: usize) -> TimerSegment {
    TimerSegment::for_partition(group.as_str(), topic(), partition as Partition, SLAB_SIZE)
}

/// The timer segment row a correct catalog reports for one partition.
fn expected_timer_segment(
    trace: &CatalogTrace,
    group: &GroupId,
    partition: usize,
) -> Option<TimerSegmentRow> {
    if partition + 1 >= trace.partitions {
        return None;
    }
    Some(timer_segment_row(timer_segment(group, partition)))
}

/// The keys of one partition whose queue still holds a row.
fn live_keys<V>(
    queues: &BTreeMap<(usize, usize), BTreeSet<V>>,
    partition: usize,
) -> BTreeSet<String>
where
    V: Ord,
{
    queues
        .iter()
        .filter(|((owner, _), queue)| *owner == partition && !queue.is_empty())
        .map(|((_, key), _)| key_name(*key))
        .collect()
}

/// Picks a value from an inclusive range of sizes.
fn pick(g: &mut Gen, range: &RangeInclusive<usize>) -> usize {
    let span = range.end() - range.start() + 1;
    range.start() + usize::arbitrary(g) % span
}
