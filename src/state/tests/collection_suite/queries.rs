//! Query bounds match independent key and position models.

use super::{deque, drain, make_session, read_event, registry_and_ref, seed_deque_window};
use crate::codec::JsonCodec;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStore;
use crate::state::collection::StateSession;
use crate::state::descriptor::map::KeysetQuery;
use crate::state::descriptor::{CellType, CollectionSpec, StateDescriptor, deque_state};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::order_codec::OrderedKeyCodec;
use crate::state::registry::CollectionDef;
use crate::state::{Direction, StateKey};
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen};
use serde_json::Value;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use uuid::Uuid;

/// One random set of direction-relative stream constraints.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StreamConstraints {
    pub(super) start: Bound<i64>,
    pub(super) end: Bound<i64>,
    pub(super) limit: Option<NonZeroUsize>,
}

impl Arbitrary for StreamConstraints {
    fn arbitrary(g: &mut Gen) -> Self {
        let edge = |g: &mut Gen| Bound::<u8>::arbitrary(g).map(|n| i64::from(n % 7) - 3);
        Self {
            start: edge(g),
            end: edge(g),
            limit: Option::<NonZeroUsize>::arbitrary(g)
                .map(|n| NonZeroUsize::MIN.saturating_add(n.get() % 7)),
        }
    }
}

impl StreamConstraints {
    pub(super) fn contains(self, key: i64, dir: Direction) -> bool {
        match dir {
            Direction::Forward => (self.start, self.end),
            Direction::Backward => (self.end, self.start),
        }
        .contains(&key)
    }

    pub(super) fn apply<S, L>(self, mut query: KeysetQuery<'_, S, L>) -> KeysetQuery<'_, S, L>
    where
        S: StateSession,
        L: CollectionSpec,
        <L::Cell as CellType>::Key: OrderedKeyCodec<Key = i64>,
    {
        query = match self.start {
            Bound::Included(key) => query.from(&key),
            Bound::Excluded(key) => query.after(&key),
            Bound::Unbounded => query,
        };
        query = match self.end {
            Bound::Included(key) => query.to(&key),
            Bound::Excluded(key) => query.before(&key),
            Bound::Unbounded => query,
        };
        if let Some(limit) = self.limit {
            query = query.limit(limit);
        }
        query
    }
}

/// Position bounds include empty, reversed, and saturated intervals.
#[derive(Clone, Debug)]
pub(crate) struct DequeConstraints {
    range: (Bound<usize>, Bound<usize>),
    limit: Option<NonZeroUsize>,
    holes: Vec<bool>,
    head: i16,
}

impl Arbitrary for DequeConstraints {
    fn arbitrary(g: &mut Gen) -> Self {
        let edge = |g: &mut Gen| {
            Bound::<u16>::arbitrary(g).map(|n| {
                if n % 8 == 0 {
                    usize::MAX
                } else {
                    usize::from(n % 260)
                }
            })
        };
        Self {
            range: (edge(g), edge(g)),
            limit: Option::<usize>::arbitrary(g).and_then(|n| NonZeroUsize::new(n % 260)),
            holes: Vec::<bool>::arbitrary(g),
            head: i16::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let shape = self.clone();
        Box::new(self.holes.shrink().map(move |holes| Self {
            holes,
            ..shape.clone()
        }))
    }
}

/// Both plans preserve position bounds, direction, and the live-result limit.
pub(crate) async fn run_deque_constraint_parity(shape: DequeConstraints) -> Result<bool> {
    let cells = MemoryCells::new();
    let store = MemoryCellStore::new(cells.clone());
    let dedup = MemoryDeduplicationStore::default();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("query"));
    let descriptor = deque_state::<JsonCodec>("deque-query");
    let (registry, collection) = registry_and_ref(
        &descriptor,
        "deque-query",
        &state_key,
        CollectionDef::new(None),
    )?;
    for width in [
        0,
        5,
        deque::DEQUE_POINT_ITERATION_MAX,
        deque::DEQUE_POINT_ITERATION_MAX + 1,
        257,
    ] {
        let values: Vec<_> = (0..width)
            .map(|i| (!shape.holes.get(i).copied().unwrap_or(false)).then_some((i % 256) as u8))
            .collect();
        seed_deque_window(&store, &collection, i64::from(shape.head), &values).await?;
        let session = make_session(&cells, &dedup, &registry, &state_key, read_event(0));
        let handle = descriptor.bind(&session)?;
        for dir in [Direction::Forward, Direction::Backward] {
            let mut expected: Vec<_> = values
                .iter()
                .enumerate()
                .filter(|(i, _)| shape.range.contains(i))
                .filter_map(|(_, value)| value.map(Value::from))
                .collect();
            if dir == Direction::Backward {
                expected.reverse();
            }
            expected.truncate(shape.limit.map_or(usize::MAX, NonZeroUsize::get));
            let mut query = handle.query(dir).range(shape.range);
            if let Some(limit) = shape.limit {
                query = query.limit(limit);
            }
            assert_eq!(drain(query.values()).await?, expected);
        }
    }
    Ok(true)
}
