//! Query bounds match independent key and position models.

use super::{deque, drain, make_session, read_event, registry_and_ref, seed_deque_window};
use crate::codec::JsonCodec;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStore;
use crate::state::collection::StateSession;
use crate::state::descriptor::map::KeysetQuery;
use crate::state::descriptor::{
    CellType, CollectionSpec, StateDescriptor, deque_state, map_state, set_state,
};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use crate::state::registry::CollectionDef;
use crate::state::{Direction, StateKey};
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::collections::BTreeSet;
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
        <L::Cell as CellType>::Key: OrderedKeyCodec<Key = i64, Borrowed = i64>,
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

/// Prefix queries share their population, cursor, and limit across projections.
#[derive(Clone, Debug)]
struct PrefixShape {
    keys: Vec<String>,
    prefix: String,
    cursor: Option<String>,
    limit: Option<NonZeroUsize>,
    tracked: bool,
}

impl Arbitrary for PrefixShape {
    fn arbitrary(g: &mut Gen) -> Self {
        let word = |g: &mut Gen, max: u8| {
            (0..u8::arbitrary(g) % (max + 1))
                .map(|_| char::from(b'a' + u8::arbitrary(g) % 3))
                .collect::<String>()
        };
        let prefix = word(g, 2);
        let cursor = bool::arbitrary(g).then(|| format!("{prefix}{}", word(g, 3)));
        Self {
            keys: (0..u8::arbitrary(g) % 64).map(|_| word(g, 3)).collect(),
            prefix,
            cursor,
            limit: Option::<NonZeroUsize>::arbitrary(g)
                .map(|n| NonZeroUsize::MIN.saturating_add(n.get() % 7)),
            tracked: bool::arbitrary(g),
        }
    }
}

impl PrefixShape {
    fn apply<'a, S, L>(&self, query: KeysetQuery<'a, S, L>) -> KeysetQuery<'a, S, L>
    where
        S: StateSession,
        L: CollectionSpec,
        <L::Cell as CellType>::Key: OrderedKeyCodec<Borrowed = str>,
    {
        let mut query = query.prefix(&self.prefix);
        if let Some(cursor) = &self.cursor {
            query = query.after(cursor);
        }
        if let Some(limit) = self.limit {
            query = query.limit(limit);
        }
        query
    }
}

async fn run_prefix_query(shape: PrefixShape) -> Result<bool> {
    let cells = MemoryCells::new();
    let dedup = MemoryDeduplicationStore::default();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("prefix"));
    let definition = CollectionDef {
        keyset_limit: if shape.tracked { 4096 } else { 0 },
        ..CollectionDef::new(None)
    };
    let map = map_state::<Utf8KeyCodec, JsonCodec>("prefix-map");
    let set = set_state::<Utf8KeyCodec>("prefix-set");
    let (map_registry, _) = registry_and_ref(&map, "prefix-map", &state_key, definition)?;
    let (set_registry, _) = registry_and_ref(&set, "prefix-set", &state_key, definition)?;
    let map_session = make_session(&cells, &dedup, &map_registry, &state_key, read_event(0));
    let set_session = make_session(&cells, &dedup, &set_registry, &state_key, read_event(1));
    let map = map.bind(&map_session)?;
    let set = set.bind(&set_session)?;
    for key in &shape.keys {
        map.set(key, Value::from(key.clone())).await?;
        set.insert(key).await?;
    }

    let distinct: BTreeSet<_> = shape.keys.iter().cloned().collect();
    for dir in [Direction::Forward, Direction::Backward] {
        let mut expected: Vec<_> = distinct
            .iter()
            .filter(|key| key.starts_with(&shape.prefix))
            .filter(|key| {
                shape.cursor.as_ref().is_none_or(|cursor| match dir {
                    Direction::Forward => *key > cursor,
                    Direction::Backward => *key < cursor,
                })
            })
            .cloned()
            .collect();
        if dir == Direction::Backward {
            expected.reverse();
        }
        expected.truncate(shape.limit.map_or(usize::MAX, NonZeroUsize::get));
        let entries: Vec<_> = expected
            .iter()
            .map(|key| (key.clone(), Value::from(key.clone())))
            .collect();
        assert_eq!(drain(shape.apply(map.query(dir)).entries()).await?, entries);
        assert_eq!(drain(shape.apply(map.query(dir)).keys()).await?, expected);
        assert_eq!(drain(shape.apply(set.query(dir)).keys()).await?, expected);
    }
    Ok(true)
}

/// Both plans preserve prefixes, cursor exclusion, direction, and result
/// limits.
#[test]
fn prop_prefix_query() {
    fn property(shape: PrefixShape) -> Result<bool> {
        TEST_RUNTIME.block_on(run_prefix_query(shape))
    }
    QuickCheck::new().quickcheck(property as fn(PrefixShape) -> Result<bool>);
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
