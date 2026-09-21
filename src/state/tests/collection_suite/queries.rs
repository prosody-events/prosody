//! Query bounds match independent key and position models.

use super::{deque, drain, make_session, read_event, registry_and_ref, seed_deque_window};
use crate::codec::JsonCodec;
use crate::consumer::middleware::deduplication::MemoryDeduplicationStore;
use crate::state::descriptor::{StateDescriptor, deque_state, map_state, set_state};
use crate::state::memory::{MemoryCellStore, MemoryCells};
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use crate::state::query::tests::query_buffer;
use crate::state::registry::CollectionDef;
use crate::state::{BorrowedKeyQuery, DequeQuery, Direction, KeyQuery, StateKey};
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

    pub(super) fn apply<'a, KC>(
        &'a self,
        mut query: BorrowedKeyQuery<'a, KC>,
    ) -> BorrowedKeyQuery<'a, KC>
    where
        KC: OrderedKeyCodec<Key = i64, Borrowed = i64>,
    {
        query = match self.start.as_ref() {
            Bound::Included(key) => query.from(key),
            Bound::Excluded(key) => query.after(key),
            Bound::Unbounded => query,
        };
        query = match self.end.as_ref() {
            Bound::Included(key) => query.to(key),
            Bound::Excluded(key) => query.before(key),
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
        let cursor = bool::arbitrary(g).then(|| {
            if bool::arbitrary(g) {
                format!("{prefix}{}", word(g, 3))
            } else {
                word(g, 3)
            }
        });
        Self {
            keys: (0..u8::arbitrary(g) % 64).map(|_| word(g, 3)).collect(),
            prefix,
            cursor,
            limit: Option::<NonZeroUsize>::arbitrary(g)
                .map(|n| NonZeroUsize::MIN.saturating_add(n.get() % 7)),
            tracked: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let shape = self.clone();
        let keys = self.keys.shrink().map(move |keys| Self {
            keys,
            ..shape.clone()
        });
        let cursor = self.cursor.as_ref().map(|_| Self {
            cursor: None,
            ..self.clone()
        });
        let limit = self.limit.map(|_| Self {
            limit: None,
            ..self.clone()
        });
        Box::new(keys.chain(cursor).chain(limit))
    }
}

impl PrefixShape {
    fn contains(&self, key: &str, dir: Direction, end: Option<&str>) -> bool {
        match (dir, self.cursor.as_deref()) {
            (Direction::Forward, Some(cursor)) => key > cursor && end.is_none_or(|end| key < end),
            (Direction::Backward, Some(cursor)) => key < cursor && key >= self.prefix.as_str(),
            (_, None) => key >= self.prefix.as_str() && end.is_none_or(|end| key < end),
        }
    }

    fn apply<'a>(&'a self, query: BorrowedKeyQuery<'a>) -> BorrowedKeyQuery<'a> {
        let mut query = query.prefix(self.prefix.as_str());
        if let Some(cursor) = self.cursor.as_deref() {
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

    let mut end = shape.prefix.clone();
    let end = end.pop().map(|last| {
        end.push(char::from(last as u8 + 1));
        end
    });
    let distinct: BTreeSet<_> = shape.keys.iter().cloned().collect();
    for dir in [Direction::Forward, Direction::Backward] {
        let mut expected: Vec<_> = distinct
            .iter()
            .filter(|key| shape.contains(key, dir, end.as_deref()))
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
        assert_eq!(
            drain(
                map.entries(query_buffer())
                    .with_query(shape.apply(KeyQuery::new().direction(dir)))
                    .stream()
            )
            .await?,
            entries
        );
        assert_eq!(
            drain(
                map.keys(query_buffer())
                    .with_query(shape.apply(KeyQuery::new().direction(dir)))
                    .stream()
            )
            .await?,
            expected
        );
        assert_eq!(
            drain(
                set.keys(query_buffer())
                    .with_query(shape.apply(KeyQuery::new().direction(dir)))
                    .stream()
            )
            .await?,
            expected
        );
    }
    Ok(true)
}

/// Both plans preserve prefix edges, cursor replacement, direction, and result
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
            let mut query = DequeQuery::new().direction(dir).range(shape.range);
            if let Some(limit) = shape.limit {
                query = query.limit(limit);
            }
            assert_eq!(
                drain(handle.values().with_query(query).stream()).await?,
                expected
            );
        }
    }
    Ok(true)
}
