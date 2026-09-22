//! Query method order agrees with independent key and position bounds.

use super::{BorrowedKeyQuery, DequeQuery, ErasedKeyQuery, KeyQuery, ReadQuery};
use crate::codec::SerializeBufGuard;
use crate::state::Direction;
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use quickcheck::QuickCheck;
use std::borrow::Borrow;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};

/// Operation, first key, second key, and result limit.
pub(crate) type KeyStep = (u8, String, String, NonZeroUsize);

/// One edge rule. A selected key satisfies every rule.
enum Rule<T> {
    Low(Bound<T>),
    High(Bound<T>),
}

impl<T: Copy + PartialOrd> Rule<T> {
    /// Models `from`, `after`, `to`, or `before` in query direction `dir`.
    fn directional(op: u8, dir: Direction, key: T) -> Self {
        let bound = if op.is_multiple_of(2) {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        };
        if (op < 2) == (dir == Direction::Forward) {
            Self::Low(bound)
        } else {
            Self::High(bound)
        }
    }

    fn admits(&self, key: T) -> bool {
        match *self {
            Self::Low(low) => (low, Bound::Unbounded).contains(&key),
            Self::High(high) => (Bound::Unbounded, high).contains(&key),
        }
    }
}

/// Applies the same ordered operations to each public read interface.
pub(crate) fn key_query(dir: Direction, steps: &[KeyStep]) -> ErasedKeyQuery {
    key_read(ReadQuery::new(ErasedKeyQuery::new(), ()), dir, steps).into_query()
}

/// Applies ordered settings to a bound read without changing its source.
pub(crate) fn key_read<'a, B: From<&'a str> + Borrow<str> + Ord + Clone, S>(
    query: ReadQuery<KeyQuery<Utf8KeyCodec, B>, S>,
    dir: Direction,
    steps: &'a [KeyStep],
) -> ReadQuery<KeyQuery<Utf8KeyCodec, B>, S> {
    let mut query = query.direction(dir);
    for (op, a, b, limit) in steps {
        query = match op % 10 {
            0 => query.from(a),
            1 => query.after(a),
            2 => query.to(a),
            3 => query.before(a),
            4 => query.prefix(a),
            5 => query.limit(*limit),
            6 => query.range((
                Bound::Included(B::from(a.as_str())),
                Bound::Excluded(B::from(b.as_str())),
            )),
            7 => query.range(..),
            8 => query.forward(),
            _ => query.reverse(),
        };
    }
    query
}

/// Models each method as a rule that every selected key satisfies.
pub(crate) fn expected_keys<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    mut dir: Direction,
    steps: &[KeyStep],
) -> Vec<String> {
    let mut rules = Vec::new();
    let mut prefixes = Vec::new();
    let mut limit = usize::MAX;
    for (op, a, b, count) in steps {
        match op % 10 {
            op @ 0..=3 => rules.push(Rule::directional(op, dir, a.as_str())),
            4 => prefixes.push(a.as_str()),
            5 => limit = count.get(),
            6 => rules.extend([
                Rule::Low(Bound::Included(a.as_str())),
                Rule::High(Bound::Excluded(b.as_str())),
            ]),
            7 => {}
            8 => dir = Direction::Forward,
            _ => dir = Direction::Backward,
        }
    }
    let mut keys: Vec<_> = keys
        .into_iter()
        .filter(|key| {
            rules.iter().all(|rule| rule.admits(key))
                && prefixes.iter().all(|prefix| key.starts_with(prefix))
        })
        .map(str::to_owned)
        .collect();
    keys.sort();
    keys.dedup();
    if dir == Direction::Backward {
        keys.reverse();
    }
    keys.truncate(limit);
    keys
}

/// Maps a string to at most three letters from `a` and `b`. Prefix relations
/// and equal bounds then occur often.
fn small(key: &str) -> String {
    key.chars()
        .take(3)
        .map(|c| if u32::from(c) % 2 == 0 { 'a' } else { 'b' })
        .collect()
}

#[test]
fn prop_key_query_method_order() {
    fn property(
        mut keys: Vec<String>,
        mut steps: Vec<KeyStep>,
        narrow: bool,
    ) -> color_eyre::Result<()> {
        steps.truncate(128);
        if narrow {
            for key in keys
                .iter_mut()
                .chain(steps.iter_mut().flat_map(|(_, a, b, _)| [a, b]))
            {
                *key = small(key);
            }
        }
        keys.extend(steps.iter().flat_map(|(_, a, b, _)| [a.clone(), b.clone()]));
        for dir in [Direction::Forward, Direction::Backward] {
            // Check every prefix of the trace so later operations cannot hide a defect.
            for end in 0..=steps.len() {
                let steps = &steps[..end];
                let query = key_query(dir, steps);
                let query: ErasedKeyQuery = serde_json::from_slice(&serde_json::to_vec(&query)?)?;
                let mut buf = SerializeBufGuard::acquire();
                let query = query.encode(&mut buf)?;
                let mut coordinates: Vec<_> =
                    keys.iter().map(|key| Utf8KeyCodec::encode(key)).collect();
                coordinates.sort();
                coordinates.dedup();
                let mut actual = query.select(coordinates);
                actual.truncate(query.limit.map_or(usize::MAX, NonZeroUsize::get));
                let expected: Vec<_> = expected_keys(keys.iter().map(String::as_str), dir, steps)
                    .iter()
                    .map(|key| Utf8KeyCodec::encode(key))
                    .collect();
                assert_eq!(actual, expected, "direction: {dir:?}; steps: {steps:?}");
            }
        }
        Ok(())
    }
    for query in [ErasedKeyQuery::new(), ErasedKeyQuery::default()] {
        assert_eq!(query.dir, Direction::Forward);
    }
    QuickCheck::new()
        .quickcheck(property as fn(Vec<String>, Vec<KeyStep>, bool) -> color_eyre::Result<()>);
}

#[test]
fn prop_deque_query_method_order() {
    fn property(mut steps: Vec<(u8, u8, u8, NonZeroUsize)>) -> color_eyre::Result<()> {
        steps.truncate(128);
        for mut dir in [Direction::Forward, Direction::Backward] {
            let mut query = ReadQuery::new(DequeQuery::new(), ()).direction(dir);
            let mut rules = Vec::new();
            let mut expected_limit = None;
            for (op, a, b, limit) in &steps {
                let (op, a, b) = (op % 9, usize::from(*a), usize::from(*b));
                query = match op {
                    0 => query.from(a),
                    1 => query.after(a),
                    2 => query.to(a),
                    3 => query.before(a),
                    4 => query.range(a..=b),
                    5 => query.limit(*limit),
                    6 => query.range(..),
                    7 => query.forward(),
                    _ => query.reverse(),
                };
                match op {
                    0..=3 => rules.push(Rule::directional(op, dir, a)),
                    4 => rules.extend([
                        Rule::Low(Bound::Included(a)),
                        Rule::High(Bound::Included(b)),
                    ]),
                    5 => expected_limit = Some(*limit),
                    6 => {}
                    7 => dir = Direction::Forward,
                    _ => dir = Direction::Backward,
                }
                let settings = query.into_query();
                let settings: DequeQuery = serde_json::from_slice(&serde_json::to_vec(&settings)?)?;
                assert_eq!(settings.dir, dir);
                assert_eq!(settings.limit, expected_limit);
                let bounds = settings.bounds();
                for position in 0..=256 {
                    assert_eq!(
                        bounds.contains(&position),
                        rules.iter().all(|rule| rule.admits(position)),
                        "position: {position}; steps: {steps:?}"
                    );
                }
                query = ReadQuery::new(DequeQuery::new(), ()).with_query(settings);
            }
        }
        Ok(())
    }
    for query in [DequeQuery::new(), DequeQuery::default()] {
        assert_eq!(query.dir, Direction::Forward);
    }
    QuickCheck::new()
        .quickcheck(property as fn(Vec<(u8, u8, u8, NonZeroUsize)>) -> color_eyre::Result<()>);
}

/// A prefix endpoint selects exactly the bytes that start with that prefix.
#[test]
fn encoded_prefix_range_matches_bytes() {
    fn property(mut bytes: Vec<u8>, mut prefix: Vec<u8>, shared: bool, offset: u8) {
        for byte in bytes.iter_mut().chain(&mut prefix) {
            *byte = [0, 1, 254, 255][usize::from(*byte % 4)];
        }
        if shared {
            bytes.splice(..0, prefix.iter().copied());
        }
        let expected = bytes.starts_with(&prefix);
        let offset = usize::from(offset);
        let mut buffer = vec![42; offset];
        buffer.extend_from_slice(&prefix);
        let bounded = super::bounds::prefix_end(&mut buffer, offset);
        assert!(buffer[..offset].iter().all(|&byte| byte == 42));
        assert_eq!(
            expected,
            bytes >= prefix && (!bounded || bytes.as_slice() < &buffer[offset..])
        );
    }
    QuickCheck::new().quickcheck(property as fn(Vec<u8>, Vec<u8>, bool, u8));
}

/// Sequential bound encoding reuses the pool after its largest query.
#[test]
fn borrowed_queries_reuse_encoding_storage() {
    fn property(mut steps: Vec<KeyStep>) -> color_eyre::Result<()> {
        steps.truncate(128);
        let longest = steps
            .iter()
            .flat_map(|(_, a, b, _)| [a.as_str(), b.as_str()])
            .max_by_key(|key| key.len())
            .unwrap_or("");
        let mut buf = SerializeBufGuard::acquire();
        BorrowedKeyQuery::<Utf8KeyCodec>::new()
            .from(longest)
            .to(longest)
            .prefix(longest)
            .encode(&mut buf)?;
        let allocation = (buf.as_ptr(), buf.capacity());
        drop(buf);

        for end in 0..=steps.len() {
            let query = key_read(
                ReadQuery::new(BorrowedKeyQuery::new(), ()),
                Direction::Forward,
                &steps[..end],
            )
            .into_query();
            let mut buf = SerializeBufGuard::acquire();
            query.encode(&mut buf)?;
            assert_eq!((buf.as_ptr(), buf.capacity()), allocation);
        }
        Ok(())
    }
    QuickCheck::new().quickcheck(property as fn(Vec<KeyStep>) -> color_eyre::Result<()>);
}

/// Stored settings keep their field names, bound variants, and direction
/// values.
#[test]
fn query_json_is_stable() -> color_eyre::Result<()> {
    let key = ErasedKeyQuery::new()
        .prefix("a")
        .reverse()
        .after("ab")
        .limit(NonZeroUsize::MIN);
    assert_eq!(
        serde_json::to_string(&key)?,
        r#"{"dir":"Backward","limit":1,"edges":{"low":"Unbounded","high":{"Excluded":"ab"}},"prefix":"a"}"#
    );
    let deque = DequeQuery::new().after(2).to(9);
    assert_eq!(
        serde_json::to_string(&deque)?,
        r#"{"dir":"Forward","edges":{"low":{"Excluded":2},"high":{"Included":9}},"limit":null}"#
    );
    Ok(())
}
