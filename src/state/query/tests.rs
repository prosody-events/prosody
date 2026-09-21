//! Query method order agrees with independent key and position bounds.

use super::{BorrowedKeyQuery, DequeQuery, ErasedKeyQuery, KeyQuery, ReadQuery};
use crate::codec::SerializeBufGuard;
use crate::state::Direction;
use crate::state::order_codec::{OrderedKeyCodec, Utf8KeyCodec};
use quickcheck::QuickCheck;
use std::num::NonZeroUsize;
use std::ops::Bound;

/// Operation, first key, second key, and result limit.
pub(crate) type KeyStep = (u8, String, String, NonZeroUsize);

enum Upper<'a> {
    Bound(Bound<&'a str>),
    Prefix(&'a str),
}

/// Applies the same ordered operations to each public read interface.
pub(crate) fn key_query(dir: Direction, steps: &[KeyStep]) -> ErasedKeyQuery {
    key_read(ReadQuery::new(ErasedKeyQuery::new(), ()), dir, steps).into_query()
}

/// Applies ordered settings to a bound read without changing its source.
pub(crate) fn key_read<'a, B: From<&'a str> + Clone, S>(
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

/// Models prefix bounds with string predicates, without a byte successor.
pub(crate) fn expected_keys<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    mut dir: Direction,
    steps: &[KeyStep],
) -> Vec<String> {
    let mut low = Bound::Unbounded;
    let mut high = Upper::Bound(Bound::Unbounded);
    let mut limit = usize::MAX;
    for (op, a, b, count) in steps {
        let op = op % 10;
        match op {
            0..=3 => {
                let bound = if op % 2 == 0 {
                    Bound::Included(a.as_str())
                } else {
                    Bound::Excluded(a.as_str())
                };
                if (op < 2) == (dir == Direction::Forward) {
                    low = bound;
                } else {
                    high = Upper::Bound(bound);
                }
            }
            4 => {
                low = Bound::Included(a.as_str());
                high = Upper::Prefix(a);
            }
            5 => limit = count.get(),
            6 => {
                low = Bound::Included(a.as_str());
                high = Upper::Bound(Bound::Excluded(b.as_str()));
            }
            7 => {
                low = Bound::Unbounded;
                high = Upper::Bound(Bound::Unbounded);
            }
            8 => dir = Direction::Forward,
            _ => dir = Direction::Backward,
        }
    }
    let mut keys: Vec<_> = keys
        .into_iter()
        .filter(|key| {
            let above = match low {
                Bound::Included(low) => *key >= low,
                Bound::Excluded(low) => *key > low,
                Bound::Unbounded => true,
            };
            above
                && match high {
                    Upper::Bound(Bound::Included(high)) => *key <= high,
                    Upper::Bound(Bound::Excluded(high)) => *key < high,
                    Upper::Bound(Bound::Unbounded) => true,
                    Upper::Prefix(prefix) => *key < prefix || key.starts_with(prefix),
                }
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

#[test]
fn prop_key_query_method_order() {
    fn property(mut keys: Vec<String>, mut steps: Vec<KeyStep>) -> color_eyre::Result<()> {
        steps.truncate(128);
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
        .quickcheck(property as fn(Vec<String>, Vec<KeyStep>) -> color_eyre::Result<()>);
}

#[test]
fn prop_deque_query_method_order() {
    fn property(mut steps: Vec<(u8, usize, usize, NonZeroUsize)>) -> color_eyre::Result<()> {
        steps.truncate(128);
        for mut dir in [Direction::Forward, Direction::Backward] {
            let mut query = ReadQuery::new(DequeQuery::new(), ()).direction(dir);
            let (mut low, mut high) = (Bound::Unbounded, Bound::Unbounded);
            let mut expected_limit = None;
            for (op, a, b, limit) in &steps {
                let op = op % 9;
                query = match op {
                    0 => query.from(*a),
                    1 => query.after(*a),
                    2 => query.to(*a),
                    3 => query.before(*a),
                    4 => query.range(*a..=*b),
                    5 => query.limit(*limit),
                    6 => query.range(..),
                    7 => query.forward(),
                    _ => query.reverse(),
                };
                match op {
                    0..=3 => {
                        let bound = if op % 2 == 0 {
                            Bound::Included(*a)
                        } else {
                            Bound::Excluded(*a)
                        };
                        if (op < 2) == (dir == Direction::Forward) {
                            low = bound;
                        } else {
                            high = bound;
                        }
                    }
                    4 => {
                        low = Bound::Included(*a);
                        high = Bound::Included(*b);
                    }
                    5 => expected_limit = Some(*limit),
                    6 => {
                        low = Bound::Unbounded;
                        high = Bound::Unbounded;
                    }
                    7 => dir = Direction::Forward,
                    _ => dir = Direction::Backward,
                }
                let settings = query.into_query();
                let settings: DequeQuery = serde_json::from_slice(&serde_json::to_vec(&settings)?)?;
                assert_eq!(settings.dir, dir);
                assert_eq!(settings.bounds(), (low, high));
                assert_eq!(settings.limit, expected_limit);
                query = ReadQuery::new(DequeQuery::new(), ()).with_query(settings);
            }
        }
        Ok(())
    }
    for query in [DequeQuery::new(), DequeQuery::default()] {
        assert_eq!(query.dir, Direction::Forward);
    }
    QuickCheck::new().quickcheck(
        property as fn(Vec<(u8, usize, usize, NonZeroUsize)>) -> color_eyre::Result<()>,
    );
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
        .limit(NonZeroUsize::MIN);
    assert_eq!(
        serde_json::to_string(&key)?,
        r#"{"dir":"Backward","limit":1,"start":{"PrefixEnd":"a"},"end":{"Bound":{"Included":"a"}}}"#
    );
    let deque = DequeQuery::new().after(2).to(9);
    assert_eq!(
        serde_json::to_string(&deque)?,
        r#"{"dir":"Forward","start":{"Excluded":2},"end":{"Included":9},"limit":null}"#
    );
    Ok(())
}
