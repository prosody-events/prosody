//! Query method order agrees with independent key and position bounds.

use super::{DequeQuery, ErasedKeyQuery};
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
    let mut query = ErasedKeyQuery::new(dir);
    for (op, a, b, limit) in steps {
        query = match op % 8 {
            0 => query.from(a),
            1 => query.after(a),
            2 => query.to(a),
            3 => query.before(a),
            4 => query.prefix(a),
            5 => query.limit(*limit),
            6 => query.range((Bound::Included(a.as_str()), Bound::Excluded(b.as_str()))),
            _ => query.range(..),
        };
    }
    query
}

/// Models prefix bounds with string predicates, without a byte successor.
pub(crate) fn expected_keys<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    dir: Direction,
    steps: &[KeyStep],
) -> Vec<String> {
    let mut low = Bound::Unbounded;
    let mut high = Upper::Bound(Bound::Unbounded);
    let mut limit = usize::MAX;
    for (op, a, b, count) in steps {
        let op = op % 8;
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
            _ => {
                low = Bound::Unbounded;
                high = Upper::Bound(Bound::Unbounded);
            }
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
    fn property(mut keys: Vec<String>, mut steps: Vec<KeyStep>) {
        steps.truncate(128);
        keys.extend(steps.iter().flat_map(|(_, a, b, _)| [a.clone(), b.clone()]));
        for dir in [Direction::Forward, Direction::Backward] {
            // Check every prefix of the trace so later operations cannot hide a defect.
            for end in 0..=steps.len() {
                let steps = &steps[..end];
                let query = key_query(dir, steps);
                let mut coordinates: Vec<_> =
                    keys.iter().map(|key| Utf8KeyCodec::encode(key)).collect();
                coordinates.sort();
                coordinates.dedup();
                let mut actual = query.encoded.select(coordinates);
                actual.truncate(query.encoded.limit.map_or(usize::MAX, NonZeroUsize::get));
                let expected: Vec<_> = expected_keys(keys.iter().map(String::as_str), dir, steps)
                    .iter()
                    .map(|key| Utf8KeyCodec::encode(key))
                    .collect();
                assert_eq!(actual, expected, "direction: {dir:?}; steps: {steps:?}");
            }
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<String>, Vec<KeyStep>));
}

#[test]
fn prop_deque_query_method_order() {
    fn property(mut steps: Vec<(u8, usize, usize, NonZeroUsize)>) {
        steps.truncate(128);
        for dir in [Direction::Forward, Direction::Backward] {
            let mut query = DequeQuery::new(dir);
            let (mut low, mut high) = (Bound::Unbounded, Bound::Unbounded);
            let mut expected_limit = None;
            for (op, a, b, limit) in &steps {
                let op = op % 7;
                query = match op {
                    0 => query.from(*a),
                    1 => query.after(*a),
                    2 => query.to(*a),
                    3 => query.before(*a),
                    4 => query.range(*a..=*b),
                    5 => query.limit(*limit),
                    _ => query.range(..),
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
                    _ => {
                        low = Bound::Unbounded;
                        high = Bound::Unbounded;
                    }
                }
                assert_eq!(query.bounds(), (low, high));
                assert_eq!(query.limit, expected_limit);
            }
        }
    }
    QuickCheck::new().quickcheck(property as fn(Vec<(u8, usize, usize, NonZeroUsize)>));
}
