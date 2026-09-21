//! Erased handler queries preserve bounds, method order, and paging.

use super::*;
use crate::state::Direction;
use crate::state::query::tests::{KeyStep, expected_keys, key_query};

#[test]
fn prop_erased_handler_queries_match_model() {
    fn property(mut keys: Vec<String>, mut steps: Vec<KeyStep>) -> Result<()> {
        keys.truncate(32);
        steps.truncate(16);
        keys.extend(steps.iter().flat_map(|(_, a, b, _)| [a.clone(), b.clone()]));
        TEST_RUNTIME.block_on(async {
            let context = parity_context::<Value>()?;
            let map = context.map_state(MAP_NAME)?;
            let set = context.set_state(SET_NAME)?;
            let deque = context.deque_state(DEQUE_NAME)?;
            for key in &keys {
                map.set(key.clone(), Value::from(key.clone())).await?;
                set.insert(key.clone()).await?;
                deque.push_back(Value::from(key.clone())).await?;
            }
            for dir in [Direction::Forward, Direction::Backward] {
                for end in 0..=steps.len() {
                    let steps = &steps[..end];
                    let query = key_query(dir, steps);
                    let expected = expected_keys(keys.iter().map(String::as_str), dir, steps);
                    let entries = expected
                        .iter()
                        .map(|key| (key.clone(), Value::from(key.clone())))
                        .collect::<Vec<_>>();
                    assert_eq!(drain_cursor(&map.entries(query.clone())).await?, entries);
                    assert_eq!(drain_cursor(&map.keys(query.clone())).await?, expected);
                    assert_eq!(drain_cursor(&set.keys(query)).await?, expected);
                }
                let query = match dir {
                    Direction::Forward => DequeQuery::new(dir).after(0).before(keys.len()),
                    Direction::Backward => DequeQuery::new(dir).after(keys.len()).before(0),
                };
                let mut expected: Vec<_> = keys.iter().skip(1).cloned().map(Value::from).collect();
                if dir == Direction::Backward {
                    expected.reverse();
                }
                assert_eq!(drain_cursor(&deque.values(query)).await?, expected);

                // Successive pages must return every distinct key exactly once.
                let expected = expected_keys(keys.iter().map(String::as_str), dir, &[]);
                let mut actual = Vec::new();
                let mut query = ErasedKeyQuery::new(dir).limit(NonZeroUsize::MIN);
                while let Some(key) = map.keys(query).next().await? {
                    query = ErasedKeyQuery::new(dir)
                        .after(&key)
                        .limit(NonZeroUsize::MIN);
                    actual.push(key);
                    assert!(actual.len() <= expected.len(), "paging must advance");
                }
                assert_eq!(actual, expected);
            }
            Ok(())
        })
    }
    QuickCheck::new().quickcheck(property as fn(Vec<String>, Vec<KeyStep>) -> Result<()>);
}
