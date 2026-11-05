//! Erased handler queries preserve bounds, method order, and paging.

use super::*;
use crate::state::Direction;
use crate::state::query::tests::{KeyStep, expected_keys, key_query, key_read};

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
                    assert_eq!(
                        drain_cursor(&key_read(map.entries(), dir, steps).stream()).await?,
                        entries
                    );
                    assert_eq!(
                        drain_cursor(&map.keys().with_query(query.clone()).stream()).await?,
                        expected
                    );
                    assert_eq!(
                        drain_cursor(&set.keys().with_query(query).stream()).await?,
                        expected
                    );
                }
                let query = match dir {
                    Direction::Forward => DequeQuery::new().after(0).before(keys.len()),
                    Direction::Backward => DequeQuery::new().reverse().after(keys.len()).before(0),
                };
                let mut expected: Vec<_> = keys.iter().skip(1).cloned().map(Value::from).collect();
                if dir == Direction::Backward {
                    expected.reverse();
                }
                assert_eq!(
                    drain_cursor(&deque.values().with_query(query).stream()).await?,
                    expected
                );

                // Successive pages must return every distinct key exactly once.
                let expected = expected_keys(keys.iter().map(String::as_str), dir, &[]);
                let mut actual = Vec::new();
                let mut query = ErasedKeyQuery::new()
                    .direction(dir)
                    .limit(NonZeroUsize::MIN);
                while let Some(key) = map.keys().with_query(query).stream().next().await? {
                    query = ErasedKeyQuery::new()
                        .direction(dir)
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
