//! Erased reader queries preserve the shared model.

use super::*;
use crate::codec::JsonCodec;
use crate::state::Direction;
use crate::state::query::tests::{KeyStep, expected_keys, key_query, key_read};
use crate::state::tests::support::drain_cursor;
use crate::state_reader::tests::support::{
    MemoryHarness, mock_count, owner_commit, publish_source, registry_of, source_state_key,
    state_name, topic,
};
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use color_eyre::eyre::bail;
use quickcheck::QuickCheck;
use serde_json::Value;

/// The erased boundary accepts the typed API's maximum batch and rejects
/// only larger batches. This prevents an FFI caller from allocating an
/// uncapped transfer buffer before the shared typed batching begins.
#[test]
fn get_many_limit_matches_typed_keyset_limit() -> Result<()> {
    assert!(validate_get_many_len(MAX_KEYSET_LIMIT - 1).is_ok());
    assert!(validate_get_many_len(MAX_KEYSET_LIMIT).is_ok());
    let Err(error) = validate_get_many_len(MAX_KEYSET_LIMIT + 1) else {
        bail!("one key above the limit must be rejected");
    };
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    assert_eq!(
        error.to_string(),
        format!(
            "get_many accepts at most {MAX_KEYSET_LIMIT} keys; got {}",
            MAX_KEYSET_LIMIT + 1
        )
    );
    Ok(())
}

#[test]
fn prop_erased_reader_queries_match_model() {
    fn property(mut keys: Vec<String>, mut steps: Vec<KeyStep>, tracked: bool) -> Result<()> {
        keys.truncate(32);
        steps.truncate(16);
        keys.extend(steps.iter().flat_map(|(_, a, b, _)| [a.clone(), b.clone()]));
        TEST_RUNTIME.block_on(check_queries(&keys, &steps, tracked))
    }
    QuickCheck::new().quickcheck(property as fn(Vec<String>, Vec<KeyStep>, bool) -> Result<()>);
}

async fn check_queries(keys: &[String], steps: &[KeyStep], tracked: bool) -> Result<()> {
    let harness = MemoryHarness::new();
    let sub = SubsystemName::try_new("query-parity")?;
    let key = Key::from("query-key");
    let topic = topic("query-topic");
    let group = "query-group";
    let count = mock_count();
    let state_key = source_state_key(topic, group, &key, count)?;
    let limit = if tracked { 128 } else { 0 };
    let map = map_state::<Utf8KeyCodec, JsonCodec>("query-map").keyset_limit(limit);
    let set = set_state::<Utf8KeyCodec>("query-set").keyset_limit(limit);
    let deque = deque_state::<JsonCodec>("query-deque");
    owner_commit(
        &harness.cells,
        &registry_of(&map, map.collection_def())?,
        &state_key,
        map,
        1,
        |handle| async move {
            for key in keys {
                handle.set(key, Value::from(key.clone())).await?;
            }
            for dir in [Direction::Forward, Direction::Backward] {
                let expected = expected_keys(keys.iter().map(String::as_str), dir, steps);
                let read = key_read(handle.keys(), dir, steps);
                let actual: Vec<_> = read.stream().try_collect().await?;
                assert_eq!(actual, expected);
            }
            Ok(())
        },
    )
    .await?;
    let registry = registry_of(&set, set.collection_def())?;
    owner_commit(
        &harness.cells,
        &registry,
        &state_key,
        set,
        2,
        |handle| async move {
            for key in keys {
                handle.insert(key).await?;
            }
            Ok(())
        },
    )
    .await?;
    owner_commit(
        &harness.cells,
        &registry_of(&deque, deque.collection_def())?,
        &state_key,
        deque,
        3,
        |handle| async move {
            for key in keys {
                handle.push_back(Value::from(key.clone())).await?;
            }
            Ok(())
        },
    )
    .await?;

    let stores = (&harness.publications, &harness.identities);
    publish_source(
        stores,
        &sub,
        &state_name("query-map")?,
        group,
        topic,
        count,
        &map,
    )
    .await;
    publish_source(
        stores,
        &sub,
        &state_name("query-set")?,
        group,
        topic,
        count,
        &set,
    )
    .await;
    publish_source(
        stores,
        &sub,
        &state_name("query-deque")?,
        group,
        topic,
        count,
        &deque,
    )
    .await;
    let deps = harness.deps();
    let map: SharedMapReader<JsonCodec> =
        Arc::new(MapReader(StateReader::new(&deps, sub.clone(), map)?));
    let set: SharedSetReader = Arc::new(SetReader(StateReader::new(&deps, sub.clone(), set)?));
    let deque: SharedDequeReader<JsonCodec> =
        Arc::new(DequeReader(StateReader::new(&deps, sub, deque)?));
    assert_queries(&map, &set, &deque, &key, keys, steps).await
}

async fn assert_queries(
    map: &SharedMapReader<JsonCodec>,
    set: &SharedSetReader,
    deque: &SharedDequeReader<JsonCodec>,
    key: &Key,
    keys: &[String],
    steps: &[KeyStep],
) -> Result<()> {
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
                drain_cursor(&*key_read(map.entries(key.to_string()), dir, steps).stream()).await?,
                entries
            );
            assert_eq!(
                drain_cursor(&*map.keys(key.to_string()).with_query(query.clone()).stream())
                    .await?,
                expected
            );
            assert_eq!(
                drain_cursor(&*set.keys(key.to_string()).with_query(query).stream()).await?,
                expected
            );
        }
        let mut query = DequeQuery::new().direction(dir).range(1..keys.len());
        let mut expected: Vec<_> = keys.iter().skip(1).cloned().map(Value::from).collect();
        if dir == Direction::Backward {
            expected.reverse();
        }
        if let Some((_, _, _, limit)) = steps.last() {
            query = query.limit(*limit);
            expected.truncate(limit.get());
        }
        assert_eq!(
            drain_cursor(&*deque.values(key.to_string()).with_query(query).stream()).await?,
            expected
        );
    }
    Ok(())
}
