//! Erased reader queries preserve the shared model.

use super::*;
use crate::codec::JsonCodec;
use crate::state::descriptor::{StateDescriptor, deque_state, map_state, set_state};
use crate::state::query::tests::{KeyStep, expected_keys, key_query, key_read};
use crate::state::tests::support::drain_cursor;
use crate::state::{DequeQuery, Direction};
use crate::state_reader::tests::support::{
    MemoryHarness, mock_count, owner_commit, publish_source, registry_of, source_state_key,
    state_name, topic,
};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use futures::TryStreamExt;
use quickcheck::QuickCheck;
use serde_json::Value;
use std::iter::once;
use std::num::NonZeroUsize;

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
    let map: SharedMapReader<Value> = Arc::new(Erased(StateReader::new(&deps, sub.clone(), map)?));
    let set: SharedSetReader = Arc::new(Erased(StateReader::new(&deps, sub.clone(), set)?));
    let deque: SharedDequeReader<Value> = Arc::new(Erased(StateReader::new(&deps, sub, deque)?));
    assert_queries(&map, &set, &deque, &key, keys, steps).await
}

async fn assert_queries(
    map: &SharedMapReader<Value>,
    set: &SharedSetReader,
    deque: &SharedDequeReader<Value>,
    key: &Key,
    keys: &[String],
    steps: &[KeyStep],
) -> Result<()> {
    assert_eq!(map.is_empty(key.to_string()).await?, keys.is_empty());
    assert_eq!(set.is_empty(key.to_string()).await?, keys.is_empty());
    // Batch size is independent of the collection's keyset storage limit.
    let probes: Vec<_> = keys
        .iter()
        .cloned()
        .chain(once("absent\0".to_owned()))
        .cycle()
        .take(4097 + steps.len())
        .collect();
    let present: Vec<_> = probes.iter().map(|probe| keys.contains(probe)).collect();
    let values = probes
        .iter()
        .zip(&present)
        .map(|(probe, &present)| present.then(|| Value::from(probe.clone())))
        .collect::<Vec<_>>();
    assert_eq!(map.get_many(key.to_string(), probes.clone()).await?, values);
    assert_eq!(
        map.contains_many(key.to_string(), probes.clone()).await?,
        present
    );
    assert_eq!(set.contains_many(key.to_string(), probes).await?, present);

    for dir in [Direction::Forward, Direction::Backward] {
        for end in 0..=steps.len() {
            let steps = &steps[..end];
            let query = key_query(dir, steps);
            let expected = expected_keys(keys.iter().map(String::as_str), dir, steps);
            let entries = expected
                .iter()
                .map(|key| (key.clone(), Value::from(key.clone())))
                .collect::<Vec<_>>();
            let read: ErasedKeyRead<(String, Value)> = {
                let reader = Arc::clone(map);
                reader.entries(key.to_string())
            };
            let read = key_read(read, dir, steps);
            let first = drain_cursor(&read.clone().limit(NonZeroUsize::MIN).stream()).await?;
            assert_eq!(first, entries.iter().take(1).cloned().collect::<Vec<_>>());
            assert_eq!(drain_cursor(&read.stream()).await?, entries);
            assert_eq!(
                drain_cursor(&map.keys(key.to_string()).with_query(query.clone()).stream()).await?,
                expected
            );
            assert_eq!(
                drain_cursor(&set.keys(key.to_string()).with_query(query).stream()).await?,
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
            drain_cursor(&deque.values(key.to_string()).with_query(query).stream()).await?,
            expected
        );
    }
    Ok(())
}
