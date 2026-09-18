//! Borrowed keys preserve owner and reader results across iterator shapes.

use super::support::{
    GROUP_A, MemoryHarness, mock_count, owner_commit, publish_source, source_state_key, state_name,
    subsystem, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::state::Direction;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{
    MapDescriptor, MapHandle, SetDescriptor, SetHandle, map_state, set_state,
};
use crate::state::order_codec::Utf8KeyCodec;
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::store::CELL_BATCH;
use crate::state_reader::StateReader;
use color_eyre::Result;
use futures::{TryStreamExt, executor::block_on};
use quickcheck::QuickCheck;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::iter::from_fn;
use std::sync::Arc;

/// Supplies keys without a length estimate.
fn unknown(keys: &[String]) -> impl Iterator<Item = &str> {
    let mut keys = keys.iter();
    from_fn(move || keys.next().map(String::as_str))
}

#[test]
fn prop_borrowed_utf8_keys_address_maps_and_sets() {
    fn property(mut operations: Vec<(String, bool)>, queries: Vec<String>) -> Result<()> {
        operations.truncate(2 * CELL_BATCH.get() + 1);
        let mut keys: Vec<String> = operations.iter().map(|(key, _)| key.clone()).collect();
        keys.extend(queries.into_iter().take(CELL_BATCH.get() + 1));
        // Repeat positions across storage batches, including absent keys.
        keys.extend_from_within(..);
        block_on(async {
            for limit in [0, 128] {
                check(&operations, &keys, limit).await?;
            }
            Ok(())
        })
    }
    QuickCheck::new().quickcheck(property as fn(Vec<(String, bool)>, Vec<String>) -> Result<()>);
}

async fn check(operations: &[(String, bool)], keys: &[String], limit: usize) -> Result<()> {
    let harness = MemoryHarness::new();
    let map = map_state::<Utf8KeyCodec, JsonCodec>("borrowed-map").keyset_limit(limit);
    let set = set_state::<Utf8KeyCodec>("borrowed-set").keyset_limit(limit);
    let mut registry = CollectionDefRegistry::default();
    registry.register(&map, CollectionDef::new(None))?;
    registry.register(&set, CollectionDef::new(None))?;
    let registry = Arc::new(registry);
    let sub = subsystem()?;
    let topic = topic("orders");
    let key = Key::from("borrowed-keys");
    let count = mock_count();
    let state_key = source_state_key(topic, GROUP_A, &key, count)?;
    let mut model = BTreeMap::new();
    for (position, (key, present)) in operations.iter().enumerate() {
        if *present {
            model.insert(key.clone(), Value::from(position));
        } else {
            model.remove(key);
        }
    }
    owner_commit(&harness.cells, &registry, &state_key, map, 1, |handle| {
        check_map(handle, operations, keys, &model)
    })
    .await?;
    owner_commit(&harness.cells, &registry, &state_key, set, 2, |handle| {
        check_set(handle, operations, keys, &model)
    })
    .await?;

    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &state_name("borrowed-map")?,
        GROUP_A,
        topic,
        count,
        &map,
    )
    .await;
    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &state_name("borrowed-set")?,
        GROUP_A,
        topic,
        count,
        &set,
    )
    .await;
    let deps = harness.deps();
    let map = StateReader::new(&deps, sub.clone(), map)?;
    let set = StateReader::new(&deps, sub, set)?;
    check_readers(map, set, &key, keys, &model).await
}

async fn check_map<S: WritableStateSession>(
    handle: MapHandle<S, Utf8KeyCodec, JsonCodec>,
    operations: &[(String, bool)],
    keys: &[String],
    model: &BTreeMap<String, Value>,
) -> Result<()> {
    let values: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
    let presence: Vec<_> = values.iter().map(Option::is_some).collect();
    let entries: Vec<_> = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let members: Vec<_> = model.keys().cloned().collect();

    for (position, (key, present)) in operations.iter().enumerate() {
        if *present {
            handle.set(key.as_str(), Value::from(position)).await?;
        } else {
            handle.remove(key.as_str()).await?;
        }
        assert_eq!(handle.contains_key(key.as_str()).await?, *present);
    }
    for key in keys {
        assert_eq!(
            handle.get(&Cow::Borrowed(key.as_str())).await?,
            model.get(key).cloned()
        );
    }
    assert_eq!(handle.get_many(keys).await?, values);
    assert_eq!(handle.get_many(unknown(keys)).await?, values);
    let filtered = keys.iter().filter(|key| key.len().is_multiple_of(2));
    let expected: Vec<_> = filtered
        .clone()
        .map(|key| model.get(key).cloned())
        .collect();
    assert_eq!(handle.get_many(filtered).await?, expected);

    assert_eq!(
        handle
            .contains_many(keys.iter().map(String::as_str))
            .await?,
        presence
    );
    assert_eq!(handle.contains_many(unknown(keys)).await?, presence);
    for edge in members.first().into_iter().chain(members.last()) {
        assert_eq!(
            handle
                .query(Direction::Forward)
                .from(edge.as_str())
                .to(edge.as_str())
                .entries()
                .try_collect::<Vec<_>>()
                .await?,
            vec![(edge.clone(), model[edge].clone())]
        );
        assert!(
            handle
                .query(Direction::Backward)
                .after(edge.as_str())
                .before(edge.as_str())
                .keys()
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
    }
    assert_eq!(
        handle
            .stream(Direction::Forward)
            .try_collect::<Vec<_>>()
            .await?,
        entries
    );
    Ok(())
}

async fn check_set<S: WritableStateSession>(
    handle: SetHandle<S, Utf8KeyCodec>,
    operations: &[(String, bool)],
    keys: &[String],
    model: &BTreeMap<String, Value>,
) -> Result<()> {
    let presence: Vec<_> = keys.iter().map(|key| model.contains_key(key)).collect();
    let members: Vec<_> = model.keys().cloned().collect();
    for (key, present) in operations {
        if *present {
            handle.insert(key.as_str()).await?;
        } else {
            handle.remove(key.as_str()).await?;
        }
        assert_eq!(handle.contains(key.as_str()).await?, *present);
    }
    assert_eq!(handle.contains_many(keys).await?, presence);
    assert_eq!(handle.contains_many(unknown(keys)).await?, presence);
    assert_eq!(
        handle
            .keys(Direction::Forward)
            .try_collect::<Vec<_>>()
            .await?,
        members
    );
    Ok(())
}

async fn check_readers(
    map: StateReader<MapDescriptor<Utf8KeyCodec>, JsonCodec>,
    set: StateReader<SetDescriptor<Utf8KeyCodec>, JsonCodec>,
    key: &Key,
    keys: &[String],
    model: &BTreeMap<String, Value>,
) -> Result<()> {
    let values: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
    let presence: Vec<_> = values.iter().map(Option::is_some).collect();
    let members: Vec<_> = model.keys().cloned().collect();
    for member in keys {
        assert_eq!(
            map.get(key.clone(), member.as_str()).await?,
            model.get(member).cloned()
        );
        assert_eq!(
            map.contains_key(key.clone(), member.as_str()).await?,
            model.contains_key(member)
        );
        assert_eq!(
            set.contains(key.clone(), member.as_str()).await?,
            model.contains_key(member)
        );
    }
    assert_eq!(map.get_many(key.clone(), keys).await?, values);
    assert_eq!(map.get_many(key.clone(), unknown(keys)).await?, values);
    assert_eq!(
        map.contains_many(key.clone(), unknown(keys)).await?,
        presence
    );
    assert_eq!(
        set.contains_many(key.clone(), keys.iter().map(String::as_str))
            .await?,
        presence
    );
    assert_eq!(
        set.contains_many(key.clone(), unknown(keys)).await?,
        presence
    );
    for edge in members.first().into_iter().chain(members.last()) {
        assert_eq!(
            map.query(key.clone(), Direction::Forward)
                .from(edge.as_str())
                .to(edge.as_str())
                .entries()
                .await?
                .try_collect::<Vec<_>>()
                .await?,
            vec![(edge.clone(), model[edge].clone())]
        );
        assert_eq!(
            set.query(key.clone(), Direction::Backward)
                .from(edge.as_str())
                .to(edge.as_str())
                .keys()
                .await?
                .try_collect::<Vec<_>>()
                .await?,
            vec![edge.clone()]
        );
        assert!(
            map.query(key.clone(), Direction::Backward)
                .after(edge.as_str())
                .before(edge.as_str())
                .keys()
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
        assert!(
            set.query(key.clone(), Direction::Forward)
                .after(edge.as_str())
                .before(edge.as_str())
                .keys()
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
    }
    Ok(())
}
