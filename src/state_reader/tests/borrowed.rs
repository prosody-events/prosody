//! Borrowed keys preserve owner and reader results across iterator shapes.

use super::support::{
    GROUP_A, MemoryHarness, collect_stream, each, mock_count, owner_commit, publish_source,
    source_state_key, state_name, subsystem, topic,
};
use crate::Key;
use crate::codec::{Codec, JsonCodec, SerializeBufGuard};
use crate::state::cell_key::Coordinate;
use crate::state::collection::WritableStateSession;
use crate::state::descriptor::{
    MapDescriptor, MapHandle, SetDescriptor, SetHandle, StateDescriptor, map_state, set_state,
};
use crate::state::order_codec::{KeyCodecError, OrderedKeyCodec, Utf8KeyCodec};
use crate::state::registry::CollectionDefRegistry;
use crate::state::store::CELL_BATCH;
use crate::state_reader::StateReader;
use color_eyre::Result;
use futures::{TryStreamExt, executor::block_on, try_join};
use quickcheck::QuickCheck;
use serde_json::Value;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::from_fn;
use std::sync::Arc;

const MAX_KEYS: usize = 2 * CELL_BATCH.get() + 1;
const BATCH_LENGTHS: [usize; 5] = [
    0,
    CELL_BATCH.get() - 1,
    CELL_BATCH.get(),
    CELL_BATCH.get() + 1,
    MAX_KEYS,
];

thread_local! {
    static OWNED_ENCODINGS: Cell<usize> = const { Cell::new(0) };
}

/// Counts owned key encodings without changing the UTF-8 wire format.
#[derive(Default)]
struct CountedUtf8;

impl Codec for CountedUtf8 {
    type Error = KeyCodecError;
    type Payload = String;

    const FORMAT_ID: &'static str = Utf8KeyCodec::FORMAT_ID;

    fn deserialize(&mut self, buf: &mut [u8]) -> Result<String, KeyCodecError> {
        Utf8KeyCodec.deserialize(buf)
    }

    fn serialize_ref(&mut self, key: &String, buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        Utf8KeyCodec.serialize_ref(key, buf)
    }
}

impl OrderedKeyCodec for CountedUtf8 {
    type Borrowed = str;
    type Key = String;

    fn encode(key: &str) -> Coordinate {
        OWNED_ENCODINGS.set(OWNED_ENCODINGS.get() + 1);
        Utf8KeyCodec::encode(key)
    }

    fn serialize_key(&mut self, key: &str, buf: &mut Vec<u8>) -> Result<(), KeyCodecError> {
        Utf8KeyCodec.serialize_key(key, buf)
    }

    fn decode(bytes: &[u8]) -> Result<String, KeyCodecError> {
        Utf8KeyCodec::decode(bytes)
    }
}

/// Supplies keys without a length estimate.
fn unknown(keys: &[String]) -> impl Iterator<Item = &str> + use<'_> {
    let mut keys = keys.iter();
    from_fn(move || keys.next().map(String::as_str))
}

#[test]
fn prop_borrowed_utf8_keys_address_maps_and_sets() -> Result<()> {
    fn property(mut keys: Vec<String>, steps: Vec<(u8, bool)>) -> Result<()> {
        keys.truncate(8);
        keys.push(String::new());
        let operations: Vec<_> = steps
            .into_iter()
            .take(MAX_KEYS)
            .map(|(index, present)| (keys[usize::from(index) % keys.len()].clone(), present))
            .collect();
        let mut absent = keys.iter().max().cloned().unwrap_or_default();
        absent.push('\0');
        keys.push(absent);
        // Reuse keys to test overwrites, removals, and duplicate batch positions.
        let keys: Vec<_> = keys.iter().cycle().take(MAX_KEYS).cloned().collect();
        block_on(async {
            for limit in [0, 128] {
                Box::pin(check(&operations, &keys, limit)).await?;
            }
            Ok(())
        })
    }
    // A filtered batch can need more key bytes than any consecutive batch.
    let lengths = [2368, 1612, 2650, 697, 690, 2057, 929, 50];
    let keys = (b'a'..)
        .zip(lengths)
        .map(|(letter, len)| char::from(letter).to_string().repeat(len));
    property(keys.collect(), Vec::new())?;
    QuickCheck::new().quickcheck(property as fn(Vec<String>, Vec<(u8, bool)>) -> Result<()>);
    Ok(())
}

async fn check(operations: &[(String, bool)], keys: &[String], limit: usize) -> Result<()> {
    let harness = MemoryHarness::new();
    let map = map_state::<CountedUtf8, JsonCodec>("borrowed-map").keyset_limit(limit);
    let set = set_state::<CountedUtf8>("borrowed-set").keyset_limit(limit);
    let mut registry = CollectionDefRegistry::default();
    registry.register(&map, map.collection_def())?;
    registry.register(&set, set.collection_def())?;
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
    Box::pin(check_readers(map, set, &key, keys, &model)).await
}

async fn check_map<S: WritableStateSession>(
    handle: MapHandle<S, CountedUtf8, JsonCodec>,
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

    // The owner session gate admits one operation at a time, so owner checks
    // run in sequence.
    for (position, (key, present)) in operations.iter().enumerate() {
        if *present {
            handle.set(key.as_str(), Value::from(position)).await?;
        } else {
            handle.remove(key.as_str()).await?;
        }
        assert_eq!(handle.contains_key(key.as_str()).await?, *present);
        assert_eq!(
            handle.get(key.as_str()).await?,
            present.then(|| Value::from(position))
        );
    }
    let encodings = OWNED_ENCODINGS.get();
    assert_eq!(handle.contains_many(keys).await?, presence);
    // Warm the pool with both batch groupings before the reuse check.
    let filtered = keys.iter().filter(|key| key.len().is_multiple_of(2));
    let expected: Vec<_> = filtered
        .clone()
        .map(|key| model.get(key).cloned())
        .collect();
    assert_eq!(handle.get_many(filtered).await?, expected);
    let storage = SerializeBufGuard::allocation();
    assert!(storage.1 > 0, "the batch must warm the encoding pool");
    for key in keys.iter().collect::<BTreeSet<_>>() {
        let expected = model.get(key).cloned();
        assert_eq!(handle.contains_key(key.as_str()).await?, expected.is_some());
        assert_eq!(handle.get(&Cow::Borrowed(key.as_str())).await?, expected);
    }
    for len in BATCH_LENGTHS {
        assert_eq!(handle.get_many(&keys[..len]).await?, values[..len]);
        assert_eq!(handle.get_many(unknown(&keys[..len])).await?, values[..len]);
    }
    let split = keys.len() / 2;
    assert_eq!(
        handle
            .get_many(
                keys[..split]
                    .iter()
                    .map(String::as_str)
                    .chain(unknown(&keys[split..]))
            )
            .await?,
        values
    );

    assert_eq!(
        handle
            .contains_many(keys.iter().map(String::as_str))
            .await?,
        presence
    );
    assert_eq!(handle.contains_many(unknown(keys)).await?, presence);
    assert_eq!(
        OWNED_ENCODINGS.get(),
        encodings,
        "reads must not encode owned keys"
    );
    assert_eq!(
        SerializeBufGuard::allocation(),
        storage,
        "sequential reads must reuse encoding storage"
    );
    for edge in members.first().into_iter().chain(members.last()) {
        assert_eq!(
            handle
                .entries()
                .from(edge.as_str())
                .to(edge.as_str())
                .stream()
                .try_collect::<Vec<_>>()
                .await?,
            vec![(edge.clone(), model[edge].clone())]
        );
        assert!(
            handle
                .keys()
                .reverse()
                .after(edge.as_str())
                .before(edge.as_str())
                .stream()
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
    }
    assert_eq!(
        handle.entries().stream().try_collect::<Vec<_>>().await?,
        entries
    );
    Ok(())
}

async fn check_set<S: WritableStateSession>(
    handle: SetHandle<S, CountedUtf8>,
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
    let encodings = OWNED_ENCODINGS.get();
    for len in BATCH_LENGTHS {
        assert_eq!(handle.contains_many(&keys[..len]).await?, presence[..len]);
        assert_eq!(
            handle.contains_many(unknown(&keys[..len])).await?,
            presence[..len]
        );
    }
    assert_eq!(
        OWNED_ENCODINGS.get(),
        encodings,
        "reads must not encode owned keys"
    );
    assert_eq!(
        handle.keys().stream().try_collect::<Vec<_>>().await?,
        members
    );
    Ok(())
}

async fn check_readers(
    map: StateReader<MapDescriptor<CountedUtf8>, JsonCodec>,
    set: StateReader<SetDescriptor<CountedUtf8>, JsonCodec>,
    key: &Key,
    keys: &[String],
    model: &BTreeMap<String, Value>,
) -> Result<()> {
    let values: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
    let presence: Vec<_> = values.iter().map(Option::is_some).collect();
    let members: Vec<_> = model.keys().cloned().collect();
    let (map, set, values, presence) = (&map, &set, values.as_slice(), presence.as_slice());

    let encodings = OWNED_ENCODINGS.get();
    each(keys.iter().collect::<BTreeSet<_>>(), |member| async move {
        let (value, present, contained) = try_join!(
            map.get(key.clone(), member.as_str()),
            map.contains_key(key.clone(), member.as_str()),
            set.contains(key.clone(), member.as_str()),
        )?;
        assert_eq!(value, model.get(member).cloned());
        assert_eq!(present, model.contains_key(member));
        assert_eq!(contained, model.contains_key(member));
        Ok(())
    })
    .await?;
    each(BATCH_LENGTHS, |len| async move {
        let (exact_values, lazy_values, exact_presence, lazy_presence) = try_join!(
            map.get_many(key.clone(), &keys[..len]),
            map.get_many(key.clone(), unknown(&keys[..len])),
            set.contains_many(key.clone(), &keys[..len]),
            set.contains_many(key.clone(), unknown(&keys[..len])),
        )?;
        assert_eq!(exact_values, values[..len]);
        assert_eq!(lazy_values, values[..len]);
        assert_eq!(exact_presence, presence[..len]);
        assert_eq!(lazy_presence, presence[..len]);
        Ok(())
    })
    .await?;
    let (lazy, mapped) = try_join!(
        map.contains_many(key.clone(), unknown(keys)),
        set.contains_many(key.clone(), keys.iter().map(String::as_str)),
    )?;
    assert_eq!(lazy, presence);
    assert_eq!(mapped, presence);

    assert_eq!(
        OWNED_ENCODINGS.get(),
        encodings,
        "reads must not encode owned keys"
    );
    each(
        [members.first(), members.last()].into_iter().flatten(),
        |edge| async move {
            let (entries, members, map_excluded, set_excluded) = try_join!(
                collect_stream(
                    map.entries(key.clone())
                        .from(edge.as_str())
                        .to(edge.as_str())
                        .stream()
                ),
                collect_stream(
                    set.keys(key.clone())
                        .reverse()
                        .from(edge.as_str())
                        .to(edge.as_str())
                        .stream()
                ),
                collect_stream(
                    map.keys(key.clone())
                        .reverse()
                        .after(edge.as_str())
                        .before(edge.as_str())
                        .stream()
                ),
                collect_stream(
                    set.keys(key.clone())
                        .after(edge.as_str())
                        .before(edge.as_str())
                        .stream()
                ),
            )?;
            assert_eq!(entries, vec![(edge.clone(), model[edge].clone())]);
            assert_eq!(members, vec![edge.clone()]);
            assert!(map_excluded.is_empty());
            assert!(set_excluded.is_empty());
            Ok(())
        },
    )
    .await
}
