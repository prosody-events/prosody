use super::*;

/// The topic every seeded message shares; the id doubles as the offset, so
/// a message is fully identified by its id.
const TOPIC: &str = "orders.v1";

/// The bounded key pool the map trace ranges over.
const MAP_KEYS: &[&str] = &["a", "b", "c", "d"];

/// Seeds the loader with the message identified by `id` (offset `id`,
/// payload `id`) and returns the [`ConsumerMessage`] a handle writes.
fn seed_message(loader: &MemoryLoader<Value>, id: i64) -> Result<ConsumerMessage<Value>> {
    let topic = Topic::from(TOPIC);
    let key: Key = Arc::from("k");
    let payload = json!(id);
    loader.store_message(topic, 0, id, key.clone(), payload.clone());
    ConsumerMessage::for_testing(topic, 0, id, key, payload)
}

/// Whether a resolved cell matches the model's message id: a `Some` cell
/// must resolve to the full message at that offset with the matching
/// payload, and absence must agree.
fn matches_model(resolved: Option<&ConsumerMessage<Value>>, expected: Option<i64>) -> bool {
    match (resolved, expected) {
        (None, None) => true,
        (Some(message), Some(id)) => {
            message.offset() == id && message.record().message() == Some(&json!(id))
        }
        _ => false,
    }
}

/// The map key for a generated index.
fn map_key(index: u8) -> String {
    MAP_KEYS[index as usize % MAP_KEYS.len()].to_owned()
}

/// Collects a message-map handle's `stream(dir)` into `(key, offset)`
/// pairs.
async fn collect_map(
    handle: &MapHandle<TestSession, Utf8KeyCodec, MessageCell<MemoryLoader<Value>>>,
    dir: Direction,
) -> Result<Vec<(String, i64)>> {
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let (key, message) = item?;
        out.push((key, message.offset()));
    }
    Ok(out)
}

/// Collects a message-deque handle's `stream(dir)` into element offsets.
async fn collect_deque(
    handle: &DequeHandle<TestSession, MessageCell<MemoryLoader<Value>>>,
    dir: Direction,
) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    let stream = handle.stream(dir);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        out.push(item?.offset());
    }
    Ok(out)
}

/// One map mutation or mid-trace read over the message-map handle.
#[derive(Clone, Copy, Debug)]
enum MsgMapOp {
    Set(u8, u8),
    Remove(u8),
    Get(u8),
}

impl Arbitrary for MsgMapOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 | 1 => Self::Set(u8::arbitrary(g), u8::arbitrary(g)),
            2 => Self::Remove(u8::arbitrary(g)),
            _ => Self::Get(u8::arbitrary(g)),
        }
    }
}

/// One deque mutation over the message-deque handle.
#[derive(Clone, Copy, Debug)]
enum MsgDequeOp {
    PushBack(u8),
    PushFront(u8),
    PopBack,
    PopFront,
}

impl Arbitrary for MsgDequeOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::PushBack(u8::arbitrary(g)),
            1 => Self::PushFront(u8::arbitrary(g)),
            2 => Self::PopBack,
            _ => Self::PopFront,
        }
    }
}

/// Drives a message-map trace against a `BTreeMap<String, i64>` model of
/// the stored ids, resolving each cell through the shared loader;
/// asserts every mid-trace `get`, the final per-key read-back, and both
/// stream directions.
async fn run_msg_map(ops: Vec<MsgMapOp>) -> Result<bool> {
    let loader = MemoryLoader::<Value>::new();
    let handle = bind_registered(
        message_map_state::<Utf8KeyCodec, MemoryLoader<Value>>("msg_map"),
        loader.clone(),
    )?;
    let mut model: BTreeMap<String, i64> = BTreeMap::new();

    for op in ops {
        match op {
            MsgMapOp::Set(key_index, id) => {
                let key = map_key(key_index);
                let id = i64::from(id);
                let message = seed_message(&loader, id)?;
                handle.set(key.clone(), &message).await?;
                model.insert(key, id);
            }
            MsgMapOp::Remove(key_index) => {
                let key = map_key(key_index);
                handle.remove(&key).await?;
                model.remove(&key);
            }
            MsgMapOp::Get(key_index) => {
                let key = map_key(key_index);
                if !matches_model(handle.get(&key).await?.as_ref(), model.get(&key).copied()) {
                    return Ok(false);
                }
            }
        }
    }

    for name in MAP_KEYS {
        let resolved = handle.get(&(*name).to_owned()).await?;
        if !matches_model(resolved.as_ref(), model.get(*name).copied()) {
            return Ok(false);
        }
    }

    let ascending: Vec<(String, i64)> = model.iter().map(|(k, v)| (k.clone(), *v)).collect();
    if collect_map(&handle, Direction::Forward).await? != ascending {
        return Ok(false);
    }
    let descending: Vec<(String, i64)> = model.iter().rev().map(|(k, v)| (k.clone(), *v)).collect();
    Ok(collect_map(&handle, Direction::Backward).await? == descending)
}

/// Drives a message-deque trace against a `VecDeque<i64>` model of the
/// stored ids; asserts every `pop` return, `len`, positional `get`, and
/// both stream directions.
async fn run_msg_deque(ops: Vec<MsgDequeOp>) -> Result<bool> {
    let loader = MemoryLoader::<Value>::new();
    let handle = bind_registered(
        message_deque_state::<MemoryLoader<Value>>("msg_deque"),
        loader.clone(),
    )?;
    let mut model: VecDeque<i64> = VecDeque::new();

    for op in ops {
        match op {
            MsgDequeOp::PushBack(id) => {
                let id = i64::from(id);
                let message = seed_message(&loader, id)?;
                handle.push_back(&message).await?;
                model.push_back(id);
            }
            MsgDequeOp::PushFront(id) => {
                let id = i64::from(id);
                let message = seed_message(&loader, id)?;
                handle.push_front(&message).await?;
                model.push_front(id);
            }
            MsgDequeOp::PopBack => {
                if !matches_model(handle.pop_back().await?.as_ref(), model.pop_back()) {
                    return Ok(false);
                }
            }
            MsgDequeOp::PopFront => {
                if !matches_model(handle.pop_front().await?.as_ref(), model.pop_front()) {
                    return Ok(false);
                }
            }
        }
    }

    if handle.len().await? != model.len() {
        return Ok(false);
    }
    for index in 0..model.len() + 2 {
        if !matches_model(handle.get(index).await?.as_ref(), model.get(index).copied()) {
            return Ok(false);
        }
    }
    let forward: Vec<i64> = model.iter().copied().collect();
    if collect_deque(&handle, Direction::Forward).await? != forward {
        return Ok(false);
    }
    let backward: Vec<i64> = model.iter().rev().copied().collect();
    Ok(collect_deque(&handle, Direction::Backward).await? == backward)
}

/// Converts a property body's `Result<bool>` into a `TestResult`, surfacing
/// the offending trace on failure.
fn finish(result: Result<bool>, label: &str, input: &str) -> TestResult {
    match result {
        Ok(true) => TestResult::passed(),
        Ok(false) => TestResult::error(format!("{label}: {input}")),
        Err(error) => TestResult::error(format!("{label}: {input}: {error:#}")),
    }
}

/// A message cell in a Map behaves exactly like a `BTreeMap` keyed by the
/// user key, valued by the resolved Kafka message.
#[test]
fn prop_message_cell_in_map_tracks_btreemap() {
    fn prop(ops: Vec<MsgMapOp>) -> TestResult {
        let input = format!("{ops:?}");
        finish(block_on(run_msg_map(ops)), "message-map divergence", &input)
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<MsgMapOp>) -> TestResult);
}

/// A message cell in a Deque behaves exactly like a `VecDeque` of resolved
/// Kafka messages.
#[test]
fn prop_message_cell_in_deque_tracks_vecdeque() {
    fn prop(ops: Vec<MsgDequeOp>) -> TestResult {
        let input = format!("{ops:?}");
        finish(
            block_on(run_msg_deque(ops)),
            "message-deque divergence",
            &input,
        )
    }
    QuickCheck::new().quickcheck(prop as fn(Vec<MsgDequeOp>) -> TestResult);
}

/// A pop whose message resolution fails (deleted/compacted offset) errors
/// *before* any mutation is buffered — the resolve-before-mutate guarantee
/// documented on the pop ops. The failed element stays at its end at the
/// same length, and pops normally once the message is restored.
#[tokio::test]
async fn deque_pop_resolve_failure_leaves_deque_unmutated() -> Result<()> {
    let loader = MemoryLoader::<Value>::new();
    let handle = bind_registered(
        message_deque_state::<MemoryLoader<Value>>("msg_deque"),
        loader.clone(),
    )?;

    let keep = seed_message(&loader, 1)?;
    let vanish = seed_message(&loader, 2)?;
    handle.push_back(&keep).await?;
    handle.push_back(&vanish).await?;
    loader.remove_message(Topic::from(TOPIC), 0, 2);

    let Err(error) = handle.pop_back().await else {
        bail!("popping a vanished message must error");
    };
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);

    // Unmutated: same length, the vanished element still at the back.
    assert_eq!(handle.len().await?, 2);
    loader.store_message(Topic::from(TOPIC), 0, 2, Arc::from("k"), json!(2_i64));
    if !matches_model(handle.pop_back().await?.as_ref(), Some(2)) {
        bail!("restored element must pop with its original id");
    }
    assert_eq!(handle.len().await?, 1);
    Ok(())
}

/// The message cell carries the same durable identity — codec
/// `"message-ref"`, resolver `Some("message-ref")` — in every kind;
/// only the key axis varies with the kind. This is what lets
/// cross-language consumers share a message collection regardless of
/// the kind it is stored in.
#[test]
fn message_cell_carries_message_identity_in_every_kind() {
    let map: MapDescriptor<Utf8KeyCodec, MessageCell<MemoryLoader<Value>>> = message_map_state("m");
    let map_id = map.structural_identity();
    assert_eq!(map_id.format_id, "message-ref");
    assert_eq!(map_id.resolver_id, Some("message-ref"));
    assert_eq!(map_id.key_format_id, "utf8.v1");

    let deque: DequeDescriptor<MessageCell<MemoryLoader<Value>>> = message_deque_state("d");
    let deque_id = deque.structural_identity();
    assert_eq!(deque_id.format_id, "message-ref");
    assert_eq!(deque_id.resolver_id, Some("message-ref"));
    assert_eq!(deque_id.key_format_id, "i64.v1");

    let value: MessageDescriptor<MemoryLoader<Value>> = message_state("v");
    let value_id = value.structural_identity();
    assert_eq!(value_id.format_id, "message-ref");
    assert_eq!(value_id.resolver_id, Some("message-ref"));
    assert_eq!(value_id.key_format_id, "unit.v1");
}
