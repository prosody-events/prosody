//! Collection operations export their operation spans.

use super::*;

/// The named attribute's exported value, stringified.
pub(super) fn span_attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

/// Runs value, map, set, and deque operations under the current span.
pub(super) async fn run_collection_ops() -> Result<()> {
    let value = bind_registered(cart(), MemoryLoader::new())?;
    value.set(json!({"qty": 1_i32})).await?;
    value.get().await?;

    let map = bind_registered(
        map_state::<Utf8KeyCodec, JsonCodec>("counts"),
        MemoryLoader::new(),
    )?;
    map.set("k1", json!(1_i32)).await?;
    map.get("k1").await?;
    map.get_many(&["k1", "k2"]).await?;
    map.contains_many(&["k1", "k2"]).await?;
    let _entries: Vec<_> = map
        .entries(KeyQuery::new(Direction::Forward))
        .try_collect()
        .await?;
    let _keys: Vec<_> = map
        .keys(KeyQuery::new(Direction::Forward))
        .try_collect()
        .await?;
    map.is_empty().await?;
    map.remove("k1").await?;

    let set = bind_registered(set_state::<Utf8KeyCodec>("tags"), MemoryLoader::new())?;
    set.insert("k1").await?;
    set.contains("k1").await?;
    set.contains_many(&["k1", "k2"]).await?;
    let _members: Vec<_> = set
        .keys(KeyQuery::new(Direction::Forward))
        .try_collect()
        .await?;
    set.is_empty().await?;
    set.remove("k1").await?;

    let deque = bind_registered(deque_state::<JsonCodec>("dq"), MemoryLoader::new())?;
    deque.push_back(json!(7_i32)).await?;
    let _elements: Vec<_> = deque
        .values(DequeQuery::new(Direction::Forward))
        .try_collect()
        .await?;
    deque.pop_front().await?;
    Ok(())
}

/// Value reads and writes, keyed map and set ops, and deque ops export spans.
/// Each span names its operation and collection under the ambient handler span.
/// Map and set point operations include their key. Each stream includes its
/// direction. Map and set streams include their projection.
#[test]
pub(super) fn collection_ops_export_operation_spans() -> Result<()> {
    let outcome: RefCell<Result<()>> = RefCell::new(Ok(()));
    let spans = captured_spans(|| {
        let handler = tracing::info_span!("handler");
        let _guard = handler.enter();
        // `TEST_RUNTIME`, not `futures::executor`: the simple span processor
        // block_ons its export on span end, which may not nest inside a
        // `LocalPool`. The root future runs on this thread, so the entered
        // `handler` span stays ambient.
        *outcome.borrow_mut() = TEST_RUNTIME.block_on(run_collection_ops());
    });
    outcome.into_inner()?;

    for name in ["map.is_empty", "set.is_empty"] {
        assert_eq!(
            spans.iter().filter(|span| span.name == name).count(),
            1,
            "one call exports one {name} span"
        );
    }
    let mut projections: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "map.stream")
        .map(|span| span_attr(span, "projection"))
        .collect();
    projections.sort_unstable();
    assert_eq!(
        projections,
        [Some("presence".to_owned()), Some("values".to_owned())],
        "map streams export one span per projection under one name"
    );

    assert_eq!(
        spans
            .iter()
            .filter(|span| span.name == "set.stream")
            .map(|span| span_attr(span, "projection"))
            .collect::<Vec<_>>(),
        [Some("presence".to_owned())],
        "set streams export one presence span"
    );

    for name in ["map.get_many", "map.contains_many", "set.contains_many"] {
        assert_eq!(
            span_attr(named(&spans, name)?, "keys").as_deref(),
            Some("2")
        );
    }

    let handler_id = named(&spans, "handler")?.span_context.span_id();

    for (name, collection) in [
        ("value.set", "cart"),
        ("value.get", "cart"),
        ("map.set", "counts"),
        ("map.get", "counts"),
        ("map.stream", "counts"),
        ("map.remove", "counts"),
        ("set.insert", "tags"),
        ("set.contains", "tags"),
        ("set.stream", "tags"),
        ("set.remove", "tags"),
        ("deque.push_back", "dq"),
        ("deque.stream", "dq"),
        ("deque.pop_front", "dq"),
    ] {
        let span = spans
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| eyre!("missing span {name}"))?;
        assert_eq!(
            span_attr(span, "collection").as_deref(),
            Some(collection),
            "{name} must carry its collection name"
        );
        assert_eq!(
            span.parent_span_id, handler_id,
            "{name} must nest under the ambient span"
        );
        let key_attr = match name.split_once('.') {
            Some(("map", op)) if op != "stream" => Some("map.key"),
            Some(("set", op)) if op != "stream" => Some("set.key"),
            _ => None,
        };
        if let Some(attr) = key_attr {
            assert_eq!(
                span_attr(span, attr).as_deref(),
                Some("k1"),
                "{name} must carry its key"
            );
        }
    }
    for name in ["map.stream", "set.stream", "deque.stream"] {
        let stream = spans
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| eyre!("missing span {name}"))?;
        assert_eq!(
            span_attr(stream, "direction").as_deref(),
            Some("Forward"),
            "{name} must carry the scan direction"
        );
    }
    Ok(())
}
