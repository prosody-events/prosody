//! Terminated sessions refuse typed collection operations.

use super::*;

/// Every typed op enforces the termination guard, and the guard holds in each
/// kind. A bound handle over a terminated session refuses the op with the
/// Transient [`StateAccessError::Terminated`]. Each kind's error type carries
/// that refusal.
#[tokio::test]
pub(super) async fn terminated_session_refuses_typed_ops_in_every_kind() -> Result<()> {
    let value = value_state::<JsonCodec>("term_value");
    let map = map_state::<Utf8KeyCodec, JsonCodec>("term_map");
    let set = set_state::<Utf8KeyCodec>("term_set");
    let deque = deque_state::<JsonCodec>("term_deque");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&value, CollectionDef::new(None))?;
    registry.register(&map, CollectionDef::new(None))?;
    registry.register(&set, CollectionDef::new(None))?;
    registry.register(&deque, CollectionDef::new(None))?;
    let session = terminated_session(MemoryLoader::new(), registry);

    let value_handle = value.bind(&session).map_err(|e| eyre!("bind value: {e}"))?;
    let value_result = value_handle.get().await;
    assert!(matches!(
        value_result,
        Err(CellStateError::Access(StateAccessError::Terminated))
    ));
    assert_eq!(
        value_result.err().map(|e| e.classify_error()),
        Some(ErrorCategory::Transient)
    );

    let map_handle = map.bind(&session).map_err(|e| eyre!("bind map: {e}"))?;
    assert!(matches!(
        map_handle.get("k").await,
        Err(MapStateError::Cell(CellStateError::Access(
            StateAccessError::Terminated
        )))
    ));

    let set_handle = set.bind(&session).map_err(|e| eyre!("bind set: {e}"))?;
    assert!(matches!(
        set_handle.contains("k").await,
        Err(MapStateError::Cell(CellStateError::Access(
            StateAccessError::Terminated
        )))
    ));

    let deque_handle = deque.bind(&session).map_err(|e| eyre!("bind deque: {e}"))?;
    assert!(matches!(
        deque_handle.len().await,
        Err(DequeStateError::Cell(CellStateError::Access(
            StateAccessError::Terminated
        )))
    ));
    Ok(())
}

/// Builds a session whose per-event cancellation is already tripped. Every
/// typed op then guards to [`StateAccessError::Terminated`]. Binding still
/// succeeds, because bind validates registration, not liveness.
pub(super) fn terminated_session(
    loader: MemoryLoader<Value>,
    registry: CollectionDefRegistry,
) -> TestSession {
    let (parts, _) = session_parts(
        loader,
        registry,
        StateKey::new(Uuid::new_v4(), Arc::from("user-1")),
        true,
    );
    KeyedStateSession::new(parts)
}

/// Compile-time regression pin for the `-> impl Future + Send` desugar, which
/// guards against rustc #100013. A handle's typed op future holds the
/// resolver's borrowed context across its await, so it must stay `Send`. A
/// plain `async fn` would drop `Send` and fail to compile here. The
/// plan-driver twin is `plan_streams_are_send` in
/// [`crate::state::collection::tests`].
#[test]
pub(super) fn typed_op_future_is_send() -> Result<()> {
    fn assert_send<T: Send>(_value: T) {}

    let handle = bind_registered(
        message_state::<MemoryLoader<Value>>("send_value"),
        MemoryLoader::<Value>::new(),
    )?;
    assert_send(handle.get());
    Ok(())
}
