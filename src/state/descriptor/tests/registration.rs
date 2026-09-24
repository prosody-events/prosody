//! Descriptor registration validates collection identity and settings.

use super::*;

/// Wire-format freeze for the key-codec tokens, pinned end to end through
/// `structural_identity()`: the token is a durable identity column compared
/// on every acquisition, so changing a key codec's `FORMAT_ID` literal — or
/// the derivation that lifts it off the cell type — silently bricks existing
/// collections. Deque and Value carry kind-pinned key axes (`I64KeyCodec`,
/// [`UnitKey`](crate::state::order_codec::UnitKey)); their tokens must stay
/// frozen just like the user-chosen ones.
#[test]
pub(super) fn key_codec_wire_contract_is_frozen() {
    use crate::state::order_codec::{I64KeyCodec, U64KeyCodec};

    let utf8: MapDescriptor<Utf8KeyCodec> = map_state("m");
    assert_eq!(utf8.structural_identity().key_format_id, "utf8.v1");
    let i64_keyed: MapDescriptor<I64KeyCodec> = map_state("m");
    assert_eq!(i64_keyed.structural_identity().key_format_id, "i64.v1");
    let u64_keyed: MapDescriptor<U64KeyCodec> = map_state("m");
    assert_eq!(u64_keyed.structural_identity().key_format_id, "u64.v1");

    let deque: DequeDescriptor = deque_state("d");
    assert_eq!(deque.structural_identity().key_format_id, "i64.v1");
    let value: ValueDescriptor = value_state("v");
    assert_eq!(value.structural_identity().key_format_id, "unit.v1");
}

/// Binding against a context without keyed state (any context whose
/// session is the
/// [`UnavailableState`](crate::state::tests::support::UnavailableState)
/// stub — here the bare mock) fails with the Permanent
/// [`StateAccessError::Unavailable`].
#[test]
pub(super) fn state_unavailable_without_keyed_state() -> Result<()> {
    let ctx: MockEventContext = MockEventContext::new();
    let Err(error) = ctx.state(Registered::new(cart())) else {
        return Err(eyre!("bind on a state-less context must fail"));
    };
    assert!(matches!(error, StateAccessError::Unavailable));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Binding an unregistered descriptor fails with a Permanent
/// [`StateAccessError::Unregistered`] — access requires prior
/// registration.
#[tokio::test]
pub(super) async fn state_with_unregistered_descriptor_errors() -> Result<()> {
    let session = test_session(MemoryLoader::new(), CollectionDefRegistry::default());
    let Err(error) = cart().bind(&session) else {
        return Err(eyre!("unregistered bind must fail"));
    };
    assert!(matches!(
        error,
        StateAccessError::Unregistered { name: "cart" }
    ));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Binding a descriptor whose identity differs from the registered one
/// fails with [`StateAccessError::IdentityMismatch`].
#[tokio::test]
pub(super) async fn bind_with_mismatched_identity_errors() -> Result<()> {
    let recoded: ValueDescriptor<CartCodec> = value_state("cart");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;
    let session = test_session(MemoryLoader::new(), registry);

    let Err(error) = recoded.bind(&session) else {
        return Err(eyre!("mismatched bind must fail"));
    };
    assert!(matches!(error, StateAccessError::IdentityMismatch { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Re-registering the same name with a *different* structural identity
/// is rejected — both for a differing `format_id` (a Kafka descriptor over a
/// name registered as a JSON value) and for a differing collection `kind` (a
/// Map, then a Deque, over a name registered as a Value).
#[test]
pub(super) fn conflicting_registration_is_rejected() -> Result<()> {
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;

    // Same kind, different codec id.
    assert!(matches!(
        registry.register(
            &message_state::<MemoryLoader<Value>>("cart"),
            CollectionDef::new(None)
        ),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    // Different kind (Map / Deque) under the Value's name.
    let map: MapDescriptor<Utf8KeyCodec> = map_state("cart");
    assert!(matches!(
        registry.register(&map, CollectionDef::new(None)),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    let deque: DequeDescriptor = deque_state("cart");
    assert!(matches!(
        registry.register(&deque, CollectionDef::new(None)),
        Err(RegisterStateError::IdentityConflict { .. })
    ));
    Ok(())
}

/// Binding a descriptor of one kind where a different kind was registered
/// under the same name fails with a Permanent
/// [`StateAccessError::IdentityMismatch`] — the kind is part of the frozen
/// structural identity.
#[tokio::test]
pub(super) async fn bind_with_mismatched_kind_errors() -> Result<()> {
    let map: MapDescriptor<Utf8KeyCodec> = map_state("cart");
    let mut registry = CollectionDefRegistry::default();
    registry.register(&map, CollectionDef::new(None))?;
    let session = test_session(MemoryLoader::new(), registry);

    // A Deque descriptor asserting a different `kind` for the same name.
    let deque: DequeDescriptor = deque_state("cart");
    let Err(error) = deque.bind(&session) else {
        return Err(eyre!("mismatched-kind bind must fail"));
    };
    assert!(matches!(error, StateAccessError::IdentityMismatch { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Re-registering the same name with an *unchanged* identity is rejected as a
/// duplicate declaration — one declaration per name per registry, never
/// last-wins. The first registration's operational settings stand; the second
/// errors [`RegisterStateError::Duplicate`] (`Permanent`) rather than silently
/// overwriting them.
#[test]
pub(super) fn reregistration_is_rejected_as_duplicate() -> Result<()> {
    let name = StateName::try_new("cart")?;
    let initial_ttl = CompactDuration::new(60);
    let updated_ttl = CompactDuration::new(7_200);

    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(Some(initial_ttl)))?;

    // Same name, same identity, different operational settings — rejected.
    let duplicate = registry.register(
        &cart(),
        CollectionDef {
            commit_mode: CommitMode::ReadUncommitted,
            ..CollectionDef::new(Some(updated_ttl))
        },
    );
    let Err(error) = duplicate else {
        return Err(eyre!("a duplicate registration must be rejected"));
    };
    assert!(matches!(error, RegisterStateError::Duplicate { .. }));
    assert_eq!(error.classify_error(), ErrorCategory::Permanent);

    // The first registration's settings are untouched — no last-wins overwrite.
    assert_eq!(
        registry.ttl_for(StateType::Application, &name),
        Some(initial_ttl),
        "the rejected re-registration must not have changed the TTL"
    );
    assert_eq!(
        registry.commit_mode_for(StateType::Application, &name),
        CommitMode::ReadCommitted,
        "the rejected re-registration must not have changed the commit mode"
    );
    Ok(())
}

/// The map and set `keyset_limit` method changes the collection definition.
/// Value and deque descriptors do not expose this method.
#[test]
pub(super) fn keyset_limit_threads_into_the_collection_def() {
    let descriptor: MapDescriptor<I64KeyCodec> = map_state("m");
    assert_eq!(descriptor.keyset_limit(7).collection_def().keyset_limit, 7);
}

/// `.published(bool)` and every read-cache policy thread into the collection
/// def. `.published` is also reversible.
#[test]
pub(super) fn visibility_and_read_cache_thread_into_the_collection_def() {
    use crate::state::ReadCachePolicy;
    use std::time::Duration;

    let ttl = Duration::from_secs(30);
    let def = cart().published(true).read_cache(ttl).collection_def();
    assert_eq!(def.visibility, StateVisibility::Published);
    assert_eq!(def.read_cache, ReadCachePolicy::Ttl(ttl));
    assert_eq!(
        cart()
            .read_cache(ReadCachePolicy::Disabled)
            .collection_def()
            .read_cache,
        ReadCachePolicy::Disabled,
    );
    assert_eq!(
        cart()
            .published(true)
            .published(false)
            .collection_def()
            .visibility,
        StateVisibility::Private,
        "published(true).published(false) reverts to Private",
    );
}

/// An empty descriptor name fails loudly at registration — the
/// fallible boundary backing the infallible `value_state`.
#[test]
pub(super) fn empty_name_rejected_at_registration() {
    let mut registry = CollectionDefRegistry::default();
    let empty: ValueDescriptor = value_state("");
    let result = registry.register(&empty, CollectionDef::new(None));
    assert!(matches!(result, Err(RegisterStateError::Name(_))));
}

/// Descriptors are plain values: for any runtime name string, two
/// descriptors built independently from equal strings are interchangeable —
/// same (interned) name, same frozen identity — so a call site can build a
/// descriptor wherever it needs one instead of sharing one declaration. The
/// registry holds one declaration per name, so registering the second is
/// rejected loudly as a [`RegisterStateError::Duplicate`] (never last-wins);
/// interchangeability is the identity/name equality, provable without a
/// successful second register.
#[test]
pub(super) fn prop_descriptors_from_equal_strings_are_interchangeable() {
    fn prop(name: String) -> TestResult {
        if name.trim().is_empty() {
            return TestResult::discard();
        }
        let input_dbg = format!("name={name:?}");
        let result = (move || -> Result<bool> {
            let a: ValueDescriptor = value_state(&name);
            let b: ValueDescriptor = value_state(&name);
            let mut registry = CollectionDefRegistry::default();
            registry.register(&a, CollectionDef::new(None))?;
            // A same-identity re-registration of the equal descriptor is a
            // duplicate declaration — rejected, not silently overwritten.
            let duplicate = registry.register(&b, CollectionDef::new(None));
            if !matches!(duplicate, Err(RegisterStateError::Duplicate { .. })) {
                return Ok(false);
            }
            Ok(a.name() == b.name() && a.structural_identity() == b.structural_identity())
        })();
        finish_trace(
            result,
            "equal strings must build interchangeable descriptors and reject a duplicate register",
            &input_dbg,
        )
    }
    QuickCheck::new().quickcheck(prop as fn(String) -> TestResult);
}
