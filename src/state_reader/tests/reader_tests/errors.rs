//! Reader identity validation and acquisition errors.

use super::*;

/// A source whose frozen identity differs from the reader's descriptor in
/// any one field fails with `IdentityMismatch` (Permanent). An unperturbed
/// identity acquires normally. A raw, unknown `kind` discriminant compares
/// unequal instead of failing to decode.
///
/// Falsify: widen `descriptor_identity::validate` to compare only
/// `format_id`. Then the kind-perturbed case acquires instead of failing.
#[tokio::test]
async fn identity_mismatch_is_permanent() -> Result<()> {
    use crate::state::descriptor_identity::DescriptorIdentityStore;
    use crate::state::publication::StatePublication;
    use std::sync::Arc;

    let descriptor = value_state::<JsonCodec>("v-ident");
    let name = state_name("v-ident")?;
    let sub = subsystem()?;
    let tp = topic("orders");
    let count = mock_count();

    let base = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        name.as_str(),
        &descriptor.structural_identity(),
    );

    let mut kind_perturbed = base.clone();
    kind_perturbed.kind = base.kind.wrapping_add(1);
    let mut fmt_perturbed = base.clone();
    fmt_perturbed.format_id = format!("{}-x", base.format_id);
    let mut key_fmt_perturbed = base.clone();
    key_fmt_perturbed.key_format_id = format!("{}-x", base.key_format_id);
    // A raw discriminant that no CollectionKindId uses. It compares unequal
    // instead of failing to decode.
    let mut unknown_kind = base.clone();
    unknown_kind.kind = 127;

    for perturbed in [
        kind_perturbed,
        fmt_perturbed,
        key_fmt_perturbed,
        unknown_kind,
    ] {
        let harness = MemoryHarness::new();
        harness
            .publications
            .upsert(
                &sub,
                StateType::Application,
                &name,
                &StatePublication {
                    group_id: Arc::from(GROUP_A),
                    topic: tp,
                    partition_count: count,
                },
            )
            .await
            .unwrap_or_else(|e| match e {});
        harness
            .identities
            .register_identity(GROUP_A, &perturbed)
            .await
            .unwrap_or_else(|e| match e {});

        let deps = harness.deps();
        let reader = StateReader::new(&deps, sub.clone(), descriptor)?;
        match reader.get(Key::from("user-1")).await {
            Err(StateReaderError::IdentityMismatch { .. }) => {}
            other => bail!("expected IdentityMismatch, got {other:?}"),
        }
    }

    // The unperturbed identity acquires (no rows written, so the read returns
    // None, not an identity error).
    let harness = MemoryHarness::new();
    publish_source(
        (&harness.publications, &harness.identities),
        &sub,
        &name,
        GROUP_A,
        tp,
        count,
        &descriptor,
    )
    .await;
    let deps = harness.deps();
    let reader = StateReader::new(&deps, sub, descriptor)?;
    assert_eq!(reader.get(Key::from("user-1")).await?, None);
    Ok(())
}

/// An empty key is rejected with `EmptyKey` before any acquisition. A
/// collection with no publication rows fails with `UnknownPublication`
/// (Transient).
///
/// Falsify: drop the `key.is_empty()` guard in `StateReader::session`. The
/// empty-key read no longer short-circuits, and the `EmptyKey` arm is never
/// reached.
#[tokio::test]
async fn boundary_errors() -> Result<()> {
    let harness = MemoryHarness::new();
    let descriptor = value_state::<JsonCodec>("v-empty");
    let sub = subsystem()?;
    let deps = harness.deps();
    let reader = StateReader::new(&deps, sub, descriptor)?;

    match reader.get(Key::from("")).await {
        Err(StateReaderError::EmptyKey) => {}
        other => bail!("expected EmptyKey, got {other:?}"),
    }
    match reader.get(Key::from("user-1")).await {
        Err(StateReaderError::UnknownPublication { .. }) => {}
        other => bail!("expected UnknownPublication, got {other:?}"),
    }
    Ok(())
}
