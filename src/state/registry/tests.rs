use super::{CollectionDef, CollectionDefRegistry, RegisterStateError, StateVisibility};
use crate::codec::JsonCodec;
use crate::state::StateType;
use crate::state::descriptor::{DescriptorIdentity, value_state};
use color_eyre::eyre::Result;

/// A `Published` collection registered under a non-`Application` state type
/// is rejected at registration. Publication routing rows carry no
/// `state_type`, and the reader addresses `Application` cells only, so any
/// other type would be unaddressable. The test uses the `#[cfg(test)]`
/// `Framework` state type. Its `Application` arm shows the guard is
/// specific to the state type, not a blanket ban on publishing.
#[test]
fn published_non_application_state_type_rejected() -> Result<()> {
    let identity = value_state::<JsonCodec>("cart").structural_identity();
    let mut def = CollectionDef::new(None);
    def.visibility = StateVisibility::Published;

    let mut registry = CollectionDefRegistry::default();
    registry.register_identity(StateType::Application, "cart", identity.clone(), def)?;

    let result = registry.register_identity(StateType::Framework, "cart", identity, def);
    assert!(
        matches!(
            result,
            Err(RegisterStateError::PublishedNonApplicationStateType { .. })
        ),
        "expected PublishedNonApplicationStateType, got {result:?}"
    );
    Ok(())
}
