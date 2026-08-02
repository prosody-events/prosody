use super::NodeId;
use uuid::{Uuid, Version};

/// Ids are minted fresh, never derived from anything a restart could repeat:
/// two mints of the same process already differ, and each is a random UUID.
#[test]
fn every_minted_node_id_is_a_fresh_random_uuid() {
    let first = NodeId::new();
    let second = NodeId::new();
    assert_ne!(first, second, "two mints must not collide");
    for id in [first, second] {
        assert_eq!(
            Uuid::from_bytes(id.into_bytes()).get_version(),
            Some(Version::Random),
            "{id} must be a random UUID"
        );
    }
}
