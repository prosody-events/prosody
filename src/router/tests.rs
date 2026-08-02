use super::{NodeId, Router, RouterHandle};
use crate::router::directory::RegistrationTtl;
use crate::router::directory::cache::{AddressCache, AddressResolver};
use crate::router::directory::tests::support::{directory, membership, registration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::FleetConfiguration;
use crate::router::loopback::LoopbackSender;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use std::sync::Arc;
use std::time::Duration;
use uuid::{Uuid, Version};

/// The lease the router's own read runs under.
const LEASE: Duration = Duration::from_secs(30);

/// How many registrations the router's cache holds in this suite.
const CACHE_CAPACITY: usize = 8;

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

/// A router hands out only what a node published, and nothing at all for a node
/// the directory does not hold.
///
/// This is where the addressing rule is enforced rather than described: a
/// response reaches an address because a process wrote that address about
/// itself, and there is no other source for one.
#[test]
fn a_router_addresses_only_what_the_directory_published() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let directory = directory(LEASE).await?;
        let published = registration(NodeId::new(), membership());
        directory.register(&published).await?;

        let (transport, _recorded) = LoopbackSender::new();
        let router = RouterHandle::new(
            AddressResolver::new(
                AddressCache::new(CACHE_CAPACITY, RegistrationTtl::try_from(LEASE)?),
                directory,
            ),
            Arc::new(DestinationFleet::new(FleetConfiguration::default())),
            Arc::new(transport),
        );

        let address = router
            .address(published.node)
            .await?
            .ok_or_else(|| eyre!("a published node must resolve"))?;
        assert_eq!(
            address, published.direct,
            "a router must hand out the endpoint the node published"
        );
        assert_eq!(
            router.address(NodeId::new()).await?,
            None,
            "a node the directory does not hold must reach no address"
        );
        Ok(())
    })
}
