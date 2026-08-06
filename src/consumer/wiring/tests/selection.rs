//! The in-memory backend selection every mode shares, and the one peer
//! combination it refuses.

use super::{common_config, consumer_config, peer_config};
use crate::JsonCodec;
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::wiring::memory_deps;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration, ConsumerSetup};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::router::directory::RegistrationTtl;
use crate::state_reader::{MemoryReaderBackend, PeerDirectoryBackend, StateReaderDependencies};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use std::net::{Ipv4Addr, SocketAddr};

/// A peer fleet on in-memory storage is refused outside mock mode.
///
/// The memory directory answers only what this process registered, so a peer
/// that binds a listener could accept a request it can never answer. Mock mode
/// still builds, because a mock fleet asks itself.
#[test]
fn a_peer_fleet_on_memory_storage_is_refused_outside_mock_mode() -> Result<()> {
    let config = consumer_config("peer-memory-selection")?;
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let common = common_config(Some(peer), Some(SubsystemName::try_new("orders")?))?;

    TEST_RUNTIME.block_on(async {
        let deps = select(&config, &common);
        assert!(matches!(
            deps.backend()
                .node_directory(RegistrationTtl::DEFAULT, false)
                .await,
            Err(ConsumerError::Peer(PeerInitError::MemoryDirectory))
        ));
        assert!(
            deps.backend()
                .node_directory(RegistrationTtl::DEFAULT, true)
                .await
                .is_ok()
        );
        Ok(())
    })
}

/// Runs the shared memory arm and returns the dependencies it builds.
fn select(
    consumer: &ConsumerConfiguration,
    common: &CommonConfiguration,
) -> StateReaderDependencies<JsonCodec, MemoryReaderBackend<JsonCodec>> {
    let trigger_store = TriggerStoreConfiguration::InMemory;
    let setup = ConsumerSetup {
        consumer,
        trigger_store: &trigger_store,
        common,
    };
    memory_deps::<JsonCodec>(&setup)
}
