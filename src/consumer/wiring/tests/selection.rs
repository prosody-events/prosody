//! The in-memory backend selection every mode shares, and the one peer
//! combination it refuses.

use super::{common_config, consumer_config, peer_config};
use crate::JsonCodec;
use crate::consumer::error::{ConsumerError, PeerInitError};
use crate::consumer::wiring::memory_deps;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration, ConsumerSetup};
use crate::consumer::{Managers, PreparePeer};
use crate::heartbeat::HeartbeatRegistry;
use crate::high_level::config::TriggerStoreConfiguration;
use crate::state_reader::{LocalPeerMode, MemoryReaderBackend, StateReaderDependencies};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;

/// A peer fleet on in-memory storage is refused outside mock mode.
///
/// The memory backend has no cross-process peer path. Mock mode still builds,
/// because its local route answers only its own client.
#[test]
fn a_peer_fleet_on_memory_storage_is_refused_outside_mock_mode() -> Result<()> {
    let config = consumer_config("peer-memory-selection")?;
    let peer = peer_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let common = common_config(Some(peer.clone()), Some(SubsystemName::try_new("orders")?))?;

    TEST_RUNTIME.block_on(async {
        let deps = select(&config, &common);
        let managers = Arc::<Managers<serde_json::Value>>::default();
        let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);
        assert!(matches!(
            LocalPeerMode::prepare(
                &peer,
                deps.backend().as_ref(),
                false,
                Arc::clone(&managers),
                &heartbeats,
            )
            .await,
            Err(ConsumerError::Peer(PeerInitError::MemoryDirectory))
        ));
        LocalPeerMode::prepare(&peer, deps.backend().as_ref(), true, managers, &heartbeats)
            .await?
            .abandon()
            .await;
        Ok(())
    })
}

/// Mock peer preparation does not bind its configured address.
#[test]
fn a_mock_peer_uses_no_listener() -> Result<()> {
    let held = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let address = held.local_addr()?;
    let config = consumer_config("local-peer-no-listener")?;
    let peer = peer_config(address)?;
    let common = common_config(Some(peer.clone()), Some(SubsystemName::try_new("orders")?))?;
    let deps = select(&config, &common);
    let managers = Arc::<Managers<serde_json::Value>>::default();
    let heartbeats = HeartbeatRegistry::new(config.group_id.clone(), config.stall_threshold);

    TEST_RUNTIME.block_on(async {
        let first = LocalPeerMode::prepare(
            &peer,
            deps.backend().as_ref(),
            true,
            Arc::clone(&managers),
            &heartbeats,
        )
        .await?;
        let second =
            LocalPeerMode::prepare(&peer, deps.backend().as_ref(), true, managers, &heartbeats)
                .await?;
        assert_ne!(
            first.node(),
            second.node(),
            "each local peer must own a distinct identity"
        );
        first.abandon().await;
        second.abandon().await;
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
