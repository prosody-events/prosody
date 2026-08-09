//! The local-only router selected by the in-memory backend.

use super::{common_config, consumer_config, peer_config};
use crate::JsonCodec;
use crate::consumer::wiring::memory_deps;
use crate::consumer::{CommonConfiguration, ConsumerConfiguration, ConsumerSetup};
use crate::high_level::config::TriggerStoreConfiguration;
use crate::peer::runtime::prepare_router;
use crate::state_reader::{MemoryReaderBackend, StateReaderDependencies};
use crate::subsystem::SubsystemName;
use crate::test_util::TEST_RUNTIME;
use color_eyre::Result;
use std::net::{Ipv4Addr, TcpListener};

/// Mock peer preparation does not bind its configured address.
#[test]
fn a_mock_peer_uses_no_listener() -> Result<()> {
    let held = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let address = held.local_addr()?;
    let config = consumer_config("local-peer-no-listener")?;
    let peer = peer_config(address)?;
    let common = common_config(Some(SubsystemName::try_new("orders")?))?;
    let deps = select(&config, &common);
    TEST_RUNTIME.block_on(async {
        let first = prepare_router(&peer, deps.backend().as_ref()).await?;
        let second = prepare_router(&peer, deps.backend().as_ref()).await?;
        assert_ne!(
            first.node(),
            second.node(),
            "each local peer must own a distinct identity"
        );
        first.shutdown().await?;
        second.shutdown().await?;
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
