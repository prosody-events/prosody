//! Peer registration lifecycle.

use super::plain_process;
use crate::peer::router::directory::PeerDirectory;
use crate::peer::router::directory::tests::suite::same_registration;
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};

/// A process registers before start returns and removes its row at shutdown.
#[test]
fn runtime_registers_on_start_and_deregisters_on_shutdown() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let process = plain_process().await?;
        let peer = process.runtime.peer();
        let registered = process
            .directory
            .read(peer)
            .await?
            .ok_or_else(|| eyre!("a started runtime must resolve"))?;
        ensure!(registered.advertised.is_none() && registered.network.is_none());
        ensure!(
            process
                .runtime
                .network
                .addresses
                .resolve(peer)
                .await?
                .as_deref()
                .is_some_and(|resolved| same_registration(resolved, &registered))
        );

        process.runtime.shutdown(|| async {}).await?;
        ensure!(process.directory.read(peer).await?.is_none());
        Ok(())
    })
}
