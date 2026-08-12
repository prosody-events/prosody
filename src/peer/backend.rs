//! Compile-time backend selection for one peer runtime.

use crate::cassandra::CassandraStore;
#[cfg(test)]
use crate::codec::Codec;
use crate::consumer::{ConsumerError, PeerInitError};
use crate::peer::PeerConfiguration;
use crate::peer::router::directory::cassandra::CassandraPeerDirectory;
#[cfg(test)]
use crate::peer::router::runtime::PreparedLocalPeerRuntime;
use crate::peer::router::runtime::PreparedPeerRuntime;
use crate::peer::runtime::prepare_network;
#[cfg(test)]
use crate::peer::runtime::{PreparedRuntime, prepare_local};
#[cfg(test)]
use crate::state_reader::{CassandraReaderBackend, MemoryReaderBackend};

pub(crate) async fn prepare_cassandra(
    config: &PeerConfiguration,
    store: CassandraStore,
) -> Result<PreparedPeerRuntime<CassandraPeerDirectory>, ConsumerError> {
    let parts = config.parts().map_err(PeerInitError::from)?;
    let directory = CassandraPeerDirectory::new(store, parts.lease)
        .await
        .map_err(|error| PeerInitError::Directory {
            message: format!("{error:#}"),
        })?;
    prepare_network(parts, directory).await
}

/// A backend that selects its peer runtime at compile time.
#[cfg(test)]
pub(crate) trait PeerBackend: Send + Sync + Sized + 'static {
    type Runtime: PreparedRuntime;

    fn prepare(
        &self,
        config: &PeerConfiguration,
    ) -> impl Future<Output = Result<Self::Runtime, ConsumerError>> + Send;
}

#[cfg(test)]
impl<C: Codec> PeerBackend for CassandraReaderBackend<C> {
    type Runtime = PreparedPeerRuntime<CassandraPeerDirectory>;

    async fn prepare(&self, config: &PeerConfiguration) -> Result<Self::Runtime, ConsumerError> {
        prepare_cassandra(config, self.cells_ref().session.clone()).await
    }
}

#[cfg(test)]
impl<C: Codec> PeerBackend for MemoryReaderBackend<C> {
    type Runtime = PreparedLocalPeerRuntime;

    async fn prepare(&self, _config: &PeerConfiguration) -> Result<Self::Runtime, ConsumerError> {
        Ok(prepare_local())
    }
}
