//! Compile-time backend selection for one peer runtime.

use crate::cassandra::CassandraStore;
use crate::codec::Codec;
use crate::consumer::{ConsumerError, PeerInitError};
use crate::peer::PeerConfiguration;
use crate::peer::runtime::{PreparedRuntime, prepare_local, prepare_network};
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::runtime::{PreparedLocalPeerRuntime, PreparedPeerRuntime};
use crate::state_reader::{CassandraReaderBackend, MemoryReaderBackend};

pub(crate) async fn prepare_cassandra(
    config: &PeerConfiguration,
    store: CassandraStore,
) -> Result<PreparedPeerRuntime<CassandraNodeDirectory>, ConsumerError> {
    let parts = config.parts().map_err(PeerInitError::from)?;
    let directory = CassandraNodeDirectory::new(store, parts.lease)
        .await
        .map_err(|error| PeerInitError::Directory {
            message: format!("{error:#}"),
        })?;
    prepare_network(parts, directory).await
}

/// A backend that selects its peer runtime at compile time.
pub(crate) trait PeerBackend: Send + Sync + Sized + 'static {
    type Runtime: PreparedRuntime;

    fn prepare(
        &self,
        config: &PeerConfiguration,
    ) -> impl Future<Output = Result<Self::Runtime, ConsumerError>> + Send;
}

impl<C: Codec> PeerBackend for CassandraReaderBackend<C> {
    type Runtime = PreparedPeerRuntime<CassandraNodeDirectory>;

    async fn prepare(&self, config: &PeerConfiguration) -> Result<Self::Runtime, ConsumerError> {
        prepare_cassandra(config, self.cells_ref().session.clone()).await
    }
}

impl<C: Codec> PeerBackend for MemoryReaderBackend<C> {
    type Runtime = PreparedLocalPeerRuntime;

    async fn prepare(&self, config: &PeerConfiguration) -> Result<Self::Runtime, ConsumerError> {
        prepare_local(config)
    }
}
