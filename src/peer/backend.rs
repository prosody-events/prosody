//! Compile-time backend selection for one peer runtime.

use crate::codec::Codec;
use crate::consumer::{ConsumerError, PeerInitError};
use crate::peer::PreparePeer;
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::directory::{NodeDirectory, RegistrationTtl};
use crate::state_reader::{CassandraReaderBackend, MemoryReaderBackend};

/// Selects a peer runtime with local and gRPC routes.
pub(crate) struct NetworkPeerMode;

/// Selects a peer runtime with only the local route.
pub(crate) struct LocalPeerMode;

/// A backend that selects its peer runtime at compile time.
pub(crate) trait PeerBackend: Send + Sync + Sized + 'static {
    type PeerMode: PreparePeer<Self>;
}

/// A backend that supplies a directory to the network peer runtime.
pub(crate) trait NetworkPeerBackend: PeerBackend {
    type Directory: NodeDirectory;

    fn node_directory(
        &self,
        lease: RegistrationTtl,
    ) -> impl Future<Output = Result<Self::Directory, ConsumerError>> + Send;
}

impl<C: Codec> PeerBackend for CassandraReaderBackend<C> {
    type PeerMode = NetworkPeerMode;
}

impl<C: Codec> NetworkPeerBackend for CassandraReaderBackend<C> {
    type Directory = CassandraNodeDirectory;

    async fn node_directory(
        &self,
        lease: RegistrationTtl,
    ) -> Result<Self::Directory, ConsumerError> {
        CassandraNodeDirectory::new(self.cells_ref().session.clone(), lease)
            .await
            .map_err(|error| {
                ConsumerError::Peer(PeerInitError::Directory {
                    message: format!("{error:#}"),
                })
            })
    }
}

impl<C: Codec> PeerBackend for MemoryReaderBackend<C> {
    type PeerMode = LocalPeerMode;
}
