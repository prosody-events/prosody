//! Concrete router types selected by client backends.

use super::PeerConfiguration;
use super::runtime::{
    ConsumerHandle, PeerRouter, PreparedResponder, ProducerHandle, RouterOwner, prepare_router,
    start_local_router,
};
use crate::Codec;
use crate::consumer::ConsumerError;
use crate::response::sender::{ResponseRoute, Then};
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::grpc::client::GrpcSender;
use crate::router::{LocalTarget, RouterHandle};
use crate::state_reader::CassandraReaderBackend;
use crate::subsystem::SubsystemName;

mod sealed {
    pub trait Router {}
    pub trait Consumer {}
}

/// A typed response capability from one router.
///
/// Consumer constructors require this capability. It contains one response
/// route and its matching fleet.
pub trait ConsumerRouter: sealed::Consumer + Send + Sync + Sized + 'static {
    /// Builds response access for one subsystem.
    #[doc(hidden)]
    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError>;
}

/// One peer route that separates into producer, consumer, and owner parts.
///
/// One call creates all three parts. Their constructors are private, so
/// production code cannot combine an identity, registry, route, or fleet from
/// different routers.
pub trait Router: sealed::Router + Send + Sync + Sized + 'static {
    /// The response capability selected by this router.
    type Consumer: ConsumerRouter;

    /// Separates this router into its three exclusive roles.
    fn split(self) -> (ProducerHandle, Self::Consumer, RouterOwner);
}

/// A local-only router for in-memory clients.
pub struct LocalRouter {
    inner: PeerRouter<LocalTarget>,
}

/// A local-first gRPC router for Cassandra clients.
pub struct GrpcRouter {
    inner: PeerRouter<Then<LocalTarget, RouterHandle<GrpcSender, CassandraNodeDirectory>>>,
}

/// The local-only response capability.
pub struct LocalConsumer {
    inner: ConsumerHandle<LocalTarget>,
}

/// The local-first gRPC response capability.
pub struct GrpcConsumer {
    inner: ConsumerHandle<Then<LocalTarget, RouterHandle<GrpcSender, CassandraNodeDirectory>>>,
}

impl LocalRouter {
    /// Starts a local-only router.
    ///
    /// # Errors
    ///
    /// Returns an error when the peer configuration is invalid.
    pub async fn new(config: &PeerConfiguration) -> Result<Self, ConsumerError> {
        Ok(Self {
            inner: start_local_router(config).await?,
        })
    }
}

impl GrpcRouter {
    pub(crate) async fn start<C: Codec>(
        config: &PeerConfiguration,
        backend: &CassandraReaderBackend<C>,
    ) -> Result<Self, ConsumerError> {
        Ok(Self {
            inner: prepare_router(config, backend).await?,
        })
    }
}

impl sealed::Router for LocalRouter {}
impl sealed::Router for GrpcRouter {}
impl<R: ResponseRoute> sealed::Consumer for ConsumerHandle<R> {}
impl sealed::Consumer for LocalConsumer {}
impl sealed::Consumer for GrpcConsumer {}

impl<R: ResponseRoute> ConsumerRouter for ConsumerHandle<R> {
    fn responder<RC: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<RC>, ConsumerError> {
        self.build_responder(subsystem)
    }
}

impl ConsumerRouter for LocalConsumer {
    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError> {
        self.inner.build_responder(subsystem)
    }
}

impl ConsumerRouter for GrpcConsumer {
    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError> {
        self.inner.build_responder(subsystem)
    }
}

impl Router for LocalRouter {
    type Consumer = LocalConsumer;

    fn split(self) -> (ProducerHandle, Self::Consumer, RouterOwner) {
        let (producer, consumer, owner) = self.inner.into_parts();
        (producer, LocalConsumer { inner: consumer }, owner)
    }
}

impl Router for GrpcRouter {
    type Consumer = GrpcConsumer;

    fn split(self) -> (ProducerHandle, Self::Consumer, RouterOwner) {
        let (producer, consumer, owner) = self.inner.into_parts();
        (producer, GrpcConsumer { inner: consumer }, owner)
    }
}
