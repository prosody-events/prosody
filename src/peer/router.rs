//! Concrete router types selected by client backends.

use super::PeerConfiguration;
use super::runtime::{PeerRouter, PreparedResponder, prepare_router, start_local_router};
use crate::Codec;
use crate::consumer::{ConsumerError, ShutdownError};
use crate::producer::ProsodyProducer;
use crate::requester::ProsodyRequester;
use crate::response::sender::ResponseRoute;
use crate::response::sender::Then;
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::grpc::client::GrpcSender;
use crate::router::{LocalTarget, RouterHandle};
use crate::state_reader::CassandraReaderBackend;
use crate::subsystem::SubsystemName;

mod sealed {
    pub trait Sealed {}
}

/// One peer route and its lifecycle.
///
/// Client backends select a concrete implementation. The type fixes local-only
/// or local-first network routing for the life of the client.
pub trait Router: sealed::Sealed + Send + Sync + Sized + 'static {
    /// Builds request access over this router.
    fn requester<C: Codec, R: Codec>(&self, producer: ProsodyProducer<C>)
    -> ProsodyRequester<C, R>;

    /// Builds response access for one subsystem.
    #[doc(hidden)]
    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError>;

    /// Stops this router.
    fn shutdown(self) -> impl Future<Output = Result<(), ShutdownError>> + Send;
}

/// A local-only router for in-memory clients.
pub struct LocalRouter {
    inner: PeerRouter<LocalTarget>,
}

/// A local-first gRPC router for Cassandra clients.
pub struct GrpcRouter {
    inner: PeerRouter<Then<LocalTarget, RouterHandle<GrpcSender, CassandraNodeDirectory>>>,
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

impl<R: ResponseRoute> sealed::Sealed for PeerRouter<R> {}
impl sealed::Sealed for LocalRouter {}
impl sealed::Sealed for GrpcRouter {}

impl<R: ResponseRoute> Router for PeerRouter<R> {
    fn requester<PC: Codec, RC: Codec>(
        &self,
        producer: ProsodyProducer<PC>,
    ) -> ProsodyRequester<PC, RC> {
        self.build_requester(producer)
    }

    fn responder<RC: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<RC>, ConsumerError> {
        self.build_responder(subsystem)
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.stop().await
    }
}

impl Router for LocalRouter {
    fn requester<PC: Codec, R: Codec>(
        &self,
        producer: ProsodyProducer<PC>,
    ) -> ProsodyRequester<PC, R> {
        self.inner.build_requester(producer)
    }

    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError> {
        self.inner.build_responder(subsystem)
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.inner.stop().await
    }
}

impl Router for GrpcRouter {
    fn requester<PC: Codec, R: Codec>(
        &self,
        producer: ProsodyProducer<PC>,
    ) -> ProsodyRequester<PC, R> {
        self.inner.build_requester(producer)
    }

    fn responder<R: Codec>(
        &self,
        subsystem: SubsystemName,
    ) -> Result<PreparedResponder<R>, ConsumerError> {
        self.inner.build_responder(subsystem)
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.inner.stop().await
    }
}
