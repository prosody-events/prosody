//! Concrete router types selected by client backends.

use super::PeerConfiguration;
use super::backend::prepare_cassandra;
use super::runtime::{
    ConsumerHandle, PeerRouter, ProducerHandle, RespondingLeaf, RespondingPeer, RouterOwner,
    prepare_router, start_local_router, start_router,
};
use crate::Codec;
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::consumer::middleware::{FallibleHandler, HandlerMiddleware};
use crate::consumer::{ConsumerError, PeerInitError};
use crate::response::frame::encode::Staged;
use crate::response::headers::RequestDeadline;
use crate::response::sender::{DropReason, ResponseRoute, RouteOutcome, Then};
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::fleet::Destination;
use crate::router::grpc::client::GrpcSender;
use crate::router::{LocalTarget, RouterHandle};
use crate::state_reader::CassandraReaderBackend;
use crate::subsystem::SubsystemName;
use std::future::Future;

mod sealed {
    use super::{ConsumerHandle, ResponseRoute};

    pub trait Router {}
    pub trait Consumer {
        type Route: ResponseRoute;

        fn handle(&self) -> &ConsumerHandle<Self::Route>;
    }
}

/// A typed response capability from one router.
///
/// Consumer constructors require this capability. It contains one response
/// route and its matching fleet.
pub trait ConsumerRouter: sealed::Consumer + Send + Sync + Sized + 'static {}

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
    inner: ConsumerHandle<LocalResponseRoute>,
}

/// The local-first gRPC response capability.
pub struct GrpcConsumer {
    inner: ConsumerHandle<GrpcResponseRoute>,
}

/// The opaque local response route selected by [`LocalConsumer`].
#[doc(hidden)]
#[derive(Clone)]
pub struct LocalResponseRoute(LocalTarget);

/// The opaque network response route selected by [`GrpcConsumer`].
#[doc(hidden)]
#[derive(Clone)]
pub struct GrpcResponseRoute(Then<LocalTarget, RouterHandle<GrpcSender, CassandraNodeDirectory>>);

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
    /// Starts a local-first gRPC router with its own Cassandra session.
    ///
    /// Use this constructor for a standalone producer or consumer. A combined
    /// high-level client shares its existing Cassandra session instead.
    ///
    /// # Errors
    ///
    /// Returns an error when Cassandra, peer configuration, or the listener
    /// cannot start.
    pub async fn new(
        config: &PeerConfiguration,
        cassandra: &CassandraConfiguration,
    ) -> Result<Self, ConsumerError> {
        let store =
            CassandraStore::new(cassandra)
                .await
                .map_err(|error| PeerInitError::Directory {
                    message: format!("{error:#}"),
                })?;
        Ok(Self {
            inner: start_router(prepare_cassandra(config, store).await?).await?,
        })
    }

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
impl<R: ResponseRoute> sealed::Consumer for ConsumerHandle<R> {
    type Route = R;

    fn handle(&self) -> &ConsumerHandle<R> {
        self
    }
}
impl sealed::Consumer for LocalConsumer {
    type Route = LocalResponseRoute;

    fn handle(&self) -> &ConsumerHandle<Self::Route> {
        &self.inner
    }
}

impl sealed::Consumer for GrpcConsumer {
    type Route = GrpcResponseRoute;

    fn handle(&self) -> &ConsumerHandle<Self::Route> {
        &self.inner
    }
}

impl<R: ResponseRoute> ConsumerRouter for ConsumerHandle<R> {}
impl ConsumerRouter for LocalConsumer {}
impl ConsumerRouter for GrpcConsumer {}

impl ResponseRoute for LocalResponseRoute {
    fn deliver(
        &self,
        frame: Staged,
        destination: &Destination,
        deadline: RequestDeadline,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send {
        self.0.deliver(frame, destination, deadline)
    }
}

impl ResponseRoute for GrpcResponseRoute {
    fn deliver(
        &self,
        frame: Staged,
        destination: &Destination,
        deadline: RequestDeadline,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send {
        self.0.deliver(frame, destination, deadline)
    }
}

type RespondingParts<RT, C, M, H> = (
    <M as HandlerMiddleware<<H as FallibleHandler>::Payload>>::Provider<
        RespondingLeaf<H, C, <RT as sealed::Consumer>::Route>,
    >,
    RespondingPeer,
);

pub(crate) fn responding_provider<RT, C, M, H>(
    router: &RT,
    subsystem: SubsystemName,
    middleware: &M,
    handler: H,
) -> RespondingParts<RT, C, M, H>
where
    RT: ConsumerRouter,
    <RT as sealed::Consumer>::Route: ResponseRoute,
    C: Codec<Payload = Result<H::Output, H::Error>>,
    M: HandlerMiddleware<H::Payload>,
    H: FallibleHandler + Clone + Send + Sync + 'static,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
{
    router
        .handle()
        .responding_provider::<C, _, _>(subsystem, middleware, handler)
}

impl Router for LocalRouter {
    type Consumer = LocalConsumer;

    fn split(self) -> (ProducerHandle, Self::Consumer, RouterOwner) {
        let (producer, consumer, owner) = self.inner.into_parts();
        (
            producer,
            LocalConsumer {
                inner: consumer.map_route(LocalResponseRoute),
            },
            owner,
        )
    }
}

impl Router for GrpcRouter {
    type Consumer = GrpcConsumer;

    fn split(self) -> (ProducerHandle, Self::Consumer, RouterOwner) {
        let (producer, consumer, owner) = self.inner.into_parts();
        (
            producer,
            GrpcConsumer {
                inner: consumer.map_route(GrpcResponseRoute),
            },
            owner,
        )
    }
}
