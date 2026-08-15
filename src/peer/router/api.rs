//! Concrete router types selected by client backends.

use super::config::PeerConfiguration;
use crate::cassandra::CassandraStore;
use crate::consumer::{ConsumerError, ShutdownError};
use crate::peer::backend::prepare_cassandra;
use crate::peer::metrics::PeerMetrics;
use crate::peer::response::frame::encode::Staged;
use crate::peer::response::headers::RequestDeadline;
use crate::peer::response::sender::{
    DropReason, PeerMetricSource, ResponseRoute, RouteOutcome, Then,
};
use crate::peer::router::directory::cassandra::CassandraPeerDirectory;
use crate::peer::router::grpc::client::GrpcSender;
use crate::peer::router::{LocalTarget, NetworkRoute};
use crate::peer::runtime::{
    LocalRoute, PeerRouter, ProducerHandle, start_local_router, start_router,
};
use opentelemetry::Context;
use std::future::Future;

mod sealed {
    pub trait Router {}
}

/// One peer route with producer and consumer capabilities.
///
/// The router owns its runtime. Dropping it starts teardown. Its private
/// constructors prevent code from combining capabilities from different
/// routers.
pub trait Router: sealed::Router + Send + Sync + Sized + 'static {
    /// The response route selected by this router.
    type Response: ResponseRoute + PeerMetricSource;

    /// Returns this router's producer capability.
    fn producer(&self) -> ProducerHandle;

    /// Returns this router's response route.
    fn response(&self) -> Self::Response;

    /// Stops this router and consumes it.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime cannot complete teardown.
    fn shutdown(self) -> impl Future<Output = Result<(), ShutdownError>> + Send;
}

/// A local-only router for in-memory clients.
pub struct LocalRouter {
    inner: PeerRouter<LocalTarget>,
}

/// A local-first gRPC router for Cassandra clients.
pub struct GrpcRouter {
    inner: PeerRouter<Then<LocalTarget, NetworkRoute<GrpcSender, CassandraPeerDirectory>>>,
}

/// The opaque local response route selected by [`LocalRouter`].
#[doc(hidden)]
#[derive(Clone)]
pub struct LocalResponseRoute {
    route: LocalTarget,
}

/// The opaque network response route selected by [`GrpcRouter`].
#[doc(hidden)]
#[derive(Clone)]
pub struct GrpcResponseRoute {
    route: Then<LocalTarget, NetworkRoute<GrpcSender, CassandraPeerDirectory>>,
}

impl LocalRouter {
    /// Starts a local-only router.
    /// # Errors
    ///
    /// Returns an error if the local runtime cannot start.
    pub async fn new() -> Result<Self, ConsumerError> {
        Ok(Self {
            inner: start_local_router().await?,
        })
    }
}

impl GrpcRouter {
    /// Starts a local-first gRPC router with a shared Cassandra store.
    ///
    /// # Errors
    ///
    /// Returns an error when Cassandra, peer configuration, or the listener
    /// cannot start.
    pub async fn new(
        config: &PeerConfiguration,
        store: CassandraStore,
    ) -> Result<Self, ConsumerError> {
        Ok(Self {
            inner: start_router(prepare_cassandra(config, store).await?).await?,
        })
    }
}

impl sealed::Router for LocalRouter {}
impl sealed::Router for GrpcRouter {}
impl<R: LocalRoute> sealed::Router for PeerRouter<R> {}

impl ResponseRoute for LocalResponseRoute {
    fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send {
        self.route.deliver(frame, deadline, context)
    }
}

impl PeerMetricSource for LocalResponseRoute {
    fn peer_metrics(&self) -> &PeerMetrics {
        self.route.peer_metrics()
    }
}

impl ResponseRoute for GrpcResponseRoute {
    fn deliver(
        &self,
        frame: Staged,
        deadline: RequestDeadline,
        context: &Context,
    ) -> impl Future<Output = Result<RouteOutcome, DropReason>> + Send {
        self.route.deliver(frame, deadline, context)
    }
}

impl PeerMetricSource for GrpcResponseRoute {
    fn peer_metrics(&self) -> &PeerMetrics {
        self.route.peer_metrics()
    }
}

impl Router for LocalRouter {
    type Response = LocalResponseRoute;

    fn producer(&self) -> ProducerHandle {
        self.inner.producer_handle()
    }

    fn response(&self) -> Self::Response {
        LocalResponseRoute {
            route: self.inner.route(),
        }
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.inner.shutdown_runtime().await
    }
}

impl Router for GrpcRouter {
    type Response = GrpcResponseRoute;

    fn producer(&self) -> ProducerHandle {
        self.inner.producer_handle()
    }

    fn response(&self) -> Self::Response {
        GrpcResponseRoute {
            route: self.inner.route(),
        }
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.inner.shutdown_runtime().await
    }
}

impl<R: LocalRoute> Router for PeerRouter<R> {
    type Response = R;

    fn producer(&self) -> ProducerHandle {
        self.producer_handle()
    }

    fn response(&self) -> Self::Response {
        self.route()
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.shutdown_runtime().await
    }
}
