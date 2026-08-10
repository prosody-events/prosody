//! Concrete router types selected by client backends.

use super::PeerConfiguration;
use super::backend::prepare_cassandra;
use super::runtime::{LocalRoute, PeerRouter, ProducerHandle, start_local_router, start_router};
use crate::cassandra::{CassandraConfiguration, CassandraStore};
use crate::consumer::{ConsumerError, PeerInitError, ShutdownError};
use crate::response::frame::encode::Staged;
use crate::response::headers::RequestDeadline;
use crate::response::sender::{DropReason, ResponseRoute, RouteOutcome, Then};
use crate::router::directory::cassandra::CassandraNodeDirectory;
use crate::router::grpc::client::GrpcSender;
use crate::router::{LocalTarget, NetworkRoute};
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
    type Response: ResponseRoute;

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
    inner: PeerRouter<Then<LocalTarget, NetworkRoute<GrpcSender, CassandraNodeDirectory>>>,
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
    route: Then<LocalTarget, NetworkRoute<GrpcSender, CassandraNodeDirectory>>,
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
