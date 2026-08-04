//! What the sender does with the status a real listener answered.

use super::{ALPHA, GrpcSender, Harness, header, payload, register};
use crate::codec::Codec;
use crate::response::frame::tests::CountingCodec;
use crate::response::sender::TypedSender;
use crate::router::directory::{Endpoint, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::grpc::TRANSPORT;
use crate::router::grpc::client::{DELIVER_RESPONSE, peer_uri};
use crate::router::grpc::generated::peer_server::SERVICE_NAME;
use crate::router::loopback::config;
use crate::router::{Host, NodeId, Route, Router, choose_route};
use crate::test_util::TEST_RUNTIME;
use crate::tracing::init_test_logging;
use color_eyre::Result;
use color_eyre::eyre::{ensure, eyre};
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tonic::transport::Endpoint as Dialled;

/// Destinations the retry pin's fleet holds: this node and the one a stale
/// entry points elsewhere.
const DESTINATIONS: usize = 2;

/// Responses one destination may hold at once.
const SLOTS: usize = 2;

/// A short payload, for the cases whose size is not the subject.
const SHORT: usize = 8;

/// Every node resolves to the one listener under test.
///
/// That is not a contrivance: it is exactly what a stale directory entry looks
/// like, which is the case the misrouted arm exists for.
#[derive(Clone)]
struct OneListener {
    fleet: Arc<DestinationFleet>,
    transport: Arc<GrpcSender>,
    registration: NodeRegistration,
}

impl Router for OneListener {
    type Error = Infallible;
    type Sender = GrpcSender;

    fn route(
        &self,
        _node: NodeId,
    ) -> impl Future<Output = Result<Option<Route>, Infallible>> + Send {
        let route = choose_route(None, &self.registration);
        async move { Ok(route) }
    }

    fn direct(
        &self,
        _node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let direct = self.registration.direct.clone();
        async move { Ok(Some(direct)) }
    }

    fn sender(&self) -> &GrpcSender {
        &self.transport
    }

    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }
}

/// Every host a node can publish makes a URI the dialer parses.
///
/// An IPv6 literal is the case that needs the brackets: unbracketed, its own
/// colons split the authority, nothing parses it, and every response to that
/// node is reported unreachable. A routed probe on an IPv6 host publishes
/// exactly such a literal.
#[test]
fn every_published_host_makes_a_dialable_uri() -> Result<()> {
    for host in ["127.0.0.1", "fd00::5", "::1", "peer.example"] {
        let uri = peer_uri(&Endpoint {
            host: Host::make(host),
            port: 8080,
        });
        Dialled::from_shared(uri.clone())
            .map_err(|error| eyre!("{host} produced {uri}, which does not parse: {error}"))?;
    }
    Ok(())
}

/// The path the client calls names the generated service, so a renamed proto
/// cannot leave the client misrouting quietly.
#[test]
fn the_method_path_names_the_generated_service() -> Result<()> {
    ensure!(
        DELIVER_RESPONSE.as_str() == format!("/{SERVICE_NAME}/DeliverResponse"),
        "the client calls {}, which is not the generated service's method",
        DELIVER_RESPONSE.as_str()
    );
    Ok(())
}

/// A status the destination answered decides whether the response is tried
/// again: `NOT_FOUND` is the destination's own answer and is never repeated,
/// while `UNAVAILABLE` may or may not have landed and is.
///
/// The count is taken at the service, after the network, so no client-side
/// bookkeeping can stand in for it, and it is read after `drain`, which awaits
/// every worker.
#[test]
fn a_terminal_status_is_attempted_once_and_an_ambiguous_one_is_retried() -> Result<()> {
    init_test_logging();
    TEST_RUNTIME.block_on(async {
        let harness = Harness::shared().await?;
        let fleet = Arc::new(DestinationFleet::new(config(DESTINATIONS, SLOTS))?);
        let router = OneListener {
            transport: Arc::new(GrpcSender::new(harness.cap, &fleet)),
            fleet,
            registration: NodeRegistration {
                node: NodeId::new(),
                direct: harness.address.clone(),
                advertised: None,
                network: None,
                group: None,
                hostname: Host::make("one-listener"),
            },
        };
        let attempts = router.fleet().config().max_send_attempts;

        // Nothing is registered under this id, so the node answers NOT_FOUND.
        let terminal = TypedSender::<CountingCodec>::new(&router, harness.cap)?;
        let served = TRANSPORT.served();
        let unregistered = register(&harness.oracle, &[ALPHA], CountingCodec::FORMAT_ID)?;
        terminal
            .send(header(harness.node, unregistered, ALPHA)?, payload(SHORT))
            .map_err(|_| eyre!("the fleet refused a slot"))?;
        terminal.drain().await;
        ensure!(
            TRANSPORT.served() == served + 1,
            "a terminal status must be attempted exactly once"
        );

        // Addressed to another node, so this one answers UNAVAILABLE.
        let ambiguous = TypedSender::<CountingCodec>::new(&router, harness.cap)?;
        let served = TRANSPORT.served();
        ambiguous
            .send(header(NodeId::new(), unregistered, ALPHA)?, payload(SHORT))
            .map_err(|_| eyre!("the fleet refused a slot"))?;
        ambiguous.drain().await;
        ensure!(
            TRANSPORT.served() == served + u64::from(attempts),
            "an ambiguous status must be attempted {attempts} times"
        );
        Ok(())
    })
}
