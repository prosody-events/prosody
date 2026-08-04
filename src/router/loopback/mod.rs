//! Test doubles every suite that drives delivery shares: an in-process
//! transport, the routers over it, and the collected record of one run.
//!
//! A helper any two suites both need lives here rather than in either of them.

use crate::router::directory::{Endpoint, NetworkId, NodeRegistration};
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::grpc::health::ProcessHealth;
use crate::router::{
    Framed, Host, NodeId, RelayHop, ResponseSender, Route, Router, SendFailure, choose_route,
};
use bytes::BytesMut;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io::Error as IoError;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;

pub(crate) mod listener;

/// Port of the first test node. Each node binds one of its own, which is also
/// the transport's script key.
const PORT_BASE: u16 = 9000;

/// Port of the first advertised test endpoint.
const ADVERTISED_PORT_BASE: u16 = 9300;

/// How many nodes the test router publishes.
pub(crate) const PUBLISHED_NODES: u8 = 8;

/// A node the test router does not publish.
pub(crate) const UNPUBLISHED_NODE: u8 = 200;

/// A deadline on every wait, so a hang fails the test instead of hanging the
/// binary. It is never the assertion.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(30);

/// A health source whose two values a test sets directly.
pub(crate) struct TestHealth {
    ready: bool,
    live: bool,
}

/// One delivery attempt, as the transport saw it.
///
/// The frame bytes are copied out at the moment of the attempt. `at` is the
/// virtual instant a paused-time test advances to, and `deadline` is the
/// instant the caller gave this attempt to finish by.
#[derive(Debug)]
pub(crate) struct Delivery {
    pub(crate) port: u16,
    pub(crate) bytes: BytesMut,
    pub(crate) at: Instant,
    pub(crate) deadline: Instant,
}

/// What one sender or responder came to, once every delivery worker had
/// finished.
pub(crate) struct Drained {
    pub(crate) deliveries: Vec<Delivery>,
    pub(crate) sent: u64,
    pub(crate) dropped: u64,
}

/// What one destination answers.
pub(crate) enum Script {
    /// Answers `failure` for the next `times` attempts, then `Ok`.
    Fail {
        /// What each of those attempts gets.
        failure: SendFailure,
        /// How many attempts fail.
        times: usize,
    },

    /// Answers nothing until the barrier has a permit, then `Ok`. A test adds
    /// permits to release held attempts; a permit rather than a notification,
    /// so a release cannot be lost when no attempt is waiting yet.
    Hold(Arc<Semaphore>),
}

/// A transport that records every attempt and answers from a per-port script.
///
/// Ports key the scripts because a test node's endpoint is the only thing the
/// transport is given, and each test node binds a distinct port.
pub(crate) struct LoopbackSender {
    deliveries: UnboundedSender<Delivery>,
    /// A `Mutex<HashMap>` rather than `scc`: a script's read, decrement and
    /// answer must be one step, and this map holds a few ports in one test.
    /// The rule against a mutex-wrapped map targets contended keyed production
    /// state, which this is not.
    scripts: Mutex<HashMap<u16, Script>>,
}

/// A router over an in-process transport, addressing a fixed set of nodes.
///
/// Every suite that drives delivery builds one of these, so a router double
/// exists once rather than once per test tree.
#[derive(Clone)]
pub(crate) struct TestRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<LoopbackSender>,
    registrations: Arc<HashMap<NodeId, NodeRegistration>>,
    here: Option<NetworkId>,
}

/// What one attempt gets, once the script has been consulted.
enum Answer {
    Accepted,
    Failed(SendFailure),
    Held(Arc<Semaphore>),
}

impl TestHealth {
    /// A source that answers `ready` and `live`.
    pub(crate) const fn new(ready: bool, live: bool) -> Self {
        Self { ready, live }
    }
}

impl ProcessHealth for TestHealth {
    fn ready(&self) -> bool {
        self.ready
    }

    fn live(&self) -> bool {
        self.live
    }
}

impl LoopbackSender {
    /// A transport with no scripts, and the stream of what it records.
    pub(crate) fn new() -> (Self, UnboundedReceiver<Delivery>) {
        let (deliveries, recorded) = unbounded_channel();
        (
            Self {
                deliveries,
                scripts: Mutex::new(HashMap::new()),
            },
            recorded,
        )
    }

    /// Sets what the destination on `port` answers. An unscripted port accepts
    /// every attempt.
    pub(crate) fn script(&self, port: u16, script: Script) {
        drop(self.scripts.lock().insert(port, script));
    }

    fn answer(&self, port: u16) -> Answer {
        let mut scripts = self.scripts.lock();
        match scripts.get_mut(&port) {
            None => Answer::Accepted,
            Some(Script::Hold(barrier)) => Answer::Held(Arc::clone(barrier)),
            Some(Script::Fail { failure, times }) => {
                if *times == 0 {
                    Answer::Accepted
                } else {
                    *times -= 1;
                    Answer::Failed(*failure)
                }
            }
        }
    }
}

impl TestRouter {
    /// Builds the fleet, transport, published addresses, and delivery stream.
    pub(crate) fn new(
        config: FleetConfiguration,
    ) -> Result<(Self, UnboundedReceiver<Delivery>), FleetConfigurationError> {
        Self::build(config, None)
    }

    /// Builds a router whose nodes publish direct and advertised endpoints.
    pub(crate) fn dual_homed(
        config: FleetConfiguration,
    ) -> Result<(Self, UnboundedReceiver<Delivery>), FleetConfigurationError> {
        Self::build(config, Some(NetworkId::make("test")))
    }

    /// Builds the requested test endpoint shape.
    fn build(
        config: FleetConfiguration,
        here: Option<NetworkId>,
    ) -> Result<(Self, UnboundedReceiver<Delivery>), FleetConfigurationError> {
        let (transport, deliveries) = LoopbackSender::new();
        let registrations = (0..PUBLISHED_NODES)
            .map(|index| {
                (
                    node(index),
                    NodeRegistration {
                        node: node(index),
                        direct: Endpoint {
                            host: Host::make("10.0.0.1"),
                            port: port(index),
                        },
                        advertised: here.as_ref().map(|_| Endpoint {
                            host: Host::make("10.0.0.1"),
                            port: advertised_port(index),
                        }),
                        network: here.clone(),
                        group: None,
                        hostname: Host::make("test"),
                    },
                )
            })
            .collect();
        Ok((
            Self {
                fleet: Arc::new(DestinationFleet::new(config)?),
                transport: Arc::new(transport),
                registrations: Arc::new(registrations),
                here,
            },
            deliveries,
        ))
    }

    /// Sets what the destination for `index` answers on its direct endpoint.
    pub(crate) fn script(&self, index: u8, script: Script) {
        self.transport.script(port(index), script);
    }

    /// Sets what the destination for `index` answers on its advertised
    /// endpoint. Scripting both is what makes a route whose every candidate
    /// fails reachable.
    pub(crate) fn script_advertised(&self, index: u8, script: Script) {
        self.transport.script(advertised_port(index), script);
    }
}

impl Router for TestRouter {
    fn route(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Route>, Infallible>> + Send {
        let route = self
            .registrations
            .get(&node)
            .and_then(|registration| choose_route(self.here.as_ref(), registration));
        async move { Ok(route) }
    }
}

impl RelayHop for TestRouter {
    type Error = Infallible;
    type Sender = LoopbackSender;

    fn direct(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let direct = self
            .registrations
            .get(&node)
            .map(|registration| registration.direct.clone());
        async move { Ok(direct) }
    }

    fn sender(&self) -> &LoopbackSender {
        &self.transport
    }

    fn fleet(&self) -> &Arc<DestinationFleet> {
        &self.fleet
    }
}

impl ResponseSender for LoopbackSender {
    fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
        deadline: Instant,
    ) -> impl Future<Output = Result<(), SendFailure>> + Send {
        let port = address.port;
        let mut bytes = BytesMut::with_capacity(frame.bytes());
        frame.write(&mut bytes);
        let answer = self.answer(port);
        // The attempt is recorded before it is answered, so a held attempt is
        // observable while it is still held. A closed stream means the test
        // already ended, and the record is simply lost.
        drop(self.deliveries.send(Delivery {
            port,
            bytes,
            at: Instant::now(),
            deadline,
        }));
        async move {
            match answer {
                Answer::Accepted => Ok(()),
                Answer::Failed(failure) => Err(failure),
                Answer::Held(barrier) => match barrier.acquire().await {
                    Ok(permit) => {
                        drop(permit);
                        Ok(())
                    }
                    Err(_) => Err(SendFailure::Unreachable),
                },
            }
        }
    }
}

/// A registration publishing `direct` and nothing else.
pub(crate) fn registration(direct: Endpoint) -> NodeRegistration {
    NodeRegistration {
        node: NodeId::new(),
        direct,
        advertised: None,
        network: None,
        group: None,
        hostname: Host::make("test-node"),
    }
}

/// A node id from one repeated byte.
pub(crate) fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// The port that belongs to `index`.
pub(crate) fn port(index: u8) -> u16 {
    PORT_BASE + u16::from(index)
}

/// Returns the advertised port for `index`.
///
/// This range cannot overlap direct ports. Scripts use ports as keys, so tests
/// identify which endpoint received an attempt.
pub(crate) fn advertised_port(index: u8) -> u16 {
    ADVERTISED_PORT_BASE + u16::from(index)
}

/// Builds a current-thread runtime with paused time and the whole driver set.
///
/// The drivers are all enabled because some subjects driven here have I/O of
/// their own; paused time costs nothing to the rest.
pub(crate) fn paused() -> Result<Runtime, IoError> {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
}

/// Every attempt `deliveries` still holds, once nothing else can write to it.
///
/// The stream is closed first, so the collection ends at what the workers
/// recorded rather than waiting for a sender that will never write again.
pub(crate) async fn collect_deliveries(
    deliveries: &mut UnboundedReceiver<Delivery>,
) -> Vec<Delivery> {
    deliveries.close();
    let mut recorded = Vec::new();
    while let Some(delivery) = deliveries.recv().await {
        recorded.push(delivery);
    }
    recorded
}

/// Builds a fleet configuration for the requested capacity.
pub(crate) fn config(max_destinations: usize, slots_each: usize) -> FleetConfiguration {
    FleetConfiguration {
        max_destinations,
        slots_each,
        ..FleetConfiguration::default()
    }
}
