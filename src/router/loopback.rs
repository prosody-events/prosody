//! An in-process transport, so delivery can be driven without a socket.

use crate::router::directory::Endpoint;
use crate::router::fleet::DestinationFleet;
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::grpc::health::ProcessHealth;
use crate::router::{Framed, Host, NodeId, ResponseSender, Router, SendFailure};
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

/// Port of the first test node. Each node binds one of its own, which is also
/// the transport's script key.
const PORT_BASE: u16 = 9000;

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
/// The frame bytes are copied out at the moment of the attempt, and the instant
/// is the virtual one a paused-time test advances, so the record answers both
/// "what went where" and "when".
#[derive(Debug)]
pub(crate) struct Delivery {
    pub(crate) port: u16,
    pub(crate) bytes: BytesMut,
    pub(crate) at: Instant,
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
    addresses: Arc<HashMap<NodeId, Endpoint>>,
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
        let (transport, deliveries) = LoopbackSender::new();
        let addresses = (0..PUBLISHED_NODES)
            .map(|index| {
                (
                    node(index),
                    Endpoint {
                        host: Host::make("10.0.0.1"),
                        port: port(index),
                    },
                )
            })
            .collect();
        Ok((
            Self {
                fleet: Arc::new(DestinationFleet::new(config)?),
                transport: Arc::new(transport),
                addresses: Arc::new(addresses),
            },
            deliveries,
        ))
    }

    /// Sets what the destination for `index` answers.
    pub(crate) fn script(&self, index: u8, script: Script) {
        self.transport.script(port(index), script);
    }
}

impl Router for TestRouter {
    type Error = Infallible;
    type Sender = LoopbackSender;

    fn address(
        &self,
        node: NodeId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let address = self.addresses.get(&node).cloned();
        async move { Ok(address) }
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

/// A node id from one repeated byte.
pub(crate) fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// The port that belongs to `index`.
pub(crate) fn port(index: u8) -> u16 {
    PORT_BASE + u16::from(index)
}

/// Builds a current-thread runtime with paused time.
pub(crate) fn paused() -> Result<Runtime, IoError> {
    Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
}

/// Builds a fleet configuration for the requested capacity.
pub(crate) fn config(max_destinations: usize, slots_each: usize) -> FleetConfiguration {
    FleetConfiguration {
        max_destinations,
        slots_each,
        ..FleetConfiguration::default()
    }
}
