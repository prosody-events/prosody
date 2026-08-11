//! Test doubles every suite that drives delivery shares: an in-process
//! transport, the routers over it, and the collected record of one run.
//!
//! A helper any two suites both need lives here rather than in either of them.

use crate::router::directory::{Endpoint, NetworkId, PeerRegistration};
use crate::router::fleet::config::{FleetConfiguration, FleetConfigurationError};
use crate::router::fleet::{Destination, DestinationFleet};
use crate::router::{
    Framed, Host, NetworkRouter, PeerId, RelayHop, ResponseSender, Route, SendFailure, choose_route,
};
use bytes::BytesMut;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io::Error as IoError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;
use tonic::codegen::http::{Uri, uri::InvalidUri};

pub(crate) mod listener;

/// How many peers the test router publishes.
pub(crate) const PUBLISHED_PEERS: u8 = 8;

/// A peer the test router does not publish.
pub(crate) const UNPUBLISHED_PEER: u8 = 200;

/// A deadline on every wait, so a hang fails the test instead of hanging the
/// binary. It is never the assertion.
pub(crate) const HANG_GUARD: Duration = Duration::from_secs(30);

/// One delivery attempt, as the transport saw it.
///
/// The frame bytes are copied out at the moment of the attempt.
#[derive(Debug)]
pub(crate) struct Delivery {
    pub(crate) uri: Uri,
    pub(crate) bytes: BytesMut,
}

/// What one sender or responder came to after all sends finished.
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

/// A transport that records every attempt and answers from an endpoint script.
pub(crate) struct LoopbackSender {
    deliveries: UnboundedSender<Delivery>,
    /// A `Mutex<HashMap>` rather than `scc`: a script's read, decrement and
    /// answer must be one step, and this map holds a few URIs in one test.
    /// The rule against a mutex-wrapped map targets contended keyed production
    /// state, which this is not.
    scripts: Mutex<HashMap<Uri, Script>>,
}

/// A router over an in-process transport, addressing a fixed set of peers.
///
/// Every suite that drives delivery builds one of these, so a router double
/// exists once rather than once per test tree.
#[derive(Clone)]
pub(crate) struct TestRouter {
    fleet: Arc<DestinationFleet>,
    transport: Arc<LoopbackSender>,
    registrations: Arc<HashMap<PeerId, Arc<PeerRegistration>>>,
    here: Option<NetworkId>,
}

/// What one attempt gets, once the script has been consulted.
enum Answer {
    Accepted,
    Failed(SendFailure),
    Held(Arc<Semaphore>),
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

    /// Sets what one endpoint answers. An unscripted endpoint accepts.
    pub(crate) fn script(&self, uri: Uri, script: Script) {
        drop(self.scripts.lock().insert(uri, script));
    }

    fn answer(&self, uri: &Uri) -> Answer {
        let mut scripts = self.scripts.lock();
        match scripts.get_mut(uri) {
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
    ) -> Result<(Self, UnboundedReceiver<Delivery>), TestRouterError> {
        Self::build(config, None)
    }

    /// Builds a router whose peers publish direct and advertised endpoints.
    pub(crate) fn dual_homed(
        config: FleetConfiguration,
    ) -> Result<(Self, UnboundedReceiver<Delivery>), TestRouterError> {
        Self::build(config, Some(NetworkId::make("test")))
    }

    /// Builds the requested test endpoint shape.
    fn build(
        config: FleetConfiguration,
        here: Option<NetworkId>,
    ) -> Result<(Self, UnboundedReceiver<Delivery>), TestRouterError> {
        let (transport, deliveries) = LoopbackSender::new();
        let registrations = (0..PUBLISHED_PEERS)
            .map(|index| {
                Ok((
                    peer(index),
                    Arc::new(PeerRegistration {
                        peer: peer(index),
                        direct: Endpoint::from(direct_uri(index)?),
                        advertised: here
                            .as_ref()
                            .map(|_| advertised_uri(index).map(Endpoint::from))
                            .transpose()?,
                        network: here.clone(),
                        hostname: Host::make("test"),
                    }),
                ))
            })
            .collect::<Result<_, InvalidUri>>()?;
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
    pub(crate) fn script(&self, index: u8, script: Script) -> Result<(), TestRouterError> {
        self.transport.script(direct_uri(index)?, script);
        Ok(())
    }

    /// Sets what the destination for `index` answers on its advertised
    /// endpoint. Scripting both is what makes a route whose every candidate
    /// fails reachable.
    pub(crate) fn script_advertised(
        &self,
        index: u8,
        script: Script,
    ) -> Result<(), TestRouterError> {
        self.transport.script(advertised_uri(index)?, script);
        Ok(())
    }
}

impl NetworkRouter for TestRouter {
    fn destination(&self, peer: PeerId) -> Arc<Destination> {
        self.fleet.destination(peer)
    }

    fn route(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Option<Route>, Infallible>> + Send {
        let route = self
            .registrations
            .get(&peer)
            .and_then(|registration| choose_route(self.here.as_ref(), Arc::clone(registration)));
        async move { Ok(route) }
    }
}

impl RelayHop for TestRouter {
    type Error = Infallible;
    type Sender = LoopbackSender;

    fn direct(
        &self,
        peer: PeerId,
    ) -> impl Future<Output = Result<Option<Endpoint>, Infallible>> + Send {
        let direct = self
            .registrations
            .get(&peer)
            .map(|registration| registration.direct.clone());
        async move { Ok(direct) }
    }

    fn sender(&self) -> &LoopbackSender {
        &self.transport
    }
}

impl ResponseSender for LoopbackSender {
    fn deliver<F: Framed + Sync>(
        &self,
        address: &Endpoint,
        frame: &F,
        _deadline: Instant,
        _context: &opentelemetry::Context,
    ) -> impl Future<Output = Result<(), SendFailure>> + Send {
        let uri = address.uri().clone();
        let mut bytes = BytesMut::with_capacity(frame.bytes());
        frame.write(&mut bytes);
        let answer = self.answer(&uri);
        // The attempt is recorded before it is answered, so a held attempt is
        // observable while it is still held. A closed stream means the test
        // already ended, and the record is simply lost.
        drop(self.deliveries.send(Delivery { uri, bytes }));
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
pub(crate) fn registration(direct: Endpoint) -> PeerRegistration {
    PeerRegistration {
        peer: PeerId::new(),
        direct,
        advertised: None,
        network: None,
        hostname: Host::make("test-peer"),
    }
}

/// A peer id from one repeated byte.
pub(crate) fn peer(index: u8) -> PeerId {
    PeerId::from_bytes([index; 16])
}

/// The direct URI that belongs to `index`.
pub(crate) fn direct_uri(index: u8) -> Result<Uri, InvalidUri> {
    format!("http://direct-{index}.test").parse()
}

/// The advertised URI that belongs to `index`.
pub(crate) fn advertised_uri(index: u8) -> Result<Uri, InvalidUri> {
    format!("http://advertised-{index}.test").parse()
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
/// The stream is closed first, so the collection ends at what the sender
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

/// Builds the response delivery configuration.
pub(crate) fn config() -> FleetConfiguration {
    FleetConfiguration::default()
}

/// What can stop shared router test scaffolding from starting.
#[derive(Debug, Error)]
pub(crate) enum TestRouterError {
    /// The fleet configuration is invalid.
    #[error(transparent)]
    Fleet(#[from] FleetConfigurationError),
    /// A test endpoint is not a valid Tonic endpoint.
    #[error(transparent)]
    Endpoint(#[from] InvalidUri),
}
