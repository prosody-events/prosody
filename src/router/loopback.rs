//! An in-process transport, so delivery can be driven without a socket.

use crate::router::directory::Endpoint;
use crate::router::{Framed, ResponseSender, SendFailure};
use bytes::BytesMut;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;

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
