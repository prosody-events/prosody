//! Test-only registration ownership for router integration tests.

use super::{PendingRegistry, Registration, Waiter};
use crate::response::RequestId;
use crate::response::frame::ResponseFrame;
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

pub(crate) struct TestRegistration {
    registration: Registration,
    waiters: SmallVec<[Waiter; 2]>,
}

pub(crate) fn pending_len(registry: &PendingRegistry) -> usize {
    registry.waiters.len()
}

impl TestRegistration {
    pub(crate) fn new(
        registry: &Arc<PendingRegistry>,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<Self> {
        let mut registration = registry.register::<Infallible>(subsystems, timeout)?;
        let waiters = registration.take_waiters();
        Ok(Self {
            registration,
            waiters,
        })
    }

    pub(crate) const fn id(&self) -> RequestId {
        self.registration.id()
    }

    pub(crate) fn receiver(&mut self) -> Result<oneshot::Receiver<ResponseFrame>> {
        self.waiters
            .pop()
            .ok_or_else(|| eyre!("the registration had no waiter"))
    }

    pub(crate) fn received(&mut self) -> bool {
        self.waiters
            .first_mut()
            .is_some_and(|waiter| waiter.try_recv().is_ok())
    }
}
