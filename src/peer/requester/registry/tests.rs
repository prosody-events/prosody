//! Test-only registration ownership for router integration tests.

use super::{FrameReceiver, INLINE_AWAITED, PendingRegistry, Registration};
use crate::peer::response::RequestId;
use crate::peer::response::headers::RequestDeadline;
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct TestRegistration {
    registration: Registration,
    receivers: SmallVec<[FrameReceiver; INLINE_AWAITED]>,
}

pub(crate) fn pending_len(registry: &PendingRegistry) -> usize {
    registry.senders.len()
}

impl TestRegistration {
    pub(crate) fn new(
        registry: &Arc<PendingRegistry>,
        subsystems: &[SubsystemName],
        timeout: Duration,
    ) -> Result<Self> {
        let deadline = RequestDeadline::after(timeout)
            .ok_or_else(|| eyre!("the test deadline was out of range"))?;
        let mut registration = registry.register::<Infallible>(subsystems, deadline)?;
        let receivers = registration.take_receivers();
        Ok(Self {
            registration,
            receivers,
        })
    }

    pub(crate) const fn id(&self) -> RequestId {
        self.registration.id()
    }

    pub(crate) fn receiver(&mut self) -> Result<FrameReceiver> {
        self.receivers
            .pop()
            .ok_or_else(|| eyre!("the registration had no response receiver"))
    }

    pub(crate) fn received(&mut self) -> bool {
        self.receivers
            .first_mut()
            .is_some_and(|receiver| receiver.try_recv().is_ok())
    }
}
