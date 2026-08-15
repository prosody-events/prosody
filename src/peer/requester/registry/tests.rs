//! Test-only registration ownership for router integration tests.

use super::{FrameReceiver, FrameReceivers, PendingRegistry, PendingRequest};
use crate::peer::response::RequestId;
use crate::peer::response::headers::RequestDeadline;
use crate::subsystem::SubsystemName;
use color_eyre::Result;
use color_eyre::eyre::eyre;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct TestRegistration {
    pending: PendingRequest,
    receivers: FrameReceivers,
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
        let registration = registry.register::<Infallible>(subsystems, deadline)?;
        let (pending, receivers) = registration.into_parts();
        Ok(Self { pending, receivers })
    }

    pub(crate) const fn id(&self) -> RequestId {
        self.pending.id
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
