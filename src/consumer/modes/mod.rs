//! The four ways a consumer is constructed, one submodule per strategy.
//!
//! `direct` dispatches straight to the handler with no middleware. `pipeline`
//! retries and defers. `low_latency` routes exhausted failures to a topic.
//! `best_effort` logs them and moves on.
//!
//! Each constructor is an inherent method on [`ProsodyConsumer`], defined in
//! the module that owns its wiring. `clippy::multiple_inherent_impl` fires on
//! inherent impls sharing a self type across files, so one module-level
//! expectation covers the whole subtree.
//!
//! [`ProsodyConsumer`]: crate::consumer::ProsodyConsumer

#![expect(
    clippy::multiple_inherent_impl,
    reason = "one impl per mode module keeps each constructor beside its wiring"
)]

mod best_effort;
mod direct;
mod low_latency;
mod pipeline;

use crate::Codec;
use crate::consumer::decode::{IgnoreRequests, ResultRequestReader};
use crate::consumer::middleware::providers::{FallibleCloneProvider, LeafHandler};
use crate::consumer::middleware::respond::{RespondHandler, Responder};
use crate::consumer::middleware::{FallibleHandler, FallibleHandlerProvider, SettlementHandler};
use crate::peer::Router;
use crate::subsystem::SubsystemName;
use std::marker::PhantomData;
use std::sync::Arc;

/// Disables peer responses for one consumer.
pub(crate) struct NoResponses;

/// Enables peer responses for one subsystem.
pub(crate) struct Responding<'a, C, R> {
    router: &'a R,
    subsystem: SubsystemName,
    codec: PhantomData<fn() -> C>,
}

/// Builds one consumer's response-specific provider and result-request reader.
pub(crate) trait ResponsePolicy<H>
where
    H: FallibleHandler + Clone,
{
    type Provider: FallibleHandlerProvider<
        Handler: FallibleHandler<Payload = H::Payload> + SettlementHandler,
    >;
    type Requests: ResultRequestReader + 'static;

    fn request_subsystem(&self) -> Option<&SubsystemName>;
    fn into_parts(self, handler: H) -> (Self::Provider, Self::Requests);
}

impl<H> ResponsePolicy<H> for NoResponses
where
    H: FallibleHandler + Clone,
{
    type Provider = FallibleCloneProvider<LeafHandler<H>>;
    type Requests = IgnoreRequests;

    fn request_subsystem(&self) -> Option<&SubsystemName> {
        None
    }

    fn into_parts(self, handler: H) -> (Self::Provider, Self::Requests) {
        (
            FallibleCloneProvider::new(LeafHandler::new(handler)),
            IgnoreRequests,
        )
    }
}

impl<'a, C, R> Responding<'a, C, R> {
    pub(crate) const fn new(router: &'a R, subsystem: SubsystemName) -> Self {
        Self {
            router,
            subsystem,
            codec: PhantomData,
        }
    }
}

impl<C, R, H> ResponsePolicy<H> for Responding<'_, C, R>
where
    C: Codec<Payload = H::Output>,
    R: Router,
    H: FallibleHandler + Clone,
    H::Output: Sync + 'static,
    H::Error: Sync + 'static,
{
    type Provider = FallibleCloneProvider<RespondHandler<LeafHandler<H>, C, R::Response>>;
    type Requests = SubsystemName;

    fn request_subsystem(&self) -> Option<&SubsystemName> {
        Some(&self.subsystem)
    }

    fn into_parts(self, handler: H) -> (Self::Provider, Self::Requests) {
        let responder = Arc::new(Responder::new(
            self.router.response(),
            self.subsystem.clone(),
        ));
        (
            FallibleCloneProvider::new(RespondHandler::new(LeafHandler::new(handler), responder)),
            self.subsystem,
        )
    }
}
