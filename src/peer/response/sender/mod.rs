//! Typed response delivery.

mod metrics;
mod route;

pub(crate) use self::metrics::DropReason;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use route::Delivery as RouteDelivery;
pub use route::{PeerMetricSource, ResponseRoute};
pub(crate) use route::{RouteOutcome, Then};
pub(crate) use route::{deliver_response, stage};
