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
