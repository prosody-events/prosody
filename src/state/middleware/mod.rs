//! Keyed-state middleware that wires handlers into the durable value bundle.
//!
//! This middleware is the runtime glue between user handlers and the keyed
//! state stack. It provides:
//!
//! * [`KeyedStateAccess`] — extension trait on
//!   [`EventContext`](crate::consumer::event_context::EventContext) that lets
//!   handlers call `ctx.value(name)` to operate on a Value collection.
//! * [`KeyedStateContext`] — wrapped context constructed per event; delegates
//!   `EventContext` calls to the inner context and exposes keyed-state access
//!   through [`KeyedStateAccess`].
//! * [`ValueHandle`] — the concrete handle returned by
//!   [`KeyedStateAccess::value`]; drives a
//!   [`TransactionValueStore`](crate::state::value::TransactionValueStore) per
//!   `(event, collection)`.
//! * [`KeyedStateMiddleware`] — the
//!   [`HandlerMiddleware`](crate::consumer::middleware::HandlerMiddleware)
//!   implementation that wraps the handler, drives
//!   [`CommitMode::Wal`](crate::state::CommitMode::Wal) seal + `StateRecovery`
//!   timer scheduling, [`CommitMode::Direct`](crate::state::CommitMode::Direct)
//!   direct apply, and the apply hook routing for `apply_sealed` /
//!   `rollback_sealed`.
//!
//! # Hook lifecycle
//!
//! For each user-handler dispatch in
//! [`CommitMode::Wal`](crate::state::CommitMode::Wal):
//!
//! 1. `on_message` / `on_timer` creates a fresh [`KeyedStateContext`] and
//!    invokes the inner handler.
//! 2. On `Ok`, the middleware seals every dirty collection captured by the
//!    context and schedules a single
//!    [`TimerType::StateRecovery`](crate::timers::TimerType::StateRecovery)
//!    timer if any collection was sealed. The seal results travel through
//!    [`KeyedStateOutput`] so the apply hooks can finalize them.
//! 3. On `Err`, the dirty workspace is dropped — nothing was sealed.
//! 4. `after_commit(Ok(_))` applies every sealed collection and clears the
//!    `StateRecovery` timer.
//! 5. `after_commit(Err(_))` / `after_abort` rolls every sealed collection back
//!    and clears the timer.
//!
//! For [`CommitMode::Direct`](crate::state::CommitMode::Direct) the middleware
//! skips the seal/recovery ceremony and calls `direct_apply` on every dirty
//! collection during `on_message` / `on_timer`. The recovery timer is **never**
//! scheduled in direct mode — that branch literally has no access to the
//! schedule helper.
//!
//! # `StateRecovery` timer
//!
//! When the recovery timer fires, the middleware streams `scan_pending`
//! over the `(segment, key)` partition. For each Value entry it consults
//! the oracle and dispatches to `apply_sealed` or `rollback_sealed`. Idle
//! partitions with a stale pending row are cleaned up via
//! [`PendingIndexStore::delete_pending`](crate::state::pending::PendingIndexStore::delete_pending).
//! Non-Value kinds are logged at
//! WARN and skipped; future kinds plug in by extending the dispatch
//! match.

mod context;
mod error;
mod handler;
mod registry;

#[cfg(test)]
mod tests;

pub use context::{
    DirtyValueBundle, DurableValueBundle, KeyedStateAccess, KeyedStateAccessError,
    KeyedStateContext, ValueAccessor, ValueHandle,
};
pub use error::{BoxedFactoryError, KeyedStateMiddlewareError, MiddlewareErrorComponent};
pub use handler::{
    KeyedStateHandler, KeyedStateMiddleware, KeyedStateMiddlewareBuildError,
    KeyedStateMiddlewareBuilder, KeyedStateOutput, KeyedStateProvider,
};
pub use registry::{CollectionDef, CollectionDefRegistry};
