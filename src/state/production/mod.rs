//! Production composition helpers for the keyed-state stack.
//!
//! This module provides small wiring utilities that the typical
//! production composition needs. It stays a thin layer over the concrete
//! types in [`crate::state::cassandra`], [`crate::state::fjall`],
//! [`crate::state::layered`], and [`crate::state::recovering`].
//!
//! Three exports, two of them a pair:
//!
//! * [`ProductionValueDurable`] names the canonical durable bundle shape,
//!   `Layered<FjallValueStore, Recovering<CassandraValueStore, O>>`. The
//!   `Recovering` layer is baked into the alias, so the production bundle
//!   cannot even be *named* without it.
//! * [`compose_value_durable`] is the only sanctioned way to build that bundle.
//!   The `Recovering` layer is load-bearing for crash safety — omitting it
//!   reopens the lost-commit data-loss window — so funneling construction
//!   through one composer keeps the layer from being silently dropped.
//! * [`capped_default_ttl`] applies the Cassandra `USING TTL` ceiling
//!   ([`MAX_CASSANDRA_TTL_SECS`]) to a configured default TTL. Producers wire
//!   this once when they read `CassandraStore::base_ttl()`; the result feeds
//!   every store and wrapper that takes a `default_ttl:
//!   Option<CompactDuration>`.
//!
//! See the [design summary][summary] for the full canonical composition:
//! `Layered<FjallValueStore, Recovering<CassandraValueStore,
//! CommitManager>>` as the Value durable bundle,
//! `CassandraValueStore` as the [`PendingIndexScanner`],
//! `CommitManager` as the [`CommitOracle`], and
//! `FjallDirtyValueStoreFactory` as the dirty-store factory.
//!
//! [summary]: ../../docs/keyed-state/design-summary.md
//! [`PendingIndexScanner`]: super::pending::PendingIndexScanner
//! [`CommitOracle`]: super::oracle::CommitOracle

use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::state::cassandra::CassandraValueStore;
use crate::state::fjall::FjallValueStore;
use crate::state::layered::LayeredValueStore;
use crate::state::middleware::CollectionDefRegistry;
use crate::state::recovering::RecoveringValueStore;
use crate::timers::duration::CompactDuration;
use std::sync::Arc;

/// The canonical production Value durable bundle:
/// `Layered<FjallValueStore, Recovering<CassandraValueStore, O>>`, with the
/// shared [`Arc<CollectionDefRegistry>`] as the recovery-write TTL resolver.
///
/// `O` is the [`CommitOracle`](crate::state::oracle::CommitOracle) — in
/// production [`CommitManager`](crate::commit_manager::CommitManager).
pub type ProductionValueDurable<O> = LayeredValueStore<
    FjallValueStore,
    RecoveringValueStore<CassandraValueStore, O, Arc<CollectionDefRegistry>>,
>;

/// Composes the canonical production Value durable bundle.
///
/// Wraps the authoritative [`CassandraValueStore`] in
/// [`RecoveringValueStore`] — so both the read-before-use (`get`) and
/// recover-before-overwrite (`seal`) recovery paths are always present — and
/// fronts it with the [`FjallValueStore`] cache. This is the one sanctioned
/// constructor for the bundle, and its return type — [`ProductionValueDurable`]
/// — bakes `Recovering` into the type, so the production bundle cannot be named
/// without it. Routing every wiring through here keeps the `Recovering` layer
/// from being silently dropped, which would reopen the lost-commit data-loss
/// window.
///
/// `registry` must be the **same** [`Arc<CollectionDefRegistry>`] passed to
/// [`KeyedStateMiddlewareBuilder::registry`](crate::state::middleware::KeyedStateMiddlewareBuilder::registry),
/// so first-touch recovery (driven by the wrapper's resolver) and the
/// timer-sweep recovery (driven by the middleware registry) bind identical
/// per-collection TTLs.
#[must_use]
pub fn compose_value_durable<O>(
    cache: FjallValueStore,
    backing: CassandraValueStore,
    oracle: O,
    registry: Arc<CollectionDefRegistry>,
) -> ProductionValueDurable<O> {
    LayeredValueStore::new(cache, RecoveringValueStore::new(backing, oracle, registry))
}

/// Caps a configured default TTL at the Cassandra `USING TTL` ceiling.
///
/// Returns `Some(base)` when the duration fits within
/// [`MAX_CASSANDRA_TTL_SECS`]; returns `None` when the
/// duration would overflow the ceiling. `None` signals "do not bind a TTL
/// on writes", which the keyed-state stores route through the `*_no_ttl`
/// Cassandra queries (an indefinite-retention fallback that is preferable
/// to a silent rejection).
#[must_use]
pub fn capped_default_ttl(base: CompactDuration) -> Option<CompactDuration> {
    (i64::from(base.seconds()) <= MAX_CASSANDRA_TTL_SECS).then_some(base)
}

#[cfg(test)]
mod tests;
