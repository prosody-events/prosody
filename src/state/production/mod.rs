//! Production composition helpers for the keyed-state stack.
//!
//! This module provides small wiring utilities that the typical
//! production composition needs. It deliberately stays a thin layer over
//! the concrete types in [`crate::state::cassandra`],
//! [`crate::state::fjall`], [`crate::state::layered`], and
//! [`crate::state::recovering`] — assembling them in user code is
//! preferred, because every consumer has slightly different durability,
//! retention, and caching trade-offs.
//!
//! The export here is [`capped_default_ttl`], which applies the Cassandra
//! `USING TTL` ceiling ([`crate::cassandra::MAX_CASSANDRA_TTL_SECS`]) to a
//! configured default TTL. Producers wire this once when they read
//! `CassandraStore::base_ttl()`; the result feeds every store and wrapper
//! that takes a `default_ttl: Option<CompactDuration>`.
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
use crate::timers::duration::CompactDuration;

/// Caps a configured default TTL at the Cassandra `USING TTL` ceiling.
///
/// Returns `Some(base)` when the duration fits within
/// [`crate::cassandra::MAX_CASSANDRA_TTL_SECS`]; returns `None` when the
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
