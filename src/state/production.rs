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
//! The exports here are:
//!
//! * [`capped_default_ttl`] — applies the Cassandra `USING TTL` ceiling
//!   (`630_720_000` seconds, ≈20 years) to a configured default TTL. Producers
//!   wire this once when they read `CassandraStore::base_ttl()`; the result
//!   feeds every store and wrapper that takes a `default_ttl:
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

use crate::timers::duration::CompactDuration;

/// Cassandra `USING TTL` ceiling, in seconds (≈20 years).
///
/// `USING TTL ?` rejects values above this limit; the production wiring
/// collapses any computed TTL that would exceed it to `None` so the
/// stores fall back to their `*_no_ttl` query variants.
pub const MAX_CASSANDRA_TTL_SECS: u32 = 630_720_000;

/// Caps a configured default TTL at the Cassandra `USING TTL` ceiling.
///
/// Returns `Some(base)` when the duration fits within
/// [`MAX_CASSANDRA_TTL_SECS`]; returns `None` when the duration would
/// overflow the ceiling. `None` signals "do not bind a TTL on writes",
/// which the keyed-state stores route through the `*_no_ttl` Cassandra
/// queries (an indefinite-retention fallback that is preferable to a
/// silent rejection).
#[must_use]
pub fn capped_default_ttl(base: CompactDuration) -> Option<CompactDuration> {
    (base.seconds() <= MAX_CASSANDRA_TTL_SECS).then_some(base)
}

#[cfg(test)]
mod tests {
    use super::{MAX_CASSANDRA_TTL_SECS, capped_default_ttl};
    use crate::timers::duration::CompactDuration;

    #[test]
    fn ttl_under_cap_is_passed_through() {
        let base = CompactDuration::new(60);
        assert_eq!(capped_default_ttl(base), Some(base));
    }

    #[test]
    fn ttl_at_cap_is_kept() {
        let base = CompactDuration::new(MAX_CASSANDRA_TTL_SECS);
        assert_eq!(capped_default_ttl(base), Some(base));
    }

    #[test]
    fn ttl_over_cap_collapses_to_none() {
        let base = CompactDuration::new(MAX_CASSANDRA_TTL_SECS + 1);
        assert_eq!(capped_default_ttl(base), None);
    }
}
