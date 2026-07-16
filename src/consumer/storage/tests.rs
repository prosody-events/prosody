use super::{StoreCreationError, dedup_ttl_seconds};
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use std::time::Duration;

/// `MAX_CASSANDRA_TTL_SECS` fits a `u32`, so the ceiling and one second past
/// it are both representable as a `u32`/`Duration`.
const CEILING_SECS: u32 = MAX_CASSANDRA_TTL_SECS as u32;

#[test]
fn dedup_ttl_at_the_ceiling_is_allowed() {
    let seconds = dedup_ttl_seconds(Duration::from_secs(u64::from(CEILING_SECS)));
    assert!(matches!(seconds, Ok(s) if i64::from(s) == MAX_CASSANDRA_TTL_SECS));
}

#[test]
fn dedup_ttl_over_the_ceiling_is_rejected() {
    let over = u64::from(CEILING_SECS) + 1;
    let error = dedup_ttl_seconds(Duration::from_secs(over));
    assert!(matches!(
        error,
        Err(StoreCreationError::DeduplicationTtl(seconds)) if seconds == over
    ));
}
