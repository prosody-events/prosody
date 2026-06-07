use super::{StoreCreationError, validate_keyed_state_ttl};
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::timers::duration::CompactDuration;

/// `MAX_CASSANDRA_TTL_SECS` fits a `u32`, so the ceiling and one second past
/// it are both representable as a `CompactDuration`.
const CEILING_SECS: u32 = MAX_CASSANDRA_TTL_SECS as u32;

#[test]
fn indefinite_retention_is_allowed() {
    assert!(validate_keyed_state_ttl(None).is_ok());
}

#[test]
fn ttl_at_the_ceiling_is_allowed() {
    let ttl = CompactDuration::new(CEILING_SECS);
    assert!(validate_keyed_state_ttl(Some(ttl)).is_ok());
}

#[test]
fn ttl_over_the_ceiling_is_rejected() {
    let over = CEILING_SECS + 1;
    let error = validate_keyed_state_ttl(Some(CompactDuration::new(over)));
    assert!(matches!(
        error,
        Err(StoreCreationError::KeyedStateTtl(seconds)) if seconds == u64::from(over)
    ));
}
