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
