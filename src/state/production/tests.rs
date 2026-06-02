use super::capped_default_ttl;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::timers::duration::CompactDuration;

#[test]
fn ttl_under_cap_is_passed_through() {
    let base = CompactDuration::new(60);
    assert_eq!(capped_default_ttl(base), Some(base));
}

#[test]
fn ttl_at_cap_is_kept() -> color_eyre::Result<()> {
    let base = CompactDuration::new(u32::try_from(MAX_CASSANDRA_TTL_SECS)?);
    assert_eq!(capped_default_ttl(base), Some(base));
    Ok(())
}

#[test]
fn ttl_over_cap_collapses_to_none() -> color_eyre::Result<()> {
    let base = CompactDuration::new(u32::try_from(MAX_CASSANDRA_TTL_SECS)? + 1);
    assert_eq!(capped_default_ttl(base), None);
    Ok(())
}
