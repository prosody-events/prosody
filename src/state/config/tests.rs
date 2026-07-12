use super::KeyedStateConfiguration;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::codec::JsonCodec;
use crate::state::descriptor::{StateDescriptor, ValueDescriptor, map_state, value_state};
use crate::state::order_codec::I64KeyCodec;
use crate::state::registry::RegisterStateError;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
use std::path::PathBuf;
use validator::Validate;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// `MAX_CASSANDRA_TTL_SECS` fits a `u32`, so the ceiling and one second
/// past it are both representable as a `CompactDuration`.
const CEILING_SECS: u32 = MAX_CASSANDRA_TTL_SECS as u32;

#[test]
fn empty_cache_dir_is_rejected() -> Result<()> {
    let config = KeyedStateConfiguration::builder()
        .cache_dir(PathBuf::new())
        .build()?;
    assert!(
        config.validate().is_err(),
        "empty cache_dir must fail validation"
    );
    Ok(())
}

/// Indefinite retention (`None`) is always allowed — the TTL ceiling
/// guards only oversized `Some` values, never the opt-out.
#[test]
fn collection_ttl_none_is_allowed() -> Result<()> {
    let mut config = KeyedStateConfiguration::builder().build()?;
    let _ = config.register(cart());
    assert!(config.build_registry().is_ok());
    Ok(())
}

#[test]
fn collection_ttl_at_the_ceiling_is_allowed() -> Result<()> {
    let ttl = CompactDuration::new(CEILING_SECS);
    let mut config = KeyedStateConfiguration::builder().build()?;
    let _ = config.register(cart().ttl(ttl));
    assert!(config.build_registry().is_ok());
    Ok(())
}

#[test]
fn collection_ttl_over_the_ceiling_is_rejected() -> Result<()> {
    let over = CEILING_SECS + 1;
    let mut config = KeyedStateConfiguration::builder().build()?;
    let _ = config.register(cart().ttl(CompactDuration::new(over)));
    assert!(matches!(
        config.build_registry(),
        Err(RegisterStateError::Ttl { seconds, .. }) if seconds == over
    ));
    Ok(())
}

/// A TTL equal to the recovery delay is rejected: the cell must *outlive*
/// the sweep, so the floor is strict (`>`), not `>=`.
#[test]
fn collection_ttl_at_the_recovery_delay_is_rejected() -> Result<()> {
    let delay = CompactDuration::new(60);
    let mut config = KeyedStateConfiguration::builder()
        .recovery_delay(delay)
        .build()?;
    let _ = config.register(cart().ttl(delay));
    assert!(matches!(
        config.build_registry(),
        Err(RegisterStateError::TtlBelowRecoveryDelay {
            ttl_seconds,
            recovery_seconds,
            ..
        }) if ttl_seconds == 60 && recovery_seconds == 60
    ));
    Ok(())
}

/// A TTL one second above the recovery delay clears the floor.
#[test]
fn collection_ttl_above_the_recovery_delay_is_allowed() -> Result<()> {
    let mut config = KeyedStateConfiguration::builder()
        .recovery_delay(CompactDuration::new(60))
        .build()?;
    let _ = config.register(cart().ttl(CompactDuration::new(61)));
    assert!(config.build_registry().is_ok());
    Ok(())
}

/// Round-trip: whatever `recovery_within` a descriptor is built with is
/// exactly what `recovery_within_for` reads back after registration (unset ⇒
/// `None`). Proves the fluent config reaches the registry unaltered and that
/// the bound needs no validation (`build_registry` accepts any duration,
/// since it is tightening-only against the recovery-delay floor).
#[test]
fn prop_recovery_within_round_trips_through_the_registry() {
    fn prop(within: Option<u32>) -> TestResult {
        let bound = within.map(CompactDuration::new);
        let mut descriptor = cart();
        if let Some(d) = bound {
            descriptor = descriptor.recovery_within(d);
        }

        match round_trip(descriptor) {
            Ok(read) if read == bound => TestResult::passed(),
            Ok(read) => TestResult::error(format!("expected {bound:?}, read {read:?}")),
            Err(e) => TestResult::error(format!("registry build failed: {e}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(Option<u32>) -> TestResult);
}

/// A Map keyset limit over the `4096` ceiling is rejected at build; the
/// ceiling itself and `0` (tracking disabled) both register cleanly.
#[test]
fn keyset_limit_over_ceiling_is_rejected() -> Result<()> {
    let mut config = KeyedStateConfiguration::builder().build()?;
    let _ = config.register(map_state::<I64KeyCodec, JsonCodec>("m").keyset_limit(4097));
    assert!(matches!(
        config.build_registry(),
        Err(RegisterStateError::KeysetLimit { limit: 4097, .. })
    ));

    let mut ok = KeyedStateConfiguration::builder().build()?;
    let _ = ok.register(map_state::<I64KeyCodec, JsonCodec>("m4096").keyset_limit(4096));
    let _ = ok.register(map_state::<I64KeyCodec, JsonCodec>("m0").keyset_limit(0));
    assert!(
        ok.build_registry().is_ok(),
        "the ceiling and 0 both register cleanly"
    );
    Ok(())
}

/// Registers `descriptor` and reads its `recovery_within` back from the
/// built registry.
fn round_trip(descriptor: ValueDescriptor) -> Result<Option<CompactDuration>> {
    use crate::state::{StateName, StateType};

    let mut config = KeyedStateConfiguration::builder().build()?;
    let _ = config.register(descriptor);
    Ok(config
        .build_registry()?
        .recovery_within_for(StateType::Application, &StateName::try_new("cart")?))
}
