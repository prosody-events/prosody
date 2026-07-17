use super::KeyedStateConfiguration;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::codec::JsonCodec;
use crate::state::descriptor::{StateDescriptor, ValueDescriptor, map_state, value_state};
use crate::state::order_codec::I64KeyCodec;
use crate::state::registry::RegisterStateError;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::Result;
use parking_lot::{Mutex, MutexGuard};
use quickcheck::{QuickCheck, TestResult};
use std::env;
use std::num::NonZeroU64;
use std::path::PathBuf;
use validator::Validate;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// Serializes the env-mutating tests. Under `cargo nextest` (the mandated
/// runner) each test is its own process, so the mutation is invisible to other
/// tests; this lock only guards the theoretical single-process `cargo test`
/// path against these tests clobbering *each other*.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard: sets (or removes) [`super::FJALL_CACHE_SIZE_ENV`] under
/// [`ENV_LOCK`] and restores the prior value on drop (including on panic).
struct CacheSizeEnvGuard {
    _lock: MutexGuard<'static, ()>,
    prior: Option<String>,
}

impl CacheSizeEnvGuard {
    #[expect(
        clippy::manual_ok_err,
        reason = "CLAUDE.md bans `.ok()` (it silently swallows a `Result`); the explicit match \
                  documents that VarError::NotPresent is the expected unset case, not a discarded \
                  error"
    )]
    fn set(value: Option<&str>) -> Self {
        let lock = ENV_LOCK.lock();
        let prior = match env::var(super::FJALL_CACHE_SIZE_ENV) {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        apply_cache_size_env(value);
        Self { _lock: lock, prior }
    }
}

impl Drop for CacheSizeEnvGuard {
    fn drop(&mut self) {
        apply_cache_size_env(self.prior.as_deref());
    }
}

fn apply_cache_size_env(value: Option<&str>) {
    // SAFETY: the caller holds ENV_LOCK, so no other thread in this process
    // mutates this variable concurrently; under nextest the process is this
    // test's alone.
    #[allow(
        unsafe_code,
        reason = "std::env::{set_var,remove_var} require unsafe since Rust 2024; ENV_LOCK \
                  serializes access, matching the sole other sanctioned unsafe use in \
                  src/consumer/decode.rs"
    )]
    unsafe {
        match value {
            Some(v) => env::set_var(super::FJALL_CACHE_SIZE_ENV, v),
            None => env::remove_var(super::FJALL_CACHE_SIZE_ENV),
        }
    }
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

/// A zero `recovery_delay` is rejected: a zero delay would fire the sweep
/// immediately, leaving no window for the fast post-commit path.
#[test]
fn zero_recovery_delay_is_rejected() -> Result<()> {
    let config = KeyedStateConfiguration::builder()
        .recovery_delay(CompactDuration::new(0))
        .build()?;
    assert!(
        config.validate().is_err(),
        "a zero recovery_delay must fail validation"
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

/// Omitted programmatic (setter never called) AND env unset ⇒ `None`.
#[test]
fn cache_size_defaults_to_none_when_env_unset() -> Result<()> {
    let _guard = CacheSizeEnvGuard::set(None);
    let config = KeyedStateConfiguration::builder().build()?;
    assert_eq!(
        config.cache_size_bytes, None,
        "omitted cache_size_bytes with the env unset must default to None"
    );
    Ok(())
}

/// A valid `PROSODY_FJALL_CACHE_SIZE_BYTES` parses into `Some(bytes)`.
#[test]
fn cache_size_env_parses_valid_value() -> Result<()> {
    let _guard = CacheSizeEnvGuard::set(Some("8388608")); // 8 MiB
    let config = KeyedStateConfiguration::builder().build()?;
    assert_eq!(
        config.cache_size_bytes,
        NonZeroU64::new(8_388_608),
        "a valid PROSODY_FJALL_CACHE_SIZE_BYTES must parse into Some(bytes)"
    );
    Ok(())
}

/// Zero, negative, non-numeric, empty, and out-of-`u64` values are all
/// rejected at build.
#[test]
fn cache_size_env_rejects_degenerate_values() {
    for bad in ["0", "-5", "abc", "", "99999999999999999999999999"] {
        let _guard = CacheSizeEnvGuard::set(Some(bad));
        assert!(
            KeyedStateConfiguration::builder().build().is_err(),
            "PROSODY_FJALL_CACHE_SIZE_BYTES = {bad:?} must be rejected at build"
        );
    }
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
