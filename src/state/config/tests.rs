use super::KeyedStateConfiguration;
use crate::ByteSize;
use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
use crate::codec::JsonCodec;
use crate::state::descriptor::{StateDescriptor, ValueDescriptor, map_state, value_state};
use crate::state::order_codec::I64KeyCodec;
use crate::state::registry::RegisterStateError;
use crate::subsystem::SubsystemName;
use crate::timers::duration::CompactDuration;
use color_eyre::eyre::Result;
use quickcheck::{QuickCheck, TestResult};
use std::path::PathBuf;
use validator::Validate;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

const _: fn(&KeyedStateConfiguration) -> &Option<ByteSize> = |c| &c.owned_cache_size;
const _: fn(&KeyedStateConfiguration) -> &Option<ByteSize> = |c| &c.read_cache_size;

#[test]
fn cache_sizes_parse_human_units_and_reject_degenerate_values() -> Result<()> {
    let parsed: ByteSize = "8 MiB"
        .parse()
        .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
    assert_eq!(parsed.get(), 8_388_608, "a valid byte count parses");
    for bad in ["0", "-5", "abc", "", "99999999999999999999999999"] {
        assert!(
            bad.parse::<ByteSize>().is_err(),
            "cache size {bad:?} must be rejected",
        );
    }
    Ok(())
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

/// A zero read-cache TTL would make every cached entry born stale. Validation
/// rejects it here, mirroring the check applied when the reader is constructed.
/// A sub-millisecond TTL is valid — reader age is measured against a
/// nanosecond-resolution monotonic clock — and so are longer TTLs and the
/// explicit `None` opt-out.
#[test]
fn zero_read_cache_ttl_is_rejected() -> Result<()> {
    use std::time::Duration;
    let degenerate = KeyedStateConfiguration::builder()
        .read_cache_ttl(Some(Duration::ZERO))
        .build()?;
    assert!(
        degenerate.validate().is_err(),
        "a zero read_cache_ttl must fail validation"
    );
    for ttl in [Duration::from_micros(500), Duration::from_millis(1)] {
        let valid = KeyedStateConfiguration::builder()
            .read_cache_ttl(Some(ttl))
            .build()?;
        valid.validate()?;
    }
    let disabled = KeyedStateConfiguration::builder()
        .read_cache_ttl(None)
        .build()?;
    disabled.validate()?;
    Ok(())
}

/// Registration accepts no expiry or a TTL that binds within the Cassandra
/// range.
#[test]
fn prop_collection_ttl_registration() {
    fn prop(seconds: u32, nanos: u32) -> TestResult {
        let check = || -> Result<()> {
            use std::time::Duration;

            let samples = [
                None,
                Some(CompactDuration::MIN),
                Some(Duration::from_nanos(u64::from(nanos % 500_000_000)).try_into()?),
                Some(Duration::from_millis(999).try_into()?),
                Some(CompactDuration::new(1)),
                Some(CompactDuration::new(60)),
                Some(CompactDuration::new(CEILING_SECS)),
                Some(CompactDuration::new(CEILING_SECS + 1)),
                Some(CompactDuration::new(seconds)),
            ];
            for ttl in samples {
                let mut config = KeyedStateConfiguration::builder().build()?;
                let descriptor = ttl.map_or_else(cart, |ttl| cart().ttl(ttl));
                let _ = config.register(descriptor);
                let result = config.build_registry();
                match ttl {
                    Some(ttl) if ttl.is_zero() || ttl.seconds() > CEILING_SECS => {
                        assert!(
                            matches!(result, Err(RegisterStateError::Ttl { seconds, .. })
                            if seconds == ttl.seconds()),
                            "TTL {ttl:?}: {result:?}"
                        );
                    }
                    _ => {
                        result?;
                    }
                }
            }
            Ok(())
        };
        match check() {
            Ok(()) => TestResult::passed(),
            Err(error) => TestResult::error(error.to_string()),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(u32, u32) -> TestResult);
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

/// Registration succeeds unless a `Published` collection is declared with no
/// configured subsystem, in which case build fails `PublishedWithoutSubsystem`.
/// Covers the full published × subsystem matrix.
#[test]
fn prop_published_requires_subsystem() {
    fn prop(published: bool, with_subsystem: bool) -> TestResult {
        let build = || -> Result<()> {
            let mut builder = KeyedStateConfiguration::builder();
            if with_subsystem {
                builder.subsystem(Some(SubsystemName::try_new("orders")?));
            }
            let mut config = builder.build()?;
            let _ = config.register(cart().published(published));
            match config.build_registry() {
                Ok(_) if published && !with_subsystem => {
                    color_eyre::eyre::bail!(
                        "published={published} with_subsystem={with_subsystem}: \
                         published-without-subsystem must be rejected"
                    )
                }
                Ok(_) => Ok(()),
                Err(RegisterStateError::PublishedWithoutSubsystem { .. })
                    if published && !with_subsystem =>
                {
                    Ok(())
                }
                Err(e) => color_eyre::eyre::bail!(
                    "published={published} with_subsystem={with_subsystem}: unexpected \
                     build_registry error: {e}"
                ),
            }
        };
        match build() {
            Ok(()) => TestResult::passed(),
            Err(e) => TestResult::error(e.to_string()),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(bool, bool) -> TestResult);
}
