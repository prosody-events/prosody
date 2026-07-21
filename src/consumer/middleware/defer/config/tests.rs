use super::*;
use color_eyre::eyre::{Result, bail};

/// Asserts `result` failed validation with an error attributed to `field`
/// (the `validator` derive keys schema-level errors under `__all__` and
/// field-level errors under the field name).
fn expect_validation_field(
    result: Result<DeferConfiguration, DeferConfigError>,
    field: &str,
) -> Result<()> {
    match result {
        Err(DeferConfigError::Validation(errors)) if errors.field_errors().contains_key(field) => {
            Ok(())
        }
        Err(DeferConfigError::Validation(errors)) => {
            bail!("expected a validation error on `{field}`, got {errors:?}")
        }
        Err(other) => bail!("expected a validation error, got {other:?}"),
        Ok(_) => bail!("expected build to fail validation"),
    }
}

#[test]
fn test_custom_configuration() -> Result<()> {
    let config = DeferConfiguration::builder()
        .base(Duration::from_mins(2))
        .max_delay(Duration::from_hours(1))
        .failure_threshold(0.8_f64)
        .failure_window(Duration::from_mins(10))
        .store_cache_size(5_000_usize)
        .build()?;

    assert_eq!(config.base, Duration::from_mins(2));
    assert_eq!(config.max_delay, Duration::from_hours(1));
    assert!((config.failure_threshold - 0.8_f64).abs() < f64::EPSILON);
    assert_eq!(config.failure_window, Duration::from_mins(10));
    assert_eq!(config.store_cache_size, 5_000_usize);
    Ok(())
}

#[test]
fn test_max_delay_less_than_base_fails() -> Result<()> {
    let result = DeferConfiguration::builder()
        .base(Duration::from_mins(2))
        .max_delay(Duration::from_mins(1))
        .build();

    expect_validation_field(result, "__all__")
}

#[test]
fn test_base_delay_too_small_fails() -> Result<()> {
    let result = DeferConfiguration::builder()
        .base(Duration::from_millis(500))
        .build();

    expect_validation_field(result, "base")
}

#[test]
fn test_failure_threshold_out_of_range_fails() -> Result<()> {
    let result = DeferConfiguration::builder()
        .failure_threshold(1.5_f64)
        .build();

    expect_validation_field(result, "failure_threshold")
}

#[test]
fn test_store_cache_size_zero_fails() -> Result<()> {
    let result = DeferConfiguration::builder()
        .store_cache_size(0_usize)
        .build();

    expect_validation_field(result, "store_cache_size")
}

#[test]
fn test_enabled_defaults_to_true() {
    let config = DeferConfiguration::builder().build();
    assert!(config.is_ok());
    assert!(config.as_ref().is_ok_and(|c| c.enabled));
}

#[test]
fn test_enabled_can_be_disabled() {
    let config = DeferConfiguration::builder().enabled(false).build();
    assert!(config.is_ok());
    assert!(config.as_ref().is_ok_and(|c| !c.enabled));
}
