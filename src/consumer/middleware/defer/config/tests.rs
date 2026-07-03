use super::*;

#[test]
fn test_default_configuration() {
    let config = DeferConfiguration::builder().build();
    assert!(config.is_ok());
}

#[test]
fn test_custom_configuration() {
    let config = DeferConfiguration::builder()
        .base(Duration::from_mins(2))
        .max_delay(Duration::from_hours(1))
        .failure_threshold(0.8_f64)
        .failure_window(Duration::from_mins(10))
        .store_cache_size(5_000_usize)
        .build();

    assert!(config.is_ok());
}

#[test]
fn test_max_delay_less_than_base_fails() {
    let result = DeferConfiguration::builder()
        .base(Duration::from_mins(2))
        .max_delay(Duration::from_mins(1))
        .build();

    assert!(result.is_err());
}

#[test]
fn test_base_delay_too_small_fails() {
    let result = DeferConfiguration::builder()
        .base(Duration::from_millis(500))
        .build();

    assert!(result.is_err());
}

#[test]
fn test_failure_threshold_out_of_range_fails() {
    let result = DeferConfiguration::builder()
        .failure_threshold(1.5_f64)
        .build();

    assert!(result.is_err());
}

#[test]
fn test_store_cache_size_zero_fails() {
    let result = DeferConfiguration::builder()
        .store_cache_size(0_usize)
        .build();

    assert!(result.is_err());
}

#[test]
fn test_default_store_is_in_memory() {
    let result = DeferConfiguration::builder().build();
    assert!(result.is_ok());
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
