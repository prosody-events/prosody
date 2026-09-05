//! Utility functions for environment variable parsing and handling.
//!
//! This module provides functions to parse environment variables into various
//! types, including vectors and durations. It also includes functions for
//! handling fallback values and optional durations.
//!
//! Throughout, a variable set to a blank value counts as unset. A deployment
//! that writes `FOO=` has not configured `FOO`, so it takes the same path as
//! one that never mentions it. Every other value the operator supplies is
//! parsed, and a value that fails to parse is an error — never a silent
//! substitution of the default.

/// Default capacity for idempotence caches (producer and consumer
/// deduplication).
///
/// Both sides read `PROSODY_IDEMPOTENCE_CACHE_SIZE` and share this default so
/// they stay in sync when the env var is unset.
pub const DEFAULT_IDEMPOTENCE_CACHE_SIZE: usize = 8192;

use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

/// A point where a process can stop between two durable steps.
///
/// Memory backends finish a durable step in one poll, so a poll-budget test
/// needs a yield to stop between steps. Production builds compile it out.
#[cfg(test)]
pub(crate) fn crash_point() -> impl Future<Output = ()> {
    use tokio::task::yield_now;
    yield_now()
}

#[cfg(not(test))]
pub(crate) fn crash_point() -> impl Future<Output = ()> {
    use std::future::ready;
    ready(())
}

/// Retrieves and parses an environment variable into the specified type.
///
/// # Errors
///
/// Returns an error if:
/// - The environment variable is not set
/// - The value cannot be parsed into the specified type
pub fn from_env<T>(env_var: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    let value_str = get_env_value(env_var)?;
    parse_with_error(env_var, &value_str)
}

/// Retrieves and parses an optional environment variable.
///
/// If the environment variable is not set, this function returns `Ok(None)`.
/// If it is set to "none" (case-insensitive), it also returns `Ok(None)`.
/// Otherwise, it attempts to parse the value into type `T`.
pub fn from_option_env<T>(env_var: &str) -> Result<Option<T>, String>
where
    T: FromStr<Err: Display>,
{
    let Some(value_str) = env_value(env_var) else {
        return Ok(None);
    };

    // Return None if the value is "none" (case-insensitive)
    if value_str.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_with_error(env_var, &value_str).map(Some)
}

/// Retrieves and parses an optional environment variable with a fallback
/// value. Returns `Some(fallback)` if the variable is unset, `None` if it is
/// set to "none" (case-insensitive), or the parsed value otherwise.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed
/// into the specified type.
pub fn from_option_env_with_fallback<T>(env_var: &str, fallback: T) -> Result<Option<T>, String>
where
    T: FromStr<Err: Display>,
{
    let Some(value_str) = env_value(env_var) else {
        return Ok(Some(fallback));
    };

    // Return None if the value is "none" (case-insensitive)
    if value_str.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    Ok(Some(parse_with_error(env_var, &value_str)?))
}

/// Retrieves and parses an environment variable with a fallback value.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed
/// into the specified type.
pub fn from_env_with_fallback<T>(env_var: &str, fallback: T) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    let Some(value_str) = env_value(env_var) else {
        return Ok(fallback);
    };

    parse_with_error(env_var, &value_str)
}

/// Retrieves and parses a comma-separated environment variable into a vector.
///
/// # Errors
///
/// Returns an error if:
/// - The environment variable is not set
/// - Any of the comma-separated values cannot be parsed into the specified type
pub fn from_vec_env<T>(env_var: &str) -> Result<Vec<T>, String>
where
    T: FromStr<Err: Display>,
{
    get_env_value(env_var)?
        .split(',')
        .map(|value_str| parse_with_error(env_var, value_str.trim()))
        .collect()
}

/// Retrieves and parses an optional comma-separated environment variable into a
/// vector.
///
/// If the environment variable is not set, this function returns `Ok(None)`.
/// Otherwise, it attempts to parse each comma-separated value into type `T`. If
/// any value fails to parse, an error is returned.
pub fn from_optional_vec_env<T>(env_var: &str) -> Result<Option<Vec<T>>, String>
where
    T: FromStr,
    <T as FromStr>::Err: Display,
{
    // Return Ok(None) if the environment variable is not set.
    let Some(value_str) = env_value(env_var) else {
        return Ok(None);
    };

    // Split on commas, trim each part, and parse each element.
    value_str
        .split(',')
        .map(|s| parse_with_error(env_var, s.trim()))
        .collect::<Result<Vec<T>, String>>()
        .map(Some)
}

/// Retrieves and parses a duration environment variable with a fallback value.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed as
/// a valid duration.
pub fn from_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Duration, String> {
    let Some(value_str) = env_value(env_var) else {
        return Ok(fallback);
    };

    parse_duration_with_error(env_var, &value_str)
}

/// Retrieves and parses an optional duration environment variable.
///
/// If the environment variable is not set, this function returns `Ok(None)`.
/// If it is set to "none" (case-insensitive), it also returns `Ok(None)`.
/// Otherwise, it attempts to parse the value as a duration using humantime.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed as
/// a valid duration (unless the value is "none", which returns `Ok(None)`).
pub fn from_option_duration_env(env_var: &str) -> Result<Option<Duration>, String> {
    let Some(value_str) = env_value(env_var) else {
        return Ok(None);
    };

    // Return None if the value is "none" (case-insensitive)
    if value_str.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_duration_with_error(env_var, &value_str).map(Some)
}

/// Retrieves and parses an optional duration environment variable with a
/// fallback value. Returns `Some(fallback)` if the variable is unset, `None`
/// if it is set to "none" (case-insensitive), or the parsed duration
/// otherwise.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed as
/// a valid duration.
pub fn from_option_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Option<Duration>, String> {
    let Some(value_str) = env_value(env_var) else {
        return Ok(Some(fallback));
    };

    // Return None if the value is "none" (case-insensitive)
    if value_str.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_duration_with_error(env_var, &value_str).map(Some)
}

/// Parses a string value into the specified type, providing a formatted error
/// message.
fn parse_with_error<T>(env_var: &str, value_str: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    value_str
        .parse()
        .map_err(|error| format!("failed to parse environment variable '${env_var}': {error:#}"))
}

/// The variable's value, or `None` when it is unset **or blank**.
///
/// A declared-but-empty variable — `FOO=` in a shell, or an env entry with an
/// empty value in a Kubernetes manifest — is how a deployment spells "not
/// configured". It takes the same path as an unset variable, so the caller
/// applies its default. The alternative is to hand `""` to the parser, which
/// rejects a value the operator never set.
fn env_value(env_var: &str) -> Option<String> {
    env::var(env_var)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// Retrieves the value of an environment variable that has no fallback.
fn get_env_value(env_var: &str) -> Result<String, String> {
    env_value(env_var).ok_or_else(|| {
        format!("value required and fallback environment variable '${env_var}' is not set")
    })
}

/// Parses a string value into a `Duration`, providing a formatted error
/// message.
fn parse_duration_with_error(env_var: &str, value_str: &str) -> Result<Duration, String> {
    match humantime::Duration::from_str(value_str) {
        Ok(duration) => Ok(duration.into()),
        Err(error) => Err(format!(
            "failed to parse environment variable '${env_var}': {error:#}"
        )),
    }
}
