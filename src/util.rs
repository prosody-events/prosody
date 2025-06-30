//! Utility functions for environment variable parsing and handling.
//!
//! This module provides functions to parse environment variables into various
//! types, including vectors and durations. It also includes functions for
//! handling fallback values and optional durations.

use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

/// Retrieves and parses an environment variable into the specified type.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
///
/// # Returns
///
/// A `Result` containing the parsed value or an error message.
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
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
///
/// # Returns
///
/// A `Result` containing an `Option<T>`:
/// - `Ok(Some(value))` if the variable is set and parsed successfully.
/// - `Ok(None)` if the variable is not set or is set to "none".
/// - `Err(String)` if parsing fails.
pub fn from_option_env<T>(env_var: &str) -> Result<Option<T>, String>
where
    T: FromStr<Err: Display>,
{
    let Ok(value_str) = env::var(env_var) else {
        return Ok(None);
    };

    // Return None if the value is "none" (case-insensitive)
    if value_str.trim().eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_with_error(env_var, &value_str).map(Some)
}

/// Retrieves and parses an optional environment variable with a fallback value.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
/// * `fallback` - The fallback value to use if the environment variable is not
///   set.
///
/// # Returns
///
/// A `Result` containing an `Option<T>` with the parsed value, fallback, or
/// `None`.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed
/// into the specified type.
pub fn from_option_env_with_fallback<T>(env_var: &str, fallback: T) -> Result<Option<T>, String>
where
    T: FromStr<Err: Display>,
{
    let Ok(value_str) = env::var(env_var) else {
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
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
/// * `fallback` - The fallback value to use if the environment variable is not
///   set.
///
/// # Returns
///
/// A `Result` containing the parsed value (or fallback) or an error message.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed
/// into the specified type.
pub fn from_env_with_fallback<T>(env_var: &str, fallback: T) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    let Ok(value_str) = env::var(env_var) else {
        return Ok(fallback);
    };

    parse_with_error(env_var, &value_str)
}

/// Retrieves and parses a comma-separated environment variable into a vector.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
///
/// # Returns
///
/// A `Result` containing a vector of parsed values or an error message.
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
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
///
/// # Returns
///
/// A `Result` containing an `Option<Vec<T>>`:
/// - `Ok(Some(vec))` if the variable is set and all values are parsed
///   successfully.
/// - `Ok(None)` if the variable is not set.
/// - `Err(String)` if any parsing error occurs.
pub fn from_optional_vec_env<T>(env_var: &str) -> Result<Option<Vec<T>>, String>
where
    T: FromStr,
    <T as FromStr>::Err: Display,
{
    // Return Ok(None) if the environment variable is not set.
    let Ok(value_str) = env::var(env_var) else {
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
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
/// * `fallback` - The fallback duration to use if the environment variable is
///   not set.
///
/// # Returns
///
/// A `Result` containing the parsed duration (or fallback) or an error message.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed as
/// a valid duration.
pub fn from_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Duration, String> {
    let Ok(value_str) = env::var(env_var) else {
        return Ok(fallback);
    };

    parse_duration_with_error(env_var, &value_str)
}

/// Retrieves and parses an optional duration environment variable with a
/// fallback value.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
/// * `fallback` - The fallback duration to use if the environment variable is
///   not set.
///
/// # Returns
///
/// A `Result` containing an `Option<Duration>` or an error message.
///
/// # Errors
///
/// Returns an error if the environment variable is set but cannot be parsed as
/// a valid duration (unless the value is "none", which returns `Ok(None)`).
pub fn from_option_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Option<Duration>, String> {
    let Ok(value_str) = env::var(env_var) else {
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
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable (for error reporting).
/// * `value_str` - The string value to parse.
///
/// # Returns
///
/// A `Result` containing the parsed value or a formatted error message.
///
/// # Errors
///
/// Returns an error if the string value cannot be parsed into the specified
/// type.
fn parse_with_error<T>(env_var: &str, value_str: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    value_str
        .parse()
        .map_err(|error| format!("failed to parse environment variable '${env_var}': {error:#}"))
}

/// Retrieves the value of an environment variable.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable to retrieve.
///
/// # Returns
///
/// A `Result` containing the environment variable value or an error message.
///
/// # Errors
///
/// Returns an error if the environment variable is not set.
fn get_env_value(env_var: &str) -> Result<String, String> {
    env::var(env_var).map_err(|_| {
        format!("value required and fallback environment variable '${env_var}' is not set")
    })
}

/// Parses a string value into a `Duration`, providing a formatted error
/// message.
///
/// # Arguments
///
/// * `env_var` - The name of the environment variable (for error reporting).
/// * `value_str` - The string value to parse.
///
/// # Returns
///
/// A `Result` containing the parsed `Duration` or a formatted error message.
///
/// # Errors
///
/// Returns an error if the string value cannot be parsed as a valid duration.
fn parse_duration_with_error(env_var: &str, value_str: &str) -> Result<Duration, String> {
    match humantime::Duration::from_str(value_str) {
        Ok(duration) => Ok(duration.into()),
        Err(error) => Err(format!(
            "failed to parse environment variable '${env_var}': {error:#}"
        )),
    }
}
