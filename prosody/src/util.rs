use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

pub fn from_env<T>(env_var: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    let value_str = get_env_value(env_var)?;
    parse_with_error(env_var, &value_str)
}

pub fn from_env_with_fallback<T>(env_var: &str, fallback: T) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    let Ok(value_str) = env::var(env_var) else {
        return Ok(fallback);
    };

    parse_with_error(env_var, &value_str)
}

pub fn from_vec_env<T>(env_var: &str) -> Result<Vec<T>, String>
where
    T: FromStr<Err: Display>,
{
    get_env_value(env_var)?
        .split(',')
        .map(|value_str| parse_with_error(env_var, value_str))
        .collect()
}

pub fn from_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Duration, String> {
    let Ok(value_str) = env::var(env_var) else {
        return Ok(fallback);
    };

    parse_duration_with_error(env_var, &value_str)
}

pub fn from_option_duration_env_with_fallback(
    env_var: &str,
    fallback: Duration,
) -> Result<Option<Duration>, String> {
    let Ok(value_str) = env::var(env_var) else {
        return Ok(Some(fallback));
    };

    if value_str.eq_ignore_ascii_case("none") {
        return Ok(None);
    }

    parse_duration_with_error(env_var, &value_str).map(Some)
}

fn parse_with_error<T>(env_var: &str, value_str: &str) -> Result<T, String>
where
    T: FromStr<Err: Display>,
{
    value_str
        .parse()
        .map_err(|error| format!("failed to parse environment variable '${env_var}': {error:#}"))
}

fn get_env_value(env_var: &str) -> Result<String, String> {
    env::var(env_var).map_err(|_| format!("environment variable '${env_var}' not set"))
}

fn parse_duration_with_error(env_var: &str, value_str: &str) -> Result<Duration, String> {
    match humantime::Duration::from_str(value_str) {
        Ok(duration) => Ok(duration.into()),
        Err(error) => Err(format!(
            "failed to parse environment variable '${env_var}': {error:#}"
        )),
    }
}
