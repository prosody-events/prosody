//! Benchmark settings, read from `PROSODY_BENCH_*` environment variables.
//!
//! The consumer itself still reads every `PROSODY_*` variable through its
//! builders. Set `PROSODY_MAX_UNCOMMITTED` or `PROSODY_POLL_INTERVAL` to sweep
//! a consumer setting without a code change.

use crate::layers::Layer;
use color_eyre::eyre::{Result, WrapErr};
use std::env;
use std::error::Error;
use std::str::FromStr;
use std::time::Duration;

/// Mean record sizes of production event topics (bytes): small events, typical
/// events, listing documents, and a large stress case.
const DEFAULT_SIZES: &[usize] = &[400, 1_000, 6_000, 65_536];

/// One run of the benchmark matrix: every layer against every size.
#[derive(Debug)]
pub(crate) struct Settings {
    /// Kafka bootstrap servers.
    pub(crate) bootstrap: Vec<String>,
    /// Cassandra contact points for the high-level layers.
    pub(crate) cassandra: Vec<String>,
    /// The upper limit of messages loaded for each size.
    pub(crate) messages: u32,
    /// The upper limit of bytes loaded for each size. Large sizes load fewer
    /// messages so that every size takes about the same time.
    pub(crate) max_bytes: u64,
    /// The serialized JSON size of each message, one topic per size.
    pub(crate) sizes: Vec<usize>,
    /// The partition count of each topic.
    pub(crate) partitions: u16,
    /// The count of distinct message keys.
    pub(crate) keys: u32,
    /// The consumer layers to measure, in order.
    pub(crate) layers: Vec<Layer>,
    /// A sleep in each handler call to simulate handler I/O.
    pub(crate) handler_delay: Duration,
    /// The hang-guard for one layer to consume one topic.
    pub(crate) timeout: Duration,
}

impl Settings {
    /// Reads the settings from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if a variable is set but cannot be parsed.
    pub(crate) fn from_env() -> Result<Self> {
        Ok(Self {
            bootstrap: list(
                "PROSODY_BOOTSTRAP_SERVERS",
                vec!["localhost:9094".to_owned()],
            )?,
            cassandra: list("PROSODY_CASSANDRA_NODES", vec!["localhost:9042".to_owned()])?,
            messages: value("PROSODY_BENCH_MESSAGES", 100_000)?,
            max_bytes: value("PROSODY_BENCH_MAX_BYTES", 256 << 20)?,
            sizes: list("PROSODY_BENCH_SIZES", DEFAULT_SIZES.to_vec())?,
            partitions: value("PROSODY_BENCH_PARTITIONS", 64)?,
            keys: value("PROSODY_BENCH_KEYS", 100_000)?,
            layers: list("PROSODY_BENCH_LAYERS", Layer::ALL.to_vec())?,
            handler_delay: duration("PROSODY_BENCH_HANDLER_DELAY", Duration::ZERO)?,
            timeout: duration("PROSODY_BENCH_TIMEOUT", Duration::from_mins(10))?,
        })
    }

    /// The message count for one size: the message limit or the byte limit,
    /// whichever is lower, and at least one message.
    #[must_use]
    pub(crate) fn messages_for(&self, size: usize) -> u32 {
        let by_bytes = u32::try_from(self.max_bytes / size.max(1) as u64).unwrap_or(u32::MAX);
        by_bytes.min(self.messages).max(1)
    }
}

fn value<T>(name: &str, default: T) -> Result<T>
where
    T: FromStr<Err: Error + Send + Sync + 'static>,
{
    env::var(name).map_or(Ok(default), |raw| {
        raw.parse()
            .wrap_err_with(|| format!("invalid {name}: {raw}"))
    })
}

fn list<T>(name: &str, default: Vec<T>) -> Result<Vec<T>>
where
    T: FromStr<Err: Error + Send + Sync + 'static>,
{
    let Ok(raw) = env::var(name) else {
        return Ok(default);
    };

    raw.split(',')
        .map(|item| {
            item.trim()
                .parse()
                .wrap_err_with(|| format!("invalid {name} item: {item}"))
        })
        .collect()
}

fn duration(name: &str, default: Duration) -> Result<Duration> {
    env::var(name).map_or(Ok(default), |raw| {
        humantime::parse_duration(&raw).wrap_err_with(|| format!("invalid {name}: {raw}"))
    })
}
