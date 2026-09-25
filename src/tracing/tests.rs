use super::{DEFAULT_LOG_DIRECTIVES, flush_telemetry, log_filter, shutdown_telemetry};
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;
use tracing::level_filters::LevelFilter;
use tracing::subscriber::with_default;
use tracing::{Level, enabled};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

/// Targets that a generated `PROSODY_LOG` directive can name. `None` is a
/// bare level.
const OVERRIDE_TARGETS: [Option<&str>; 5] = [
    None,
    Some("scylla"),
    Some("opentelemetry"),
    Some("opentelemetry_sdk"),
    Some("prosody"),
];

/// Levels that a generated directive can set. `None` is an invalid level.
const OVERRIDE_LEVELS: [Option<LevelFilter>; 7] = [
    Some(LevelFilter::OFF),
    Some(LevelFilter::ERROR),
    Some(LevelFilter::WARN),
    Some(LevelFilter::INFO),
    Some(LevelFilter::DEBUG),
    Some(LevelFilter::TRACE),
    None,
];

/// Targets that the property checks, in the order of [`probe_all`].
/// `rdkafka` has no default and no generated directive.
const PROBE_TARGETS: [&str; 6] = [
    "scylla",
    "opentelemetry",
    "opentelemetry_sdk",
    "opentelemetry-otlp",
    "prosody",
    "rdkafka",
];

/// Event levels that the property checks, in the order of [`probe`].
const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];

/// One generated `PROSODY_LOG` directive.
#[derive(Clone, Debug)]
struct Override {
    target: Option<&'static str>,
    level: Option<LevelFilter>,
}

impl Override {
    fn render(&self) -> String {
        let level = self
            .level
            .map_or_else(|| "loud".to_owned(), |level| level.to_string());
        match self.target {
            None => level,
            Some(target) => format!("{target}={level}"),
        }
    }
}

impl Arbitrary for Override {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            target: *g.choose(&OVERRIDE_TARGETS).unwrap_or(&None),
            level: *g.choose(&OVERRIDE_LEVELS).unwrap_or(&None),
        }
    }
}

/// Reports, for each level in [`LEVELS`], whether the current subscriber
/// enables an event for `$target`.
///
/// A callsite fixes its target and level at compile time. So each pair needs
/// its own `enabled!` call.
macro_rules! probe {
    ($target:literal) => {
        [
            enabled!(target: $target, Level::ERROR),
            enabled!(target: $target, Level::WARN),
            enabled!(target: $target, Level::INFO),
            enabled!(target: $target, Level::DEBUG),
            enabled!(target: $target, Level::TRACE),
        ]
    };
}

/// A `PROSODY_LOG` directive replaces the default for its own target, and a
/// bare level replaces the `info` default. Defaults for other targets stay,
/// and invalid directives change nothing.
///
/// The model keeps one level for each target. The defaults go in first. Then
/// each valid override replaces the entry for its target. A probe target
/// gets the level of the longest entry that is a prefix of it.
#[quickcheck]
fn prop_prosody_log_replaces_defaults_per_target(overrides: Vec<Override>) -> bool {
    let mut model: BTreeMap<Option<&str>, LevelFilter> = DEFAULT_LOG_DIRECTIVES
        .split(',')
        .map(default_directive)
        .collect();
    let mut rendered = Vec::with_capacity(overrides.len());
    for directive in overrides {
        rendered.push(directive.render());
        if let Some(level) = directive.level {
            model.insert(directive.target, level);
        }
    }

    let expected = PROBE_TARGETS.map(|probe| {
        let level = model
            .iter()
            .filter(|(target, _)| target.is_none_or(|target| probe.starts_with(target)))
            .max_by_key(|(target, _)| target.map_or(0, str::len))
            .map_or(LevelFilter::OFF, |(_, level)| *level);
        LEVELS.map(|event| event <= level)
    });

    let filter = log_filter(&rendered.join(","));
    let actual = with_default(Registry::default().with(filter), probe_all);

    actual == expected
}

/// FFI clients call flush and shutdown unconditionally on dispose or process
/// exit, even when [`super::initialize_tracing`] was never called. Both must
/// succeed as no-ops in that case.
///
/// This test only holds while no other test in this binary calls
/// `initialize_tracing`. The global tracing subscriber can be set once per
/// process.
#[test]
fn flush_and_shutdown_are_noops_when_uninitialized() -> Result<()> {
    flush_telemetry()?;
    shutdown_telemetry()?;
    Ok(())
}

/// Probes every target in [`PROBE_TARGETS`] under the current subscriber.
fn probe_all() -> [[bool; 5]; 6] {
    [
        probe!("scylla"),
        probe!("opentelemetry"),
        probe!("opentelemetry_sdk"),
        probe!("opentelemetry-otlp"),
        probe!("prosody"),
        probe!("rdkafka"),
    ]
}

/// Splits one default directive into its target and level.
fn default_directive(directive: &str) -> (Option<&str>, LevelFilter) {
    match directive.split_once('=') {
        None => (None, level_filter(directive)),
        Some((target, level)) => (Some(target), level_filter(level)),
    }
}

fn level_filter(level: &str) -> LevelFilter {
    level.parse().unwrap_or(LevelFilter::OFF)
}
