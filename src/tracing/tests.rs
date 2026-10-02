use super::{QUIET_TARGETS, flush_telemetry, log_filter, shutdown_telemetry};
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

/// Levels that a generated directive can set.
const OVERRIDE_LEVELS: [LevelFilter; 6] = [
    LevelFilter::OFF,
    LevelFilter::ERROR,
    LevelFilter::WARN,
    LevelFilter::INFO,
    LevelFilter::DEBUG,
    LevelFilter::TRACE,
];

/// Event levels that the property checks, in the order of [`probe`].
const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];

/// One generated segment of a `PROSODY_LOG` value.
#[derive(Clone, Debug)]
enum Override {
    /// A valid directive for a target, or a bare level.
    Valid(Option<&'static str>, LevelFilter),

    /// A directive with the level `!loud`. It is also invalid as a bare
    /// target.
    Invalid(Option<&'static str>),

    /// A segment that holds no directive. An unset `PROSODY_LOG` is one empty
    /// segment.
    Blank(&'static str),
}

impl Override {
    fn render(&self) -> String {
        match self {
            Self::Valid(None, level) => level.to_string(),
            Self::Valid(Some(target), level) => format!("{target}={level}"),
            Self::Invalid(None) => "!loud".to_owned(),
            Self::Invalid(Some(target)) => format!("{target}=!loud"),
            Self::Blank(segment) => (*segment).to_owned(),
        }
    }
}

impl Arbitrary for Override {
    fn arbitrary(g: &mut Gen) -> Self {
        let target = *g.choose(&OVERRIDE_TARGETS).unwrap_or(&None);
        match g.choose(&[0_u8, 1, 2]).unwrap_or(&0) {
            0 => Self::Valid(
                target,
                *g.choose(&OVERRIDE_LEVELS).unwrap_or(&LevelFilter::OFF),
            ),
            1 => Self::Invalid(target),
            _ => Self::Blank(if bool::arbitrary(g) { "" } else { " " }),
        }
    }
}

/// Returns `$target` and, for each level in [`LEVELS`], whether the current
/// subscriber enables an event for it.
///
/// A callsite fixes its target and level at compile time. So each pair needs
/// its own `enabled!` call.
macro_rules! probe {
    ($target:literal) => {
        (
            $target,
            [
                enabled!(target: $target, Level::ERROR),
                enabled!(target: $target, Level::WARN),
                enabled!(target: $target, Level::INFO),
                enabled!(target: $target, Level::DEBUG),
                enabled!(target: $target, Level::TRACE),
            ],
        )
    };
}

/// A `PROSODY_LOG` directive replaces the default for its own target. The
/// last bare level replaces the `info` default and caps the
/// [`QUIET_TARGETS`] at that level. Invalid and blank segments change nothing.
///
/// The model keeps one level for each target. The defaults go in first. Then
/// each valid targeted override replaces the entry for its target. A probe
/// target gets the level of the longest entry that is a prefix of it.
#[quickcheck]
fn prop_prosody_log_replaces_defaults_per_target(overrides: Vec<Override>) -> bool {
    let base = overrides
        .iter()
        .filter_map(|directive| match directive {
            Override::Valid(None, level) => Some(*level),
            _ => None,
        })
        .next_back()
        .unwrap_or(LevelFilter::INFO);

    let mut model = BTreeMap::from([(None, base)]);
    for target in QUIET_TARGETS {
        model.insert(Some(target), base.min(LevelFilter::WARN));
    }

    let mut rendered = Vec::with_capacity(overrides.len());
    for directive in overrides {
        rendered.push(directive.render());
        if let Override::Valid(Some(target), level) = directive {
            model.insert(Some(target), level);
        }
    }

    let filter = log_filter(&rendered.join(","));
    let probes = with_default(Registry::default().with(filter), probe_all);

    probes.into_iter().all(|(probe, enabled)| {
        let level = model
            .iter()
            .filter(|(target, _)| target.is_none_or(|target| probe.starts_with(target)))
            .max_by_key(|(target, _)| target.map_or(0, str::len))
            .map_or(LevelFilter::OFF, |(_, level)| *level);
        enabled == LEVELS.map(|event| event <= level)
    })
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

/// Probes each checked target under the current subscriber. `rdkafka` has no
/// default and no generated directive.
fn probe_all() -> [(&'static str, [bool; LEVELS.len()]); 6] {
    [
        probe!("scylla"),
        probe!("opentelemetry"),
        probe!("opentelemetry_sdk"),
        probe!("opentelemetry-otlp"),
        probe!("prosody"),
        probe!("rdkafka"),
    ]
}
