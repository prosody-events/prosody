//! User-facing configuration for the always-on keyed-state layer.

use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::descriptor::{Registered, StateDescriptor, StructuralIdentity};
use crate::state::{StateName, StateType};
use crate::timers::duration::CompactDuration;
use crate::util::{from_duration_env_with_fallback, from_env_with_fallback, from_option_env};
use derive_builder::Builder;
use std::env;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;
use validator::{Validate, ValidationError};

/// Environment variable for the local fjall workspace directory.
const FJALL_CACHE_DIR_ENV: &str = "PROSODY_FJALL_CACHE_DIR";

/// Environment variable for the fjall block-cache capacity, in bytes.
const FJALL_CACHE_SIZE_ENV: &str = "PROSODY_FJALL_CACHE_SIZE_BYTES";

/// Environment variable for the `StateRecovery` backstop delay.
const RECOVERY_DELAY_ENV: &str = "PROSODY_KEYED_STATE_RECOVERY_DELAY";

/// Default delay between staging a cell and the `StateRecovery` sweep.
const DEFAULT_RECOVERY_DELAY_SECS: u32 = 30;

/// Configuration for the pipeline consumer's keyed-state layer.
///
/// The layer is always present; with no registered descriptors it is inert
/// (no durable identity I/O, no cell writes). Register collections with
/// [`Self::register`], which returns the [`Registered`] capability handle a
/// handler binds via `ctx.state(...)`:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use prosody::consumer::KeyedStateConfiguration;
/// use prosody::state::descriptor::{ValueDescriptor, value_state};
///
/// let cart: ValueDescriptor = value_state("cart");
///
/// let mut keyed_state = KeyedStateConfiguration::builder().build()?;
/// let registered_cart = keyed_state.register(cart);
/// # Ok(())
/// # }
/// ```
#[derive(Builder, Clone, Debug, Validate)]
pub struct KeyedStateConfiguration {
    /// Root directory for the local fjall workspace (the committed-value
    /// cache).
    ///
    /// Production deployments mount this (e.g. a Kubernetes `emptyDir`) and
    /// **must** set it — the cache is wiped on process restart, so the mount
    /// needs no persistence. Each live client needs its own directory (fjall
    /// locks it exclusively). Defaults to a per-client temporary directory so
    /// unconfigured consumers (and consumers that never register state) work
    /// out of the box, even several in one process.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_DIR`
    #[builder(default = "from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())?")]
    #[validate(custom(function = "validate_cache_dir"))]
    pub cache_dir: PathBuf,

    /// Delay between staging a provisional cell and the `StateRecovery`
    /// backstop sweep that resolves any cell the eager post-commit promote
    /// did not. Every registered collection's TTL must strictly exceed this
    /// (checked at consumer build) so a provisional cell cannot expire before
    /// the sweep reaches it.
    ///
    /// Environment variable: `PROSODY_KEYED_STATE_RECOVERY_DELAY`. Accepts a
    /// duration at second granularity and defaults to 30 seconds. Must be at
    /// least one second: a zero delay would schedule the sweep to run
    /// immediately, leaving no window for the fast post-commit path to resolve
    /// the cell first.
    #[builder(default = "recovery_delay_from_env()?")]
    #[validate(custom(function = "validate_recovery_delay"))]
    pub recovery_delay: CompactDuration,

    /// Block-cache capacity for the local fjall workspace, in **bytes**.
    ///
    /// `None` (the default) leaves fjall to choose its own library default —
    /// prosody deliberately does not copy that number, so an upstream tuning
    /// change is inherited. `Some(bytes)` sets the capacity of the one
    /// `Database` this consumer opens at `cache_dir`; it is shared by every
    /// per-partition keyspace, never multiplied per partition.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_SIZE_BYTES` (a positive
    /// integer count of bytes; `0`, negative, non-numeric, and
    /// out-of-`u64`-range values are rejected at build).
    #[builder(default = "from_option_env(FJALL_CACHE_SIZE_ENV)?")]
    pub cache_size_bytes: Option<NonZeroU64>,

    #[builder(setter(skip), default)]
    registrations: Vec<(StateType, &'static str, StructuralIdentity, CollectionDef)>,
}

impl Default for KeyedStateConfiguration {
    fn default() -> Self {
        Self {
            cache_dir: from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())
                .unwrap_or_else(|_| default_cache_dir()),
            recovery_delay: recovery_delay_from_env()
                .unwrap_or_else(|_| CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS)),
            cache_size_bytes: from_option_env(FJALL_CACHE_SIZE_ENV).unwrap_or_default(),
            registrations: Vec::new(),
        }
    }
}

impl KeyedStateConfiguration {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> KeyedStateConfigurationBuilder {
        KeyedStateConfigurationBuilder::default()
    }

    /// Registers `descriptor`'s collection, returning the [`Registered`]
    /// capability handle [`EventContext::state`] requires.
    ///
    /// The descriptor carries its own operational settings (TTL, commit mode)
    /// fluently — `value_state("cart").ttl(d).read_uncommitted()` — recorded
    /// here via [`StateDescriptor::collection_def`]. Name validation,
    /// TTL-ceiling rejection, and identity-conflict rejection all run at
    /// consumer build, the fallible boundary.
    ///
    /// [`EventContext::state`]: crate::consumer::event_context::EventContext::state
    pub fn register<D>(&mut self, descriptor: D) -> Registered<D>
    where
        D: StateDescriptor,
    {
        self.registrations.push((
            descriptor.state_type(),
            descriptor.name(),
            descriptor.structural_identity(),
            descriptor.collection_def(),
        ));
        Registered::new(descriptor)
    }

    /// Returns whether any collections are registered.
    #[must_use]
    pub(crate) fn has_registrations(&self) -> bool {
        !self.registrations.is_empty()
    }

    /// Builds the collection registry from the registrations.
    ///
    /// Each registration's TTL is checked against `recovery_delay` here, the
    /// one boundary that knows both: a provisional cell carries the
    /// collection's TTL, so it must outlive the `StateRecovery` sweep or a
    /// committed write could expire before recovery resolves it (invariant
    /// 10). The intrinsic per-collection checks (name, Cassandra TTL ceiling,
    /// identity conflict) stay in [`CollectionDefRegistry::register_identity`].
    ///
    /// Fails with [`RegisterStateError`] on an empty descriptor name, a TTL
    /// over Cassandra's `USING TTL` ceiling, a TTL at or below
    /// `recovery_delay`, or an identity conflict.
    pub(crate) fn build_registry(&self) -> Result<CollectionDefRegistry, RegisterStateError> {
        let mut registry = CollectionDefRegistry::default();
        for (state_type, name, identity, def) in &self.registrations {
            if let Some(ttl) = def.ttl
                && ttl.seconds() <= self.recovery_delay.seconds()
            {
                return Err(RegisterStateError::TtlBelowRecoveryDelay {
                    name: StateName::try_new(name)?,
                    ttl_seconds: ttl.seconds(),
                    recovery_seconds: self.recovery_delay.seconds(),
                });
            }
            registry.register_identity(*state_type, name, identity.clone(), *def)?;
        }
        Ok(registry)
    }
}

/// Per-client fallback fjall workspace, used when [`FJALL_CACHE_DIR_ENV`] is
/// unset: `<temp>/prosody/keyed-state/<uuid>`. Wiped on restart, so it needs
/// no persistence. The UUID leaf gives every client its own database — fjall
/// locks the directory exclusively per live client, so two default-config
/// clients in one process never contend.
fn default_cache_dir() -> PathBuf {
    env::temp_dir()
        .join("prosody")
        .join("keyed-state")
        .join(Uuid::new_v4().simple().to_string())
}

/// Reads [`RECOVERY_DELAY_ENV`] as a duration, falling back to
/// [`DEFAULT_RECOVERY_DELAY_SECS`] seconds when unset. Sub-second values round
/// to the nearest second per [`CompactDuration`]'s `Duration` conversion.
fn recovery_delay_from_env() -> Result<CompactDuration, String> {
    let fallback = Duration::from_secs(u64::from(DEFAULT_RECOVERY_DELAY_SECS));
    let duration = from_duration_env_with_fallback(RECOVERY_DELAY_ENV, fallback)?;
    CompactDuration::try_from(duration).map_err(|error| error.to_string())
}

fn validate_cache_dir(cache_dir: &Path) -> Result<(), ValidationError> {
    if cache_dir.as_os_str().is_empty() {
        return Err(ValidationError::new("cache_dir_empty"));
    }
    Ok(())
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "the validator derive invokes custom functions by reference"
)]
fn validate_recovery_delay(recovery_delay: &CompactDuration) -> Result<(), ValidationError> {
    if recovery_delay.seconds() == 0 {
        return Err(ValidationError::new("recovery_delay_zero"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
