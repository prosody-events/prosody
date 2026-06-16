//! User-facing configuration for the always-on keyed-state layer.

use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::StateName;
use crate::state::descriptor::{Registered, StateDescriptor, StructuralIdentity};
use crate::timers::duration::CompactDuration;
use crate::util::from_env_with_fallback;
use derive_builder::Builder;
use std::path::{Path, PathBuf};
use std::{env, process};
use validator::{Validate, ValidationError};

/// Environment variable for the local fjall workspace directory.
const FJALL_CACHE_DIR_ENV: &str = "PROSODY_FJALL_CACHE_DIR";

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
    /// needs no persistence. Defaults to a per-process temporary directory
    /// so unconfigured consumers (and consumers that never register state)
    /// work out of the box.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_DIR`
    #[builder(default = "from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())?")]
    #[validate(custom(function = "validate_cache_dir"))]
    pub cache_dir: PathBuf,

    /// Fallback TTL for state rows whose collection is **not** in the
    /// registry — recovery-sweep leftovers whose descriptor was since
    /// removed from the application. `None` means indefinite retention for
    /// such rows.
    ///
    /// Registered collections never inherit this value: their TTL is
    /// exactly the `Option` passed to [`CollectionDef::new`], where `None`
    /// is an explicit choice of indefinite retention.
    #[builder(default)]
    pub default_ttl: Option<CompactDuration>,

    /// Delay between staging a provisional cell and the `StateRecovery`
    /// backstop sweep that resolves any cell the eager post-commit promote
    /// did not. Every registered collection's TTL must strictly exceed this
    /// (checked at consumer build) so a provisional cell cannot expire before
    /// the sweep reaches it.
    #[builder(default = "CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS)")]
    pub recovery_delay: CompactDuration,

    #[builder(setter(skip), default)]
    registrations: Vec<(&'static str, StructuralIdentity, CollectionDef)>,
}

impl Default for KeyedStateConfiguration {
    fn default() -> Self {
        Self {
            cache_dir: from_env_with_fallback(FJALL_CACHE_DIR_ENV, default_cache_dir())
                .unwrap_or_else(|_| default_cache_dir()),
            default_ttl: None,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
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
            descriptor.name(),
            descriptor.structural_identity(),
            descriptor.collection_def(),
        ));
        Registered::new(descriptor)
    }

    /// Returns whether any collections are registered.
    #[must_use]
    pub fn has_registrations(&self) -> bool {
        !self.registrations.is_empty()
    }

    /// Builds the collection registry from the registrations and
    /// `default_ttl`.
    ///
    /// Each registration's TTL is checked against `recovery_delay` here, the
    /// one boundary that knows both: a provisional cell carries the
    /// collection's TTL, so it must outlive the `StateRecovery` sweep or a
    /// committed write could expire before recovery resolves it (invariant
    /// 10). The intrinsic per-collection checks (name, Cassandra TTL ceiling,
    /// identity conflict) stay in [`CollectionDefRegistry::register_identity`].
    ///
    /// # Errors
    ///
    /// Returns [`RegisterStateError`] on an empty descriptor name, a TTL over
    /// Cassandra's `USING TTL` ceiling, a TTL at or below `recovery_delay`,
    /// or an identity conflict.
    pub(crate) fn build_registry(&self) -> Result<CollectionDefRegistry, RegisterStateError> {
        let mut registry = CollectionDefRegistry::new(self.default_ttl);
        for (name, identity, def) in &self.registrations {
            if let Some(ttl) = def.ttl
                && ttl.seconds() <= self.recovery_delay.seconds()
            {
                return Err(RegisterStateError::TtlBelowRecoveryDelay {
                    name: StateName::try_new(name)?,
                    ttl_seconds: ttl.seconds(),
                    recovery_seconds: self.recovery_delay.seconds(),
                });
            }
            registry.register_identity(name, identity.clone(), *def)?;
        }
        Ok(registry)
    }
}

/// Per-process fallback fjall workspace, used when [`FJALL_CACHE_DIR_ENV`] is
/// unset. Wiped on restart, so it needs no persistence.
fn default_cache_dir() -> PathBuf {
    env::temp_dir().join(format!("prosody-keyed-state-{}", process::id()))
}

fn validate_cache_dir(cache_dir: &Path) -> Result<(), ValidationError> {
    if cache_dir.as_os_str().is_empty() {
        return Err(ValidationError::new("cache_dir_empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::KeyedStateConfiguration;
    use crate::cassandra::MAX_CASSANDRA_TTL_SECS;
    use crate::state::descriptor::{ValueDescriptor, value_state};
    use crate::state::registry::RegisterStateError;
    use crate::timers::duration::CompactDuration;
    use color_eyre::eyre::Result;
    use std::path::PathBuf;
    use validator::Validate;

    fn cart() -> ValueDescriptor {
        value_state("cart")
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

    /// Indefinite retention (`None`) is always allowed — the TTL ceiling
    /// guards only oversized `Some` values, never the opt-out.
    #[test]
    fn collection_ttl_none_is_allowed() -> Result<()> {
        let mut config = KeyedStateConfiguration::builder().build()?;
        let _ = config.register(cart());
        assert!(config.build_registry().is_ok());
        Ok(())
    }

    #[test]
    fn collection_ttl_at_the_ceiling_is_allowed() -> Result<()> {
        let ttl = CompactDuration::new(CEILING_SECS);
        let mut config = KeyedStateConfiguration::builder().build()?;
        let _ = config.register(cart().ttl(ttl));
        assert!(config.build_registry().is_ok());
        Ok(())
    }

    #[test]
    fn collection_ttl_over_the_ceiling_is_rejected() -> Result<()> {
        let over = CEILING_SECS + 1;
        let mut config = KeyedStateConfiguration::builder().build()?;
        let _ = config.register(cart().ttl(CompactDuration::new(over)));
        assert!(matches!(
            config.build_registry(),
            Err(RegisterStateError::Ttl { seconds, .. }) if seconds == over
        ));
        Ok(())
    }

    /// A TTL equal to the recovery delay is rejected: the cell must *outlive*
    /// the sweep, so the floor is strict (`>`), not `>=`.
    #[test]
    fn collection_ttl_at_the_recovery_delay_is_rejected() -> Result<()> {
        let delay = CompactDuration::new(60);
        let mut config = KeyedStateConfiguration::builder()
            .recovery_delay(delay)
            .build()?;
        let _ = config.register(cart().ttl(delay));
        assert!(matches!(
            config.build_registry(),
            Err(RegisterStateError::TtlBelowRecoveryDelay {
                ttl_seconds,
                recovery_seconds,
                ..
            }) if ttl_seconds == 60 && recovery_seconds == 60
        ));
        Ok(())
    }

    /// A TTL one second below the recovery delay is rejected.
    #[test]
    fn collection_ttl_below_the_recovery_delay_is_rejected() -> Result<()> {
        let mut config = KeyedStateConfiguration::builder()
            .recovery_delay(CompactDuration::new(60))
            .build()?;
        let _ = config.register(cart().ttl(CompactDuration::new(59)));
        assert!(matches!(
            config.build_registry(),
            Err(RegisterStateError::TtlBelowRecoveryDelay { .. })
        ));
        Ok(())
    }

    /// A TTL one second above the recovery delay clears the floor.
    #[test]
    fn collection_ttl_above_the_recovery_delay_is_allowed() -> Result<()> {
        let mut config = KeyedStateConfiguration::builder()
            .recovery_delay(CompactDuration::new(60))
            .build()?;
        let _ = config.register(cart().ttl(CompactDuration::new(61)));
        assert!(config.build_registry().is_ok());
        Ok(())
    }
}
