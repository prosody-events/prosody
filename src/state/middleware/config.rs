//! User-facing configuration for the always-on keyed-state layer.

use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError};
use crate::state::descriptor::{DescriptorIdentity, StructuralIdentity};
use crate::timers::duration::CompactDuration;
use crate::util::from_env_with_fallback;
use std::path::PathBuf;
use std::{env, process};

/// Default delay between sealing and the `StateRecovery` sweep.
const DEFAULT_RECOVERY_DELAY_SECS: u32 = 30;

/// Configuration for the pipeline consumer's keyed-state layer.
///
/// The layer is always present; with no registered descriptors it is inert
/// (no durable identity I/O, no seals). Register collections with
/// [`Self::state`]:
///
/// ```
/// use prosody::consumer::KeyedStateConfiguration;
/// use prosody::state::descriptor::{ValueDescriptor, value_state};
/// use prosody::state::middleware::CollectionDef;
///
/// const CART: ValueDescriptor<serde_json::Value> = value_state("cart");
///
/// let keyed_state = KeyedStateConfiguration::default().state(&CART, CollectionDef::new(None));
/// ```
#[derive(Clone, Debug)]
pub struct KeyedStateConfiguration {
    /// Root directory for the local fjall workspace (committed-value cache
    /// + dirty overlays).
    ///
    /// Production deployments mount this (e.g. a Kubernetes `emptyDir`) and
    /// **must** set it — the cache is wiped on process restart, so the mount
    /// needs no persistence. Defaults to a per-process temporary directory
    /// so unconfigured consumers (and consumers that never register state)
    /// work out of the box.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_DIR`
    pub cache_dir: PathBuf,

    /// Middleware-wide default TTL for collections whose
    /// [`CollectionDef`] does not override it. `None` means indefinite
    /// retention.
    pub default_ttl: Option<CompactDuration>,

    /// Delay between sealing a WAL and the `StateRecovery` backstop sweep.
    pub recovery_delay: CompactDuration,

    registrations: Vec<(&'static str, StructuralIdentity, CollectionDef)>,
}

impl Default for KeyedStateConfiguration {
    fn default() -> Self {
        let fallback = env::temp_dir().join(format!("prosody-keyed-state-{}", process::id()));
        let cache_dir = match from_env_with_fallback("PROSODY_FJALL_CACHE_DIR", fallback.clone()) {
            Ok(dir) => dir,
            // `PathBuf` parsing is infallible; keep the fallback as the
            // sensible default if that ever changes.
            Err(_) => fallback,
        };
        Self {
            cache_dir,
            default_ttl: None,
            recovery_delay: CompactDuration::new(DEFAULT_RECOVERY_DELAY_SECS),
            registrations: Vec::new(),
        }
    }
}

impl KeyedStateConfiguration {
    /// Registers `descriptor`'s collection with operational settings `def`.
    ///
    /// Name validation and identity-conflict rejection run at consumer
    /// build, the fallible boundary.
    #[must_use]
    pub fn state<DESC>(mut self, descriptor: &DESC, def: CollectionDef) -> Self
    where
        DESC: DescriptorIdentity,
    {
        self.registrations
            .push((descriptor.name(), descriptor.structural_identity(), def));
        self
    }

    /// Returns whether any collections are registered.
    #[must_use]
    pub fn has_registrations(&self) -> bool {
        !self.registrations.is_empty()
    }

    /// Builds the collection registry from the registrations and
    /// `default_ttl`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterStateError`] on an empty descriptor name or an
    /// identity conflict.
    pub(crate) fn build_registry(&self) -> Result<CollectionDefRegistry, RegisterStateError> {
        let mut registry = CollectionDefRegistry::new(self.default_ttl);
        for (name, identity, def) in &self.registrations {
            registry.register_identity(name, identity.clone(), def.clone())?;
        }
        Ok(registry)
    }
}
