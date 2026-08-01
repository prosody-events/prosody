//! User-facing configuration for the always-on keyed-state layer.

use super::registry::{CollectionDef, CollectionDefRegistry, RegisterStateError, StateVisibility};
use crate::ByteSize;
use crate::state::descriptor::{Registered, StateDescriptor, StructuralIdentity};
use crate::state::{StateName, StateType};
use crate::subsystem::SubsystemName;
use crate::timers::duration::CompactDuration;
use crate::util::{
    from_duration_env_with_fallback, from_env_with_fallback,
    from_option_duration_env_with_fallback, from_option_env,
};
use derive_builder::Builder;
use std::env;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;
use validator::{Validate, ValidationError, ValidationErrors};

/// Environment variable for the local keyed-state cache directory.
const STATE_CACHE_DIR_ENV: &str = "PROSODY_STATE_CACHE_DIR";

/// Environment variable for the owning keyed-state cache capacity.
const STATE_OWNED_CACHE_SIZE_ENV: &str = "PROSODY_STATE_OWNED_CACHE_SIZE";

/// Environment variable for the reader-side read-through cache capacity, in
/// bytes.
const STATE_READ_CACHE_SIZE_ENV: &str = "PROSODY_STATE_READ_CACHE_SIZE";

/// Environment variable for the default read-cache TTL of composed readers.
const STATE_READ_CACHE_TTL_ENV: &str = "PROSODY_STATE_READ_CACHE_TTL";

/// Environment variable for the subsystem name.
const SUBSYSTEM_ENV: &str = "PROSODY_SUBSYSTEM";

/// Built-in default read-cache TTL, applied when the client composes readers
/// and no other TTL is set. Five seconds trades a small staleness window for
/// fewer repeated store reads on hot keys. It stays well within the delay
/// reads already tolerate. The recovery sweep converges committed values
/// within [`DEFAULT_RECOVERY_DELAY_SECS`] seconds, and routing snapshots
/// refresh every 60 seconds.
const DEFAULT_READ_CACHE_TTL: Duration = Duration::from_secs(5);

const DEFAULT_READER_CACHE_SIZE: ByteSize = match NonZeroU64::new(1_048_576) {
    Some(budget) => ByteSize::new(budget),
    None => ByteSize::new(NonZeroU64::MIN),
};

/// Environment variable for the `StateRecovery` backstop delay.
const RECOVERY_DELAY_ENV: &str = "PROSODY_STATE_RECOVERY_DELAY";

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
///
/// There is deliberately **no `Default`**. Several fields read a
/// environment override, and a malformed one must fail the
/// build rather than be replaced by a default. An infallible constructor has
/// nowhere to report that, so it can only ignore the operator's value —
/// silently, or with a log nobody reads. The builder is the only way in, and it
/// returns the parse error. Do not add a `Default` impl.
#[derive(Builder, Clone, Debug, Validate)]
pub struct KeyedStateConfiguration {
    /// Disk workspace for the local keyed-state cache.
    ///
    /// Production deployments mount this (e.g. a Kubernetes `emptyDir`) and
    /// **must** set it — the cache is wiped on process restart, so the mount
    /// needs no persistence. Each live client needs its own directory because
    /// it is locked exclusively. Defaults to a per-client temporary directory
    /// so unconfigured consumers (and consumers that never register state)
    /// work out of the box, even several in one process.
    ///
    /// Environment variable: `PROSODY_STATE_CACHE_DIR`
    #[builder(default = "from_env_with_fallback(STATE_CACHE_DIR_ENV, default_cache_dir())?")]
    #[validate(custom(function = "validate_cache_dir"))]
    pub cache_dir: PathBuf,

    /// Delay between staging a provisional cell and the `StateRecovery`
    /// backstop sweep that resolves any cell the eager post-commit promote
    /// did not. Every registered collection's TTL must strictly exceed this
    /// (checked at consumer build) so a provisional cell cannot expire before
    /// the sweep reaches it.
    ///
    /// Environment variable: `PROSODY_STATE_RECOVERY_DELAY`. Accepts a
    /// duration at second granularity and defaults to 30 seconds. Must be at
    /// least one second: a zero delay would schedule the sweep to run
    /// immediately, leaving no window for the fast post-commit path to resolve
    /// the cell first.
    #[builder(default = "recovery_delay_from_env()?")]
    #[validate(custom(function = "validate_recovery_delay"))]
    pub recovery_delay: CompactDuration,

    /// Capacity of the in-memory keyed-state cache, in **bytes**.
    ///
    /// `None` (the default) leaves the storage engine to choose its own
    /// default. `Some(bytes)` sets the capacity of the one cache this consumer
    /// opens at `cache_dir`; it is shared by every partition, never multiplied
    /// per partition.
    ///
    /// Environment variable: `PROSODY_STATE_OWNED_CACHE_SIZE`. Accepts a
    /// positive human-readable byte size such as `64 MiB` or `500 MB`. A bare
    /// number is interpreted as bytes.
    #[builder(default = "from_option_env(STATE_OWNED_CACHE_SIZE_ENV)?")]
    pub owned_cache_size: Option<ByteSize>,

    /// Byte budget for the reader-side read-through cache. The high-level
    /// client sizes this cache when it composes standalone readers.
    ///
    /// `None` (the default) uses
    /// [`owned_cache_size`](Self::owned_cache_size) when it is set. It uses
    /// 1 MiB when both sizes are unset. Only the composing client reads this
    /// value. A consumer never opens a reader cache.
    ///
    /// Environment variable: `PROSODY_STATE_READ_CACHE_SIZE`. Accepts a
    /// positive human-readable byte size such as `1 MiB`. A bare number is
    /// interpreted as bytes.
    #[builder(default = "from_option_env(STATE_READ_CACHE_SIZE_ENV)?")]
    pub read_cache_size: Option<ByteSize>,

    /// Default read-cache TTL for the readers this client composes. It sets how
    /// long a `StateReader` may serve a collection's reads from cache before
    /// re-reading the store. Defaults to 5 seconds, well inside the delay reads
    /// already tolerate (see the recovery sweep on [`Self::recovery_delay`]).
    ///
    /// `None` disables the inherited default. A descriptor can replace this
    /// TTL or select
    /// [`ReadCachePolicy::Disabled`](crate::state::ReadCachePolicy)
    /// to bypass it. This setting affects only composed readers, never the
    /// owning consumer's writes. It is unrelated to a collection's durable TTL.
    ///
    /// Environment variable: `PROSODY_STATE_READ_CACHE_TTL` (a humantime
    /// duration such as `5s` or `750ms`; `none` disables the inherited
    /// default). Must not be zero: every cached entry would be born stale.
    #[builder(
        default = "from_option_duration_env_with_fallback(STATE_READ_CACHE_TTL_ENV, \
                   DEFAULT_READ_CACHE_TTL)?"
    )]
    #[validate(custom(function = "validate_read_cache_ttl"))]
    pub read_cache_ttl: Option<Duration>,

    /// Subsystem this consumer publishes keyed state under. Required whenever
    /// any registered collection is `.published(true)`. A published collection
    /// with no subsystem is rejected at build
    /// ([`RegisterStateError::PublishedWithoutSubsystem`]). `None` (the
    /// default) is valid for consumers that publish nothing.
    ///
    /// Keep this set across the deploy that un-publishes a collection. Startup
    /// reconciliation withdraws a collection's routing row only while it is
    /// still registered `Private` under a configured subsystem. Dropping the
    /// subsystem or the registration in the same deploy as `.published(false)`
    /// strands the row instead of withdrawing it. See [`StateVisibility`].
    ///
    /// Environment variable: `PROSODY_SUBSYSTEM`. `none` disables it.
    #[builder(default = "from_option_env(SUBSYSTEM_ENV)?")]
    pub subsystem: Option<SubsystemName>,

    #[builder(setter(skip), default)]
    registrations: Vec<(StateType, &'static str, StructuralIdentity, CollectionDef)>,
}

impl KeyedStateConfiguration {
    /// Creates a new builder.
    #[must_use]
    pub fn builder() -> KeyedStateConfigurationBuilder {
        KeyedStateConfigurationBuilder::default()
    }

    pub(crate) fn reader_cache_size(&self) -> NonZeroU64 {
        self.read_cache_size
            .or(self.owned_cache_size)
            .unwrap_or(DEFAULT_READER_CACHE_SIZE)
            .nonzero()
    }

    /// Validates the configuration and every registered descriptor.
    ///
    /// Bindings call this after mapping host-language definitions into typed
    /// descriptors. All collection semantics remain owned by Prosody.
    ///
    /// # Errors
    ///
    /// Returns an error when a configuration field or descriptor registration
    /// violates a keyed-state invariant.
    pub fn validate(&self) -> Result<(), KeyedStateValidationError> {
        Validate::validate(self)?;
        self.build_registry()?;
        Ok(())
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

    pub(crate) fn try_register<D>(
        &mut self,
        descriptor: D,
    ) -> Result<Registered<D>, RegisterStateError>
    where
        D: StateDescriptor,
    {
        validate_publication(
            descriptor.name(),
            descriptor.collection_def(),
            self.subsystem.as_ref(),
        )?;
        Ok(self.register(descriptor))
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
    /// `recovery_delay`, a `Published` collection with no configured subsystem,
    /// or an identity conflict.
    pub(crate) fn build_registry(&self) -> Result<CollectionDefRegistry, RegisterStateError> {
        let mut registry = CollectionDefRegistry::default();
        for (state_type, name, identity, def) in &self.registrations {
            validate_publication(name, *def, self.subsystem.as_ref())?;
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

fn validate_publication(
    name: &str,
    definition: CollectionDef,
    subsystem: Option<&SubsystemName>,
) -> Result<(), RegisterStateError> {
    if definition.visibility == StateVisibility::Published && subsystem.is_none() {
        return Err(RegisterStateError::PublishedWithoutSubsystem {
            name: StateName::try_new(name)?,
        });
    }
    Ok(())
}

/// Per-client fallback keyed-state cache workspace, used when
/// [`STATE_CACHE_DIR_ENV`] is unset: `<temp>/prosody/keyed-state/<uuid>`. Wiped
/// on restart, so it needs no persistence. The UUID leaf gives every client its
/// own database. The directory is locked exclusively per live client, so two
/// default-config clients in one process never contend.
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

fn validate_read_cache_ttl(ttl: &Duration) -> Result<(), ValidationError> {
    if ttl.is_zero() {
        return Err(ValidationError::new("read_cache_ttl_zero"));
    }
    Ok(())
}

/// A keyed-state configuration or descriptor registration was invalid.
#[derive(Debug, Error)]
pub enum KeyedStateValidationError {
    /// A scalar configuration field failed validation.
    #[error("invalid keyed-state configuration: {0:#}")]
    Configuration(#[from] ValidationErrors),

    /// A registered descriptor violated a collection invariant.
    #[error(transparent)]
    Registration(#[from] RegisterStateError),
}

#[cfg(test)]
mod tests;
