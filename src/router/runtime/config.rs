//! What an operator sets for peer routing, and the rules that refuse a
//! degenerate value at startup.

use crate::router::MAX_LABEL_BYTES;
use crate::router::directory::RegistrationTtl;
use derive_builder::Builder;
use validator::{Validate, ValidationError};

/// How many peer registrations stay cached at once, by default.
const DEFAULT_ADDRESS_CACHE_CAPACITY: usize = 1024;

/// Most peer registrations a process may hold at once. A registration is a few
/// short strings. A million of them is already more memory than the cache is
/// worth. The bound stops a typo from asking for a heap the process lacks.
pub(super) const MAX_ADDRESS_CACHE_CAPACITY: usize = 1_048_576;

/// What an operator sets for peer routing.
///
/// Every field has a working default, so a deployment on one network needs no
/// configuration at all.
#[derive(Builder, Clone, Debug, Validate)]
#[builder(setter(into, strip_option), default)]
#[validate(schema(function = "validate_entry_point"))]
pub(crate) struct RouterConfiguration {
    /// The host that peers on another network use to reach this process — a
    /// gateway, an ingress, a translated address. Unset means intra-network
    /// only.
    ///
    /// Folding this and `advertised_port` into one optional pair would make a
    /// port with no host beside it unrepresentable. The two stay separate
    /// because every cross-field rule in this crate is a schema validation, and
    /// one flat builder setter per field is the shape an operator expects.
    #[validate(custom(function = "validate_label"))]
    pub(crate) advertised_host: Option<String>,

    /// The port to publish beside `advertised_host`. Unset publishes the
    /// listener's own port, which is what an entry point that forwards a port
    /// unchanged wants. Validation refuses a port with no host beside it. It
    /// refuses port zero too: an advertised port is a port peers dial, never a
    /// request for one the operating system chooses.
    #[validate(range(min = 1_u16))]
    pub(crate) advertised_port: Option<u16>,

    /// The operator's name for the set of processes that reach each other
    /// directly. Two processes that share it skip the entry point.
    #[validate(custom(function = "validate_label"))]
    pub(crate) network: Option<String>,

    /// How long a registration survives without a refresh. The type carries the
    /// range, so a lease outside it never reaches a configuration at all.
    pub(crate) registration_ttl: RegistrationTtl,

    /// How many peer registrations stay cached at once, up to
    /// [`MAX_ADDRESS_CACHE_CAPACITY`].
    #[validate(range(min = 1_usize, max = MAX_ADDRESS_CACHE_CAPACITY))]
    pub(crate) address_cache_capacity: usize,
}

impl Default for RouterConfiguration {
    fn default() -> Self {
        Self {
            advertised_host: None,
            advertised_port: None,
            network: None,
            registration_ttl: RegistrationTtl::DEFAULT,
            address_cache_capacity: DEFAULT_ADDRESS_CACHE_CAPACITY,
        }
    }
}

impl RouterConfiguration {
    /// Creates a configuration builder.
    #[must_use]
    pub(crate) fn builder() -> RouterConfigurationBuilder {
        RouterConfigurationBuilder::default()
    }
}

/// Refuses a blank label and one longer than a host or network name may be.
/// An absent label never reaches this function.
///
/// It is the one label rule this crate publishes under. Discovery holds the
/// machine name to it as well, so a configured host and a discovered one are
/// accepted on the same terms.
///
/// A `length` rule cannot replace this one: `validator` counts characters,
/// while [`MAX_LABEL_BYTES`] is the byte capacity that keeps a label inline in
/// [`Host`](crate::router::Host).
///
/// Length and blankness are the only rules here, and a dialability rule does
/// not belong beside them: an advertised host may be an IPv6 literal, so a rule
/// that refused a colon would refuse a legal address.
pub(super) fn validate_label(label: &str) -> Result<(), ValidationError> {
    if label.is_empty() {
        return Err(ValidationError::new("label_empty"));
    }
    if label.len() > MAX_LABEL_BYTES {
        return Err(ValidationError::new("label_too_long"));
    }
    Ok(())
}

/// Refuses a published port with no host beside it. An entry point is a host
/// and a port together, and a port alone reaches nothing.
fn validate_entry_point(config: &RouterConfiguration) -> Result<(), ValidationError> {
    if config.advertised_port.is_some() && config.advertised_host.is_none() {
        return Err(ValidationError::new("advertised_port_without_host"));
    }
    Ok(())
}
