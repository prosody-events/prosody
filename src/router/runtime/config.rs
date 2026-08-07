//! What an operator sets for peer routing, and the rules that refuse a
//! degenerate value at startup.

use crate::router::label_fits;
use derive_builder::Builder;
use validator::{Validate, ValidationError};

/// What an operator sets for peer routing.
///
/// Every field has a working default, so a deployment on one network needs no
/// configuration at all.
#[derive(Builder, Clone, Debug, Default, Validate)]
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
}

impl RouterConfiguration {
    /// Creates a configuration builder.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn builder() -> RouterConfigurationBuilder {
        RouterConfigurationBuilder::default()
    }
}

/// Refuses a blank label and one longer than a host or network name may be.
/// An absent label never reaches this function.
///
/// The rule itself is [`label_fits`], which the directory's checked group
/// constructor also reads. This function is its `validator` adapter, so a
/// configured host, a discovered machine name and a published group label are
/// all accepted on the same terms.
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
    if !label_fits(label) {
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
