//! What an operator sets for peer routing, and the rules that refuse a
//! degenerate value at startup.

use crate::router::directory::Endpoint;
use crate::router::label_fits;
use derive_builder::Builder;
use validator::{Validate, ValidationError};

/// What an operator sets for peer routing.
///
/// Every field has a working default, so a deployment on one network needs no
/// configuration at all.
#[derive(Builder, Clone, Debug, Default, Validate)]
#[builder(setter(into, strip_option), default)]
pub(crate) struct RouterConfiguration {
    /// The validated entry point peers on another network use. Unset means
    /// intra-network only.
    pub(crate) advertised: Option<Endpoint>,

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
