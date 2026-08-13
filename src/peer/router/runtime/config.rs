//! What an operator sets for peer routing, and the rules that refuse a
//! degenerate value at startup.

use crate::peer::router::directory::Endpoint;
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

/// Refuses a blank network label. An absent label never reaches this function.
pub(super) fn validate_label(label: &str) -> Result<(), ValidationError> {
    if label.is_empty() {
        return Err(ValidationError::new("label_empty"));
    }
    Ok(())
}
