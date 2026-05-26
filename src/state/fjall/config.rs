//! Configuration for the fjall-backed Value cache.

use crate::util::from_env_with_fallback;
use derive_builder::Builder;
use std::path::{Path, PathBuf};
use validator::Validate;

const DEFAULT_CACHE_DIR: &str = "/var/cache/prosody";

/// Configuration for the fjall-backed Value cache.
///
/// Slice 6 exposes only the on-disk root. Production deployments mount this
/// at an emptyDir-type volume; on partition revocation the per-partition
/// keyspace is dropped; on process restart the whole root is wiped because
/// Cassandra is authoritative.
#[derive(Builder, Clone, Debug, Validate)]
pub struct FjallConfiguration {
    /// Root directory under which the fjall keyspace is opened.
    ///
    /// Environment variable: `PROSODY_FJALL_CACHE_DIR`
    /// Default: `/var/cache/prosody`
    #[builder(
        default = "from_env_with_fallback(\"PROSODY_FJALL_CACHE_DIR\", \
                   PathBuf::from(DEFAULT_CACHE_DIR))?",
        setter(into)
    )]
    #[validate(custom(function = "validate_cache_dir"))]
    pub cache_dir: PathBuf,
}

impl FjallConfiguration {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> FjallConfigurationBuilder {
        FjallConfigurationBuilder::default()
    }
}

fn validate_cache_dir(cache_dir: &Path) -> Result<(), validator::ValidationError> {
    if cache_dir.as_os_str().is_empty() {
        return Err(validator::ValidationError::new("empty_cache_dir"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::FjallConfiguration;
    use color_eyre::eyre::Result;
    use std::path::PathBuf;
    use validator::Validate;

    #[test]
    fn explicit_cache_dir_round_trips() -> Result<()> {
        let dir = PathBuf::from("/tmp/prosody-fjall-test");
        let config = FjallConfiguration::builder()
            .cache_dir(dir.clone())
            .build()?;
        assert_eq!(config.cache_dir, dir);
        config.validate()?;
        Ok(())
    }

    #[test]
    fn empty_path_is_rejected_by_validator() {
        let config = FjallConfiguration {
            cache_dir: PathBuf::new(),
        };
        assert!(
            config.validate().is_err(),
            "empty cache_dir must fail validation"
        );
    }
}
