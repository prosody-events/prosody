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
