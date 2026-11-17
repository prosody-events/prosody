//! The loader's librdkafka configuration keeps the base settings and
//! fetches with the consumer's rack.

use crate::consumer::ConsumerConfiguration;
use crate::loader::kafka::{LoaderConfiguration, client_config};
use crate::test_util::kafka::extends_base;
use color_eyre::Result;
use quickcheck_macros::quickcheck;

/// The loader keeps every base setting. It takes the consumer's rack and
/// sends it as `client.rack` exactly when the consumer has one.
///
/// Falsify: start the loader's `client_config` from `ClientConfig::new`, drop
/// `rack` in `LoaderConfiguration::for_consumer`, or drop the `client.rack`
/// setting in the loader's `client_config`.
#[quickcheck]
fn loader_keeps_the_base_and_sends_the_rack(rack: Option<String>) -> Result<()> {
    let expected = rack.clone();
    let consumer = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("rack")
        .rack(rack)
        .build()?;

    let loader = LoaderConfiguration::for_consumer(&consumer, None);
    let client = client_config(&loader, "loader");
    assert!(extends_base(&client));
    assert_eq!(client.get("client.rack"), expected.as_deref());
    Ok(())
}
