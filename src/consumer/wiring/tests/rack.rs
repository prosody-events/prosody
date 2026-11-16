//! The consumer's rack reaches librdkafka as `client.rack`.

use crate::consumer::ConsumerConfiguration;
use crate::consumer::wiring::runtime::client_config;
use color_eyre::Result;
use quickcheck_macros::quickcheck;

/// The main consumer sends `client.rack` exactly when a rack is configured,
/// and with the configured value.
///
/// Falsify: drop the `client.rack` setting in `client_config`.
#[quickcheck]
fn consumer_sends_the_configured_rack(rack: Option<String>) -> Result<()> {
    let expected = rack.clone();
    let config = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("rack")
        .rack(rack)
        .build()?;

    let client = client_config(&config)?;
    assert_eq!(client.get("client.rack"), expected.as_deref());
    Ok(())
}
