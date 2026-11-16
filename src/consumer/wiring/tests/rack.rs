//! The consumer's librdkafka configuration keeps the base settings and
//! sends the rack.

use crate::consumer::ConsumerConfiguration;
use crate::consumer::wiring::runtime::client_config;
use crate::test_util::kafka::extends_base;
use color_eyre::Result;
use quickcheck_macros::quickcheck;

/// The main consumer keeps every base setting. It sends `client.rack` exactly
/// when a rack is configured, and with the configured value.
///
/// Falsify: start `client_config` from `ClientConfig::new`, or drop its
/// `client.rack` setting.
#[quickcheck]
fn consumer_keeps_the_base_and_sends_the_rack(rack: Option<String>) -> Result<()> {
    let expected = rack.clone();
    let config = ConsumerConfiguration::builder()
        .bootstrap_servers(vec!["kafka:9092".to_owned()])
        .group_id("rack")
        .rack(rack)
        .build()?;

    let client = client_config(&config)?;
    assert!(extends_base(&client));
    assert_eq!(client.get("client.rack"), expected.as_deref());
    Ok(())
}
