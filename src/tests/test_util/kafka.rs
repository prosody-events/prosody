//! Kafka topic fixtures for unit tests that run against the local broker.

use crate::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use color_eyre::Result;

/// The local broker that `docker-compose.yaml` starts.
pub(crate) const BOOTSTRAP: &str = "localhost:9094";

/// The admin client every test shares.
pub(crate) fn admin() -> Result<&'static ProsodyAdminClient> {
    let config = AdminConfiguration::new(vec![BOOTSTRAP.to_owned()])?;
    Ok(ProsodyAdminClient::cached(&config)?)
}

/// Creates the topic `name` with `partitions` partitions.
pub(crate) async fn create_topic(name: &str, partitions: u16) -> Result<()> {
    let config = TopicConfiguration::builder()
        .name(name)
        .partition_count(partitions)
        .build()?;
    admin()?.create_topic(&config).await?;
    Ok(())
}

/// Deletes the topic `name`.
pub(crate) async fn delete_topic(name: &str) -> Result<()> {
    admin()?.delete_topic(name).await?;
    Ok(())
}

/// Runs `body` against a new topic with `partitions` partitions, and deletes
/// the topic on every path before it returns the outcome of `body`.
pub(crate) async fn with_topic(
    name: &str,
    partitions: u16,
    body: impl AsyncFn(&str) -> Result<()>,
) -> Result<()> {
    create_topic(name, partitions).await?;
    let outcome = body(name).await;
    delete_topic(name).await?;
    outcome
}
