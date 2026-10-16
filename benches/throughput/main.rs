//! Consumer throughput against a live Kafka broker and Cassandra.
//!
//! For each message size, the benchmark creates a topic, loads it, lets each
//! consumer layer drain it with a fresh group, and deletes the topic. It
//! prints one row for each size and layer. Compare adjacent layers to find
//! what costs throughput.
//!
//! Start the backends with `docker compose up -d`, then run:
//!
//! ```text
//! cargo bench --bench throughput
//! ```
//!
//! `PROSODY_BENCH_*` variables choose the workload (see `settings`). All other
//! `PROSODY_*` variables configure the consumer as they do in production.

#![recursion_limit = "512"]

mod layers;
mod progress;
mod settings;
mod workload;

use crate::progress::Measurement;
use crate::settings::Settings;
use crate::workload::Workload;
use color_eyre::eyre::Result;
use prosody::tracing::{initialize_tracing, shutdown_telemetry};
use std::env;
use std::io::{Write, stdout};
use tokio::runtime::Builder;
use tracing_subscriber::layer::Identity;

fn main() -> Result<()> {
    color_eyre::install()?;

    // `cargo bench` passes `--bench`; a filter argument is not supported.
    let settings = Settings::from_env()?;
    let result = Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            // Spans cost what they cost in production. Set
            // `OTEL_EXPORTER_OTLP_ENDPOINT` to send them to a collector.
            initialize_tracing(None::<Identity>)?;
            run(&settings).await
        });
    shutdown_telemetry()?;
    result
}

async fn run(settings: &Settings) -> Result<()> {
    let mut out = stdout().lock();
    writeln!(out, "{settings:#?}")?;
    for (name, value) in env::vars().filter(|(name, _)| is_consumer_setting(name)) {
        writeln!(out, "{name}={value}")?;
    }
    writeln!(
        out,
        "\n{:>7} {:>11} {:>8} {:>9} {:>10} {:>9}",
        "size", "layer", "messages", "startup", "msgs/s", "MiB/s"
    )?;

    for &size in &settings.sizes {
        let (workload, load_time) = Workload::load(settings, size).await?;
        writeln!(
            out,
            "{size:>7} {:>11} {:>8} {:>8.1}s {:>10.0}",
            "load",
            workload.messages,
            load_time.as_secs_f64(),
            f64::from(workload.messages) / load_time.as_secs_f64(),
        )?;

        let result = run_layers(settings, &workload, size, &mut out).await;
        workload.delete().await;
        result?;
    }

    Ok(())
}

async fn run_layers(
    settings: &Settings,
    workload: &Workload,
    size: usize,
    out: &mut impl Write,
) -> Result<()> {
    for &layer in &settings.layers {
        let measurement: Measurement = Box::pin(layer.run(settings, workload)).await?;
        let rate = measurement.rate();
        writeln!(
            out,
            "{size:>7} {layer:>11} {:>8} {:>8.1}s {rate:>10.0} {:>9.1}",
            measurement.messages,
            measurement.startup.as_secs_f64(),
            rate * workload.mean_bytes / f64::from(1_u32 << 20_u32),
        )?;
        out.flush()?;
    }
    Ok(())
}

/// Whether an environment variable configures the consumer. Secrets never
/// print.
fn is_consumer_setting(name: &str) -> bool {
    name.starts_with("PROSODY_")
        && !name.starts_with("PROSODY_BENCH_")
        && !name.contains("PASSWORD")
}
