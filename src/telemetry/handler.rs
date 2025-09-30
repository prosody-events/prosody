use crate::telemetry::event::TelemetryEvent;
use tokio::sync::mpsc;

pub async fn handle_events(rx: mpsc::Receiver<TelemetryEvent>) {}
