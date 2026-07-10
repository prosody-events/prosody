//! End-to-end span-relation probe against a live OTLP collector.
//!
//! Drives a real consumer (live Kafka + Cassandra) with the OTLP exporter
//! pointed at a collector (e.g. the LGTM stack). For each of `E2E_KEYS`
//! distinct keys it schedules a timer under a per-key scheduling span, with
//! one shared fire time so message and timer handlers run concurrently
//! across the runtime's worker threads. Prints the per-key trace/span ids
//! plus the observed handler thread spread as one JSON line, so the
//! relations can be verified externally through the collector's query API
//! (e.g. Tempo): every dispatch span must target its own key's scheduling
//! span, never another key's.
//!
//! Environment:
//! - `OTEL_EXPORTER_OTLP_ENDPOINT` — e.g. `http://localhost:4318`
//! - `E2E_TIMER_RELATION` — `child` or `follows_from` (default)
//! - `E2E_KEYS` — number of concurrent keys (default 64)

#![recursion_limit = "256"]

use color_eyre::eyre::{Result, eyre};
use opentelemetry::trace::TraceContextExt as _;
use prosody::admin::{AdminConfiguration, ProsodyAdminClient, TopicConfiguration};
use prosody::otel::SpanRelation;
use prosody::prelude::*;
use prosody::timers::duration::CompactDuration;
use prosody::tracing::{Identity, initialize_tracing};
use prosody::{JsonCodec, Key, Topic};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::env;
use std::thread;
use tokio::sync::mpsc::{Sender, channel};
use tokio::time::{Duration, sleep, timeout};
use tracing::{Instrument, Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use uuid::Uuid;

/// Formats a span's `OTel` identity as `"<trace_id> <span_id>"`.
fn span_ids(span: &Span) -> String {
    let context = span.context();
    let span_ref = context.span();
    let sc = span_ref.span_context();
    format!("{} {}", sc.trace_id(), sc.span_id())
}

/// One probe event per handler invocation: phase, key, ids, thread.
fn probe_line(phase: &str, key: &Key, span: &Span) -> String {
    let thread = format!("{:?}", thread::current().id());
    format!("{phase} {key} {} {thread}", span_ids(span))
}

#[derive(Clone)]
struct SpanProbe {
    sender: Sender<String>,
    fire_at: CompactDateTime,
}

impl FallibleHandler for SpanProbe {
    type Error = Infallible;
    type Output = ();
    type Payload = serde_json::Value;

    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<serde_json::Value>,
        _demand_type: DemandType,
    ) -> Result<(), Infallible>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let key = message.key().clone();
        // No explicit parent: dispatch instruments the handler with the
        // receive span, so ambient parenting nests this under it.
        let sched_span = info_span!("e2e.schedule", key = %key);
        let outcome = context
            .schedule(self.fire_at, TimerType::Application)
            .instrument(sched_span.clone())
            .await;

        let line = match outcome {
            Ok(()) => probe_line("sched", &key, &sched_span),
            Err(e) => format!("error scheduling {key} failed: {e:#}"),
        };
        let _ = self.sender.send(line).await;
        Ok(())
    }

    async fn on_timer<C>(
        &self,
        _context: C,
        trigger: Trigger,
        _demand_type: DemandType,
    ) -> Result<(), Infallible>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        // Bare span: dispatch instruments the handler with the trigger span,
        // so ambient parenting nests this under it.
        let handle_span = info_span!("e2e.handle", key = %trigger.key);
        let _ = self
            .sender
            .send(probe_line("disp", &trigger.key, &trigger.span()))
            .instrument(handle_span)
            .await;
        Ok(())
    }

    async fn shutdown(self) {}
}

#[tokio::main]
async fn main() -> Result<()> {
    let relation: SpanRelation = env::var("E2E_TIMER_RELATION")
        .unwrap_or_else(|_| "follows_from".to_owned())
        .parse()?;
    let keys: usize = env::var("E2E_KEYS")
        .unwrap_or_else(|_| "64".to_owned())
        .parse()?;
    initialize_tracing(None::<Identity>)?;

    let topic: Topic = Uuid::new_v4().to_string().as_str().into();
    let bootstrap_servers = vec!["localhost:9094".to_owned()];

    let admin = ProsodyAdminClient::cached(&AdminConfiguration::new(bootstrap_servers.clone())?)?;
    admin
        .create_topic(
            &TopicConfiguration::builder()
                .name(topic.to_string())
                .partition_count(4_u16)
                .replication_factor(1_u16)
                .build()?,
        )
        .await?;

    let mut consumer_config = ConsumerConfiguration::builder();
    consumer_config
        .bootstrap_servers(bootstrap_servers.clone())
        .group_id(Uuid::new_v4().to_string())
        .probe_port(None)
        .timer_spans(relation)
        .subscribed_topics([topic.to_string()]);

    let mut producer_config = ProducerConfiguration::builder();
    producer_config
        .bootstrap_servers(bootstrap_servers)
        .source_system("span-relations-e2e");

    let mut cassandra_config = CassandraConfigurationBuilder::default();
    cassandra_config.nodes(vec!["localhost:9042".to_owned()]);

    let consumer_builders = ConsumerBuilders {
        consumer: consumer_config,
        ..ConsumerBuilders::default()
    };

    let (sender, mut receiver) = channel(keys * 2 + 4);
    let client = HighLevelClient::<SpanProbe, JsonCodec>::new(
        Mode::Pipeline,
        &mut producer_config,
        &consumer_builders,
        &cassandra_config,
    )?;

    // One shared absolute fire time lands every timer in the same instant, so
    // the fires dispatch concurrently across the runtime's worker threads.
    let fire_at = CompactDateTime::now()?.add_duration(CompactDuration::new(8))?;
    client.subscribe(SpanProbe { sender, fire_at }).await?;

    for i in 0..keys {
        let key = format!("k{i:03}");
        client.send(topic, &key, json!({"probe": i})).await?;
    }

    // Collect `keys` sched events and `keys` disp events, keyed.
    let mut sched: BTreeMap<String, String> = BTreeMap::new();
    let mut disp: BTreeMap<String, String> = BTreeMap::new();
    let mut sched_threads: BTreeSet<String> = BTreeSet::new();
    let mut disp_threads: BTreeSet<String> = BTreeSet::new();
    while sched.len() < keys || disp.len() < keys {
        // Hang-guard only; correctness is the id assertions done externally.
        let line = timeout(Duration::from_mins(2), receiver.recv())
            .await
            .map_err(|_| {
                eyre!(
                    "timed out: sched {}/{keys}, disp {}/{keys}",
                    sched.len(),
                    disp.len()
                )
            })?
            .ok_or_else(|| eyre!("probe channel closed early"))?;
        let parts: Vec<&str> = line.splitn(5, ' ').collect();
        match parts.as_slice() {
            ["sched", key, trace, span, thread] => {
                sched.insert((*key).to_owned(), format!("{trace} {span}"));
                sched_threads.insert((*thread).to_owned());
            }
            ["disp", key, trace, span, thread] => {
                disp.insert((*key).to_owned(), format!("{trace} {span}"));
                disp_threads.insert((*thread).to_owned());
            }
            _ => return Err(eyre!("probe failure: {line}")),
        }
    }

    client.unsubscribe().await?;
    admin.delete_topic(&topic).await?;

    // The batch span processor exports on a ~5s schedule; give it two cycles.
    sleep(Duration::from_secs(10)).await;

    #[allow(
        clippy::print_stdout,
        reason = "the probe's deliverable is this JSON line"
    )]
    {
        println!(
            "{}",
            json!({
                "relation": format!("{relation:?}"),
                "keys": keys,
                "sched": sched,
                "disp": disp,
                "sched_threads": sched_threads.len(),
                "disp_threads": disp_threads.len(),
            })
        );
    }

    Ok(())
}
