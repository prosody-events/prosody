use super::{missing_from, missing_topics};
use crate::Topic;
use crate::producer::ProducerConfiguration;
use crate::producer::ProsodyProducer;
use crate::telemetry::Telemetry;
use color_eyre::Result;
use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::collections::HashSet;

/// Names the generator draws random noise from, into either list. Any of these
/// may or may not survive, so the oracle decides — none of the property's
/// guaranteed witnesses live here.
///
/// The vocabulary is finite on purpose: [`Topic`] is an interned string, so
/// generating fresh names per iteration would intern them for the life of the
/// process.
const VOCABULARY: [&str; 5] = ["orders", "orders.v2", "shipments", "注文", "^noise.*"];

/// Always in both lists, so the existing-name filter always has something to
/// remove.
const SHARED: &str = "orders";

/// Always requested twice and never existing, so duplicate collapsing always
/// has something to collapse. Absent from [`VOCABULARY`], so noise cannot make
/// it exist.
const ABSENT: &str = "never-created";

/// Always requested, never existing, and not pattern subscriptions, so these
/// must always survive. `has^caret` pins that only a *leading* caret means
/// "pattern".
const SURVIVORS: [&str; 2] = ["", "has^caret"];

/// Always requested, never existing, and always dropped as a pattern.
const PATTERN: &str = "^only-a-pattern";

/// Owns the helper-produced mock cluster alongside the producer so the
/// cluster's Drop runs when the test ends (no `mem::forget` leaks).
struct ProducerFixture {
    producer: ProsodyProducer,
    _cluster: MockCluster<'static, DefaultProducerContext>,
}

/// A pair of topic lists that always contains every case the property needs to
/// be non-vacuous.
#[derive(Clone, Debug)]
struct TopicLists {
    existing: Vec<u8>,
    requested: Vec<u8>,
}

impl TopicLists {
    /// The `(existing, requested)` pair. The guaranteed witnesses are appended
    /// around the random noise, so shrinking can strip the noise without making
    /// any clause of the property vacuous. `ABSENT` is placed at both ends of
    /// `requested` so its two occurrences are never adjacent — collapsing them
    /// therefore requires the sort, not just the dedup.
    fn lists(self) -> (Vec<&'static str>, Vec<&'static str>) {
        let pick = |index: &u8| VOCABULARY[usize::from(*index) % VOCABULARY.len()];

        let mut existing: Vec<&'static str> = self.existing.iter().map(pick).collect();
        existing.push(SHARED);

        let mut requested: Vec<&'static str> = vec![ABSENT, SHARED, PATTERN];
        requested.extend(SURVIVORS);
        requested.extend(self.requested.iter().map(pick));
        requested.push(ABSENT);

        (existing, requested)
    }
}

impl Arbitrary for TopicLists {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            existing: Vec::arbitrary(g),
            requested: Vec::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(
            (self.existing.clone(), self.requested.clone())
                .shrink()
                .map(|(existing, requested)| Self {
                    existing,
                    requested,
                }),
        )
    }
}

/// A `ProsodyProducer` against a mock cluster that has exactly one topic.
fn producer_with_topic(topic: &str) -> Result<ProducerFixture> {
    let cluster = MockCluster::<DefaultProducerContext>::new(1)?;
    let bootstrap = cluster.bootstrap_servers();
    cluster.create_topic(topic, 1, 1)?;

    let config = ProducerConfiguration::builder()
        .bootstrap_servers(vec![bootstrap])
        .source_system("test")
        .build()?;

    let producer = ProsodyProducer::pipeline_producer(config, Telemetry::new().sender())?;
    Ok(ProducerFixture {
        producer,
        _cluster: cluster,
    })
}

/// `missing_from` returns exactly the requested names that are neither pattern
/// subscriptions nor already present, each once.
///
/// Set equality pins membership in both directions — nothing missing, nothing
/// invented — and the length pins uniqueness. Order is deliberately unasserted:
/// `swap_remove` makes it arbitrary and no caller reads it.
///
/// The oracle compares `&str` through a `HashSet` rather than a
/// `BTreeSet<Topic>` so it never reuses the subject's own comparison behavior
/// to check the subject.
#[quickcheck]
fn prop_missing_from_is_the_non_pattern_set_difference(lists: TopicLists) {
    let (existing, requested) = lists.lists();
    let existing_set: HashSet<&str> = existing.iter().copied().collect();

    // The generator's guarantees, asserted rather than assumed: without these a
    // clause could pass because its precondition never fired.
    assert!(
        requested.iter().any(|name| existing_set.contains(name)),
        "generator produced no already-existing name"
    );
    assert!(
        requested.iter().any(|name| name.starts_with('^')),
        "generator produced no pattern subscription"
    );
    assert!(
        requested
            .iter()
            .any(|name| !name.starts_with('^') && name.contains('^')),
        "generator produced no interior caret"
    );
    assert!(
        requested.iter().filter(|name| **name == ABSENT).count() > 1,
        "generator produced no duplicate"
    );

    let expected: HashSet<&str> = requested
        .iter()
        .copied()
        .filter(|name| !name.starts_with('^') && !existing_set.contains(name))
        .collect();

    let output = missing_from(
        requested.iter().copied().map(Topic::from).collect(),
        existing.iter().copied(),
    );
    let actual: HashSet<&str> = output.iter().map(|topic| &**topic).collect();

    assert_eq!(actual, expected, "membership");
    assert_eq!(output.len(), expected.len(), "duplicates in {output:?}");
}

/// `missing_topics` feeds the cluster's real metadata into `missing_from`.
///
/// The property above covers the set logic; this covers only the wiring, which
/// no pure test can reach.
#[test]
fn missing_topics_subtracts_the_clusters_own_metadata() -> Result<()> {
    let fixture = producer_with_topic("present")?;

    let missing = missing_topics(
        &fixture.producer,
        vec![Topic::from("present"), Topic::from("absent")],
    )?;

    assert_eq!(missing, vec![Topic::from("absent")]);
    Ok(())
}
