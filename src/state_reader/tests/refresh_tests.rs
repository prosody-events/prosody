//! Snapshot acquisition and the three-outcome refresh rule.
//!
//! [`prop_snapshot_refresh`] proves the whole rule over random refresh scripts.
//! Withdrawals apply unconditionally. An already-admitted source is never
//! re-validated: the property proves this by counting identity-store reads.
//! A failed routing read keeps the prior snapshot. An emptied routing table
//! fails with `UnknownPublication`. A table whose every new source lacks a
//! frozen identity fails with `IdentityUnavailable`.
//!
//! [`prop_refresh_pacing`] proves what a failed refresh leaves behind: a window
//! in which every read serves the held outcome without a store read, and past
//! which the next read re-reads the routing table.
//!
//! The focused example [`identity_mismatch_sticky`] covers the one case the
//! properties do not: an identity that is present but disagrees with the
//! descriptor. That needs precise interval and clock control they do not model.
//!
//! Every reader here refreshes on every operation (`new_eager`), except the
//! sticky example, which uses `new_with_interval` for the cached fast path.
//! Both properties drive a mocked clock, never a sleep.

use super::support::{
    CountingIdentityStore, GROUP_A, GROUP_B, ScriptedEnv, mock_clock_cache, topic,
};
use crate::Key;
use crate::codec::JsonCodec;
use crate::error::ErrorCategory;
use crate::state::descriptor::{DescriptorIdentity, value_state};
use crate::state::descriptor_identity::DurableDescriptorIdentity;
use crate::state::publication::{PublicationStore, StatePublication};
use crate::state::tests::support::ScriptedPublicationStore;
use crate::state::{StateName, StateType};
use crate::state_reader::PartitionCount;
use crate::state_reader::StateReaderError;
use crate::state_reader::reader::{ABSENT_BACKOFF, REFRESH_BACKOFF};
use crate::state_reader::source::MAX_PUBLICATION_SOURCES;
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::iter::{empty, once};
use std::sync::Arc;
use std::time::Duration;

// --- Snapshot-refresh property ----------------------------------------------

/// The ordered source pool the refresh script drives: `g0 < g1 < g2`.
/// `SourceId` order equals index order, so the lowest admitted source is
/// always the lowest index.
const REFRESH_GROUPS: [&str; 3] = ["refresh-g0", "refresh-g1", "refresh-g2"];

/// One source's edit in a refresh round.
#[derive(Clone, Copy, Debug)]
enum SourceEdit {
    /// No change to this source's advertisement or identity.
    Leave,
    /// Advertise it with a matching frozen identity (idempotent).
    Admit,
    /// Withdraw its publication row (identity untouched).
    Withdraw,
    /// Advertise it but seed no identity. A group newly advertised this way
    /// reads as missing an identity.
    PresentNoIdentity,
}

impl Arbitrary for SourceEdit {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::Leave,
            1 => Self::Admit,
            2 => Self::Withdraw,
            _ => Self::PresentNoIdentity,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::Leave => Box::new(empty()),
            _ => Box::new(once(Self::Leave)),
        }
    }
}

/// One refresh round: an edit per source and whether the routing read fails.
#[derive(Clone, Debug)]
struct RefreshRound {
    edits: [SourceEdit; REFRESH_GROUPS.len()],
    fail: bool,
}

impl Arbitrary for RefreshRound {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            edits: [
                SourceEdit::arbitrary(g),
                SourceEdit::arbitrary(g),
                SourceEdit::arbitrary(g),
            ],
            // Weight the outage low so admission coverage dominates.
            fail: u8::arbitrary(g) % 4 == 0,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let edits = self.edits;
        let base_fail = self.fail;
        // Shrink toward no outage, then toward all-Leave rounds.
        let drop_fail = base_fail.then_some(Self { edits, fail: false });
        let simpler = (0..edits.len()).flat_map(move |i| {
            edits[i].shrink().map(move |e| {
                let mut edits = edits;
                edits[i] = e;
                Self {
                    edits,
                    fail: base_fail,
                }
            })
        });
        Box::new(drop_fail.into_iter().chain(simpler))
    }
}

/// A shrinkable sequence of refresh rounds.
#[derive(Clone, Debug)]
struct RefreshScript {
    rounds: Vec<RefreshRound>,
}

impl Arbitrary for RefreshScript {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rounds: Vec::<RefreshRound>::arbitrary(g)
                .into_iter()
                .take(16)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.rounds.shrink().map(|rounds| Self { rounds }))
    }
}

/// The reader outcome one round predicts.
#[derive(Debug, PartialEq, Eq)]
enum Expect {
    /// `Ok(Some(element(idx)))` — the lowest admitted source answers.
    Value(usize),
    /// `Err(UnknownPublication)`.
    UnknownPublication,
    /// `Err(IdentityUnavailable)`.
    IdentityUnavailable,
    /// A failed routing read with no prior snapshot → a store `Err`.
    ReadError,
    /// `Err(RefreshUnavailable)` — a read inside the pacing window left by a
    /// failed refresh, with nothing held to serve.
    RefreshUnavailable,
}

/// The committed value that source `idx` holds.
fn element(idx: usize) -> Value {
    Value::from(idx as i64)
}

/// The three-outcome refresh rule over random scripts. See the module doc.
///
/// FALSIFICATION: in `StateReader::refresh`, keep the prior snapshot on a
/// successful read (apply additions only, skip withdrawals) → a withdraw round
/// still serves the withdrawn source → the value assert reds. Drop the
/// `prior_groups` filter in `admit` (re-read admitted groups) → the identity
/// read delta exceeds the model's newly-admitted count → red. Stop stepping the
/// clock between rounds → the round after an outage is paced instead of
/// refreshing → the delta assert reds.
#[test]
fn prop_snapshot_refresh() {
    fn property(script: RefreshScript) -> Result<bool> {
        block_on(run_snapshot_refresh(script))
    }
    QuickCheck::new().quickcheck(property as fn(RefreshScript) -> Result<bool>);
}

async fn run_snapshot_refresh(script: RefreshScript) -> Result<bool> {
    let descriptor = value_state::<JsonCodec>("refresh-v");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let identity = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        env.name.as_str(),
        &descriptor.structural_identity(),
    );

    // Seed every source's committed value once; visibility is governed entirely
    // by the publication/identity control plane the rounds edit.
    for (idx, group) in REFRESH_GROUPS.iter().enumerate() {
        let tp = topic(group);
        env.commit(group, tp, &key, idx as u128 + 1, move |handle| async move {
            handle
                .set(element(idx))
                .await
                .map_err(|e| eyre!("set: {e}"))
        })
        .await?;
    }

    let fixture = RefreshFixture {
        publications: env.publications.clone(),
        identities: env.identities.clone(),
        sub: env.sub.clone(),
        name: env.name.clone(),
        identity,
        count: env.count,
    };
    let (cache, mock) = mock_clock_cache(1 << 20);
    let reader = env.reader_eager_with_cache(cache)?;

    // Model state.
    let mut advertised = [false; REFRESH_GROUPS.len()];
    let mut identity_seeded = [false; REFRESH_GROUPS.len()];
    let mut admitted: Vec<usize> = Vec::new();

    for round in script.rounds {
        fixture
            .apply_round(&round, &mut advertised, &mut identity_seeded)
            .await?;
        let (expect, next_admitted, expected_delta) =
            predict_round(round.fail, &advertised, &identity_seeded, &admitted);

        let reads_before = fixture.identities.reads();
        let observed = reader.get(key.clone()).await;
        let reads_delta = fixture.identities.reads() - reads_before;

        if !outcome_matches(&expect, &observed) || reads_delta != expected_delta {
            return Ok(false);
        }
        // A failed read leaves the snapshot untouched; a successful one adopts
        // the newly computed admission.
        if !round.fail {
            admitted = next_admitted;
        }
        // Step exactly to any pacing deadline a failed round established, so
        // every round refreshes. `prop_refresh_pacing` owns the window itself.
        mock.increment(REFRESH_BACKOFF);
    }
    Ok(true)
}

/// Bundles the scripted publication and identity stores with the routing
/// constants one refresh trace uses.
struct RefreshFixture {
    publications: ScriptedPublicationStore,
    identities: CountingIdentityStore,
    sub: SubsystemName,
    name: StateName,
    identity: DurableDescriptorIdentity,
    count: PartitionCount,
}

impl RefreshFixture {
    /// Applies one round's edits to the real stores and mirrors the
    /// advertisement/identity flags into the model.
    async fn apply_round(
        &self,
        round: &RefreshRound,
        advertised: &mut [bool],
        identity_seeded: &mut [bool],
    ) -> Result<()> {
        for (idx, edit) in round.edits.iter().enumerate() {
            let group = REFRESH_GROUPS[idx];
            let tp = topic(group);
            let row = StatePublication {
                group_id: Arc::from(group),
                topic: tp,
                partition_count: self.count,
            };
            match edit {
                SourceEdit::Leave => {}
                SourceEdit::Admit => {
                    self.publications
                        .seed(&self.sub, StateType::Application, &self.name, &row)
                        .await;
                    if !identity_seeded[idx] {
                        self.identities.seed(group, &self.identity).await;
                        identity_seeded[idx] = true;
                    }
                    advertised[idx] = true;
                }
                SourceEdit::Withdraw => {
                    self.publications
                        .remove_group(&self.sub, StateType::Application, &self.name, group)
                        .await
                        .map_err(|e| eyre!("remove_group: {e}"))?;
                    advertised[idx] = false;
                }
                SourceEdit::PresentNoIdentity => {
                    self.publications
                        .seed(&self.sub, StateType::Application, &self.name, &row)
                        .await;
                    advertised[idx] = true;
                }
            }
        }
        if round.fail {
            self.publications.fail_reads_with(ErrorCategory::Transient);
        } else {
            self.publications.heal_reads();
        }
        Ok(())
    }
}

/// Predicts one round's outcome, the new admitted set, and the identity-read
/// delta, mirroring `StateReader::refresh`'s three-outcome rule.
fn predict_round(
    fail: bool,
    advertised: &[bool],
    identity_seeded: &[bool],
    prior: &[usize],
) -> (Expect, Vec<usize>, usize) {
    if fail {
        // A failed routing read keeps the prior snapshot untouched.
        return match prior.iter().copied().min() {
            Some(lowest) => (Expect::Value(lowest), prior.to_vec(), 0),
            None => (Expect::ReadError, prior.to_vec(), 0),
        };
    }
    let advertised_now: Vec<usize> = (0..advertised.len()).filter(|i| advertised[*i]).collect();
    if advertised_now.is_empty() {
        return (Expect::UnknownPublication, Vec::new(), 0);
    }
    // Identity is read once per newly-advertised group (those not already in the
    // prior snapshot); a prior-admitted group is never re-validated.
    let delta = advertised_now.iter().filter(|i| !prior.contains(i)).count();
    let mut next = Vec::new();
    let mut any_missing = false;
    for &i in &advertised_now {
        if prior.contains(&i) || identity_seeded[i] {
            next.push(i);
        } else {
            any_missing = true;
        }
    }
    match next.iter().min() {
        Some(&lowest) => (Expect::Value(lowest), next, delta),
        None if any_missing => (Expect::IdentityUnavailable, Vec::new(), delta),
        None => (Expect::UnknownPublication, Vec::new(), delta),
    }
}

/// Whether the reader's observed outcome matches the predicted [`Expect`].
fn outcome_matches(expect: &Expect, observed: &Result<Option<Value>, StateReaderError>) -> bool {
    match expect {
        Expect::Value(idx) => observed.as_ref().ok() == Some(&Some(element(*idx))),
        Expect::UnknownPublication => {
            matches!(observed, Err(StateReaderError::UnknownPublication { .. }))
        }
        Expect::IdentityUnavailable => {
            matches!(observed, Err(StateReaderError::IdentityUnavailable { .. }))
        }
        Expect::ReadError => matches!(observed, Err(StateReaderError::Store { .. })),
        Expect::RefreshUnavailable => {
            matches!(observed, Err(StateReaderError::RefreshUnavailable { .. }))
        }
    }
}

// --- Failed-refresh pacing --------------------------------------------------

/// The refresh interval the pacing property's reader runs with.
///
/// Every [`Advance`] except `Long` lands inside it, so a completed refresh
/// stays fresh across the shorter steps. That is what makes the absent window
/// observable: an absence outlives its own deadline while still inside this
/// interval, and only excluding it from freshness lets the next read re-read.
const PACING_INTERVAL: Duration = REFRESH_BACKOFF;

/// How far one pacing step moves the mock clock before its read.
///
/// The two windows differ in length, so the steps are chosen to land on both
/// sides of each: `Short` stays inside either, `Absent` lands exactly on the
/// absent deadline while still inside a failure window, and `Long` clears both.
#[derive(Clone, Copy, Debug)]
enum Advance {
    /// Not at all, so the read lands inside any open window.
    None,
    /// Half the absent window, so two steps are needed to cross it.
    Short,
    /// Exactly the absent window, which a failure window outlives.
    Absent,
    /// The whole failure window, landing on the later of the two deadlines.
    Long,
}

impl Advance {
    /// The mock-clock step this advance takes.
    fn duration(self) -> Duration {
        match self {
            Self::None => Duration::ZERO,
            Self::Short => ABSENT_BACKOFF / 2,
            Self::Absent => ABSENT_BACKOFF,
            Self::Long => REFRESH_BACKOFF,
        }
    }
}

impl Arbitrary for Advance {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => Self::None,
            1 => Self::Short,
            2 => Self::Absent,
            _ => Self::Long,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Self::None => Box::new(empty()),
            _ => Box::new(once(Self::None)),
        }
    }
}

/// One pacing step: how far the clock moves, whether the collection is
/// advertised, and whether the routing read fails.
#[derive(Clone, Copy, Debug)]
struct PacingStep {
    advance: Advance,
    published: bool,
    fail: bool,
}

impl Arbitrary for PacingStep {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            advance: Advance::arbitrary(g),
            published: bool::arbitrary(g),
            fail: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let Self {
            advance,
            published,
            fail,
        } = *self;
        // Shrink toward no outage, then toward a published collection, then
        // toward a clock that does not move.
        let healed = fail.then_some(Self {
            advance,
            published,
            fail: false,
        });
        let advertised = (!published).then_some(Self {
            advance,
            published: true,
            fail,
        });
        let slower = advance.shrink().map(move |advance| Self {
            advance,
            published,
            fail,
        });
        Box::new(healed.into_iter().chain(advertised).chain(slower))
    }
}

/// A shrinkable sequence of pacing steps.
#[derive(Clone, Debug)]
struct PacingScript {
    steps: Vec<PacingStep>,
}

impl Arbitrary for PacingScript {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            steps: Vec::<PacingStep>::arbitrary(g)
                .into_iter()
                .take(16)
                .collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.steps.shrink().map(|steps| Self { steps }))
    }
}

/// What the reader's acquisition holds, mirroring `Acquired`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Held {
    /// Nothing acquired: a fresh reader, or one whose absence a failed refresh
    /// discarded.
    Nothing,
    /// A validated snapshot.
    Sources,
    /// A completed refresh that found no publication row.
    Absent,
}

/// Neither refresh outcome that yields no snapshot may re-read the routing
/// table on every read. A failed refresh paces the next attempt by
/// `REFRESH_BACKOFF`; one that finds no publisher paces by the shorter
/// `ABSENT_BACKOFF`. Inside either window a read serves the held outcome
/// without touching the store, and the first read at or past the deadline
/// re-reads. Proven over random outage, publish/withdraw, and clock-advance
/// scripts against a model of both windows.
///
/// The absence is deliberately **not** durable across an outage: a failed
/// refresh discards it, because "no publisher yet" is the one claim that read
/// could not confirm. Sources survive so reads keep working.
///
/// Pacing and the refresh interval are the only two things that suppress a
/// routing read, and both are modelled, so the expected read count is exact.
/// The interval is [`PACING_INTERVAL`], long enough that an absence's shorter
/// window expires well inside it.
///
/// FALSIFICATION: drop the `within_pacing` term from `StateReader::snapshot`'s
/// gate → a read inside either window re-reads the routing table → the
/// read-count assert reds. Stop pacing an absence (drop the `Acquired::Absent`
/// arm in `acquire`) → a script that stays unpublished re-reads every step →
/// red. Include an absence in `is_fresh` → the read after the absent deadline
/// is suppressed → red. Let `failed` keep an absence → a fail step after an
/// unpublished one serves `UnknownPublication` where the model expects the
/// store error → red. Fall through to the store instead of
/// `RefreshUnavailable` when paced with nothing held → red.
#[test]
fn prop_refresh_pacing() {
    fn property(script: PacingScript) -> Result<bool> {
        block_on(run_refresh_pacing(script))
    }
    QuickCheck::new().quickcheck(property as fn(PacingScript) -> Result<bool>);
}

async fn run_refresh_pacing(script: PacingScript) -> Result<bool> {
    let descriptor = value_state::<JsonCodec>("pacing-v");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");
    let tp = topic(GROUP_A);
    env.commit(GROUP_A, tp, &key, 1, |handle| async move {
        handle.set(element(0)).await.map_err(|e| eyre!("set: {e}"))
    })
    .await?;

    let (cache, mock) = mock_clock_cache(1 << 20);
    let reader = env.reader_with_interval(cache, PACING_INTERVAL)?;

    // Model state: elapsed mock time, when pacing next permits an attempt, when
    // the last refresh completed, and what it acquired.
    let mut elapsed = Duration::ZERO;
    let mut retry_after: Option<Duration> = None;
    let mut refreshed_at: Option<Duration> = None;
    let mut held = Held::Nothing;

    for step in script.steps {
        let advance = step.advance.duration();
        mock.increment(advance);
        elapsed += advance;
        if step.published {
            env.publish(GROUP_A, tp).await;
        } else {
            env.publications
                .remove_group(&env.sub, StateType::Application, &env.name, GROUP_A)
                .await
                .map_err(|e| eyre!("remove_group: {e}"))?;
        }
        if step.fail {
            env.publications.fail_reads_with(ErrorCategory::Transient);
        } else {
            env.publications.heal_reads();
        }

        let paced = retry_after.is_some_and(|deadline| elapsed < deadline);
        // An absence never counts as fresh: only its own shorter window paces
        // it, so a publisher that appears is admitted within `ABSENT_BACKOFF`.
        let fresh = held != Held::Absent
            && refreshed_at.is_some_and(|at| elapsed.saturating_sub(at) < PACING_INTERVAL);

        let (expect, expected_reads) = if paced || fresh {
            // Inside a window: serve what is held, touching no store.
            let served = match held {
                Held::Sources => Expect::Value(0),
                Held::Absent => Expect::UnknownPublication,
                Held::Nothing => Expect::RefreshUnavailable,
            };
            (served, 0)
        } else if step.fail {
            retry_after = Some(elapsed + REFRESH_BACKOFF);
            // Sources survive the outage; an unconfirmed absence does not.
            if held == Held::Absent {
                held = Held::Nothing;
                refreshed_at = None;
            }
            let served = match held {
                Held::Sources => Expect::Value(0),
                _ => Expect::ReadError,
            };
            (served, 1)
        } else if step.published {
            retry_after = None;
            refreshed_at = Some(elapsed);
            held = Held::Sources;
            (Expect::Value(0), 1)
        } else {
            retry_after = Some(elapsed + ABSENT_BACKOFF);
            refreshed_at = Some(elapsed);
            held = Held::Absent;
            (Expect::UnknownPublication, 1)
        };

        let reads_before = env.publications.reads();
        let observed = reader.get(key.clone()).await;
        let reads = env.publications.reads() - reads_before;
        if !outcome_matches(&expect, &observed) || reads != expected_reads {
            return Ok(false);
        }
    }
    Ok(true)
}

// --- Sticky identity mismatch -----------------------------------------------

/// A frozen identity that disagrees with the descriptor is sticky. Once a
/// source's identity mismatches, every read within the refresh interval
/// surfaces the Permanent `IdentityMismatch`. So does a failed refresh past
/// the interval. Neither ever falls back to the admitted, valid subset.
///
/// `GROUP_A` carries a matching identity, so it stays admitted. `GROUP_B`
/// carries a perturbed identity that disagrees with the descriptor. The
/// property above does not model a mismatched identity: doing so needs
/// precise interval and clock control. This test covers that case instead.
///
/// Falsify: drop the sticky-mismatch cached-path branch in
/// `StateReader::snapshot` → op 2 within the interval serves A's `Ok(None)`.
/// Drop the failed-read sticky arm in `refresh` → op 3 serves A's subset.
#[tokio::test]
async fn identity_mismatch_sticky() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("v-sticky");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");

    let tp_a = topic("topic-a");
    let tp_b = topic("topic-b");

    // A: matching identity → admitted.
    env.publish(GROUP_A, tp_a).await;
    // B: advertised, but its identity is perturbed to disagree with the descriptor.
    env.publications
        .seed(
            &env.sub,
            StateType::Application,
            &env.name,
            &StatePublication {
                group_id: Arc::from(GROUP_B),
                topic: tp_b,
                partition_count: env.count,
            },
        )
        .await;
    let mut perturbed = DurableDescriptorIdentity::from_identity(
        descriptor.state_type(),
        env.name.as_str(),
        &descriptor.structural_identity(),
    );
    perturbed.kind = perturbed.kind.wrapping_add(1);
    env.identities.seed(GROUP_B, &perturbed).await;

    let (cache, mock) = mock_clock_cache(1 << 20);
    // A non-zero interval so op 2 takes the cached-snapshot fast path.
    let reader = env.reader_with_interval(cache, Duration::from_mins(1))?;

    // Op 1, at t=0: the initial refresh detects B's mismatch. A is still admitted.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 1 expected IdentityMismatch, got {other:?}"),
    }
    let reads_after_first = env.publications.reads();
    // Op 2 (within the interval, cached fast path): the mismatch must still
    // surface, not A's admitted subset, and without a second routing read.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 2 (within interval) expected a sticky IdentityMismatch, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads(),
        reads_after_first,
        "a sticky fault must not re-read the routing table"
    );
    // Elapse the interval and make every routing read fail.
    mock.increment(Duration::from_mins(2));
    env.publications.fail_reads_with(ErrorCategory::Transient);
    // Op 3 (stale → refresh, read fails): the sticky mismatch outranks the
    // admitted subset even though the routing read failed.
    match reader.get(key).await {
        Err(StateReaderError::IdentityMismatch { .. }) => Ok(()),
        other => bail!("op 3 (failed refresh) expected a sticky IdentityMismatch, got {other:?}"),
    }
}

// --- Oversized routing table ------------------------------------------------

/// An oversized routing table is the other Permanent fault a refresh can find,
/// and it is sticky the same way: every read within the refresh interval
/// surfaces `TooManySources` without re-reading the table. Past the interval
/// the reader re-validates, so withdrawing a source clears the fault.
///
/// This is the twin of [`identity_mismatch_sticky`]. Both prove one rule: a
/// Permanent fault is cached like a snapshot, because only an operator can
/// clear it, while a Transient absence re-reads eagerly.
///
/// Falsify: propagate the rejection out of `StateReader::refresh` instead of
/// publishing it as a fault → op 2 re-reads the routing table → the read-count
/// assert reds. Publish it with no fault → op 2 falls through to
/// `UnknownPublication` → the op 2 match reds.
#[tokio::test]
async fn oversized_routing_table_is_sticky() -> Result<()> {
    let descriptor = value_state::<JsonCodec>("v-oversized");
    let env = ScriptedEnv::new(descriptor)?;
    let key = Key::from("user-1");

    // One more source than the reader admits, each with a matching frozen
    // identity, so admission succeeds and the bound is the only rejection.
    for i in 0..=MAX_PUBLICATION_SOURCES {
        env.publish(
            &format!("oversized-g{i}"),
            topic(&format!("oversized-t{i}")),
        )
        .await;
    }

    let (cache, mock) = mock_clock_cache(1 << 20);
    let reader = env.reader_with_interval(cache, Duration::from_mins(1))?;

    // Op 1, at t=0: the initial refresh reads the table and rejects it.
    match reader.get(key.clone()).await {
        Err(StateReaderError::TooManySources { found, max }) => {
            assert_eq!(found, MAX_PUBLICATION_SOURCES + 1);
            assert_eq!(max, MAX_PUBLICATION_SOURCES);
        }
        other => bail!("op 1 expected TooManySources, got {other:?}"),
    }
    let reads_after_first = env.publications.reads();
    // Op 2 (within the interval): the same Permanent fault, served from the
    // cached state.
    match reader.get(key.clone()).await {
        Err(StateReaderError::TooManySources { .. }) => {}
        other => bail!("op 2 (within interval) expected a sticky TooManySources, got {other:?}"),
    }
    assert_eq!(
        env.publications.reads(),
        reads_after_first,
        "a sticky fault must not re-read the routing table"
    );

    // Op 3, past the interval with one source withdrawn: the table is back
    // within the bound, so the re-validation clears the fault.
    mock.increment(Duration::from_mins(2));
    env.publications
        .remove_group(&env.sub, StateType::Application, &env.name, "oversized-g0")
        .await
        .map_err(|e| eyre!("remove_group: {e}"))?;
    match reader.get(key).await {
        Ok(None) => Ok(()),
        other => bail!("op 3 (withdrawn back within the bound) expected Ok(None), got {other:?}"),
    }
}
