//! Snapshot acquisition and the three-outcome refresh rule.
//!
//! [`prop_snapshot_refresh`] proves the whole rule over random refresh scripts.
//! Withdrawals apply unconditionally. An already-admitted source is never
//! re-validated: the property proves this by counting identity-store reads.
//! A failed routing read keeps the prior snapshot. An emptied routing table
//! fails with `UnknownPublication`. A table whose every new source lacks a
//! frozen identity fails with `IdentityUnavailable`.
//!
//! The focused example [`identity_mismatch_sticky`] covers the one case the
//! property does not: an identity that is present but disagrees with the
//! descriptor. That needs precise interval and clock control the property
//! does not model.
//!
//! Every reader here refreshes on every operation (`new_eager`), except the
//! sticky example, which uses `new_with_interval` for the cached fast path.

use super::support::{
    CountingIdentityStore, GROUP_A, GROUP_B, ScriptedEnv, fixed_clock_cache, topic,
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
use crate::subsystem::SubsystemName;
use color_eyre::eyre::{Result, bail, eyre};
use futures::executor::block_on;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use serde_json::Value;
use std::iter::{empty, once};
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
/// read delta exceeds the model's newly-admitted count → red.
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
    let reader = env.reader_eager()?;

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
    }
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

    let (cache, clock) = fixed_clock_cache(1 << 20);
    // A non-zero interval so op 2 takes the cached-snapshot fast path.
    let reader = env.reader_with_interval(cache, 60_000)?;

    // Op 1, at t=0: the initial refresh detects B's mismatch. A is still admitted.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 1 expected IdentityMismatch, got {other:?}"),
    }
    // Op 2 (within the interval, cached fast path): the mismatch must still
    // surface, not A's admitted subset.
    match reader.get(key.clone()).await {
        Err(StateReaderError::IdentityMismatch { .. }) => {}
        other => bail!("op 2 (within interval) expected a sticky IdentityMismatch, got {other:?}"),
    }
    // Elapse the interval and make every routing read fail.
    clock.store(120_000, Ordering::Relaxed);
    env.publications.fail_reads_with(ErrorCategory::Transient);
    // Op 3 (stale → refresh, read fails): the sticky mismatch outranks the
    // admitted subset even though the routing read failed.
    match reader.get(key).await {
        Err(StateReaderError::IdentityMismatch { .. }) => Ok(()),
        other => bail!("op 3 (failed refresh) expected a sticky IdentityMismatch, got {other:?}"),
    }
}
