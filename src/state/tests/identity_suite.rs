//! Backend-generic descriptor-identity store suite.
//!
//! One set of runners over any [`DescriptorIdentityStore`], instantiated by the
//! memory suite (`state::descriptor_identity::tests`, `QUICKCHECK_TESTS`) and
//! the Cassandra suite (`state::cassandra::tests`, `INTEGRATION_TESTS`). Every
//! backend must satisfy the same invariants:
//!
//! * **Immutability** — once a `(group, state_type, name)` row is registered,
//!   no later `register_identity` overwrites it; the loser sees the *original*
//!   identity ([`RegisterOutcome::Conflict`]), and reads return it unchanged.
//! * **Namespacing** — the model keys on `(state_type, name)`, so the same name
//!   under two `state_type`s is two independent rows. The trace pool spans both
//!   namespaces, so a divergence would surface as a model mismatch.
//! * **Concurrent convergence** — N concurrent registrations of one identity
//!   yield exactly one `Applied`; every other caller validates the winner's
//!   row. Differing identities converge on whichever wins, and the loser sees
//!   the winner — the basis for first-use racing across partition owners.
//!
//! The model is a plain `HashMap`, never a re-implementation of the store. The
//! trace asserts equivalence after **every** operation.

use crate::state::StateType;
use crate::state::descriptor_identity::{
    DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
};
use crate::state::tests::cell_suite::capped_vec;
use color_eyre::eyre::{Result, eyre};
use futures::future::join_all;
use quickcheck::{Arbitrary, Gen};
use std::collections::HashMap;

/// The two namespaces and three names the trace pool spans, so the same name
/// recurs across namespaces (namespacing) and registrations collide on a key
/// (immutability).
const NAMES: [&str; 3] = ["c0", "c1", "c2"];

/// Resolves a key seed to a `(state_type, name)` collection key from the pool.
fn key_for(seed: u8) -> (StateType, &'static str) {
    let state_type = if seed & 1 == 0 {
        StateType::Application
    } else {
        StateType::Framework
    };
    (state_type, NAMES[usize::from(seed >> 1) % NAMES.len()])
}

/// Builds the wire identity row a `(key, identity)` seed pair names. The
/// `kind`/`format_id`/`key_format_id` axes vary independently (including
/// unknown discriminants) so collisions on a key carry genuinely different
/// identities.
fn row_for(key_seed: u8, ident_seed: u8) -> DurableDescriptorIdentity {
    let (state_type, name) = key_for(key_seed);
    let kind = [1_i8, 2, 7][usize::from(ident_seed) % 3];
    let format_id = ["json", "binary", "message-ref"][usize::from(ident_seed >> 2_u8) % 3];
    let key_format_id = ["unit.v1", "utf8.v1"][usize::from((ident_seed >> 6_u8) & 1)];
    DurableDescriptorIdentity {
        state_type: state_type.into(),
        name: name.to_owned(),
        kind,
        format_id: format_id.to_owned(),
        key_format_id: key_format_id.to_owned(),
    }
}

/// One store operation in an [`IdentityTrace`].
#[derive(Clone, Debug)]
enum IdentityOp {
    /// First-use registration of `(key, identity)`.
    Register { key: u8, ident: u8 },

    /// Point-read of `key`.
    Read { key: u8 },
}

impl Arbitrary for IdentityOp {
    fn arbitrary(g: &mut Gen) -> Self {
        if bool::arbitrary(g) {
            Self::Register {
                key: u8::arbitrary(g),
                ident: u8::arbitrary(g),
            }
        } else {
            Self::Read {
                key: u8::arbitrary(g),
            }
        }
    }
}

/// A bounded random sequence of identity-store operations.
#[derive(Clone, Debug)]
pub(crate) struct IdentityTrace(Vec<IdentityOp>);

impl Arbitrary for IdentityTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(capped_vec(g, 24))
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        // Shrinking the op vector (dropping ops) reduces a failing trace to a
        // minimal reproduction.
        Box::new(self.0.shrink().map(Self))
    }
}

/// Drives `store` and a plain `HashMap` model through `trace` under `group`,
/// asserting equivalence after every op. Returns `Ok(false)` on a model
/// divergence (a real invariant break); store errors propagate.
pub(crate) async fn run_identity_trace<St>(
    store: &St,
    group: &str,
    trace: IdentityTrace,
) -> Result<bool>
where
    St: DescriptorIdentityStore,
{
    let mut model: HashMap<(i8, String), DurableDescriptorIdentity> = HashMap::new();
    for op in trace.0 {
        match op {
            IdentityOp::Register { key, ident } => {
                let row = row_for(key, ident);
                let model_key = (row.state_type, row.name.clone());
                let outcome = store
                    .register_identity(group, &row)
                    .await
                    .map_err(|e| eyre!("register_identity failed: {e}"))?;
                match model.get(&model_key) {
                    None => {
                        if outcome != RegisterOutcome::Applied {
                            return Ok(false);
                        }
                        model.insert(model_key, row);
                    }
                    // A present key must never be overwritten: the conflict
                    // carries the *first* registered identity, not the newest.
                    Some(frozen) => {
                        if outcome != RegisterOutcome::Conflict(frozen.clone()) {
                            return Ok(false);
                        }
                    }
                }
            }
            IdentityOp::Read { key } => {
                let (state_type, name) = key_for(key);
                let got = store
                    .read_identity(group, state_type, name)
                    .await
                    .map_err(|e| eyre!("read_identity failed: {e}"))?;
                if got.as_ref() != model.get(&(state_type.into(), name.to_owned())) {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Fires `n` concurrent registrations of one identity (generated from the
/// seeds) under `group`: exactly one must apply and every other caller must
/// validate the winner's row. Concurrency comes from joining the futures — no
/// sleep, no timing assertion.
pub(crate) async fn run_concurrent_identical<St>(
    store: &St,
    group: &str,
    key_seed: u8,
    ident_seed: u8,
    n: usize,
) -> Result<bool>
where
    St: DescriptorIdentityStore,
{
    let row = row_for(key_seed, ident_seed);
    let outcomes = join_all((0..n).map(|_| store.register_identity(group, &row))).await;
    let mut applied = 0_usize;
    for outcome in outcomes {
        match outcome.map_err(|e| eyre!("register_identity failed: {e}"))? {
            RegisterOutcome::Applied => applied += 1,
            RegisterOutcome::Conflict(existing) => {
                if existing != row {
                    return Ok(false);
                }
            }
        }
    }
    Ok(applied == 1)
}

/// Fires two concurrent registrations of *different* identities on the same
/// key: exactly one applies and the loser's conflict carries the winner's row
/// (never its own attempted identity). This is the differing-identity race
/// across partition owners.
pub(crate) async fn run_concurrent_conflicting<St>(
    store: &St,
    group: &str,
    key_seed: u8,
) -> Result<bool>
where
    St: DescriptorIdentityStore,
{
    // Two rows on the same key with deliberately different identities.
    let a = row_for(key_seed, 0);
    let b = DurableDescriptorIdentity {
        format_id: "deliberately-different".to_owned(),
        ..a.clone()
    };
    if a == b {
        return Err(eyre!("test rows must differ"));
    }
    let outcomes = join_all([
        store.register_identity(group, &a),
        store.register_identity(group, &b),
    ])
    .await;

    let mut applied = Vec::new();
    let mut conflicts = Vec::new();
    for outcome in outcomes {
        match outcome.map_err(|e| eyre!("register_identity failed: {e}"))? {
            RegisterOutcome::Applied => applied.push(()),
            RegisterOutcome::Conflict(existing) => conflicts.push(existing),
        }
    }
    // Exactly one winner, one loser, and the loser saw a row equal to one of
    // the two attempts (the winner's) — never a torn or absent identity.
    let one_winner = applied.len() == 1 && conflicts.len() == 1;
    let loser_saw_winner = conflicts.first().is_some_and(|c| *c == a || *c == b);
    Ok(one_winner && loser_saw_winner)
}
