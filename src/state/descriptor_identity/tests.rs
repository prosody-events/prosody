//! Descriptor-identity validation tests.
//!
//! The backend-generic store invariants (immutability, namespacing, concurrent
//! convergence) run here over the memory store via the shared
//! [`identity_suite`](crate::state::tests::identity_suite) runners, and again
//! over Cassandra in `state::cassandra::tests`. The `acquire`-flow tests pin
//! the orchestration on top: first-use registration, idempotent re-validation,
//! and the seed-stale-state path where a frozen row disagrees with the
//! registered descriptor (Permanent).

use super::{
    DescriptorIdentityError, DescriptorIdentityStore, DurableDescriptorIdentity, RegisterOutcome,
    acquire_descriptor_identities, validate,
};
use crate::error::{ClassifyError, ErrorCategory};
use crate::state::descriptor::{DescriptorIdentity, ValueDescriptor, value_state};
use crate::state::memory::{MemoryCellStore, MemoryDescriptorIdentityStore};
use crate::state::registry::{CollectionDef, CollectionDefRegistry};
use crate::state::tests::identity_suite::{
    IdentityTrace, run_concurrent_conflicting, run_concurrent_identical, run_identity_trace,
};
use crate::state::{StateName, StateType};
use color_eyre::eyre::{Result, eyre};
use futures::executor::block_on;
use quickcheck::{QuickCheck, TestResult};
use std::convert::Infallible;
use std::sync::Arc;
use uuid::Uuid;

fn cart() -> ValueDescriptor {
    value_state("cart")
}

/// A fresh group per call so concurrent test runs and quickcheck iterations
/// never collide on the process-shared memory store.
fn group() -> String {
    Uuid::new_v4().to_string()
}

/// The backend-generic store contract (immutability, namespacing, idempotence)
/// over the memory store. The Cassandra instantiation in
/// `state::cassandra::tests` runs the same runner.
#[test]
fn prop_memory_identity_trace() {
    fn prop(trace: IdentityTrace) -> TestResult {
        let store = MemoryDescriptorIdentityStore::new();
        match block_on(run_identity_trace(&store, &group(), trace)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::failed(),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(IdentityTrace) -> TestResult);
}

/// N concurrent registrations of one identity converge on exactly one
/// `Applied`, every other caller validating the winner.
#[test]
fn prop_memory_concurrent_identical_registration() {
    fn prop(key_seed: u8, ident_seed: u8, n: u8) -> TestResult {
        let store = MemoryDescriptorIdentityStore::new();
        let n = 1 + usize::from(n % 8);
        match block_on(run_concurrent_identical(
            &store,
            &group(),
            key_seed,
            ident_seed,
            n,
        )) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("concurrent identical registration did not converge"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(u8, u8, u8) -> TestResult);
}

/// Two concurrent registrations of differing identities: one wins, the loser
/// sees the winner.
#[test]
fn prop_memory_concurrent_conflicting_registration() {
    fn prop(key_seed: u8) -> TestResult {
        let store = MemoryDescriptorIdentityStore::new();
        match block_on(run_concurrent_conflicting(&store, &group(), key_seed)) {
            Ok(true) => TestResult::passed(),
            Ok(false) => TestResult::error("conflicting registration did not converge on a winner"),
            Err(error) => TestResult::error(format!("{error:?}")),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(u8) -> TestResult);
}

/// Wire-format freeze: the `keyed_state_identity` row's discriminants are a
/// durable contract compared on every read, so changing any value silently
/// bricks existing collections (a renamed variant still round-trips). Pin the
/// literals so such a change fails loudly here, not in production. The format
/// tokens are frozen in their own codecs' tests.
#[test]
fn durable_identity_wire_contract_is_frozen() {
    use crate::state::CollectionKindId;

    assert_eq!(i8::from(StateType::Application), 0);
    // `Framework` is test-only today, but its discriminant is frozen into
    // test-written identity rows and must stay reserved.
    assert_eq!(i8::from(StateType::Framework), 1);
    assert_eq!(i8::from(CollectionKindId::Value), 1);
    assert_eq!(i8::from(CollectionKindId::Map), 2);
    assert_eq!(i8::from(CollectionKindId::Deque), 3);
    // Value is single-cell: its key axis is the unit codec, and that token must
    // stay frozen or existing Value collections silently brick.
    assert_eq!(cart().structural_identity().key_format_id, "unit.v1");
}

/// An empty registry does no identity I/O and succeeds — the inert state layer.
#[tokio::test]
async fn empty_registry_does_no_io() -> Result<()> {
    let store = MemoryDescriptorIdentityStore::new();
    let registry = CollectionDefRegistry::default();
    acquire_descriptor_identities(&store, &registry, &group())
        .await
        .map_err(|e| eyre!("{e}"))?;
    Ok(())
}

/// Acquisition registers a first-seen collection, then re-validates it
/// idempotently against the now-present row — the steady-state path every
/// later process takes.
#[tokio::test]
async fn acquire_registers_first_use_then_validates() -> Result<()> {
    let store = MemoryDescriptorIdentityStore::new();
    let group = group();
    let mut registry = CollectionDefRegistry::default();
    registry.register(&cart(), CollectionDef::new(None))?;

    // First acquire registers via the (memory) insert-if-absent.
    acquire_descriptor_identities(&store, &registry, &group)
        .await
        .map_err(|e| eyre!("first acquire failed: {e}"))?;
    let stored = store
        .read_identity(&group, StateType::Application, "cart")
        .await?;
    assert_eq!(
        stored,
        Some(DurableDescriptorIdentity::from_identity(
            StateType::Application,
            "cart",
            &cart().structural_identity(),
        )),
        "first acquire must freeze the registered identity",
    );

    // Second acquire takes the read-and-match path; idempotent.
    acquire_descriptor_identities(&store, &registry, &group)
        .await
        .map_err(|e| eyre!("second acquire failed: {e}"))?;
    Ok(())
}

/// Seed-stale-state: a frozen row that disagrees with the registered
/// descriptor (in any of `kind` / `format_id` / `key_format_id`) makes
/// acquisition fail `Permanent` — the row is written through the low-level
/// store API, the way normal execution could not produce it. Property over the
/// differing axis.
#[test]
fn prop_acquire_rejects_seeded_mismatch() {
    fn prop(kind: i8, format_sel: u8, key_format_sel: u8) -> TestResult {
        // cart's real identity is kind=1 (Value), format="json",
        // key format="unit.v1".
        let format_id = ["json", "binary", "legacy"][usize::from(format_sel) % 3].to_owned();
        let key_format_id =
            ["unit.v1", "utf8.v1", "i64.v1"][usize::from(key_format_sel) % 3].to_owned();
        let stale = DurableDescriptorIdentity {
            state_type: StateType::Application.into(),
            name: "cart".to_owned(),
            kind,
            format_id,
            key_format_id,
        };
        // Skip the (rare) case where the generated row equals cart's identity:
        // then there is no mismatch to detect.
        let cart_identity = DurableDescriptorIdentity::from_identity(
            StateType::Application,
            "cart",
            &cart().structural_identity(),
        );
        if stale == cart_identity {
            return TestResult::discard();
        }

        let mut registry = CollectionDefRegistry::default();
        if registry
            .register(&cart(), CollectionDef::new(None))
            .is_err()
        {
            return TestResult::error("cart registration failed");
        }
        let store = MemoryDescriptorIdentityStore::new();
        let g = group();
        // Seed the stale row directly, then acquire with the real descriptor.
        let Ok(RegisterOutcome::Applied) = block_on(store.register_identity(&g, &stale)) else {
            return TestResult::error("seeding the stale row failed");
        };
        let outcome = block_on(acquire_descriptor_identities(&store, &registry, &g));
        match outcome {
            Err(error @ DescriptorIdentityError::Mismatch { .. }) => {
                if error.classify_error() == ErrorCategory::Permanent {
                    TestResult::passed()
                } else {
                    TestResult::error("identity mismatch must classify Permanent")
                }
            }
            Err(other) => TestResult::error(format!("expected Mismatch, got {other:?}")),
            Ok(()) => TestResult::error("a stale identity row must fail acquisition"),
        }
    }
    QuickCheck::new().quickcheck(prop as fn(i8, u8, u8) -> TestResult);
}

/// `key_format_id` is part of the frozen identity: two rows identical but for
/// it are distinct. The first registrant freezes its row; a later registration
/// of the differing row sees the *original* in its `Conflict`, and validating
/// the differing asserted identity against the frozen row is a `Permanent`
/// mismatch. This isolates the derived-`PartialEq` pickup of the field at the
/// `validate` locus — a property varying many axes at once cannot prove this
/// one field alone moves the needle.
#[tokio::test]
async fn key_format_id_alone_is_an_identity_mismatch() -> Result<()> {
    let store = MemoryDescriptorIdentityStore::new();
    let group = group();
    let frozen = DurableDescriptorIdentity {
        state_type: StateType::Application.into(),
        name: "cart".to_owned(),
        kind: 1,
        format_id: "json".to_owned(),
        key_format_id: "unit.v1".to_owned(),
    };
    let asserted = DurableDescriptorIdentity {
        key_format_id: "utf8.v1".to_owned(),
        ..frozen.clone()
    };
    assert_ne!(
        frozen, asserted,
        "key_format_id alone must distinguish two identities",
    );

    // The first registrant freezes `frozen`; the second sees it in the conflict.
    assert_eq!(
        store.register_identity(&group, &frozen).await?,
        RegisterOutcome::Applied,
    );
    assert_eq!(
        store.register_identity(&group, &asserted).await?,
        RegisterOutcome::Conflict(frozen.clone()),
        "the conflict must carry the first-frozen row, key_format_id and all",
    );

    // Validating the differing asserted identity against the frozen row is a
    // Permanent mismatch — the validate locus picks up the field.
    let Err(mismatch) = validate::<Infallible>(frozen, &asserted) else {
        return Err(eyre!("differing key_format_id must fail validation"));
    };
    assert!(matches!(mismatch, DescriptorIdentityError::Mismatch { .. }));
    assert_eq!(mismatch.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// The headline cross-process safety property: two deploys registering the
/// **same** collection with **different** identities race first-use
/// registration against one group; exactly one acquires successfully and the
/// other fails `Permanent`. This drives the register-`Conflict`→`validate`
/// route directly (both start with the row absent, so both attempt the LWT —
/// the loser validates the winner's echoed row), the cross-process race the
/// seeded-mismatch property cannot reach via the read-present path.
#[tokio::test]
async fn concurrent_acquire_with_conflicting_identities_one_wins_one_permanent() -> Result<()> {
    use crate::codec::{I64Codec, JsonCodec};

    let store = MemoryDescriptorIdentityStore::new();
    let group = group();

    // Same name, different format ("json" vs "i64-be") ⇒ different identity.
    let mut json_registry = CollectionDefRegistry::default();
    json_registry.register(&value_state::<JsonCodec>("cart"), CollectionDef::new(None))?;
    let mut i64_registry = CollectionDefRegistry::default();
    i64_registry.register(&value_state::<I64Codec>("cart"), CollectionDef::new(None))?;

    // Race both acquires against the shared store — concurrency by joining, no
    // sleep. Whichever registers first wins; the loser reads its conflict.
    let (json_result, i64_result) = futures::join!(
        acquire_descriptor_identities(&store, &json_registry, &group),
        acquire_descriptor_identities(&store, &i64_registry, &group),
    );

    let oks = [&json_result, &i64_result]
        .into_iter()
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(oks, 1, "exactly one deploy may freeze the identity");

    let loser = [json_result, i64_result]
        .into_iter()
        .find_map(Result::err)
        .ok_or_else(|| eyre!("one deploy must lose"))?;
    assert!(
        matches!(loser, DescriptorIdentityError::Mismatch { .. }),
        "the loser must see an identity mismatch, got {loser:?}",
    );
    assert_eq!(loser.classify_error(), ErrorCategory::Permanent);
    Ok(())
}

/// Namespacing extends to cells: a `(state_type, name)` change yields a
/// distinct durable cell key, so an Application and a Framework collection
/// sharing a name never read each other's value.
#[tokio::test]
async fn state_type_namespaces_cells() -> Result<()> {
    use crate::state::cell::Committed;
    use crate::state::cell_key::{CellKey, Coordinate, Section};
    use crate::state::memory::MemoryCells;
    use crate::state::store::CellStore;
    use crate::state::tests::cell_suite::ScriptedOracle;
    use crate::state::{CollectionId, CollectionRef, EventRef, StateKey};
    use bytes::Bytes;

    let store = MemoryCellStore::new(
        MemoryCells::new(),
        ScriptedOracle::default(),
        Arc::new(CollectionDefRegistry::default()),
    );
    let key: crate::Key = Arc::from("k");
    let state_key = StateKey::new(Uuid::new_v4(), key);
    let name = StateName::try_new("cart")?;
    let cell = CellKey {
        section: Section::new(0),
        coordinate: Coordinate::empty(),
    };
    let app = CollectionRef::new(
        CollectionId::new(state_key.clone(), StateType::Application, name.clone()),
        None,
    );
    let fw = CollectionRef::new(
        CollectionId::new(state_key, StateType::Framework, name),
        None,
    );

    store
        .write_resolved(
            &app,
            &[(cell.clone(), Some(Bytes::from_static(b"app")))],
            &[],
        )
        .await?;
    store
        .write_resolved(&fw, &[(cell.clone(), Some(Bytes::from_static(b"fw")))], &[])
        .await?;

    // A resolved cell never consults the oracle, so the probe event is inert.
    let probe = EventRef::Message {
        dedup_id: Uuid::from_u128(0),
    };
    assert_eq!(
        store.get(app.id(), &cell, probe).await?,
        Committed::new(Some(Bytes::from_static(b"app"))),
    );
    assert_eq!(
        store.get(fw.id(), &cell, probe).await?,
        Committed::new(Some(Bytes::from_static(b"fw"))),
        "the framework-namespaced cell holds its own value",
    );
    Ok(())
}
