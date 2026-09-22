//! Map batch queries and their aligned result model.

use super::*;

/// Population pool for the `Map::get_many` parity property — 20 distinct keys,
/// enough content to distinguish present, absent, and duplicate answers. Query
/// keys at/above it (see below) are guaranteed absent.
pub(super) const GET_MANY_POPULATE_KEYS: u8 = 20;
/// Query-key range `[lo, hi)`: it spans below `0` and at/above the populate
/// pool, so keys `< 0` and `>= 20` are guaranteed absent (never set), forcing
/// the absent-key path.
pub(super) const GET_MANY_QUERY_LO: i64 = -4;
pub(super) const GET_MANY_QUERY_HI: i64 = 24;
/// Max query-list length. Derived from [`CELL_BATCH`] rather than hand-numbered
/// so the "spans more than one sub-batch" claim below cannot rot when the store
/// batch width moves.
pub(super) const GET_MANY_MAX_QUERIES: usize = CELL_BATCH.get() + 32;

/// A random map population plus a random query list for the `Map::get_many`
/// parity property. The independent pools guarantee absent keys; a small query
/// range over a long list guarantees duplicates; the length cap crosses
/// `CELL_BATCH`, so a single call spans more than one sub-batch. `commit`
/// selects the read arm: the same event's dirty overlay, or a fresh event over
/// the committed base.
#[derive(Clone, Debug)]
pub(crate) struct MapGetManyInput {
    pub(super) entries: Vec<(i64, u8)>,
    pub(super) queries: Vec<i64>,
    pub(super) commit: bool,
}

impl Arbitrary for MapGetManyInput {
    fn arbitrary(g: &mut Gen) -> Self {
        let entries = capped_vec::<(u8, u8)>(g, 24)
            .into_iter()
            .map(|(k, b)| (i64::from(k % GET_MANY_POPULATE_KEYS), b))
            .collect();
        let span = GET_MANY_QUERY_HI - GET_MANY_QUERY_LO;
        let qlen = usize::arbitrary(g) % (GET_MANY_MAX_QUERIES + 1);
        let queries = (0..qlen)
            .map(|_| GET_MANY_QUERY_LO + i64::arbitrary(g).rem_euclid(span))
            .collect();
        Self {
            entries,
            queries,
            commit: bool::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let commit = self.commit;
        let entries = self.entries.clone();
        Box::new(self.queries.shrink().map(move |queries| Self {
            entries: entries.clone(),
            queries,
            commit,
        }))
    }
}

/// Map batch-read parity against the already-trusted point paths:
/// `get_many(queries)` answers each position exactly as `queries.map(get)`, and
/// `contains_many(queries)` answers each position as `queries.map(contains)`.
/// including duplicate, present, and absent keys, across the sub-batch
/// boundary. No TTL is in play and the JSON identity resolver is deterministic,
/// so the observation rules collapse to exact point-parity and this isolates
/// the batch plumbing (coordinate lowering, dedupe/scatter, sub-batch
/// concatenation, and ordered `buffered` resolution).
/// Proven over both the dirty-overlay arm (uncommitted) and the committed arm.
pub(crate) async fn run_map_get_many_parity_trace(input: MapGetManyInput) -> Result<bool> {
    let dedup = MemoryDeduplicationStore::default();
    let cells = MemoryCells::new();
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let descriptor = map_state::<I64KeyCodec, JsonCodec>("mp");
    let (registry, collection_ref) =
        registry_and_ref(&descriptor, "mp", &state_key, CollectionDef::new(None))?;
    let id = collection_ref.id();

    // Event 0: populate.
    let ev0 = EventRef::Message {
        dedup_id: Uuid::from_u128(0),
    };
    let session0 = make_session(&cells, &dedup, &registry, &state_key, ev0);
    let handle0 = descriptor.bind(&session0).map_err(|e| eyre!("bind: {e}"))?;
    for (k, b) in &input.entries {
        handle0.set(k, Value::from(*b)).await?;
    }

    // Read arm: the same (dirty) session, or a fresh event after committing 0.
    let batch;
    let presence;
    let mut point = Vec::with_capacity(input.queries.len());
    let mut point_presence = Vec::with_capacity(input.queries.len());
    if input.commit {
        finalize_and_promote(&session0, &dedup, event_dedup(ev0), &cells, id).await?;
        let ev1 = EventRef::Message {
            dedup_id: Uuid::from_u128(1),
        };
        let session1 = make_session(&cells, &dedup, &registry, &state_key, ev1);
        let handle1 = descriptor.bind(&session1).map_err(|e| eyre!("bind: {e}"))?;
        batch = handle1.get_many(&input.queries).await?;
        presence = handle1.contains_many(&input.queries).await?;
        for q in &input.queries {
            point.push(handle1.get(q).await?);
            point_presence.push(handle1.contains_key(q).await?);
        }
    } else {
        batch = handle0.get_many(&input.queries).await?;
        presence = handle0.contains_many(&input.queries).await?;
        for q in &input.queries {
            point.push(handle0.get(q).await?);
            point_presence.push(handle0.contains_key(q).await?);
        }
    }

    // Alignment is part of the contract: one answer per input position.
    if batch.len() != input.queries.len() {
        return Ok(false);
    }
    Ok(batch == point && presence == point_presence)
}
