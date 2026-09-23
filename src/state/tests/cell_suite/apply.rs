//! Apply idempotence of committed and aborted stages.

use super::*;

/// One step of the generated apply sequence.
#[derive(Clone, Copy, Debug)]
enum ApplyOp {
    /// Resolves the marker as one unit, including an already settled marker.
    ResolveMarker,
    /// Re-apply the verdict-matching settle over the full staged set.
    Settle,
    /// Reads one cell through the committed-value resolver.
    ReadCell(u8),
}

impl Arbitrary for ApplyOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 3 {
            0 => Self::ResolveMarker,
            1 => Self::Settle,
            _ => Self::ReadCell(u8::arbitrary(g)),
        }
    }
}

/// Generated input for the apply-idempotence property: a committed pre-clear
/// base, one event's staged set + cleared sections (survivors frozen from the
/// staged set), a verdict, and a shuffled interleaving of re-applies and
/// reads.
#[derive(Clone, Debug)]
pub(crate) struct ApplyTrace {
    /// Committed base rows as `(section idx, coord, value)`.
    base: Vec<(u8, u8, u8)>,
    /// The staged set as `(section idx, coord, mutation)`.
    staged: Vec<(u8, u8, Mutation)>,
    /// Section indices the stage durably clears (deduped).
    cleared: Vec<u8>,
    committed: bool,
    ops: Vec<ApplyOp>,
}

impl Arbitrary for ApplyTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        let base = capped_vec::<(u8, u8, u8)>(g, 8)
            .into_iter()
            .map(|(s, c, v)| (section_idx(s), c % CELLS, v))
            .collect();
        let staged = capped_vec::<(u8, u8, Mutation)>(g, 6)
            .into_iter()
            .map(|(s, c, m)| (section_idx(s), c % CELLS, m))
            .collect();
        let mut cleared: Vec<u8> = capped_vec::<u8>(g, 2)
            .into_iter()
            .map(section_idx)
            .collect();
        cleared.sort_unstable();
        cleared.dedup();
        Self {
            base,
            staged,
            cleared,
            committed: bool::arbitrary(g),
            ops: capped_vec(g, 12),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let this = self.clone();
        let ops = self.ops.shrink().map({
            let this = this.clone();
            move |ops| Self {
                ops,
                ..this.clone()
            }
        });
        let base = self.base.shrink().map({
            let this = this.clone();
            move |base| Self {
                base,
                ..this.clone()
            }
        });
        let staged = self.staged.shrink().map(move |staged| Self {
            staged,
            ..this.clone()
        });
        Box::new(ops.chain(base).chain(staged))
    }
}

/// Proves that repeated settlement produces the same final state.
///
/// The test varies operation order and the event result.
/// The final state must contain no unsettled marker or provisional cell.
pub(crate) async fn run_apply_idempotence<S, P>(
    store: S,
    input: ApplyTrace,
    probe: &P,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("apply")?,
    );
    let collection = CollectionRef::new(id.clone(), None);
    let event = EventRef::Message {
        dedup_id: Uuid::from_u128(1),
    };

    // Committed pre-clear base (last-writer-wins per row).
    let mut base: BTreeMap<(u8, u8), Bytes> = BTreeMap::new();
    for &(s, c, v) in &input.base {
        base.insert((s, c), bytes(v));
    }
    let seed: Vec<(CellKey, Option<Bytes>)> = base
        .iter()
        .map(|(&(s, c), value)| (cell_in(s, c), Some(value.clone())))
        .collect();
    if !seed.is_empty() {
        store.write_resolved(&collection, &seed, &[]).await?;
    }

    // Stage the event's writes (prev = the committed base) + frozen clears.
    let mut staged_map: BTreeMap<(u8, u8), Mutation> = BTreeMap::new();
    for &(s, c, mutation) in &input.staged {
        staged_map.insert((s, c), mutation);
    }
    let writes: Vec<(CellKey, ProvisionalWrite)> = staged_map
        .iter()
        .map(|(&(s, c), mutation)| {
            let prev = Committed::new(base.get(&(s, c)).cloned());
            (
                cell_in(s, c),
                ProvisionalWrite::new(mutation.value(), prev, event),
            )
        })
        .collect();
    let clears: Vec<SectionClear> = input
        .cleared
        .iter()
        .map(|&s| SectionClear::frozen(SECTIONS[s as usize], &writes))
        .collect();
    if writes.is_empty() && clears.is_empty() {
        // Nothing staged and nothing cleared — no marker is written, so the
        // apply machinery is a no-op. Verify rather than skip: the committed
        // base must survive untouched (no marker, no provisional residue, and
        // every base row still reads its seeded value).
        let keys: BTreeSet<(u8, u8)> = base.keys().copied().collect();
        return assert_apply_settled(&store, probe, &id, &base, &keys).await;
    }
    let marker = EventMarker::frozen(event, &writes, clears.clone(), &evidence([].into(), None));
    store
        .write_provisional(&collection, listed(&marker, &writes)?)
        .await?;
    if input.committed {
        seed_commit_evidence(&store, &collection).await?;
    }

    // The generated interleaving, then one final verdict-matching settle so
    // every schedule ends fully settled.
    for op in &input.ops {
        match op {
            ApplyOp::ResolveMarker => {
                resolve_event_marker(
                    &store,
                    &collection,
                    &marker,
                    if input.committed {
                        CommitDecision::Committed
                    } else {
                        CommitDecision::NotCommitted
                    },
                )
                .await?;
            }
            ApplyOp::Settle => {
                reapply_settle(&store, &collection, input.committed, &writes, &marker).await?;
            }
            ApplyOp::ReadCell(i) => {
                let Some((cell, _)) = writes.get(*i as usize % writes.len().max(1)) else {
                    continue;
                };
                if let Some(provisional) = store.provisional_cell_at(&id, cell).await? {
                    EvidenceLookup::new(&store, collection.id())
                        .resolve(Provisional(provisional))
                        .await?;
                }
            }
        }
    }
    reapply_settle(&store, &collection, input.committed, &writes, &marker).await?;

    // The verdict state: committed ⇒ cleared sections collapse to survivors
    // and staged mutations land; aborted ⇒ exactly the pre-stage base.
    let keys: BTreeSet<(u8, u8)> = base.keys().chain(staged_map.keys()).copied().collect();
    let expected = apply_model(base, &staged_map, &input);
    assert_apply_settled(&store, probe, &id, &expected, &keys).await
}

/// The verdict-matching settle re-apply ([`run_apply_idempotence`]'s
/// idempotent subject): commit with the frozen clears, or abort.
async fn reapply_settle<S>(
    store: &S,
    collection: &CollectionRef,
    committed: bool,
    writes: &[(CellKey, ProvisionalWrite)],
    marker: &EventMarker,
) -> Result<(), S::Error>
where
    S: CellStore,
{
    if committed {
        store.commit_provisional(collection, marker, writes).await
    } else {
        store.abort_provisional(collection, writes).await
    }
}

/// [`run_apply_idempotence`]'s postconditions: the physical row shape equals
/// the verdict state exactly, no marker stands, nothing is provisional, and
/// every touched key's committed projection matches.
async fn assert_apply_settled<S, P>(
    store: &S,
    probe: &P,
    id: &CollectionId,
    expected: &BTreeMap<(u8, u8), Bytes>,
    keys: &BTreeSet<(u8, u8)>,
) -> Result<bool>
where
    S: CellStore,
    P: ShapeProbe,
{
    let present: RowKeys = expected
        .keys()
        .map(|&(s, c)| row_key(&cell_in(s, c)))
        .collect();
    if probe.cell_rows(id).await? != present {
        return Ok(false);
    }
    if probe.unsettled_marker(id).await?.is_some() {
        return Ok(false);
    }
    if !probe.provisional_rows(id).await?.is_empty() {
        return Ok(false);
    }
    for &(s, c) in keys {
        let committed = CellRead::<Values>::read(store, id, cell_in(s, c).as_ref())
            .await?
            .0;
        if committed.into_inner() != expected.get(&(s, c)).cloned() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn apply_model(
    base: BTreeMap<(u8, u8), Bytes>,
    staged_map: &BTreeMap<(u8, u8), Mutation>,
    input: &ApplyTrace,
) -> BTreeMap<(u8, u8), Bytes> {
    let mut expected = base;
    if input.committed {
        for &s in &input.cleared {
            expected.retain(|&(sect, _), _| sect != s);
        }
        for (&(s, c), mutation) in staged_map {
            match mutation.value() {
                Some(value) => {
                    expected.insert((s, c), value);
                }
                None => {
                    expected.remove(&(s, c));
                }
            }
        }
    }
    expected
}
