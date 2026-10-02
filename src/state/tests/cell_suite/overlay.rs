//! The overlay point merge over a multi-cell collection.

use super::*;

/// One op on a multi-cell collection view, intermixing point reads, range
/// scans, dirty buffering, section clears, and committed writes so the
/// property exercises their interaction — a `scan` between a `buffer_set` and
/// a `clear`, a `get` after a dropped scan, a `clear_section` between commits
/// and buffers, and so on (TESTING.md "interleaved operations").
#[derive(Clone, Debug)]
enum OverlayOp {
    /// Buffer a set into the dirty leg at `(section idx, coord)`.
    BufferSet(u8, u8, u8),
    /// Buffer a clear into the dirty leg.
    BufferClear(u8, u8),
    /// Commit a present value to the committed lower store (resolved).
    CommitSet(u8, u8, u8),
    /// Commit a known-absent value to the lower store (resolved).
    CommitClear(u8, u8),
    /// Buffer a dirty clear marker over one sampled section: that section's
    /// committed cells vanish behind it (even ones committed later) while its
    /// siblings stay visible; post-clear `BufferSet`s are survivors.
    ClearSection(u8),
    /// Commit a durable section clear with survivors to the lower store
    /// (`write_resolved` with a frozen [`SectionClear`]) — the generated
    /// producer for the direct-apply clear leg beneath the overlay. On a
    /// `Cached` lower store this is the op that kills a missing
    /// `write_resolved` section-clear cache guard section delete: a warm
    /// pre-clear cell of the cleared section would serve stale and diverge
    /// on the next point leg.
    CommitClearSection(SeedClear),
    /// Run a range scan and assert it against the oracle (the range leg,
    /// intermixed with the point reads asserted after every op).
    Scan(ScanReq),
}

impl Arbitrary for OverlayOp {
    fn arbitrary(g: &mut Gen) -> Self {
        let s = section_idx(u8::arbitrary(g));
        let c = u8::arbitrary(g) % CELLS;
        match u8::arbitrary(g) % 7 {
            0 => Self::BufferSet(s, c, u8::arbitrary(g)),
            1 => Self::BufferClear(s, c),
            2 => Self::CommitSet(s, c, u8::arbitrary(g)),
            3 => Self::CommitClear(s, c),
            4 => Self::ClearSection(s),
            5 => Self::CommitClearSection(SeedClear::arbitrary(g)),
            _ => Self::Scan(ScanReq::arbitrary(g)),
        }
    }
}

/// A shrinkable overlay trace.
#[derive(Clone, Debug)]
pub(crate) struct OverlayTrace {
    ops: Vec<OverlayOp>,
}

impl Arbitrary for OverlayTrace {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            ops: capped_vec(g, MAX_TRACE_OPS),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        Box::new(self.ops.shrink().map(|ops| Self { ops }))
    }
}

/// The visible-value model over the sampled section pool: dirty wins
/// (`Set`→present, `Cleared`→absent), an unsettled dirty clear marker hides the
/// committed leg **of its section only**, else the committed value (present or
/// absent). Keys are `(section idx, coord)`.
#[derive(Default)]
pub(super) struct CellModel {
    pub(super) committed: BTreeMap<(u8, u8), Option<Bytes>>,
    pub(super) dirty: BTreeMap<(u8, u8), Option<Bytes>>,
    /// The section indices over which a dirty clear marker stands — each
    /// hides its own section's committed leg, and only that section's.
    pub(super) cleared: BTreeSet<u8>,
}

impl CellModel {
    /// The visible committed bytes at `(section idx s, coordinate c)`.
    fn visible(&self, s: u8, c: u8) -> Option<Bytes> {
        match self.dirty.get(&(s, c)) {
            Some(value) => value.clone(),
            None if self.cleared.contains(&s) => None,
            None => self.committed.get(&(s, c)).cloned().flatten(),
        }
    }

    /// Section `s`'s visible cells in coordinate order (only present values)
    /// — a scan reads ONE section.
    pub(super) fn visible_ordered(&self, s: u8) -> Vec<(u8, Bytes)> {
        let mut coords: Vec<u8> = self
            .committed
            .keys()
            .chain(self.dirty.keys())
            .filter(|&&(sect, _)| sect == s)
            .map(|&(_, c)| c)
            .collect();
        coords.sort_unstable();
        coords.dedup();
        coords
            .into_iter()
            .filter_map(|c| self.visible(s, c).map(|b| (c, b)))
            .collect()
    }
}

/// Checks interleaved operations on a multi-cell [`Overlay`] against a
/// sorted-map oracle. The trace mixes dirty writes, section clears,
/// committed writes, and scans with varied bounds, direction, and fetch hints.
/// Each scan checks the visible range, including early stops.
/// After every operation, point reads check every cell in each sampled section.
/// Dirty writes win, and clears hide only their own section's lower cells.
/// [`SECTIONS`] sampling detects a marker read from the wrong section.
pub(crate) async fn run_overlay_trace<S>(lower: S, trace: OverlayTrace) -> Result<bool>
where
    S: CellStore,
{
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), lower);
    let mut model = CellModel::default();

    for op in trace.ops {
        match op {
            OverlayOp::BufferSet(s, c, b) => {
                overlay.dirty().set(&id, &cell_in(s, c), &bytes(b));
                model.dirty.insert((s, c), Some(bytes(b)));
            }
            OverlayOp::BufferClear(s, c) => {
                overlay.dirty().clear(&id, &cell_in(s, c));
                model.dirty.insert((s, c), None);
            }
            OverlayOp::CommitSet(s, c, b) => {
                // Committed seeds are written resolved (no event) so reads stay
                // pure dirty-over-committed.
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_in(s, c), Some(bytes(b)))], &[])
                    .await?;
                model.committed.insert((s, c), Some(bytes(b)));
            }
            OverlayOp::CommitClear(s, c) => {
                overlay
                    .lower()
                    .write_resolved(&collection_ref, &[(cell_in(s, c), None)], &[])
                    .await?;
                model.committed.insert((s, c), None);
            }
            OverlayOp::ClearSection(s) => {
                overlay
                    .dirty()
                    .clear_section(&id, SECTIONS[s as usize % SECTIONS.len()]);
                // The marker wipes ITS section's buffered cells and hides
                // that section's committed leg; siblings stay untouched;
                // later `BufferSet`s repopulate as survivors.
                model.dirty.retain(|&(sect, _), _| sect != s);
                model.cleared.insert(s);
            }
            OverlayOp::CommitClearSection(clear) => {
                // A committed write beneath the overlay: the cleared section's
                // committed leg collapses to exactly its survivors; the dirty
                // leg and its markers are untouched.
                seed_section_clear(overlay.lower(), &collection_ref, &mut model, clear).await?;
            }
            OverlayOp::Scan(req) => {
                let expected = scan_oracle(&model, req);
                if let Some(k) = req.partial {
                    // Early stop: the k-prefix matches the oracle prefix, then a
                    // follow-up full scan still yields the complete result
                    // (dropping a scan mid-stream corrupts nothing).
                    let k = (k as usize).min(expected.len());
                    if collect_scan(&overlay, &id, &req, Some(k)).await? != expected[..k] {
                        return Ok(false);
                    }
                }
                if collect_scan(&overlay, &id, &req, None).await? != expected {
                    return Ok(false);
                }
                let expected_keys: Vec<u8> = expected.iter().map(|(key, _)| *key).collect();
                if collect_scan_coordinates(&overlay, &id, &req).await? != expected_keys {
                    return Ok(false);
                }
            }
        }

        // Point leg: after every op (mutation OR scan), every cell of every
        // sampled section `get`s the model's answer — so point reads
        // interleave with the scans above and the section scoping of every
        // dirty/committed interaction is checked cell-by-cell.
        for s in 0..SECTIONS.len() as u8 {
            for c in 0..CELLS {
                if overlay
                    .get::<Values>(&id, cell_in(s, c).as_ref())
                    .await?
                    .into_inner()
                    != model.visible(s, c)
                {
                    return Ok(false);
                }
            }
        }

        // Batch leg: the same section's cells read through `get_many` (plus a
        // duplicate of coord 0) answer each position exactly as the point-`get`
        // oracle, and the two coord-0 positions co-observe. This runs the
        // overlay's split-and-scatter against the same model, after every op.
        for s in 0..SECTIONS.len() as u8 {
            // `CELLS + 1` (= 13) ≤ `CELL_BATCH`, so `chunks` yields one batch;
            // `CELLS ≥ 1` makes the iterator non-empty, so `next()` is `Some`.
            let batch = batch_of((0..CELLS).chain(iter::once(0)))?;
            let got = overlay
                .get_many::<Values>(&id, SECTIONS[s as usize], &batch.as_ref())
                .await?;
            let presence = overlay
                .get_many::<Presence>(&id, SECTIONS[s as usize], &batch.as_ref())
                .await?;
            if presence_of(&presence) != presence_of(&got) {
                return Ok(false);
            }
            if got.len() != CELLS as usize + 1 {
                return Ok(false);
            }
            for c in 0..CELLS {
                if got[c as usize] != model.visible(s, c) {
                    return Ok(false);
                }
            }
            // The duplicate coord-0 position co-observes the coord-0 answer.
            if got[CELLS as usize] != got[0] {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn presence_of<T>(batch: &[Option<T>]) -> CellBuffer<bool> {
    batch.iter().map(Option::is_some).collect()
}

/// Proves that a dirty value takes priority over a dirty section clear.
///
/// Duplicate reads must return the same value without a lower read.
pub(crate) async fn run_overlay_precedence_pin<S: CellStore>(
    counting: CountingCellStore<S>,
) -> Result<()> {
    let state_key = StateKey::new(Uuid::new_v4(), Arc::from("key"));
    let id = CollectionId::new(
        state_key,
        StateType::Application,
        StateName::try_new("entries")?,
    );
    let collection_ref = CollectionRef::new(id.clone(), None);
    let overlay = Overlay::new(Arc::new(DirtyStore::new()), counting.clone());
    // Committed base under the section.
    overlay
        .lower()
        .write_resolved(&collection_ref, &[(cell_in(0, 5), Some(bytes(42)))], &[])
        .await?;
    counting.reset();
    // An unsettled dirty section-clear, then a dirty `Set` repopulating coord 5.
    overlay.dirty().clear_section(&id, SECTIONS[0]);
    overlay.dirty().set(&id, &cell_in(0, 5), &bytes(7));
    let batch = batch_of([5, 5])?;
    let got = overlay
        .get_many::<Values>(&id, SECTIONS[0], &batch.as_ref())
        .await?;
    assert_eq!(got.len(), 2, "every input position is answered");
    assert_eq!(
        got[0],
        Some(bytes(7)),
        "a dirty Set beats an unsettled section-clear"
    );
    assert_eq!(got[0], got[1], "the duplicate position co-observes the Set");
    assert_eq!(
        counting.batch_reads(),
        0,
        "dirty-answered positions never reach the lower batch"
    );
    Ok(())
}
